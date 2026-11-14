"""可稳定复现“响应已返回、登记遇写锁”的测试夹具。

核心能力：
- ``ControlledTransport``：httpx 自定义 transport，按脚本返回响应；
  每次响应只在测试放行时才交还给采集进程，因此可以精确制造
  “源已返回 -> 登记撞锁 -> 解锁”的时序。
- ``WriteLock``：独立 SQLite 连接持有 RESERVED 写锁（BEGIN IMMEDIATE），
  用线程模拟“另一个连接持写锁超过默认 busy_timeout”。
- ``SourceLog``：记录每个“请求到达”时刻，用于断言“登记重试不重新取源”
  以及“冷却前不得有新请求”。
"""
from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import threading
import time
from dataclasses import dataclass, field

import httpx

from app import config, db

SOURCE_A = "http://source-a.test"
SOURCE_B = "http://source-b.test"


# ---------------------------------------------------------------- 受控响应
@dataclass
class ScriptedResponse:
    status_code: int = 200
    payload: dict | None = None
    headers: dict[str, str] = field(default_factory=dict)
    # 响应交给采集进程前先等待测试放行（None 表示立即返回）
    gate: threading.Event | None = None
    raw_body: bytes | None = None

    def build(self) -> httpx.Response:
        if self.raw_body is not None:
            return httpx.Response(
                self.status_code, content=self.raw_body, headers=self.headers
            )
        body = self.payload if self.payload is not None else {}
        return httpx.Response(self.status_code, json=body, headers=self.headers)


class ControlledTransport(httpx.AsyncBaseTransport):
    """记录请求时刻，并按顺序/按站点返回脚本化响应。"""

    def __init__(self) -> None:
        self.requests: list[dict] = []
        self._lock = threading.Lock()
        self._script: dict[str, list[ScriptedResponse]] = {}
        self._default: dict[str, ScriptedResponse] = {}
        # 每个请求到达时的事件，测试可等待“源已收到请求”
        self.request_events: list[threading.Event] = []

    # ---------------------------------------------------------------- 脚本设置
    def push(self, source: str, resp: ScriptedResponse) -> None:
        self._script.setdefault(source, []).append(resp)

    def set_default(self, source: str, resp: ScriptedResponse) -> None:
        self._default[source] = resp

    def clear(self) -> None:
        with self._lock:
            self._script.clear()
            self._default.clear()
            self.requests.clear()
            self.request_events.clear()

    # ---------------------------------------------------------------- 传输实现
    def requests_to(self, source: str) -> list[dict]:
        with self._lock:
            return [r for r in self.requests if r["source"] == source]

    def request_count(self, source: str) -> int:
        return len(self.requests_to(source))

    def wait_for_request(self, source: str, count: int = 1, timeout: float = 5.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.request_count(source) >= count:
                return True
            time.sleep(0.01)
        return False

    async def await_request(
        self, source: str, count: int = 1, timeout: float = 5.0
    ) -> bool:
        """在事件循环内等待请求到达（不能阻塞循环，否则 worker 无法推进）。"""
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.request_count(source) >= count:
                return True
            await asyncio.sleep(0.01)
        return False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        source = str(request.url).split("/daily")[0]
        received_at = time.time()
        with self._lock:
            self.requests.append(
                {
                    "source": source,
                    "url": str(request.url),
                    "received_at": received_at,
                }
            )
            queue = self._script.get(source) or []
            spec = queue.pop(0) if queue else self._default.get(source) or ScriptedResponse(
                payload={"station_id": "x", "date": "x", "precipitation": 0.0}
            )
            event = threading.Event()
            self.request_events.append(event)
        event.set()
        if spec.gate is not None:
            # 响应“已返回”之前先阻塞；测试放行后才进入解析/登记阶段
            while not spec.gate.is_set():
                await asyncio.sleep(0.005)
        return spec.build()


def make_client(transport: ControlledTransport) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=transport, timeout=10)


# ---------------------------------------------------------------- 写锁
class WriteLock:
    """独立连接持有 SQLite 写锁（RESERVED），模拟另一个进程持锁。

    连接必须创建、提交、关闭都在同一个线程里，所以锁由一个专职线程持有，
    release() 只是通知该线程回滚并退出。
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.held = threading.Event()
        self.released = threading.Event()
        self._stop = threading.Event()
        self.last_error: Exception | None = None
        self._thread: threading.Thread | None = None

    def _hold(self) -> None:
        conn = sqlite3.connect(self.db_path, isolation_level=None, timeout=30)
        try:
            conn.execute("PRAGMA busy_timeout=5000")
            deadline = time.time() + 10
            while True:
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    break
                except sqlite3.OperationalError as exc:
                    self.last_error = exc
                    if time.time() > deadline:
                        raise
                    time.sleep(0.02)
            conn.execute(
                "INSERT INTO source_states(source_base) VALUES ('__lock_probe__') "
                "ON CONFLICT(source_base) DO NOTHING"
            )
            self.held.set()
            self._stop.wait(60)
        finally:
            with contextlib.suppress(sqlite3.Error):
                conn.execute("ROLLBACK")
            conn.close()
            self.released.set()

    def acquire(self, timeout: float = 10.0) -> "WriteLock":
        self._thread = threading.Thread(target=self._hold, daemon=True)
        self._thread.start()
        if not self.held.wait(timeout):
            raise RuntimeError("无法获得测试写锁")
        return self

    def release(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
        self.released.set()

    def __enter__(self) -> "WriteLock":
        return self

    def __exit__(self, *exc) -> None:
        self.release()


def start_write_lock(db_path: str) -> WriteLock:
    """在后台线程获取写锁并保持；返回后锁已生效。"""
    return WriteLock(db_path).acquire()


async def async_start_write_lock(db_path: str) -> WriteLock:
    """在事件循环里安全地获取写锁。

    不能在事件循环线程里同步等待写锁：采集进程的某个短写事务可能正处在
    “已 BEGIN、等下一轮事件循环 COMMIT”的窗口里，阻塞循环会让双方互相等待。
    """
    return await asyncio.get_running_loop().run_in_executor(
        None, start_write_lock, db_path
    )


def release_write_lock(lock: WriteLock | None) -> None:
    if lock is not None:
        lock.release()


async def async_release_write_lock(lock: WriteLock | None) -> None:
    await asyncio.get_running_loop().run_in_executor(None, release_write_lock, lock)


# ---------------------------------------------------------------- 数据库辅助
async def open_conn(db_path: str, busy_timeout_ms: int = 10000):
    conn = await db.connect(db_path, busy_timeout_ms=busy_timeout_ms)
    await db.init_db(conn)
    return conn


def sqlite_fetchall(db_path: str, sql: str, params: tuple = ()) -> list[tuple]:
    con = sqlite3.connect(db_path, timeout=5)
    try:
        return con.execute(sql, params).fetchall()
    finally:
        con.close()


def sqlite_fetchone(db_path: str, sql: str, params: tuple = ()):
    rows = sqlite_fetchall(db_path, sql, params)
    return rows[0] if rows else None


def wait_until(predicate, timeout: float = 5.0, interval: float = 0.01) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


def patch_paths(monkeypatch, tmp_path) -> None:
    """把库与协调目录都指向临时目录（两个 worker 共享同一份）。"""
    monkeypatch.setattr(config, "DATA_DIR", tmp_path)
    monkeypatch.setattr(config, "COLLECTOR_DB", tmp_path / "collector.db")
    monkeypatch.setattr(config, "COORD_DIR", tmp_path / "coord")
    (tmp_path / "coord").mkdir(parents=True, exist_ok=True)
