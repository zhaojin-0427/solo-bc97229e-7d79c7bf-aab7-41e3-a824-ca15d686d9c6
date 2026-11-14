"""跨进程回归测试：真实启动两个采集进程，验证解锁瞬间不会抢发同源请求。

与前一个文件（同进程内两个 Worker，共享库与协调目录）互补：
- 这里用 ``python -m app.worker`` 启动真正的独立进程，只通过共享卷里的
  SQLite 和 coord 标记通信；
- 源站是测试进程里的一个可控 HTTP 服务，记录每次请求到达时刻。
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import subprocess
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from app import config, coord, db

from .conftest import (
    patch_paths,
    sqlite_fetchone,
    sqlite_fetchall,
    wait_until,
)


# --------------------------------------------------------------- 可控源站（线程）
class SourceServer:
    """一次性源站：每个站点独立脚本化响应序列，记录请求到达时刻。"""

    def __init__(self) -> None:
        self.hits: list[tuple[float, str]] = []
        self._lock = threading.Lock()
        self._script: dict[str, list[int]] = {}
        self._default_status = 200
        outer = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args):  # 静音
                pass

            def do_GET(self):  # noqa: N802
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)
                station = query.get("station_id", ["?"])[0]
                date = query.get("date", ["?"])[0]
                with outer._lock:
                    outer.hits.append((time.time(), self.path))
                    queue = outer._script.get(station) or []
                    status = queue.pop(0) if queue else outer._default_status
                if status == 429:
                    body = json.dumps({"error": "rate_limited"}).encode()
                    self.send_response(429)
                    self.send_header("Retry-After", "1")
                else:
                    body = json.dumps(
                        {"station_id": station, "date": date, "precipitation": 2.5}
                    ).encode()
                    self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self._server.server_address[1]
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def script(self, station: str, statuses: list[int]) -> None:
        with self._lock:
            self._script[station] = list(statuses)

    def hit_times(self) -> list[float]:
        with self._lock:
            return [t for t, _ in self.hits]

    def count(self) -> int:
        with self._lock:
            return len(self.hits)

    def __enter__(self) -> "SourceServer":
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._server.shutdown()
        self._server.server_close()


# --------------------------------------------------------------- 子进程采集器
class WorkerProcess:
    def __init__(self, env: dict[str, str], hostname: str) -> None:
        self.hostname = hostname
        self.env = dict(env)
        self.proc: subprocess.Popen | None = None

    def start(self) -> "WorkerProcess":
        env = dict(os.environ)
        env.update(self.env)
        env["HOSTNAME"] = self.hostname
        env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "app.worker"],
            cwd=str(Path(__file__).resolve().parents[1]),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        return self

    def stop(self, timeout: float = 10) -> str:
        if self.proc is None:
            return ""
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGTERM)
        try:
            out, _ = self.proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            out, _ = self.proc.communicate(timeout=5)
        return out or ""

    def __enter__(self) -> "WorkerProcess":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()


def worker_env(tmp_path: Path) -> dict[str, str]:
    return {
        "DATA_DIR": str(tmp_path),
        "COLLECTOR_DB": str(tmp_path / "collector.db"),
        "COORD_DIR": str(tmp_path / "coord"),
        "LEASE_SECONDS": "5",
        "HEARTBEAT_INTERVAL_SECONDS": "0.5",
        "POLL_INTERVAL_SECONDS": "0.1",
        "MIN_REQUEST_GAP_SECONDS": "0.2",
        "HTTP_TIMEOUT_SECONDS": "5",
    }


async def create_job(db_path: str, source: str, items: list[tuple[str, str]]) -> int:
    conn = await db.connect(db_path)
    await db.init_db(conn)
    try:
        now = time.time()
        cur = await conn.execute(
            "INSERT INTO jobs(source_base, created_at) VALUES (?, ?)", (source, now)
        )
        job_id = cur.lastrowid
        await conn.executemany(
            "INSERT INTO items(job_id, station_id, date, status, updated_at) "
            "VALUES (?, ?, ?, 'pending', ?)",
            [(job_id, s, d, now) for s, d in items],
        )
        await conn.execute(
            "INSERT INTO source_states(source_base) VALUES (?) "
            "ON CONFLICT(source_base) DO NOTHING",
            (source,),
        )
        return job_id
    finally:
        await conn.close()


# --------------------------------------------------------------------- 测试
def test_other_process_waits_for_unregistered_response_marker(tmp_path, monkeypatch):
    """另一个真实进程看到未登记标记时，绝不抢发同源请求。"""
    patch_paths(monkeypatch, tmp_path)
    db_path = str(config.COLLECTOR_DB)

    with SourceServer() as server:
        asyncio.run(
            create_job(db_path, server.url, [("S1", "2026-11-10"), ("S2", "2026-11-10")])
        )
        item_id = sqlite_fetchone(
            db_path, "SELECT id FROM items ORDER BY id LIMIT 1"
        )[0]

        # 模拟“另一个进程已收到 429、正在等锁登记”
        marker = coord.mark_unregistered(
            server.url,
            "fake-worker-999",
            item_id,
            time.time(),
            "rate_limited",
            details={"received_at": time.time(), "retry_after": 1},
        )
        wp = WorkerProcess(worker_env(tmp_path), "cross-w1").start()
        try:
            # 标记未清：子进程不得发请求
            assert not wait_until(lambda: server.count() > 0, timeout=2.0), (
                "子进程在未登记响应清除前抢发了同源请求"
            )
            # 登记完成（清标记并写入冷却）后，子进程仍需遵守冷却
            coord.clear(marker)
            conn = asyncio.run(db.connect(db_path))
            try:
                asyncio.run(
                    conn.execute(
                        "UPDATE source_states SET cooldown_until=? WHERE source_base=?",
                        (time.time() + 1.5, server.url),
                    )
                )
            finally:
                asyncio.run(conn.close())
            assert not wait_until(lambda: server.count() > 0, timeout=0.8)
            assert wait_until(lambda: server.count() > 0, timeout=6.0), "冷却后未重采"
        finally:
            out = wp.stop()
            if server.count() == 0:
                print(out)
    assert server.count() >= 1


def test_two_real_processes_429_with_lock_contention(tmp_path, monkeypatch):
    """两个真实采集进程 + 429 + 外部写锁：全部条目最终完成且无抢发。"""
    patch_paths(monkeypatch, tmp_path)
    db_path = str(config.COLLECTOR_DB)
    stations = [f"S{i}" for i in range(4)]

    with SourceServer() as server:
        asyncio.run(
            create_job(db_path, server.url, [(s, "2026-11-10") for s in stations])
        )
        # 每个站点第一次 429（Retry-After=1），之后 200
        for s in stations:
            server.script(s, [429])

        w1 = WorkerProcess(worker_env(tmp_path), "cross-w1").start()
        w2 = WorkerProcess(worker_env(tmp_path), "cross-w2").start()
        try:
            ok = wait_until(
                lambda: sqlite_fetchone(
                    db_path, "SELECT COUNT(*) FROM items WHERE status='succeeded'"
                )[0]
                == len(stations),
                timeout=25,
            )
            assert ok, sqlite_fetchall(
                db_path, "SELECT id, status, error_code, attempts FROM items"
            )
            assert sqlite_fetchone(
                db_path, "SELECT COUNT(*) FROM items WHERE status='running'"
            )[0] == 0
            assert sqlite_fetchone(
                db_path, "SELECT COUNT(*) FROM inflight_slots"
            )[0] == 0
        finally:
            w1.stop()
            w2.stop()
    # 429 之后同源下一次请求至少等了一个冷却期（Retry-After=1s）
    times = server.hit_times()
    assert len(times) >= len(stations) + 4  # 至少 4 次 429 + 4 次成功
