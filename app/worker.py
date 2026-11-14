"""采集进程。

关键约束（两进程共用同一 SQLite 库）：
1. 同源全局在途请求 <= 2；同一源相邻请求“开始”时刻至少间隔 1 秒。
2. 收到 429：以“收到响应的时刻”为起点，按 Retry-After 整数秒设置该源冷却；
   后续任何响应只会延长、不会缩短冷却；冷却期间其他源照常采集。
3. 429 项自动重试（回 pending）；其他 HTTP 错误或字段不符 -> 该项失败。
4. 等待网络 / 冷却 / 登记写锁期间不持有任何数据库写事务，查询始终可响应。
5. 进程退出：在途项回 pending、释放名额；重启后自动接续，成功项不重复保存。
6. 只回收心跳过期进程的名额，绝不动存活进程；冷却落在数据库里，重启不绕过。

响应登记（本模块的修复重点）：
- 响应一旦到手就“必须保留”。落库遇 SQLite 写锁（例如另一个连接持锁超过默认
  busy_timeout）时不回退、不重新取源，而是原地指数退避重试登记，直到成功或
  进程优雅退出。重试只做一次短写事务，等待期间不占事务。
- 收到响应（含 429）的瞬间，先在共享目录写“未登记标记”（见 coord.py）。
  标记存在期间任何进程都不会对该源发起新请求 —— 包括写锁刚释放时抢跑的另一个
  采集进程。因此 429 冷却一定先于下一次同源请求落库。
- 标记还保护“响应已到手、尚未落库”的条目不被心跳/租约回收成 pending 后被别的
  进程重取（避免重复向源站发请求）。
- 成功值 / 普通 HTTP 错误与名额释放在同一写事务提交；429 的冷却、条目回 pending
  与名额释放也在同一写事务提交。
- 冷却截止时刻 = 收到响应时刻 + Retry-After，登记延迟多久都不改变起点；
  新的冷却只取 max(新值, 旧值)，绝不缩短已有冷却。
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import time
import aiosqlite
import httpx

from . import config, coord, db


class Worker:
    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        db_path: str | None = None,
        busy_timeout_ms: int | None = None,
        install_signals: bool = True,
        hostname: str | None = None,
        registration_busy_timeout_ms: int | None = None,
    ) -> None:
        self.worker_pk: int | None = None
        self.hostname = hostname or os.getenv("HOSTNAME", f"worker-{os.getpid()}")
        self.worker_token = f"{self.hostname}-{os.getpid()}-{time.time_ns()}"
        self.conn: aiosqlite.Connection | None = None
        # 登记专用连接：等锁上限更短，撞锁时快速让出，不拖住同进程的其它入口
        self.reg_conn: aiosqlite.Connection | None = None
        self._registration_busy_timeout_ms = registration_busy_timeout_ms
        self.client: httpx.AsyncClient | None = client
        self._owns_client = client is None
        self._db_path = db_path
        self._busy_timeout_ms = busy_timeout_ms
        self._install_signals = install_signals
        self.write_lock = asyncio.Lock()
        # 登记专用连接同一时刻只能有一个事务：并发完成的响应排队登记
        self.reg_lock = asyncio.Lock()
        self.sem = asyncio.Semaphore(config.WORKER_CONCURRENCY)
        self.tasks: set[asyncio.Task] = set()
        self.shutdown_event = asyncio.Event()
        # 本进程的内存提示，减少无效轮询；数据库状态才是唯一权威
        self.mem_cooldown: dict[str, float] = {}
        self.mem_gap_hint: dict[str, float] = {}

    # ------------------------------------------------------------------ 生命周期
    async def run(self) -> None:
        config.COORD_DIR.mkdir(parents=True, exist_ok=True)
        self.conn = await db.connect(
            self._db_path, busy_timeout_ms=self._busy_timeout_ms
        )
        await db.init_db(self.conn)
        self.reg_conn = await db.connect(
            self._db_path,
            busy_timeout_ms=self._registration_busy_timeout_ms
            if self._registration_busy_timeout_ms is not None
            else config.REGISTRATION_BUSY_TIMEOUT_MS,
        )
        now = time.time()
        cur = await self.conn.execute(
            "INSERT INTO workers(hostname, started_at, last_heartbeat, active) "
            "VALUES (?, ?, ?, 1)",
            (self.hostname, now, now),
        )
        self.worker_pk = cur.lastrowid

        if self.client is None:
            self.client = httpx.AsyncClient(timeout=config.HTTP_TIMEOUT_SECONDS)
        if self._install_signals:
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                with contextlib.suppress(NotImplementedError):
                    loop.add_signal_handler(sig, self.shutdown_event.set)

        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await self._main_loop()
        finally:
            self.shutdown_event.set()
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task
            await self._shutdown()
            if self._owns_client and self.client is not None:
                await self.client.aclose()
            if self.reg_conn is not None:
                await self.reg_conn.close()
            await self.conn.close()

    async def _main_loop(self) -> None:
        while not self.shutdown_event.is_set():
            if len(self.tasks) < config.WORKER_CONCURRENCY:
                launched = await self._spawn_work()
                if not launched:
                    await self._pause(config.POLL_INTERVAL_SECONDS)
            else:
                await self._pause(config.POLL_INTERVAL_SECONDS)
            # 回收已结束的任务
            done = {t for t in self.tasks if t.done()}
            if done:
                self.tasks -= done
                for t in done:
                    exc = t.exception()
                    if exc:
                        print(f"[{self.hostname}] task error: {exc!r}", flush=True)

    async def _pause(self, seconds: float) -> None:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.shutdown_event.wait(), timeout=seconds)

    async def _spawn_work(self) -> bool:
        """每个轮询周期为每个“当前可发起请求”的源至多启动一个请求。"""
        assert self.conn is not None
        async with self.write_lock:
            rows = await db.fetchall(
                self.conn,
                "SELECT source_base, last_started_at, cooldown_until FROM source_states",
            )
            sources = [r["source_base"] for r in rows]
            # 还没有状态行的源（新提交的任务）也需要考虑
            job_rows = await db.fetchall(
                self.conn,
                "SELECT DISTINCT j.source_base AS sb FROM jobs j "
                "JOIN items i ON i.job_id = j.id WHERE i.status = 'pending'",
            )
            for r in job_rows:
                if r["sb"] not in sources:
                    sources.append(r["sb"])

        launched = False
        for source in sources:
            if len(self.tasks) >= config.WORKER_CONCURRENCY:
                break
            state = await self._acquire(source)
            if state is None:
                continue
            task = asyncio.create_task(self._process_item(source, *state))
            self.tasks.add(task)
            launched = True
        return launched

    # ------------------------------------------------------------- 名额/限速获取
    async def _acquire(self, source: str):
        """原子地检查冷却/在途/1秒间隔并认领一个 pending 项。

        返回 (item_id, station_id, date, request_started_at)；不可发起返回 None。
        整个判定在一个 BEGIN IMMEDIATE 短事务内完成，跨进程串行化。
        注意：事务的每个出口都必须 COMMIT 或 ROLLBACK，绝不能在事务内裸 return。
        """
        assert self.conn is not None
        now = time.time()
        hint = self.mem_cooldown.get(source)
        if hint and hint > now:
            return None
        gap_hint = self.mem_gap_hint.get(source)
        if gap_hint and gap_hint > now:
            return None

        # 其它进程的响应还在等登记时，绝不对该源发新请求。这里只是一次只读
        # 文件检查，不占数据库写事务；检查到标记就跳过该源、本轮去推进其它源，
        # 绝不在主循环里阻塞等待（否则一个源会拖住所有源）。
        if coord.source_is_registering(source):
            return None

        async with self.write_lock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
            except aiosqlite.OperationalError:
                return None
            # BEGIN 可能等过写锁（busy_timeout），上面的时间戳可能已经过期。
            # 认领时一律重新取墙钟：否则会把旧时刻记成“请求开始”，使请求间隔
            # 和刚登记的 429 冷却都被绕过。
            now = time.time()
            try:
                row = await db.fetchone(
                    self.conn,
                    "SELECT last_started_at, cooldown_until FROM source_states "
                    "WHERE source_base = ?",
                    (source,),
                )
                last_started = row["last_started_at"] if row else None
                cooldown_until = row["cooldown_until"] if row else None

                reason: str | None = None
                if cooldown_until and cooldown_until > now:
                    self.mem_cooldown[source] = cooldown_until
                    reason = "cooldown"
                else:
                    inflight = await db.fetchone(
                        self.conn,
                        "SELECT COUNT(*) AS c FROM inflight_slots WHERE source_base = ?",
                        (source,),
                    )
                    if inflight["c"] >= config.MAX_INFLIGHT_PER_SOURCE:
                        reason = "inflight_full"
                    elif (
                        last_started is not None
                        and now - last_started < config.MIN_REQUEST_GAP_SECONDS
                    ):
                        self.mem_gap_hint[source] = (
                            last_started + config.MIN_REQUEST_GAP_SECONDS
                        )
                        reason = "gap"
                    elif coord.source_is_registering(source):
                        # 等锁判定的瞬间刚写下标记：这一轮不认领
                        reason = "registering"

                item = None
                if reason is None:
                    item = await db.fetchone(
                        self.conn,
                        "SELECT i.id, i.station_id, i.date FROM items i "
                        "JOIN jobs j ON j.id = i.job_id "
                        "WHERE j.source_base = ? AND i.status = 'pending' "
                        "ORDER BY i.id LIMIT 1",
                        (source,),
                    )
                    if item is None:
                        reason = "no_pending"

                if item is not None:
                    await self.conn.execute(
                        "UPDATE items SET status='running', worker_id=?, "
                        "attempts=attempts+1, lease_until=?, updated_at=? WHERE id=?",
                        (
                            self.worker_pk,
                            now + config.LEASE_SECONDS,
                            now,
                            item["id"],
                        ),
                    )
                    await self.conn.execute(
                        "INSERT INTO inflight_slots(source_base, item_id, worker_id, started_at) "
                        "VALUES (?, ?, ?, ?)",
                        (source, item["id"], self.worker_pk, now),
                    )
                    await self.conn.execute(
                        "INSERT INTO source_states(source_base, last_started_at, cooldown_until) "
                        "VALUES (?, ?, NULL) "
                        "ON CONFLICT(source_base) DO UPDATE SET last_started_at=excluded.last_started_at",
                        (source, now),
                    )
                # 唯一的事务出口：无论是否认领都提交（只读判定也需结束事务）
                await self.conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.conn.execute("ROLLBACK")
                raise
        if item is None:
            return None
        return item["id"], item["station_id"], item["date"], now

    # ------------------------------------------------------------------ 单条采集
    async def _process_item(
        self, source: str, item_id: int, station_id: str, date: str, started_at: float
    ) -> None:
        async with self.sem:
            try:
                assert self.client is not None
                url = source.rstrip("/") + "/daily"
                # 等待网络期间不持有任何数据库事务
                resp = await self.client.get(
                    url, params={"station_id": station_id, "date": date}
                )
                received_at = time.time()
                outcome = self._interpret(resp, station_id, date, received_at)
            except httpx.HTTPError as exc:
                outcome = _Failed("NETWORK_ERROR", str(exc))
            except asyncio.CancelledError:
                # 优雅退出路径：shutdown() 会统一回滚，这里不做任何写操作
                raise
            except Exception as exc:  # 防御性兜底，保证 worker 不挂
                outcome = _Failed("WORKER_ERROR", repr(exc))

            # 到这里响应（或网络错误结论）已经确定：先落标记，再登记，绝不重取源。
            registered = await self._register(source, item_id, started_at, outcome)
            if not registered:
                print(
                    f"[{self.hostname}] 退出前响应仍未登记，已保留 (source={source} item={item_id})",
                    flush=True,
                )

    @classmethod
    def _interpret(
        cls, resp: httpx.Response, station_id: str, date: str, received_at: float
    ) -> "_Outcome":
        if resp.status_code == 429:
            return _RateLimited(received_at, cls._parse_retry_after(resp))
        if resp.status_code != 200:
            return _Failed(
                f"HTTP_{resp.status_code}", f"上游返回 HTTP {resp.status_code}"
            )
        try:
            payload = resp.json()
        except Exception as exc:
            return _Failed("BAD_JSON", f"响应不是合法 JSON: {exc}")
        error = cls._validate(payload, station_id, date)
        if error:
            return _Failed("FIELD_MISMATCH", error)
        return _Succeeded(float(payload["precipitation"]))  # type: ignore[arg-type]

    @staticmethod
    def _parse_retry_after(resp: httpx.Response) -> int:
        raw = resp.headers.get("Retry-After", "5")
        try:
            value = int(raw)
            return max(0, value)
        except (TypeError, ValueError):
            return 5

    @staticmethod
    def _validate(payload: object, station_id: str, date: str) -> str | None:
        """核对返回站号、日期，并校验非负毫米降水量。"""
        if not isinstance(payload, dict):
            return "响应体不是 JSON 对象"
        if payload.get("station_id") != station_id:
            return f"站号不符: 期望 {station_id}，实际 {payload.get('station_id')!r}"
        if payload.get("date") != date:
            return f"日期不符: 期望 {date}，实际 {payload.get('date')!r}"
        precip = payload.get("precipitation")
        if isinstance(precip, bool) or not isinstance(precip, (int, float)):
            return f"降水量不是数字: {precip!r}"
        if precip < 0:
            return f"降水量为负值: {precip}"
        return None

    # ------------------------------------------------------------------ 响应登记
    async def _register(
        self, source: str, item_id: int, started_at: float, outcome: "_Outcome"
    ) -> bool:
        """把已到手的响应写入库；遇写锁则原地重试，不重新取源。

        标记文件在第一次尝试前写好，直到登记成功才删除。返回 True 表示登记
        完成（或条目已被其它路径结束）；False 表示进程退出前仍未登记。
        """
        assert self.conn is not None
        mark = coord.mark_unregistered(
            source,
            self.worker_token,
            item_id,
            started_at,
            outcome.kind,
            details=_mark_details(outcome),
        )
        delay = config.REGISTRATION_RETRY_MIN_SECONDS
        attempts = 0
        try:
            while True:
                # 登记是一次独立的短写事务；等待锁期间不持有事务。
                if await self._try_register(source, item_id, started_at, outcome):
                    coord.clear(mark)
                    return True
                if self.shutdown_event.is_set():
                    # 未登记：标记保留响应结论，退出流程把条目回 pending；标记由
                    # 进程退出清理或 TTL 回收，不会让其它进程误判为“可抢发”。
                    return False
                attempts += 1
                if attempts == 1:
                    print(
                        f"[{self.hostname}] 登记遇写锁，保留响应等待接续 "
                        f"(source={source} item={item_id} kind={outcome.kind})",
                        flush=True,
                    )
                coord.touch(mark)  # 仍在接续中，刷新活标志
                await self._pause(delay)
                delay = min(delay * 2, config.REGISTRATION_RETRY_MAX_SECONDS)
        except asyncio.CancelledError:
            # 取消（退出/超时）：标记留在原地，语义不丢；退出流程会把条目回
            # pending，标记由退出清理或 TTL 回收，其它进程不会误判为可抢发。
            raise

    async def _try_register(
        self, source: str, item_id: int, started_at: float, outcome: "_Outcome"
    ) -> bool:
        """尝试一次短事务登记。返回 False 表示写锁被占用（可重试），
        True 表示已成功（含“条目已被其它路径结束，无需再写”）。

        使用登记专用连接与较短的等锁上限：撞锁尽快失败并让出，主连接始终
        留给心跳、认领与其它源，不会让本进程被一次长等锁拖住。
        """
        assert self.reg_conn is not None
        async with self.reg_lock:
            try:
                await self.reg_conn.execute("BEGIN IMMEDIATE")
            except aiosqlite.OperationalError:
                return False
            try:
                await self._apply_outcome(
                    self.reg_conn, source, item_id, started_at, outcome
                )
                await self.reg_conn.execute("COMMIT")
                return True
            except aiosqlite.OperationalError:
                # 写锁类错误：事务未生效，回滚后由重试循环再试
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.reg_conn.execute("ROLLBACK")
                return False
            except Exception:
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.reg_conn.execute("ROLLBACK")
                raise

    async def _apply_outcome(
        self,
        conn: aiosqlite.Connection,
        source: str,
        item_id: int,
        started_at: float,
        outcome: "_Outcome",
    ) -> None:
        """在一个已开启的写事务内落定条目，并与名额释放原子提交。"""
        now = time.time()

        if isinstance(outcome, _RateLimited):
            # 冷却按“收到响应时刻”起算；登记晚到多久都不改变起点。
            new_until = outcome.received_at + outcome.retry_after
            row = await db.fetchone(
                conn,
                "SELECT cooldown_until FROM source_states WHERE source_base=?",
                (source,),
            )
            old_until = row["cooldown_until"] if row else None
            # 后续响应不能缩短冷却：只取更晚的截止时刻
            effective = max(new_until, old_until) if old_until else new_until
            await conn.execute(
                "INSERT INTO source_states(source_base, cooldown_until) "
                "VALUES (?, ?) "
                "ON CONFLICT(source_base) DO UPDATE SET "
                "cooldown_until=excluded.cooldown_until",
                (source, effective),
            )
            await conn.execute(
                "UPDATE items SET status='pending', worker_id=NULL, "
                "lease_until=NULL, error_code='HTTP_429', error_message=?, updated_at=? "
                "WHERE id=? AND status='running' AND worker_id=?",
                (
                    f"收到 429，冷却至 {effective:.3f}"
                    f"（Retry-After={outcome.retry_after}s，"
                    f"自收到响应时刻 {outcome.received_at:.3f} 起算）",
                    now,
                    item_id,
                    self.worker_pk,
                ),
            )
            # 内存提示只是加速；以库内值为准
            self.mem_cooldown[source] = effective
        elif isinstance(outcome, _Succeeded):
            await conn.execute(
                "UPDATE items SET status='succeeded', precipitation=?, "
                "error_code=NULL, error_message=NULL, lease_until=NULL, updated_at=? "
                "WHERE id=? AND status='running' AND worker_id=?",
                (outcome.precipitation, now, item_id, self.worker_pk),
            )
        else:  # _Failed
            await conn.execute(
                "UPDATE items SET status='failed', error_code=?, error_message=?, "
                "lease_until=NULL, updated_at=? "
                "WHERE id=? AND status='running' AND worker_id=?",
                (outcome.code, outcome.message, now, item_id, self.worker_pk),
            )
        # 同一个事务里释放名额：条目状态与名额要么一起生效，要么都不生效。
        await conn.execute(
            "DELETE FROM inflight_slots WHERE item_id=? AND started_at=?",
            (item_id, started_at),
        )

    # ------------------------------------------------------------- 心跳与死亡回收
    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(config.HEARTBEAT_INTERVAL_SECONDS)
            if self.shutdown_event.is_set():
                return
            await self._heartbeat()

    async def _heartbeat(self) -> None:
        assert self.conn is not None and self.worker_pk is not None
        now = time.time()
        async with self.write_lock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
            except aiosqlite.OperationalError:
                return
            try:
                # 自己的心跳
                await self.conn.execute(
                    "UPDATE workers SET last_heartbeat=? WHERE id=?",
                    (now, self.worker_pk),
                )
                # 续租自己的在途项（慢请求不会因租约过期被误判）
                await self.conn.execute(
                    "UPDATE items SET lease_until=? WHERE worker_id=? AND status='running'",
                    (now + config.LEASE_SECONDS, self.worker_pk),
                )
                # 只回收“心跳过期”的进程；但“响应已到手、正在等锁登记”的条目
                # 受 coord 标记保护，不能在这里被放回 pending 让其它进程重取。
                protected = sorted(coord.active_item_ids())
                dead = await db.fetchall(
                    self.conn,
                    "SELECT id FROM workers WHERE active=1 AND last_heartbeat < ?",
                    (now - config.LEASE_SECONDS,),
                )
                for row in dead:
                    dead_id = row["id"]
                    await self._reclaim_worker(dead_id, protected, now)
                    await self.conn.execute(
                        "UPDATE workers SET active=0 WHERE id=?", (dead_id,)
                    )
                # 兜底：租约过期且无对应存活 worker 的 running 项
                guard = (
                    " AND id NOT IN ({0})".format(",".join("?" * len(protected)))
                    if protected
                    else ""
                )
                await self.conn.execute(
                    f"UPDATE items SET status='pending', worker_id=NULL, "
                    f"lease_until=NULL, updated_at=? "
                    f"WHERE status='running' AND lease_until < ? "
                    f"AND (worker_id IS NULL OR worker_id IN "
                    f"(SELECT id FROM workers WHERE active=0)){guard}",
                    (now, now, *protected),
                )
                # 对应名额一并回收：只保留“条目仍 running”的名额（即受保护、
                # 响应还在等登记的条目），避免死进程留下永久占位。
                await self.conn.execute(
                    "DELETE FROM inflight_slots WHERE item_id NOT IN "
                    "(SELECT id FROM items WHERE status='running')"
                )
                await self.conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.conn.execute("ROLLBACK")
                raise

    async def _reclaim_worker(
        self, dead_id: int, protected: list[int], now: float
    ) -> None:
        """回收死亡进程名下条目；响应正在等登记的条目保留 running 与名额。"""
        assert self.conn is not None
        item_guard = (
            " AND id NOT IN ({0})".format(",".join("?" * len(protected)))
            if protected
            else ""
        )
        await self.conn.execute(
            f"UPDATE items SET status='pending', worker_id=NULL, "
            f"lease_until=NULL, updated_at=? "
            f"WHERE worker_id=? AND status='running'{item_guard}",
            (now, dead_id, *protected),
        )
        slot_guard = (
            " AND item_id NOT IN ({0})".format(",".join("?" * len(protected)))
            if protected
            else ""
        )
        await self.conn.execute(
            f"DELETE FROM inflight_slots WHERE worker_id=?{slot_guard}",
            (dead_id, *protected),
        )

    # ------------------------------------------------------------------ 优雅退出
    async def _shutdown(self) -> None:
        assert self.conn is not None and self.worker_pk is not None
        # 先等在途请求自然结束（它们会自己落库并释放名额）；仍在等锁登记的
        # 响应会在这段宽限期内继续重试，尽量不让已经到手的响应白等。
        # （收到 SIGTERM 时 shutdown_event 已置位，登记循环会在当前这次重试后
        #  退出并保留标记，条目随后由下面的回滚语句变回 pending。）
        if self.tasks:
            _, pending = await asyncio.wait(
                list(self.tasks),
                timeout=config.SHUTDOWN_GRACE_SECONDS,
                return_when=asyncio.ALL_COMPLETED,
            )
            for t in pending:
                t.cancel()
            with contextlib.suppress(Exception):
                await asyncio.gather(*pending, return_exceptions=True)
        # 未登记成功的标记：本进程已不再接续，清理掉；条目由下面的回滚语句
        # 变回 pending（重启/其它进程重新采集），标记不会错误地挡住该源。
        coord.clear_worker_marks(self.worker_token)
        now = time.time()
        async with self.write_lock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
            except aiosqlite.OperationalError:
                return
            try:
                # 仍未结束的项回 pending，重启后接续；已 succeeded/failed 的不受影响
                await self.conn.execute(
                    "UPDATE items SET status='pending', worker_id=NULL, "
                    "lease_until=NULL, updated_at=? "
                    "WHERE worker_id=? AND status='running'",
                    (now, self.worker_pk),
                )
                await self.conn.execute(
                    "DELETE FROM inflight_slots WHERE worker_id=?", (self.worker_pk,)
                )
                await self.conn.execute(
                    "UPDATE workers SET active=0, last_heartbeat=? WHERE id=?",
                    (now, self.worker_pk),
                )
                await self.conn.execute("COMMIT")
            except Exception:
                with contextlib.suppress(aiosqlite.OperationalError):
                    await self.conn.execute("ROLLBACK")
                raise


# ---------------------------------------------------------------- 响应结论载体
class _Outcome:
    kind: str = ""


class _Succeeded(_Outcome):
    kind = "succeeded"

    def __init__(self, precipitation: float) -> None:
        self.precipitation = precipitation


class _Failed(_Outcome):
    kind = "failed"

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message


class _RateLimited(_Outcome):
    kind = "rate_limited"

    def __init__(self, received_at: float, retry_after: int) -> None:
        self.received_at = received_at
        self.retry_after = retry_after


def _mark_details(outcome: _Outcome) -> dict:
    """标记文件里保存响应结论摘要，便于排查与崩溃后判断。"""
    if isinstance(outcome, _RateLimited):
        return {
            "received_at": outcome.received_at,
            "retry_after": outcome.retry_after,
        }
    if isinstance(outcome, _Succeeded):
        return {"precipitation": outcome.precipitation}
    if isinstance(outcome, _Failed):
        return {"error_code": outcome.code}
    return {}


async def main() -> None:
    await Worker().run()


if __name__ == "__main__":
    asyncio.run(main())
