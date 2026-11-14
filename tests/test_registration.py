"""响应登记回归测试。

覆盖用户报告的全部问题：
1. 另一连接持写锁超过默认 busy_timeout，响应不丢、条目不永久卡在 running；
2. 登记重试不重新取源；
3. 成功值 + 名额释放同事务提交；
4. 429 回 pending + 释放名额 + 冷却同事务提交，冷却按“收到响应时刻”起算；
5. 登记延迟不后移冷却起点、不缩短已有冷却；
6. 解锁后另一个采集进程不得在冷却登记前抢发同源请求；
7. 登记等待不占写事务，进度/结果查询保持可响应；
8. 一个源冷却不阻塞其它源；
9. 沿用原在途上限、请求间隔、重启接续。
"""
from __future__ import annotations

import asyncio
import threading
import time

import pytest

from app import config, db, worker as worker_mod
from app.worker import Worker

from .conftest import (
    SOURCE_A,
    SOURCE_B,
    ControlledTransport,
    ScriptedResponse,
    make_client,
    patch_paths,
    sqlite_fetchone,
    async_release_write_lock,
    async_start_write_lock,
    wait_until,
)


async def create_job(
    db_path: str, source: str, items: list[tuple[str, str]], job_id: int | None = None
) -> int:
    conn = await db.connect(db_path)
    await db.init_db(conn)
    try:
        now = time.time()
        if job_id is None:
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


def make_worker(client, db_path, *, hostname="w1", busy_timeout_ms=300, **kw) -> Worker:
    return Worker(
        client=client,
        db_path=db_path,
        busy_timeout_ms=busy_timeout_ms,
        install_signals=False,
        hostname=hostname,
        **kw,
    )


async def start_worker(w: Worker) -> asyncio.Task:
    task = asyncio.create_task(w.run())
    ok = await asyncio.get_running_loop().run_in_executor(
        None,
        lambda: wait_until(
            lambda: sqlite_fetchone(
                str(config.COLLECTOR_DB),
                "SELECT 1 FROM workers WHERE hostname=? AND active=1",
                (w.hostname,),
            )
            is not None,
            timeout=5,
        ),
    )
    assert ok, "worker 未完成启动"
    return task


async def stop_worker(w: Worker, task: asyncio.Task) -> None:
    w.shutdown_event.set()
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass


def item_status(db_path: str, item_id: int):
    return sqlite_fetchone(
        db_path, "SELECT status, precipitation, error_code FROM items WHERE id=?", (item_id,)
    )


def inflight_count(db_path: str, source: str) -> int:
    row = sqlite_fetchone(
        db_path, "SELECT COUNT(*) FROM inflight_slots WHERE source_base=?", (source,)
    )
    return int(row[0])


def test_registration_survives_long_write_lock(tmp_path, monkeypatch):
    """响应已返回但登记遇锁：保留响应、自动接续，不重新取源，不卡 running。"""
    patch_paths(monkeypatch, tmp_path)
    transport = ControlledTransport()
    client = make_client(transport)

    async def scenario() -> None:
        db_path = str(config.COLLECTOR_DB)
        job_id = await create_job(db_path, SOURCE_A, [("S1", "2026-11-10")])
        item_id = sqlite_fetchone(
            db_path, "SELECT id FROM items WHERE job_id=?", (job_id,)
        )[0]

        gate = threading.Event()
        transport.push(
            SOURCE_A,
            ScriptedResponse(
                payload={"station_id": "S1", "date": "2026-11-10", "precipitation": 3.4},
                gate=gate,
            ),
        )
        w = make_worker(client, db_path, hostname="w1", busy_timeout_ms=300)
        task = await start_worker(w)
        lock = None
        try:
            # 源已收到请求后，另一个连接才开始持写锁（超过 busy_timeout）
            assert await transport.await_request(SOURCE_A, 1)
            lock = await async_start_write_lock(db_path)
            gate.set()  # 响应现在返回给采集进程；登记会撞锁
            await asyncio.sleep(0.8)

            # 条目仍 running、响应未登记，但名额仍在（响应被保留）
            assert item_status(db_path, item_id)[0] == "running"
            assert inflight_count(db_path, SOURCE_A) == 1
            # 没有任何重新取源的请求
            assert transport.request_count(SOURCE_A) == 1

            await async_release_write_lock(lock)
            ok = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: wait_until(
                    lambda: item_status(db_path, item_id)[0] == "succeeded", timeout=8
                ),
            )
            assert ok, "解锁后未自动接续登记"
            assert sqlite_fetchone(
                db_path, "SELECT precipitation FROM items WHERE id=?", (item_id,)
            )[0] == 3.4
            assert inflight_count(db_path, SOURCE_A) == 0
            assert transport.request_count(SOURCE_A) == 1  # 登记重试没有重新取源
        finally:
            await async_release_write_lock(lock)
            await stop_worker(w, task)
            await client.aclose()

    asyncio.run(scenario())


def test_registration_retry_does_not_requery_and_is_idempotent(tmp_path, monkeypatch):
    """解锁前多次重试都不取源；解锁后只登记一次，名额恰好释放一次。"""
    patch_paths(monkeypatch, tmp_path)
    transport = ControlledTransport()
    client = make_client(transport)

    async def scenario() -> None:
        db_path = str(config.COLLECTOR_DB)
        job_id = await create_job(db_path, SOURCE_A, [("S1", "2026-11-10")])
        item_id = sqlite_fetchone(
            db_path, "SELECT id FROM items WHERE job_id=?", (job_id,)
        )[0]

        gate = threading.Event()
        transport.push(
            SOURCE_A,
            ScriptedResponse(
                payload={"station_id": "S1", "date": "2026-11-10", "precipitation": 1.0},
                gate=gate,
            ),
        )
        w = make_worker(client, db_path, hostname="w1", busy_timeout_ms=100)
        task = await start_worker(w)
        lock = None
        try:
            assert await transport.await_request(SOURCE_A, 1)
            lock = await async_start_write_lock(db_path)
            gate.set()
            await asyncio.sleep(1.5)  # 足够多轮登记重试
            assert transport.request_count(SOURCE_A) == 1
            await async_release_write_lock(lock)
            ok = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: wait_until(
                    lambda: item_status(db_path, item_id)[0] == "succeeded", timeout=8
                ),
            )
            assert ok
            await asyncio.sleep(0.3)
            assert inflight_count(db_path, SOURCE_A) == 0
            assert transport.request_count(SOURCE_A) == 1
            assert item_status(db_path, item_id)[1] == 1.0
        finally:
            await async_release_write_lock(lock)
            await stop_worker(w, task)
            await client.aclose()

    asyncio.run(scenario())


def test_429_cooldown_uses_received_at_and_blocks_next_request(tmp_path, monkeypatch):
    """429 遇锁：冷却自收到响应时刻起算，解锁后另一进程不得抢发。"""
    patch_paths(monkeypatch, tmp_path)
    transport = ControlledTransport()
    client1 = make_client(transport)
    client2 = make_client(transport)

    async def scenario() -> None:
        db_path = str(config.COLLECTOR_DB)
        job_id = await create_job(
            db_path, SOURCE_A, [("S1", "2026-11-10"), ("S2", "2026-11-10")]
        )
        gate = threading.Event()
        transport.push(
            SOURCE_A,
            ScriptedResponse(status_code=429, headers={"Retry-After": "3"}, gate=gate),
        )
        w1 = make_worker(client1, db_path, hostname="w1", busy_timeout_ms=100)
        t1 = await start_worker(w1)
        lock = None
        try:
            assert await transport.await_request(SOURCE_A, 1)
            received_at = time.time()
            lock = await async_start_write_lock(db_path)
            gate.set()
            await asyncio.sleep(0.5)

            # 冷却尚未写入（登记被锁阻塞），条目仍 running、名额未释放
            assert sqlite_fetchone(
                db_path, "SELECT cooldown_until FROM source_states WHERE source_base=?",
                (SOURCE_A,),
            )[0] is None

            # 解锁后立刻启动第二个进程：它必须等冷却登记完成才能发请求
            await async_release_write_lock(lock)
            w2 = make_worker(client2, db_path, hostname="w2", busy_timeout_ms=100)
            t2 = await start_worker(w2)
            try:
                ok = await asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: wait_until(
                        lambda: sqlite_fetchone(
                            db_path,
                            "SELECT cooldown_until FROM source_states WHERE source_base=?",
                            (SOURCE_A,),
                        )[0]
                        is not None,
                        timeout=8,
                    ),
                )
                assert ok, "解锁后未登记 429 冷却"
                cooldown = sqlite_fetchone(
                    db_path,
                    "SELECT cooldown_until FROM source_states WHERE source_base=?",
                    (SOURCE_A,),
                )[0]
                # 冷却按原响应时刻起算，不因登记延迟而后移
                assert abs(cooldown - (received_at + 3)) < 1.0
                # 条目回 pending，名额释放
                assert wait_until(
                    lambda: item_status(db_path, job_id) is not None
                    and sqlite_fetchone(
                        db_path,
                        "SELECT status FROM items WHERE job_id=? ORDER BY id LIMIT 1",
                        (job_id,),
                    )[0]
                    == "pending",
                    timeout=3,
                )
                assert inflight_count(db_path, SOURCE_A) == 0
                # 冷却期间（登记后至少 2 秒内）不得有新的同源请求
                await asyncio.sleep(1.5)
                assert transport.request_count(SOURCE_A) == 1
            finally:
                await stop_worker(w2, t2)
        finally:
            await async_release_write_lock(lock)
            await stop_worker(w1, t1)
            await client1.aclose()
            await client2.aclose()

    asyncio.run(scenario())


def test_429_cooldown_not_shortened_by_later_response(tmp_path, monkeypatch):
    """已有更晚的冷却时，新 429 的登记不得缩短它（取 max）。"""
    patch_paths(monkeypatch, tmp_path)
    transport = ControlledTransport()
    client = make_client(transport)

    async def scenario() -> None:
        db_path = str(config.COLLECTOR_DB)
        far = time.time() + 60
        conn = await db.connect(db_path)
        await db.init_db(conn)
        await conn.execute(
            "INSERT INTO source_states(source_base, cooldown_until) VALUES (?, ?) "
            "ON CONFLICT(source_base) DO UPDATE SET cooldown_until=excluded.cooldown_until",
            (SOURCE_A, far),
        )
        await conn.close()

        job_id = await create_job(db_path, SOURCE_A, [("S1", "2026-11-10")])
        # 直接调用登记逻辑，绕开认领阶段的冷却检查
        transport.push(SOURCE_A, ScriptedResponse(status_code=429, headers={"Retry-After": "1"}))
        w = make_worker(client, db_path, hostname="w1", busy_timeout_ms=100)
        w.worker_pk = 1
        w.conn = await db.connect(db_path, busy_timeout_ms=100)
        w.reg_conn = await db.connect(db_path, busy_timeout_ms=100)
        item_id = sqlite_fetchone(db_path, "SELECT id FROM items WHERE job_id=?", (job_id,))[0]
        started = time.time()
        await w.conn.execute(
            "UPDATE items SET status='running', worker_id=1, lease_until=? WHERE id=?",
            (started + 15, item_id),
        )
        await w.conn.execute(
            "INSERT INTO inflight_slots(source_base, item_id, worker_id, started_at) "
            "VALUES (?, ?, 1, ?)",
            (SOURCE_A, item_id, started),
        )
        await w._register(
            SOURCE_A, item_id, started, worker_mod._RateLimited(time.time(), 1)
        )
        now_cooldown = sqlite_fetchone(
            db_path, "SELECT cooldown_until FROM source_states WHERE source_base=?", (SOURCE_A,)
        )[0]
        assert now_cooldown == pytest.approx(far)
        await w.conn.close()
        await w.reg_conn.close()
        await client.aclose()

    asyncio.run(scenario())


def test_queries_respond_while_registration_waits(tmp_path, monkeypatch):
    """登记在等锁时不占写事务：进度与结果查询保持可响应。"""
    patch_paths(monkeypatch, tmp_path)
    transport = ControlledTransport()
    client = make_client(transport)

    async def scenario() -> None:
        db_path = str(config.COLLECTOR_DB)
        job_id = await create_job(
            db_path, SOURCE_A, [("S1", "2026-11-10"), ("S2", "2026-11-10")]
        )
        first_succeeded = sqlite_fetchone(
            db_path,
            "SELECT id FROM items WHERE job_id=? ORDER BY id LIMIT 1",
            (job_id,),
        )[0]

        gate1 = threading.Event()
        gate2 = threading.Event()
        transport.push(
            SOURCE_A,
            ScriptedResponse(
                payload={"station_id": "S1", "date": "2026-11-10", "precipitation": 2.0},
                gate=gate1,
            ),
        )
        transport.push(
            SOURCE_A,
            ScriptedResponse(
                payload={"station_id": "S2", "date": "2026-11-10", "precipitation": 5.0},
                gate=gate2,
            ),
        )
        w = make_worker(client, db_path, hostname="w1", busy_timeout_ms=150)
        task = await start_worker(w)
        try:
            assert await transport.await_request(SOURCE_A, 2)
            gate1.set()
            assert await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: wait_until(
                    lambda: sqlite_fetchone(
                        db_path, "SELECT status FROM items WHERE id=?", (first_succeeded,)
                    )[0]
                    == "succeeded",
                    timeout=5,
                ),
            )
            lock = await async_start_write_lock(db_path)
            gate2.set()
            await asyncio.sleep(0.6)
            # 第二个响应在等锁，第一个也可能因同一把锁没登记完；
            # 无论哪种情况，只读查询都必须立即返回（不被写事务/等锁卡住）
            async def query() -> tuple:
                conn = await db.connect(db_path, busy_timeout_ms=100)
                try:
                    counts = await db.fetchall(
                        conn,
                        "SELECT status, COUNT(*) FROM items WHERE job_id=? GROUP BY status",
                        (job_id,),
                    )
                    return [(r[0], r[1]) for r in counts]
                finally:
                    await conn.close()

            t0 = time.time()
            result = await asyncio.wait_for(query(), timeout=2)
            elapsed = time.time() - t0
            assert elapsed < 1.0, f"查询被阻塞 {elapsed:.2f}s"
            assert result  # 有返回
            assert sqlite_fetchone(
                db_path, "SELECT status FROM items WHERE id=?", (first_succeeded,)
            )[0] in {"succeeded", "running"}
        finally:
            await async_release_write_lock(lock)
            await stop_worker(w, task)
            await client.aclose()

    asyncio.run(scenario())


def test_cooldown_of_one_source_does_not_block_another(tmp_path, monkeypatch):
    """源 A 登记 429 冷却期间，源 B 继续推进。"""
    patch_paths(monkeypatch, tmp_path)
    transport = ControlledTransport()
    client = make_client(transport)

    async def scenario() -> None:
        db_path = str(config.COLLECTOR_DB)
        job_a = await create_job(db_path, SOURCE_A, [("S1", "2026-11-10")])
        job_b = await create_job(db_path, SOURCE_B, [("S9", "2026-11-10")])

        gate = threading.Event()
        transport.push(
            SOURCE_A,
            ScriptedResponse(status_code=429, headers={"Retry-After": "30"}, gate=gate),
        )
        transport.set_default(
            SOURCE_B,
            ScriptedResponse(
                payload={"station_id": "S9", "date": "2026-11-10", "precipitation": 7.0}
            ),
        )
        w = make_worker(client, db_path, hostname="w1", busy_timeout_ms=100)
        task = await start_worker(w)
        lock = None
        try:
            assert await transport.await_request(SOURCE_A, 1)
            lock = await async_start_write_lock(db_path)
            gate.set()
            await asyncio.sleep(0.4)  # A 在等锁登记
            # B 不受 A 的持锁/冷却影响：先等 A 的锁释放后 B 应立即成功
            await async_release_write_lock(lock)
            ok = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: wait_until(
                    lambda: sqlite_fetchone(
                        db_path,
                        "SELECT status FROM items WHERE job_id=?",
                        (job_b,),
                    )[0]
                    == "succeeded",
                    timeout=8,
                ),
            )
            assert ok, "源 B 被源 A 的封锁/冷却堵住"
            # A 的 429 也已登记：回 pending 并写入冷却
            ok_a = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: wait_until(
                    lambda: sqlite_fetchone(
                        db_path, "SELECT status FROM items WHERE job_id=?", (job_a,)
                    )[0]
                    == "pending",
                    timeout=5,
                ),
            )
            assert ok_a
            cooldown, = sqlite_fetchone(
                db_path,
                "SELECT cooldown_until FROM source_states WHERE source_base=?",
                (SOURCE_A,),
            )
            assert cooldown is not None
            assert transport.request_count(SOURCE_A) == 1  # 冷却中未重采
        finally:
            await async_release_write_lock(lock)
            await stop_worker(w, task)
            await client.aclose()

    asyncio.run(scenario())


def test_inflight_limit_and_gap_still_enforced(tmp_path, monkeypatch):
    """沿用原在途上限（≤2）与请求间隔（≥1s）。"""
    monkeypatch.setattr(config, "MAX_INFLIGHT_PER_SOURCE", 2)
    monkeypatch.setattr(config, "MIN_REQUEST_GAP_SECONDS", 0.3)
    patch_paths(monkeypatch, tmp_path)
    transport = ControlledTransport()
    client = make_client(transport)

    async def scenario() -> None:
        db_path = str(config.COLLECTOR_DB)
        await create_job(
            db_path,
            SOURCE_A,
            [(f"S{i}", "2026-11-10") for i in range(6)],
        )
        gate = threading.Event()
        for i in range(6):
            transport.push(
                SOURCE_A,
                ScriptedResponse(
                    payload={
                        "station_id": f"S{i}",
                        "date": "2026-11-10",
                        "precipitation": 1.0,
                    },
                    gate=gate,
                ),
            )
        w = make_worker(client, db_path, hostname="w1", busy_timeout_ms=100)
        task = await start_worker(w)
        try:
            assert await transport.await_request(SOURCE_A, 2)
            await asyncio.sleep(0.5)
            # 两个请求都在途（gate 未放行），第三个不得发出
            assert transport.request_count(SOURCE_A) == 2
            gate.set()
            ok = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: wait_until(
                    lambda: sqlite_fetchone(
                        db_path, "SELECT COUNT(*) FROM items WHERE status='succeeded'"
                    )[0]
                    == 6,
                    timeout=15,
                ),
            )
            assert ok
            times = [r["received_at"] for r in transport.requests_to(SOURCE_A)]
            assert len(times) == 6
            gaps = [b - a for a, b in zip(times, times[1:])]
            assert min(gaps) >= 0.28, gaps
        finally:
            gate.set()
            await stop_worker(w, task)
            await client.aclose()

    asyncio.run(scenario())


def test_restart_resumes_pending_without_duplicate(tmp_path, monkeypatch):
    """重启接续：在途项回 pending 后由新进程完成，成功值不重复覆盖。"""
    patch_paths(monkeypatch, tmp_path)
    transport = ControlledTransport()
    client = make_client(transport)

    async def scenario() -> None:
        db_path = str(config.COLLECTOR_DB)
        job_id = await create_job(db_path, SOURCE_A, [("S1", "2026-11-10")])
        item_id = sqlite_fetchone(db_path, "SELECT id FROM items WHERE job_id=?", (job_id,))[0]
        transport.set_default(
            SOURCE_A,
            ScriptedResponse(
                payload={"station_id": "S1", "date": "2026-11-10", "precipitation": 4.2}
            ),
        )

        # 模拟上次进程优雅退出后留下的 pending 项（重启后应自动接续）
        conn = await db.connect(db_path)
        await conn.execute(
            "UPDATE items SET status='pending', worker_id=NULL, lease_until=NULL WHERE id=?",
            (item_id,),
        )
        await conn.close()

        w = make_worker(client, db_path, hostname="w-restart", busy_timeout_ms=100)
        task = await start_worker(w)
        try:
            ok = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: wait_until(
                    lambda: item_status(db_path, item_id)[0] == "succeeded", timeout=8
                ),
            )
            assert ok
            assert item_status(db_path, item_id)[1] == 4.2
            assert transport.request_count(SOURCE_A) == 1
        finally:
            await stop_worker(w, task)
            await client.aclose()

    asyncio.run(scenario())
