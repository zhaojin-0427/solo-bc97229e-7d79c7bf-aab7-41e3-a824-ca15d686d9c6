"""端到端（无 Docker 版）：真实启动 source + api + 两个采集进程。

与 docker-compose 拓扑一致：一个模拟气象源、一个 API、两个采集进程，全部用
子进程运行并共享同一目录下的 SQLite 库与 coord 目录。验证交互式文档演示的
三个动作：提交补采、进度查询、结果查询；再注入写锁验证响应不丢、429 冷却
不丢、解锁后自动接续。
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _wait_http(url: str, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, OSError):
            time.sleep(0.1)
    return False


def _get_json(url: str):
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.load(resp)


def _post_json(url: str, payload: dict):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.load(resp)


def _spawn(args: list[str], env: dict[str, str]) -> subprocess.Popen:
    full_env = dict(os.environ)
    full_env.update(env)
    full_env["PYTHONPATH"] = str(ROOT)
    return subprocess.Popen(
        args,
        cwd=str(ROOT),
        env=full_env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _stop(procs: list[subprocess.Popen]) -> str:
    out = []
    for p in procs:
        if p.poll() is None:
            p.send_signal(signal.SIGTERM)
    for p in procs:
        try:
            text, _ = p.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            p.kill()
            text, _ = p.communicate(timeout=5)
        out.append(text or "")
    return "\n".join(out)


@pytest.mark.slow
def test_compose_like_end_to_end(tmp_path):
    import sqlite3

    source_port = _free_port()
    api_port = _free_port()
    src_url = f"http://127.0.0.1:{source_port}"
    api_url = f"http://127.0.0.1:{api_port}"

    base_env = {
        "DATA_DIR": str(tmp_path),
        "COLLECTOR_DB": str(tmp_path / "collector.db"),
        "SOURCE_DB": str(tmp_path / "source.db"),
        "COORD_DIR": str(tmp_path / "coord"),
        "POLL_INTERVAL_SECONDS": "0.1",
        "MIN_REQUEST_GAP_SECONDS": "0.2",
        "HEARTBEAT_INTERVAL_SECONDS": "0.5",
        "LEASE_SECONDS": "5",
        "REGISTRATION_BUSY_TIMEOUT_MS": "250",
    }
    procs: list[subprocess.Popen] = []
    procs.append(
        _spawn(
            [sys.executable, "-m", "uvicorn", "app.source:app", "--port", str(source_port)],
            base_env,
        )
    )
    procs.append(
        _spawn(
            [sys.executable, "-m", "uvicorn", "app.api:app", "--port", str(api_port)],
            base_env,
        )
    )
    try:
        assert _wait_http(f"{src_url}/health"), "气象源未启动"
        assert _wait_http(f"{api_url}/health"), "API 未启动"
        for i in (1, 2):
            env = dict(base_env, HOSTNAME=f"e2e-worker{i}")
            procs.append(_spawn([sys.executable, "-m", "app.worker"], env))
        # 等两个 worker 注册
        db_path = str(tmp_path / "collector.db")
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                con = sqlite3.connect(db_path, timeout=2)
                n = con.execute(
                    "SELECT COUNT(*) FROM workers WHERE active=1"
                ).fetchone()[0]
                con.close()
                if n >= 2:
                    break
            except sqlite3.Error:
                pass
            time.sleep(0.2)
        else:
            pytest.fail("两个采集进程未完成注册")

        # 源策略：每 4 个请求一次 429，Retry-After=2
        _post_json(
            f"{src_url}/control/policy",
            {"mode": "nth429", "n": 4, "retry_after": 2},
        )
        # 1) 提交补采
        job = _post_json(
            f"{api_url}/jobs",
            {
                "source_base": src_url,
                "items": [
                    {"station_id": f"E{i % 3}", "date": f"2026-11-{10 + i:02d}"}
                    for i in range(8)
                ],
            },
        )
        job_id = job["job_id"]
        assert job["item_count"] == 8

        # 2) 进度可查
        progress = _get_json(f"{api_url}/jobs/{job_id}/progress")
        assert progress["total"] == 8

        # 3) 写锁注入：另起 holdlock 持锁 6 秒
        hold = _spawn(
            [sys.executable, "-m", "app.holdlock", "--seconds", "6"], base_env
        )
        procs.append(hold)
        time.sleep(0.5)
        # 持锁期间查询仍然可响应（不阻塞）
        t0 = time.time()
        _get_json(f"{api_url}/jobs/{job_id}/progress")
        assert time.time() - t0 < 2.0

        # 等任务完成
        deadline = time.time() + 60
        while time.time() < deadline:
            progress = _get_json(f"{api_url}/jobs/{job_id}/progress")
            if progress["done"]:
                break
            time.sleep(0.5)
        assert progress["done"], progress
        assert progress["succeeded"] == 8, progress

        # 4) 结果查询
        results = _get_json(f"{api_url}/jobs/{job_id}/results")
        assert len(results) == 8
        assert all(r["precipitation"] >= 0 for r in results)

        # 源侧核对：没有在“429 冷却登记完成”之前抢发的同源请求，
        # 且同源在途峰值 <= 2、相邻请求间隔 >= 配置值。
        stats = _get_json(f"{src_url}/control/stats")
        assert stats["max_concurrent_observed"] <= 2
        if stats["min_gap_seconds"] is not None:
            assert stats["min_gap_seconds"] >= 0.18

        items = _get_json(f"{api_url}/jobs/{job_id}/items")
        assert all(it["status"] == "succeeded" for it in items)
        assert any(it["attempts"] > 1 for it in items)  # 确实发生过 429 重试
        sources = _get_json(f"{api_url}/sources")
        assert sources and "cooldown_remaining" in sources[0]
    finally:
        logs = _stop(procs)
        if os.environ.get("E2E_VERBOSE_LOGS"):
            print(logs)
