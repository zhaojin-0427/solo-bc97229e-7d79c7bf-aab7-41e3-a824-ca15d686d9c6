"""故障注入工具：以“另一个连接”的身份持有 SQLite 写锁。

用于演示/复现“采集进程已收到响应、登记时写锁被占用”的场景：
    python -m app.holdlock --seconds 15
默认锁 /data/collector.db（或 $COLLECTOR_DB）。持锁期间采集进程的登记会
失败并进入“保留响应 -> 退避重试”，但 API 的进度/结果查询仍然可响应。
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import time

from . import config


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="持有采集库的 SQLite 写锁以复现登记冲突")
    parser.add_argument("--seconds", type=float, default=15.0, help="持锁秒数")
    parser.add_argument("--db", default=str(config.COLLECTOR_DB), help="库文件路径")
    parser.add_argument(
        "--journal",
        action="store_true",
        help="每 0.5 秒打印一次时间戳，便于观察等待过程",
    )
    args = parser.parse_args(argv)

    conn = sqlite3.connect(args.db, isolation_level=None, timeout=30)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO source_states(source_base) VALUES ('__holdlock__') "
            "ON CONFLICT(source_base) DO NOTHING"
        )
        print(
            f"已持写锁 {args.seconds}s：{os.path.abspath(args.db)}",
            flush=True,
        )
        deadline = time.time() + args.seconds
        while time.time() < deadline:
            if args.journal:
                print(f"  lock held t-{deadline - time.time():.1f}s", flush=True)
            time.sleep(0.5)
        print("即将释放写锁", flush=True)
    finally:
        conn.execute("ROLLBACK")
        conn.close()
    print("写锁已释放", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
