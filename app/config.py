"""运行参数，全部可用环境变量覆盖。"""
import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
COLLECTOR_DB = Path(os.getenv("COLLECTOR_DB", str(DATA_DIR / "collector.db")))
SOURCE_DB = Path(os.getenv("SOURCE_DB", str(DATA_DIR / "source.db")))
# 未完成登记（响应已返回、尚未落库）的响应标记目录。必须与库文件一样由两个采集
# 进程共享：它是一条不依赖数据库写锁的跨进程协调通道。
COORD_DIR = Path(os.getenv("COORD_DIR", str(DATA_DIR / "coord")))

# 每个数据源允许的最大在途请求数（两个采集进程共用同一库，因此是全局值）
MAX_INFLIGHT_PER_SOURCE = int(os.getenv("MAX_INFLIGHT_PER_SOURCE", "2"))
# 同一数据源相邻两次“请求开始”的最小间隔（秒）
MIN_REQUEST_GAP_SECONDS = float(os.getenv("MIN_REQUEST_GAP_SECONDS", "1"))
# 采集轮询间隔
POLL_INTERVAL_SECONDS = float(os.getenv("POLL_INTERVAL_SECONDS", "0.25"))
# 单个采集进程同时处理的条目上限（在途名额的上限仍由数据源状态表裁决）
WORKER_CONCURRENCY = int(os.getenv("WORKER_CONCURRENCY", "5"))
# 心跳 / 租约时长
HEARTBEAT_INTERVAL_SECONDS = float(os.getenv("HEARTBEAT_INTERVAL_SECONDS", "2"))
LEASE_SECONDS = float(os.getenv("LEASE_SECONDS", "15"))
# 优雅退出时等待在途请求的最长时间
SHUTDOWN_GRACE_SECONDS = float(os.getenv("SHUTDOWN_GRACE_SECONDS", "20"))
HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "20"))
# SQLite 单次等写锁的时长。等待登记的重试循环不依赖它，但把它调小可以让
# “遇锁 -> 保留响应 -> 退避重试”的路径更快进入重试。
DB_BUSY_TIMEOUT_MS = int(os.getenv("DB_BUSY_TIMEOUT_MS", "10000"))
# 响应登记使用独立连接与更短的等锁上限：登记撞锁时必须尽快让出连接，
# 好让同一进程的心跳、认领和“别的源”继续推进。
REGISTRATION_BUSY_TIMEOUT_MS = int(
    os.getenv("REGISTRATION_BUSY_TIMEOUT_MS", "250")
)

# 响应登记重试：遇到数据库写锁时，保留响应并在本机上重试登记；不重新取源。
# 每次等待从 50ms 起翻倍，最多 2s；持续重试直到登记成功或进程优雅退出。
REGISTRATION_RETRY_MIN_SECONDS = float(
    os.getenv("REGISTRATION_RETRY_MIN_SECONDS", "0.05")
)
REGISTRATION_RETRY_MAX_SECONDS = float(
    os.getenv("REGISTRATION_RETRY_MAX_SECONDS", "2")
)
# 未登记标记的可回收时间：存活进程每次登记重试都会刷新标记，因此 TTL 只需
# 大于“两次刷新之间的最长间隔”（退避上限 2s + 一次短事务），即可保证不会
# 误收存活进程的标记；进程被杀后，最多这么久就会判定“无人接续”并恢复该源。
REGISTRATION_MARK_TTL_SECONDS = float(
    os.getenv("REGISTRATION_MARK_TTL_SECONDS", "10")
)
