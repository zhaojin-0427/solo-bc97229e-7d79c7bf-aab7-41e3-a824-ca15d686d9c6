"""未登记响应标记：跨进程、不依赖数据库写锁的协调通道。

场景：采集进程收到源响应后，落库时写锁被另一连接占用超过 busy_timeout。
要求是“保留响应并自动接续”，不能重新取源，也不能只靠延长超时。

做法：
- 进程收到响应（含 429）后，立刻在共享目录写一个标记文件
  ``coord/pending/resp-<source_hash>-<worker>-<item>-<started>.json``；
  文件里带着响应结论的摘要（成功值 / 错误码 / 429 的收到时刻与 Retry-After）。
- 标记存在期间，任何进程（包括解锁后抢跑的另一个采集进程）在认领该源之前
  都会先看到它：响应尚未登记，这一轮不认领该源（只跳过，不阻塞，其它源照常
  推进），绝不能在 429 冷却登记之前发出同源新请求。
- 登记期间进程每次重试都会刷新标记时间戳（touch），因此 TTL 只会在
  “持有者真的不再接续（崩溃/被杀）”之后才会到期，不会被等锁时间误判。
- 标记同时保护该条目不被租约回收拿回 pending（避免别的进程重新取源）；
  只有确认无人接续后才解除保护。
- 登记成功后删除标记；进程崩溃则标记留到 TTL 后由其它进程回收。

标记很小，写入只做原子 rename，读取只读文件，全程不碰数据库，因此不会阻塞
API 的进度/结果查询，也不会因为数据库写锁而卡住。
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import time
from pathlib import Path

from . import config

_SUFFIX = ".resp.json"


def _pending_dir() -> Path:
    return Path(config.COORD_DIR) / "pending"


def _source_tag(source: str) -> str:
    return hashlib.sha1(source.encode("utf-8")).hexdigest()[:16]


def _record_path(source: str, worker_token: str, item_id: int, started_at: float) -> Path:
    token = "".join(c if c.isalnum() or c in "-_" else "_" for c in worker_token)
    return _pending_dir() / (
        f"resp-{_source_tag(source)}-{token}-{item_id}-{int(started_at * 1e6)}"
        f"{_SUFFIX}"
    )


def _write_atomic(path: Path, payload: dict) -> None:
    tmp = path.with_name(f".tmp-{os.getpid()}-{path.name}")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)  # 原子可见


# ----------------------------------------------------------------------- 写/删
def mark_unregistered(
    source: str,
    worker_token: str,
    item_id: int,
    started_at: float,
    kind: str,
    details: dict | None = None,
    recoverable_after: float | None = None,
) -> Path:
    """响应返回时立即写标记（同步小 IO，仅在微秒级，不影响请求路径）。"""
    _pending_dir().mkdir(parents=True, exist_ok=True)
    path = _record_path(source, worker_token, item_id, started_at)
    payload = {
        "source_base": source,
        "worker_token": worker_token,
        "item_id": item_id,
        "started_at": started_at,
        "kind": kind,
        "marked_at": time.time(),
        "pid": os.getpid(),
        "host": os.getenv("HOSTNAME", ""),
    }
    if recoverable_after is not None:
        payload["recoverable_after"] = recoverable_after
    if details:
        payload.update(details)
    _write_atomic(path, payload)
    return path


def touch(path: Path) -> None:
    """登记重试期间刷新活标志：标记只会在持有者停止刷新（崩溃）后过期。"""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            payload = json.load(fh)
    except (FileNotFoundError, OSError, ValueError):
        return
    payload["marked_at"] = time.time()
    with contextlib.suppress(FileNotFoundError, OSError):
        _write_atomic(path, payload)


def clear(path: Path) -> None:
    with contextlib.suppress(FileNotFoundError):
        os.unlink(path)


# ----------------------------------------------------------------------- 查询
def _records(source: str) -> list[tuple[Path, dict]]:
    tag = _source_tag(source)
    out: list[tuple[Path, dict]] = []
    try:
        names = os.listdir(_pending_dir())
    except FileNotFoundError:
        return out
    for name in names:
        if not (name.startswith(f"resp-{tag}-") and name.endswith(_SUFFIX)):
            continue
        path = _pending_dir() / name
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (FileNotFoundError, OSError, ValueError):
            continue  # 已被登记方删除或正在写入
        if data.get("source_base") == source:
            out.append((path, data))
    return out


def _is_live(data: dict, path: Path) -> bool:
    """标记是否仍代表“有人在接续、响应尚未落库”。

    持有者每次重试都会刷新 marked_at，因此存活进程的标记不会过期；只有进程
    被杀后停止刷新，才会在 TTL 后判定为无人接续。取 mtime 与 marked_at 的较大
    者，避免持有者本地时钟回拨导致标记提前失效。
    """
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return False
    age = time.time() - max(mtime, float(data.get("marked_at", 0)))
    return age < config.REGISTRATION_MARK_TTL_SECONDS


def source_is_registering(source: str) -> bool:
    """该源是否有响应正在等待登记（此时不得发起同源新请求）。

    顺带回收已过期（确认无人接续）的标记，避免目录无限增长。
    """
    live = False
    for path, data in _records(source):
        if _is_live(data, path):
            live = True
        else:
            clear(path)
    return live


def pending_marks(source: str) -> list[dict]:
    """该源上仍在等待登记的响应摘要（供 API 展示，只读文件）。"""
    return [dict(data) for path, data in _records(source) if _is_live(data, path)]


def active_item_ids() -> set[int]:
    """所有仍在等待登记的条目 id（租约回收时必须放过这些条目）。"""
    ids: set[int] = set()
    try:
        names = os.listdir(_pending_dir())
    except FileNotFoundError:
        return ids
    for name in names:
        if not (name.startswith("resp-") and name.endswith(_SUFFIX)):
            continue
        path = _pending_dir() / name
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (FileNotFoundError, OSError, ValueError):
            continue
        if _is_live(data, path):
            try:
                ids.add(int(data["item_id"]))
            except (KeyError, TypeError, ValueError):
                continue
    return ids


def clear_worker_marks(worker_token: str) -> int:
    """进程退出时清理自己留下的标记（响应不再接续，条目由退出流程回 pending）。"""
    removed = 0
    try:
        names = os.listdir(_pending_dir())
    except FileNotFoundError:
        return 0
    for name in names:
        if not (name.startswith("resp-") and name.endswith(_SUFFIX)):
            continue
        path = _pending_dir() / name
        try:
            with open(path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
        except (FileNotFoundError, OSError, ValueError):
            continue
        if data.get("worker_token") == worker_token:
            clear(path)
            removed += 1
    return removed
