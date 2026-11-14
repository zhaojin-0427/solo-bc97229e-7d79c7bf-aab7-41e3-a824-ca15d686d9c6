#!/usr/bin/env bash
# 端到端演示脚本（容器内/本机均可运行，要求 curl 可用）。
# 用法: BASE=http://localhost:8080 SRC=http://localhost:8081 bash scripts/demo.sh
set -euo pipefail

BASE="${BASE:-http://localhost:8080}"
SRC="${SRC:-http://localhost:8081}"
# 任务中写入库的源基址：容器内用 compose 服务名，本机直连用 SRC。
JOB_SOURCE="${JOB_SOURCE:-http://source:8081}"

echo "== 1. 气象源切换为每 3 次请求返回一次 429（Retry-After=3 秒） =="
curl -s -X POST "$SRC/control/policy" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"nth429","n":3,"retry_after":3}' | head -c 300; echo

echo "== 2. 提交 12 组站日（同源，最多 20 组） =="
ITEMS=$(JOB_SOURCE="$JOB_SOURCE" python3 - <<'PY'
import json, os
print(json.dumps({
    "source_base": os.environ["JOB_SOURCE"],
    "items": [
        {"station_id": f"S{1000 + i % 4}", "date": f"2026-11-{(i % 9) + 10:02d}"}
        for i in range(12)
    ],
}))
PY
)
JOB=$(curl -s -X POST "$BASE/jobs" -H 'Content-Type: application/json' -d "$ITEMS")
echo "$JOB"
JOB_ID=$(printf '%s' "$JOB" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')

echo "== 3. 轮询进度 =="
for _ in $(seq 1 30); do
  P=$(curl -s "$BASE/jobs/$JOB_ID/progress")
  echo "$P"
  if printf '%s' "$P" | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d["done"] else 1)'; then
    break
  fi
  sleep 1
done

echo "== 4. 站日降水结果 =="
curl -s "$BASE/jobs/$JOB_ID/results" | python3 -m json.tool

echo "== 5. 失败项（本场景应为空，429 均自动重试成功） =="
curl -s "$BASE/jobs/$JOB_ID/errors" | python3 -m json.tool

echo "== 6. 源限速状态（冷却/在途/worker 心跳） =="
curl -s "$BASE/sources" | python3 -m json.tool

echo "== 7. 气象源请求时刻核对：最小相邻间隔与在途峰值 =="
curl -s "$SRC/control/stats" | python3 -m json.tool

echo "== 8. 响应登记遇写锁：保留响应、自动接续、冷却不丢 =="
echo "-- 源对下 2 个请求返回 429（Retry-After=4s）"
curl -s -X POST "$SRC/control/policy" \
  -H 'Content-Type: application/json' \
  -d '{"mode":"nth429","n":1,"retry_after":4}' >/dev/null
ITEMS2=$(JOB_SOURCE="$JOB_SOURCE" python3 - <<'PY'
import json, os
print(json.dumps({
    "source_base": os.environ["JOB_SOURCE"],
    "items": [{"station_id": "S2001", "date": "2026-11-10"}],
}))
PY
)
JOB2=$(curl -s -X POST "$BASE/jobs" -H 'Content-Type: application/json' -d "$ITEMS2")
JOB2_ID=$(printf '%s' "$JOB2" | python3 -c 'import json,sys; print(json.load(sys.stdin)["job_id"])')
sleep 2  # 让 worker 发出请求并收到 429
echo "-- 另一个连接持写锁 8 秒（模拟登记时撞锁）"
python -m app.holdlock --seconds 8 &
HOLD_PID=$!
sleep 1
echo "-- 持锁期间进度查询仍立即返回："
time curl -s "$BASE/jobs/$JOB2_ID/progress"; echo
curl -s "$BASE/sources" | python3 -c '
import json, sys
fields = ("source_base", "cooldown_until", "cooldown_remaining", "inflight", "pending_registrations")
print(json.dumps([{k: s[k] for k in fields} for s in json.load(sys.stdin)], indent=2, ensure_ascii=False))
'
wait "$HOLD_PID"
echo "-- 解锁后冷却按收到响应时刻写入（不会因登记延迟而后移）"
sleep 1
curl -s "$BASE/sources" | python3 -m json.tool
echo "-- 冷却结束后自动重采完成"
for _ in $(seq 1 30); do
  P=$(curl -s "$BASE/jobs/$JOB2_ID/progress")
  echo "$P"
  if printf '%s' "$P" | python3 -c 'import json,sys; d=json.load(sys.stdin); sys.exit(0 if d["done"] else 1)'; then
    break
  fi
  sleep 1
done
curl -s "$BASE/jobs/$JOB2_ID/items" | python3 -m json.tool
echo "-- 请求记录（429 后没有在冷却登记前抢发的新请求）"
curl -s "$SRC/control/requests?limit=20" | python3 -m json.tool

echo "== 9. 恢复源策略 =="
curl -s -X POST "$SRC/control/policy" -H 'Content-Type: application/json' -d '{"mode":"normal"}' >/dev/null
echo done
