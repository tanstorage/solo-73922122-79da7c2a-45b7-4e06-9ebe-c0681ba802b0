#!/usr/bin/env bash
# 演示脚本：对 docker compose 启动的两个 API 进程执行
# 保全 -> 清理 -> 波形查询 的完整流程。
# 用法：./scripts/demo.sh [BASE_A] [BASE_B]
set -euo pipefail

A="${1:-http://localhost:8001}"
B="${2:-http://localhost:8002}"
PP='python3 -m json.tool'

step() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

step "0. 两个进程健康检查（api-a=$A, api-b=$B）"
curl -s "$A/health" | $PP
curl -s "$B/health" | $PP

step "1. 查看全部分区（8 个种子分区，部分已到期）"
curl -s "$A/partitions" | python3 -c "
import json, sys
for p in json.load(sys.stdin):
    print(f\"{p['partition_id']} {p['vehicle_id']} [{p['start']} .. {p['end']}] expires={p['expires_at']} {'EXPIRED' if p['expired'] else 'valid  '} {p['status']}\")"

step "2. 波形查询：T203 故障冲击段 [00:30:50, 00:31:10]（注意 00:31 起幅值突变）"
curl -s "$B/samples?vehicle_id=T203&start=2026-10-03T00:30:50Z&end=2026-10-03T00:31:10Z" | $PP

step "3. 调查人员保全 T203 [00:30, 00:35]（闭区间，覆盖含故障段的 P0006）"
PRES=$(curl -s -X POST "$A/preservations" -H 'Content-Type: application/json' -d '{
  "vehicle_id": "T203",
  "start": "2026-10-03T00:30:00Z",
  "end": "2026-10-03T00:35:00Z",
  "investigator": "inspector-01",
  "reason": "转向架故障调查：保全冲击段波形"
}')
echo "$PRES" | $PP
PRS_ID=$(echo "$PRES" | python3 -c "import json,sys; print(json.load(sys.stdin)['preservation']['preservation_id'])")
echo "保全编号：$PRS_ID"

step "4. 在另一个进程上触发清理：到期且无保全的分区被回收，P0006 因保全存活"
curl -s -X POST "$B/cleanup/run" | $PP

step "5. 清理状态总览（P0006 仍为 active，active_preservations=1）"
curl -s "$A/cleanup/status" | $PP
curl -s "$A/partitions?vehicle_id=T203" | $PP

step "6. 对已清理分区（T101 P0001 时段）申请保全 -> 整次拒绝 409"
curl -s -w '\nHTTP %{http_code}\n' -X POST "$A/preservations" -H 'Content-Type: application/json' -d '{
  "vehicle_id": "T101", "start": "2026-10-01T00:10:00Z", "end": "2026-10-01T00:20:00Z",
  "investigator": "inspector-02", "reason": "为时已晚的保全"
}'

step "7. 保全期间波形仍可查询"
curl -s "$B/samples?vehicle_id=T203&start=2026-10-03T00:31:00Z&end=2026-10-03T00:31:20Z" | $PP

step "8. 解除保全 -> 再次清理 -> P0006 被回收"
curl -s -X POST "$A/preservations/$PRS_ID/release" | $PP
curl -s -X POST "$B/cleanup/run" | $PP

step "9. 回收后波形查询为空；清理状态确认"
curl -s "$A/samples?vehicle_id=T203&start=2026-10-03T00:30:00Z&end=2026-10-03T00:35:00Z" | $PP
curl -s "$B/cleanup/status" | $PP

step "10. 两个进程看到一致状态（api-b 视角的分区清单）"
curl -s "$B/partitions" | python3 -c "
import json, sys
for p in json.load(sys.stdin):
    print(f\"{p['partition_id']} {p['vehicle_id']} {p['status']} file_exists={p['file_exists']}\")"

echo
echo "演示完成。可重启任一容器（docker compose restart api-a）验证：已清理分区不会被重新加入。"
