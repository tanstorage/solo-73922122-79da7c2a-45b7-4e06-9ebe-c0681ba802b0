#!/usr/bin/env bash
# 任务交接演示（慢删除跨租约 + 迟到报错不得覆盖接管者）。
#
# 前置：docker compose --profile handover up --build
# 拓扑：
#   handover-a (8011)：P0001 慢删除 10s（租约仅 3s）且注入删除失败，
#                      复现“旧任务租约到期后迟到报错”的原始缺陷场景；
#   handover-b (8012)：干净进程，每轮只认领 1 个分区，租约到期即接管。
# 迟到“成功”被围栏挡回的场景由 tests/handover_test.py 的 A1 用例覆盖。
#
# 用法：./scripts/demo_handover.sh [A_BASE] [B_BASE]
set -euo pipefail

A="${1:-http://localhost:8011}"
B="${2:-http://localhost:8012}"
PP='python3 -m json.tool'

step() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

step "0. 健康检查与故障注入配置（handover-a 慢删除+失败注入，handover-b 干净）"
curl -s "$A/meta/info" | $PP
curl -s "$B/meta/info" | $PP

step "1. 初始状态：P0001 为 active 且文件在盘"
curl -s "$A/partitions" | python3 -c "
import json, sys
p = next(x for x in json.load(sys.stdin) if x['partition_id'] == 'P0001')
print(p['partition_id'], p['status'], 'file_exists=', p['file_exists'])"

step "2. 在 handover-a 触发清理，2 秒后主动断开（模拟客户端取消等待；工作线程继续删）"
# --max-time 2 让 curl 断开连接，但服务端删除工作不中断
curl -s --max-time 2 -X POST "$A/cleanup/run" || true
echo "(curl 已断开；handover-a 的删除工作仍在后台执行)"

step "3. handover-a 视角：P0001 已认领为 deleting 并进入慢删除，in_progress=true"
sleep 1
curl -s "$A/cleanup/status" | python3 -c "
import json, sys
st = json.load(sys.stdin)
print('in_progress =', st['in_progress'])
print('in_flight   =', [(x['partition_id'], x['owner'], x['lease_expires_at']) for x in st['in_flight']])"

step "4. 等待租约（3s）到期，由 handover-b 高频接管（每轮只认领 1 个分区）"
sleep 4
for i in $(seq 1 10); do
  curl -s -X POST "$B/cleanup/run" > /tmp/handover-b-run.json || true
  st=$(curl -s "$B/partitions" | python3 -c "
import json, sys
print(next(p['status'] for p in json.load(sys.stdin) if p['partition_id'] == 'P0001'))")
  echo "  handover-b 第 ${i} 次接管后：P0001 = $st"
  [ "$st" = "cleaned" ] && break
  sleep 0.8
done
echo "handover-b 接管轮次的响应："
cat /tmp/handover-b-run.json | $PP

step "5. 等 handover-a 的慢删除（10s）迟到落库：失败应被围栏挡回（superseded）"
echo "  轮询 handover-a 的 last_run.outcomes ..."
for i in $(seq 1 20); do
  n=$(curl -s "$A/cleanup/status" | python3 -c "
import json, sys
lr = json.load(sys.stdin).get('last_run') or {}
outs = {o['partition_id']: o['result'] for o in lr.get('outcomes', [])}
print(1 if outs.get('P0001') == 'superseded' else 0)")
  [ "$n" = "1" ] && break
  sleep 1
done
curl -s "$A/cleanup/status" | python3 -c "
import json, sys
st = json.load(sys.stdin)
print('in_progress =', st['in_progress'])
print('last_run    =', json.dumps(st['last_run'], ensure_ascii=False, indent=2))"

step "6. 终态校验：P0001 保持 cleaned（迟到报错未把状态改回 delete_failed）"
curl -s "$B/partitions" | python3 -c "
import json, sys
p = next(x for x in json.load(sys.stdin) if x['partition_id'] == 'P0001')
print('P0001 status=', p['status'], 'file_exists=', p['file_exists'], 'last_error=', p['last_error'])
assert p['status'] == 'cleaned' and not p['file_exists'] and p['last_error'] is None
print('OK: 接管者的 cleaned 终态、租约与错误均未被旧任务覆盖')"

echo
echo "演示完成。要点："
echo "  - handover-a 的慢删除跨越 3s 租约，handover-b 到期接管（attempts+1，新围栏）；"
echo "  - handover-a 迟到的失败落库 rowcount=0，响应记 superseded，不计本轮 cleaned/failed，"
echo "    cleaned 终态不被回退（原始缺陷：旧代码会把它改回 delete_failed）；"
echo "  - 全程删除期间不持数据库写锁，其他分区可正常保全/查询。"
