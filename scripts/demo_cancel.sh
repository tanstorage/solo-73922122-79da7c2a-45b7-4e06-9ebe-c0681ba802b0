#!/usr/bin/env bash
# 取消等待演示：取消 run_cleanup_once 的等待只放弃 HTTP 响应，删除线程继续跑；
# 同进程下一轮必须等实际工作结束才放行，重复取消也不能提前放行。
#
# 前置：docker compose --profile cancel-demo up --build
# 拓扑：单服务 cancel-demo (8021)，P0001 慢删除 10s，后台循环关闭（全手动）。
#
# 用法：./scripts/demo_cancel.sh [BASE]
set -euo pipefail

BASE="${1:-http://localhost:8021}"

step() { printf '\n\033[1m== %s ==\033[0m\n' "$1"; }

poll_status() {
  curl -s "$BASE/cleanup/status" | python3 -c "
import json, sys
st = json.load(sys.stdin)
ip = st['in_progress']
print(f\"in_progress={ip['in_progress']} reason={ip['reason']} started_at={ip['started_at']}\")"
}

step "0. 健康检查与慢删除配置"
curl -s "$BASE/meta/info" | python3 -m json.tool

step "1. 第一轮清理 P0001（慢删除 10s），curl 2s 后断开 = 取消等待"
curl -s --max-time 2 -X POST "$BASE/cleanup/run" || true
echo "(连接已断开；服务端工作线程不受影响)"

step "2. 立刻再发第二轮并重复取消两次：均只能排队，不会并发开跑"
curl -s --max-time 1 -X POST "$BASE/cleanup/run" || true
curl -s --max-time 1 -X POST "$BASE/cleanup/run" || true
echo "(两次排队请求都已被客户端取消，闸门仍未放行)"

step "3. 轮询状态：整个慢删除窗口内 in_progress 始终为 true"
for i in $(seq 1 10); do
  printf '  t+%02ds  ' "$i"
  poll_status
  sleep 1
done

step "4. 实际工作结束后闸门放行：新一轮可以立即执行"
curl -s -X POST "$BASE/cleanup/run" | python3 -m json.tool
poll_status

step "5. 终态：P0001 已回收，文件不在盘上"
curl -s "$BASE/partitions" | python3 -c "
import json, sys
p = next(x for x in json.load(sys.stdin) if x['partition_id'] == 'P0001')
print('P0001 status=', p['status'], 'file_exists=', p['file_exists'])"

echo
echo "演示完成。要点：取消等待 ≠ 取消工作；闸门以实际工作结束为放行条件，"
echo "重复取消同样不能提前放行；迟到结束的结果仍写入 last_cleanup_run。"
