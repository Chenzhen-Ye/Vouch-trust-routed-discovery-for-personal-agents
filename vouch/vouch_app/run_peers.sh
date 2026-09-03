#!/bin/bash
# run_peers.sh — 一键起 3 个对等 peer 自测(alice/bob/carol,链式拓扑)。
#
# alice(9001, translate:llm) ↔ bob(9002, draft:claude) ↔ carol(9003, calc+text:rule)
# alice 找 calc 需经 bob 两跳——演示多跳发现 + 介绍人担保。
#
# 退出时 trap 清后台。也可各终端分别跑:
#   python3 -m vouch_app.peer --profile demo_a
set -e
cd "$(dirname "$0")/.."   # 到 vouch/ 根(让 -m vouch_app 可 import)

# 清理上次自测的公钥交换目录(模拟信道重置)
rm -rf /tmp/vouch_pub

echo "启动 bob / carol(后台服务)..."
# 顺序起:carol 先(无人依赖),bob 后(等 carol 公钥);各自启动时都会把公钥
# 写进 /tmp/vouch_pub——互相引荐的节点启动有先后,公钥读取有 5s 重试兜底。
python3 -u -m vouch_app.peer --profile demo_c --serve &
sleep 0.6
python3 -u -m vouch_app.peer --profile demo_b --serve &

# 退出时杀掉所有后台子进程(进程组)
trap 'kill 0' EXIT INT TERM

# 给后台 peer 起服务留时间(它们要写公钥到 pub_dir)
sleep 0.8

echo "启动 alice(前台 REPL)..."
python3 -u -m vouch_app.peer --profile demo_a
