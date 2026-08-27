#!/bin/bash
# run_all.sh — 一键起全部 6 进程:5 个能力/中继节点后台 + 助手前台。
# 退出时 trap 清后台。
# 也可各终端分别跑:python -m vouch_app.node <Name>
set -e
cd "$(dirname "$0")/.."   # 到 vouch/ 根(让 -m vouch_app 可 import)

echo "启动能力/中继节点(后台)..."
python3 -m vouch_app.node Broker      &
python3 -m vouch_app.node Translator  &
python3 -m vouch_app.node Drafter     &
python3 -m vouch_app.node Accountant  &
python3 -m vouch_app.node TextTooler  &

# 退出时杀掉所有后台子进程(进程组)
trap 'kill 0' EXIT INT TERM

# 给后台节点起服务留时间
sleep 0.6

echo "启动助手(前台 REPL)..."
python3 -m vouch_app.node You
