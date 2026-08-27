"""vouch_app — 基于 vouch_sdk 的个人助手应用。

真联网多进程:每个节点一个进程,通过 TCP 真连。你在助手 REPL 输入任务,
助手经熟人链发现能力方 agent,把任务发去执行,返回真实结果。

入口:
  python -m vouch_app             # 默认起助手(You);需先起能力节点
  python -m vouch_app.node <Name> # 起指定节点
  bash vouch_app/run_all.sh       # 一键起全部 6 进程
"""
__version__ = "0.1.0"
