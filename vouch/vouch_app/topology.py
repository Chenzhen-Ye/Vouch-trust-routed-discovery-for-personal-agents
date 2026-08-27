"""topology.py — 应用层网络拓扑配置。

6 节点,6 端口(8001-8006),真联网多进程。每进程只建自己的 Agent + 熟人表。

节点角色:
  You        助手(无能力,跑 REPL)
  Translator 翻译(translate)
  Broker     中继(无能力,只转发——演示多跳)
  Drafter    起草(draft)
  Accountant 算账(calc)
  TextTooler 文本工具(text)

边:You 直连 Translator + Broker;Broker 连 Drafter/Accountant/TextTooler。
Broker 的 tags 覆盖它能引荐到的所有能力,让 guided pick 选中它作中继。
"""
from __future__ import annotations

HOST = "127.0.0.1"

# name -> (port, caps, cap_key)。cap_key 指向 capabilities.CAPABILITIES;None=无能力节点。
NODES: dict = {
    "You":        {"port": 8001, "caps": [],                                   "cap_key": None},
    "Translator": {"port": 8002, "caps": ["translate", "translation"],          "cap_key": "translate"},
    "Broker":     {"port": 8003, "caps": [],                                   "cap_key": None},
    "Drafter":    {"port": 8004, "caps": ["draft", "writing"],                   "cap_key": "draft"},
    "Accountant": {"port": 8005, "caps": ["calc", "finance"],                   "cap_key": "calc"},
    "TextTooler": {"port": 8006, "caps": ["text", "textools"],                  "cap_key": "text"},
}

# 有向边:(from, to, tags, trust)。to 的 tags 是 from 对它的认知(Broker 的认知覆盖下游能力)。
EDGES = [
    ("You", "Translator", ["translate", "translation"], 0.8),
    ("You", "Broker",     ["draft", "writing", "calc", "finance", "text", "textools"], 0.7),
    ("Broker", "Drafter",    ["draft", "writing"],  0.8),
    ("Broker", "Accountant", ["calc", "finance"],   0.8),
    ("Broker", "TextTooler", ["text", "textools"],  0.8),
]
