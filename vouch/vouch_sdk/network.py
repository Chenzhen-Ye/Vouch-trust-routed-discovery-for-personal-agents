"""network.py — 把原 vouch.py 的四个模块级可变全局封装成一个 Network 对象。

这是 SDK 化的核心：旧原型的 REGISTRY/_DOWN/_CLOCK/_COUNT 是模块级可变单例，
导致「import 即有状态、同进程跑不了两个网络」。Network 类持有它们后，
一个 Network = 一个独立的熟人叠加网络；多个 Network 同进程互不串。

Agent 不再自己往全局注册——改由 Network.register(agent) 显式登记。
Network 同时是 build_graph（手填种子拓扑）的归宿。
"""
from __future__ import annotations
from .config import Config


class Network:
    """一个独立的 Vouch 熟人叠加网络。

    持有：节点注册表、下线集合、逻辑时钟、消息计数器、参数配置。
    多个 Network 同进程独立——这是全局化时代做不到的。
    """

    def __init__(self, config: Config | None = None):
        self.config = config or Config()
        self.registry: dict = {}       # name -> Agent
        self.down: set = set()         # 模拟「下线」的节点名集合
        self.clock = [0]               # 全局逻辑时钟（演示用；真实系统用墙钟时间）
        self.counts: dict = {}         # 消息计数（query/response/task/flood...）

    # ---- 节点注册 ----
    def register(self, agent):
        self.registry[agent.name] = agent

    def get(self, name):
        return self.registry.get(name)

    def all_agents(self):
        return list(self.registry.values())

    # ---- 下线模拟 ----
    def is_down(self, name) -> bool:
        return name in self.down

    def mark_down(self, name):
        self.down.add(name)

    def clear_down(self, name):
        self.down.discard(name)

    # ---- 时钟 / 计数 ----
    def bump(self, kind):
        self.counts[kind] = self.counts.get(kind, 0) + 1

    def tick(self) -> int:
        self.clock[0] += 1
        return self.clock[0]

    # ---- 拓扑重算 ----
    def set_degree_all(self):
        """重新计算所有 degree（只数强连接，抗 Sybil）。加边后调用。"""
        cfg = self.config
        for ag in self.registry.values():
            for name, acq in ag.acq.items():
                other = self.registry.get(name)
                if other:
                    acq.degree = sum(1 for x in other.acq.values()
                                     if x.trust >= cfg.route_trust_threshold)

    # ---- 手填种子拓扑（拓扑生命周期起点）----
    def build_graph(self, sparse=True):
        """sparse=True: 仅 3 条手填种子边（拓扑生命周期起点）。
        sparse=False: 完整 14 边小世界图（churn 演示用）。

        返回 agents 列表。所有 Agent 用本 network 注册。
        """
        # 延迟 import 避免循环（agent.py import network 会成环）
        from .agent import Agent

        specs = [
            ("Alice", 7001, ["python", "backend"]),
            ("Bob",   7002, ["python", "design"]),
            ("Carol", 7003, ["design", "art"]),
            ("Dave",  7004, ["law", "finance"]),
            ("Eve",   7005, ["law", "writing"]),
            ("Frank", 7006, ["art", "design"]),
            ("Grace", 7007, ["writing", "editing"]),
        ]

        def good(task):  return (f"{task}→成品@好", 0.9)
        def shaky(task): return (f"{task}→成品@一般", 0.5)
        def bad(task):   return (f"{task}→成品@差", 0.1)
        for n, p, c in specs:
            qf = shaky if n == "Dave" else (bad if n == "Eve" else good)
            Agent(n, p, c, network=self, quality_fn=qf)

        if sparse:
            self.registry["Alice"].knows("Bob", self.registry["Bob"].port, ["python", "design"], 0.7)
            self.registry["Bob"].knows("Alice", self.registry["Alice"].port, ["python"], 0.7)
            self.registry["Bob"].knows("Dave", self.registry["Dave"].port, ["law", "finance"], 0.6)
        else:
            edges = [
                ("Alice", "Bob",   ["python", "design"], 0.9),
                ("Alice", "Carol", ["design", "art"],    0.6),
                ("Bob",   "Alice", ["python"],           0.9),
                ("Bob",   "Carol", ["design"],            0.6),
                ("Bob",   "Dave",  ["law", "finance"],    0.7),
                ("Bob",   "Eve",   ["writing"],          0.5),
                ("Carol", "Bob",   ["design"],            0.6),
                ("Carol", "Frank", ["art", "design"],     0.7),
                ("Dave",  "Bob",   ["python", "design"],  0.7),
                ("Dave",  "Eve",   ["law", "writing"],    0.8),
                ("Eve",   "Dave",  ["law"],               0.8),
                ("Eve",   "Grace", ["writing", "editing"], 0.7),
                ("Frank", "Carol", ["art"],                0.7),
                ("Frank", "Grace", ["writing"],           0.6),
                ("Grace", "Eve",   ["writing"],           0.6),
            ]
            for frm, to, tags, trust in edges:
                self.registry[frm].knows(to, self.registry[to].port, tags, trust)
        # 初始信任都 < 阈值，演示「要攒信任才能路由」
        for ag in self.registry.values():
            for name, acq in ag.acq.items():
                acq.last_seen = self.tick()
                # 带外信任锚：我认识的熟人，我预先持有其公钥（介绍人担保验身份用）
                if name in self.registry:
                    acq.pub = self.registry[name].rsa_pub
        self.set_degree_all()
        return list(self.registry.values())
