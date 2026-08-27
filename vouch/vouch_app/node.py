"""node.py — 通用节点入口。

每个进程跑一个节点:`python -m vouch_app.node <Name>`。
本进程只建自己的 Agent + 熟人表(其他节点在别的进程,通过 TCP 真连)。

设计要点:
  · 每进程独立 Network(SDK 化的收益:互不串台)
  · 熟人表的 degree 看不到全局 → 手设 1 占位(路由靠 sem+trust,hub 分量小)
  · 能力节点的 quality_fn 包装真实能力执行器(translate/draft/calc/text)
  · You 跑 REPL,其余节点常驻 serve
"""
from __future__ import annotations
import asyncio
import sys

from vouch_sdk import Network, Agent

from . import topology as topo
from .capabilities import CAPABILITIES
from .embedding_ext import install as _install_embedding

# 把应用层能力标签的语义向量合并进 SDK 的 EMBEDDING(幂等)。
# 让能力名(calc/text/translate/...)有语义,guided pick 能精准选中对的邻居,
# 而非因零向量退化成纯 trust 比较。
_install_embedding()


def make_quality_fn(cap_key):
    """能力 agent 的任务执行器:task 正文直接喂给能力函数。"""
    if cap_key is None:
        return None  # You/Broker 无能力,默认 quality_fn(不会被 ask 到)
    fn = CAPABILITIES[cap_key][0]

    def wrapped(task):
        try:
            result = fn(task)
            return (result, 1.0)   # 真实执行成功 → 满质量 → trust 升
        except Exception as e:
            return (f"[{cap_key} 执行失败] {e}", 0.1)  # 失败 → 低质量 → trust 降

    return wrapped


def build_local_agent(name: str, network: Network) -> Agent:
    """在本进程建指定节点:只建自己 + 自己的熟人(指向别的进程端口)。"""
    spec = topo.NODES[name]
    me = Agent(name, spec["port"], spec["caps"], network=network,
               quality_fn=make_quality_fn(spec["cap_key"]))
    # 加熟人(只加自己作为 from 的边)
    for frm, to, tags, trust in topo.EDGES:
        if frm == name:
            to_spec = topo.NODES[to]
            me.knows(to, to_spec["port"], tags, trust)
    # 本进程看不到别的 agent 实例 → degree 看不到全局 → 手设 1 占位。
    # 路由主要靠语义相似度 + trust(hub=0.3*degree/max_deg 权重小),不影响发现。
    for a in me.acq.values():
        a.degree = 1
    return me


async def run_node(name: str):
    if name not in topo.NODES:
        print(f"未知节点: {name}; 可选: {', '.join(topo.NODES)}")
        return
    # You 是助手,首跳 fanout=2:同时问 Translator(翻译直命中)和 Broker(其他能力中转),
    # 避免单跳选错邻居 → 超时 → 重试的 ~3秒延迟。其余节点默认 fanout=1。
    from vouch_sdk import Config
    cfg = Config(guided_fanout=2) if name == "You" else Config()
    net = Network(config=cfg)
    me = build_local_agent(name, net)
    server = await me.serve()
    print(f"[{name}] 已上线 @{me.port}", end="")
    if me.caps:
        print(f" 能力={sorted(me.caps)}")
    else:
        print(" (无能力,纯中继/助手)")
    try:
        if name == "You":
            from .assistant import repl
            await repl(me, server)
        else:
            # 能力节点常驻
            async with server:
                await asyncio.Event().wait()
    finally:
        server.close()
        await server.wait_closed()


def main():
    if len(sys.argv) < 2:
        print("用法: python -m vouch_app.node <Name>")
        print(f"节点: {', '.join(topo.NODES)}")
        print("或一键起全部: bash vouch_app/run_all.sh")
        return
    name = sys.argv[1]
    try:
        asyncio.run(run_node(name))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
