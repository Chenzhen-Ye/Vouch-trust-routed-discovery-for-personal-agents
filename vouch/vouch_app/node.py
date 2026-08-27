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


def make_quality_fn(cap_key, llm_cfg=None):
    """能力 agent 的任务执行器。双轨:按配置选 LLM 或规则式实现。

    LLM 双轨(§12.5):
      · translate/draft/text:配了 LLM 且该 cap 在 llm_caps → 走 LLM
      · calc:始终规则式(精确计算 LLM 不可靠)
      · LLM 调用失败 → catch → 降级规则式 + 质量 0.7(降级不算满质量)
      · 规则式成功 → 1.0;规则式失败 → 0.1
    """
    if cap_key is None:
        return None  # You/Broker 无能力,默认 quality_fn(不会被 ask 到)
    rule_fn = CAPABILITIES[cap_key][0]
    use_llm = llm_cfg is not None and llm_cfg.use_llm(cap_key)

    def wrapped(task):
        if use_llm:
            try:
                from .prompts import prompt_for
                from .llm_executor import chat
                system, user = prompt_for(cap_key, task)
                result = chat(llm_cfg, system, user)
                return (result, 1.0)   # LLM 成功 → 满质量
            except Exception as e:
                # LLM 失败(网络/超时/模型不存在)→ 降级规则式
                try:
                    rule_result = rule_fn(task)
                    return (f"{rule_result}\n[LLM 不可用,已降级规则式: {type(e).__name__}]",
                            0.7)   # 降级不算满质量
                except Exception as e2:
                    return (f"[{cap_key} 规则式也失败] {e2}", 0.1)
        try:
            return (rule_fn(task), 1.0)
        except Exception as e:
            return (f"[{cap_key} 执行失败] {e}", 0.1)

    return wrapped


def build_local_agent(name: str, network: Network, llm_cfg=None) -> Agent:
    """在本进程建指定节点:只建自己 + 自己的熟人(指向别的进程端口)。"""
    spec = topo.NODES[name]
    me = Agent(name, spec["port"], spec["caps"], network=network,
               quality_fn=make_quality_fn(spec["cap_key"], llm_cfg))
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
    from .llm_config import LLMConfig
    llm_cfg = LLMConfig.from_env()
    # LLM 慢调用(秒级)vs 任务传输超时(默认 2s)。LLM 启用时,源端等响应要放大
    # send_timeout(LLM 超时 + 余量),否则 LLM 还没回,源端已判 churn 超时。
    send_timeout = (llm_cfg.timeout + 5) if llm_cfg.enabled else 2.0
    cfg = Config(guided_fanout=2, send_timeout=send_timeout) if name == "You" else Config()
    net = Network(config=cfg)
    llm_cfg = LLMConfig.from_env()
    me = build_local_agent(name, net, llm_cfg=llm_cfg)
    server = await me.serve()
    print(f"[{name}] 已上线 @{me.port}", end="")
    if me.caps:
        llm_mark = " [LLM]" if llm_cfg.use_llm(topo.NODES[name]["cap_key"]) else " [规则]"
        print(f" 能力={sorted(me.caps)}{llm_mark}")
    else:
        print(" (无能力,纯中继/助手)")
    if me.caps and llm_cfg.use_llm(topo.NODES[name]["cap_key"]):
        print(f"  LLM: {llm_cfg.describe()}")
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
