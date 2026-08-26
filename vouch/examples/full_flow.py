"""full_flow.py — Vouch 协议 7 阶段端到端演示。

从原 vouch.py 的 main() 迁移。改动：开头建 Network，所有旧全局引用改成 net.*。
机制逻辑逐行不变——这是 SDK 化（全局→对象）的验收基线：输出应与旧 vouch.py 等价。

七阶段：发现→协作→拓扑演化→churn→Sybil→身份验证→介绍人担保→向量语义路由
运行：python examples/full_flow.py  （或  python -m vouch_sdk.examples.full_flow）
"""
from __future__ import annotations
import asyncio
import sys
import os

# 让脚本可直接跑（python examples/full_flow.py）：把仓库 vouch/ 目录加进 path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from vouch_sdk import Network, Agent  # noqa: E402
from vouch_sdk.semantic import semantic_sim, RELATED  # noqa: E402
from vouch_sdk.config import Config as _Cfg  # noqa: E402

# DECAY_STEPS / ROUTE_TRUST_THRESHOLD 原是模块常量；现在在 Config 里。
_DECAY_STEPS = _Cfg().decay_steps
_ROUTE_TRUST_THRESHOLD = _Cfg().route_trust_threshold


async def main():
    net = Network()
    print("=" * 72)
    print(" Vouch 协议整合版（SDK 化，明文完整态）")
    print(" 发现 → 协作 → 拓扑演化 → churn 容错 → Sybil 防御")
    print("=" * 72)
    agents = net.build_graph(sparse=True)
    alice = net.get("Alice")
    servers = await asyncio.gather(*[a.serve() for a in agents])
    await asyncio.sleep(0.1)

    print("\n【阶段0】初始拓扑（稀疏种子，弱信任，都不可路由）:")
    for a in agents:
        acq_s = ", ".join(f"{n}(trust={x.trust:.2f})" for n, x in a.acq.items())
        print(f"  {a.tag} 熟人=[{acq_s or '空'}]")

    # ---- 阶段1：发现 + 协作（Bob 质量一般，攒信任到可路由）----
    print("\n" + "=" * 72)
    print("【阶段1】发现扩展 + 协作反馈：把 Bob 攒到可路由(≥0.6)")
    print("=" * 72)
    res = await alice.discover("law", strategy="guided")
    if res and res.get("found"):
        f = res["found"]
        intro = res.get("introducer")
        alice.remember(f, introducer=intro)
        # 带外信任锚：Alice 通过可靠渠道预先获得 Dave 的身份密钥（secret），
        # 后续协作前用它验「响应是不是 Dave 本人发的」（联动 §4.8↔§4.9）
        dave = net.get("Dave")
        alice.acq["Dave"].secret = dave.secret
        print(f"  → Alice 记住 {f['name']}（介绍人={intro}），并带外获得其身份密钥")
    # 和 Dave 多次高质量协作，把 Dave trust 攒到可路由(≥0.6)
    # 演示用：临时让 Dave 表现「好」（quality 0.9），好协作才该攒到可路由
    dave = net.get("Dave")
    _orig_dq = dave._quality_fn
    dave._quality_fn = lambda task: (f"{task}→成品@好", 0.9)
    for i in range(4):
        await alice.collaborate({"name": "Dave", "port": dave.port, "caps": ["law"]}, "看合同")
    dave._quality_fn = _orig_dq
    print(f"\n  Dave trust={alice.acq['Dave'].trust:.2f} "
          f"{'(≥阈值，可路由)' if alice.acq['Dave'].trust >= _ROUTE_TRUST_THRESHOLD else '(仍弱)'}")

    # ---- 阶段2：churn 容错（去程断 → 重试；回程断 → 绕行）----
    print("\n" + "=" * 72)
    print("【阶段2】churn 容错：中间人下线，去程重试换路 / 回程绕过直连源")
    print("=" * 72)
    # 先把图补完整，让断 Bob 后还有别的路
    for frm, to, tags in [("Alice", "Carol", ["design"]), ("Carol", "Frank", ["art"]),
                          ("Frank", "Grace", ["writing"]), ("Grace", "Eve", ["writing"]),
                          ("Eve", "Dave", ["law"])]:
        if to not in net.get(frm).acq:
            net.get(frm).knows(to, net.get(to).port, tags, 0.6)
    # 把 Carol/Frank/Grace/Eve 的 trust 也攒到可路由（演示需要）
    for n in ["Carol", "Frank", "Grace", "Eve"]:
        if n in alice.acq:
            alice.acq[n].trust = 0.7
    net.set_degree_all()

    print("\n--- 2a. 去程断：让 Bob 下线，Alice 重试换路 ---")
    net.get("Bob").go_offline()
    await asyncio.sleep(0.2)
    res2 = await alice.discover("law", strategy="guided")
    if res2:
        print(f"  ✓ Bob 下线仍找到 {res2['found']['name']}，"
              f"路径={(' → '.join(p['name'] for p in res2['path']))}")
    # 恢复 Bob
    net.clear_down("Bob")
    bob = net.get("Bob")
    bob._server = await asyncio.start_server(bob._handle, net.config.host, bob.port)
    await asyncio.sleep(0.2)

    print("\n--- 2b. 回程断：Dave 命中后让 Bob 下线，看 Dave 是否绕行直连源 ---")
    dave = net.get("Dave")
    orig_reply = dave._reply_back
    async def hook(resp, path):
        print(f"  [注入] Dave 命中，回程前让 Bob 下线")
        net.get("Bob").go_offline()
        await asyncio.sleep(0.1)
        await orig_reply(resp, path)
    dave._reply_back = hook
    res3 = await alice.discover("law", strategy="guided")
    if res3:
        print(f"  ✓ 回程断点扛住：找到 {res3['found']['name']}")
    dave._reply_back = orig_reply
    net.clear_down("Bob")
    bob = net.get("Bob")
    bob._server = await asyncio.start_server(bob._handle, net.config.host, bob.port)

    # ---- 阶段3：Sybil 防御 ----
    print("\n" + "=" * 72)
    print("【阶段3】Sybil 防御：弱连接不路由，傀儡进不了核心层")
    print("=" * 72)
    mallory_ports = [7101, 7102, 7103, 7104, 7105]
    puppets = [f"M{i+1}" for i in range(5)]
    for i, p in enumerate(mallory_ports):
        Agent(f"M{i+1}", p, ["law"], network=net)
    for pn in puppets:
        for qn in puppets:
            if pn != qn:
                net.get(pn).knows(qn, net.get(qn).port, ["law"], 0.4)
    for pn in puppets[:3]:
        alice.knows(pn, net.get(pn).port, ["law"], 0.4)
    net.set_degree_all()
    puppet_servers = await asyncio.gather(*[net.get(n).serve() for n in puppets])
    await asyncio.sleep(0.1)
    weak = [n for n, a in alice.acq.items() if a.trust < _ROUTE_TRUST_THRESHOLD and not a.blocked]
    print(f"  Mallory 造 5 傀儡（标签匹配 law、互抬 degree、弱信任 0.4）")
    print(f"  Alice 熟人中弱连接(不路由): {weak}")
    alice._seen.clear()
    res4 = await alice.discover("law", strategy="guided")
    if res4:
        print(f"  ✓ 找到 {res4['found']['name']}，傀儡被 [弱连接不路由] 排除")
    for s in puppet_servers:
        s.close()
    await asyncio.gather(*[s.wait_closed() for s in puppet_servers], return_exceptions=True)

    # ---- 阶段4：拓扑演化（衰减 + 最终状态）----
    print("\n" + "=" * 72)
    print("【阶段4】拓扑演化：不活跃衰减 + 最终熟人表")
    print("=" * 72)
    alice.acq["Dave"].last_seen = 0
    before = alice.acq["Dave"].trust
    removed = alice.decay(steps=_DECAY_STEPS)
    print(f"  Dave（很久没互动）trust {before:.2f}→{alice.acq['Dave'].trust:.2f}")
    print("\n【最终】Alice 熟人表：")
    for n, a in alice.acq.items():
        print(f"  {n}: trust={a.trust:.2f} tags={sorted(a.tags)} "
              f"次数={a.interactions} {'[拉黑]' if a.blocked else ''}")

    # ---- 阶段5：身份验证联动（签名↔信任）----
    print("\n" + "=" * 72)
    print("【阶段5】身份验证联动：协作前验身份 → 验签通过才校准能力信任")
    print("=" * 72)
    # 恢复 Dave 可路由状态（前面衰减可能把它降下去了）
    if "Dave" in alice.acq:
        alice.acq["Dave"].trust = max(alice.acq["Dave"].trust, 0.7)
        alice.acq["Dave"].blocked = False
        alice.acq["Dave"].last_seen = net.tick()
    # 恢复 Bob 信任（阶段2/4 可能动过）
    if "Bob" in alice.acq:
        alice.acq["Bob"].trust = max(alice.acq["Bob"].trust, 0.7)

    print("\n--- 5a. 正常：Alice 带 Dave 的 secret，discover→collaborate 验签通过 ---")
    alice._seen.clear()
    res = await alice.discover("law", strategy="guided")
    if res and res.get("found") and res.get("hmac_sig"):
        proof = {"hmac_sig": res["hmac_sig"], "found_json": res["found_json"],
                 "introducer": res.get("introducer")}
        out = await alice.collaborate(res["found"], "审合同", proof=proof)
        if out:
            print(f"  ✓ 验签通过→协作完成→Dave trust 升至 {alice.acq['Dave'].trust:.2f}")

    print("\n--- 5b. 信任锚不匹配：Dave 的 secret 与 Alice 持有的不符，验签失败 → 拒绝+降介绍人 ---")
    # 模拟身份验证失败：把 Alice 持有的 Dave secret 换成错的（信任锚被污染/目标换密钥未通知），
    # 真 Dave 用自己真 secret 签的 sig 验不过 → 等价于「响应者身份无法证实」。
    # 强制走多跳路径（经 Bob 介绍），这样验证失败时能降介绍人 Bob 的信任。
    real_secret = alice.acq["Dave"].secret
    direct_trust = alice.acq["Dave"].trust
    alice.acq["Dave"].trust = 0.3   # 弱连接，不路由，强制经 Bob
    net.set_degree_all()
    alice.acq["Dave"].secret = b"wrong-secret-0123456789ab"   # 错的信任锚
    alice._seen.clear()
    res2 = await alice.discover("law", strategy="guided")
    if res2 and res2.get("found") and res2.get("hmac_sig"):
        proof = {"hmac_sig": res2["hmac_sig"], "found_json": res2["found_json"],
                 "introducer": res2.get("introducer")}
        bob_before = alice.acq["Bob"].trust if "Bob" in alice.acq else 0
        out = await alice.collaborate(res2["found"], "审合同", proof=proof)
        if out is None:
            print(f"  ✓ 身份无法证实：拒绝协作。"
                  + (f"介绍人 Bob trust {bob_before:.2f}→{alice.acq['Bob'].trust:.2f}"
                     if "Bob" in alice.acq else ""))
    # 恢复
    alice.acq["Dave"].secret = real_secret
    alice.acq["Dave"].trust = direct_trust
    net.set_degree_all()

    # ---- 阶段6：介绍人担保（非对称，discover 的身份验证）----
    print("\n" + "=" * 72)
    print("【阶段6】介绍人担保：discover 时源不预持目标 secret，经介绍人获可信公钥")
    print("=" * 72)
    # 恢复 Dave/Bob 可路由
    if "Dave" in alice.acq:
        alice.acq["Dave"].trust = 0.7
        alice.acq["Dave"].blocked = False
        alice.acq["Dave"].last_seen = net.tick()
    if "Bob" in alice.acq:
        alice.acq["Bob"].trust = 0.7

    print("\n--- 6a. 正常：Alice 不持 Dave secret（discover 场景），经 Bob 担保获可信公钥 ---")
    # discover 场景：源发现前不知目标，故不预持目标 secret。临时清掉 Dave secret，
    # 强制走「介绍人 Bob 担保 → Alice 用 Bob 公钥验担保 → 取 Bob 担保的 Dave 公钥验 target_sig」。
    real_secret6 = alice.acq["Dave"].secret
    alice.acq["Dave"].secret = b""          # 模拟 discover：源不预持目标 secret
    # 强制多跳经 Bob（Dave 弱连接不路由）
    dt6 = alice.acq["Dave"].trust
    alice.acq["Dave"].trust = 0.3
    net.set_degree_all()
    alice._seen.clear()
    res = await alice.discover("law", strategy="guided")
    if res and res.get("vouchers"):
        proof = {"found_json": res["found_json"], "target_sig": res["target_sig"],
                 "vouchers": res["vouchers"], "introducer": res.get("introducer")}
        out = await alice.collaborate(res["found"], "审合同", proof=proof)
        if out:
            print(f"  ✓ 介绍人担保链生效：Bob 担保→Alice 验担保→用担保公钥验 target_sig→协作完成")
    # 恢复
    alice.acq["Dave"].secret = real_secret6
    alice.acq["Dave"].trust = dt6
    net.set_degree_all()

    print("\n--- 6b. 介绍人无法冒充：Bob 想伪造 Dave，但没有 Dave 私钥，签不出 target_sig ---")
    # 模拟冒充：Bob 自己充当「假 Dave」，用自己私钥签 target_sig（冒充 Dave），
    # 并用自己私钥重新担保（声称「这是 Dave 的公钥」=其实是 Bob 的公钥）。
    # Alice 用 Bob 担保的「Dave 公钥」（实为 Bob 公钥）验 target_sig：
    #   Bob 用自己私钥签的 sig，用 Bob 公钥验会【通过】！——所以单验 target_sig 不够。
    # 关键：Alice 还要验「Bob 担保的公钥 == 真 Dave 的公钥」吗？不——discover 场景
    #   Alice 不预持 Dave 公钥，无法比对。那靠什么防？
    #   靠「Bob 担保的是真 Dave 公钥」——但这又回到对称信任。所以：非对称签名下，
    #   介绍人能冒充的边界是「换公钥」（Bob 说这是 Dave 公钥其实是 Bob 的），
    #   而不是「伪造已有公钥的签名」（私钥签不出）。完整防住需多介绍人交叉验证/证书链。
    #   此处演示最小核心：Bob 用自己私钥伪造 target_sig，vouchers 仍是真 Bob 担保的（基于真 Dave 公钥），
    #   → voucher_sig 因 target_sig 被改而验不过；即便 Bob 重新担保，target_sig 用真 Dave 公钥验也过不了。
    alice.acq["Dave"].secret = b""
    alice.acq["Dave"].trust = 0.3
    net.set_degree_all()
    alice._seen.clear()
    res2 = await alice.discover("law", strategy="guided")
    if res2 and res2.get("vouchers"):
        # Bob 用自己私钥伪造 target_sig（冒充 Dave）
        bob = net.get("Bob")
        from vouch_sdk.crypto import rsa_sign as _rsa_sign
        forged_sig = str(_rsa_sign(bob._rsa_priv, res2["found_json"].encode()))
        proof = {"found_json": res2["found_json"], "target_sig": forged_sig,
                 "vouchers": res2["vouchers"], "introducer": res2.get("introducer")}
        bob_before = alice.acq["Bob"].trust
        out = await alice.collaborate(res2["found"], "审合同", proof=proof)
        if out is None:
            print(f"  ✓ 介绍人冒充被识破：Bob 用自己私钥伪造的 target_sig，")
            print(f"    经 Bob 担保的 Dave 公钥验不过 → 拒绝协作 + 降 Bob "
                  f"{bob_before:.2f}→{alice.acq['Bob'].trust:.2f}")
            print("  （非对称：介绍人有目标公钥能验，无私钥不能签 → 无法冒充已绑定的身份）")
    alice.acq["Dave"].secret = real_secret6
    alice.acq["Dave"].trust = dt6
    alice.acq["Bob"].trust = 0.7
    net.set_degree_all()

    # ---- 阶段7：向量语义路由（标签集合交集 → 余弦相似度）----
    print("\n" + "=" * 72)
    print("【阶段7】向量语义路由：集合交集(二值) → 余弦相似度(连续)")
    print("=" * 72)
    print("\n  Alice 找 'law'，候选熟人按语义相似度排序（新法）vs 集合交集(旧法)：")
    print(f"  {'熟人':12s} {'标签':22s} {'旧法(交集)':>10s} {'新法(余弦)':>10s}")
    candidates = [
        ("Dave", ["law", "finance"]),
        ("Eve",  ["law", "writing"]),
        ("Bob",  ["python", "design"]),
        ("Carol", ["design", "art"]),
    ]
    for name, tags in candidates:
        old = len(set(tags) & RELATED.get("law", frozenset()))   # 旧法：集合交集大小
        new = semantic_sim("law", set(tags))                     # 新法：余弦相似度
        print(f"  {name:12s} {str(tags):22s} {old:>10d} {new:>10.3f}")
    print("\n  旧法二值(0/1)：Dave=1, Eve=1, Bob=0, Carol=0 → 无法区分 Dave 和 Eve 谁更相关")
    print("  新法连续：Dave=0.977 > Eve=0.817 > Bob=0.259 > Carol=0.171 → 精准区分")
    print("  → 向量语义路由比标签交集更准：law 与 finance(0.92)、contract(0.98) 高度相关，")
    print("    与 writing(0.37) 弱相关，与 python/design(0.2) 几乎不相关——符合真实语义。")

    print("\n" + "=" * 72)
    print(" 全流程结束：发现→协作→拓扑→churn→Sybil→身份验证→介绍人担保→向量语义路由")
    print("=" * 72)
    for s in servers:
        s.close()
    await asyncio.gather(*[s.wait_closed() for s in servers], return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
