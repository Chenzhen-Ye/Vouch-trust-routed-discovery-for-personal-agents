"""
agentnet_privacy.py — Vouch 协议（隐私版）：转发时不暴露完整熟人表，只返回「方向」

在 agentnet.py 基础上收紧信息流，演示四个隐私性质，并用「信息流审计」
把「每个节点学到了什么 / 没学到什么」打印出来对照：

  1. 消息不带完整路径 —— 中继看不到整条链，只看得到自己的上一跳/下一跳。
  2. 回程用「分布式私有回信令牌」逐跳回传 —— 回程路径散落在各中继的私有
     内存里，任何单条消息都只含一跳的回信地址；源只拿到结果，拿不到中间人名单。
  3. 源只学到 跳数(hop_count) + 目标(解密后)，不是路由 —— 知道「多远」「找到
     了谁」，不知道「经过谁」。
  4. 结果负载端到端加密（DH 协商会话密钥）—— 中继转发的是密文，看不到找到的是谁。

代价（隐私 vs 效率的张力，分析里的核心）：
  · 隐藏路径 ⇒ 中继无法用「已访问集合」做剪枝；环路防止只靠 query_id 去重 + TTL。
    flood 模式下会出现发往「已访问节点」的冗余消息（被去重丢弃，但消息已发出）。
    运行末尾的消息计数会显示这个代价。

诚实标注的残缺泄漏（任何熟人路由都难避免）：
  · 上一跳端口可被中继识别（你的邻居本来就知道你是谁；等价于 Tor 入口节点）。
  · 源必然学到目标身份 —— 否则无法协作。这是「发现即揭示」的设计性泄漏。
  · 所求能力(capability) 明文出现在查询里（中继需要它来引导转发 + 自我匹配），
    但能力比「具体人名」更不具身份性 —— 这是 discover 比 lookup 更私有的原因。
  · DH 用 64 位安全素数仅为演示（生成快）；生产需 ≥2048 位 + 真实曲线/库。

零依赖，仅标准库。运行：python3 agentnet_privacy.py
"""
from __future__ import annotations
import asyncio
import json
import secrets
import hashlib
from dataclasses import dataclass

HOST = "127.0.0.1"
DEFAULT_TTL = 6
GUIDED_FANOUT = 1

RELATED = {
    "law":     frozenset({"law", "finance", "contract", "policy"}),
    "writing": frozenset({"writing", "editing", "blog", "translation"}),
    "python":  frozenset({"python", "backend", "data", "ml"}),
    "design":  frozenset({"design", "art", "ui", "brand"}),
    "finance": frozenset({"finance", "law", "accounting"}),
}

# ---------------- 紧凑 DH（演示用）----------------
def _miller_rabin(n: int, k: int = 8) -> bool:
    if n < 2:
        return False
    for p in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47):
        if n % p == 0:
            return n == p
    d, r = n - 1, 0
    while d % 2 == 0:
        d //= 2; r += 1
    for _ in range(k):
        a = 2 + secrets.randbelow(n - 3)
        x = pow(a, d, n)
        if x in (1, n - 1):
            continue
        for _ in range(r - 1):
            x = pow(x, 2, n)
            if x == n - 1:
                break
        else:
            return False
    return True

def _gen_safe_prime(bits: int = 64) -> int:
    while True:
        q = secrets.randbits(bits) | (1 << (bits - 1)) | 1
        if not _miller_rabin(q):
            continue
        p = 2 * q + 1
        if _miller_rabin(p):
            return p

P = _gen_safe_prime(64)
G = 2

def _derive_key(shared: int) -> bytes:
    return hashlib.sha256(b"vouch-e2e-v1|" + str(shared).encode()).digest()

def _xor_stream(key: bytes, data: bytes) -> bytes:
    return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))


# ---------------- 数据结构 ----------------
@dataclass
class Acquaintance:
    name: str
    port: int
    tags: frozenset
    trust: float = 0.8
    degree: int = 0


REGISTRY: dict = {}
AUDIT: list = []          # (query_id, name, role, info) —— 信息流审计
_COUNT: dict = {}
DELIVER = "__DELIVER__"   # 源的回信令牌映射到此哨兵：收到即交付


def _bump(k): _COUNT[k] = _COUNT.get(k, 0) + 1
def _counts(): return dict(_COUNT)
def _reset(): _COUNT.clear()
def _audit(qid, name, role, info): AUDIT.append((qid, name, role, info))


class Agent:
    def __init__(self, name, port, caps):
        self.name = name
        self.port = port
        self.caps = frozenset(caps)
        self.acq: dict = {}
        self._seen: set = set()           # query_id 去重（环路防止）
        self._envelopes: dict = {}        # token -> 上一跳 Env | DELIVER（私有回信链）
        self._pending: dict = {}          # query_id -> Future
        self._dhpriv: dict = {}           # query_id -> DH 私钥（源侧）
        self._qctr = 0
        self.tag = f"[{name}@{port}]"
        REGISTRY[name] = self

    def knows(self, other_name, port, tags, trust=0.8):
        self.acq[other_name] = Acquaintance(other_name, port, frozenset(tags), trust)

    def _port_to_name(self, port):
        for a in self.acq.values():
            if a.port == port:
                return a.name
        return f"?@{port}"

    # ---------- 服务器 ----------
    async def serve(self):
        return await asyncio.start_server(self._handle, HOST, self.port)

    async def _handle(self, reader, writer):
        try:
            line = await reader.readline()
            if not line:
                return
            msg = json.loads(line.decode())
            if msg["type"] == "task":
                out = self._do_task(msg)
                writer.write((json.dumps({"result": out}) + "\n").encode())
            else:
                await self._dispatch(msg)
                writer.write(b'{"ok":true}\n')
            await writer.drain()
        except Exception as e:
            print(f"{self.tag} 处理出错: {e!r}")
        finally:
            writer.close()

    async def _dispatch(self, msg):
        if msg["type"] == "query":
            await self._on_query(msg)
        elif msg["type"] == "response":
            await self._on_response(msg)

    async def _send(self, port, msg):
        kind = msg.get("strategy") if msg["type"] == "query" else msg["type"]
        _bump(kind)
        try:
            r, w = await asyncio.open_connection(HOST, port)
            w.write((json.dumps(msg) + "\n").encode())
            await w.drain()
            await r.readline()
            w.close()
        except OSError as e:
            print(f"{self.tag} 连接 {port} 失败: {e!r}")

    # ---------- 源发起 ----------
    async def discover(self, capability, strategy="guided", ttl=DEFAULT_TTL):
        qid = self._next_qid(); self._seen.add(qid)
        priv = secrets.randbelow(P - 2) + 1
        pub = pow(G, priv, P)
        self._dhpriv[qid] = priv
        fut = asyncio.get_running_loop().create_future()
        self._pending[qid] = fut
        token = secrets.token_hex(4)
        self._envelopes[token] = DELIVER            # 我的回信令牌 → 交付
        env = {"port": self.port, "token": token}
        msg = {"type": "query", "mode": "discover", "capability": capability,
               "strategy": strategy, "ttl": ttl, "query_id": qid, "hop_count": 0,
               "source_dh_pub": pub, "return_env": env}
        _audit(qid, self.name, "源-发起", {"所求能力": capability, "策略": strategy,
                                        "学到目标?": "尚否(待解密结果)", "学到路由?": "否"})
        print(f"\n{self.tag} 发起 discover(cap={capability}, strat={strategy})  [隐私模式]")
        await self._forward(msg, predecessor_env=None)
        return await self._await(qid)

    def _next_qid(self):
        q = f"{self.name}-{self._qctr}"; self._qctr += 1; return q

    async def _await(self, qid):
        try:
            return await asyncio.wait_for(self._pending[qid], timeout=8)
        except asyncio.TimeoutError:
            print(f"{self.tag} 超时，未找到"); return None
        finally:
            self._pending.pop(qid, None); self._dhpriv.pop(qid, None)

    # ---------- 收到查询 ----------
    async def _on_query(self, msg):
        qid = msg["query_id"]
        if qid in self._seen:                 # 环路防止：重复查询直接丢弃
            return
        self._seen.add(qid)
        env = msg["return_env"]
        prev_port = env["port"]
        prev_name = self._port_to_name(prev_port)
        hop = msg["hop_count"]

        hit = msg["mode"] == "discover" and msg["capability"] in self.caps
        if hit:
            _audit(qid, self.name, "目标", {"所求能力": msg["capability"], "跳数": hop,
                                          "上一跳端口": prev_port, "认识上一跳?": prev_name != f"?@{prev_port}",
                                          "学到源身份?": "否(仅上一跳端口)", "学到完整路径?": "否"})
            print(f"{self.tag} ✓ 命中（我是目标） hop={hop}")
            await self._reply_target(msg)
            return

        if msg["ttl"] <= 0:
            print(f"{self.tag} TTL 耗尽，停止"); return

        # 包装回信令牌：我给下游一个 {我的端口, 新令牌}；我自己私存「上一跳 env」。
        token = secrets.token_hex(4)
        self._envelopes[token] = env           # 私有：回程时把响应转发给 env.port
        new_env = {"port": self.port, "token": token}
        msg2 = dict(msg)
        msg2["return_env"] = new_env
        msg2["hop_count"] = hop + 1
        msg2["ttl"] = msg["ttl"] - 1
        await self._forward(msg2, predecessor_env=env)

    # ---------- 转发决策 ----------
    async def _forward(self, msg, predecessor_env):
        if not self.acq:
            return
        ports = ([a.port for a in self.acq.values()] if msg.get("strategy") == "flood"
                  else self._guided_pick(msg))
        names = [self._port_to_name(p) for p in ports]
        prev = (predecessor_env or {}).get("port")
        prev_disp = self._port_to_name(prev) if prev else "(源)"
        _audit(msg["query_id"], self.name, "中继", {
            "上一跳": prev_disp, "下一跳": names, "所求能力": msg.get("capability"),
            "hop": msg["hop_count"], "看到结果负载?": "否(此时只有查询)"})
        print(f"{self.tag} 转发(mode={msg['mode']}, ttl={msg['ttl']}, hop={msg['hop_count']}, "
              f"strat={msg.get('strategy')}) {prev_disp} → {names}")
        for p in ports:
            await self._send(p, msg)

    def _guided_pick(self, msg):
        """无路径 ⇒ 无法跳过已访问节点；靠 query_id 去重兜底。按语义相关度+桥梁度挑 top-k。"""
        cap = msg.get("capability")
        rel = RELATED.get(cap, frozenset({cap}) if cap else frozenset())
        cands = list(self.acq.values())
        if not cands:
            return []
        max_deg = max(a.degree for a in cands) or 1
        scored = []
        for a in cands:
            tag = len(a.tags & rel)
            hub = 0.3 * (a.degree / max_deg)
            scored.append((tag + hub, a.trust, a.port))
        scored.sort(reverse=True)
        return [p for _, _, p in scored[:GUIDED_FANOUT]]

    # ---------- 目标回程：加密 found，发回给上一跳 ----------
    async def _reply_target(self, msg):
        qid = msg["query_id"]
        found = {"name": self.name, "port": self.port, "caps": sorted(self.caps)}
        # DH：用源公钥 + 我的临时私钥协商会话密钥，加密 found
        tpriv = secrets.randbelow(P - 2) + 1
        tpub = pow(G, tpriv, P)
        shared = pow(msg["source_dh_pub"], tpriv, P)
        key = _derive_key(shared)
        ct = _xor_stream(key, json.dumps(found).encode()).hex()
        resp = {"type": "response", "query_id": qid, "hop_count": msg["hop_count"],
                "target_dh_pub": tpub, "payload_ct": ct, "return_env": msg["return_env"]}
        await self._send(msg["return_env"]["port"], resp)

    # ---------- 中继/源 收到响应 ----------
    async def _on_response(self, msg):
        env = msg["return_env"]
        token = env["token"]
        if token not in self._envelopes:
            return
        nxt = self._envelopes.pop(token)
        if nxt is DELIVER:
            # 我就是源：解密 found，交付
            qid = msg["query_id"]
            priv = self._dhpriv.get(qid)
            if priv is None:
                return
            shared = pow(msg["target_dh_pub"], priv, P)
            key = _derive_key(shared)
            found = json.loads(_xor_stream(key, bytes.fromhex(msg["payload_ct"])).decode())
            _audit(qid, self.name, "源-收结果", {"找到目标": found["name"],
                "目标能力": found["caps"], "跳数": msg["hop_count"],
                "学到中间人?": "否", "学到路由?": "否(仅跳数)"})
            print(f"{self.tag} 收到结果：找到 {found['name']}  hop={msg['hop_count']}  "
                  f"(中间人/路由: 隐藏)")
            f = self._pending.get(qid)
            if f and not f.done():
                f.set_result({"found": found, "hop_count": msg["hop_count"]})
            return
        # 我是中继：把响应转发给上一跳（用我私存的 env），不碰密文
        _audit(msg["query_id"], self.name, "中继-回程", {
            "上一跳": self._port_to_name(nxt["port"]), "看到结果负载?": "否(密文不可读)",
            "学到目标?": "否"})
        msg2 = dict(msg); msg2["return_env"] = nxt
        print(f"{self.tag} 回程转发密文 → {self._port_to_name(nxt['port'])}（不解密）")
        await self._send(nxt["port"], msg2)

    # ---------- 协作 ----------
    async def send_task(self, port, task):
        _bump("task")
        r, w = await asyncio.open_connection(HOST, port)
        w.write((json.dumps({"type": "task", "from": self.name, "task": task}) + "\n").encode())
        await w.drain()
        line = await r.readline(); w.close()
        return json.loads(line.decode())["result"]

    def _do_task(self, msg):
        return f"{self.name}（能力={sorted(self.caps)}）完成了「{msg['task']}」→ 成品@{self.name}"

    def remember(self, found, trust=0.4):
        if found["name"] not in self.acq:
            self.acq[found["name"]] = Acquaintance(found["name"], found["port"],
                                                  frozenset(found["caps"]), trust, degree=1)
            return True
        return False


def build_graph():
    specs = [
        ("Alice", 7001, ["python", "backend"]),
        ("Bob",   7002, ["python", "design"]),
        ("Carol", 7003, ["design", "art"]),
        ("Dave",  7004, ["law", "finance"]),
        ("Eve",   7005, ["law", "writing"]),
        ("Frank", 7006, ["art", "design"]),
        ("Grace", 7007, ["writing", "editing"]),
    ]
    for n, p, c in specs:
        Agent(n, p, c)
    edges = [
        ("Alice", "Bob",   ["python", "design"], 0.9),
        ("Alice", "Carol", ["design", "art"],    0.6),
        ("Bob",   "Alice", ["python"],           0.9),
        ("Bob",   "Carol", ["design"],           0.6),
        ("Bob",   "Dave",  ["law", "finance"],   0.7),
        ("Bob",   "Eve",   ["writing"],          0.5),
        ("Carol", "Bob",   ["design"],           0.6),
        ("Carol", "Frank", ["art", "design"],    0.7),
        ("Dave",  "Bob",   ["python", "design"], 0.7),
        ("Dave",  "Eve",   ["law", "writing"],   0.8),
        ("Eve",   "Dave",  ["law"],              0.8),
        ("Eve",   "Grace", ["writing", "editing"], 0.7),
        ("Frank", "Carol", ["art"],               0.7),
        ("Frank", "Grace", ["writing"],          0.6),
        ("Grace", "Eve",   ["writing"],          0.6),
    ]
    for frm, to, tags, trust in edges:
        REGISTRY[frm].knows(to, REGISTRY[to].port, tags, trust)
    for ag in REGISTRY.values():
        for name, acq in ag.acq.items():
            acq.degree = len(REGISTRY[name].acq)
    return list(REGISTRY.values())


def print_audit(for_qid):
    print("\n" + "─" * 72)
    print(f"信息流审计  query_id={for_qid}")
    print("─" * 72)
    rows = [r for r in AUDIT if r[0] == for_qid]
    for qid, name, role, info in rows:
        info_s = "  ".join(f"{k}={v}" for k, v in info.items())
        print(f"  {name:6s} [{role}]  {info_s}")
    print("─" * 72)
    print("对照：明文版会暴露完整路径 Alice → Bob → Dave；隐私版源只学到「hop=2, 目标=Dave」。")


async def main():
    print("=" * 72)
    print(" 个人智能体发现与协作原型 — 隐私版")
    print(" 收紧：无路径列表 / 分布式回信令牌 / DH 端到端加密结果")
    print("=" * 72)
    agents = build_graph()
    alice = REGISTRY["Alice"]
    servers = await asyncio.gather(*[a.serve() for a in agents])

    print(f"\nDH 安全素数 P = {P.bit_length()} 位（演示用；生产 ≥2048）")

    print("\n拓扑（与明文版相同）：")
    for a in agents:
        acq_s = ", ".join(f"{n}(tags={sorted(x.tags)},deg={x.degree})" for n, x in a.acq.items())
        print(f"  {a.tag} caps={sorted(a.caps)}  熟人=[{acq_s}]")

    # ---- 场景 1：引导式 discover「懂 law 的人」 ----
    _reset()
    res = await alice.discover("law", strategy="guided")
    main_qid = f"Alice-0"
    print_audit(main_qid)
    print(f"\n[复杂度] guided(隐私) 消息数: {_counts()}")

    # ---- 场景 2：洪泛 discover，对照「隐藏路径的代价」 ----
    _reset()
    AUDIT.clear()
    print("\n" + "=" * 72)
    print(" flood 模式：隐藏路径 ⇒ 无法剪枝已访问节点，会出现发往已访问节点的冗余消息")
    print("=" * 72)
    await alice.discover("law", strategy="flood")
    print(f"\n[复杂度] flood(隐私) 消息数: {_counts()}")
    print("（部分 query 被去重丢弃，但消息已发出 —— 这就是「不能用已访问集合剪枝」的代价）")
    for a in agents:
        a._seen.clear()

    # ---- 场景 3：发现 → 直接协作 ----
    print("\n" + "=" * 72)
    print(" 发现 → 协作（源用解密得到的目标端口直接联系；这是「发现即揭示」的设计性泄漏）")
    print("=" * 72)
    if res and res.get("found"):
        f = res["found"]
        new = alice.remember(f)
        print(f"{alice.tag} 把 {f['name']} 以弱信任加入熟人表了吗？"
              f"{'是' if new else '已是熟人'}")
        out = await alice.send_task(f["port"], "帮我起草一份雇佣合同要点")
        print(f"{alice.tag} 协作产物 ← {out}")

        # 第二次 discover：现在 Alice→目标直连，应 1 跳命中
        _reset(); AUDIT.clear()
        print("\n第二次 discover('law')：路径缓存后近 O(1)，且仍只暴露 1 跳：")
        await alice.discover("law", strategy="guided")
        print(f"[复杂度] 缓存后再查 消息数: {_counts()}")

    print("\n" + "=" * 72)
    print(" 结束")
    print("=" * 72)
    for s in servers:
        s.close()
    await asyncio.gather(*[s.wait_closed() for s in servers])


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
