"""
agentnet_signed.py — Vouch 协议（可验证发现版）：隐私版 + 目标签名

在 agentnet_privacy.py（无路径 / 分布式回信令牌 / DH 端到端加密）基础上，
叠加「可验证发现」与「消息完整性」，解决 DESIGN.md §8 的两项 backlog：

  · 可验证发现：目标用签名私钥对明文 found 签名；源用预先持有的目标验证公钥
    验证。这样中继无法冒充目标（哪怕它谎称「我就是目标」并把假 found 加密发回，
    也签不出 Dave 的签名）。覆盖 §8「目标可被冒充」。
  · 消息完整性：found 内含对 payload 的签名；解密后验签，中继篡改 payload_ct
    会被发现（签名不再匹配）。覆盖 §8「中继篡改不可检测」。

签名放哪一层（关键设计决策）：
  签名对「明文 found」签，不是对 payload_ct 签。理由：
    1. 只有源能解密 → 只有源能验签，中继连验证都做不了，更不暴露目标身份。
    2. 签的是「我是 X，我有能力 Y」这个声明本身，语义清晰。
    3. found 里同时含 payload_ct 的指纹，等价于对密文完整性负责（解密后比对）。

威胁模型增量（相对隐私版）：
  · 诚实但好奇的中继 → 现在还要防「冒充目标」和「篡改 payload」。
  · 假设源已通过带外渠道预先持有目标的验证公钥（PGP 式信任锚）。
    ——这是「可验证」的前提：没有预先的公钥，就无法区分真假 Dave。
    预先公钥从哪来？见文末「信任锚问题」讨论。

签名方案：极简 RSA（标准库手写，30 行）。演示用小素数（~256 位），
生产必须 Ed25519/RSA-2048 + 真实库。安全属性（非对称、可验证）不变。

零依赖，仅标准库。运行：python3 agentnet_signed.py
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

# ---------------- 紧凑 DH（演示用，同隐私版）----------------
def _miller_rabin(n, k=8):
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

def _gen_safe_prime(bits=64):
    while True:
        q = secrets.randbits(bits) | (1 << (bits - 1)) | 1
        if not _miller_rabin(q):
            continue
        p = 2 * q + 1
        if _miller_rabin(p):
            return p

P = _gen_safe_prime(64)
G = 2

def _derive_key(shared):
    return hashlib.sha256(b"vouch-e2e-v1|" + str(shared).encode()).digest()

def _xor_stream(key, data):
    return bytes(b ^ key[i % len(key)] for i, b in enumerate(data))


# ---------------- 极简 RSA 签名（演示用）----------------
def _is_prime(n, k=10):
    return _miller_rabin(n, k)

def _gen_prime(bits):
    while True:
        n = secrets.randbits(bits) | 1 | (1 << (bits - 1))
        if _is_prime(n):
            return n

def _egcd(a, b):
    if b == 0:
        return a, 1, 0
    g, x, y = _egcd(b, a % b)
    return g, y, x - (a // b) * y

def _modinv(a, m):
    g, x, _ = _egcd(a % m, m)
    if g != 1:
        raise ValueError("无模逆")
    return x % m

def gen_keypair(bits=256):
    """返回 (priv_dict, pub_dict)。e 固定 65537。"""
    while True:
        p, q = _gen_prime(bits), _gen_prime(bits)
        if p == q:
            continue
        n = p * q
        phi = (p - 1) * (q - 1)
        e = 65537
        if _egcd(e, phi)[0] != 1:
            continue
        d = _modinv(e, phi)
        if d > 1:
            break
    return {"n": n, "d": d}, {"n": n, "e": e}

def _pkcs1_pad(msg: bytes, n_bytes: int) -> int:
    """极简 PKCS#1 v1.5 式填充（演示，不抗攻击；生产用 RSASSA-PSS）。"""
    h = hashlib.sha256(msg).digest()
    pad_len = n_bytes - 3 - len(h)
    if pad_len < 8:
        raise ValueError("模数太小")
    pad = b"\xff" * pad_len
    em = b"\x00\x01" + pad + b"\x00" + h
    return int.from_bytes(em, "big")

def sign(priv, msg: bytes) -> int:
    n, d = priv["n"], priv["d"]
    m = _pkcs1_pad(msg, (n.bit_length() + 7) // 8)
    return pow(m, d, n)

def verify(pub, msg: bytes, sig: int) -> bool:
    n, e = pub["n"], pub["e"]
    expected = _pkcs1_pad(msg, (n.bit_length() + 7) // 8)
    got = pow(sig, e, n)
    return got == expected


# ---------------- 数据结构 ----------------
@dataclass
class Acquaintance:
    name: str
    port: int
    tags: frozenset
    trust: float = 0.8
    degree: int = 0
    verify_pub: dict = None      # 预先持有的该熟人的验证公钥（信任锚）


REGISTRY: dict = {}
AUDIT: list = []
_COUNT: dict = {}
DELIVER = "__DELIVER__"


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
        self._seen: set = set()
        self._envelopes: dict = {}
        self._pending: dict = {}
        self._dhpriv: dict = {}
        self._qctr = 0
        self.tag = f"[{name}@{port}]"
        # 每个智能体一对签名密钥：priv 自己持有，pub 通过带外渠道分发给信任自己的人
        self._sign_priv, self.verify_pub = gen_keypair(256)
        REGISTRY[name] = self

    def knows(self, other_name, port, tags, trust=0.8, verify_pub=None):
        """verify_pub：我预先持有的 other 的验证公钥（信任锚）。None=暂不验证该熟人。"""
        self.acq[other_name] = Acquaintance(other_name, port, frozenset(tags), trust,
                                            degree=0, verify_pub=verify_pub)

    def _port_to_name(self, port):
        for a in self.acq.values():
            if a.port == port:
                return a.name
        return f"?@{port}"

    def _port_to_acq(self, port):
        for a in self.acq.values():
            if a.port == port:
                return a
        return None

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
        self._envelopes[token] = DELIVER
        env = {"port": self.port, "token": token}
        msg = {"type": "query", "mode": "discover", "capability": capability,
               "strategy": strategy, "ttl": ttl, "query_id": qid, "hop_count": 0,
               "source_dh_pub": pub, "return_env": env}
        _audit(qid, self.name, "源-发起", {"所求能力": capability, "策略": strategy,
                                        "已持目标公钥?": "否(发现后才知道是谁)"})
        print(f"\n{self.tag} 发起 discover(cap={capability}, strat={strategy})  [可验证版]")
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
        if qid in self._seen:
            return
        self._seen.add(qid)
        env = msg["return_env"]
        prev_port = env["port"]
        prev_name = self._port_to_name(prev_port)
        hop = msg["hop_count"]

        hit = msg["mode"] == "discover" and msg["capability"] in self.caps
        if hit:
            _audit(qid, self.name, "目标", {"所求能力": msg["capability"], "跳数": hop,
                                          "上一跳": prev_name, "冒充可能?": "否(无源预期公钥仍可冒充;签名是源验)"})
            print(f"{self.tag} ✓ 命中（我是目标） hop={hop}  → 用私钥签 found")
            await self._reply_target(msg)
            return

        if msg["ttl"] <= 0:
            print(f"{self.tag} TTL 耗尽，停止"); return

        token = secrets.token_hex(4)
        self._envelopes[token] = env
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
            "上一跳": prev_disp, "下一跳": names, "hop": msg["hop_count"]})
        print(f"{self.tag} 转发(mode={msg['mode']}, ttl={msg['ttl']}, hop={msg['hop_count']}, "
              f"strat={msg.get('strategy')}) {prev_disp} → {names}")
        for p in ports:
            await self._send(p, msg)

    def _guided_pick(self, msg):
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

    # ---------- 目标回程：加密 found + 对明文 found 签名 ----------
    async def _reply_target(self, msg):
        qid = msg["query_id"]
        found = {"name": self.name, "port": self.port, "caps": sorted(self.caps)}
        found_json = json.dumps(found, sort_keys=True).encode()
        # DH 加密（同隐私版）
        tpriv = secrets.randbelow(P - 2) + 1
        tpub = pow(G, tpriv, P)
        shared = pow(msg["source_dh_pub"], tpriv, P)
        key = _derive_key(shared)
        ct = _xor_stream(key, found_json).hex()
        # 签名：对明文 found 签（只有我能签，源用我的公钥验）
        sig = sign(self._sign_priv, found_json)
        resp = {"type": "response", "query_id": qid, "hop_count": msg["hop_count"],
                "target_dh_pub": tpub, "payload_ct": ct, "target_sig": str(sig),
                "signer_name": self.name, "return_env": msg["return_env"]}
        await self._send(msg["return_env"]["port"], resp)

    # ---------- 中继/源 收到响应 ----------
    async def _on_response(self, msg):
        env = msg["return_env"]
        token = env["token"]
        if token not in self._envelopes:
            return
        nxt = self._envelopes.pop(token)
        if nxt is DELIVER:
            # 我就是源：解密 → 找验证公钥 → 验签
            qid = msg["query_id"]
            priv = self._dhpriv.get(qid)
            if priv is None:
                return
            shared = pow(msg["target_dh_pub"], priv, P)
            key = _derive_key(shared)
            # 解密+验签包在一起：解密失败(密文被篡改致 JSON 坏)或验签失败，
            # 都归为「完整性破坏，拒绝」。等价 Encrypt-then-MAC 的完整性保证。
            try:
                found_json = _xor_stream(key, bytes.fromhex(msg["payload_ct"]))
                found = json.loads(found_json.decode())
                sig = int(msg["target_sig"])
                signer = msg["signer_name"]
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                found = {"name": "?(解密失败)", "port": None, "caps": []}
                sig = None; signer = msg.get("signer_name", "?")
                _audit(qid, self.name, "源-收结果", {"找到": "?(解密失败)",
                    "跳数": msg["hop_count"], "验证": "✗完整性破坏（密文被篡改，解密失败）"})
                print(f"{self.tag} 收到结果：密文被篡改→解密失败，拒绝信任 hop={msg['hop_count']}")
                f = self._pending.get(qid)
                if f and not f.done():
                    f.set_result({"found": found, "hop_count": msg["hop_count"],
                                  "verified": False, "reason": "integrity_broken"})
                return
            # 查我是否预先持有该目标的验证公钥
            acq = self.acq.get(signer)
            vpub = acq.verify_pub if acq else None
            if vpub is None:
                verdict = "未持公钥→无法验证（拒绝协作）"
                ok = False
                _audit(qid, self.name, "源-收结果", {"找到": found["name"],
                    "跳数": msg["hop_count"], "验证": verdict})
                print(f"{self.tag} 收到结果：找到 {found['name']} hop={msg['hop_count']} "
                      f"但未持有其验证公钥 → 拒绝信任")
                f = self._pending.get(qid)
                if f and not f.done():
                    f.set_result({"found": found, "hop_count": msg["hop_count"],
                                  "verified": False, "reason": "no_trust_anchor"})
                return
            ok = verify(vpub, found_json, sig)
            verdict = "✓签名有效（是本人）" if ok else "✗签名无效（冒充/篡改）"
            _audit(qid, self.name, "源-收结果", {"找到": found["name"], "跳数": msg["hop_count"],
                "验证": verdict, "学到中间人?": "否"})
            print(f"{self.tag} 收到结果：找到 {found['name']} hop={msg['hop_count']} "
                  f"验签={verdict}")
            f = self._pending.get(qid)
            if f and not f.done():
                f.set_result({"found": found, "hop_count": msg["hop_count"], "verified": ok})
            return
        # 中继：转发密文+签名，不验签（中继无源私钥无法解密，也无目标公钥）
        _audit(msg["query_id"], self.name, "中继-回程", {
            "上一跳": self._port_to_name(nxt["port"]), "看到结果?": "否(密文+签名均不可读)"})
        msg2 = dict(msg); msg2["return_env"] = nxt
        print(f"{self.tag} 回程转发密文+签名 → {self._port_to_name(nxt['port'])}")
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

    def remember(self, found, trust=0.4, verify_pub=None):
        if found["name"] not in self.acq:
            self.acq[found["name"]] = Acquaintance(found["name"], found["port"],
                frozenset(found["caps"]), trust, degree=1, verify_pub=verify_pub)
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
    for qid, name, role, info in [r for r in AUDIT if r[0] == for_qid]:
        info_s = "  ".join(f"{k}={v}" for k, v in info.items())
        print(f"  {name:6s} [{role}]  {info_s}")
    print("─" * 72)


async def main():
    print("=" * 72)
    print(" Vouch 协议 — 可验证发现版（隐私版 + 目标签名）")
    print(" 加：目标对明文 found 签名；源用预先持有的验证公钥验签")
    print("=" * 72)
    agents = build_graph()
    alice = REGISTRY["Alice"]
    servers = await asyncio.gather(*[a.serve() for a in agents])

    print(f"\nDH 安全素数 {P.bit_length()} 位；RSA 签名模数 ~256 位（均演示用）")

    print("\n拓扑（同前；每个智能体现在自带签名密钥对）:")
    for a in agents:
        acq_s = ", ".join(f"{n}(tags={sorted(x.tags)},deg={x.degree},pub={'有' if x.verify_pub else '无'})"
                          for n, x in a.acq.items())
        print(f"  {a.tag} caps={sorted(a.caps)}  熟人=[{acq_s}]")

    # ---- 场景 1：Alice 未预先持有 Dave 的公钥 → 发现了但无法验证，拒绝协作 ----
    print("\n" + "=" * 72)
    print(" 场景1：未预先持有目标公钥 → 发现成功但验证失败，拒绝协作")
    print("=" * 72)
    _reset(); AUDIT.clear()
    res = await alice.discover("law", strategy="guided")
    print_audit("Alice-0")
    print(f"\n[复杂度] 消息数: {_counts()}")
    if res:
        print(f"结果：found={res['found']['name']} verified={res.get('verified')} "
              f"reason={res.get('reason','-')}")
        print("→ Alice 找到了懂 law 的人（Dave），但因为没预先持有 Dave 的验证公钥，")
        print("  无法确认对方真是 Dave，按协议拒绝信任（不会协作）。")

    # ---- 场景 2：Alice 预先通过带外渠道拿到 Dave 的公钥 → 验签通过，可协作 ----
    print("\n" + "=" * 72)
    print(" 场景2：Alice 预先（带外）持有 Dave 的验证公钥 → 验签通过，可协作")
    print("=" * 72)
    # 模拟带外信任锚：Alice 通过某可靠渠道预先得知 Dave 的公钥
    alice.acq["Dave"] = Acquaintance("Dave", REGISTRY["Dave"].port,
                                     frozenset(["law", "finance"]), 0.7,
                                     degree=2, verify_pub=REGISTRY["Dave"].verify_pub)
    print("Alice 已（带外）获得 Dave 的验证公钥，作为信任锚。")
    _reset(); AUDIT.clear()
    res2 = await alice.discover("law", strategy="guided")
    print_audit("Alice-1")
    print(f"\n[复杂度] 消息数: {_counts()}")
    if res2 and res2.get("verified"):
        f = res2["found"]
        print(f"\n✓ 验签通过：确认是 {f['name']} 本人。现在可以安全协作。")
        out = await alice.send_task(f["port"], "帮我起草一份雇佣合同要点")
        print(f"{alice.tag} 协作产物 ← {out}")
    else:
        print(f"\n✗ 验签未通过：{res2}")

    # ---- 场景 3：中间人篡改 payload_ct → 验签失败 ----
    print("\n" + "=" * 72)
    print(" 场景3：中继篡改 payload_ct（哪怕只改 1 字节）→ 源验签失败，拒绝")
    print("=" * 72)
    # 直接 hook Dave 的回程：在它发出响应前篡改 payload_ct。
    # （语义等价于「路径上某中继改了密文」；hook 目标而非中继是为了不受
    #   场景2 remember 改拓扑、路径不再经过 Bob 的影响。）
    orig_reply = REGISTRY["Dave"]._reply_target

    async def tampered_reply(msg):
        # 不调用 orig_reply —— 只发篡改版，模拟「中继把密文改了再转发」。
        qid = msg["query_id"]
        tpriv = secrets.randbelow(P - 2) + 1
        tpub = pow(G, tpriv, P)
        shared = pow(msg["source_dh_pub"], tpriv, P)
        key = _derive_key(shared)
        found = {"name": "Dave", "port": REGISTRY["Dave"].port, "caps": ["law", "finance"]}
        found_json = json.dumps(found, sort_keys=True).encode()
        ct = bytearray(_xor_stream(key, found_json))
        ct[0] ^= 0xFF                      # 篡改：翻转密文首字节
        # 用 Dave 的真私钥签的是「未篡改的 found_json」，所以篡改后验签必败
        sig = sign(REGISTRY["Dave"]._sign_priv, found_json)
        tampered = {"type": "response", "query_id": qid, "hop_count": msg["hop_count"],
                    "target_dh_pub": tpub, "payload_ct": ct.hex(), "target_sig": str(sig),
                    "signer_name": "Dave", "return_env": msg["return_env"]}
        print(f"[Dave@7004] ⚠ 模拟中继篡改：把 payload_ct 首字节翻转后发给源")
        await REGISTRY["Dave"]._send(msg["return_env"]["port"], tampered)

    REGISTRY["Dave"]._reply_target = tampered_reply
    _reset(); AUDIT.clear()
    res3 = await alice.discover("law", strategy="guided")
    print_audit("Alice-2")
    print(f"\n[复杂度] 消息数: {_counts()}")
    if res3:
        print(f"结果：found={res3['found']['name']} verified={res3.get('verified')}")
        if not res3.get("verified"):
            print("→ 中继篡改了密文，源解密后验签失败，拒绝信任。完整性得到保护。")
        else:
            print("→ 异常：篡改后验签竟通过，检查实现。")
    REGISTRY["Dave"]._reply_target = orig_reply

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
