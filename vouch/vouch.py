"""
vouch.py — Vouch 协议整合版（明文，完整态）

把四个明文原型（agentnet / agentnet_topology / agentnet_churn）整合成一个文件，
跑通端到端全流程：发现 → 协作 → 拓扑演化 → churn 容错 → Sybil 防御。

机制清单（对应 DESIGN.md §4）：
  §4.1-4.6  guided/flood 路由 · discover/lookup · 发现即扩展 · 协作
  §4.9      拓扑维护：信任度随协作升降 · 衰减 · 拉黑 · churn/恶意区分
  §4.10     churn 容错：回程绕断点 · 去程多路径+源重试
  §4.11     Sybil 防御：弱连接不路由 · 桥梁度只数强连接 · 引荐名额

合并策略（关键决策）：
  · _send 返回 bool + timeout（churn 版）——回程绕断点的前提
  · _reply_back/_on_response 沿 path 往回找能连的 hop（churn 版是直发版的超集）
  · _forward/_guided_pick 加路由门槛（trust≥ROUTE_TRUST_THRESHOLD 才转发）+ fanout 参数
  · discover 带源重试（SOURCE_RETRIES）+ fanout 递增 + guided→flood 升级
  · collaborate 整套反馈循环（_on_collab_success/_on_churn_fail）+ 协作层重试 COLLAB_RETRIES
  · remember 加 introducer + INTRO_QUOTA 引荐名额
  · degree 只数强连接（抗 Sybil）
  · go_offline/_DOWN 模拟节点下线
  注意：SOURCE_RETRIES（发现层重试）≠ COLLAB_RETRIES（协作层重试），层次不同，各自保留。

不考虑隐私版（见记忆 vouch-scope-no-privacy）。
零依赖，仅标准库。运行：python3 vouch.py
"""
from __future__ import annotations
import asyncio
import json
import hmac
import hashlib
import secrets
from dataclasses import dataclass, field

HOST = "127.0.0.1"
DEFAULT_TTL = 6
GUIDED_FANOUT = 1

# ---- 拓扑维护参数（§4.9）----
ALPHA = 0.1              # 成功协作增益
BETA = 0.3               # 恶意失败重罚（响应了但质量差 / 多次 churn）
GAMMA = 0.05             # 每衰减周期
BLOCK_THRESHOLD = 0.2    # 低于此值 → 拉黑
DECAY_STEPS = 3          # 每周期代表「一段时间不互动」

# ---- churn 容错参数（§4.10）----
SOURCE_RETRIES = 2       # 发现层：源超时后重试次数
RETRY_FANOUT_STEP = 1   # 每次重试 fanout 加多少
COLLAB_RETRIES = 2       # 协作层：单次协作超时重试（区分 churn vs 恶意）
CHURN_PENALTY = 0.1      # churn 失败轻罚（< BETA，临时掉线不该重罚）
SEND_TIMEOUT = 2.0      # 单次连接/读超时：超时即判下线

# ---- Sybil 防御参数（§4.11）----
ROUTE_TRUST_THRESHOLD = 0.6  # 信任度低于此值的熟人【不参与路由】，只记录
INTRO_QUOTA = 2              # 每熟人每周期最多引荐 N 个新面孔

RELATED = {
    "law":     frozenset({"law", "finance", "contract", "policy"}),
    "writing": frozenset({"writing", "editing", "blog", "translation"}),
    "python":  frozenset({"python", "backend", "data", "ml"}),
    "design":  frozenset({"design", "art", "ui", "brand"}),
    "finance": frozenset({"finance", "law", "accounting"}),
}

# ---- 身份验证（明文简化版：预共享密钥 HMAC，联动签名↔信任）----
# §4.8 签名验「是不是本人」，§4.9 信任度校准「靠不靠谱」——两者原本平行。
# 联动：collaborate 前先用预共享 secret 验 found 的 HMAC（验身份），
# 验签通过才校准能力信任；验签失败拒绝协作 + 降介绍人信任（它引荐了假目标）。
def _hmac_sign(secret: bytes, msg: bytes) -> str:
    return hmac.new(secret, msg, hashlib.sha256).hexdigest()

def _hmac_verify(secret: bytes, msg: bytes, sig: str) -> bool:
    return hmac.compare_digest(_hmac_sign(secret, msg), sig)


_COUNT = {}
_DOWN: set = set()          # 模拟「下线」的节点名集合
REGISTRY = {}
_CLOCK = [0]               # 全局逻辑时钟（演示用；真实系统用墙钟时间）


def _bump(kind): _COUNT[kind] = _COUNT.get(kind, 0) + 1
def _counts(): return dict(_COUNT)
def _reset(): _COUNT.clear()
def tick():
    _CLOCK[0] += 1
    return _CLOCK[0]


@dataclass
class Acquaintance:
    name: str
    port: int
    tags: set = field(default_factory=set)    # 可变集合（标签随协作扩展）
    trust: float = 0.8
    degree: int = 0
    last_seen: int = 0        # 最后协作的逻辑时钟步
    interactions: int = 0     # 累计协作次数
    blocked: bool = False    # 拉黑（保留记录，不参与路由）
    intro_count: int = 0     # 本周期已引荐的新面孔数（Sybil 引荐名额）
    secret: bytes = b""      # 我预先持有的该熟人的身份密钥（信任锚，HMAC 验身份）


class Agent:
    def __init__(self, name, port, caps, quality_fn=None):
        self.name = name
        self.port = port
        self.caps = frozenset(caps)
        self.acq: dict = {}
        self._seen: set = set()
        self._pending: dict = {}
        self._qctr = 0
        self._server = None
        self.tag = f"[{name}@{port}]"
        # quality_fn：接任务时返回 (成品, 质量分0~1)。默认高质量；可让某些节点「坑」。
        self._quality_fn = quality_fn or (lambda task: (f"{self.name} 完成了「{task}」", 0.9))
        # 身份密钥：本智能体自己的 secret；信任我的人预先持有它，用来验我的响应。
        self.secret = secrets.token_bytes(16)
        REGISTRY[name] = self

    def knows(self, other_name, port, tags, trust=0.8, secret=b""):
        # secret：我预先持有的 other 的身份密钥（带外信任锚）；空=暂不验该熟人身份。
        self.acq[other_name] = Acquaintance(other_name, port, set(tags), trust, secret=secret)

    def _name_of_port(self, port):
        for a in self.acq.values():
            if a.port == port and not a.blocked:
                return a.name
        return f"?@{port}"

    def go_offline(self):
        """模拟节点下线：关停 server，后续连接被拒。"""
        _DOWN.add(self.name)
        if self._server:
            self._server.close()
        print(f"{self.tag} ✘ 已下线")

    # ---------- 服务器 ----------
    async def serve(self):
        self._server = await asyncio.start_server(self._handle, HOST, self.port)
        return self._server

    async def _handle(self, reader, writer):
        if self.name in _DOWN:
            writer.close(); return
        try:
            line = await reader.readline()
            if not line:
                return
            msg = json.loads(line.decode())
            if msg["type"] == "task":
                result, quality = self._quality_fn(msg["task"])
                writer.write((json.dumps({"result": result, "quality": quality}) + "\n").encode())
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

    async def _send(self, port, msg, timeout=SEND_TIMEOUT):
        """发消息，返回 True/False。失败（对方下线/超时）返回 False——回程绕断点的前提。"""
        kind = msg.get("strategy") if msg["type"] == "query" else msg["type"]
        _bump(kind)
        try:
            r, w = await asyncio.wait_for(asyncio.open_connection(HOST, port), timeout=timeout)
            w.write((json.dumps(msg) + "\n").encode())
            await w.drain()
            await asyncio.wait_for(r.readline(), timeout=timeout)
            w.close()
            return True
        except (OSError, asyncio.TimeoutError) as e:
            print(f"{self.tag} 连接 {self._name_of_port(port)}({port}) 失败: {type(e).__name__}")
            return False

    # ---------- 发起发现（带源重试 + fanout 递增 + 策略升级）----------
    async def discover(self, capability, strategy="guided", ttl=DEFAULT_TTL,
                       retries=SOURCE_RETRIES, fanout=None):
        attempt = 0
        cur_fanout = fanout or GUIDED_FANOUT
        cur_strat = strategy
        last_res = None
        while attempt <= retries:
            qid = self._next_qid(); self._seen.add(qid)
            fut = asyncio.get_running_loop().create_future()
            self._pending[qid] = fut
            msg = {"type": "query", "mode": "discover", "capability": capability,
                   "strategy": cur_strat, "ttl": ttl, "query_id": qid,
                   "fanout": cur_fanout,
                   "path": [{"name": self.name, "port": self.port}]}
            tag = f"尝试{attempt+1}" if attempt else "发起"
            print(f"\n{self.tag} {tag} discover(cap={capability}, strat={cur_strat}, "
                  f"fanout={cur_fanout})")
            await self._forward(msg, cur_fanout)
            last_res = await self._await(qid)
            if last_res is not None:
                return last_res
            attempt += 1
            if attempt <= retries:
                cur_fanout += RETRY_FANOUT_STEP
                if attempt >= 2 and cur_strat == "guided":
                    cur_strat = "flood"
                    print(f"{self.tag} 升级策略 guided→flood，撒大网抗 churn")
        print(f"{self.tag} {retries+1} 次尝试均失败")
        return last_res

    async def lookup(self, target, hints=(), ttl=DEFAULT_TTL):
        qid = self._next_qid(); self._seen.add(qid)
        fut = asyncio.get_running_loop().create_future()
        self._pending[qid] = fut
        msg = {"type": "query", "mode": "lookup", "target": target,
               "strategy": "guided", "ttl": ttl, "query_id": qid,
               "fanout": GUIDED_FANOUT, "hints": list(hints),
               "path": [{"name": self.name, "port": self.port}]}
        print(f"\n{self.tag} 发起 lookup(target={target}, hints={list(hints)})")
        await self._forward(msg, GUIDED_FANOUT)
        return await self._await(qid)

    def _next_qid(self):
        q = f"{self.name}-{self._qctr}"; self._qctr += 1; return q

    async def _await(self, qid, timeout=6):
        try:
            return await asyncio.wait_for(self._pending[qid], timeout=timeout)
        except asyncio.TimeoutError:
            print(f"{self.tag} 超时，未找到"); return None
        finally:
            self._pending.pop(qid, None)

    # ---------- 收到查询 ----------
    async def _on_query(self, msg):
        qid = msg["query_id"]
        if qid in self._seen:
            return
        self._seen.add(qid)
        path = msg["path"] + [{"name": self.name, "port": self.port}]
        hit = (msg["mode"] == "lookup" and msg["target"] == self.name) or \
              (msg["mode"] == "discover" and msg["capability"] in self.caps)
        if hit:
            print(f"{self.tag} ✓ 命中！路径={' → '.join(p['name'] for p in path)}")
            found = {"name": self.name, "port": self.port, "caps": sorted(self.caps)}
            found_json = json.dumps(found, sort_keys=True).encode()
            # 身份验证：用自己的 secret 对 found 做 HMAC。源若预先持有我的 secret
            # 即可验证「这响应是我发的，不是中继冒充的」。联动 §4.8↔§4.9 的关键。
            sig = _hmac_sign(self.secret, found_json)
            resp = {"type": "response", "query_id": qid, "path": path,
                    "found": found, "found_json": found_json.decode(), "hmac_sig": sig}
            await self._reply_back(resp, path)
            return
        if msg["ttl"] <= 0:
            print(f"{self.tag} TTL 耗尽，停止")
            return
        msg2 = dict(msg); msg2["path"] = path; msg2["ttl"] = msg["ttl"] - 1
        await self._forward(msg2, msg.get("fanout", GUIDED_FANOUT))

    # ---------- 转发决策：Sybil 门槛 + fanout ----------
    async def _forward(self, msg, fanout=None):
        # Sybil 防御：弱连接（trust < 阈值）不参与路由
        cands = [a for a in self.acq.values() if not a.blocked
                 and a.trust >= ROUTE_TRUST_THRESHOLD]
        if not cands:
            return
        fanout = fanout or GUIDED_FANOUT
        if msg.get("strategy") == "flood":
            ports = [a.port for a in cands]
        else:
            ports = self._guided_pick(msg, cands, fanout)
        names = [self._name_of_port(p) for p in ports]
        weak = [n for n, a in self.acq.items() if not a.blocked and a.trust < ROUTE_TRUST_THRESHOLD]
        print(f"{self.tag} 转发(ttl={msg['ttl']}, strat={msg.get('strategy')}, "
              f"fanout={fanout}) → {names}"
              + (f"  [弱连接不路由: {weak}]" if weak else ""))
        for p in ports:
            await self._send(p, msg)

    def _guided_pick(self, msg, cands, fanout):
        cap = msg.get("capability")
        hints = frozenset(msg.get("hints", ()))
        visited = {p["name"] for p in msg["path"]}
        cands = [a for a in cands if a.name not in visited]
        if not cands:
            return []
        max_deg = max(a.degree for a in cands) or 1
        scored = []
        for a in cands:
            if msg["mode"] == "discover":
                rel = RELATED.get(cap, frozenset({cap}) if cap else frozenset())
                tag = len(a.tags & rel)
            else:
                tag = len(a.tags & hints)
            hub = 0.3 * (a.degree / max_deg)       # degree 只数强连接（抗 Sybil）
            trust_w = 0.2 * a.trust                 # 更信的人更愿意把话筒给他
            scored.append((tag + hub + trust_w, a.trust, a.port))
        scored.sort(reverse=True)
        return [p for _, _, p in scored[:fanout]]

    # ---------- 响应回传：沿 path 往回找，断点绕过 ----------
    async def _reply_back(self, resp, path):
        """目标把响应发回源。沿 path 回传，上一跳掉线则绕过找更上游。"""
        for idx in range(len(path) - 2, -1, -1):
            hop = path[idx]
            if hop["name"] == self.name:
                continue
            ok = await self._send(hop["port"], resp)
            if ok:
                if idx < len(path) - 2:
                    print(f"{self.tag} 绕过断点：跳过 {path[idx+1]['name']}，直连 {hop['name']}")
                return
            print(f"{self.tag} 回程 {hop['name']} 下线，往回找更上游")
        print(f"{self.tag} 回程所有中继下线，直连源 {path[0]['name']}")
        await self._send(path[0]["port"], resp)

    async def _on_response(self, msg):
        """中继收到响应：往源方向转发，断点绕过。"""
        path = msg["path"]
        if path[0]["name"] == self.name:
            self._deliver(msg); return
        names = [p["name"] for p in path]
        if self.name not in names:
            return
        i = names.index(self.name)
        for idx in range(i - 1, -1, -1):
            hop = path[idx]
            if hop["name"] == self.name:
                continue
            ok = await self._send(hop["port"], msg)
            if ok:
                if idx < i - 1:
                    print(f"{self.tag} 绕过断点：跳过 {path[i-1]['name']}，直连 {hop['name']}")
                return
            print(f"{self.tag} 回程 {hop['name']} 下线，往回找更上游")
        print(f"{self.tag} 回程所有上游下线，直连源 {path[0]['name']}")
        await self._send(path[0]["port"], msg)

    def _deliver(self, resp):
        path = resp["path"]
        introducer = path[-2]["name"] if len(path) >= 2 else None
        print(f"{self.tag} 收到结果：找到 {resp['found']['name']} "
              f"路径={' → '.join(p['name'] for p in path)} 介绍人={introducer}")
        f = self._pending.get(resp["query_id"])
        if f and not f.done():
            f.set_result({"found": resp["found"], "path": path, "introducer": introducer,
                          "hmac_sig": resp.get("hmac_sig"), "found_json": resp.get("found_json")})

    # ---------- 协作 + 反馈（拓扑维护核心）----------
    async def collaborate(self, found, task, proof=None):
        """发现到目标后发起协作，按结果调信任度/标签。
        联动 §4.8↔§4.9：协作前先验身份（proof={hmac_sig, found_json}）。
        验签通过才校准能力信任；验签失败 → 拒绝协作 + 降介绍人信任。
        区分 churn 失败（超时，先重试再轻罚）vs 恶意失败（响应但质量差，重罚）。"""
        name = found["name"]; port = found["port"]
        print(f"{self.tag} 向 {name} 发起协作：「{task}」")
        acq = self.acq.get(name)
        if acq is None:
            self.remember(found); acq = self.acq[name]
        before = acq.trust

        # ---- 第1段：身份验证（签名↔信任联动的关键衔接）----
        if proof and proof.get("hmac_sig") and acq.secret:
            ok = _hmac_verify(acq.secret, proof["found_json"].encode(), proof["hmac_sig"])
            if not ok:
                print(f"  {self.tag} ✗ 身份验证失败：响应非 {name} 本人（冒充）→ 拒绝协作")
                # 降介绍人信任：它引荐了一个身份不实的目标。
                # 守卫：介绍人不能是自己（直连路径下 path[-2]==源），也不在已拉黑集。
                intro = proof.get("introducer")
                if intro and intro != self.name and intro in self.acq and not self.acq[intro].blocked:
                    intro_acq = self.acq[intro]
                    intro_before = intro_acq.trust
                    self._on_collab_fail(intro_acq)   # 重罚介绍人（引荐假目标）
                    print(f"  {self.tag} 介绍人 {intro} trust "
                          f"{intro_before:.2f}→{intro_acq.trust:.2f}（引荐了身份不实目标）")
                else:
                    print(f"  {self.tag} （直连目标无介绍人可降，或介绍人已拉黑）")
                return None
            print(f"  {self.tag} ✓ 身份验证通过：确认是 {name} 本人，进入协作")

        outcome = None
        for attempt in range(1, COLLAB_RETRIES + 2):
            outcome = await self._send_task(port, task)
            if outcome is not None:
                break
            if attempt <= COLLAB_RETRIES:
                print(f"  {self.tag} 超时（可能 churn），重试 {attempt}/{COLLAB_RETRIES}")

        if outcome is None:
            self._on_churn_fail(acq)
            print(f"  {self.tag} 协作失败(churn: 多次超时) → {name} trust {before:.2f}→{acq.trust:.2f}"
                  f"{'（拉黑）' if acq.blocked else ''}")
            return None
        result, quality = outcome
        self._on_collab_success(acq, found, quality)
        print(f"  {self.tag} 协作成功(质量={quality:.1f}) → {name} trust {before:.2f}→{acq.trust:.2f}, "
              f"标签={sorted(acq.tags)}, 次数={acq.interactions}")
        return result

    def _on_collab_success(self, acq, found, quality):
        acq.last_seen = tick()
        acq.interactions += 1
        acq.tags |= set(found.get("caps", []))
        if quality >= 0.7:
            acq.trust += ALPHA * (1 - acq.trust)            # 好→升
        elif quality >= 0.4:
            acq.trust += 0.3 * ALPHA * (1 - acq.trust)      # 一般→微升
        else:
            acq.trust -= BETA * acq.trust                    # 差→重罚
            if acq.trust < BLOCK_THRESHOLD:
                acq.blocked = True
        acq.trust = max(0.0, min(1.0, acq.trust))

    def _on_churn_fail(self, acq):
        """churn 失败（多次超时，长期离线）：轻罚。"""
        acq.trust -= CHURN_PENALTY * acq.trust
        if acq.trust < BLOCK_THRESHOLD:
            acq.blocked = True

    def _on_collab_fail(self, acq):
        """明确的恶意失败（保留接口；恶意场景在 _on_collab_success 按质量处理）。"""
        acq.trust -= BETA * acq.trust
        if acq.trust < BLOCK_THRESHOLD:
            acq.blocked = True

    async def _send_task(self, port, task, timeout=SEND_TIMEOUT):
        _bump("task")
        try:
            r, w = await asyncio.wait_for(asyncio.open_connection(HOST, port), timeout=2)
        except (OSError, asyncio.TimeoutError):
            return None
        try:
            w.write((json.dumps({"type": "task", "from": self.name, "task": task}) + "\n").encode())
            await w.drain()
            line = await asyncio.wait_for(r.readline(), timeout=timeout)
            d = json.loads(line.decode())
            return d["result"], d.get("quality", 0.5)
        except (OSError, asyncio.TimeoutError, json.JSONDecodeError):
            return None
        finally:
            try: w.close()
            except Exception: pass

    # ---------- 不活跃衰减 ----------
    def decay(self, steps=DECAY_STEPS):
        now = tick()
        removed = []
        for name, a in list(self.acq.items()):
            idle = now - a.last_seen
            for _ in range(min(idle, steps)):
                if a.blocked:
                    break
                a.trust *= (1 - GAMMA)
            if a.trust < BLOCK_THRESHOLD and not a.blocked:
                a.blocked = True
                removed.append(name)
        return removed

    # ---------- 发现即扩展（带 Sybil 引荐名额）----------
    def remember(self, found, trust=0.4, introducer=None):
        if found["name"] in self.acq:
            return False
        if introducer and introducer in self.acq:
            intro_acq = self.acq[introducer]
            if intro_acq.intro_count >= INTRO_QUOTA:
                print(f"{self.tag} ⚠ 拒绝引荐：{introducer} 本周期引荐名额"
                      f"({INTRO_QUOTA})已满，不接受新面孔 {found['name']}")
                return False
            intro_acq.intro_count += 1
        self.acq[found["name"]] = Acquaintance(found["name"], found["port"],
            set(found.get("caps", [])), trust, degree=1, last_seen=tick())
        return True


def _set_degree_all():
    """重新计算所有 degree（只数强连接，抗 Sybil）。加边后调用。"""
    for ag in REGISTRY.values():
        for name, acq in ag.acq.items():
            other = REGISTRY.get(name)
            if other:
                acq.degree = sum(1 for x in other.acq.values()
                                 if x.trust >= ROUTE_TRUST_THRESHOLD)


def build_graph(sparse=True):
    """sparse=True: 仅 3 条手填种子边（拓扑生命周期起点）。
    sparse=False: 完整 14 边小世界图（churn 演示用）。"""
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
        Agent(n, p, c, quality_fn=qf)
    if sparse:
        REGISTRY["Alice"].knows("Bob", REGISTRY["Bob"].port, ["python", "design"], 0.7)
        REGISTRY["Bob"].knows("Alice", REGISTRY["Alice"].port, ["python"], 0.7)
        REGISTRY["Bob"].knows("Dave", REGISTRY["Dave"].port, ["law", "finance"], 0.6)
    else:
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
    # 初始信任都 < 阈值，演示「要攒信任才能路由」
    for ag in REGISTRY.values():
        for name, acq in ag.acq.items():
            acq.last_seen = tick()
    _set_degree_all()
    return list(REGISTRY.values())


async def main():
    print("=" * 72)
    print(" Vouch 协议整合版（明文完整态）")
    print(" 发现 → 协作 → 拓扑演化 → churn 容错 → Sybil 防御")
    print("=" * 72)
    agents = build_graph(sparse=True)
    alice = REGISTRY["Alice"]
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
        f = res["found"]; intro = res.get("introducer")
        alice.remember(f, introducer=intro)
        # 带外信任锚：Alice 通过可靠渠道预先获得 Dave 的身份密钥（secret），
        # 后续协作前用它验「响应是不是 Dave 本人发的」（联动 §4.8↔§4.9）
        dave = REGISTRY["Dave"]
        alice.acq["Dave"].secret = dave.secret
        print(f"  → Alice 记住 {f['name']}（介绍人={intro}），并带外获得其身份密钥")
    # 和 Dave 多次高质量协作，把 Dave trust 攒到可路由(≥0.6)
    # 演示用：临时让 Dave 表现「好」（quality 0.9），好协作才该攒到可路由
    dave = REGISTRY["Dave"]
    _orig_dq = dave._quality_fn
    dave._quality_fn = lambda task: (f"{task}→成品@好", 0.9)
    for i in range(4):
        await alice.collaborate({"name": "Dave", "port": dave.port, "caps": ["law"]}, "看合同")
    dave._quality_fn = _orig_dq
    print(f"\n  Dave trust={alice.acq['Dave'].trust:.2f} "
          f"{'(≥阈值，可路由)' if alice.acq['Dave'].trust >= ROUTE_TRUST_THRESHOLD else '(仍弱)'}")

    # ---- 阶段2：churn 容错（去程断 → 重试；回程断 → 绕行）----
    print("\n" + "=" * 72)
    print("【阶段2】churn 容错：中间人下线，去程重试换路 / 回程绕过直连源")
    print("=" * 72)
    # 先把图补完整，让断 Bob 后还有别的路
    for frm, to, tags in [("Alice","Carol",["design"]), ("Carol","Frank",["art"]),
                          ("Frank","Grace",["writing"]), ("Grace","Eve",["writing"]),
                          ("Eve","Dave",["law"])]:
        if to not in REGISTRY[frm].acq:
            REGISTRY[frm].knows(to, REGISTRY[to].port, tags, 0.6)
    # 把 Carol/Frank/Grace/Eve 的 trust 也攒到可路由（演示需要）
    for n in ["Carol", "Frank", "Grace", "Eve"]:
        if n in alice.acq:
            alice.acq[n].trust = 0.7
    _set_degree_all()

    print("\n--- 2a. 去程断：让 Bob 下线，Alice 重试换路 ---")
    REGISTRY["Bob"].go_offline()
    await asyncio.sleep(0.2)
    res2 = await alice.discover("law", strategy="guided")
    if res2:
        print(f"  ✓ Bob 下线仍找到 {res2['found']['name']}，"
              f"路径={(' → '.join(p['name'] for p in res2['path']))}")
    # 恢复 Bob
    _DOWN.discard("Bob")
    REGISTRY["Bob"]._server = await asyncio.start_server(REGISTRY["Bob"]._handle, HOST, REGISTRY["Bob"].port)
    await asyncio.sleep(0.2)

    print("\n--- 2b. 回程断：Dave 命中后让 Bob 下线，看 Dave 是否绕行直连源 ---")
    orig_reply = REGISTRY["Dave"]._reply_back
    async def hook(resp, path):
        print(f"  [注入] Dave 命中，回程前让 Bob 下线")
        REGISTRY["Bob"].go_offline()
        await asyncio.sleep(0.1)
        await orig_reply(resp, path)
    REGISTRY["Dave"]._reply_back = hook
    res3 = await alice.discover("law", strategy="guided")
    if res3:
        print(f"  ✓ 回程断点扛住：找到 {res3['found']['name']}")
    REGISTRY["Dave"]._reply_back = orig_reply
    _DOWN.discard("Bob")
    REGISTRY["Bob"]._server = await asyncio.start_server(REGISTRY["Bob"]._handle, HOST, REGISTRY["Bob"].port)

    # ---- 阶段3：Sybil 防御 ----
    print("\n" + "=" * 72)
    print("【阶段3】Sybil 防御：弱连接不路由，傀儡进不了核心层")
    print("=" * 72)
    mallory_ports = [7101, 7102, 7103, 7104, 7105]
    puppets = [f"M{i+1}" for i in range(5)]
    for i, p in enumerate(mallory_ports):
        Agent(f"M{i+1}", p, ["law"])
    for pn in puppets:
        for qn in puppets:
            if pn != qn:
                REGISTRY[pn].knows(qn, REGISTRY[qn].port, ["law"], 0.4)
    for pn in puppets[:3]:
        alice.knows(pn, REGISTRY[pn].port, ["law"], 0.4)
    _set_degree_all()
    puppet_servers = await asyncio.gather(*[REGISTRY[n].serve() for n in puppets])
    await asyncio.sleep(0.1)
    weak = [n for n, a in alice.acq.items() if a.trust < ROUTE_TRUST_THRESHOLD and not a.blocked]
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
    removed = alice.decay(steps=DECAY_STEPS)
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
        alice.acq["Dave"].last_seen = tick()
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
    _set_degree_all()
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
    _set_degree_all()

    print("\n" + "=" * 72)
    print(" 全流程结束：发现→协作→拓扑演化→churn→Sybil→身份验证联动")
    print("=" * 72)
    for s in servers:
        s.close()
    await asyncio.gather(*[s.wait_closed() for s in servers], return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
