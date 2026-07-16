"""
agentnet_topology.py — Vouch 协议（拓扑来源与维护版）：明文版 + 熟人表的动态生命周期

在 agentnet.py（明文基础版）基础上，回答「我认识谁初始怎么写、会更新吗」：
当前从「写死的静态图」升级为「手填种子 → 发现扩展 → 协作反馈校准 → 衰减/拉黑」
四阶段动态生命周期。

核心机制：信任度随协作结果升降（把抽象的 0.4 锚定到真实协作结果）。

  · 手填种子   build_graph 只给极少的初始边（3 条），其余靠运行时长出来。
  · 发现即扩展 remember：找到陌生人 → 弱信任(0.4)加入熟人表（同前）。
  · 协作反馈   collaborate：发任务→收结果→根据「质量分」调信任度、扩展标签、
               刷新 last_seen。这是让网络「越用越准」的唯一来源。
  · 不活跃衰减 decay：长时间无协作 → trust 软化（模拟关系变淡）。
  · 自动拉黑   trust 跌破阈值 → 从熟人表移除（关系失效）。

信任度更新公式（信任难建易毁）：
  成功: trust += α·(1 - trust)      α=0.1  逼近 1 但不骤变
  失败: trust -= β·trust             β=0.3  惩罚更快
  衰减: trust *= (1 - γ)             γ=0.05 每周期
  拉黑: trust < THRESH(0.2) → 移除

协作成败判定（原型简化）：
  目标响应且产物非空 + 质量分高 → 成功（升 trust）
  目标响应但质量分低 / 超时无响应 → 失败（降 trust）
  真实系统的「质量分」来自应用层（用户反馈 / 结果校验），这里用目标自报质量分模拟。

先不管隐私版：明文路径、明文 found、无签名验证。focus 在拓扑生命周期。

零依赖，仅标准库。运行：python3 agentnet_topology.py
"""
from __future__ import annotations
import asyncio
import json
from dataclasses import dataclass, field

HOST = "127.0.0.1"
DEFAULT_TTL = 6
GUIDED_FANOUT = 1

# 信任度更新参数
ALPHA = 0.1          # 成功协作增益
BETA = 0.3           # 恶意失败惩罚（响应了但质量差 / 多次 churn 失败）
GAMMA = 0.05         # 每衰减周期
BLOCK_THRESHOLD = 0.2   # 低于此值 → 拉黑移除
DECAY_STEPS = 3      # 每个周期代表「一段时间不互动」

# churn 容错参数（区分 churn vs 恶意失败）
COLLAB_RETRIES = 2         # 协作超时重试次数（临时抖动先给机会）
CHURN_PENALTY = 0.1        # churn 失败惩罚（远小于 BETA：临时掉线不该重罚）

# Sybil 防御参数
ROUTE_TRUST_THRESHOLD = 0.6  # 信任度低于此值的熟人【不参与路由】，只记录
                             # 核心：Mallory 的傀儡都是新面孔（弱信任），进不了路由核心层
INTRO_QUOTA = 2              # 每个熟人每周期最多「引荐」INTRO_QUOTA 个新面孔给我
                             # 卡住「伪造大量身份刷量」——傀儡再多，经一个熟人只能进来 N 个

RELATED = {
    "law":     frozenset({"law", "finance", "contract", "policy"}),
    "writing": frozenset({"writing", "editing", "blog", "translation"}),
    "python":  frozenset({"python", "backend", "data", "ml"}),
    "design":  frozenset({"design", "art", "ui", "brand"}),
    "finance": frozenset({"finance", "law", "accounting"}),
}

_COUNT = {}


def _bump(kind): _COUNT[kind] = _COUNT.get(kind, 0) + 1
def _counts(): return dict(_COUNT)
def _reset(): _COUNT.clear()


@dataclass
class Acquaintance:
    name: str
    port: int
    tags: set = field(default_factory=set)    # 可变集合（标签会扩展）
    trust: float = 0.8
    degree: int = 0
    last_seen: int = 0        # 最后一次协作的「逻辑时钟」步
    interactions: int = 0     # 累计协作次数
    blocked: bool = False    # 是否被拉黑（保留记录，不参与路由）
    intro_count: int = 0     # 本周期已「引荐」给我多少新面孔（Sybil 引荐名额）


REGISTRY = {}
_CLOCK = [0]   # 全局逻辑时钟（演示用；真实系统用墙钟时间）


def tick():
    _CLOCK[0] += 1
    return _CLOCK[0]


class Agent:
    def __init__(self, name, port, caps, quality_fn=None):
        self.name = name
        self.port = port
        self.caps = frozenset(caps)
        self.acq: dict = {}
        self._seen: set = set()
        self._pending: dict = {}
        self._qctr = 0
        self.tag = f"[{name}@{port}]"
        # quality_fn: 本智能体接任务时，返回「成品 + 质量分(0~1)」。
        # 默认高质量；可在 build_graph 里让某些节点「坑」（低质量）演示信任下降。
        self._quality_fn = quality_fn or (lambda task: (f"{self.name} 完成了「{task}」", 0.9))
        REGISTRY[name] = self

    def knows(self, other_name, port, tags, trust=0.8):
        self.acq[other_name] = Acquaintance(other_name, port, set(tags), trust)

    def _name_of_port(self, port):
        for a in self.acq.values():
            if a.port == port and not a.blocked:
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

    # ---------- 发起发现 ----------
    async def discover(self, capability, strategy="guided", ttl=DEFAULT_TTL):
        qid = self._next_qid(); self._seen.add(qid)
        fut = asyncio.get_running_loop().create_future()
        self._pending[qid] = fut
        msg = {"type": "query", "mode": "discover", "capability": capability,
               "strategy": strategy, "ttl": ttl, "query_id": qid,
               "path": [{"name": self.name, "port": self.port}]}
        print(f"\n{self.tag} 发起 discover(cap={capability}, strat={strategy})")
        await self._forward(msg)
        return await self._await(qid)

    def _next_qid(self):
        q = f"{self.name}-{self._qctr}"; self._qctr += 1; return q

    async def _await(self, qid, timeout=8):
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
        if msg["mode"] == "discover" and msg["capability"] in self.caps:
            print(f"{self.tag} ✓ 命中！路径={' → '.join(p['name'] for p in path)}")
            resp = {"type": "response", "query_id": qid, "path": path,
                    "found": {"name": self.name, "port": self.port, "caps": sorted(self.caps)}}
            await self._reply_back(resp, path)
            return
        if msg["ttl"] <= 0:
            return
        msg2 = dict(msg); msg2["path"] = path; msg2["ttl"] = msg["ttl"] - 1
        await self._forward(msg2)

    async def _forward(self, msg):
        # Sybil 防御核心：弱连接（trust < ROUTE_TRUST_THRESHOLD）不参与路由。
        # Mallory 的傀儡都是新面孔（弱信任），进不了路由核心层。
        cands = [a for a in self.acq.values() if not a.blocked
                 and a.trust >= ROUTE_TRUST_THRESHOLD]
        if not cands:
            return
        ports = ([a.port for a in cands] if msg.get("strategy") == "flood"
                  else self._guided_pick(msg, cands))
        names = [self._name_of_port(p) for p in ports]
        weak = [n for n, a in self.acq.items() if not a.blocked and a.trust < ROUTE_TRUST_THRESHOLD]
        print(f"{self.tag} 转发(ttl={msg['ttl']}, strat={msg.get('strategy')}) → {names}"
              + (f"  [弱连接不路由: {weak}]" if weak else ""))
        for p in ports:
            await self._send(p, msg)

    def _guided_pick(self, msg, cands):
        cap = msg.get("capability")
        rel = RELATED.get(cap, frozenset({cap}) if cap else frozenset())
        visited = {p["name"] for p in msg["path"]}
        cands = [a for a in cands if a.name not in visited]
        if not cands:
            return []
        max_deg = max(a.degree for a in cands) or 1
        scored = []
        for a in cands:
            tag = len(a.tags & rel)
            # 桥梁度：只数「强连接」熟人（抗 Sybil——傀儡互抬的虚高 degree 失效）
            hub = 0.3 * (a.degree / max_deg)
            trust_w = 0.2 * a.trust
            scored.append((tag + hub + trust_w, a.trust, a.port))
        scored.sort(reverse=True)
        return [p for _, _, p in scored[:GUIDED_FANOUT]]

    async def _reply_back(self, resp, path):
        if len(path) < 2:
            self._deliver(resp); return
        await self._send(path[-2]["port"], resp)

    async def _on_response(self, msg):
        path = msg["path"]
        if path[0]["name"] == self.name:
            self._deliver(msg); return
        names = [p["name"] for p in path]
        if self.name not in names:
            return
        i = names.index(self.name)
        if i == 0:
            return
        await self._send(path[i - 1]["port"], msg)

    def _deliver(self, resp):
        path = resp["path"]
        # 介绍人 = path 倒数第二跳（把目标介绍给我的那个人）
        introducer = path[-2]["name"] if len(path) >= 2 else None
        print(f"{self.tag} 收到结果：找到 {resp['found']['name']} "
              f"路径={' → '.join(p['name'] for p in path)} 介绍人={introducer}")
        f = self._pending.get(resp["query_id"])
        if f and not f.done():
            f.set_result({"found": resp["found"], "path": path, "introducer": introducer})

    # ---------- 协作 + 反馈（核心新增）----------
    async def collaborate(self, found, task):
        """发现到目标后，发起协作，并根据结果调整对该熟人的信任度/标签。

        关键：区分 churn 失败 vs 恶意失败：
          · 超时无响应 → 可能是临时 churn → 先重试 N 次，都失败才按 churn 轻罚
          · 响应了但质量差 → 是真坑 → 按恶意重罚
        这样临时抖动的好熟人不被误拉黑。"""
        name = found["name"]; port = found["port"]
        print(f"{self.tag} 向 {name} 发起协作：「{task}」")
        acq = self.acq.get(name)
        if acq is None:
            self.remember(found); acq = self.acq[name]
        before = acq.trust

        outcome = None
        for attempt in range(1, COLLAB_RETRIES + 2):   # 1 + COLLAB_RETRIES 次
            outcome = await self._send_task(port, task)
            if outcome is not None:
                break
            if attempt <= COLLAB_RETRIES:
                print(f"  {self.tag} 超时（可能是 churn），重试 {attempt}/{COLLAB_RETRIES}")

        if outcome is None:
            # 多次超时 → 判为 churn 失败（长期离线或网络差），轻罚
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
        # 标签扩展：从 found.caps 学到对方的新能力（标签会越积越准）
        acq.tags |= set(found.get("caps", []))
        # 信任度：质量高升得多，质量一般升得慢，质量差按失败惩罚
        if quality >= 0.7:
            acq.trust += ALPHA * (1 - acq.trust)            # 好：升
        elif quality >= 0.4:
            acq.trust += 0.3 * ALPHA * (1 - acq.trust)      # 一般：微升
        else:
            acq.trust -= BETA * acq.trust                    # 差：按失败惩罚（难建易毁）
            if acq.trust < BLOCK_THRESHOLD:
                acq.blocked = True
        acq.trust = max(0.0, min(1.0, acq.trust))

    def _on_churn_fail(self, acq):
        """churn 失败（多次超时，长期离线）：轻罚，可能拉黑。"""
        acq.trust -= CHURN_PENALTY * acq.trust
        if acq.trust < BLOCK_THRESHOLD:
            acq.blocked = True

    def _on_collab_fail(self, acq):
        """明确的恶意失败（保留接口；当前恶意场景在 _on_collab_success 里按质量处理）。"""
        acq.trust -= BETA * acq.trust
        if acq.trust < BLOCK_THRESHOLD:
            acq.blocked = True

    async def _send_task(self, port, task, timeout=5):
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
            try:
                w.close()
            except Exception:
                pass

    # ---------- 不活跃衰减 ----------
    def decay(self, steps=DECAY_STEPS):
        """模拟「一段时间过去」：所有熟人按不活跃程度衰减信任。"""
        now = tick()
        removed = []
        for name, a in list(self.acq.items()):
            idle = now - a.last_seen
            # 越久没协作，衰减越多
            for _ in range(min(idle, steps)):
                if a.blocked:
                    break
                a.trust *= (1 - GAMMA)
            if a.trust < BLOCK_THRESHOLD and not a.blocked:
                a.blocked = True
                removed.append(name)
        return removed

    # ---------- 发现即扩展 ----------
    def remember(self, found, trust=0.4, introducer=None):
        """把发现到的目标加入熟人表。
        Sybil 防御：introducer（路径上的介绍人）每周期引荐名额 INTRO_QUOTA。
        超额则拒绝接受此新面孔——卡住「伪造大量身份刷量」。
        """
        if found["name"] in self.acq:
            return False
        # 检查介绍人的引荐名额
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


def build_graph():
    """刻意从稀疏开始：只有 3 条手填种子边，其余靠运行时长出来。"""
    specs = [
        ("Alice", 7001, ["python", "backend"]),
        ("Bob",   7002, ["python", "design"]),
        ("Carol", 7003, ["design", "art"]),
        ("Dave",  7004, ["law", "finance"]),
        ("Eve",   7005, ["law", "writing"]),
        ("Frank", 7006, ["art", "design"]),
        ("Grace", 7007, ["writing", "editing"]),
    ]
    # quality_fn：让某些节点「坑」（低质量），演示信任下降
    def good(task):  return (f"{task}→成品@好", 0.9)
    def shaky(task): return (f"{task}→成品@一般", 0.5)
    def bad(task):   return (f"{task}→成品@差", 0.1)
    agents = []
    for n, p, c in specs:
        if n == "Dave":
            a = Agent(n, p, c, quality_fn=shaky)      # Dave 质量一般
        elif n == "Eve":
            a = Agent(n, p, c, quality_fn=bad)        # Eve 质量差（坑）
        else:
            a = Agent(n, p, c, quality_fn=good)
        agents.append(a)
    # 仅 3 条种子边（手填冷启动）：Alice 只认识 Bob、Carol
    REGISTRY["Alice"].knows("Bob", REGISTRY["Bob"].port, ["python", "design"], 0.7)
    REGISTRY["Bob"].knows("Alice", REGISTRY["Alice"].port, ["python"], 0.7)
    REGISTRY["Bob"].knows("Dave", REGISTRY["Dave"].port, ["law", "finance"], 0.6)
    for ag in REGISTRY.values():
        for name, acq in ag.acq.items():
            # degree 只数「强连接」熟人：抗 Sybil。
            # 否则 Mallory 的傀儡互抬会让 degree 虚高，桥梁度评分被污染。
            other = REGISTRY.get(name)
            if other:
                acq.degree = sum(1 for x in other.acq.values()
                                 if x.trust >= ROUTE_TRUST_THRESHOLD)
            acq.last_seen = tick()
    return list(REGISTRY.values())


def _set_degree_all():
    """重新计算所有 degree（只数强连接）。加边后调用。"""
    for ag in REGISTRY.values():
        for name, acq in ag.acq.items():
            other = REGISTRY.get(name)
            if other:
                acq.degree = sum(1 for x in other.acq.values()
                                 if x.trust >= ROUTE_TRUST_THRESHOLD)


async def main():
    print("=" * 72)
    print(" Vouch 协议 — 拓扑来源与维护版（明文 + 熟人表动态生命周期）")
    print(" 手填种子 → 发现扩展 → 协作反馈校准 → 衰减/拉黑")
    print("=" * 72)
    agents = build_graph()
    alice = REGISTRY["Alice"]
    servers = await asyncio.gather(*[a.serve() for a in agents])

    print("\n【阶段0】初始拓扑（仅手填种子，很稀疏）：")
    for a in agents:
        acq_s = ", ".join(f"{n}(trust={x.trust:.2f},tags={sorted(x.tags)})" for n, x in a.acq.items())
        print(f"  {a.tag} 熟人=[{acq_s or '空'}]")

    # ---- 阶段1：发现扩展 ----
    print("\n" + "=" * 72)
    print("【阶段1】发现扩展：Alice 通过 Bob 发现 Dave，记住他")
    print("=" * 72)
    res = await alice.discover("law", strategy="guided")
    if res and res.get("found"):
        f = res["found"]
        alice.remember(f)
        print(f"  → Alice 现在认识 {f['name']}（弱信任 0.4）。熟人表变长了。")

    # ---- 阶段2：协作反馈 —— 和 Dave 协作（质量一般 0.5）----
    print("\n" + "=" * 72)
    print("【阶段2】协作反馈校准：信任度随协作结果升降")
    print("=" * 72)
    print("\n--- 2a. 和 Dave 协作（Dave 质量一般 0.5，预期 trust 微升）---")
    dave_found = {"name": "Dave", "port": REGISTRY["Dave"].port, "caps": ["law", "finance"]}
    for i in range(3):
        await alice.collaborate(dave_found, "看合同")
    print(f"\n  Alice 对 Dave 的信任：0.40 → {alice.acq['Dave'].trust:.2f}（质量一般，缓慢上升）")

    # --- 2b. 让 Alice 发现并多次协作 Eve（Eve 质量差 0.1）---
    print("\n--- 2b. 发现 Eve，多次协作（Eve 质量差 0.1，预期 trust 跌、被拉黑）---")
    print("  注：当前稀疏种子图里 Bob 只通到 law 圈，没有到 writing 圈的路，")
    print("     所以 discover('writing') 会超时——这正是稀疏冷启动的可达性局限。")
    print("     这里手动把 Eve 加进来以演示协作反馈机制本身。")
    eve = REGISTRY["Eve"]
    if "Eve" not in alice.acq:
        alice.knows("Eve", eve.port, ["writing"], 0.4)
        alice.acq["Eve"].last_seen = tick()
    for i in range(3):
        await alice.collaborate({"name": "Eve", "port": eve.port, "caps": ["writing"]}, "写文案")
    print(f"\n  Alice 对 Eve 的信任：0.40 → {alice.acq['Eve'].trust:.2f}"
          f"{'（已被拉黑）' if alice.acq['Eve'].blocked else ''}")

    # --- 2c. churn vs 恶意失败的惩罚对比（区分机制演示）---
    print("\n--- 2c. churn 失败 vs 恶意失败的惩罚对比 ---")
    print("  一次 churn 失败（临时掉线，轻罚 CHURN_PENALTY=0.1）")
    churn_acq = Acquaintance("TempChurn", 0, {"x"}, 0.5, last_seen=tick())
    alice._on_churn_fail(churn_acq)
    print(f"    churn 一次：trust 0.50 → {churn_acq.trust:.2f}（轻罚，不拉黑）")
    mal_acq = Acquaintance("TempMal", 0, {"x"}, 0.5, last_seen=tick())
    alice._on_collab_fail(mal_acq)
    print(f"    恶意 一次：trust 0.50 → {mal_acq.trust:.2f}（重罚 BETA=0.3，难建易毁）")
    print(f"  → 同样 0.50 起点，churn 一次降到 {churn_acq.trust:.2f}，恶意一次降到 {mal_acq.trust:.2f}")
    print("    临时抖动的好熟人不被误伤，长期 churn 累积才会拉黑。")

    # ---- 场景4：Sybil 防御 ----
    print("\n" + "=" * 72)
    print("【场景4】Sybil 防御：弱连接不路由，傀儡进不了路由核心层")
    print("=" * 72)
    # 先把 Dave 的信任通过好协作刷高（≥0.6，可路由）——真熟人才有路由权
    dave = REGISTRY["Dave"]
    if "Dave" in alice.acq and alice.acq["Dave"].trust < ROUTE_TRUST_THRESHOLD:
        for _ in range(5):
            alice._on_collab_success(alice.acq["Dave"],
                {"caps": ["law"]}, 0.9)   # 假装高质量协作，把 Dave trust 攒上去
        print(f"  Dave 经多次好协作攒到 trust={alice.acq['Dave'].trust:.2f}（≥{ROUTE_TRUST_THRESHOLD}，可路由）")

    # Mallory 造一批傀儡：标签匹配 law、互相认识（degree 虚高）、但弱信任 0.4
    print("\n  Mallory 造 5 个傀儡（标签匹配 law、互抬 degree、弱信任 0.4）")
    mallory_ports = [7101, 7102, 7103, 7104, 7105]
    puppets = []
    for i, p in enumerate(mallory_ports):
        Agent(f"M{i+1}", p, ["law"])         # 傀儡都声称懂 law
        puppets.append(f"M{i+1}")
    # 傀儡互相认识（虚高 degree + 互抬信任）
    for pn in puppets:
        for qn in puppets:
            if pn != qn:
                REGISTRY[pn].knows(qn, REGISTRY[qn].port, ["law"], 0.4)
    # 让 Alice 经 Bob 认识几个傀儡（弱信任 0.4）
    for pn in puppets[:3]:
        alice.knows(pn, REGISTRY[pn].port, ["law"], 0.4)
    _set_degree_all()
    weak = [n for n, a in alice.acq.items() if a.trust < ROUTE_TRUST_THRESHOLD and not a.blocked]
    strong = [n for n, a in alice.acq.items() if a.trust >= ROUTE_TRUST_THRESHOLD and not a.blocked]
    print(f"  Alice 熟人 → 可路由(强): {strong}  不可路由(弱): {weak}")
    print(f"  （傀儡 M1/M2/M3 虽标签匹配 law，但 trust=0.4 < {ROUTE_TRUST_THRESHOLD}，不参与路由）")

    # 启动傀儡 server
    puppet_servers = await asyncio.gather(*[REGISTRY[n].serve() for n in puppets])
    await asyncio.sleep(0.1)

    print("\n  Alice discover('law')：看是否绕开傀儡找到真目标 Dave")
    # 清 seen 让能重新转发（query_id 不同了，但 path visited 会重算）
    alice._seen.clear()
    res = await alice.discover("law", strategy="guided")
    if res:
        print(f"  ✓ 找到 {res['found']['name']}，路径={(' → '.join(p['name'] for p in res['path']))}")
        print("  → guided 只在强连接里选，傀儡(弱信任)被排除，路由未被污染。")
    else:
        print("  ✗ 未找到（Dave 可能已被直连缓存跳过）")
    print("\n  [对照] 若关闭 Sybil 防御（阈值=0），傀儡 M* 因标签匹配 law 会被选中转发，")
    print("        路由被污染风险升高——这正是「弱连接不路由」要防的。")

    for s in puppet_servers:
        s.close()
    await asyncio.gather(*[s.wait_closed() for s in puppet_servers], return_exceptions=True)

    # ---- 阶段3：不活跃衰减 ----
    print("\n" + "=" * 72)
    print("【阶段3】不活跃衰减：模拟「一段时间过去」，关系变淡")
    print("=" * 72)
    # 先把 Dave 标记为「很久没协作」
    alice.acq["Dave"].last_seen = 0
    before = alice.acq["Dave"].trust
    removed = alice.decay(steps=DECAY_STEPS)
    print(f"  Dave（很久没互动）trust {before:.2f}→{alice.acq['Dave'].trust:.2f}")
    if removed:
        print(f"  被拉黑：{removed}")

    print("\n【最终】Alice 的熟人表（动态维护后）：")
    for n, a in alice.acq.items():
        print(f"  {n}: trust={a.trust:.2f} tags={sorted(a.tags)} "
              f"次数={a.interactions} {'[拉黑]' if a.blocked else ''}")

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
