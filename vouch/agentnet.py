"""
agentnet.py — Vouch 协议（明文基础版）：信任受限熟人图上的多跳发现

每个智能体 = 一个异步 TCP 服务器（监听一个端口），只存储自己「认识的人」
（熟人表：名字 / 地址 / 语义标签 / 信任度 / 连接度估计）。源智能体 A 通过多跳
转发在熟人图上完成：
  · discover  ——「找一个能做 X 的人」（能力发现）
  · lookup   ——「找某个具体的人」（身份查找）
命中后，A 直接向目标发送任务并收回产物，形成「发现 + 协作」闭环。
成功发现还会把目标以弱信任加入熟人表，演示「发现即扩展网络 / 路径缓存」。

转发策略对照（分析中的核心张力）：
  · guided（引导式贪心）—— 按「语义相关度 + 桥梁度」挑 top-k 熟人转发
  · flood （洪泛）       —— 向所有熟人广播
两种策略的消息复杂度差异在运行末尾打印：引导式 ≈ O(路径长)，洪泛 ≈ O(节点数)。

环路防止：每个智能体记录已处理 query_id，重复即丢弃。TTL 同时是失控兜底。
响应回传：路径里每个跳点带地址，所以哪怕回头链是非对称的也能原路返回
（你收到过我的查询，就等于我留了回信地址）。

零依赖，仅用 Python 标准库。运行：python3 agentnet.py
"""
from __future__ import annotations
import asyncio
import json
from dataclasses import dataclass

HOST = "127.0.0.1"

# 能力 → 相关标签集（引导式路由的「线索」；真实系统换成向量相似度）
RELATED = {
    "law":     frozenset({"law", "finance", "contract", "policy"}),
    "writing": frozenset({"writing", "editing", "blog", "translation"}),
    "python":  frozenset({"python", "backend", "data", "ml"}),
    "design":  frozenset({"design", "art", "ui", "brand"}),
    "finance": frozenset({"finance", "law", "accounting"}),
}
GUIDED_FANOUT = 1          # 引导式每跳转发给几个熟人；调大=更鲁棒但消息更多
DEFAULT_TTL   = 6          # 跳数预算

# ---- 全局消息计数，便于对比两种策略 ----
_COUNT = {}


def _bump(kind): _COUNT[kind] = _COUNT.get(kind, 0) + 1
def _counts():   return dict(_COUNT)
def _reset():    _COUNT.clear()


@dataclass
class Acquaintance:
    name: str
    port: int
    tags: frozenset
    trust: float = 0.8
    degree: int = 0          # 我对该熟人「连接度」的估计：桥梁度高 = 更可能是好跳板


REGISTRY = {}               # name -> Agent


class Agent:
    def __init__(self, name, port, caps):
        self.name = name
        self.port = port
        self.caps = frozenset(caps)
        self.acq: dict = {}                       # 熟人表
        self._seen: set = set()                   # 已处理 query_id（环路防止）
        self._pending: dict = {}                  # query_id -> Future
        self._qctr = 0
        self.tag = f"[{name}@{port}]"
        REGISTRY[name] = self

    def knows(self, other_name, port, tags, trust=0.8):
        self.acq[other_name] = Acquaintance(other_name, port, frozenset(tags), trust)

    def _name_of_port(self, port):
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
            await r.readline()          # 等 ack（fire-and-forget 的送达确认）
            w.close()
        except OSError as e:
            print(f"{self.tag} 连接 {port} 失败: {e!r}")

    # ---------- 发起查询 ----------
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

    async def lookup(self, target, hints=(), ttl=DEFAULT_TTL):
        qid = self._next_qid(); self._seen.add(qid)
        fut = asyncio.get_running_loop().create_future()
        self._pending[qid] = fut
        msg = {"type": "query", "mode": "lookup", "target": target,
               "strategy": "guided", "ttl": ttl, "query_id": qid,
               "hints": list(hints),
               "path": [{"name": self.name, "port": self.port}]}
        print(f"\n{self.tag} 发起 lookup(target={target}, hints={list(hints)})")
        await self._forward(msg)
        return await self._await(qid)

    def _next_qid(self):
        q = f"{self.name}-{self._qctr}"; self._qctr += 1; return q

    async def _await(self, qid):
        try:
            return await asyncio.wait_for(self._pending[qid], timeout=8)
        except asyncio.TimeoutError:
            print(f"{self.tag} 超时，未找到")
            return None
        finally:
            self._pending.pop(qid, None)

    # ---------- 收到查询 ----------
    async def _on_query(self, msg):
        qid = msg["query_id"]
        if qid in self._seen:                       # 环路防止：已处理就丢弃
            return
        self._seen.add(qid)
        path = msg["path"] + [{"name": self.name, "port": self.port}]
        hit = (msg["mode"] == "lookup" and msg["target"] == self.name) or \
              (msg["mode"] == "discover" and msg["capability"] in self.caps)
        if hit:
            print(f"{self.tag} ✓ 命中！我是目标  路径={' → '.join(p['name'] for p in path)}")
            resp = {"type": "response", "query_id": qid, "path": path,
                    "found": {"name": self.name, "port": self.port, "caps": sorted(self.caps)}}
            await self._reply_back(resp, path)
            return
        if msg["ttl"] <= 0:
            print(f"{self.tag} TTL 耗尽，停止")
            return
        msg2 = dict(msg); msg2["path"] = path; msg2["ttl"] = msg["ttl"] - 1
        await self._forward(msg2)

    # ---------- 转发决策：引导式 vs 洪泛 ----------
    async def _forward(self, msg):
        if not self.acq:
            return
        if msg.get("strategy") == "flood":
            ports = [a.port for a in self.acq.values()]
        else:
            ports = self._guided_pick(msg)
        names = [self._name_of_port(p) for p in ports]
        print(f"{self.tag} 转发(mode={msg['mode']}, ttl={msg['ttl']}, "
              f"strat={msg.get('strategy')}) → {names}")
        for p in ports:
            await self._send(p, msg)

    def _guided_pick(self, msg):
        """挑 top-k 熟人：语义相关度为主，无直接线索时偏向桥梁熟人。"""
        cap = msg.get("capability")
        hints = frozenset(msg.get("hints", ()))
        visited = {p["name"] for p in msg["path"]}
        cands = [a for a in self.acq.values() if a.name not in visited]
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
            hub = 0.3 * (a.degree / max_deg)       # 无线索时偏向「认识人多」的熟人
            scored.append((tag + hub, a.trust, a.port))
        scored.sort(reverse=True)
        return [p for _, _, p in scored[:GUIDED_FANOUT]]

    # ---------- 响应沿路径回传 ----------
    async def _reply_back(self, resp, path):
        if len(path) < 2:                          # 我就是源
            self._deliver(resp); return
        prev = path[-2]                            # 路径里我的上游
        await self._send(prev["port"], resp)       # 用路径里的地址，不依赖反向熟人

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
        prev = path[i - 1]
        await self._send(prev["port"], msg)

    def _deliver(self, resp):
        print(f"{self.tag} 收到结果：找到 {resp['found']['name']} "
              f"路径={' → '.join(p['name'] for p in resp['path'])}")
        f = self._pending.get(resp["query_id"])
        if f and not f.done():
            f.set_result(resp)

    # ---------- 协作（任务）----------
    async def send_task(self, port, task):
        _bump("task")
        r, w = await asyncio.open_connection(HOST, port)
        w.write((json.dumps({"type": "task", "from": self.name, "task": task}) + "\n").encode())
        await w.drain()
        line = await r.readline(); w.close()
        return json.loads(line.decode())["result"]

    def _do_task(self, msg):
        return f"{self.name}（能力={sorted(self.caps)}）完成了「{msg['task']}」→ 成品@{self.name}"

    # ---------- 发现即扩展网络 ----------
    def remember(self, found, trust=0.4):
        if found["name"] not in self.acq:
            self.acq[found["name"]] = Acquaintance(found["name"], found["port"],
                                                  frozenset(found["caps"]), trust, degree=1)
            return True
        return False


def build_graph():
    """三个语义簇（tech / law / art-writing）+ 桥梁熟人，构成小世界社交图。"""
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
    edges = [  # (from, to, tags, trust)
        ("Alice", "Bob",   ["python", "design"], 0.9),
        ("Alice", "Carol", ["design", "art"],    0.6),
        ("Bob",   "Alice", ["python"],           0.9),
        ("Bob",   "Carol", ["design"],           0.6),
        ("Bob",   "Dave",  ["law", "finance"],   0.7),   # tech↔law 桥梁
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
    # 用真实连接度填充 degree（我对自己熟人「有多人脉」的估计）
    for ag in REGISTRY.values():
        for name, acq in ag.acq.items():
            acq.degree = len(REGISTRY[name].acq)
    return list(REGISTRY.values())


async def main():
    print("=" * 72)
    print(" 个人智能体发现与协作原型 — 熟人图上的多跳路由")
    print(" 关注：guided 走一条短路径，flood 触达几乎所有节点；发现后可直接协作")
    print("=" * 72)
    agents = build_graph()
    alice = REGISTRY["Alice"]
    servers = await asyncio.gather(*[a.serve() for a in agents])

    print("\n拓扑（caps=自身能力, 熟人=[名字 tags 连接度]）：")
    for a in agents:
        acq_str = ", ".join(f"{n}(tags={sorted(x.tags)},deg={x.degree})" for n, x in a.acq.items())
        print(f"  {a.tag} caps={sorted(a.caps)}  熟人=[{acq_str}]")

    # ---- 场景 1：引导式 discover「懂 law 的人」----
    _reset()
    res = await alice.discover("law", strategy="guided")
    print(f"\n[复杂度] guided  消息数: {_counts()}")

    # ---- 场景 2：洪泛 discover 同一查询，对照消息数 ----
    _reset()
    await alice.discover("law", strategy="flood")
    print(f"\n[复杂度] flood   消息数: {_counts()}")
    for a in agents:
        a._seen.clear()  # 清掉洪泛的 seen，不影响后续

    # ---- 场景 3：lookup 找 Grace（带语义线索）----
    await alice.lookup("Grace", hints=("writing", "editing"))

    # ---- 场景 4：发现 → 直接协作（给找到的人发任务）----
    if res and res.get("found"):
        f = res["found"]
        new = alice.remember(f)   # 发现即扩展网络：把目标以弱信任加入熟人表
        print(f"\n{alice.tag} 现在直接认识 {f['name']} 了吗？"
              f"{'是（弱信任 0.4 加入熟人表）' if new else '已是熟人'}")
        out = await alice.send_task(f["port"], "帮我起草一份雇佣合同要点")
        print(f"{alice.tag} 协作产物 ← {out}")

        # 第二次再 discover：现在 Alice→目标已是直连，应 1 跳命中
        _reset()
        print("\n第二次 discover('law')，演示「路径缓存后近 O(1)」：")
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
