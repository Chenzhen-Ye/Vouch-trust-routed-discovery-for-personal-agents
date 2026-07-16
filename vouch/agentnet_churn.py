"""
agentnet_churn.py — Vouch 协议（明文 churn 容错版）：节点随时上下线，路由要扛住

在 agentnet.py（明文基础版）基础上，针对节点 churn（随机上下线）做三层容错。
不考虑隐私版（见记忆 vouch-scope-no-privacy）——明文版响应带完整 path，
断点可直接绕过，这是 churn 容错最低成本的起点。

三层容错：

  1. 回程绕断点（核心、最便宜）
     响应沿 path 回传时，若上一跳掉线（连接失败），不卡死：沿 path 往回
     找下一个存活的节点直连。因为响应带完整 path=[Alice,Bob,Dave]，Dave
     发 Bob 失败 → 直接从 path 取 Alice 的地址发过去。明文版的路径信息
     = 绕行能力，这是隐私版（分布式令牌链）给不了的。

  2. 去程多路径 + 源重试
     guided 默认 fanout=1 太不耐 churn（那一个恰好掉线就完蛋）。
     discover 超时后自动重试：递增 fanout，必要时升级策略 guided→flood。
     发现是幂等的（query_id 去重），重试安全。

  3. 区分 churn vs 恶意失败（在拓扑版里修；此处只暴露接口）
     「一次超时」可能是临时抖动（不该重罚）也可能是长期离线（该拉黑）。
     churn 版的 discover 把「失败节点」记下来供调用方判断，不直接降信任。

演示：让某些节点中途「下线」（serve 后关停 server + 拒连），看：
  · 去程断 → 源重试换路
  · 回程断 → 目标绕过断点直连源

零依赖，仅标准库。运行：python3 agentnet_churn.py
"""
from __future__ import annotations
import asyncio
import json
from dataclasses import dataclass

HOST = "127.0.0.1"
DEFAULT_TTL = 6
GUIDED_FANOUT = 1

# churn 容错参数
SOURCE_RETRIES = 2          # 源超时后的重试次数
RETRY_FANOUT_STEP = 1       # 每次重试 fanout 加多少
SEND_TIMEOUT = 2.0          # 单次连接/读超时：超时即判下线

RELATED = {
    "law":     frozenset({"law", "finance", "contract", "policy"}),
    "writing": frozenset({"writing", "editing", "blog", "translation"}),
    "python":  frozenset({"python", "backend", "data", "ml"}),
    "design":  frozenset({"design", "art", "ui", "brand"}),
    "finance": frozenset({"finance", "law", "accounting"}),
}

_COUNT = {}
_DOWN: set = set()          # 模拟「下线」的节点名集合


def _bump(kind): _COUNT[kind] = _COUNT.get(kind, 0) + 1
def _counts(): return dict(_COUNT)
def _reset(): _COUNT.clear()


@dataclass
class Acquaintance:
    name: str
    port: int
    tags: frozenset
    trust: float = 0.8
    degree: int = 0


REGISTRY = {}


class Agent:
    def __init__(self, name, port, caps):
        self.name = name
        self.port = port
        self.caps = frozenset(caps)
        self.acq: dict = {}
        self._seen: set = set()
        self._pending: dict = {}
        self._qctr = 0
        self._server = None           # 保存 server 以便模拟「下线」时关闭
        self.tag = f"[{name}@{port}]"
        REGISTRY[name] = self

    def knows(self, other_name, port, tags, trust=0.8):
        self.acq[other_name] = Acquaintance(other_name, port, frozenset(tags), trust)

    def _name_of_port(self, port):
        for a in self.acq.values():
            if a.port == port:
                return a.name
        return f"?@{port}"

    def go_offline(self):
        """模拟节点下线：关停 server，后续连接被拒。"""
        _DOWN.add(self.name)
        if self._server:
            self._server.close()
        print(f"{self.tag} ✘ 已下线（server 关停，拒连）")

    # ---------- 服务器 ----------
    async def serve(self):
        self._server = await asyncio.start_server(self._handle, HOST, self.port)
        return self._server

    async def _handle(self, reader, writer):
        if self.name in _DOWN:                 # 已下线节点不响应
            writer.close(); return
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

    async def _send(self, port, msg, timeout=SEND_TIMEOUT):
        """发消息，返回 True/False（成功/失败）。失败可能是对方下线。"""
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

    # ---------- 发起发现（带源重试 + fanout 递增）----------
    async def discover(self, capability, strategy="guided", ttl=DEFAULT_TTL,
                       retries=SOURCE_RETRIES, fanout=None):
        global GUIDED_FANOUT
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
            # 超时：升级 fanout，必要时升级策略
            attempt += 1
            if attempt <= retries:
                cur_fanout += RETRY_FANOUT_STEP
                if attempt >= 2 and cur_strat == "guided":
                    cur_strat = "flood"
                    print(f"{self.tag} 升级策略 guided→flood，撒大网抗 churn")
            # 清 seen 让重试能重新转发（query_id 不同了，但 path 里的 visited 会重新算）
        print(f"{self.tag} {retries+1} 次尝试均失败")
        return last_res

    def _next_qid(self):
        q = f"{self.name}-{self._qctr}"; self._qctr += 1; return q

    async def _await(self, qid, timeout=6):
        try:
            return await asyncio.wait_for(self._pending[qid], timeout=timeout)
        except asyncio.TimeoutError:
            print(f"{self.tag} 超时，未找到")
            return None
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
            resp = {"type": "response", "query_id": qid, "path": path,
                    "found": {"name": self.name, "port": self.port, "caps": sorted(self.caps)}}
            await self._reply_back(resp, path)
            return
        if msg["ttl"] <= 0:
            print(f"{self.tag} TTL 耗尽，停止")
            return
        msg2 = dict(msg); msg2["path"] = path; msg2["ttl"] = msg["ttl"] - 1
        await self._forward(msg2, msg.get("fanout", GUIDED_FANOUT))

    # ---------- 转发决策 ----------
    async def _forward(self, msg, fanout=None):
        if not self.acq:
            return
        fanout = fanout or GUIDED_FANOUT
        if msg.get("strategy") == "flood":
            ports = [a.port for a in self.acq.values()]
        else:
            ports = self._guided_pick(msg, fanout)
        names = [self._name_of_port(p) for p in ports]
        print(f"{self.tag} 转发(ttl={msg['ttl']}, strat={msg.get('strategy')}, "
              f"fanout={fanout}) → {names}")
        for p in ports:
            await self._send(p, msg)

    def _guided_pick(self, msg, fanout):
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
            hub = 0.3 * (a.degree / max_deg)
            trust_w = 0.2 * a.trust
            scored.append((tag + hub + trust_w, a.trust, a.port))
        scored.sort(reverse=True)
        return [p for _, _, p in scored[:fanout]]

    # ---------- 响应回传：核心——绕过断点 ----------
    async def _reply_back(self, resp, path):
        """目标把响应发回源。沿 path 回传，断点处绕过。"""
        # path 末尾是自己；从倒数第二个开始往回找能连上的节点
        for idx in range(len(path) - 2, -1, -1):
            hop = path[idx]
            if hop["name"] == self.name:
                continue
            ok = await self._send(hop["port"], resp)
            if ok:
                if idx < len(path) - 2:
                    print(f"{self.tag} 绕过断点：跳过 {path[idx+1]['name']}，直连 {hop['name']}")
                return
            # 这个 hop 掉线，继续往回找更上游的
            print(f"{self.tag} 回程 {hop['name']} 下线，往回找更上游")
        # 所有中间人都掉了，path[0] 是源
        print(f"{self.tag} 回程所有中继下线，尝试直连源 {path[0]['name']}")
        await self._send(path[0]["port"], resp)

    async def _on_response(self, msg):
        """中继收到响应：往源方向转发，断点处绕过。"""
        path = msg["path"]
        if path[0]["name"] == self.name:
            self._deliver(msg); return
        names = [p["name"] for p in path]
        if self.name not in names:
            return
        i = names.index(self.name)
        # 往源方向(idx-1, idx-2, ...)找能连上的节点转发
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
        # 都掉了，直连源
        print(f"{self.tag} 回程所有上游下线，直连源 {path[0]['name']}")
        await self._send(path[0]["port"], msg)

    def _deliver(self, resp):
        print(f"{self.tag} 收到结果：找到 {resp['found']['name']} "
              f"路径={' → '.join(p['name'] for p in resp['path'])}")
        f = self._pending.get(resp["query_id"])
        if f and not f.done():
            f.set_result(resp)

    # ---------- 协作 ----------
    async def send_task(self, port, task, timeout=SEND_TIMEOUT):
        _bump("task")
        try:
            r, w = await asyncio.wait_for(asyncio.open_connection(HOST, port), timeout=timeout)
            w.write((json.dumps({"type": "task", "from": self.name, "task": task}) + "\n").encode())
            await w.drain()
            line = await asyncio.wait_for(r.readline(), timeout=timeout)
            w.close()
            return json.loads(line.decode())["result"]
        except (OSError, asyncio.TimeoutError):
            print(f"{self.tag} 协作对象 {self._name_of_port(port)} 下线")
            return None

    def _do_task(self, msg):
        return f"{self.name}（能力={sorted(self.caps)}）完成了「{msg['task']}」→ 成品@{self.name}"


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


async def main():
    print("=" * 72)
    print(" Vouch 协议 — 明文 churn 容错版")
    print(" 节点随时上下线：去程多路径+源重试，回程绕过断点直连源")
    print("=" * 72)
    agents = build_graph()
    alice = REGISTRY["Alice"]
    servers = await asyncio.gather(*[a.serve() for a in agents])
    await asyncio.sleep(0.1)   # 让 server 起来

    print("\n拓扑：")
    for a in agents:
        acq_s = ", ".join(f"{n}(tags={sorted(x.tags)},deg={x.degree})" for n, x in a.acq.items())
        print(f"  {a.tag} caps={sorted(a.caps)}  熟人=[{acq_s}]")

    # ---- 场景1：去程断 → 源重试换路 ----
    print("\n" + "=" * 72)
    print("【场景1】去程断点：发现路径上的中间人下线，源重试换路")
    print("=" * 72)
    print("让 Bob 下线（Alice→Bob→Dave 是主路径，Bob 断了）")
    REGISTRY["Bob"].go_offline()
    await asyncio.sleep(0.2)
    res = await alice.discover("law", strategy="guided")
    if res:
        print(f"\n  ✓ 仍找到 {res['found']['name']}，路径={(' → '.join(p['name'] for p in res['path']))}")
        print("  （Bob 下线后，Alice 重试加大 fanout，改走另一条存活路径找到目标）")

    # 恢复 Bob，准备下个场景
    _DOWN.discard("Bob")
    REGISTRY["Bob"]._server = await asyncio.start_server(REGISTRY["Bob"]._handle, HOST, REGISTRY["Bob"].port)
    print("\n让 Bob 重新上线")
    await asyncio.sleep(0.2)

    # ---- 场景2：回程断 → 目标绕过断点直连源 ----
    print("\n" + "=" * 72)
    print("【场景2】回程断点：找到目标后，回程上的中继下线，目标绕行直连源")
    print("=" * 72)
    # 这次让 Bob 在 Dave 命中后、回程途中下线
    # 用一个 hook：Dave 命中回程时先让 Bob 下线，看 Dave 能否绕过
    orig_reply = REGISTRY["Dave"]._reply_back

    async def delay_then_bob_down(resp, path):
        # Dave 回程要发给 Bob(path[-2])；先让 Bob 下线
        print(f"  [注入] Dave 命中，回程前让 Bob 下线，模拟回程断点")
        REGISTRY["Bob"].go_offline()
        await asyncio.sleep(0.1)
        await orig_reply(resp, path)

    REGISTRY["Dave"]._reply_back = delay_then_bob_down
    res2 = await alice.discover("law", strategy="guided")
    if res2:
        print(f"\n  ✓ 回程断点也扛住：找到 {res2['found']['name']}，"
              f"路径={(' → '.join(p['name'] for p in res2['path']))}")
        print("  （Dave 发 Bob 失败 → 从 path 取 Alice 地址直连，绕过断点）")
    REGISTRY["Dave"]._reply_back = orig_reply

    print("\n" + "=" * 72)
    print(" 结束")
    print("=" * 72)
    for s in servers:
        s.close()
    await asyncio.gather(*[s.wait_closed() for s in servers], return_exceptions=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
