"""agent.py — Agent + Acquaintance。

从原 vouch.py 搬出。核心 SDK 化改动：Agent.__init__ 收 network 参数，
把对模块级全局（REGISTRY/_DOWN/_CLOCK/_COUNT/HOST/常量）的引用全部改成
self.net.* / self.net.config.*。机制逻辑（churn 绕断点、Sybil 门槛、
HMAC+RSA 双路径身份验证、向量语义打分）逐行不变——只换符号引用。
"""
from __future__ import annotations
import asyncio
import json
import secrets
from dataclasses import dataclass, field

from .config import Config
from .network import Network
from .crypto import hmac_sign, hmac_verify, gen_keypair, rsa_sign, rsa_verify
from .semantic import semantic_sim, cosine, tags_vec, RELATED


@dataclass
class Acquaintance:
    name: str
    port: int
    tags: set = field(default_factory=set)    # 可变集合（标签随协作扩展）
    trust: float = 0.8
    degree: int = 0
    last_seen: int = 0        # 最后协作的逻辑时钟步
    interactions: int = 0     # 累计协作次数
    blocked: bool = False     # 拉黑（保留记录，不参与路由）
    intro_count: int = 0     # 本周期已引荐的新面孔数（Sybil 引荐名额）
    secret: bytes = b""       # 我预先持有的该熟人的 HMAC 身份密钥（lookup 验身份）
    pub: dict = None          # 我预先持有的该熟人的 RSA 公钥（discover 介绍人担保验身份）


class Agent:
    def __init__(self, name, port, caps, network: Network, quality_fn=None):
        self.name = name
        self.port = port
        self.caps = frozenset(caps)
        self.net = network          # ★ 持有所属网络（替代旧全局 REGISTRY/_DOWN/...）
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
        # 非对称密钥对：priv 自己持（签名），pub 作为身份公钥（带外分发给信任方）。
        # 介绍人担保用：目标用 priv 签 found（只有目标能签），介绍人/源用 pub 验。
        self._rsa_priv, self.rsa_pub = gen_keypair(256)
        network.register(self)      # ★ 显式注册（替代旧 REGISTRY[name] = self）

    @property
    def _cfg(self) -> Config:
        return self.net.config

    def knows(self, other_name, port, tags, trust=0.8, secret=b"", pub=None):
        # secret：HMAC 身份密钥（lookup）；pub：RSA 公钥（discover 介绍人担保）。带外信任锚。
        self.acq[other_name] = Acquaintance(other_name, port, set(tags), trust,
                                           secret=secret, pub=pub)

    def _name_of_port(self, port):
        for a in self.acq.values():
            if a.port == port and not a.blocked:
                return a.name
        return f"?@{port}"

    def go_offline(self):
        """模拟节点下线：关停 server，后续连接被拒。"""
        self.net.mark_down(self.name)
        if self._server:
            self._server.close()
        print(f"{self.tag} ✘ 已下线")

    # ---------- 服务器 ----------
    async def serve(self):
        self._server = await asyncio.start_server(self._handle, self._cfg.host, self.port)
        return self._server

    async def _handle(self, reader, writer):
        if self.net.is_down(self.name):
            writer.close()
            return
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

    async def _send(self, port, msg, timeout=None):
        """发消息，返回 True/False。失败（对方下线/超时）返回 False——回程绕断点的前提。"""
        if timeout is None:
            timeout = self._cfg.send_timeout
        kind = msg.get("strategy") if msg["type"] == "query" else msg["type"]
        self.net.bump(kind)
        try:
            r, w = await asyncio.wait_for(asyncio.open_connection(self._cfg.host, port), timeout=timeout)
            w.write((json.dumps(msg) + "\n").encode())
            await w.drain()
            await asyncio.wait_for(r.readline(), timeout=timeout)
            w.close()
            return True
        except (OSError, asyncio.TimeoutError) as e:
            print(f"{self.tag} 连接 {self._name_of_port(port)}({port}) 失败: {type(e).__name__}")
            return False

    # ---------- 发起发现（带源重试 + fanout 递增 + 策略升级）----------
    async def discover(self, capability, strategy="guided", ttl=None,
                       retries=None, fanout=None):
        cfg = self._cfg
        if ttl is None:
            ttl = cfg.default_ttl
        if retries is None:
            retries = cfg.source_retries
        cur_fanout = fanout or cfg.guided_fanout
        cur_strat = strategy
        last_res = None
        attempt = 0
        while attempt <= retries:
            qid = self._next_qid()
            self._seen.add(qid)
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
                cur_fanout += cfg.retry_fanout_step
                if attempt >= 2 and cur_strat == "guided":
                    cur_strat = "flood"
                    print(f"{self.tag} 升级策略 guided→flood，撒大网抗 churn")
        print(f"{self.tag} {retries+1} 次尝试均失败")
        return last_res

    async def lookup(self, target, hints=(), ttl=None):
        cfg = self._cfg
        if ttl is None:
            ttl = cfg.default_ttl
        qid = self._next_qid()
        self._seen.add(qid)
        fut = asyncio.get_running_loop().create_future()
        self._pending[qid] = fut
        msg = {"type": "query", "mode": "lookup", "target": target,
               "strategy": "guided", "ttl": ttl, "query_id": qid,
               "fanout": cfg.guided_fanout, "hints": list(hints),
               "path": [{"name": self.name, "port": self.port}]}
        print(f"\n{self.tag} 发起 lookup(target={target}, hints={list(hints)})")
        await self._forward(msg, cfg.guided_fanout)
        return await self._await(qid)

    def _next_qid(self):
        q = f"{self.name}-{self._qctr}"
        self._qctr += 1
        return q

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
            found = {"name": self.name, "port": self.port, "caps": sorted(self.caps)}
            found_json = json.dumps(found, sort_keys=True).encode()
            # 身份验证（两条路径并存）：
            # (a) HMAC：用自己的 secret 签 found（lookup 场景，源预先持我的 secret 可验）
            # (b) RSA：用自己私钥签 found（discover 场景，介绍人持我公钥可验、源经介绍人
            #     担保获得可信公钥后可验）。只有我能签 → 介绍人无法冒充我（非对称打破对称天花板）
            hmac_sig = hmac_sign(self.secret, found_json)
            rsa_sig = str(rsa_sign(self._rsa_priv, found_json))
            resp = {"type": "response", "query_id": qid, "path": path, "found": found,
                    "found_json": found_json.decode(), "hmac_sig": hmac_sig,
                    "target_pub": self.rsa_pub, "target_sig": rsa_sig,
                    "vouchers": []}   # 介绍人担保链，沿回程层层累积
            await self._reply_back(resp, path)
            return
        if msg["ttl"] <= 0:
            print(f"{self.tag} TTL 耗尽，停止")
            return
        msg2 = dict(msg)
        msg2["path"] = path
        msg2["ttl"] = msg["ttl"] - 1
        await self._forward(msg2, msg.get("fanout", self._cfg.guided_fanout))

    # ---------- 转发决策：Sybil 门槛 + fanout ----------
    async def _forward(self, msg, fanout=None):
        cfg = self._cfg
        # Sybil 防御：弱连接（trust < 阈值）不参与路由
        cands = [a for a in self.acq.values() if not a.blocked
                 and a.trust >= cfg.route_trust_threshold]
        if not cands:
            return
        fanout = fanout or cfg.guided_fanout
        if msg.get("strategy") == "flood":
            ports = [a.port for a in cands]
        else:
            ports = self._guided_pick(msg, cands, fanout)
        names = [self._name_of_port(p) for p in ports]
        weak = [n for n, a in self.acq.items() if not a.blocked and a.trust < cfg.route_trust_threshold]
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
                # 向量语义路由：能力与熟人标签的余弦相似度（连续 0~1），替代集合交集大小
                sem = semantic_sim(cap, a.tags)
            else:
                # lookup：用 hints 向量与熟人标签的相似度
                hints_vec = tags_vec(hints) if hints else [0.0]*8
                sem = max(0.0, cosine(hints_vec, tags_vec(a.tags)))
            hub = 0.3 * (a.degree / max_deg)       # degree 只数强连接（抗 Sybil）
            trust_w = 0.2 * a.trust                 # 更信的人更愿意把话筒给他
            scored.append((sem + hub + trust_w, a.trust, a.port))
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
        """中继收到响应：往源方向转发，断点绕过。
        若我是目标的直接上一跳（介绍人）且持有目标公钥，先验 target_sig，
        验过才附上自己的担保签名（用自己私钥签），把可信的目标公钥传给源。"""
        path = msg["path"]
        if path[0]["name"] == self.name:
            self._deliver(msg)
            return
        names = [p["name"] for p in path]
        if self.name not in names:
            return
        i = names.index(self.name)
        # 介绍人担保：我是目标直接上一跳（i == len-2），且我持有目标公钥
        if i == len(names) - 2 and msg.get("target_sig") and msg.get("target_pub"):
            target = msg["found"]["name"]
            acq = self.acq.get(target)
            if acq and acq.pub:
                ok = rsa_verify(acq.pub, msg["found_json"].encode(), int(msg["target_sig"]))
                if not ok:
                    print(f"{self.tag} ⚠ 目标 {target} 的 target_sig 验签失败——不担保，丢弃")
                    return
                voucher_msg = (msg["found_json"] + json.dumps(msg["target_pub"], sort_keys=True)
                               + str(msg["target_sig"])).encode()
                voucher_sig = str(rsa_sign(self._rsa_priv, voucher_msg))
                msg["vouchers"] = msg.get("vouchers", []) + [{
                    "vouching": self.name, "target": target,
                    "target_pub": msg["target_pub"], "voucher_sig": voucher_sig}]
                print(f"{self.tag} 担保：验过 {target} 的 target_sig，附上担保签名")
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
                          "hmac_sig": resp.get("hmac_sig"), "found_json": resp.get("found_json"),
                          "target_pub": resp.get("target_pub"), "target_sig": resp.get("target_sig"),
                          "vouchers": resp.get("vouchers", [])})

    # ---------- 协作 + 反馈（拓扑维护核心）----------
    async def collaborate(self, found, task, proof=None):
        """发现到目标后发起协作，按结果调信任度/标签。
        联动 §4.8↔§4.9：协作前先验身份（proof={hmac_sig, found_json}）。
        验签通过才校准能力信任；验签失败 → 拒绝协作 + 降介绍人信任。
        区分 churn 失败（超时，先重试再轻罚）vs 恶意失败（响应但质量差，重罚）。"""
        cfg = self._cfg
        name = found["name"]
        port = found["port"]
        print(f"{self.tag} 向 {name} 发起协作：「{task}」")
        acq = self.acq.get(name)
        if acq is None:
            self.remember(found)
            acq = self.acq[name]
        before = acq.trust

        # ---- 第1段：身份验证（签名↔信任联动的关键衔接）----
        # 两条路径：
        # (a) HMAC 直验：源预先持目标 secret（lookup 场景），直接验 hmac_sig。
        # (b) 介绍人担保（discover 场景）：源持直接介绍人公钥 → 验介绍人 voucher_sig
        #     → 从担保里取可信 target_pub → 用 target_pub 验 target_sig → 确认目标身份。
        #     非对称：介绍人只有目标公钥（能验不能签），无法冒充目标——打破对称天花板。
        verified = False
        if proof:
            found_json = proof.get("found_json", "").encode()
            # 路径(a)：HMAC 直验
            if proof.get("hmac_sig") and acq.secret:
                verified = hmac_verify(acq.secret, found_json, proof["hmac_sig"])
                if verified:
                    print(f"  {self.tag} ✓ HMAC 身份验证通过：确认是 {name} 本人")
            # 路径(b)：介绍人担保
            if not verified and proof.get("vouchers") and proof.get("target_sig"):
                intro = proof.get("introducer")
                intro_acq = self.acq.get(intro) if intro and intro != self.name else None
                if intro_acq and intro_acq.pub:
                    # 取介绍人的担保（vouching == introducer 的那条）
                    voucher = next((v for v in proof["vouchers"]
                                    if v["vouching"] == intro), None)
                    if voucher:
                        vmsg = (proof["found_json"]
                                + json.dumps(voucher["target_pub"], sort_keys=True)
                                + str(proof["target_sig"])).encode()
                        vok = rsa_verify(intro_acq.pub, vmsg, int(voucher["voucher_sig"]))
                        if vok:
                            # 介绍人担保可信 → 用它担保的 target_pub 验 target_sig
                            tok = rsa_verify(voucher["target_pub"], found_json,
                                             int(proof["target_sig"]))
                            verified = tok
                            if tok:
                                print(f"  {self.tag} ✓ 介绍人 {intro} 担保验证通过 → "
                                      f"用其担保的公钥验 target_sig → 确认是 {name} 本人")
                            else:
                                print(f"  {self.tag} ✗ 介绍人担保了，但 target_sig 验不过（目标冒充）")
                        else:
                            print(f"  {self.tag} ✗ 介绍人 {intro} 的担保签名验不过（冒充介绍人）")
        if proof and not verified:
            print(f"  {self.tag} ✗ 身份验证失败：响应非 {name} 本人 → 拒绝协作")
            intro = proof.get("introducer")
            if intro and intro != self.name and intro in self.acq and not self.acq[intro].blocked:
                intro_acq = self.acq[intro]
                ib = intro_acq.trust
                self._on_collab_fail(intro_acq)   # 重罚介绍人（引荐了身份不实目标）
                print(f"  {self.tag} 介绍人 {intro} trust {ib:.2f}→{intro_acq.trust:.2f}"
                      f"（引荐了身份不实目标）")
            else:
                print(f"  {self.tag} （直连目标无介绍人可降，或介绍人已拉黑）")
            return None

        outcome = None
        for attempt in range(1, cfg.collab_retries + 2):
            outcome = await self._send_task(port, task)
            if outcome is not None:
                break
            if attempt <= cfg.collab_retries:
                print(f"  {self.tag} 超时（可能 churn），重试 {attempt}/{cfg.collab_retries}")

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
        cfg = self._cfg
        acq.last_seen = self.net.tick()
        acq.interactions += 1
        acq.tags |= set(found.get("caps", []))
        if quality >= 0.7:
            acq.trust += cfg.alpha * (1 - acq.trust)            # 好→升
        elif quality >= 0.4:
            acq.trust += 0.3 * cfg.alpha * (1 - acq.trust)      # 一般→微升
        else:
            acq.trust -= cfg.beta * acq.trust                    # 差→重罚
            if acq.trust < cfg.block_threshold:
                acq.blocked = True
        acq.trust = max(0.0, min(1.0, acq.trust))

    def _on_churn_fail(self, acq):
        """churn 失败（多次超时，长期离线）：轻罚。"""
        cfg = self._cfg
        acq.trust -= cfg.churn_penalty * acq.trust
        if acq.trust < cfg.block_threshold:
            acq.blocked = True

    def _on_collab_fail(self, acq):
        """明确的恶意失败（保留接口；恶意场景在 _on_collab_success 按质量处理）。"""
        cfg = self._cfg
        acq.trust -= cfg.beta * acq.trust
        if acq.trust < cfg.block_threshold:
            acq.blocked = True

    async def _send_task(self, port, task, timeout=None):
        cfg = self._cfg
        if timeout is None:
            timeout = cfg.send_timeout
        self.net.bump("task")
        try:
            r, w = await asyncio.wait_for(asyncio.open_connection(cfg.host, port), timeout=2)
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
    def decay(self, steps=None):
        cfg = self._cfg
        if steps is None:
            steps = cfg.decay_steps
        now = self.net.tick()
        removed = []
        for name, a in list(self.acq.items()):
            idle = now - a.last_seen
            for _ in range(min(idle, steps)):
                if a.blocked:
                    break
                a.trust *= (1 - cfg.gamma)
            if a.trust < cfg.block_threshold and not a.blocked:
                a.blocked = True
                removed.append(name)
        return removed

    # ---------- 发现即扩展（带 Sybil 引荐名额）----------
    def remember(self, found, trust=0.4, introducer=None):
        cfg = self._cfg
        if found["name"] in self.acq:
            return False
        if introducer and introducer in self.acq:
            intro_acq = self.acq[introducer]
            if intro_acq.intro_count >= cfg.intro_quota:
                print(f"{self.tag} ⚠ 拒绝引荐：{introducer} 本周期引荐名额"
                      f"({cfg.intro_quota})已满，不接受新面孔 {found['name']}")
                return False
            intro_acq.intro_count += 1
        self.acq[found["name"]] = Acquaintance(found["name"], found["port"],
            set(found.get("caps", [])), trust, degree=1, last_seen=self.net.tick())
        return True
