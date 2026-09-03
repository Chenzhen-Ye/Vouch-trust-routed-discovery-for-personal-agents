"""peer.py — 对等节点入口(每人电脑上跑一个)。

与编排式 node.py 的区别:无 topology 硬编码——只有「我自己」+ 我手动配的
引导熟人。每个 peer 是平等的:既可发起 ask,也可被别人发现/中继/执行。

用法:
  python -m vouch_app.peer --profile demo_a        # 用 profiles/ 示例(自测)
  python -m vouch_app.peer --name dave --port 9100 # 临时 peer(无能力,可中继)
  python -m vouch_app.peer --name dave --port 9100 --cap translate --cap calc:text

身份与公钥(§4.13):
  · 密钥对启动时生成(不落盘)。whoami 打印公钥指纹 + blob,复制粘贴交换。
  · pub_dir(自测):各 peer 把公钥写进共享目录,add 时自动取——模拟带外信道。
    真实场景:对方 whoami 给你 blob,你 add 时粘贴(见 assistant.py)。
"""
from __future__ import annotations
import argparse
import asyncio
import json
import os

from vouch_sdk import Network, Agent, Config
from vouch_sdk.semantic import EMBEDDING  # noqa: F401(确认可导入)

from .backends import make_quality_fn, describe_backends
from .embedding_ext import install as _install_embedding
from .llm_config import LLMConfig
from .peer_config import PeerConfig, CapSpec, load_profile

_install_embedding()   # 应用能力标签向量合并进 SDK(幂等)


def _publish_pub(cfg: PeerConfig, me: Agent):
    """自测:把我的公钥写进 pub_dir/<name>.json,模拟带外公钥交换信道。"""
    if not cfg.pub_dir:
        return
    os.makedirs(cfg.pub_dir, exist_ok=True)
    path = os.path.join(cfg.pub_dir, f"{cfg.name}.json")
    with open(path, "w") as f:
        json.dump({"name": cfg.name, "pub": me.rsa_pub}, f)


def _read_pub_from_dir(pub_dir: str, name: str) -> dict | None:
    """自测:从 pub_dir 读对方公钥。没有返回 None(不阻塞加熟人,后续 verify)。
    互相引荐的 peer 几乎同时启动,对方公钥可能晚几秒才写入——重试等几秒。"""
    if not pub_dir:
        return None
    path = os.path.join(pub_dir, f"{name}.json")
    import time
    for _ in range(20):            # 最多等 5 秒
        if os.path.exists(path):
            with open(path) as f:
                return json.load(f).get("pub")
        time.sleep(0.25)
    return None


async def _pub_backfill(me: Agent, cfg: PeerConfig):
    """启动等待窗口内没换到公钥的熟人,后台继续等(长窗口),换到就补进 acq.pub。
    不阻塞节点上线——公钥后到不影响已上线的服务。"""
    import time
    missing = [b.name for b in cfg.bootstrap
               if not (me.acq.get(b.name) and me.acq[b.name].pub)]
    for name in missing:
        for _ in range(120):       # 后台再等 30s
            pub = _read_pub_from_dir_nowait(cfg.pub_dir, name)
            if pub:
                me.acq[name].pub = pub
                print(f"  [公钥补齐] {name}: {fingerprint_of(pub)}")
                break
            await asyncio.sleep(0.25)


def _read_pub_from_dir_nowait(pub_dir: str, name: str) -> dict | None:
    if not pub_dir:
        return None
    path = os.path.join(pub_dir, f"{name}.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f).get("pub")
    return None


def fingerprint_of(pub: dict) -> str:
    from .peer_config import fingerprint
    return fingerprint(pub)


def build_peer(cfg: PeerConfig, llm_cfg: LLMConfig, net: Network) -> Agent:
    """按配置建我自己的 Agent + 引导熟人。"""
    me = Agent(cfg.name, cfg.port, cfg.cap_names, network=net,
               quality_fn=make_quality_fn(cfg.caps, llm_cfg),
               host=cfg.advertise_host)
    for b in cfg.bootstrap:
        # 引导熟人的公钥:优先带外交换文件(pub_dir,自测),真实场景经 REPL add 粘贴
        pub = _read_pub_from_dir(cfg.pub_dir, b.name)
        me.knows(b.name, b.port, set(b.tags), trust=b.trust, pub=pub, host=b.host)
        fp = "(待换公钥)" if pub is None else _fp(pub)
        print(f"  引导熟人 {b.name}@{b.host}:{b.port} tags={sorted(b.tags)} "
              f"trust={b.trust} 公钥={fp}")
    # 单进程看不到全局 degree → 手设 1 占位(路由靠 sem+trust)
    for a in me.acq.values():
        a.degree = 1
    return me


def _fp(pub: dict) -> str:
    from .peer_config import fingerprint
    return fingerprint(pub)


async def run_peer(cfg: PeerConfig, serve_only: bool = False):
    llm_cfg = LLMConfig.from_env()
    # LLM/claude 是秒级慢调用 → 源端 send_timeout 放大,否则误判 churn
    send_timeout = max(llm_cfg.timeout + 5, 30) if (
            llm_cfg.enabled or any(c.backend == "claude" for c in cfg.caps)) else 2.0
    cfg_obj = Config(guided_fanout=2, send_timeout=send_timeout)
    net = Network(config=cfg_obj)
    me = build_peer(cfg, llm_cfg, net)
    _publish_pub(cfg, me)
    # 启动等待窗口内没换到公钥的熟人 → 后台长窗口补齐(不阻塞上线)
    asyncio.get_running_loop().create_task(_pub_backfill(me, cfg))
    server = await me.serve()
    print(f"\n[{cfg.name}] 对等节点已上线 @{cfg.host}:{cfg.port}"
          f"(对外 {cfg.advertise_host}:{cfg.port})")
    if cfg.caps:
        print(f"  能力: {describe_backends(cfg.caps, llm_cfg)}")
    else:
        print("  能力: (无,纯中继/助手)")
    try:
        if serve_only:
            # --serve:常驻服务模式(不进 REPL;stdin 不管,靠 Ctrl-C/kill 退出)
            print("(常驻服务模式;Ctrl-C 退出)")
            await asyncio.Event().wait()
        else:
            from .assistant import repl
            await repl(me, server, peer_cfg=cfg)
    finally:
        server.close()
        await server.wait_closed()


def _parse_cap(s: str) -> dict:
    """--cap translate 或 --cap calc:text(冒号后=后端)"""
    cap, _, backend = s.partition(":")
    return {"cap": cap, "backend": backend or "rule"}


def main():
    ap = argparse.ArgumentParser(description="Vouch 对等节点")
    ap.add_argument("--profile", help="profiles/ 下的配置名(如 demo_a)")
    ap.add_argument("--name", help="节点名(profile 未给时)")
    ap.add_argument("--port", type=int, help="监听端口(默认 9000)")
    ap.add_argument("--host", default="0.0.0.0", help="监听地址")
    ap.add_argument("--advertise-host", dest="advertise_host", default=None,
                    help="对外宣称地址(跨机填真实 IP)")
    ap.add_argument("--cap", action="append", default=[],
                    help="能力[:后端],如 translate / calc / draft:claude(可多次)")
    ap.add_argument("--bootstrap", action="append", default=[],
                    help="引导熟人 name@host:port:tags(逗号分隔,可多次)")
    ap.add_argument("--pub-dir", dest="pub_dir", default="",
                    help="公钥交换目录(自测模拟带外信道)")
    ap.add_argument("--serve", action="store_true",
                    help="常驻服务模式(不进 REPL,靠 Ctrl-C 退出)")
    args = ap.parse_args()

    if args.profile:
        cfg = load_profile(args.profile)
    else:
        if not args.name:
            ap.error("需 --profile 或 --name")
        caps = [CapSpec(**_parse_cap(c)) for c in args.cap]
        boot = []
        for b in args.bootstrap:
            # 格式 name@host:port:tags(host 可为 IP 或域名,不含冒号)
            name_rest = b.split("@", 1)
            name, rest = (name_rest if len(name_rest) == 2 else (name_rest[0], None))
            if rest is None:
                ap.error(f"--bootstrap 格式 name@host:port:tags → {b!r}")
            hostport, _, tags = rest.rpartition(":")   # 从右切:port:tags
            host, _, port_s = hostport.rpartition(":")
            boot.append({"name": name, "host": host or "127.0.0.1",
                         "port": int(port_s), "tags": tuple(
                             t for t in tags.split(",") if t), "trust": 0.7})
        cfg = PeerConfig(
            name=args.name, port=args.port or 9000, host=args.host,
            advertise_host=args.advertise_host or ("127.0.0.1"
                if args.host in ("0.0.0.0", "127.0.0.1") else args.host),
            caps=caps, bootstrap=[type("B", (), b) for b in boot] if boot else [],
            pub_dir=args.pub_dir)
        # bootstrap 用简单 dict 转 Bootstrap
        from .peer_config import Bootstrap
        cfg.bootstrap = [Bootstrap(**b) for b in boot]

    try:
        asyncio.run(run_peer(cfg, serve_only=args.serve))
    except KeyboardInterrupt:
        print("\n下线")


if __name__ == "__main__":
    main()
