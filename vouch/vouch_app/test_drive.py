"""test_drive.py — 对等网络验收驱动(开发自测用,非产品代码)。

以某 profile 的视角新建「影子」Agent(临时端口),连到真实在跑的 peers:
- 影子不与 serve 进程冲突(端口不同)
- 目标身份验证不受影响:验的是目标(serve 进程)的公钥,pub_dir 里是真的
- 响应回程按 path[0] 直连影子端口,无需对方认识影子

用法(需对应 peers 已在线):
  python3 -m vouch_app.test_drive demo_a ask translate 你好世界
  python3 -m vouch_app.test_drive demo_a ask calc "loan principal=1000000 rate=0.05 years=30"
"""
from __future__ import annotations
import asyncio
import json
import sys

from vouch_sdk import Network, Agent, Config

from .backends import make_quality_fn
from .embedding_ext import install as _install_embedding
from .llm_config import LLMConfig
from .peer_config import load_profile

_install_embedding()


async def drive(profile: str, cmd: str, arg: str):
    cfg = load_profile(profile)
    llm_cfg = LLMConfig.from_env()
    net = Network(config=Config(guided_fanout=2, send_timeout=60))
    me = Agent(cfg.name, 0, cfg.cap_names, network=net,
               quality_fn=make_quality_fn(cfg.caps, llm_cfg),
               host="127.0.0.1")
    # 端口 0 → 让系统分配临时端口;serve 后回填真实端口(path 里带的)
    server = await me.serve()
    me.port = server.sockets[0].getsockname()[1]   # 回填真实端口,回程才能找到影子
    for b in cfg.bootstrap:
        pub = None
        try:
            pub = json.load(open(f"{cfg.pub_dir}/{b.name}.json"))["pub"]
        except Exception:
            pass
        me.knows(b.name, b.port, set(b.tags), trust=b.trust, pub=pub, host=b.host)
    for a in me.acq.values():
        a.degree = 1
    print(f"[驱动] {me.name}@{me.port} 就绪,执行: {cmd} {arg}")
    if cmd == "ask":
        cap, _, task = arg.partition(" ")
        res = await me.discover(cap, strategy="guided")
        if not res or not res.get("found"):
            print("未找到能力方")
            return 1
        found = res["found"]
        path = " → ".join(p["name"] for p in res["path"])
        print(f"命中 {found['name']}(@{found.get('host')}:{found['port']}) 路径={path}")
        proof = {"found_json": res.get("found_json"),
                 "hmac_sig": res.get("hmac_sig"),
                 "target_pub": res.get("target_pub"),
                 "target_sig": res.get("target_sig"),
                 "vouchers": res.get("vouchers", []),
                 "introducer": res.get("introducer")}
        out = await me.collaborate(found, f"{cap} {task}", proof=proof)
        if out is not None:
            print(f"\n===== 结果(来自 {found['name']}) =====")
            print(out)
            return 0
        print("协作失败")
        return 1
    print(f"未知命令: {cmd}")
    return 1


def main():
    profile, cmd = sys.argv[1], sys.argv[2]
    arg = " ".join(sys.argv[3:])   # 任务含空格,合并剩余 argv
    try:
        rc = asyncio.run(asyncio.wait_for(drive(profile, cmd, arg), timeout=180))
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc or 0)


if __name__ == "__main__":
    main()
