"""assistant.py — 对等助手 async REPL。

你在 `vouch>` 输入命令,助手经熟人链发现能力方 peer,验身份后把任务发去执行。

命令:
  whoami                            我的三要素:name/host:port/公钥指纹 + caps + 公钥 blob
  contacts                          列熟人(host:port/tags/trust/公钥指纹/可路由)
  find <cap>                        发起发现,打印命中 + 路径
  ask <cap> <task>                  发现 + 担保验证 + 执行,返回成品
  trust                             列熟人信任度演化
  add <name> <host:port> <tags> [trust] [pub_blob]
                                    加引导熟人(pub_blob=对方 whoami 的 PUB 行;
                                    省略则显示待交换)
  verify <name>                     显示与对方公钥指纹(人工带外核对)
  exit                              退出

身份验证(§4.13):
  ask 时把 discover 返回的 found_json/target_sig/vouchers 作为 proof 传给
  collaborate——SDK 走两条路径:HMAC 直验(持 secret)或介绍人担保
  (介绍人签过 target_pub,我用介绍人公钥验担保,再验 target_sig)。
  对等场景:直连熟人走「已换公钥」的担保/直验;间接熟人在 path 有介绍人。
"""
from __future__ import annotations
import asyncio

from .peer_config import fingerprint, pub_to_blob, blob_to_pub


async def _ainput(prompt: str) -> str:
    """async input:用线程执行器阻塞读,不卡事件循环(零外部依赖,无需 aioconsole)。"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, input, prompt)


_HELP = """\
Vouch 对等助手 — 命令:
  whoami                                  我的身份(名字/地址/公钥指纹/能力)
  contacts                                列熟人(地址/标签/信任度/公钥状态)
  find <cap>                              发现能力方(只找不执行)
  ask <cap> <task>                        发现 + 担保验证 + 执行
  trust                                   列熟人信任度
  add <name> <host:port> <tags> [trust] [pub_blob]
                                          加引导熟人(pub_blob=对方 whoami 的 PUB 行)
  verify <name>                           显示对方公钥指纹(带外核对)
  exit                                    退出
示例:
  ask translate hello world
  ask draft email to=bob subject=问候 body=近况
  ask calc loan principal=1000000 rate=0.05 years=30
  ask text count hello world this is a test
  add bob 127.0.0.1:9002 draft,writing 0.8 <PUB_BLOB>"""


async def repl(me, server, peer_cfg=None):
    """对等助手主循环。me=我的 Agent;peer_cfg=PeerConfig(whoami 用,可 None)。"""
    print("\n" + "=" * 60)
    print(f" Vouch 对等助手已就绪 — {me.name}")
    print("=" * 60)
    print(_HELP)
    print("=" * 60)
    async with server:
        while True:
            try:
                line = await _ainput("\nvouch> ")
            except (EOFError, KeyboardInterrupt):
                print("\n再见")
                return
            line = line.strip()
            if not line:
                continue
            cmd, _, rest = line.partition(" ")
            try:
                keep = await _dispatch(me, cmd, rest)
            except Exception as e:
                print(f"[命令出错] {e!r}")
            if keep is False:
                return


async def _dispatch(me, cmd, rest) -> bool | None:
    cfg = me._cfg
    if cmd == "help":
        print(_HELP)

    elif cmd == "whoami":
        fp = fingerprint(me.rsa_pub)
        print(f"名字   : {me.name}")
        print(f"地址   : {me.host}:{me.port}")
        print(f"公钥指纹: {fp}")
        print(f"能力   : {sorted(me.caps) if me.caps else '(无,纯中继)'}")
        print(f"PUB(复制给对方 add 用):")
        print(f"  {pub_to_blob(me.rsa_pub)}")

    elif cmd == "contacts":
        print(f"{'熟人':10s} {'地址':21s} {'标签':36s} {'trust':>6s} {'公钥':>8s} {'路由':>4s}")
        for name, a in me.acq.items():
            addr = f"{a.host}:{a.port}"
            routable = "是" if a.trust >= cfg.route_trust_threshold and not a.blocked else "否"
            has_pub = "已换" if a.pub else "待换"
            mark = " [拉黑]" if a.blocked else ""
            print(f"{name:10s} {addr:21s} {str(sorted(a.tags))[:36]:36s} "
                  f"{a.trust:>6.2f} {has_pub:>8s} {routable:>4s}{mark}")

    elif cmd == "find":
        cap = rest.strip()
        if not cap:
            print("用法: find <capability>"); return
        print(f"发现 {cap} ...")
        res = await me.discover(cap, strategy="guided")
        _print_found(res)

    elif cmd == "ask":
        cap, _, task = rest.partition(" ")
        if not cap or not task:
            print("用法: ask <capability> <task>"); return
        print(f"发现 {cap} ...")
        res = await me.discover(cap, strategy="guided")
        if not res or not res.get("found"):
            print(f"未找到能力方 {cap}。试试 contacts 看熟人,或确认对方已上线。")
            return
        found = res["found"]
        path = " → ".join(p["name"] for p in res["path"])
        print(f"命中 {found['name']}(@{found.get('host', '127.0.0.1')}:{found['port']}) 路径={path}")
        # §4.13 担保验证:把 discover 响应里的签名材料作为 proof 传入。
        # SDK 三条路径:HMAC 直验 / 直连公钥直验(target_pub) / 介绍人担保(vouchers)。
        proof = {"found_json": res.get("found_json"),
                 "hmac_sig": res.get("hmac_sig"),
                 "target_pub": res.get("target_pub"),
                 "target_sig": res.get("target_sig"),
                 "vouchers": res.get("vouchers", []),
                 "introducer": res.get("introducer")}
        print(f"派发任务:「{task}」(带担保验证)")
        # 对等约定:task 首词带 cap,目标端按首词路由执行器
        out = await me.collaborate(found, f"{cap} {task}", proof=proof)
        if out is not None:
            print(f"\n结果(来自 {found['name']}):")
            print(out)
        else:
            print(f"协作失败(身份验证不过/目标下线/超时)。详见上方日志。")

    elif cmd == "trust":
        print(f"{'熟人':10s} {'trust':>6s} {'次数':>4s} {'状态':>4s}")
        for name, a in me.acq.items():
            st = "拉黑" if a.blocked else "活"
            print(f"{name:10s} {a.trust:>6.2f} {a.interactions:>4d} {st:>4s}")

    elif cmd == "add":
        parts = rest.split()
        if len(parts) < 3:
            print("用法: add <name> <host:port> <tags> [trust] [pub_blob]"); return
        name, addr, tags_s = parts[0], parts[1], parts[2]
        trust = 0.7
        pub = None
        if len(parts) >= 4:
            try:
                trust = float(parts[3])
            except ValueError:
                print(f"trust 须为数字: {parts[3]}"); return
        if len(parts) >= 5:
            try:
                pub = blob_to_pub(parts[4])
            except Exception as e:
                print(f"公钥 blob 解析失败: {e}"); return
        host, _, port_s = addr.rpartition(":")
        if not host or not port_s.isdigit():
            print(f"地址须为 host:port → {addr}"); return
        tags = set(t.strip() for t in tags_s.split(",") if t.strip())
        me.knows(name, int(port_s), tags, trust=trust, pub=pub, host=host)
        fp = fingerprint(pub) if pub else "待交换(对方 whoami → verify 核对)"
        print(f"已加熟人 {name}@{host}:{int(port_s)} tags={sorted(tags)} "
              f"trust={trust} 公钥={fp}")

    elif cmd == "verify":
        name = rest.strip()
        if not name:
            print("用法: verify <name>"); return
        a = me.acq.get(name)
        if a is None:
            print(f"不认识 {name}(先 add)"); return
        if a.pub:
            print(f"{name} 的公钥指纹: {fingerprint(a.pub)}")
            print("与对方 whoami 显示的指纹人工核对(带外:微信/当面)。")
        else:
            print(f"尚未与 {name} 交换公钥。请对方 whoami,把 PUB 行粘贴进:")
            print(f"  add {name} {a.host}:{a.port} <tags> {a.trust} <PUB_BLOB>")

    elif cmd == "exit":
        print("再见")
        return False
    else:
        print(f"未知命令: {cmd}(输入 help 看命令)")
    return None


def _print_found(res):
    if not res or not res.get("found"):
        print("未找到")
        return
    found = res["found"]
    path = " → ".join(p["name"] for p in res["path"])
    intro = res.get("introducer")
    print(f"命中 {found['name']}(@{found.get('host', '127.0.0.1')}:{found['port']}) "
          f"caps={found.get('caps')}")
    print(f"路径={path} 介绍人={intro}")
