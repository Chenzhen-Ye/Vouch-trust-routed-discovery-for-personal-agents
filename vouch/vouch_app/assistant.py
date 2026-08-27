"""assistant.py — 助手 async REPL。

你在 `vouch>` 输入命令,助手经熟人链发现能力方 agent,把任务发去执行,返回真实结果。

命令:
  help                      列命令
  contacts                  列熟人(name/port/tags/trust/可路由?)
  find <capability>        发起发现,打印命中节点 + 路径
  ask <capability> <task>   发现 + 协作:把 task 发给能力方执行,返回成品
  trust                     列熟人信任度
  add <name> <port> <tags>  运行时加熟人(逗号分隔 tags)
  exit                      退出

示例:
  vouch> ask translate hello world
  vouch> ask translate 你好世界
  vouch> ask draft email to=bob subject=问候 body=近况
  vouch> ask calc loan principal=1000000 rate=0.05 years=30
  vouch> ask text count hello world this is a test

信任边界:应用层熟人由 topology.py 配置给定(你自己配的邻居)。
SDK 的 HMAC/RSA 身份验证(§4.12-4.13)给不信任的远端熟人链,应用层默认不启用
(传 proof=None,走无验证分支直接执行);quality 反馈仍生效:成功→trust 升,失败→降。
"""
from __future__ import annotations
import asyncio


async def _ainput(prompt: str) -> str:
    """async input:用线程执行器阻塞读,不卡事件循环(零外部依赖,无需 aioconsole)。"""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, input, prompt)


_HELP = """\
Vouch 个人助手 — 命令:
  help                        本帮助
  contacts                    列熟人(地址/标签/信任度/可路由)
  find <cap>                  发现能力方(只找不执行)
  ask <cap> <task>            发现 + 执行,返回成品
  trust                       列熟人信任度
  add <name> <port> <tags>    加熟人(tags 逗号分隔)
  exit                        退出
示例:
  ask translate hello world
  ask draft email to=bob subject=问候 body=近况
  ask calc loan principal=1000000 rate=0.05 years=30
  ask text count hello world this is a test"""


async def repl(me, server):
    """助手主循环。me=You Agent。server 后台接 TCP 连接,REPL 前台交互。"""
    print("\n" + "=" * 60)
    print(" Vouch 个人助手已就绪")
    print("=" * 60)
    print(_HELP)
    print("=" * 60)
    # server 需在 loop 里持续 accept;async with 保活。REPL 在同一 loop 跑。
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
    elif cmd == "contacts":
        print(f"{'熟人':12s} {'端口':>5s}  {'标签':30s} {'trust':>6s} {'路由':>4s}")
        for name, a in me.acq.items():
            routable = "是" if a.trust >= cfg.route_trust_threshold and not a.blocked else "否"
            mark = " [拉黑]" if a.blocked else ""
            print(f"{name:12s} {a.port:>5d}  {str(sorted(a.tags)):30s} {a.trust:>6.2f} {routable:>4s}{mark}")
    elif cmd == "find":
        cap = rest.strip()
        if not cap:
            print("用法: find <capability>"); return
        print(f"发现 {cap} ...")
        res = await me.discover(cap, strategy="guided")
        _print_found(res)
    elif cmd == "ask":
        cap, _, task = rest.partition(" ")
        if not cap:
            print("用法: ask <capability> <task>"); return
        print(f"发现 {cap} ...")
        res = await me.discover(cap, strategy="guided")
        if not res or not res.get("found"):
            print(f"未找到能力方 {cap}。试试 contacts 看熟人,或确认能力节点已上线。")
            return
        found = res["found"]
        path = " → ".join(p["name"] for p in res["path"])
        print(f"命中 {found['name']}(@{found['port']}) 路径={path}")
        print(f"派发任务:「{task}」")
        # 应用层信任=配置给的熟人;不传 proof,走无验证分支直接执行。
        # quality 反馈生效:成功 1.0→trust 升,失败→降。
        out = await me.collaborate(found, task)
        if out is not None:
            print(f"\n结果(来自 {found['name']}):")
            print(out)
        else:
            print(f"协作失败(目标下线/超时?)。trust 已下调。")
    elif cmd == "trust":
        print(f"{'熟人':12s} {'trust':>6s} {'次数':>4s} {'状态':>4s}")
        for name, a in me.acq.items():
            st = "拉黑" if a.blocked else "活"
            print(f"{name:12s} {a.trust:>6.2f} {a.interactions:>4d} {st:>4s}")
    elif cmd == "add":
        parts = rest.split()
        if len(parts) < 3:
            print("用法: add <name> <port> <tag1,tag2,...>"); return
        name, port_s, tags_s = parts[0], parts[1], parts[2]
        try:
            port = int(port_s)
        except ValueError:
            print(f"端口须为整数: {port_s}"); return
        tags = set(t.strip() for t in tags_s.split(",") if t.strip())
        me.knows(name, port, tags, trust=0.5)
        print(f"已加熟人 {name}@{port} tags={sorted(tags)} trust=0.5(弱,需协作攒)")
    elif cmd == "exit":
        print("再见")
        return False
    else:
        print(f"未知命令: {cmd}(输入 help 看命令)")
    return None


def _print_found(res):
    if not res:
        print("未找到")
        return
    if not res.get("found"):
        print("未找到")
        return
    found = res["found"]
    path = " → ".join(p["name"] for p in res["path"])
    intro = res.get("introducer")
    print(f"命中 {found['name']}(@{found['port']}) caps={found.get('caps')}")
    print(f"路径={path} 介绍人={intro}")
