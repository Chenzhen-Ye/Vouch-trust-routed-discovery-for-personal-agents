# Vouch — Trust-Routed Discovery for Personal Agents

> **Vouch**(引荐协议):每个人有自己的智能体,智能体只存储自己认识的人;源智能体通过熟人链多跳转发,在信任受限的叠加图上完成发现与协作。每一跳中继都在为路径"背书"(vouch)——这正是协议名的由来。

探索一个去中心化未来:智能体在互联网上**只通过熟人链发现彼此并协作**,而非把所有关系数据交给中心化平台。信任关系是路由的约束,不是事后补的安全补丁。

## 仓库结构

```
vouch/
├── vouch_sdk/              SDK 包（明文完整态，可 pip install）
│   ├── __init__.py         公共导出：Network/Agent/Config + 加密/语义原语
│   ├── config.py           Config dataclass（原模块级常量）
│   ├── crypto.py           HMAC + RSA（gen_keypair/sign/verify）
│   ├── semantic.py         向量语义路由（EMBEDDING/cosine/semantic_sim）
│   ├── agent.py            Agent + Acquaintance（接 network 参数，不再写全局）
│   ├── network.py          Network 类（封装 REGISTRY/_DOWN/_CLOCK/_COUNT）+ build_graph
│   └── cli.py              vouch-demo 命令行入口
├── examples/
│   └── full_flow.py        7 阶段端到端演示（原 vouch.py 的 main()）
├── tests/
│   └── test_smoke.py       冒烟测试（import 无副作用 + 双网络同进程 + 发现→协作）
├── pyproject.toml          pip 打包配置（零依赖）
├── vouch_app/              基于 SDK 的个人助手应用（真联网多进程，REPL）
│   ├── capabilities.py     四个真实能力执行器（翻译/起草/算账/文本）
│   ├── topology.py         节点+边配置（6 节点，端口 8001-8006）
│   ├── node.py             通用节点入口（python -m vouch_app.node <Name>）
│   ├── assistant.py        助手 async REPL
│   ├── embedding_ext.py    应用层语义向量扩展（让能力名有语义）
│   └── run_all.sh          一键起全部 6 进程
├── agentnet.py             明文基础版（路由 + 发现即扩展 + 协作，分机制演示）
├── agentnet_privacy.py     隐私版（无路径 + 分布式回信令牌 + DH 端到端加密）
├── agentnet_signed.py      可验证发现版（隐私版 + 目标签名）
├── agentnet_topology.py    拓扑维护版（信任度升降 + 衰减 + 拉黑 + Sybil 防御）
├── agentnet_churn.py       churn 容错版（回程绕断点 + 去程重试）
└── DESIGN.md               协议设计文档（含威胁模型与运行对照表 + §11 SDK 化）
```

> `vouch_sdk/` 是 SDK 化后的主入口（`from vouch_sdk import Network, Agent`）。`examples/full_flow.py` 是其端到端演示。其余 `agentnet_*.py` 是各机制的独立单文件演示版。

## 快速开始

零依赖,仅 Python 标准库。端口 7001–7007 需空闲。

```bash
cd vouch
pip install -e .              # 可选：装成可 import 的库
python examples/full_flow.py # 7 阶段端到端全流程（推荐先看这个）
pytest tests/ -q              # 冒烟测试
vouch-demo                   # 装后命令行入口（等价 full_flow）

# 分机制演示版
python3 agentnet.py           # 明文基础版
python3 agentnet_privacy.py   # 隐私版
```

作为库使用：

```python
from vouch_sdk import Network, Agent, Config

net = Network(Config(route_trust_threshold=0.7))  # 自定义参数
alice = Agent("Alice", 7001, ["python"], network=net)
# 同进程可跑第二个独立网络——旧原型做不到
net2 = Network()
```

明文版会演示:guided 路由 2 跳命中(2 条消息)vs flood 触达全网(11 条);发现后直接协作;二次查询近 O(1)。
隐私版额外演示:中继只搬密文不识内容,源只学到「hop=跳数, 找到谁」而非「经过谁」。

## 个人助手应用(基于 SDK)

`vouch_app/` 是基于 vouch_sdk 的**真联网多进程**个人助手:6 个节点各一个进程,通过 TCP 真连。你在助手 REPL 输入任务,助手经熟人链发现能力方 agent,把任务发去执行,返回真实结果。

```bash
cd vouch
bash vouch_app/run_all.sh        # 一键起 6 进程(5 能力后台 + 助手前台)
# 或各终端分别: python3 -m vouch_app.node <Name>
```

REPL 命令:
```
vouch> help                       # 命令帮助
vouch> contacts                   # 列熟人(地址/标签/信任度/可路由)
vouch> ask translate hello world  # 发现+执行 → 你好世界
vouch> ask translate 你好世界      #               → hello world
vouch> ask draft email to=bob subject=问候 body=近况   # 2跳经Broker→Drafter 起草邮件
vouch> ask calc loan principal=1000000 rate=0.05 years=30  # → 月供¥5368.22(真实等额本息)
vouch> ask calc compound principal=10000 rate=0.08 years=10 # → 复利终值
vouch> ask text count hello world this is a test             # → 词数=6
vouch> ask text dedup a,b,a,c,b                              # → 去重
vouch> trust                      # 列信任度(成功协作后能力方 trust 上升)
vouch> exit
```

节点拓扑(端口 8001-8006):
```
You(助手) ├─ Translator(翻译,1跳直连)
          └─ Broker(中继) ├─ Drafter(起草,2跳)
                          ├─ Accountant(算账,2跳)
                          └─ TextTooler(文本工具,2跳)
```

能力是真实可跑的(翻译=中英词典、起草=模板、算账=等额本息/复利/汇率公式、文本=词数/去重/排序),零外部依赖。SDK 的反馈循环在应用层自然生效:成功协作 → 能力方 trust 上升;节点下线 → 超时/churn 容错。详见 DESIGN.md §12。

## 协议要点

- **两种查询模式**:`discover`(按能力找人)、`lookup`(按身份找人)
- **两种路由策略**:`guided`(语义相关度+桥梁度挑 top-k,~O(路径长))、`flood`(广播,~O(节点数))
- **环路防止**:`query_id` 去重 + TTL 兜底
- **发现即扩展**:成功发现后,源把目标以弱信任加入熟人表,二次查询近 O(1)
- **隐私三件套**(仅隐私版):无路径列表 / 分布式私有回信令牌 / DH 端到端加密

## 定位

Vouch 本质是**信任受限社交叠加图上的分布式路由**。对照已知系统:

| Vouch 机制 | 对应 |
|---|---|
| 只存熟人 | 非结构化 P2P(Gnutella/Freenet) |
| 多跳转发 | 小世界路由(Kleinberg / Milgram) |
| 熟人间路由 | 联邦协议(ActivityPub / Fediverse) |
| 路径信任链 | PGP Web of Trust |
| 隐藏路径 + 中继搬密文 | 洋葱路由(Tor 思路,弱化版) |

独特之处:把"信任关系"作为叠加层,路由只能在熟人间发生——天然提供信任模型,但也直接约束可达性。

## 设计文档

完整设计见 [`vouch/DESIGN.md`](vouch/DESIGN.md):架构、消息协议、路由算法、隐私威胁模型、信息流审计、运行结果对照表、已知局限与未来方向。

## 已知局限

当前明文协议机制已完整(详见 DESIGN.md §8):churn 容错、Sybil 防御、可验证发现、身份验证联动、介绍人担保、向量语义路由均已实现,并 SDK 化为可 import 的库。尚未实现:mixnet 升级(需先恢复隐私版)、多介绍人交叉验证(防"换公钥冒充")。

## License

MIT
