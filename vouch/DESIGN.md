# Vouch — 个人智能体发现与协作协议设计文档

> **Vouch**(引荐协议):信任受限熟人图上的多跳发现协议。每一跳中继都在为路径"背书"(vouch),这正是协议名的由来。
> 对应实现:`agentnet.py`(明文基础版)、`agentnet_privacy.py`(隐私版)
> 状态:最小可运行原型,零依赖,单文件,多节点真正联网跑通。

## 1. 概述与设计目标

探索一个去中心化的未来图景:**每个人的智能体在互联网上,只通过自己信任的熟人链发现其他智能体并与之协作**,而非把所有关系数据上交给中心化平台。

核心设计约束：

1. **信任受限叠加图** —— 智能体只向自己「认识的人」转发查询。信任关系是路由的约束，而非事后补的安全补丁。
2. **可发现 + 可协作** —— 找到目标后能直接发起任务，形成「发现 → 协作」闭环。
3. **隐私可控** —— 隐私版收紧信息流，中继只搬密文不识内容，源只学到「多远、找到谁」而非「经过谁」。

## 2. 问题定位

本机制本质是**信任受限社交叠加图上的分布式路由问题**。

| 本机制 | 对应已知系统 |
|---|---|
| 每个智能体只存熟人 | 非结构化 P2P（Gnutella/Freenet） |
| 多跳转发找目标 | 小世界路由（Kleinberg 模型 / Milgram 实验） |
| 只在熟人间路由 | 联邦协议（ActivityPub / Fediverse actor 解析） |
| 路径上的信任链 | 信任网络（PGP Web of Trust） |
| 隐藏路径 + 中继搬密文 | 洋葱路由（Tor 思路，弱化版） |

独特之处：把「信任关系」作为叠加层，路由只能在熟人间发生，天然提供信任模型，但也直接约束可达性。

## 3. 系统架构

### 3.1 节点模型

每个智能体（Agent）= 一个异步 TCP 服务器（监听独立端口）+ 本地状态：

| 状态 | 明文版 | 隐私版 | 说明 |
|---|---|---|---|
| `name`, `port`, `caps` | ✓ | ✓ | 身份、地址、自身能力集合 |
| `acq`（熟人表） | ✓ | ✓ | `name → Acquaintance` |
| `_seen`（去重集） | ✓ | ✓ | 已处理 `query_id`，环路防止 |
| `_pending`（Future 表） | ✓ | ✓ | `query_id → Future`，等结果 |
| `_envelopes`（回信令牌表） | — | ✓ | `token → 上一跳Env \| DELIVER`，分布式回程链 |
| `_dhpriv`（DH 私钥表） | — | ✓ | `query_id → DH 私钥`（源侧） |

### 3.2 熟人表条目 `Acquaintance`

```
name    : str          熟人名字
port    : int          地址
tags    : frozenset    语义标签（路由线索）
trust   : float        信任度（0~1），影响引导式评分
degree  : int          我对该熟人「连接度」的估计：桥梁度高 → 更可能是好跳板
```

**语义标签是系统能否扩展的分水岭**：无标签则引导式退化为瞎猜，系统不可扩展（退化为指数洪泛）。

### 3.3 消息协议

**明文版**：

```
query    : {type:"query", mode:"discover"|"lookup", capability|target,
            strategy:"guided"|"flood", ttl:int, query_id:str,
            path:[{name,port}], hints?:[...]}
response : {type:"response", query_id:str, path:[...], found:{name,port,caps}}
task     : {type:"task", from:str, task:str} → {result:str}
```

**隐私版**：

```
query    : {type:"query", mode, capability|target, strategy, ttl, query_id,
            hop_count:int, source_dh_pub:int, return_env:{port,token}, hints?}
response : {type:"response", query_id, hop_count, target_dh_pub:int,
            payload_ct:hex, return_env:{port,token}}
task     : （同明文版）
```

差异要点：
- 隐私版 `query` **无 `path` 字段**，改用 `hop_count`（只计距离不记路径）。
- 隐私版 `query` 多 `source_dh_pub`（源 DH 公钥，供目标端协商加密）。
- 隐私版回程用 `return_env:{port,token}` 逐跳令牌，替代明文版的完整 `path`。
- 隐私版 `response` 的 `found` 被 DH 会话密钥加密成 `payload_ct`，中继不可读。

## 4. 核心机制

### 4.1 查询模式

| 模式 | 目标描述 | 匹配函数 | 隐私性 |
|---|---|---|---|
| `discover` | 按能力找人（"懂 law 的人"） | `capability ∈ self.caps` | 较高（能力不具身份性） |
| `lookup` | 按身份找人（"找 Grace"） | `target == self.name` | 较低（目标名即身份） |

### 4.2 路由策略

源不知道目标在哪，凭什么决定转发给哪个熟人——这是整个设计最有张力的部分。

| 策略 | 做法 | 消息复杂度 | 命中保证 |
|---|---|---|---|
| `guided`（引导式贪心） | 按「语义相关度 + 桥梁度」挑 top-k 熟人转发 | ~O(路径长) | 依赖图结构 + 标签质量 |
| `flood`（洪泛） | 向所有熟人广播，TTL 递减 | O(d^TTL)，指数 | 高（但消息爆炸） |

**引导式评分函数**：

```
discover : rel = RELATED[capability]              # 能力 → 相关标签集
           tag = |acquaintance.tags ∩ rel|
lookup   : tag = |acquaintance.tags ∩ hints|      # 调用方提供语义线索
hub      = 0.3 × (degree / max_degree)            # 无直接线索时偏向桥梁熟人
score    = tag + hub
选择     : 按 score 降序取前 GUIDED_FANOUT 个
```

关键洞察：**纯洪泛保证能到但不可扩展；引导式高效但依赖「目标线索」**。`RELATED` 映射（能力→相关标签集）就是这条线索的载体。真实系统换成向量相似度即可。

### 4.3 环路防止

- **去重**：每个智能体记录已处理 `query_id`，重复即丢弃。
- **TTL 兜底**：每跳 `ttl -= 1`，归零即停止，防失控传播。
- **隐私版的代价**：隐藏路径 ⇒ 中继无法用「已访问集合」做剪枝；环路防止只靠 `query_id` 去重 + TTL。洪泛模式下会出现「发往已访问节点」的冗余消息（被去重丢弃，但消息已发出）——见 §7 对照。

### 4.4 响应回传

**明文版**：响应携带完整 `path`，沿路径原路返回。源最终拿到 `path`（知道经过谁）。

**隐私版（分布式私有回信令牌）**：
1. 源生成令牌 `token`，私存 `_envelopes[token] = DELIVER`（哨兵：收到即交付）。
2. 每个中继收到上游 `env={port,token}`，生成**新令牌**，私存 `_envelopes[新token] = 上游env`，给下游发 `{我的port, 新token}`。
3. 目标把加密响应发给 `return_env.port`。
4. 中继收到响应：`pop(令牌)`，若为 `DELIVER` 则是源（解密交付）；否则转发给私存的上一跳 `env.port`，不碰密文。

**性质**：回程路径散落在各中继私有内存里，**任何单条消息只含一跳的回信地址**；源只拿到结果，拿不到中间人名单。

### 4.5 发现即扩展网络（路径缓存）

成功发现后，源把目标以**弱信任**（默认 0.4，远低于强连接 0.9）加入熟人表。二次查询同一目标时直连命中，近 O(1)。

**信任衰减的体现**：弱信任缓存值（0.4）就是「路径越长终点可信度越低」在数据结构层面的落地——若要决定是否把敏感任务交给刚发现的人，这个值是决策依据。

### 4.6 发现 → 协作

源用解密得到的目标端口（明文版直接用 `found.port`）发起 `task` 消息，目标执行并返回产物。**发现和协作是同一张图上的两个动作**。

### 4.7 端到端加密（仅隐私版）

紧凑 DH（演示用 64 位安全素数，生产需 ≥2048 位或真实曲线）：

```
P = gen_safe_prime(64)        # 全局
G = 2
源  : priv_s ← rand; pub_s = G^priv_s mod P          # 放进 query.source_dh_pub
目标: priv_t ← rand; pub_t = G^priv_t mod P          # 放进 response.target_dh_pub
       shared = pub_s^priv_t mod P  (≡ pub_t^priv_s mod P)
key = SHA256("agentnet-priv-v1|" || shared)
ct = XOR_stream(key, JSON(found))                    # 放进 response.payload_ct
```

中继只搬运 `payload_ct` 密文，无法解密。只有持 DH 私钥的源能解出 `found`。

### 4.8 可验证发现与消息完整性（仅可验证版）

隐私版解决了「中继看不到结果」，但留下两个缺口：中继可**冒充目标**（谎称「我是 Dave」发回假 found），可**篡改 payload**（改密文）。可验证版用非对称签名补上。

**信任锚前提**：源必须**预先**（带外渠道）持有目标的验证公钥。没有预先公钥，就无法区分真假 Dave——这是 PGP 式 Web of Trust 的固有要求。预先公钥从哪来见 §5.4。

**机制**：每个智能体有一对签名密钥（`priv` 自持，`pub` 带外分发）。目标在回程时对**明文 `found`** 签名：

```
目标: found = {name, port, caps}
      found_json = canonical_json(found)
      ct  = XOR_stream(dh_key, found_json)          # 同 §4.7 加密
      sig = RSA_sign(target_sign_priv, found_json)   # 只有目标能签
      resp = {..., payload_ct: ct, target_sig: sig, signer_name: name}
源  : found_json = XOR_decrypt(dh_key, payload_ct)
      verify(target_verify_pub, found_json, sig)    # 用预先持有的公钥验签
```

**签名放在加密里面**（对明文 found 签，不对 payload_ct 签）。理由：只有源能解密 → 只有源能验签，中继连验证都做不了，更不暴露目标身份；签的是「我是 X，我有能力 Y」这个声明本身，语义清晰。

**完整性统一处理**：解密与验签一体。密文被篡改会触发两种失败之一——解密失败（XOR 流加密翻转字节致 JSON 损坏）或验签失败（签名不再匹配）。两者都归为「完整性破坏，拒绝」，等价 Encrypt-then-MAC 的完整性保证。

**信任决策矩阵**（源收到响应后）：

| 源的状态 | 结果 | 协作？ |
|---|---|---|
| 未预先持目标公钥 | `verified=False, reason=no_trust_anchor` | 否（发现即揭示但不轻信） |
| 持公钥 + 验签通过 | `verified=True` | 是（确认本人，可协作） |
| 持公钥 + 密文被篡改 | `verified=False, reason=integrity_broken` | 否（完整性破坏） |

**残缺**：RSA 用 ~256 位模数 + PKCS#1 v1.5 式填充，纯演示。生产必须 Ed25519 或 RSA-2048 + RSASSA-PSS。安全属性（非对称、可验证）不变。

### 4.9 拓扑来源与维护（仅拓扑版）

前述版本假设熟人表是写死的静态图。拓扑版回答「我认识谁初始怎么写、会更新吗」：熟人表有**四阶段动态生命周期**。

**来源（熟人怎么进来）**：

1. **手填种子** —— 冷启动唯一零依赖方式：用户手动说「我信 Bob」，填入地址/标签/信任度。`build_graph()` 刻意只给 3 条种子边，演示从稀疏到稠密。
2. **发现即扩展** —— 多跳发现陌生人后 `remember` 以弱信任(0.4)加入（同 §4.5）。
3. **协作反馈校准** —— 协作后按结果调信任度（核心，见下）。
4. **（未实现）被动介绍** —— 朋友把某人介绍给我，我选择是否接受。

**维护（关系怎么变）——核心是信任度随协作结果升降**：

```
成功(质量 q):
  q ≥ 0.7:  trust += α·(1 - trust)        α=0.1   好→升
  q ≥ 0.4:  trust += 0.3α·(1 - trust)              一般→微升
  q < 0.4:  trust -= β·trust                β=0.3   差→按失败惩罚
失败(无响应/超时):
           trust -= β·trust                        难建易毁
拉黑:      trust < 0.2 → blocked=True，移出路由
衰减:      每周期 trust *= (1-γ)         γ=0.05    不活跃→变淡
```

设计原则「信任难建易毁」：β(0.3) > α(0.1)，坏名声比好名声积累快。

**协作副产物**：成功协作同时刷新 `last_seen`、累加 `interactions`、**扩展标签**（`tags |= found.caps`，标签越积越准）。

**协作成败判定**：目标响应且产物非空+质量分高=成功；质量分低=差(按失败罚)；超时无响应=失败(顺带演示 churn)。质量分由应用层提供（用户反馈/结果校验），原型用目标自报 `quality` 模拟。

**拓扑形成总图**：手填种子 → 发现扩展 → 协作反馈校准 → 衰减/拉黑。前一阶段喂后一阶段；协作反馈是唯一把抽象信任锚定到真实结果的来源，没有它信任度永远是常数、标签永远是声明值、网络是静态图。

**稀疏冷启动的可达性局限**：种子太少时，某些目标根本到不了（拓扑版演示 `discover('writing')` 超时——Bob 只通 law 圈，没路到 writing 圈）。这是稀疏图的真实特性，非 bug：解法是更密的种子、或跨簇桥梁熟人的引入。

**残缺**：逻辑时钟是全局单调计数（演示用），真实系统用墙钟时间；信任度参数(α/β/γ/阈值)需实证调参；标签信任(「Dave 真懂 law 吗」)仍依赖声明，未与可验证发现(§4.8)联动。

### 4.10 churn 容错（仅 churn 版 + 拓扑版）

节点随时上下线（churn）。完全去中心化的熟人图路由没有运维保证节点常驻，必须扛住。

**关键洞察：churn 对去程和回程的杀伤力不同。**

- **去程（查询转发）断** —— 相对好扛。单条路断了不影响别的路，查询本就尽力而为+TTL 兜底。解法：源重试、多路径。
- **回程（响应返回）断** —— 这是要命的。找到目标后响应要沿原路返回，中继掉线就卡死。明文版能扛（见下），隐私版无法扛（分布式令牌链断了无法绕行——这是隐私换鲁棒性的硬代价）。

**明文版回程绕断点（核心、最便宜）**：响应带完整 `path=[Alice,Bob,Dave]`。目标/中继发上一跳失败时，沿 `path` 往回找下一个能连上的节点直连。Bob 掉线 → Dave 从 path 取 Alice 地址直连。**路径信息 = 绕行能力**——这正是隐私版藏路径所牺牲的。

**三层容错**：

```
1. 回程绕断点（agentnet_churn.py）
   _reply_back / _on_response：发上一跳失败 → 沿 path 往回找存活节点直连
   断点处跳过，直至直连源

2. 去程多路径 + 源重试（agentnet_churn.py）
   discover 超时 → 自动重试 SOURCE_RETRIES 次
   每次重试 fanout += RETRY_FANOUT_STEP
   第 2 次起策略 guided→flood 升级，撒大网
   发现幂等（query_id 去重），重试安全

3. 区分 churn vs 恶意失败（agentnet_topology.py）
   超时无响应 ≠ 响应了但质量差：
     · 超时 → 先重试 COLLAB_RETRIES 次，都失败才按 churn 轻罚
       _on_churn_fail: trust -= CHURN_PENALTY(0.1)·trust
     · 响应但质量差 → 按恶意重罚（§4.9 的 BETA=0.3）
   临时抖动的好熟人（一次超时 0.50→0.45）不被误拉黑，
   长期 churn 累积或恶意坑人（0.50→0.35）才降/拉黑。
```

**设计原则**：churn 惩罚(0.1) < 恶意惩罚(0.3) < 不存在的上限。信任难建易毁，但 churn 是"非恶意"的，给更宽容的衰减斜率。

**与拓扑维护的纠缠**：协作失败现在分两类——churn 失败（轻罚、先重试）和恶意失败（重罚）。这是 §4.9 留的坑：原来一次超时就按 β=0.3 罚会误伤临时抖动，现修复。

**残缺**：源重试清 `_seen` 的方式略糙（靠新 query_id 规避，而非真正重置已访问集）；回程绕断点假设源仍在线（源掉了谁也救不了）；未实现"冗余回信路径"（多回程令牌链，因当前明文版不需要）。

### 4.11 Sybil 防御（拓扑版内）

Sybil 攻击：恶意者零成本伪造大量假身份，靠数量污染"按多数/桥梁度路由"的协议。Vouch 特别脆：桥梁度(degree)、信任度、fanout 都隐含"一身份=一独立人"。

**核心病根**：协议把身份当免费的——假设一个名字=一个独立的人。去中心化无发号中心，谁来收"注册费"？

**核心防御：弱连接不参与路由**（复用 §4.9 信任度，非新机制）。

```
ROUTE_TRUST_THRESHOLD = 0.6
_forward / _guided_pick：trust < 阈值 的熟人【不转发、不当桥梁】，只记录
```

Mallory 的傀儡都是新面孔（弱信任 0.4），进不了路由核心层。她要傀儡获得路由权，得让每个傀儡跟真人好好协作攒信任——而协作要真干活，零成本伪造的身份攒不起真实信任。**代价**：真新用户也起步难（得攒信任才能用），但这正是"信任难建"的本意。

**桥梁度抗污染**：`degree` 改为只数"强连接"熟人（trust≥阈值）。否则 Mallory 让傀儡互抬，degree 虚高，桥梁度评分被污染。

```
degree = Σ(1 for acq in other.acq if acq.trust >= ROUTE_TRUST_THRESHOLD)
```

**引荐名额**（防傀儡刷量）：`remember` 时检查介绍人（path 倒数第二跳）本周期引荐计数，超额拒绝接受新面孔。把"伪造身份成本"从零提到"得收买真人"。

```
INTRO_QUOTA = 2   # 每熟人每周期最多引荐 N 个新面孔
remember(found, introducer=path[-2]): introducer.intro_count >= QUOTA → 拒绝
```

**实跑**：Mallory 造 5 个傀儡（标签匹配 law、互抬 degree、弱信任 0.34），Alice 经 Bob 认识 3 个。discover('law') 时傀儡被打印 `[弱连接不路由: M1,M2,M3]`，guided 选强连接 Dave 直达。傀儡虽标签匹配、degree 虚高，但进不了路由核心层。

**与已有机制的关系**：Sybil 防御不新加模块，而是让 §4.9 信任度对"身份可廉价复制"敏感。§4.10 的"恶意失败"也能降 Sybil 傀儡信任。§4.8 签名防冒充但防不了 Sybil（傀儡可签真名骗信任）——所以签名+强连接门槛互补。

**残缺**：未实现"算力/质押绑定身份"（太重，变味，需上公链）；引荐名额的周期重置未做（逻辑时钟演示用）；对照实验（关防御看污染）未在 main 里跑（避免污染前面状态）。

### 4.12 身份验证联动（签名↔信任）

§4.8 签名（验"是不是本人"）与 §4.9 信任度（校准"靠不靠谱"）原本是两条平行线，中间断开：拓扑版 `collaborate` 收响应直接校准信任，从不验证"响应是不是真 Dave 发的"；签名版验完就完了，不反馈到信任度。本节把两者联动。

**三段串联**（整合版 vouch.py 内）：

```
1. 身份验证 → 确认响应者真是目标（否则拒绝，不进入协作）
2. 能力校准 → 验签通过后才发任务，按质量升/降信任
3. 路由决策 → 信任度够高才参与路由（§4.11 已有）
```

**明文简化身份验证**（不搞 DH/RSA，用预共享密钥 HMAC）：

```
Agent 生成 secret（随机字节）；通过带外渠道，信任方预先持有。
目标命中时:  sig = HMAC(self.secret, canonical_json(found))   # 放进 response
源 collaborate 前:  if acq.secret: verify(acq.secret, found_json, sig)
  通过 → 进入第2段协作校准
  失败 → 拒绝协作 + 降介绍人信任（它引荐了身份不实目标）
```

**关键衔接：验签失败传导到介绍人信任**。介绍人（path 倒数第二跳）引荐了一个身份无法证实的目标 → 重罚介绍人（`_on_collab_fail`，BETA=0.3）。守卫：介绍人不能是源自己（直连路径下 path[-2]==源）。

**实跑**（vouch.py 阶段5）：
- 5a 正常：Alice 带 Dave 的 secret，验签通过 → 协作 → Dave trust 0.70→0.71。
- 5b 不匹配：Alice 持有的 secret 与 Dave 不符，验签失败 → 拒绝协作 → 介绍人 Bob trust 0.70→0.49（重罚引荐假目标）。

**局限**：discover 模式下源无法预先持目标 secret（发现前不知目标是谁），故身份验证仅对 lookup（找已知人）或"带外事后获得 secret"成立；discover 的身份担保应走"介绍人担保目标身份"（介绍人持目标 secret 代签），当前未实现，留为后续。HMAC 用对称密钥，防冒充但不如非对称签名可公开验证。

### 4.13 介绍人担保（非对称签名，discover 的身份验证）

§4.12 的 HMAC 身份验证对 discover 不成立：源发现前不知目标是谁，没有目标 secret 可验。介绍人担保解决它。

**对称 HMAC 的天花板**：能验就能签。任何持 secret 的人既能签也能验，所以"持目标 secret 的介绍人"自己就能冒充目标——签出与真目标不可区分的签名。这是结构性限制，不是设计缺陷。

**非对称打破它**：每个 Agent 有 RSA 密钥对 `(priv, pub)`。`priv` 只自己持（能签），`pub` 作为身份公钥（能验不能签）。介绍人持目标 `pub`，能验目标签名但签不出——无法冒充。

**信任链**（源不预持目标公钥，从介绍人担保里获得可信公钥）：

```
目标命中: target_sig = RSA_sign(dave_priv, found)          # 只有 Dave 能签
         响应带 {found, target_pub=dave_pub, target_sig, vouchers=[]}
介绍人(目标直接上一跳,持 dave_pub)转发前:
         verify(dave_pub, found, target_sig) 验过才担保
         voucher_sig = RSA_sign(carol_priv, found ‖ target_pub ‖ target_sig)
         vouchers += {vouching:carol, target:"Dave", target_pub, voucher_sig}
源 collaborate 前(持直接介绍人 bob_pub):
         verify(bob_pub, found‖target_pub‖target_sig, voucher_sig)  # 验介绍人担保
         → 取担保里的 target_pub → verify(target_pub, found, target_sig)  # 验目标身份
```

**关键**：源用介绍人公钥验介绍人担保 → 从担保里取目标公钥 → 用目标公钥验目标签名。两段非对称验证。介绍人无目标私钥，签不出 target_sig，**无法冒充已绑定的身份**。

**实跑**（vouch.py 阶段6）：
- 6a 正常：Alice 不持 Dave secret（discover 场景），经 Bob 担保获可信公钥 → 验 target_sig → 协作。
- 6b 冒充失败：Bob 用自己私钥伪造 target_sig → 经 Bob 担保的 Dave 公钥验不过 → 拒绝 + 降 Bob。

**残留边界**（诚实标注）：介绍人仍可"换公钥"——声称"这是 Dave 的公钥"其实是自己的公钥，然后用自己私钥签 target_sig（自己公钥验能过）。完整防住需：多介绍人交叉验证、或目标公钥经证书链绑定、或与目标直接协作后回溯校验。当前单介绍人担保防的是"伪造已绑定公钥的签名"，不防"换公钥冒充"。

**残缺**：RSA 256 位 + 教科书式签名（对 SHA256 哈希直接签，非 PSS），纯演示；多跳路径只让直接上一跳担保（中间跳不再层层签名）；"换公钥冒充"未防（见残留边界）。

### 4.14 向量语义路由（标签集合交集 → 余弦相似度）

原打分 `tag = |a.tags & RELATED[cap]|` 是集合交集大小，二值（0/1/2...），靠人工维护 RELATED 表。向量语义路由换成连续相似度。

**机制**：每个能力词有一个语义向量（原型手编 8 维，模拟嵌入模型的潜在因子），熟人标签集的向量是其质心。打分用余弦相似度：

```
EMBEDDING["law"]      = [0.9, 0.1, 0.0, 0.0, 0.6, 0.1, 0.1, 0.5]   # 高在法律/金融维
EMBEDDING["finance"]  = [0.6, 0.1, 0.0, 0.0, 0.9, 0.2, 0.0, 0.8]   # 高在金融维
_tags_vec(tags)       = mean(EMBEDDING[t] for t in tags)           # 熟人向量=标签质心
_guided_pick 打分      = cosine(_cap_vec(cap), _tags_vec(a.tags))  # 连续 0~1
```

**为什么更准**（实测）：

| 能力对 | 集合交集（旧） | 余弦（新） | 语义判断 |
|---|---|---|---|
| law ~ finance | 1（RELATED 硬编码） | 0.92 | 高度相关 ✓ |
| law ~ contract | 1 | 0.98 | 极相关 ✓ |
| law ~ writing | 0（不相关） | 0.37 | 弱相关（写合同涉及写作）✓ |
| law ~ python | 0 | 0.21 | 几乎不相关 ✓ |

旧法二值无法表达"law 和 writing 弱相关"（交集为 0）；新法连续，且无需人工维护 RELATED 表。Dave(law,finance)=0.977 > Eve(law,writing)=0.817 > Bob(python,design)=0.259 > Carol(design,art)=0.171，精准排序。

**与已有机制的衔接**：只改 `_guided_pick` 的打分函数，路由/门槛/Sybil/churn/身份验证全不受影响——纯优化打分精度。RELATED 表保留作降级（未知能力时）。

**残缺**：向量是手编 8 维（模拟嵌入），真实系统用预训练嵌入模型（如 sentence-transformers）把能力描述映射到 768 维；向量不随协作更新（可加：协作后用结果校准向量，类似推荐系统的反馈学习）；维度含义是人为设定的，真实嵌入的维度无显式语义。

## 5. 隐私扩展：威胁模型与性质

### 5.1 信任假设

- 中继可能好奇（想多了解信息）但**诚实转发**（不篡改、不丢弃、不冒充目标）。
- 中继之间**不合谋**（不交换各自私存的回信令牌表）。
- 信道不抗主动篡改（无消息签名）；目标可被冒充（无可验证凭证）——见 §5.4 残缺泄漏与 §8 未实现项。

### 5.2 三大收紧机制

1. **无路径列表** —— 消息不再带 `path`，中继只看得到自己的上一跳/下一跳。
2. **分布式私有回信令牌** —— 回程链散落各中继私有内存，逐跳解令牌转发，无人握有完整回程。
3. **DH 端到端加密结果** —— `found` payload 端到端加密，中继搬密文不可读，源独享明文。

### 5.3 信息流审计

隐私版逐节点记录 `(query_id, name, role, info)`，把「学到/没学到什么」打印成可观测属性。典型一次 `discover("law")` 的审计表：

| 节点 | 角色 | 学到了什么 | 没学到什么（关键） |
|---|---|---|---|
| Alice | 源 | 目标=Dave、能力、hop=1 | 中间人是谁、路由 |
| Bob | 中继 | 上一跳=Alice、下一跳=Dave | 结果是谁（密文不可读） |
| Dave | 目标 | 所求能力=law、上一跳端口 | 源身份（仅端口）、完整路径 |

**对照明文版**：明文版源会拿到 `path=Alice→Bob→Dave`（知道是 Bob 帮的忙）；隐私版源只知道「hop=1, 找到 Dave」，Bob 不可见。

### 5.4 设计性泄漏（不可消除，诚实标注）

1. **发现即揭示** —— 源最终要协作，必然学到目标身份。`discover`（按能力）比 `lookup`（按人名）更私有，因能力不具身份性。这是协作类查询的宿命。
2. **上一跳端口可见** —— 邻居本来认识你（等价 Tor 入口节点知源 IP）。要藏到对邻居都不可见，需 mixnet（洋葱+延迟+批处理），代价大。
3. **所求能力明文** —— `capability` 必须出现在查询里（中继需它引导转发 + 自我匹配），但比具体人名更不具身份性。
4. **DH 仅 64 位** —— 纯演示（生成快），生产必须 ≥2048 位或真实曲线。安全属性不变，仅抗破解强度。

## 6. 拓扑与实验配置

固定 7 节点小世界图，三个语义簇（tech / law / art-writing）+ 桥梁熟人：

| 节点 | 端口 | 能力 | 簇 |
|---|---|---|---|
| Alice | 7001 | python, backend | tech |
| Bob | 7002 | python, design | tech（桥梁：连 law） |
| Carol | 7003 | design, art | art |
| Dave | 7004 | law, finance | law |
| Eve | 7005 | law, writing | law（桥梁：连 writing） |
| Frank | 7006 | art, design | art |
| Grace | 7007 | writing, editing | writing |

边集（有向，带 tags 与 trust）：见源码 `build_graph()`。
全局参数：`HOST=127.0.0.1`、`DEFAULT_TTL=6`、`GUIDED_FANOUT=1`。

## 7. 运行结果对照

| 场景 | 策略 | 路径 | 消息数 | 说明 |
|---|---|---|---|---|
| 明文 discover("law") | guided | Alice→Bob→Dave | 2 | 标签引导精准命中 |
| 明文 discover("law") | flood | 触达全网 | 11 | 指数展开，找到 Eve 与 Dave |
| 明文 lookup("Grace") | guided | Alice→Bob→Eve→Grace | 3 | hints 线索引导 |
| 明文 缓存后再查 | guided | Alice→Dave | 1 | 发现即扩展，近 O(1) |
| 隐私 discover("law") | guided | （隐藏，hop=1） | 2 | 中继搬密文，源不知路由 |
| 隐私 discover("law") | flood | （隐藏，hop=4） | 11 | 无法剪枝的代价（含冗余） |
| 隐私 缓存后再查 | guided | （隐藏，hop=0） | 1 | 直连，仍只暴露 1 跳 |

**核心张力**：guided ≈ O(路径长) 可扩展；flood ≈ O(节点数) 保证命中但爆炸。隐私版 flood 因无法用已访问集合剪枝，冗余更显著——故**隐私版几乎必须配 guided 或缓存**，纯洪泛在隐私模式下浪费更狠。

## 8. 已知局限与未来方向

分析框架中的工程难题进展：

| 方向 | 状态 | 说明 |
|---|---|---|
| **可验证发现** | ✅ 已实现 | 见 §4.8：目标签名，源用预先持有的验证公钥验签 |
| **消息完整性** | ✅ 已实现 | 解密+验签一体；密文被篡改→解密失败或验签失败，拒绝 |
| **拓扑来源与维护** | ✅ 已实现 | 见 §4.9：手填种子→发现扩展→协作反馈校准→衰减/拉黑 |
| **churn 容错** | ✅ 已实现 | 见 §4.10：回程绕断点 + 去程多路径/源重试 + 区分 churn/恶意失败 |
| **Sybil 防御** | ✅ 已实现 | 见 §4.11：弱连接不路由 + 桥梁度只数强连接 + 引荐名额 |
| **身份验证联动** | ✅ 已实现 | 见 §4.12：协作前验身份(HMAC)→验签失败降介绍人信任，打通签名↔信任 |
| **介绍人担保** | ✅ 已实现 | 见 §4.13：非对称签名，discover 时源经介绍人获可信公钥，介绍人无法冒充 |
| **向量语义路由** | ✅ 已实现 | 见 §4.14：标签集合交集 → 余弦相似度（连续 0~1），更准且无需维护 RELATED 表 |
| **mixnet 升级** | ⏳ 未实现 | 加延迟+批处理，打乱时序关联，藏到对邻居不可见（且需先恢复隐私版） |

## 9. 运行说明

```bash
# 明文基础版
python3 agentnet.py

# 隐私版
python3 agentnet_privacy.py

# 可验证发现版（隐私版 + 目标签名）
python3 agentnet_signed.py

# 拓扑来源与维护版（明文 + 熟人表动态生命周期）
python3 agentnet_topology.py

# churn 容错版（明文 + 节点上下线容错）
python3 agentnet_churn.py
```

零依赖，仅 Python 标准库。端口 7001–7007 需空闲。

## 10. 文件清单

| 文件 | 内容 |
|---|---|
| `vouch.py` | **整合版（明文完整态）**：发现→协作→拓扑演化→churn→Sybil→身份验证→介绍人担保→向量语义路由 一气呵成 |
| `agentnet.py` | 明文基础版：路由 + 发现即扩展 + 协作（分机制演示） |
| `agentnet_privacy.py` | 隐私版：无路径 + 分布式回信令牌 + DH 加密 + 信息流审计 |
| `agentnet_signed.py` | 可验证发现版：隐私版 + 目标签名 + 完整性校验（RSA 演示） |
| `agentnet_topology.py` | 拓扑维护版：明文 + 信任度随协作升降 + 衰减 + 拉黑 + churn/恶意区分 + Sybil 防御 |
| `agentnet_churn.py` | churn 容错版：明文 + 回程绕断点 + 去程多路径 + 源重试 |

> `vouch.py` 是四个明文分机制版本的整合，跑通端到端全流程。其余 `agentnet_*.py` 是各机制的独立演示版（可单独运行看单个机制）。
| `DESIGN.md` | 本设计文档 |
