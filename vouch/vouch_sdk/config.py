"""config.py — Vouch 协议的全局可调参数。

原 vouch.py 的模块级常量集中到此 dataclass。
一个 Config 实例 = 一组网络参数；Network 持有它，Agent 通过 self.net.config 读取。
这样不同网络可跑不同参数（如更严的 Sybil 阈值），而旧原型的全局常量被消除。
"""
from __future__ import annotations
from dataclasses import dataclass


@dataclass
class Config:
    # ---- 基础网络 ----
    host: str = "127.0.0.1"
    default_ttl: int = 6
    guided_fanout: int = 1

    # ---- 拓扑维护参数（§4.9）----
    alpha: float = 0.1              # 成功协作增益
    beta: float = 0.3               # 恶意失败重罚（响应了但质量差 / 多次 churn）
    gamma: float = 0.05             # 每衰减周期
    block_threshold: float = 0.2    # 低于此值 → 拉黑
    decay_steps: int = 3           # 每周期代表「一段时间不互动」

    # ---- churn 容错参数（§4.10）----
    source_retries: int = 2         # 发现层：源超时后重试次数
    retry_fanout_step: int = 1      # 每次重试 fanout 加多少
    collab_retries: int = 2         # 协作层：单次协作超时重试（区分 churn vs 恶意）
    churn_penalty: float = 0.1      # churn 失败轻罚（< beta，临时掉线不该重罚）
    send_timeout: float = 2.0       # 单次连接/读超时：超时即判下线

    # ---- Sybil 防御参数（§4.11）----
    route_trust_threshold: float = 0.6   # 信任度低于此值的熟人【不参与路由】，只记录
    intro_quota: int = 2                  # 每熟人每周期最多引荐 N 个新面孔
