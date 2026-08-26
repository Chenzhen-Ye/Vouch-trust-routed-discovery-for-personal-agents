"""vouch_sdk — Vouch 协议的可 import 库（明文，完整态）。

信任受限熟人图上的多跳发现协议参考实现，SDK 化为可 pip install 的库。
import 本包无副作用（不启动服务器、不写全局状态）——与旧单文件原型 vouch.py 的区别。

公共 API：
    Config      网络参数（信任阈值、重试次数、衰减率等）
    Network     一个独立的熟人叠加网络（封装旧全局 REGISTRY/_DOWN/_CLOCK/_COUNT）
    Agent       智能体节点（异步 TCP 服务器 + 本地熟人表）
    Acquaintance 熟人表条目

    身份验证原语：gen_keypair / rsa_sign / rsa_verify / hmac_sign / hmac_verify
    语义路由原语：semantic_sim / cosine / tags_vec / EMBEDDING / RELATED

机制对应 DESIGN.md §4：
  §4.1-4.6  guided/flood 路由 · discover/lookup · 发现即扩展 · 协作
  §4.8-4.9  可验证发现 + 拓扑维护（签名验身份 ↔ 信任校准能力，§4.12 联动）
  §4.10     churn 容错：回程绕断点 · 去程多路径+源重试
  §4.11     Sybil 防御：弱连接不路由 · 桥梁度只数强连接 · 引荐名额
  §4.12     身份验证联动：协作前验身份 → 失败降介绍人
  §4.13     介绍人担保：非对称签名，discover 时源经介绍人获可信公钥
  §4.14     向量语义路由：标签集合交集 → 余弦相似度

不考虑隐私版（见 DESIGN.md §8 mixnet 项与记忆 vouch-scope-no-privacy）。
零依赖，仅标准库。
"""
from .config import Config
from .network import Network
from .agent import Agent, Acquaintance
from .crypto import (
    gen_keypair, rsa_sign, rsa_verify,
    hmac_sign, hmac_verify,
)
from .semantic import semantic_sim, cosine, tags_vec, cap_vec, EMBEDDING, RELATED

__all__ = [
    "Config", "Network", "Agent", "Acquaintance",
    "gen_keypair", "rsa_sign", "rsa_verify", "hmac_sign", "hmac_verify",
    "semantic_sim", "cosine", "tags_vec", "cap_vec", "EMBEDDING", "RELATED",
]

__version__ = "0.1.0"
