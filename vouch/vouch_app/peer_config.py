"""peer_config.py — 对等节点配置(每人一份)。

与编排式 topology.py 的区别:topology 写死全网 6 节点;PeerConfig 只描述
「我自己」——我的名字/地址/我提供的能力(及各能力的后端)/我手动加的引导熟人。

身份(§4.13):
  · 每个节点的 RSA 密钥对启动时生成(不落盘),公钥指纹 = SHA256 前 16 hex。
  · 加引导熟人需对方公钥(带外交换:对方 whoami 打印,你复制粘贴进 add)。
  · pub_dir(仅自测):各节点把公钥写进共享目录,模拟带外信道——
    真实场景靠人(微信/当面)传,不自动化带外信道。
"""
from __future__ import annotations
import base64
import hashlib
import importlib
import json
from dataclasses import dataclass, field

VALID_BACKENDS = ("rule", "llm", "claude")


@dataclass
class CapSpec:
    """我提供的一个能力:名字 + 执行后端。"""
    cap: str                 # 能力名(对外发现用,如 translate)
    backend: str = "rule"    # rule / llm / claude

    def __post_init__(self):
        if self.backend not in VALID_BACKENDS:
            raise ValueError(f"未知后端 {self.backend!r}; 可用: {VALID_BACKENDS}")


@dataclass
class Bootstrap:
    """引导熟人:我自己手动配的(带外换过公钥)。"""
    name: str
    host: str = "127.0.0.1"
    port: int = 0
    tags: tuple = ()
    trust: float = 0.7


@dataclass
class PeerConfig:
    name: str                       # 我的节点名(如 alice)
    port: int = 9000                # 监听端口
    host: str = "0.0.0.0"           # 监听地址(0.0.0.0 接受跨机连接)
    advertise_host: str = "127.0.0.1"   # 对外宣称的地址(跨机填真实 IP;path/found 里带的)
    caps: list = field(default_factory=list)       # [CapSpec]
    bootstrap: list = field(default_factory=list)  # [Bootstrap]
    pub_dir: str = ""               # 自测:公钥文件交换目录(""=禁用)

    @property
    def cap_names(self) -> list:
        return [c.cap for c in self.caps]


# ---------- 公钥指纹与传输编码 ----------

def fingerprint(pub: dict) -> str:
    """公钥指纹:规范 JSON 的 SHA256 前 16 hex。供人眼带外比对。"""
    if not pub:
        return "(无公钥)"
    blob = json.dumps(pub, sort_keys=True).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def pub_to_blob(pub: dict) -> str:
    """公钥 → 单行 base64(whoami 打印,对方复制粘贴进 add)。"""
    raw = json.dumps(pub, sort_keys=True).encode()
    return base64.b64encode(raw).decode()


def blob_to_pub(blob: str) -> dict:
    """add 收到的 base64 → 公钥 dict。解析失败抛异常。"""
    blob = blob.strip()
    pub = json.loads(base64.b64decode(blob).decode())
    if not isinstance(pub, dict) or "n" not in pub:
        raise ValueError("不是合法公钥 blob(应为 whoami 打印的 PUB 行)")
    return pub


# ---------- 从 profiles/ 读示例配置 ----------

def load_profile(profile_name: str) -> PeerConfig:
    """从 vouch_app.profiles.<name> 读 PROFILE dict 建 PeerConfig。"""
    mod = importlib.import_module(f"vouch_app.profiles.{profile_name}")
    d = dict(mod.PROFILE)
    caps = [CapSpec(**c) for c in d.pop("caps", [])]
    boot = [Bootstrap(**b) for b in d.pop("bootstrap", [])]
    return PeerConfig(caps=caps, bootstrap=boot, **d)
