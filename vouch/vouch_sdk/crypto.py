"""crypto.py — Vouch 的身份验证原语：对称 HMAC + 非对称 RSA。

从原 vouch.py 搬出，零逻辑改动。纯函数无全局状态，可被任意 Agent 复用。

两类签名对应两种身份验证场景：
  · HMAC（对称）：lookup 场景，源预先持目标 secret，目标用 secret 签 found，源验。
  · RSA（非对称）：discover 场景，目标用私钥签 found（只有目标能签），
    介绍人/源用目标公钥验——打破「能验就能签」的对称天花板（介绍人无法冒充）。
"""
from __future__ import annotations
import hmac
import hashlib
import secrets


# ---- 对称 HMAC（lookup 身份验证）----
def hmac_sign(secret: bytes, msg: bytes) -> str:
    return hmac.new(secret, msg, hashlib.sha256).hexdigest()


def hmac_verify(secret: bytes, msg: bytes, sig: str) -> bool:
    return hmac.compare_digest(hmac_sign(secret, msg), sig)


# ---- 非对称 RSA（discover 介绍人担保）----
# 教科书式 RSA，演示用小模数（256 bits）；生产需 ≥2048 + PSS/Ed25519。
def _miller_rabin(n, k=8):
    if n < 2:
        return False
    for p in (2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47):
        if n % p == 0:
            return n == p
    d, r = n - 1, 0
    while d % 2 == 0:
        d //= 2
        r += 1
    for _ in range(k):
        a = 2 + secrets.randbelow(n - 3)
        x = pow(a, d, n)
        if x in (1, n - 1):
            continue
        for _ in range(r - 1):
            x = pow(x, 2, n)
            if x == n - 1:
                break
        else:
            return False
    return True


def _gen_prime(bits):
    while True:
        n = secrets.randbits(bits) | 1 | (1 << (bits - 1))
        if _miller_rabin(n):
            return n


def _egcd(a, b):
    if b == 0:
        return a, 1, 0
    g, x, y = _egcd(b, a % b)
    return g, y, x - (a // b) * y


def _modinv(a, m):
    g, x, _ = _egcd(a % m, m)
    if g != 1:
        raise ValueError("无模逆")
    return x % m


def gen_keypair(bits=256):
    """返回 (priv, pub)。e=65537。演示用小模数，生产需 ≥2048 + Ed25519/PSS。"""
    while True:
        p, q = _gen_prime(bits), _gen_prime(bits)
        if p == q:
            continue
        n = p * q
        phi = (p - 1) * (q - 1)
        e = 65537
        if _egcd(e, phi)[0] != 1:
            continue
        d = _modinv(e, phi)
        if d > 1:
            break
    return {"n": n, "d": d}, {"n": n, "e": e}


def rsa_sign(priv, msg: bytes) -> int:
    """对消息的 SHA256 哈希签名（教科书式 RSA，演示用；非 PSS）。"""
    h = hashlib.sha256(msg).digest()
    m = int.from_bytes(h, "big")
    return pow(m, priv["d"], priv["n"])


def rsa_verify(pub, msg: bytes, sig: int) -> bool:
    h = hashlib.sha256(msg).digest()
    expected = int.from_bytes(h, "big")
    return pow(sig, pub["e"], pub["n"]) == expected
