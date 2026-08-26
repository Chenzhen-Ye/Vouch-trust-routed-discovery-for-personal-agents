"""semantic.py — 向量语义路由（§4.14）。

从原 vouch.py 搬出，零逻辑改动。纯数据 + 纯函数，无全局状态。

把路由打分从「标签集合交集」（二值 0/1/2）升级到「向量余弦相似度」（连续 0~1）。
真实系统用嵌入模型把「law」映射到高维向量；原型手编 8 维特征向量模拟。
维度是潜在语义因子（法律/文字/技术/视觉/金融/工程/内容/业务）。
余弦相似度连续 0~1，比「集合交集大小」更准：law≈finance（同维度高），
law 远离 python（维度正交）。RELATED 表保留作降级/对照。
"""
from __future__ import annotations

EMBEDDING = {
    "law":         [0.9, 0.1, 0.0, 0.0, 0.6, 0.1, 0.1, 0.5],
    "contract":    [0.9, 0.3, 0.0, 0.0, 0.5, 0.2, 0.2, 0.6],
    "policy":      [0.8, 0.2, 0.0, 0.0, 0.3, 0.1, 0.1, 0.5],
    "finance":     [0.6, 0.1, 0.0, 0.0, 0.9, 0.2, 0.0, 0.8],
    "accounting":  [0.5, 0.1, 0.1, 0.0, 0.9, 0.2, 0.0, 0.7],
    "writing":     [0.2, 0.9, 0.0, 0.1, 0.1, 0.0, 0.8, 0.3],
    "editing":     [0.2, 0.9, 0.0, 0.1, 0.1, 0.0, 0.7, 0.3],
    "blog":        [0.1, 0.8, 0.1, 0.2, 0.1, 0.0, 0.9, 0.3],
    "translation": [0.3, 0.9, 0.0, 0.0, 0.1, 0.0, 0.6, 0.3],
    "python":      [0.0, 0.1, 0.9, 0.0, 0.2, 0.8, 0.0, 0.2],
    "backend":     [0.0, 0.1, 0.9, 0.0, 0.2, 0.9, 0.0, 0.3],
    "data":        [0.1, 0.1, 0.8, 0.0, 0.5, 0.7, 0.0, 0.4],
    "ml":          [0.1, 0.1, 0.8, 0.0, 0.4, 0.6, 0.0, 0.3],
    "design":      [0.0, 0.1, 0.1, 0.9, 0.0, 0.2, 0.3, 0.4],
    "art":         [0.0, 0.2, 0.0, 0.9, 0.0, 0.0, 0.5, 0.2],
    "ui":          [0.0, 0.1, 0.3, 0.8, 0.0, 0.4, 0.2, 0.4],
    "brand":       [0.1, 0.3, 0.0, 0.8, 0.2, 0.0, 0.4, 0.6],
}

# 降级/对照用：旧法的集合交集表。未知能力路由时退化到它。
RELATED = {
    "law":     frozenset({"law", "finance", "contract", "policy"}),
    "writing": frozenset({"writing", "editing", "blog", "translation"}),
    "python":  frozenset({"python", "backend", "data", "ml"}),
    "design":  frozenset({"design", "art", "ui", "brand"}),
    "finance": frozenset({"finance", "law", "accounting"}),
}

_VEC_DIM = 8


def cosine(a, b):
    """余弦相似度，-1~1。未知词返回 0（正交）。"""
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(x * x for x in b) ** 0.5
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def cap_vec(cap):
    """能力的语义向量（从 EMBEDDING 查；未知能力退化为其自身单标签向量）。"""
    return EMBEDDING.get(cap, [0.0] * _VEC_DIM)


def tags_vec(tags):
    """一个熟人的语义向量 = 其所有标签向量的平均（质心）。"""
    vecs = [EMBEDDING.get(t) for t in tags if t in EMBEDDING]
    if not vecs:
        return [0.0] * _VEC_DIM
    return [sum(v[i] for v in vecs) / len(vecs) for i in range(len(vecs[0]))]


def semantic_sim(cap, tags):
    """能力与熟人标签集的语义相似度（0~1，余弦归一化）。替代 |tags & RELATED[cap]|。"""
    sim = cosine(cap_vec(cap), tags_vec(tags))
    return max(0.0, sim)   # 负相似度截断为 0（不相关）
