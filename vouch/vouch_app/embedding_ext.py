"""embedding_ext.py — 应用层扩展 SDK 的语义向量表。

SDK 的 EMBEDDING 覆盖通用词(law/finance/writing/python...),但应用用到的
能力标签名(calc/text/translate/draft/textools/translation)不在表里,会退化成
零向量 → 与任何 tags 余弦=0 → 路由退化成按 trust 选(失语义)。

本模块在 node 启动时把这些能力标签的向量合并进 vouch_sdk.semantic.EMBEDDING,
让应用的能力名有语义,guided pick 能按能力精准选中对的邻居。
"""
from __future__ import annotations
from vouch_sdk import semantic as _sem

# 应用层能力标签向量(8 维,与 SDK 同维度:法律/文字/技术/视觉/金融/工程/内容/业务)
# 设计目标:能力名语义合理 且 互相可分(避免 draft/text 与 translate 太像 → 路由选错邻居)。
# 关键:translate 跨语言(文字高、内容低),draft/text 起草处理(文字高、内容也高),
# 靠「内容」维度拉开夹角。calc 走金融/工程,文字维度低,与文字类自然分开。
_APP_EMBEDDING = {
    #                 法  文  技  视  金  工  内  业
    "translate":   [0.0, 0.8, 0.0, 0.0, 0.0, 0.0, 0.2, 0.3],
    "translation": [0.0, 0.8, 0.0, 0.0, 0.0, 0.0, 0.2, 0.3],
    "draft":       [0.1, 0.9, 0.0, 0.1, 0.1, 0.1, 0.9, 0.4],
    "calc":        [0.4, 0.1, 0.6, 0.0, 0.9, 0.7, 0.0, 0.6],
    "text":        [0.0, 0.6, 0.3, 0.0, 0.0, 0.3, 0.9, 0.2],
    "textools":    [0.0, 0.5, 0.5, 0.0, 0.0, 0.5, 0.8, 0.2],
}


def install():
    """把应用向量合并进 SDK 的 EMBEDDING(幂等)。node 启动时调用。"""
    _sem.EMBEDDING.update(_APP_EMBEDDING)
