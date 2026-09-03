"""profiles/ — 对等节点示例配置(自测用三人组)。

alice ↔ bob ↔ carol 链式拓扑(刻意非全联通,演示多跳发现):
  alice: translate(LLM)      端口 9001,只认识 bob
  bob:   draft(claude)       端口 9002,认识 alice + carol
  carol: calc + text(规则式) 端口 9003,只认识 bob

alice 找 calc 要经 bob 两跳——真实多跳发现 + 介绍人担保(bob 为 carol 担保)。
"""
