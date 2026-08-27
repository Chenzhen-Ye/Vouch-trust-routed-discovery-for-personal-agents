"""prompts.py — 各 LLM 能力的 system/user prompt 模板。

每个能力把 task 正文包成对话,system 约束输出格式(只输出结果,不加解释),
让 LLM 输出可控。draft 用规则模板作 few-shot 格式引导。

calc 不接 LLM(精确计算),无 prompt。
"""
from __future__ import annotations


def prompt_for(cap_key: str, task: str):
    """返回 (system, user) 二元组。"""
    if cap_key == "translate":
        return (
            "你是翻译助手。在中文与英文之间互译:给中文翻成英文,给英文翻成中文。"
            "只输出译文,不加解释、注释或多余文字。",
            f"翻译以下文本:\n\n{task}",
        )
    if cap_key == "draft":
        # few-shot:把规则模板的格式作为引导,让 LLM 输出接近该格式但不放飞
        return (
            "你是起草助手。按用户给的规格起草中文邮件或合同片段。"
            "规格形如 `email to=... subject=... body=...` 或 "
            "`contract party=... amount=... purpose=...`。"
            "只输出起草的正文,不加解释。",
            f"规格:\n{task}",
        )
    if cap_key == "text":
        return (
            "你是文本工具助手。按指令对文本执行操作(count/dedup/sort/stats),"
            "只输出结果,不加解释。指令如 `count <文>`、`dedup a,b,c`、`sort x,y,z`。",
            f"{task}",
        )
    raise ValueError(f"无 LLM prompt 模板: {cap_key}")
