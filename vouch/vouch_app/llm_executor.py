"""llm_executor.py — 用标准库 urllib 调 OpenAI 兼容 API。

不引入 openai 包,保持零依赖。调 /v1/chat/completions,返回 assistant 文本。
失败抛异常,上层(node.py make_quality_fn)catch 后降级规则式。

兼容后端:
  · OpenAI 官方: https://api.openai.com/v1 + 真实 key
  · 本地 Ollama: http://localhost:11434/v1 + "ollama" 占位 key(ollama 兼容此接口)
  · LM Studio:   http://localhost:1234/v1 + 任意 key
"""
from __future__ import annotations
import json
import urllib.request
import urllib.error

from .llm_config import LLMConfig


def chat(cfg: LLMConfig, system: str, user: str) -> str:
    """调 /v1/chat/completions,返回 assistant 文本。失败抛异常。"""
    url = cfg.base_url.rstrip("/") + "/chat/completions"
    body = json.dumps({
        "model": cfg.model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": cfg.temperature,
    }).encode("utf-8")
    req = urllib.request.Request(url, data=body, headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cfg.api_key or 'ollama'}",
    })
    with urllib.request.urlopen(req, timeout=cfg.timeout) as r:
        data = json.loads(r.read().decode("utf-8"))
        return data["choices"][0]["message"]["content"].strip()
