"""llm_config.py — LLM 后端配置(OpenAI 兼容 API)。

读环境变量填充,不把密钥写进仓库。未配置时 enabled=False,能力全走规则式
(应用仍零依赖可跑)。配置后,translate/draft/text 走 LLM,calc 始终规则式
(精确计算 LLM 不可靠)。

环境变量(都可选):
  VOUCH_LLM_BASE_URL / OPENAI_BASE_URL   默认 http://localhost:11434/v1(本地 Ollama)
  VOUCH_LLM_API_KEY  / OPENAI_API_KEY    Ollama 不需,"ollama" 占位即可
  VOUCH_LLM_MODEL                          模型名;本地默认 qwen2.5:1.5b,远程默认 gpt-4o-mini
  VOUCH_LLM_TEMPERATURE                    默认 0.3
  VOUCH_LLM_TIMEOUT                        默认 30 秒

默认指向本地 Ollama(离线友好):先装 Ollama,ollama pull qwen2.5:1.5b,
再 export VOUCH_LLM_MODEL=qwen2.5:1.5b(或就用默认)即可。
"""
from __future__ import annotations
import os
from dataclasses import dataclass, field


@dataclass
class LLMConfig:
    base_url: str = ""
    api_key: str = ""
    model: str = ""
    temperature: float = 0.3
    timeout: float = 30.0
    # 哪些能力用 LLM。calc 不在内(精确计算 LLM 不可靠,始终规则式)。
    llm_caps: tuple = ("translate", "draft", "text")

    @classmethod
    def from_env(cls) -> "LLMConfig":
        base = (os.environ.get("VOUCH_LLM_BASE_URL")
                or os.environ.get("OPENAI_BASE_URL", ""))
        key = (os.environ.get("VOUCH_LLM_API_KEY")
               or os.environ.get("OPENAI_API_KEY", ""))
        model = os.environ.get("VOUCH_LLM_MODEL", "")
        temp = float(os.environ.get("VOUCH_LLM_TEMPERATURE", "0.3"))
        timeout = float(os.environ.get("VOUCH_LLM_TIMEOUT", "30"))
        return cls(base_url=base, api_key=key, model=model,
                   temperature=temp, timeout=timeout)

    @property
    def enabled(self) -> bool:
        """base_url 和 model 都配了才算启用。"""
        return bool(self.base_url and self.model)

    def use_llm(self, cap_key) -> bool:
        """该能力是否走 LLM。启用 且 cap 在 llm_caps。"""
        return self.enabled and cap_key in self.llm_caps

    def describe(self) -> str:
        """启动时打印的状态摘要。"""
        if not self.enabled:
            return "未启用(全规则式)"
        return (f"已启用: {self.base_url} model={self.model} "
                f"LLM能力={list(self.llm_caps)} calc=规则式")
