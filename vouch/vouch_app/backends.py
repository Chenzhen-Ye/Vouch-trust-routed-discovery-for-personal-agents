"""backends.py — 能力后端注册:rule / llm / claude 三选一(每个能力独立配置)。

统一出口:`make_quality_fn(cap_specs, llm_cfg) -> f(task) -> (result, quality)`。

对等节点一个进程多能力,task 格式约定为 `<cap> <正文>`(首词=能力名,与 REPL
的 ask <cap> <task> 天然一致)——按首词路由到该能力声明的后端。

降级链(诚实计质量分):
  claude/llm 失败 → 该 cap 有 rule 实现 → 降级执行,质量 0.7
  claude/llm 失败 → 无 rule 实现       → 失败,质量 0.1
  rule 失败                              → 失败,质量 0.1
成功(任意后端)= 1.0。calc 即使配了 llm/claude 也强制 rule(精确计算不给模型)。
"""
from __future__ import annotations

from .capabilities import CAPABILITIES
from .llm_config import LLMConfig


def _rule_exec(cap_key: str, task_body: str, llm_cfg=None) -> str:
    fn, _tags = CAPABILITIES[cap_key]
    return fn(task_body)


def _llm_exec(cap_key: str, task_body: str, llm_cfg: LLMConfig) -> str:
    from .prompts import prompt_for
    from .llm_executor import chat
    system, user = prompt_for(cap_key, task_body)
    return chat(llm_cfg, system, user)


def _claude_exec(cap_key: str, task_body: str, llm_cfg=None) -> str:
    from .claude_backend import run_claude
    return run_claude(cap_key, task_body)


def make_quality_fn(cap_specs, llm_cfg: LLMConfig):
    """按每个 CapSpec 声明的后端建执行器表。

    cap_specs: [CapSpec(cap=..., backend=...)]
    llm_cfg:   LLMConfig(llm 后端连接参数;backends=llm 必需)
    返回 quality_fn(task) -> (result, quality 0~1)
    """
    handlers = {}
    for spec in cap_specs:
        cap = spec.cap
        if cap not in CAPABILITIES:
            # 能力名不在 rule 实现表:llm/claude 后端必须有 prompt 模板,否则启动即拒
            from .prompts import prompt_for
            try:
                prompt_for(cap, "probe")
            except Exception as e:
                raise ValueError(
                    f"能力 {cap!r} 无 rule 实现也无 LLM prompt 模板,无法提供: {e}")
        # calc 强制 rule:精确计算不给模型(§12.5 原则)
        backend = "rule" if cap == "calc" else spec.backend
        if backend == "llm" and not llm_cfg.enabled:
            backend = "rule" if cap in CAPABILITIES else backend  # 无 rule 实现则仍走 llm(启动时报连接错)
        handlers[cap] = (backend, "rule" if cap in CAPABILITIES else None)

    def wrapped(task: str):
        # task 首词 = 能力名(与 ask <cap> <task> 一致),其余 = 正文
        cap, _, body = task.partition(" ")
        if cap not in handlers:
            return (f"[未知能力 {cap!r}; 我提供: {sorted(handlers)}]", 0.1)
        backend, _fallback = handlers[cap]
        # rule 直走
        if backend == "rule":
            try:
                return (_rule_exec(cap, body), 1.0)
            except Exception as e:
                return (f"[{cap} 执行失败] {e}", 0.1)
        # llm / claude:失败降级 rule(有实现才降;降级质量 0.7)
        try:
            if backend == "llm":
                return (_llm_exec(cap, body, llm_cfg), 1.0)
            else:
                return (_claude_exec(cap, body), 1.0)
        except Exception as e:
            if cap in CAPABILITIES:
                try:
                    rule_result = _rule_exec(cap, body)
                    return (f"{rule_result}\n[{backend} 不可用,已降级规则式: {type(e).__name__}]",
                            0.7)
                except Exception as e2:
                    return (f"[{cap} 降级规则式也失败] {e2}", 0.1)
            return (f"[{cap} {backend} 后端失败且无规则式实现] {e}", 0.1)

    return wrapped


def describe_backends(cap_specs, llm_cfg: LLMConfig) -> str:
    """启动时打印每个能力的后端选择(含强制/降级说明)。"""
    from .claude_backend import claude_available
    lines = []
    for spec in cap_specs:
        backend = "rule" if spec.cap == "calc" else spec.backend
        note = ""
        if spec.cap == "calc" and spec.backend != "rule":
            note = "(calc 强制规则式:精确计算)"
        elif backend == "llm" and not llm_cfg.enabled:
            note = "(LLM 未配置" + ("→降级规则式)" if spec.cap in CAPABILITIES else ")")
        elif backend == "claude":
            path = claude_available()
            note = f"(via {path})" if path else "(claude 不可用" + (
                "→降级规则式)" if spec.cap in CAPABILITIES else ")")
        lines.append(f"{spec.cap}:{backend}{note}")
    return " ".join(lines)
