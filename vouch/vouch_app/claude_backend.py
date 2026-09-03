"""claude_backend.py — Claude 后端:调第三方 agent(Claude)执行能力任务。

两条路径,优先 CLI:
  1. Claude Code CLI: `claude -p "<prompt>" --output-format text`
     零依赖(用户装了 Claude Code 即可),本机就有。
  2. Anthropic API: POST /v1/messages,需 ANTHROPIC_API_KEY。

失败抛异常,由 backends.py 统一 catch 降级 rule。
"""
from __future__ import annotations
import json
import os
import shutil
import subprocess
import urllib.request

from .prompts import prompt_for

ANTHROPIC_URL = "https://api.anthropic.com/v1/messages"
DEFAULT_MODEL = "claude-sonnet-5"
_API_TIMEOUT = 120.0   # agent 式调用,秒级起步
_CLI_TIMEOUT = 120.0


def claude_available() -> str:
    """返回可用路径:"cli" / "api" / ""(都不可用)。"""
    if shutil.which("claude"):
        return "cli"
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "api"
    return ""


def run_claude(cap_key: str, task: str) -> str:
    """用 Claude 执行能力任务。成功返回文本,失败抛异常。"""
    system, user = prompt_for(cap_key, task)
    path = claude_available()
    if path == "cli":
        return _via_cli(system, user)
    if path == "api":
        return _via_api(system, user)
    raise RuntimeError("Claude 不可用(无 claude CLI 也无 ANTHROPIC_API_KEY)")


def _via_cli(system: str, user: str) -> str:
    """Claude Code CLI 无交互模式。system prompt 用 --append-system-prompt 拼入。"""
    proc = subprocess.run(
        ["claude", "-p", user, "--output-format", "text",
         "--append-system-prompt", system],
        capture_output=True, text=True, timeout=_CLI_TIMEOUT)
    if proc.returncode != 0:
        raise RuntimeError(f"claude CLI 退出码 {proc.returncode}: {proc.stderr[:200]}")
    out = proc.stdout.strip()
    if not out:
        raise RuntimeError("claude CLI 返回空输出")
    return out


def _via_api(system: str, user: str) -> str:
    """Anthropic messages API(标准库 urllib,零依赖)。"""
    key = os.environ["ANTHROPIC_API_KEY"]
    model = os.environ.get("ANTHROPIC_MODEL", DEFAULT_MODEL)
    body = json.dumps({
        "model": model,
        "max_tokens": 2048,
        "system": system,
        "messages": [{"role": "user", "content": user}],
    }).encode()
    req = urllib.request.Request(ANTHROPIC_URL, data=body, headers={
        "Content-Type": "application/json",
        "x-api-key": key,
        "anthropic-version": "2023-06-01",
    })
    with urllib.request.urlopen(req, timeout=_API_TIMEOUT) as r:
        data = json.loads(r.read().decode())
        return "".join(b.get("text", "") for b in data.get("content", [])).strip()
