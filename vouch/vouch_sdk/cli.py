"""cli.py — `vouch-demo` 命令行入口。

pip install 后可 `vouch-demo` 跑 7 阶段端到端演示。
等价于 python examples/full_flow.py。
"""
from __future__ import annotations
import asyncio


def main():
    """跑 full_flow 演示。延迟 import 避免安装时硬依赖 examples 目录。"""
    import sys
    import os
    # examples/ 不打包进 site-packages，从仓库根找
    here = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(here)
    examples_dir = os.path.join(repo_root, "examples")
    if examples_dir not in sys.path:
        sys.path.insert(0, repo_root)
    try:
        from examples.full_flow import main as _main
    except ImportError:
        # 装到 site-packages 后 examples 不在；内联最小演示
        print("Vouch SDK 已安装，但 examples/full_flow 未随包打包。")
        print("从仓库源码跑：python examples/full_flow.py")
        return
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
