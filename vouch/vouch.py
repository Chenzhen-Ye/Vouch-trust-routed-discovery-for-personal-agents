"""vouch.py — 向后兼容入口。

SDK 化后，原 1000 行单文件原型已拆成 vouch_sdk/ 包 + examples/full_flow.py。
本文件保留作旧入口，重定向到新演示，方便旧文档/脚本的 `python vouch.py` 仍可用。
"""
import asyncio
import os
import sys

# 把仓库根加进 path，让 examples/full_flow 可 import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from examples.full_flow import main

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
