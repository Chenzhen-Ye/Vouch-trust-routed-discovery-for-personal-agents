"""vouch_app.__main__ — `python -m vouch_app` 默认起助手(You)。

注意:助手只起了 You 一个节点。完整网络需另起 Broker/Translator/Drafter/
Accountant/TextTooler(它们在别的端口)。一键起全部用 `bash vouch_app/run_all.sh`。
"""
from .node import main

if __name__ == "__main__":
    main("You")
