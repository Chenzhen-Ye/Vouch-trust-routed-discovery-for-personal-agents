"""demo_b — bob:起草能力(Claude 后端,不可用降级规则式)+ 中继。自测三人组之一。"""

PROFILE = {
    "name": "bob",
    "port": 9002,
    "host": "127.0.0.1",
    "advertise_host": "127.0.0.1",
    "pub_dir": "/tmp/vouch_pub",
    "caps": [
        {"cap": "draft", "backend": "claude"},
    ],
    "bootstrap": [
        {"name": "alice", "host": "127.0.0.1", "port": 9001,
         "tags": ("translate", "translation"), "trust": 0.8},
        {"name": "carol", "host": "127.0.0.1", "port": 9003,
         "tags": ("calc", "finance", "text", "textools"), "trust": 0.8},
    ],
}
