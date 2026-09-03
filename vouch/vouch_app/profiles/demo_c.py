"""demo_c — carol:算账 + 文本工具(规则式)。自测三人组之一。"""

PROFILE = {
    "name": "carol",
    "port": 9003,
    "host": "127.0.0.1",
    "advertise_host": "127.0.0.1",
    "pub_dir": "/tmp/vouch_pub",
    "caps": [
        {"cap": "calc", "backend": "rule"},
        {"cap": "text", "backend": "rule"},
    ],
    "bootstrap": [
        {"name": "bob", "host": "127.0.0.1", "port": 9002,
         "tags": ("translate", "translation", "draft", "writing"),
         "trust": 0.8},
    ],
}
