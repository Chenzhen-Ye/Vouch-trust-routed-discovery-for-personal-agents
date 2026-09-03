"""demo_a — alice:翻译能力(LLM 后端)。自测三人组之一。"""

PROFILE = {
    "name": "alice",
    "port": 9001,
    "host": "127.0.0.1",
    "advertise_host": "127.0.0.1",
    "pub_dir": "/tmp/vouch_pub",
    "caps": [
        {"cap": "translate", "backend": "llm"},
    ],
    "bootstrap": [
        # bob:中继熟人,tags 覆盖他能引荐到的所有能力(下游 draft/calc/text)
        {"name": "bob", "host": "127.0.0.1", "port": 9002,
         "tags": ("draft", "writing", "calc", "finance", "text", "textools"),
         "trust": 0.8},
    ],
}
