"""capabilities.py — 四个真实可跑的能力执行器。

每个是纯函数 `f(task_body: str) -> str`,被 node.py 的 quality_fn 包装成 (result, 1.0)。
task 正文格式由各能力自定(解析键值或直接文本)。零外部依赖,仅标准库。

能力清单：
  translate : 中英互译(词对词 + 词典 + 短语)
  draft     : 模板起草(邮件 / 合同片段)
  calc      : 金融计算(等额本息月供 / 复利终值 / 汇率换算)
  text_tool : 文本工具(词数 / 去重 / 排序 / 统计)
"""
from __future__ import annotations
import re
import math

# ---- translate:中英词典(常用词 + 短语,可扩)----
_ZH2EN = {
    "你好": "hello", "世界": "world", "朋友": "friend", "谢谢": "thanks",
    "再见": "bye", "中国": "china", "智能体": "agent", "协议": "protocol",
    "信任": "trust", "发现": "discover", "协作": "collaborate", "邻居": "neighbor",
    "你好世界": "hello world",
}
_EN2ZH = {v: k for k, v in _ZH2EN.items()}
# 补充不在短语表里的单字
_EN2ZH.update({
    "hello": "你好", "world": "世界", "friend": "朋友", "thanks": "谢谢",
    "bye": "再见", "china": "中国", "agent": "智能体", "protocol": "协议",
    "trust": "信任", "discover": "发现", "collaborate": "协作", "neighbor": "邻居",
    "the": "这个", "is": "是", "a": "一个", "an": "一个", "and": "和",
})

_CJK = re.compile(r"[一-鿿]")


def _is_zh(s: str) -> bool:
    return bool(_CJK.search(s))


def translate(text: str) -> str:
    """中英互译。先查短语表,再逐词。未知词原样保留。"""
    text = text.strip()
    if not text:
        return "(空输入)"
    if _is_zh(text):
        # 中→英:先整句短语
        if text in _ZH2EN:
            return _ZH2EN[text]
        # 逐词(按已知短语切分,贪心最长匹配)
        out = []
        i = 0
        while i < len(text):
            matched = None
            for L in range(min(4, len(text) - i), 0, -1):
                piece = text[i:i + L]
                if piece in _ZH2EN:
                    matched = piece
                    break
            if matched:
                out.append(_ZH2EN[matched])
                i += len(matched)
            else:
                out.append(text[i])  # 未知单字原样
                i += 1
        return " ".join(out)
    else:
        # 英→中:先整句
        low = text.lower()
        if low in _EN2ZH:
            return _EN2ZH[low]
        words = re.findall(r"[A-Za-z]+|[^A-Za-z]+", text)
        out = []
        for w in words:
            key = w.lower()
            if re.match(r"[A-Za-z]+", w):
                out.append(_EN2ZH.get(key, w))
            else:
                out.append(w)
        return "".join(out)


# ---- draft:模板起草 ----
_EMAIL_TPL = """\
To: {to}
Subject: {subject}

{body}

—— {signature}
"""
_CONTRACT_TPL = """\
合同片段(草稿)

甲方: {party}
金额: ¥{amount}
用途: {purpose}

(本片段由模板生成,正式文本需人工审阅)
"""


def _parse_kv(spec: str) -> dict:
    """解析 `key1=val1 key2=val2 ...`。值含空格用引号: body="近况 说明"."""
    d = {}
    tokens = re.findall(r'(\w+)=(?:"([^"]*)"|(\S+))', spec)
    for name, qv, bv in tokens:
        d[name] = qv if qv else bv
    return d


def draft(spec: str) -> str:
    """模板起草。spec: `email to=x subject=问候 body=近况` 或
    `contract party=甲方 amount=10000 purpose=咨询`."""
    spec = spec.strip()
    if not spec:
        return "(起草用法: draft email to=... subject=... body=...  或  contract party=... amount=... purpose=...)"
    kind, _, rest = spec.partition(" ")
    kv = _parse_kv(rest)
    if kind == "email":
        return _EMAIL_TPL.format(
            to=kv.get("to", "(未指定)"),
            subject=kv.get("subject", "(无主题)"),
            body=kv.get("body", "(无正文)"),
            signature=kv.get("from", "Vouch 助手"),
        )
    elif kind == "contract":
        return _CONTRACT_TPL.format(
            party=kv.get("party", "(未指定)"),
            amount=kv.get("amount", "0"),
            purpose=kv.get("purpose", "(未指定)"),
        )
    else:
        return f"(未知模板: {kind}; 可用: email / contract)"


# ---- calc:金融计算 ----
_FX = {  # 简化固定汇率(兑人民币),演示用
    "usd": 7.2, "cny": 1.0, "eur": 7.8, "jpy": 0.048, "hkd": 0.92, "gbp": 9.1,
}


def calc(spec: str) -> str:
    """金融计算。
    loan principal=P rate=R years=N        → 等额本息月供
    compound principal=P rate=R years=N   → 复利终值
    fx amount=A from=XXX to=YYY            → 汇率换算
    """
    spec = spec.strip()
    if not spec:
        return "(calc 用法: loan principal=... rate=... years=... | compound ... | fx amount=... from=... to=...)"
    kind, _, rest = spec.partition(" ")
    kv = _parse_kv(rest)
    try:
        if kind == "loan":
            p = float(kv["principal"])
            r = float(kv["rate"]) / 12  # 年利率→月
            n = int(kv["years"]) * 12
            if r == 0:
                pay = p / n
            else:
                pay = p * r * (1 + r) ** n / ((1 + r) ** n - 1)
            total = pay * n
            return (f"贷款 ¥{p:,.0f} 利率 {float(kv['rate']):.2%} {kv['years']}年\n"
                    f"  月供 ¥{pay:,.2f}  总还 ¥{total:,.0f}  利息 ¥{total - p:,.0f}")
        elif kind == "compound":
            p = float(kv["principal"])
            r = float(kv["rate"])
            n = int(kv["years"])
            fv = p * (1 + r) ** n
            return (f"复利 本金 ¥{p:,.0f} 年率 {r:.2%} {n}年\n"
                    f"  终值 ¥{fv:,.0f}  收益 ¥{fv - p:,.0f}")
        elif kind == "fx":
            amt = float(kv["amount"])
            frm = kv.get("from", "usd").lower()
            to = kv.get("to", "cny").lower()
            if frm not in _FX or to not in _FX:
                return f"(不支持的币种; 支持: {', '.join(_FX)})"
            cny = amt * _FX[frm]
            out = cny / _FX[to]
            return f"汇率换算 {amt} {frm.upper()} = {out:,.2f} {to.upper()}"
        else:
            return f"(未知计算: {kind}; 可用: loan / compound / fx)"
    except (KeyError, ValueError) as e:
        return f"(参数错误: {e}; 检查 key=value 是否齐全)"


# ---- text_tool:文本工具 ----
def text_tool(spec: str) -> str:
    """文本小工具。
    count <text>          → 词数/字符数
    dedup <逗号分隔项>     → 去重
    sort <逗号分隔项>      → 排序
    stats <text>          → 统计
    """
    spec = spec.strip()
    if not spec:
        return "(text 用法: count <文> | dedup a,b,c | sort a,b,c | stats <文>)"
    kind, _, rest = spec.partition(" ")
    if kind == "count":
        words = rest.split()
        return f"词数={len(words)}  字符数={len(rest)}  含空格={rest.count(' ')+1}"
    elif kind == "dedup":
        items = [x.strip() for x in rest.split(",") if x.strip()]
        seen, out = set(), []
        for it in items:
            if it not in seen:
                seen.add(it)
                out.append(it)
        return f"去重({len(items)}→{len(out)}): {', '.join(out)}"
    elif kind == "sort":
        items = [x.strip() for x in rest.split(",") if x.strip()]
        return ", ".join(sorted(items))
    elif kind == "stats":
        words = rest.split()
        chars = len(rest)
        uniq = len(set(rest))
        return (f"字符={chars}  去重字符={uniq}  词数={len(words)}  "
                f"最长词={max((len(w) for w in words), default=0)}")
    else:
        return f"(未知工具: {kind}; 可用: count / dedup / sort / stats)"


# 能力注册表:name -> (函数, 标签集)
CAPABILITIES = {
    "translate": (translate, frozenset({"translate", "translation"})),
    "draft":     (draft,     frozenset({"draft", "writing"})),
    "calc":      (calc,      frozenset({"calc", "finance"})),
    "text":      (text_tool, frozenset({"text", "textools"})),
}
