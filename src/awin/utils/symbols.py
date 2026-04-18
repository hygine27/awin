from __future__ import annotations


def normalize_stock_code(raw: object) -> str:
    text = str(raw or "").strip()
    if text.isdigit() and len(text) <= 6:
        return text.zfill(6)
    if "." in text:
        base = text.split(".", 1)[0]
        if base.isdigit() and len(base) <= 6:
            return base.zfill(6)
    return text


def infer_symbol_from_stock_code(stock_code: str) -> str:
    code = normalize_stock_code(stock_code)
    if not code:
        return ""
    if code.startswith(("000", "001", "002", "003", "300", "301")):
        return f"{code}.SZ"
    if code.startswith(("600", "601", "603", "605", "688", "689")):
        return f"{code}.SH"
    if code.startswith(("430", "431", "832", "833", "834", "835", "836", "837", "838", "839", "870", "871", "872", "873", "874", "875", "876", "920")):
        return f"{code}.BJ"
    return code
