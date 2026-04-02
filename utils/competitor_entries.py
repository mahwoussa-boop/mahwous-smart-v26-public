"""تحليل إدخالات منافسين من نص مجمّع — مُستخرَج من app.py."""
from __future__ import annotations


def parse_bulk_competitor_urls(text: str) -> list[str]:
    """سطور أو فواصل — روابط فريدة بالترتيب (بدون تاب ثلاثي الأعمدة)."""
    if not (text or "").strip():
        return []
    parts: list[str] = []
    for chunk in text.replace("\r\n", "\n").replace(",", "\n").split("\n"):
        chunk = chunk.strip().strip(",;")
        if not chunk or "\t" in chunk:
            continue
        u = chunk
        if not u.startswith(("http://", "https://")):
            u = "https://" + u.lstrip("/")
        if "://" in u:
            parts.append(u)
    return list(dict.fromkeys(parts))


def parse_competitor_bulk_entries(text: str) -> list[dict]:
    """جدول منسوخ: «اسم المنافس» ثم تاب ثم «رابط المتجر» ثم تاب ثم «sitemap» — أو رابط واحد لكل سطر.

    يُعاد قائمة قواميس: label, store_url, sitemap_url (اختياري).
    """
    out: list[dict] = []
    if not (text or "").strip():
        return out
    for line in text.replace("\r\n", "\n").split("\n"):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "\t" in line:
            parts = [p.strip() for p in line.split("\t") if p.strip()]
            if not parts:
                continue
            if len(parts) >= 3:
                label, store, sm = parts[0], parts[1], parts[2]
                if store.startswith(("http://", "https://")) and sm.startswith(
                    ("http://", "https://")
                ):
                    out.append(
                        {"label": label, "store_url": store, "sitemap_url": sm}
                    )
                    continue
            if len(parts) == 2:
                a, b = parts[0], parts[1]
                if b.startswith(("http://", "https://")) and not a.startswith(
                    ("http://", "https://")
                ):
                    out.append({"label": a, "store_url": b, "sitemap_url": None})
                elif a.startswith(("http://", "https://")) and b.startswith(
                    ("http://", "https://")
                ):
                    out.append({"label": "", "store_url": a, "sitemap_url": b})
                continue
            if len(parts) == 1 and parts[0].startswith(("http://", "https://")):
                out.append({"label": "", "store_url": parts[0], "sitemap_url": None})
            continue
        for u in parse_bulk_competitor_urls(line):
            out.append({"label": "", "store_url": u, "sitemap_url": None})
    return out


def dedupe_competitor_entries(entries: list[dict]) -> list[dict]:
    """منع تكرار نفس خريطة الموقع أو نفس رابط المتجر."""
    seen: set[str] = set()
    res: list[dict] = []
    for e in entries:
        sm = (e.get("sitemap_url") or "").strip()
        st = (e.get("store_url") or "").strip()
        key = sm if sm else st
        if not key or key in seen:
            continue
        seen.add(key)
        res.append(e)
    return res
