"""قائمة المنافسين المسبقة من JSON — مُستخرَجة من app.py."""
from __future__ import annotations

import json
import os

from config import PRESET_COMPETITORS_PATH


def load_preset_competitors(path: str | None = None) -> list[dict]:
    """قائمة المنافسين الثابتة من `data/preset_competitors.json` (اسم، متجر، sitemap)."""
    p = path or PRESET_COMPETITORS_PATH
    if not os.path.isfile(p):
        return []
    try:
        with open(p, encoding="utf-8") as f:
            raw = json.load(f)
    except Exception:
        return []
    if not isinstance(raw, list):
        return []
    out: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        su = str(item.get("store_url") or "").strip()
        sm = str(item.get("sitemap_url") or "").strip()
        if not name:
            continue
        if not su.startswith(("http://", "https://")) and not sm.startswith(
            ("http://", "https://")
        ):
            continue
        out.append({"name": name, "store_url": su, "sitemap_url": sm})
    return out
