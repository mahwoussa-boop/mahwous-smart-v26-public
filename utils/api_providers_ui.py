"""حالة مزوّدي API للواجهة — شارات الإعدادات والشريط الجانبي."""
from __future__ import annotations

from html import escape as html_escape

import streamlit as st

from config import (
    get_cohere_api_key,
    get_gemini_api_keys,
    get_openrouter_api_key,
    get_webhook_missing_products,
    get_webhook_update_prices,
)

API_STATUS_HINT = {
    "ok": ("#00C853", "يعمل"),
    "warn": ("#FF9800", "حد/بطء (429)"),
    "bill": ("#E65100", "رصيد/فاتورة منتهية (402)"),
    "bad": ("#FF1744", "رفض/خطأ"),
    "absent": ("#78909C", "غير مضاف"),
    "unknown": ("#5C6BC0", "لم يُختبر بعد"),
}


def classify_provider_line(status: str) -> str:
    s = status or ""
    if "غير موجود" in s and "مفتاح" in s:
        return "absent"
    if "402" in s or "منته" in s or ("رصيد" in s and "❌" in s):
        return "bill"
    if "✅" in s:
        return "ok"
    if "⚠️" in s or "429" in s:
        return "warn"
    if "❌" in s:
        return "bad"
    return "unknown"


def infer_api_diag_summary(diag: dict) -> dict:
    """تلخيص نتيجة diagnose_ai_providers → حالة لكل مزود."""
    out: dict = {}
    if not get_gemini_api_keys():
        out["gemini"] = "absent"
    else:
        gs = diag.get("gemini") or []
        if not gs:
            out["gemini"] = "unknown"
        else:
            parts = [classify_provider_line(g.get("status", "")) for g in gs]
            if "bill" in parts:
                out["gemini"] = "bill"
            elif all(p == "ok" for p in parts):
                out["gemini"] = "ok"
            elif "ok" in parts and "bad" not in parts and "warn" not in parts:
                out["gemini"] = "ok"
            elif "ok" in parts and ("bad" in parts or "warn" in parts):
                out["gemini"] = "warn"
            elif "warn" in parts:
                out["gemini"] = "warn"
            elif "bad" in parts:
                out["gemini"] = "bad"
            else:
                out["gemini"] = "unknown"
    if not get_openrouter_api_key():
        out["openrouter"] = "absent"
    else:
        out["openrouter"] = classify_provider_line(diag.get("openrouter", ""))
    if not get_cohere_api_key():
        out["cohere"] = "absent"
    else:
        out["cohere"] = classify_provider_line(diag.get("cohere", ""))
    out["wh_price"] = "ok" if get_webhook_update_prices() else "absent"
    out["wh_new"] = "ok" if get_webhook_missing_products() else "absent"
    return out


def presence_api_summary() -> dict:
    """بدون تشخيص — الوجود فقط (مفتاح مضاف أم لا)."""
    return {
        "gemini": "ok" if get_gemini_api_keys() else "absent",
        "openrouter": "ok" if get_openrouter_api_key() else "absent",
        "cohere": "ok" if get_cohere_api_key() else "absent",
        "wh_price": "ok" if get_webhook_update_prices() else "absent",
        "wh_new": "ok" if get_webhook_missing_products() else "absent",
    }


def merged_api_summary() -> dict:
    d = st.session_state.get("api_diag_summary")
    if isinstance(d, dict) and d.get("_from_diag"):
        return {k: v for k, v in d.items() if not str(k).startswith("_")}
    return presence_api_summary()


def api_badges_html() -> str:
    m = merged_api_summary()
    wh_has = bool(get_webhook_update_prices() or get_webhook_missing_products())
    wh_st = "ok" if wh_has else "absent"
    items = [
        ("✨", "Gemini", m.get("gemini", "unknown")),
        ("🔀", "OpenRouter", m.get("openrouter", "unknown")),
        ("◎", "Cohere", m.get("cohere", "unknown")),
        ("🔗", "Make", wh_st),
    ]
    chips = []
    for icon, label, stt in items:
        col, hint = API_STATUS_HINT.get(stt, ("#9E9E9E", "?"))
        chips.append(
            f'<span title="{html_escape(hint)}" style="display:inline-flex;align-items:center;gap:3px;'
            f"background:{col}18;border:1px solid {col};color:{col};border-radius:999px;"
            f'padding:3px 9px;font-size:0.72rem;font-weight:700;margin:2px">{icon} {label}</span>'
        )
    return (
        '<div style="display:flex;flex-wrap:wrap;gap:4px;justify-content:center;'
        f'margin-top:6px">{"".join(chips)}</div>'
        '<p style="font-size:0.65rem;color:#888;text-align:center;margin:4px 0 0 0">'
        "🟢 يعمل · 🟠 حد · 🟤 فاتورة/رصيد · 🔴 خطأ · ⚪ غير مضاف — "
        "<b>شغّل «تشخيص شامل» من الإعدادات</b> لتحديث دقيق</p>"
    )


def settings_api_card_html(name: str, icon: str, stt: str) -> str:
    col, hint = API_STATUS_HINT.get(stt, ("#9E9E9E", "?"))
    return (
        f'<div style="border-right:4px solid {col};background:{col}10;border-radius:8px;'
        f'padding:10px 12px;margin-bottom:8px">'
        f'<div style="font-weight:800;color:{col};font-size:1rem">{icon} {html_escape(name)}</div>'
        f'<div style="color:#666;font-size:0.85rem">{html_escape(hint)}</div></div>'
    )
