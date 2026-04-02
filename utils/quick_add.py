"""
منتج سريع (Quick Add) — صف واحد لاستيراد سلة بصيغة «بيانات المنتج» (نفس helpers: 40 عموداً).
- fill_row: بناء قاموس يطابق مدخلات _missing_row_to_salla_cells
- standardize_product_name: تسمية موحّدة لمتجر مهووس
- ai_enrich_product_row / ai_generate: تجهيز الوصف الغني عند التصدير (Gemini عبر make_salla_desc_fn)
- sniff_product_page: لمسة خفيفة من رابط لصفحة منتج
"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

import requests

_RE_MULTI_SPACE = re.compile(r"\s+")
_RE_META_OG = re.compile(
    r'<meta[^>]+property=["\']og:(title|image)["\'][^>]+content=["\']([^"\']+)["\']',
    re.I,
)
_RE_META_OG_REV = re.compile(
    r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:(title|image)["\']',
    re.I,
)
_RE_TITLE = re.compile(r"<title[^>]*>([^<]+)</title>", re.I)


def standardize_product_name(
    product_kind: str,
    core_name: str,
    brand: str,
    concentration: str = "",
    size_ml: Any = "",
) -> str:
    """
    إعادة صياغة اسم العرض: نوع + الاسم + الماركة + التركيز (+ الحجم إن وُجد).
    مثال: عطر Sauvage Dior EDP 100ml
    """
    parts: list[str] = []
    for p in (product_kind, core_name, brand, concentration):
        s = str(p or "").strip()
        if s and s.lower() not in ("nan", "none", "—"):
            parts.append(s)
    base = _RE_MULTI_SPACE.sub(" ", " ".join(parts)).strip()
    if size_ml is None or str(size_ml).strip() in ("", "nan", "None"):
        return base
    raw = str(size_ml).strip().lower().replace("مل", "ml")
    raw = raw.replace(" ", "")
    m = re.search(r"([\d.]+)\s*ml", raw, re.I)
    if m:
        try:
            v = float(m.group(1))
            if v > 0:
                suf = f"{int(v)}ml" if v == int(v) else f"{v}ml"
                return f"{base} {suf}".strip() if base else suf
        except ValueError:
            pass
    return f"{base} {size_ml}".strip() if base else str(size_ml)


def fill_row(
    *,
    name: str,
    brand: str,
    price: float,
    image_url: str = "",
    category_path: str = "",
    competitor: str = "يدوي",
) -> Dict[str, Any]:
    """
    بناء صف «مفقود» يمرّ عبر export_missing_products_to_salla_csv_bytes
    و validate_export_product_dataframe (بعد ensure_export_brands).
    """
    return {
        "منتج_المنافس": str(name or "").strip(),
        "الماركة": str(brand or "").strip(),
        "سعر_المنافس": float(price),
        "صورة_المنافس": str(image_url or "").strip(),
        "تصنيف_مرجعي": str(category_path or "").strip(),
        "المنافس": str(competitor or "يدوي").strip(),
    }


def ai_generate(
    product_name: str,
    price: float,
    *,
    frag_info: Optional[Dict[str, Any]] = None,
) -> str:
    """
    توليد وصف HTML تسويقي (نفس مسار المفقودات — Gemini عبر generate_mahwous_description).
    """
    try:
        from engines.ai_engine import generate_mahwous_description
    except ImportError:
        from ai_engine import generate_mahwous_description
    fi = frag_info if isinstance(frag_info, dict) else {}
    return generate_mahwous_description(str(product_name), float(price), fi)


def ai_enrich_product_row(row: Dict[str, Any], use_ai: bool) -> Dict[str, Any]:
    """
    عند use_ai يُكمّل الصف بمعلومات Fragrantica عبر fetch_fragrantica_info
    لاستخدامها لاحقاً في make_salla_desc_fn أو استدعاء ai_generate يدوياً.
    """
    if not use_ai:
        return row
    name = str(row.get("منتج_المنافس", "") or "").strip()
    if not name:
        return row
    try:
        from engines.ai_engine import fetch_fragrantica_info
    except ImportError:
        from ai_engine import fetch_fragrantica_info
    try:
        row = dict(row)
        row["_fragrantica_cache"] = fetch_fragrantica_info(name)
    except Exception:
        pass
    return row


def sniff_product_page(url: str) -> Dict[str, str]:
    """
    جلب خفيف لصفحة منتج: title, image (og), error.
    لا يضمن استخراج السعر — يُفضَّل إدخال السعر يدوياً.
    """
    out: Dict[str, str] = {"title": "", "image": "", "error": ""}
    u = (url or "").strip()
    if not u:
        out["error"] = "رابط فارغ"
        return out
    if not u.startswith(("http://", "https://")):
        u = "https://" + u.lstrip("/")
    try:
        r = requests.get(
            u,
            timeout=20,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
            },
        )
        if r.status_code != 200:
            out["error"] = f"HTTP {r.status_code}"
            return out
        html = r.text[:800_000]
        for rx in (_RE_META_OG, _RE_META_OG_REV):
            for m in rx.finditer(html):
                if m.group(1).lower() == "title" and not out["title"]:
                    out["title"] = _html_unescape(m.group(2).strip())
                if m.group(1).lower() == "image" and not out["image"]:
                    out["image"] = m.group(2).strip()
        if not out["title"]:
            tm = _RE_TITLE.search(html)
            if tm:
                out["title"] = _html_unescape(tm.group(1).strip())
    except Exception as e:
        out["error"] = str(e)[:200]
    return out


def _html_unescape(s: str) -> str:
    try:
        from html import unescape

        return unescape(s)
    except Exception:
        return s


def render_quick_add_tab() -> None:
    """واجهة Streamlit: رابط أو يدوي + تصدير CSV سلة (مع تحقق mahwous_core)."""
    import pandas as pd
    import streamlit as st

    from engines.mahwous_core import ensure_export_brands, validate_export_product_dataframe
    from utils.helpers import export_missing_products_to_salla_csv_bytes, make_salla_desc_fn

    st.caption(
        "يُصدّر صفاً واحداً بصيغة **بيانات المنتج** لسلة (نفس مسار المفقودات — أعمدة القالب في `utils/helpers`). "
        "التحقق النهائي قبل التحميل عبر `validate_export_product_dataframe`."
    )
    mode = st.radio(
        "طريقة الإدخال",
        ["من رابط صفحة", "إدخال يدوي"],
        horizontal=True,
        key="qa_mode",
    )

    title = ""
    img_url = ""
    if mode == "من رابط صفحة":
        st.text_input("رابط المنتج (HTTPS)", key="qa_url", placeholder="https://...")
        if st.button("🔍 جلب عنوان وصورة تقريبية", key="qa_sniff"):
            with st.spinner("جاري الجلب..."):
                sn = sniff_product_page(str(st.session_state.get("qa_url") or ""))
                if sn.get("error"):
                    st.warning(sn["error"])
                st.session_state["qa_sniff_title"] = sn.get("title", "")
                st.session_state["qa_sniff_img"] = sn.get("image", "")
                if sn.get("image"):
                    st.session_state["qa_img"] = str(sn["image"])[:2000]
                if sn.get("title"):
                    st.session_state["qa_core"] = str(sn["title"])[:300]
                st.rerun()
        _tit = str(st.session_state.get("qa_sniff_title") or "")
        if _tit:
            st.caption(f"عنوان مُستخرج: **{_tit[:120]}**")

    c1, c2 = st.columns(2)
    with c1:
        kind = st.text_input("نوع المنتج (اختياري)", value="عطر", key="qa_kind")
        st.text_input(
            "اسم المنتج (الجزء المركزي)",
            key="qa_core",
            placeholder="مثال: Sauvage",
        )
    with c2:
        brand = st.text_input("الماركة", key="qa_brand", placeholder="Dior")
        conc = st.text_input("التركيز (اختياري)", key="qa_conc", placeholder="EDP")
        size_ml = st.text_input("الحجم (اختياري)", key="qa_size", placeholder="100")

    price = st.number_input("السعر (ر.س)", min_value=0.01, value=0.01, step=1.0, key="qa_price")
    cat = st.text_input(
        "مسار التصنيف (اختياري — يطابق categories.csv)",
        key="qa_cat",
        placeholder="العطور > عطور رجالية",
    )
    manual_img = st.text_input(
        "رابط صورة المنتج (HTTPS)",
        key="qa_img",
        help="استيراد سلة يتطلب رابطاً عاماً. ارفع الصورة لوسيط خارجي ثم الصق الرابط.",
    )

    use_ai = st.checkbox("وصف تسويقي + هرم عطري بالذكاء الاصطناعي (Gemini عند التصدير)", value=False, key="qa_ai")

    _core = str(st.session_state.get("qa_core") or "").strip()
    _sniff_t = str(st.session_state.get("qa_sniff_title") or "").strip()
    _auto_name = standardize_product_name(kind, _core or _sniff_t, brand, conc, size_ml)
    if st.button("🧩 تطبيق تسمية مهووس موحّدة", key="qa_std"):
        st.session_state["qa_final_name"] = _auto_name
        st.rerun()
    if "qa_final_name" not in st.session_state:
        st.session_state["qa_final_name"] = _auto_name
    st.caption(f"معاينة تلقائية: **{_auto_name}**")
    st.text_input("الاسم النهائي للتصدير", key="qa_final_name")
    final_name = str(st.session_state.get("qa_final_name") or "").strip() or _auto_name

    if st.button("📥 تجهيز ملف سلة CSV", type="primary", key="qa_export"):
        row = fill_row(
            name=final_name,
            brand=brand,
            price=float(price),
            image_url=manual_img,
            category_path=cat,
        )
        if use_ai:
            row = ai_enrich_product_row(row, True)
        df = pd.DataFrame([row])
        df = ensure_export_brands(df)
        ok, issues = validate_export_product_dataframe(df)
        if not ok:
            st.error("❌ التحقق فشل — أصلح الأخطاء ثم أعد المحاولة:")
            for it in issues[:30]:
                st.warning(it)
        else:
            kw: Dict[str, Any] = {}
            if use_ai:
                kw["generate_description"] = make_salla_desc_fn(True, 1)
            blob = export_missing_products_to_salla_csv_bytes(df, **kw)
            st.session_state["qa_csv_blob"] = blob
            st.success("✅ جاهز للتحميل")

    if st.session_state.get("qa_csv_blob"):
        st.download_button(
            "📥 تحميل quick_add_salla.csv",
            data=st.session_state["qa_csv_blob"],
            file_name="quick_add_salla_import.csv",
            mime="text/csv; charset=utf-8",
            key="qa_dl",
        )
