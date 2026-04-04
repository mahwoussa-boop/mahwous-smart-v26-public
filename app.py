"""
app.py - نظام التسعير الذكي مهووس v26.0
✅ معالجة خلفية مع حفظ تلقائي
✅ جداول مقارنة بصرية في كل الأقسام
✅ أزرار AI + قرارات لكل منتج
✅ بحث أسعار السوق والمنافسين
✅ بحث mahwous.com للمنتجات المفقودة
✅ تحديث تلقائي للأسعار عند إعادة رفع المنافس
✅ تصدير Make لكل منتج وللمجموعات
✅ Gemini Chat مباشر
✅ فلاتر ذكية في كل قسم
✅ تاريخ جميل لكل العمليات
✅ محرك أتمتة ذكي مع قواعد تسعير قابلة للتخصيص (v26.0)
✅ لوحة تحكم الأتمتة متصلة بالتنقل (v26.0)
"""
import html
import streamlit as st
import pandas as pd
import threading
import time
import uuid
from datetime import datetime

try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx
except ImportError:
    try:
        from streamlit.scriptrunner import add_script_run_ctx
    except ImportError:
        def add_script_run_ctx(t): return t

from config import *
from styles import (get_styles, stat_card, vs_card, comp_strip, miss_card,
                    get_sidebar_toggle_js, lazy_img_tag, linked_product_title)
from engines.mahwous_core import validate_export_product_dataframe
from engines.engine import (read_file, run_full_analysis, find_missing_products,
                             smart_missing_barrier,
                             extract_brand, extract_size, extract_type, is_sample,
                             resolve_catalog_columns, detect_input_columns,
                             apply_user_column_map,
                             _first_image_url_from_row)
from engines.ai_engine import (call_ai, verify_match, analyze_product,
                                bulk_verify, suggest_price,
                                search_market_price, search_mahwous,
                                check_duplicate,
                                fetch_fragrantica_info, fetch_product_images,
                                generate_mahwous_description, _parse_seo_json_block,
                                reclassify_review_items, ai_deep_analysis)
from engines.automation import (AutomationEngine, ScheduledSearchManager,
                                 auto_push_decisions, auto_process_review_items,
                                 log_automation_decision, get_automation_log,
                                 get_automation_stats)
from utils.helpers import (apply_filters, get_filter_options, export_to_excel,
                            export_multiple_sheets, parse_pasted_text,
                            safe_float, format_price, format_diff,
                            fetch_og_image_url, favicon_url_for_site,
                            fetch_page_title_from_url)
from utils.make_helper import (send_price_updates, send_new_products,
                                send_missing_products, send_single_product,
                                verify_webhook_connection, export_to_make_format,
                                send_batch_smart)
from utils.salla_shamel_export import export_to_salla_shamel
from utils.data_helpers import (safe_results_for_json, restore_results_from_json,
                                ts_badge, decision_badge,
                                row_media_urls_from_analysis,
                                our_product_url_from_row,
                                competitor_product_url_from_row)
from utils.db_manager import (init_db, log_event, log_decision,
                               log_analysis, get_events, get_decisions,
                               get_analysis_history, upsert_price_history,
                               get_price_history, get_price_changes,
                               save_job_progress, get_job_progress, get_last_job,
                               save_hidden_product, get_hidden_product_keys,
                               init_db_v26, upsert_our_catalog, upsert_comp_catalog,
                               save_processed, get_processed, undo_processed,
                               get_processed_keys, migrate_db_v26)


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_thumb_from_product_url(page_url: str) -> str:
    """صورة معاينة من صفحة المنتج عندما لا يوجد عمود صورة في الجدول المحفوظ."""
    u = (page_url or "").strip()
    if not u.startswith("http"):
        return ""
    og = fetch_og_image_url(u)
    if og:
        return og
    return favicon_url_for_site(u)


@st.cache_data(ttl=86400, show_spinner=False)
def _cached_title_from_product_url(page_url: str) -> str:
    """عنوان المنتج من og:title / <title> عندما يكون الاسم مخزّناً كرابط."""
    return fetch_page_title_from_url(page_url) or ""


# ── إعداد الصفحة ──────────────────────────
st.set_page_config(page_title=APP_TITLE, page_icon=APP_ICON,
                   layout="wide", initial_sidebar_state="expanded")
st.markdown(get_styles(), unsafe_allow_html=True)
st.markdown(get_sidebar_toggle_js(), unsafe_allow_html=True)
try:
    init_db()
    init_db_v26()
    migrate_db_v26()  # v26.0 — ترحيل آمن (idempotent)
except Exception as e:
    st.error(f"Database Initialization Error: {e}")

# ── Session State ─────────────────────────
_defaults = {
    "results": None, "missing_df": None, "analysis_df": None,
    "job_id": None, "job_running": False,
    "decisions_pending": {},   # {product_name: action}
    "our_df": None, "comp_dfs": None,  # حفظ الملفات للمنتجات المفقودة
    "hidden_products": set(),  # منتجات أُرسلت لـ Make أو أُزيلت
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# تحميل المنتجات المخفية من قاعدة البيانات عند كل تشغيل
_db_hidden = get_hidden_product_keys()
st.session_state.hidden_products = st.session_state.hidden_products | _db_hidden

# ════════════════════════════════════════════════
#  دوال المعالجة — يجب تعريفها قبل استخدامها
# ════════════════════════════════════════════════
def _split_results(df):
    """تقسيم نتائج التحليل على الأقسام بأمان تام"""
    def _contains(col, txt):
        try:
            return df[col].str.contains(txt, na=False, regex=False)
        except Exception:
            return pd.Series([False] * len(df))
    return {
        "price_raise": df[_contains("القرار", "أعلى")].reset_index(drop=True),
        "price_lower": df[_contains("القرار", "أقل")].reset_index(drop=True),
        "approved":    df[_contains("القرار", "موافق")].reset_index(drop=True),
        "review":      df[_contains("القرار", "مراجعة")].reset_index(drop=True),
        "excluded":    df[_contains("القرار", "مستبعد")].reset_index(drop=True),
        "all":         df,
    }


def _analysis_mask_for_review_row(adf: pd.DataFrame, row: pd.Series) -> pd.Series:
    """مفتاح مطابقة صف المراجعة مع جدول التحليل الكامل."""
    try:
        oid = str(row.get("معرف_المنتج", "") or "").strip()
        cid = str(row.get("معرف_المنافس", "") or "").strip()
        if oid and oid != "nan" and cid and cid != "nan":
            m = (adf["معرف_المنتج"].astype(str).str.strip() == oid) & (
                adf["معرف_المنافس"].astype(str).str.strip() == cid
            )
            if m.any():
                return m
        n1 = str(row.get("المنتج", "") or "").strip()
        n2 = str(row.get("منتج_المنافس", "") or "").strip()
        return (adf["المنتج"].astype(str).str.strip() == n1) & (
            adf["منتج_المنافس"].astype(str).str.strip() == n2
        )
    except Exception:
        return pd.Series([False] * len(adf))


def _reclassify_section_to_qarar(section: str):
    """يحوّل قيمة section بعد التطبيع في ai_engine إلى نص عمود القرار."""
    if not section:
        return None
    s = str(section)
    if "مراجعة" in s or s.strip() == "⚠️ تحت المراجعة":
        return None
    if "🔵" in s or ("مفقود" in s and "منتجات" not in s):
        return "🔍 منتجات مفقودة"
    if "🔴" in s or "أعلى" in s:
        return "🔴 سعر أعلى"
    if "🟢" in s or "أقل" in s:
        return "🟢 سعر أقل"
    if "✅" in s or "موافق" in s:
        return "✅ موافق"
    return None


def _apply_reclassify_to_analysis(adf: pd.DataFrame, review_df: pd.DataFrame,
                                  rc_results: list, min_conf: float = 75.0):
    """
    يحدّث عمود القرار في analysis_df حسب نتائج reclassify_review_items.
    يعيد (الجدول المحدث، إحصاءات).
    """
    stats = {
        "applied": 0, "skip_conf": 0, "skip_review": 0, "skip_idx": 0,
        "skip_no_row": 0, "skip_no_qarar": 0,
    }
    if adf is None or adf.empty or not rc_results:
        return adf, stats
    out = adf.copy()
    batch = review_df.head(30).reset_index(drop=True)
    nbatch = len(batch)
    for rc in rc_results:
        try:
            conf = float(rc.get("confidence") or 0)
        except Exception:
            conf = 0.0
        if conf < min_conf:
            stats["skip_conf"] += 1
            continue
        sec = rc.get("section", "")
        qarar = _reclassify_section_to_qarar(sec)
        if qarar is None:
            stats["skip_review"] += 1
            continue
        try:
            idx = int(rc.get("idx", 0) or 0)
        except Exception:
            idx = 0
        if idx < 1 or idx > nbatch:
            stats["skip_idx"] += 1
            continue
        row = batch.iloc[idx - 1]
        mask = _analysis_mask_for_review_row(out, row)
        if not mask.any():
            stats["skip_no_row"] += 1
            continue
        out.loc[mask, "القرار"] = qarar
        stats["applied"] += 1
    return out, stats


def _persist_analysis_after_reclassify(adf: pd.DataFrame):
    """يحدّث job_progress إن وُجد job_id وحالة done."""
    jid = st.session_state.get("job_id")
    if not jid:
        return
    try:
        job = get_job_progress(jid)
        if not job or str(job.get("status", "")) != "done":
            return
        miss = job.get("missing") if isinstance(job.get("missing"), list) else []
        save_job_progress(
            jid,
            int(job.get("total") or len(adf)),
            int(job.get("processed") or len(adf)),
            safe_results_for_json(adf.to_dict("records")),
            "done",
            str(job.get("our_file") or ""),
            str(job.get("comp_files") or ""),
            missing=miss,
        )
    except Exception:
        pass


# ── تحميل تلقائي للنتائج المحفوظة عند فتح التطبيق ──
if st.session_state.results is None and not st.session_state.job_running:
    _auto_job = get_last_job()
    if _auto_job and _auto_job["status"] == "done" and _auto_job.get("results"):
        _auto_records = restore_results_from_json(_auto_job["results"])
        _auto_df = pd.DataFrame(_auto_records)
        if not _auto_df.empty:
            _auto_miss = pd.DataFrame(_auto_job.get("missing", [])) if _auto_job.get("missing") else pd.DataFrame()
            _auto_r = _split_results(_auto_df)
            _auto_r["missing"] = _auto_miss
            st.session_state.results     = _auto_r
            st.session_state.analysis_df = _auto_df
            st.session_state.job_id      = _auto_job.get("job_id")


# ── دوال مساعدة ───────────────────────────
def db_log(page, action, details=""):
    try: log_event(page, action, details)
    except: pass


def _effective_column_map(df: pd.DataFrame, key_prefix: str):
    """
    يقرأ اختيارات القوائم المنسدلة (إن وُجدت) وإلا يعود لنتيجة التعرف التلقائي.
    """
    if df is None or df.empty:
        return {"name": None, "price": None, "id_col": None, "img": None, "url": None}
    rc = resolve_catalog_columns(df)
    skip = "— (تخطي)"
    cols = {str(c) for c in df.columns}

    def _one(suffix: str, fallback_raw):
        k = f"{key_prefix}_{suffix}"
        v = st.session_state.get(k)
        fb = str(fallback_raw or "").strip()
        if v is None or v == skip:
            return fb if fb and fb in cols else None
        sv = str(v).strip()
        if sv == skip or sv not in cols:
            return fb if fb and fb in cols else None
        return sv

    return {
        "name": _one("name", rc.get("name")),
        "price": _one("price", rc.get("price")),
        "id_col": _one("id", rc.get("id")),
        "img": _one("img", rc.get("img")),
        "url": _one("url", rc.get("url")),
    }


def _render_column_mapping_expander(df: pd.DataFrame, key_prefix: str):
    """
    تحديد الأعمدة بقوائم منسدلة + معاينة صفوف قابلة للضبط + 5 قيم من عمود واحد.
    """
    if df is None or df.empty:
        st.warning("ملف فارغ أو غير مقروء")
        return
    rc = resolve_catalog_columns(df)
    cols_list = [str(c) for c in df.columns]
    skip = "— (تخطي)"
    options = [skip] + cols_list
    n_total = len(df)

    def _ix(fallback_raw):
        fb = str(fallback_raw or "").strip()
        if fb and fb in options:
            return options.index(fb)
        return 0

    st.caption(f"📊 **{len(cols_list)}** عمود — اضبط الأدوار أو اترك التعرف التلقائي")
    if len(cols_list) <= 4:
        st.caption("أسماء الأعمدة: " + "، ".join(f"«{c}»" for c in cols_list))
    g1, g2 = st.columns(2)
    with g1:
        st.selectbox("🏷️ اسم المنتج", options, index=_ix(rc.get("name")), key=f"{key_prefix}_name")
        st.selectbox("💰 السعر", options, index=_ix(rc.get("price")), key=f"{key_prefix}_price")
        st.selectbox("🔢 المعرف / SKU", options, index=_ix(rc.get("id")), key=f"{key_prefix}_id")
    with g2:
        st.selectbox("🖼️ صورة المنتج", options, index=_ix(rc.get("img")), key=f"{key_prefix}_img")
        st.selectbox("🔗 رابط المنتج", options, index=_ix(rc.get("url")), key=f"{key_prefix}_url")

    st.markdown("**عرض صفوف الملف**")
    pr1, pr2 = st.columns([1, 2])
    with pr1:
        n_preview = st.number_input(
            "عدد الصفوف",
            min_value=1,
            max_value=min(n_total, 500),
            value=min(5, n_total),
            step=1,
            key=f"{key_prefix}_preview_rows",
            help="معاينة من بداية الملف (كل الأعمدة).",
        )
    with pr2:
        st.caption(f"إجمالي الصفوف في الملف: **{n_total}**")
    _n = int(n_preview)
    st.dataframe(
        df.head(_n),
        use_container_width=True,
        height=min(520, 100 + _n * 28 + len(cols_list) * 2),
    )

    st.markdown("**معاينة — 5 قيم من عمود واحد**")
    peek_opts = ["— اختر عموداً —"] + cols_list
    pc = st.selectbox("العمود", peek_opts, key=f"{key_prefix}_peek")
    if pc and not str(pc).startswith("—"):
        try:
            st.dataframe(df[[pc]].head(5), use_container_width=True)
        except Exception:
            st.caption("تعذر عرض هذا العمود.")

    with st.expander("🔧 JSON — تفاصيل التعرف الخام", expanded=False):
        st.json(detect_input_columns(df))


def _run_analysis_background(job_id, our_df, comp_dfs, our_file_name, comp_names):
    """تعمل في thread منفصل — تحفظ النتائج كل 10 منتجات مع حماية شاملة من الأخطاء"""
    total     = len(our_df)
    processed = 0
    _last_save = [0]  # آخر عدد تم حفظه (mutable لـ closure)

    def progress_cb(pct, current_results):
        nonlocal processed
        processed = int(pct * total)
        # حفظ كل 25 منتجاً أو عند الاكتمال (تقليل ضغط SQLite)
        if processed - _last_save[0] >= 25 or processed >= total:
            _last_save[0] = processed
            try:
                safe_res = safe_results_for_json(current_results)
                save_job_progress(
                    job_id, total, processed,
                    safe_res,
                    "running",
                    our_file_name, comp_names
                )
            except Exception as _save_err:
                # لا نوقف المعالجة بسبب خطأ حفظ جزئي
                import traceback
                traceback.print_exc()

    analysis_df = pd.DataFrame()
    missing_df  = pd.DataFrame()

    # ── المرحلة 1: التحليل الرئيسي ──────────────────────────────────
    try:
        analysis_df = run_full_analysis(
            our_df, comp_dfs,
            progress_callback=progress_cb
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        # حفظ ما تم تحليله حتى الآن كنتائج جزئية
        save_job_progress(
            job_id, total, processed,
            [], f"error: تحليل المقارنة فشل — {str(e)[:200]}",
            our_file_name, comp_names
        )
        return

    # ── المرحلة 2: حفظ تاريخ الأسعار (لا يوقف المعالجة إذا فشل) ────
    try:
        for _, row in analysis_df.iterrows():
            if safe_float(row.get("نسبة_التطابق", 0)) > 0:
                upsert_price_history(
                    str(row.get("المنتج",       "")),
                    str(row.get("المنافس",       "")),
                    safe_float(row.get("سعر_المنافس", 0)),
                    safe_float(row.get("السعر",       0)),
                    safe_float(row.get("الفرق",        0)),
                    safe_float(row.get("نسبة_التطابق", 0)),
                    str(row.get("القرار",         ""))
                )
    except Exception:
        pass  # تاريخ الأسعار ثانوي — لا نوقف المعالجة

    # ── المرحلة 3: المنتجات المفقودة (منفصلة عن التحليل) ────────────
    try:
        raw_missing_df = find_missing_products(our_df, comp_dfs)
        missing_df = smart_missing_barrier(raw_missing_df, our_df)
    except Exception as e:
        import traceback
        traceback.print_exc()
        missing_df = pd.DataFrame()  # فشلت المفقودة لكن النتائج الرئيسية محفوظة

    # ── المرحلة 4: الحفظ النهائي ────────────────────────────────────
    try:
        safe_records = safe_results_for_json(analysis_df.to_dict("records"))
        safe_missing = missing_df.to_dict("records") if not missing_df.empty else []

        save_job_progress(
            job_id, total, total,
            safe_records,
            "done",
            our_file_name, comp_names,
            missing=safe_missing
        )
        log_analysis(
            our_file_name, comp_names, total,
            int((analysis_df.get("نسبة_التطابق", pd.Series(dtype=float)) > 0).sum()),
            len(missing_df)
        )
    except Exception as e:
        import traceback
        traceback.print_exc()
        # محاولة أخيرة — حفظ بدون missing
        try:
            save_job_progress(
                job_id, total, total,
                safe_results_for_json(analysis_df.to_dict("records")),
                "done",
                our_file_name, comp_names,
                missing=[]
            )
        except Exception:
            save_job_progress(
                job_id, total, processed,
                [], f"error: فشل الحفظ النهائي — {str(e)[:200]}",
                our_file_name, comp_names
            )


def _find_analysis_row_for_processed(product_name: str):
    """
    يعيد صف التحليل المطابق لسجل «تمت المعالجة»: اسم منتجنا أو اسم المنتج عند المنافس.
    يبحث في analysis_df ثم في أقسام results (والجدول الكامل all).
    """
    pn = str(product_name or "").strip()
    if not pn:
        return None

    def _match_df(df):
        if df is None or getattr(df, "empty", True):
            return None
        for col in ("المنتج", "منتج_المنافس"):
            if col not in df.columns:
                continue
            try:
                m = df[df[col].astype(str).str.strip() == pn]
                if not m.empty:
                    return m.iloc[0]
            except Exception:
                continue
        return None

    adf = st.session_state.get("analysis_df")
    r = _match_df(adf)
    if r is not None:
        return r

    res = st.session_state.get("results") or {}
    for key in ("all", "price_raise", "price_lower", "approved", "review", "excluded", "missing"):
        r = _match_df(res.get(key))
        if r is not None:
            return r
    return None


def _lookup_images_from_analysis_session(product_name: str):
    """صورة منتجنا + صورة المنافس من جلسة التحليل أو أقسام النتائج."""
    row = _find_analysis_row_for_processed(product_name)
    if row is None:
        return "", ""
    try:
        return row_media_urls_from_analysis(row)
    except Exception:
        return "", ""


def _lookup_product_urls_from_analysis_session(product_name: str):
    """رابط منتجنا + رابط صفحة المنتج عند المنافس."""
    row = _find_analysis_row_for_processed(product_name)
    if row is None:
        return "", ""
    try:
        return our_product_url_from_row(row), competitor_product_url_from_row(row)
    except Exception:
        return "", ""


def _processed_dual_image_html(our_img: str, comp_img: str, title_our: str, title_comp: str) -> str:
    """خليتان للصور: منتجنا | المنافس — تحميل eager حتى تظهر فوراً في Streamlit."""
    w, h = 56, 56

    def _slot(label: str, url: str, alt: str) -> str:
        if url and str(url).strip():
            img = lazy_img_tag(url, w, h, alt, loading="eager")
        else:
            img = (
                f'<div style="width:{w}px;height:{h}px;border-radius:8px;background:#121c2e;'
                f'border:1px dashed #2a3f5f;display:flex;align-items:center;justify-content:center;'
                f'color:#4a5c78;font-size:.75rem">—</div>'
            )
        return (
            f'<div style="display:flex;flex-direction:column;align-items:center;gap:5px;min-width:64px">'
            f'<span style="font-size:.68rem;color:#7eb8ff;font-weight:800;letter-spacing:.02em">{label}</span>'
            f"{img}</div>"
        )

    return (
        '<div style="display:flex;gap:16px;flex-shrink:0;align-items:flex-end;padding:2px 0">'
        f'{_slot("منتجنا", our_img, title_our[:40])}'
        f'{_slot("المنافس", comp_img, title_comp[:40])}'
        "</div>"
    )


def _is_http_url_text(s) -> bool:
    t = str(s or "").strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _humanize_competitor_upload(comp: str) -> str:
    """اسم ملف CSV/Excel → اسم متجر مقروء للعرض (بدون الامتداد)."""
    c = str(comp or "").strip()
    if not c:
        return "—"
    low = c.lower()
    for ext in (".csv", ".xlsx", ".xls", ".tsv", ".ods"):
        if low.endswith(ext):
            return c[: -len(ext)].strip() or c
    return c


def _display_name_for_missing_row(row) -> str:
    """
    اسم عرض للمفقودات: يفضّل نصاً حقيقياً من أي عمود معروف قبل اعتبار الاسم رابطاً فقط.
    """
    def _clean(v):
        x = str(v or "").strip()
        if not x or x.lower() in ("nan", "none", "<na>"):
            return ""
        return x

    for key in (
        "المنتج",
        "اسم المنتج",
        "اسم_المنتج",
        "Product",
        "Name",
        "name",
        "title",
        "الاسم",
        "منتج_المنافس",
    ):
        if key not in row.index:
            continue
        v = _clean(row.get(key))
        if v and not _is_http_url_text(v):
            return v

    br = _clean(row.get("الماركة"))
    sz = _clean(row.get("الحجم"))
    pt = _clean(row.get("النوع"))
    chunks = [c for c in (br, sz, pt) if c]
    if chunks:
        return " · ".join(chunks)

    return ""


def _processed_row_url_chips_html(our_url: str, comp_url: str) -> str:
    """روابط مختصرة بجانب سطر الملاحظات في «تمت المعالجة»."""
    parts = []
    ou = (our_url or "").strip()
    cu = (comp_url or "").strip()
    if ou.startswith("http"):
        parts.append(
            f'<a href="{html.escape(ou, quote=True)}" target="_blank" rel="noopener noreferrer" '
            f'style="color:#4fc3f7;font-size:.73rem;font-weight:600;text-decoration:underline">🔗 رابط منتجنا</a>'
        )
    if cu.startswith("http"):
        parts.append(
            f'<a href="{html.escape(cu, quote=True)}" target="_blank" rel="noopener noreferrer" '
            f'style="color:#ff9800;font-size:.73rem;font-weight:600;text-decoration:underline">🔗 عند المنافس</a>'
        )
    if not parts:
        return ""
    return '<span style="margin-right:8px">&nbsp;|&nbsp;</span>' + '<span style="margin:0 4px;color:#555">·</span>'.join(parts)


# ════════════════════════════════════════════════
#  مكوّن جدول المقارنة البصري (مشترك)
# ════════════════════════════════════════════════
def render_pro_table(df, prefix, section_type="update", show_search=True):
    """
    جدول احترافي بصري مع:
    - فلاتر ذكية
    - أزرار AI + قرار لكل منتج
    - تصدير Make
    - Pagination
    """
    if df is None or df.empty:
        st.info("لا توجد منتجات")
        return

    # ── فلاتر ─────────────────────────────────
    opts = get_filter_options(df)
    with st.expander("🔍 فلاتر متقدمة", expanded=False):
        c1, c2, c3, c4 = st.columns(4)
        search   = c1.text_input("🔎 بحث",    key=f"{prefix}_s")
        brand_f  = c2.selectbox("🏷️ الماركة", opts["brands"],      key=f"{prefix}_b")
        comp_f   = c3.selectbox("🏪 المنافس", opts["competitors"], key=f"{prefix}_c")
        type_f   = c4.selectbox("🧴 النوع",   opts["types"],       key=f"{prefix}_t")
        c5, c6, c7 = st.columns(3)
        match_min  = c5.slider("أقل تطابق%", 0, 100, 0, key=f"{prefix}_m")
        price_min  = c6.number_input("سعر من", 0.0, key=f"{prefix}_p1")
        price_max  = c7.number_input("سعر لـ", 0.0, key=f"{prefix}_p2")

    filters = {
        "search": search, "brand": brand_f, "competitor": comp_f,
        "type": type_f,
        "match_min": match_min if match_min > 0 else None,
        "price_min": price_min if price_min > 0 else 0.0,
        "price_max": price_max if price_max > 0 else None,
    }
    filtered = apply_filters(df, filters)

    # ── شريط الأدوات ───────────────────────────
    ac1, ac2, ac3, ac4, ac5 = st.columns(5)
    with ac1:
        _exdf = filtered.copy()
        if "جميع المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع_المنافسين"])
        excel_data = export_to_excel(_exdf, prefix)
        st.download_button("📥 Excel", data=excel_data,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{prefix}_xl")
    with ac2:
        _csdf = filtered.copy()
        if "جميع المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع_المنافسين"])
        _csv_bytes = _csdf.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📄 CSV", data=_csv_bytes,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv", key=f"{prefix}_csv")
    with ac3:
        _bulk_labels = {"raise": "🤖 تحليل ذكي — خفض (أول 20)",
                        "lower": "🤖 تحليل ذكي — رفع (أول 20)",
                        "review": "🤖 تحقق جماعي (أول 20)",
                        "approved": "🤖 مراجعة (أول 20)"}
        if st.button(_bulk_labels.get(prefix, "🤖 AI جماعي (أول 20)"), key=f"{prefix}_bulk"):
            with st.spinner("🤖 AI يحلل البيانات..."):
                _section_map = {"raise": "price_raise", "lower": "price_lower",
                                "review": "review", "approved": "approved"}
                items = [{
                    "our": str(r.get("المنتج", "")),
                    "comp": str(r.get("منتج_المنافس", "")),
                    "our_price": safe_float(r.get("السعر", 0)),
                    "comp_price": safe_float(r.get("سعر_المنافس", 0))
                } for _, r in filtered.head(20).iterrows()]
                res = bulk_verify(items, _section_map.get(prefix, "general"))
                st.markdown(f'<div class="ai-box">{res["response"]}</div>',
                            unsafe_allow_html=True)
    with ac4:
        if section_type == "excluded":
            st.caption("إرسال Make غير متاح لهذا القسم")
        elif st.button("📤 إرسال كل لـ Make", key=f"{prefix}_make_all"):
            products = export_to_make_format(filtered, section_type)
            if section_type in ("missing", "new"):
                res = send_new_products(products)
            else:
                res = send_price_updates(products)
            if res["success"]:
                st.success(res["message"])
                # v26: سجّل كل منتج في processed_products
                for _i, (_idx, _r) in enumerate(filtered.iterrows()):
                    _pname = str(_r.get("المنتج", _r.get("منتج_المنافس", "")))
                    _pkey  = f"{prefix}_{_pname}_{_i}"
                    _pid_r = str(_r.get("معرف_المنتج", _r.get("معرف_المنافس", "")))
                    _comp  = str(_r.get("المنافس",""))
                    _op    = safe_float(_r.get("السعر", _r.get("سعر_المنافس", 0)))
                    _np    = safe_float(_r.get("سعر_المنافس", _r.get("السعر", 0)))
                    st.session_state.hidden_products.add(_pkey)
                    save_hidden_product(_pkey, _pname, "sent_to_make_bulk")
                    save_processed(_pkey, _pname, _comp, "send_price",
                                   old_price=_op, new_price=_np,
                                   product_id=_pid_r,
                                   notes=f"إرسال جماعي ← {prefix}")
                st.rerun()
            else:
                st.error(res["message"])
    with ac5:
        # جمع القرارات المعلقة وإرسالها
        pending = {k: v for k, v in st.session_state.decisions_pending.items()
                   if v["action"] in ["approved", "deferred", "removed"]}
        if pending and st.button(f"📦 ترحيل {len(pending)} قرار → Make", key=f"{prefix}_send_decisions"):
            to_send = [{"name": k, "action": v["action"], "reason": v.get("reason", "")}
                       for k, v in pending.items()]
            res = send_price_updates(to_send)
            st.success(f"✅ تم إرسال {len(to_send)} قرار لـ Make")
            # v26: سجّل القرارات المعلقة في processed_products
            for k, v in pending.items():
                _pkey = f"decision_{k}"
                _act  = v.get("action","approved")
                save_processed(_pkey, k, v.get("competitor",""), _act,
                               old_price=safe_float(v.get("our_price",0)),
                               new_price=safe_float(v.get("comp_price",0)),
                               notes=f"قرار معلق → Make | {v.get('reason','')}")
            st.session_state.decisions_pending = {}
            st.rerun()

    st.caption(f"عرض {len(filtered)} من {len(df)} منتج — {datetime.now().strftime('%H:%M:%S')}")

    # ── Pagination ─────────────────────────────
    PAGE_SIZE = 25
    total_pages = max(1, (len(filtered) + PAGE_SIZE - 1) // PAGE_SIZE)
    if total_pages > 1:
        page_num = st.number_input("الصفحة", 1, total_pages, 1, key=f"{prefix}_pg")
    else:
        page_num = 1
    start = (page_num - 1) * PAGE_SIZE
    page_df = filtered.iloc[start:start + PAGE_SIZE]

       # ── الجدول البصري ─────────────────────
    for idx, row in page_df.iterrows():
        our_name   = str(row.get("المنتج", "—"))
        # تخطي المنتجات التي أُرسلت لـ Make أو أُزيلت
        _hide_key = f"{prefix}_{our_name}_{idx}"
        if _hide_key in st.session_state.hidden_products:
            continue
        comp_name  = str(row.get("منتج_المنافس", "—"))
        our_price  = safe_float(row.get("السعر", 0))
        comp_price = safe_float(row.get("سعر_المنافس", 0))
        diff       = safe_float(row.get("الفرق", our_price - comp_price))
        match_pct  = safe_float(row.get("نسبة_التطابق", 0))
        comp_src   = str(row.get("المنافس", ""))
        brand      = str(row.get("الماركة", ""))
        size       = row.get("الحجم", "")
        ptype      = str(row.get("النوع", ""))
        risk       = str(row.get("الخطورة", ""))
        decision   = str(row.get("القرار", ""))
        ts_now     = datetime.now().strftime("%Y-%m-%d %H:%M")
        _is_excluded = "مستبعد" in decision
        _vs_border = "#9e9e9e" if _is_excluded else None
        _vs_row_bg = "rgba(245,245,245,0.07)" if _is_excluded else None

        # سحب رقم المنتج من جميع الأعمدة المحتملة
        _pid_raw = (
            row.get("معرف_المنتج", "") or
            row.get("product_id", "") or
            row.get("رقم المنتج", "") or
            row.get("رقم_المنتج", "") or
            row.get("معرف المنتج", "") or ""
        )
        _pid_str = ""
        if _pid_raw and str(_pid_raw) not in ("", "nan", "None", "0"):
            try: _pid_str = str(int(float(str(_pid_raw))))
            except: _pid_str = str(_pid_raw)

        _our_img_v, _comp_img_v = row_media_urls_from_analysis(row)
        _comp_url_v = competitor_product_url_from_row(row)
        _our_url_v = our_product_url_from_row(row)

        # بطاقة VS مع رقم المنتج + صور (lazy) عند توفرها
        _vs_html = vs_card(our_name, our_price, comp_name,
                           comp_price, diff, comp_src, _pid_str,
                           our_img=_our_img_v, comp_img=_comp_img_v,
                           comp_url=_comp_url_v, our_url=_our_url_v,
                           accent_border=_vs_border, row_bg=_vs_row_bg)
        st.markdown(_vs_html, unsafe_allow_html=True)

        # شريط المعلومات
        match_color = ("#00C853" if match_pct >= 90
                       else "#FFD600" if match_pct >= 70 else "#FF1744")
        risk_html = ""
        if risk:
            rc = {"حرج": "#FF1744", "عالي": "#FF1744", "متوسط": "#FFD600", "منخفض": "#00C853", "عادي": "#00C853"}.get(risk.replace("🔴 ","").replace("🟡 ","").replace("🟢 ",""), "#888")
            risk_html = f'<span style="color:{rc};font-size:.75rem;font-weight:700">⚡{risk}</span>'

        # تاريخ آخر تغيير سعر
        ph = get_price_history(our_name, comp_src, limit=2)
        price_change_html = ""
        if len(ph) >= 2:
            old_p = ph[1]["price"]
            chg = ph[0]["price"] - old_p
            chg_c = "#FF1744" if chg > 0 else "#00C853"
            price_change_html = f'<span style="color:{chg_c};font-size:.7rem">{"▲" if chg>0 else "▼"}{abs(chg):.0f} منذ {ph[1]["date"]}</span>'

        # قرار معلق؟
        pend = st.session_state.decisions_pending.get(our_name, {})
        pend_html = decision_badge(pend.get("action", "")) if pend else ""

        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
                    padding:3px 12px;font-size:.8rem;flex-wrap:wrap;gap:4px;">
          <span>🏷️ <b>{brand}</b> {size} {ptype}</span>
          <span>تطابق: <b style="color:{match_color}">{match_pct:.0f}%</b></span>
          {risk_html}
          {price_change_html}
          {pend_html}
          {ts_badge(ts_now)}
        </div>""", unsafe_allow_html=True)

        # شريط المنافسين المصغر — يعرض كل المنافسين بأسعارهم
        all_comps = row.get("جميع_المنافسين", row.get("جميع المنافسين", []))
        if isinstance(all_comps, list) and len(all_comps) > 0:
            st.markdown(comp_strip(all_comps), unsafe_allow_html=True)

        # ── أزرار لكل منتج (شريط إجراءات منفصل لسعر أعلى / فرصة رفع) ─────
        _auto_price = round(comp_price - 1, 2) if comp_price > 0 else our_price
        if prefix in ("raise", "lower"):
            prc_col, hint_col = st.columns([1, 4])
            with prc_col:
                _custom_price = st.number_input(
                    "سعر", value=float(_auto_price), min_value=0.0,
                    step=1.0, key=f"cp_{prefix}_{idx}",
                    label_visibility="collapsed"
                )
            with hint_col:
                st.caption("اضبط السعر المستهدف ثم استخدم «تحديث السعر» لإرساله إلى Make.com")
            col_btn1, col_btn2, col_btn3 = st.columns([1, 1, 3])
            with col_btn1:
                if st.button("🚀 تحديث السعر", key=f"update_{prefix}_{idx}"):
                    _pid_raw = (
                        row.get("معرف_المنتج", "") or
                        row.get("product_id", "") or
                        row.get("رقم المنتج", "") or
                        row.get("رقم_المنتج", "") or
                        row.get("معرف المنتج", "") or ""
                    )
                    try:
                        _fv = float(_pid_raw)
                        _pid = str(int(_fv)) if _fv == int(_fv) else str(_pid_raw)
                    except (ValueError, TypeError):
                        _pid = str(_pid_raw).strip()
                    if _pid in ("nan", "None", "NaN", ""):
                        _pid = ""
                    _final_price = _custom_price if _custom_price > 0 else _auto_price
                    res = send_single_product({
                        "product_id": _pid,
                        "name": our_name, "price": _final_price,
                        "comp_name": comp_name, "comp_price": comp_price,
                        "diff": diff, "decision": decision, "competitor": comp_src
                    })
                    if res["success"]:
                        _hk = f"{prefix}_{our_name}_{idx}"
                        st.session_state.hidden_products.add(_hk)
                        save_hidden_product(_hk, our_name, "sent_to_make")
                        save_processed(_hk, our_name, comp_src, "send_price",
                                       old_price=our_price, new_price=_final_price,
                                       product_id=_pid,
                                       notes=f"Make ← {prefix} | منافس: {comp_src} | {comp_price:.0f}→{_final_price:.0f}ر.س")
                        st.rerun()
            with col_btn2:
                if st.button("🗑️ استبعاد", key=f"ignore_{prefix}_{idx}"):
                    st.session_state.decisions_pending[our_name] = {
                        "action": "removed", "reason": "استبعاد",
                        "our_price": our_price, "comp_price": comp_price,
                        "diff": diff, "competitor": comp_src,
                        "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                    }
                    log_decision(our_name, prefix, "removed",
                                 "استبعاد", our_price, comp_price, diff, comp_src)
                    _hk = f"{prefix}_{our_name}_{idx}"
                    st.session_state.hidden_products.add(_hk)
                    save_hidden_product(_hk, our_name, "removed")
                    save_processed(_hk, our_name, comp_src, "removed",
                                   old_price=our_price, new_price=our_price,
                                   product_id=str(row.get("معرف_المنتج", "")),
                                   notes=f"استبعاد من {prefix}")
                    st.rerun()
            with col_btn3:
                st.empty()
            st.markdown("---")

        if prefix in ("raise", "lower"):
            b1, b2, b3, b4, b8, b9 = st.columns([1, 1, 1, 1, 1, 1])
        elif prefix == "approved":
            # قسم الموافقات: بدون تكرار «تحقق» الثاني ولا «تاريخ» — يبقى 🤖 تحقق في b1
            b1, b2, b3, b4, b5, b6, b7 = st.columns(7)
        else:
            b1, b2, b3, b4, b5, b6, b7, b8, b9 = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1])

        with b1:  # AI تحقق ذكي — يُصحح القسم
            _ai_label = {"raise": "🤖 هل نخفض؟", "lower": "🤖 هل نرفع؟",
                         "review": "🤖 هل يطابق؟", "approved": "🤖 تحقق"}.get(prefix, "🤖 تحقق")
            if st.button(_ai_label, key=f"v_{prefix}_{idx}"):
                with st.spinner("🤖 AI يحلل ويتحقق..."):
                    r = verify_match(our_name, comp_name, our_price, comp_price)
                    if r.get("success"):
                        icon = "✅" if r.get("match") else "❌"
                        conf = r.get("confidence", 0)
                        reason = r.get("reason","")[:200]
                        correct_sec = r.get("correct_section","")
                        suggested_price = r.get("suggested_price", 0)

                        # تحديد القسم الحالي من prefix
                        current_sec_map = {
                            "raise": "🔴 سعر أعلى",
                            "lower": "🟢 سعر أقل",
                            "approved": "✅ موافق",
                            "review": "⚠️ تحت المراجعة",
                            "excluded": "⚪ مستبعد (لا يوجد تطابق)",
                        }
                        current_sec = current_sec_map.get(prefix, "")

                        # هل AI يوافق على القسم الحالي؟
                        section_ok = True
                        if correct_sec and current_sec:
                            # مقارنة مبسطة
                            if ("اعلى" in correct_sec or "أعلى" in correct_sec) and prefix != "raise":
                                section_ok = False
                            elif ("اقل" in correct_sec or "أقل" in correct_sec) and prefix != "lower":
                                section_ok = False
                            elif "موافق" in correct_sec and prefix != "approved":
                                section_ok = False
                            elif ("مفقود" in correct_sec or "🔵" in correct_sec) and r.get("match") == False:
                                section_ok = False

                        if r.get("match"):
                            # مطابقة صحيحة — عرض نتيجة السعر
                            diff_info = ""
                            if prefix == "raise":
                                diff_info = f"\n\n💡 **توصية:** {'خفض السعر' if diff > 20 else 'إبقاء السعر'}"
                            elif prefix == "lower":
                                diff_info = f"\n\n💡 **توصية:** {'رفع السعر' if abs(diff) > 20 else 'إبقاء السعر'}"
                            if suggested_price > 0:
                                diff_info += f"\n💰 **السعر المقترح: {suggested_price:,.0f} ر.س**"

                            st.success(f"{icon} **تطابق {conf}%** — المطابقة صحيحة\n\n{reason}{diff_info}")

                            if not section_ok:
                                st.warning(f"⚠️ AI يرى أن هذا المنتج يجب أن يكون في قسم: **{correct_sec}**")
                        else:
                            # مطابقة خاطئة — تنبيه
                            st.error(f"{icon} **المطابقة خاطئة** ({conf}%)\n\n{reason}")
                            st.warning("🔵 هذا المنتج يجب أن يكون في **المنتجات المفقودة**")
                    else:
                        st.error("فشل AI")

        with b2:  # بحث سعر السوق ذكي
            _mkt_label = {"raise": "🌐 سعر عادل؟", "lower": "🌐 فرصة رفع؟"}.get(prefix, "🌐 سوق")
            if st.button(_mkt_label, key=f"mkt_{prefix}_{idx}"):
                with st.spinner("🌐 يبحث في السوق السعودي..."):
                    r = search_market_price(our_name, our_price)
                    if r.get("success"):
                        mp  = r.get("market_price", 0)
                        rng = r.get("price_range", {})
                        rec = r.get("recommendation", "")[:250]
                        web_ctx = r.get("web_context","")
                        comps = r.get("competitors", [])
                        conf = r.get("confidence", 0)

                        _verdict = ""
                        if prefix == "raise" and mp > 0:
                            _verdict = "✅ سعرنا ضمن السوق" if our_price <= mp * 1.1 else "⚠️ سعرنا أعلى من السوق — يُنصح بالخفض"
                        elif prefix == "lower" and mp > 0:
                            _gap = mp - our_price
                            _verdict = f"💰 فرصة رفع ~{_gap:.0f} ر.س" if _gap > 10 else "✅ سعرنا قريب من السوق"

                        _comps_txt = ""
                        if comps:
                            _comps_txt = "\n\n**منافسون:**\n" + "\n".join(
                                f"• {c.get('name','')}: {c.get('price',0):,.0f} ر.س" for c in comps[:3]
                            )

                        _price_range = f"{rng.get('min',0):.0f}–{rng.get('max',0):.0f}" if rng else "—"
                        st.info(
                            f"💹 **سعر السوق: {mp:,.0f} ر.س** ({_price_range} ر.س)\n\n"
                            f"{rec}{_comps_txt}\n\n{'**' + _verdict + '**' if _verdict else ''}"
                        )
                        if web_ctx:
                            with st.expander("🔍 مصادر البحث"):
                                st.caption(web_ctx)
                    else:
                        st.warning("تعذر البحث في السوق")

        with b3:  # موافق
            if st.button("✅ موافق", key=f"ok_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "approved", "reason": "موافقة يدوية",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "approved",
                             "موافقة يدوية", our_price, comp_price, diff, comp_src)
                _hk3 = f"{prefix}_{our_name}_{idx}"
                st.session_state.hidden_products.add(_hk3)
                save_hidden_product(_hk3, our_name, "approved")
                save_processed(_hk3, our_name, comp_src, "approved",
                               old_price=our_price, new_price=our_price,
                               product_id=str(row.get("معرف_المنتج","")),
                               notes=f"موافق من {prefix} | منافس: {comp_src}")
                st.rerun()

        with b4:  # تأجيل
            if st.button("⏸️ تأجيل", key=f"df_{prefix}_{idx}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "deferred", "reason": "تأجيل",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "deferred",
                             "تأجيل", our_price, comp_price, diff, comp_src)
                st.warning("⏸️")

        if prefix not in ("raise", "lower"):
            with b5:  # إزالة
                if st.button("🗑️ إزالة", key=f"rm_{prefix}_{idx}"):
                    st.session_state.decisions_pending[our_name] = {
                        "action": "removed", "reason": "إزالة",
                        "our_price": our_price, "comp_price": comp_price,
                        "diff": diff, "competitor": comp_src,
                        "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                    }
                    log_decision(our_name, prefix, "removed",
                                 "إزالة", our_price, comp_price, diff, comp_src)
                    _hk = f"{prefix}_{our_name}_{idx}"
                    st.session_state.hidden_products.add(_hk)
                    save_hidden_product(_hk, our_name, "removed")
                    save_processed(_hk, our_name, comp_src, "removed",
                                   old_price=our_price, new_price=our_price,
                                   product_id=str(row.get("معرف_المنتج","")),
                                   notes=f"إزالة من {prefix}")
                    st.rerun()

            with b6:  # سعر يدوي
                _auto_price_row = round(comp_price - 1, 2) if comp_price > 0 else our_price
                _custom_price = st.number_input(
                    "سعر", value=_auto_price_row, min_value=0.0,
                    step=1.0, key=f"cp_{prefix}_{idx}",
                    label_visibility="collapsed"
                )

            with b7:  # تصدير Make
                if st.button("📤 Make", key=f"mk_{prefix}_{idx}"):
                    _pid_raw = (
                        row.get("معرف_المنتج", "") or
                        row.get("product_id", "") or
                        row.get("رقم المنتج", "") or
                        row.get("رقم_المنتج", "") or
                        row.get("معرف المنتج", "") or ""
                    )
                    try:
                        _fv = float(_pid_raw)
                        _pid = str(int(_fv)) if _fv == int(_fv) else str(_pid_raw)
                    except (ValueError, TypeError):
                        _pid = str(_pid_raw).strip()
                    if _pid in ("nan", "None", "NaN", ""):
                        _pid = ""
                    _final_price = _custom_price if _custom_price > 0 else _auto_price_row
                    res = send_single_product({
                        "product_id": _pid,
                        "name": our_name, "price": _final_price,
                        "comp_name": comp_name, "comp_price": comp_price,
                        "diff": diff, "decision": decision, "competitor": comp_src
                    })
                    if res["success"]:
                        _hk = f"{prefix}_{our_name}_{idx}"
                        st.session_state.hidden_products.add(_hk)
                        save_hidden_product(_hk, our_name, "sent_to_make")
                        save_processed(_hk, our_name, comp_src, "send_price",
                                       old_price=our_price, new_price=_final_price,
                                       product_id=_pid,
                                       notes=f"Make ← {prefix} | منافس: {comp_src} | {comp_price:.0f}→{_final_price:.0f}ر.س")
                        st.rerun()

        if prefix != "approved":
            with b8:  # تحقق AI — يُصحح القسم (مكرر مع b1 في «موافق» فأُزيل من قسم الموافقات)
                if st.button("🔍 تحقق", key=f"vrf_{prefix}_{idx}"):
                    with st.spinner("🤖 يتحقق..."):
                        _vr2 = verify_match(our_name, comp_name, our_price, comp_price)
                        if _vr2.get("success"):
                            _mc2 = "✅ متطابق" if _vr2.get("match") else "❌ غير متطابق"
                            _conf2 = _vr2.get("confidence",0)
                            _sec2 = _vr2.get("correct_section","")
                            _reason2 = _vr2.get("reason","")[:150]
                            st.markdown(f"{_mc2} {_conf2}%\n\n{_reason2}")
                            if _sec2 and not _vr2.get("match"):
                                st.warning(f"يجب نقله → **{_sec2}**")

            with b9:  # تاريخ السعر
                if st.button("📈 تاريخ", key=f"ph_{prefix}_{idx}"):
                    history = get_price_history(our_name, comp_src)
                    if history:
                        rows_h = [f"📅 {h['date']}: {h['price']:,.0f} ر.س" for h in history[:5]]
                        st.info("\n".join(rows_h))
                    else:
                        st.info("لا يوجد تاريخ بعد")

        st.markdown('<hr style="border:none;border-top:1px solid #1a1a2e;margin:6px 0">', unsafe_allow_html=True)


# ════════════════════════════════════════════════
#  الشريط الجانبي
# ════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"## {APP_ICON} {APP_TITLE}")
    st.caption(f"الإصدار {APP_VERSION}")

    # حالة AI — تشخيص مفصل
    ai_ok = bool(GEMINI_API_KEYS)
    if ai_ok:
        ai_color = "#00C853"
        ai_label = f"🤖 Gemini ✅ ({len(GEMINI_API_KEYS)} مفتاح)"
    else:
        ai_color = "#FF1744"
        ai_label = "🔴 AI غير متصل — تحقق من Secrets"

    st.markdown(
        f'<div style="background:{ai_color}22;border:1px solid {ai_color};'
        f'border-radius:6px;padding:6px;text-align:center;color:{ai_color};'
        f'font-weight:700;font-size:.85rem">{ai_label}</div>',
        unsafe_allow_html=True
    )

    # زر تشخيص سريع
    if not ai_ok:
        if st.button("🔍 تشخيص المشكلة", key="diag_btn"):
            import os
            st.write("**الـ secrets المتاحة:**")
            try:
                available = list(st.secrets.keys())
                for k in available:
                    val = str(st.secrets[k])
                    masked = val[:8] + "..." if len(val) > 8 else val
                    st.write(f"  `{k}` = `{masked}`")
            except Exception as e:
                st.error(f"خطأ: {e}")
            # محاولة مباشرة
            for key_name in ["GEMINI_API_KEYS","GEMINI_API_KEY","GEMINI_KEY_1"]:
                try:
                    v = st.secrets[key_name]
                    st.success(f"✅ وجدت {key_name} = {str(v)[:20]}...")
                except:
                    st.warning(f"❌ {key_name} غير موجود")

    # حالة المعالجة — تحديث حي مع auto-rerun
    if st.session_state.job_id:
        job = get_job_progress(st.session_state.job_id)
        if job:
            if job["status"] == "running":
                pct = job["processed"] / max(job["total"], 1)
                st.progress(min(pct, 0.99),
                            f"⚙️ {job['processed']}/{job['total']} منتج")
                # تحديث تلقائي كل 4 ثوانٍ بدون إعادة تشغيل الكود كاملاً
                try:
                    from streamlit_autorefresh import st_autorefresh
                    st_autorefresh(interval=4000, key="progress_refresh")
                except ImportError:
                    # fallback: rerun عادي إذا لم تكن المكتبة موجودة
                    time.sleep(4)
                    st.rerun()
            elif job["status"] == "done" and st.session_state.job_running:
                # اكتمل — حمّل النتائج تلقائياً مع استعادة القوائم
                if job.get("results"):
                    _restored = restore_results_from_json(job["results"])
                    df_all = pd.DataFrame(_restored)
                    missing_df = pd.DataFrame(job.get("missing", [])) if job.get("missing") else pd.DataFrame()
                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results     = _r
                    st.session_state.analysis_df = df_all
                st.session_state.job_running = False
                st.balloons()
                st.rerun()
            elif job["status"].startswith("error"):
                st.error(f"❌ فشل: {job['status'][7:80]}")
                st.session_state.job_running = False

    page = st.radio("الأقسام", SECTIONS, label_visibility="collapsed")

    st.markdown("---")
    if st.session_state.results:
        r = st.session_state.results
        st.markdown("**📊 ملخص:**")
        for key, icon, label in [
            ("price_raise","🔴","أعلى"), ("price_lower","🟢","أقل"),
            ("approved","✅","موافق"), ("missing","🔍","مفقود"),
            ("review","⚠️","مراجعة"), ("excluded","⚪","مستبعد"),
        ]:
            cnt = len(r.get(key, pd.DataFrame()))
            st.caption(f"{icon} {label}: **{cnt}**")
        # ملخص الثقة للمفقودات
        _miss_df = r.get("missing", pd.DataFrame())
        if not _miss_df.empty and "مستوى_الثقة" in _miss_df.columns:
            _gc = len(_miss_df[_miss_df["مستوى_الثقة"] == "green"])
            _yc = len(_miss_df[_miss_df["مستوى_الثقة"] == "yellow"])
            _rc = len(_miss_df[_miss_df["مستوى_الثقة"] == "red"])
            st.markdown(
                f'<div style="background:#1a1a2e;border-radius:6px;padding:6px;margin-top:4px;font-size:.75rem">'
                f'🟢 مؤكد: <b>{_gc}</b> &nbsp; '
                f'🟡 محتمل: <b>{_yc}</b> &nbsp; '
                f'🔴 مشكوك: <b>{_rc}</b></div>',
                unsafe_allow_html=True)

    # قرارات معلقة
    pending_cnt = len(st.session_state.decisions_pending)
    if pending_cnt:
        st.markdown(f'<div style="background:#FF174422;border:1px solid #FF1744;'
                    f'border-radius:6px;padding:6px;text-align:center;color:#FF1744;'
                    f'font-size:.8rem">📦 {pending_cnt} قرار معلق</div>',
                    unsafe_allow_html=True)


# ════════════════════════════════════════════════
#  1. لوحة التحكم
# ════════════════════════════════════════════════
if page == "📊 لوحة التحكم":
    st.header("📊 لوحة التحكم")
    db_log("dashboard", "view")

    # تغييرات الأسعار
    changes = get_price_changes(7)
    if changes:
        st.markdown("#### 🔔 تغييرات أسعار آخر 7 أيام")
        c_df = pd.DataFrame(changes)
        st.dataframe(c_df[["product_name","competitor","old_price","new_price",
                            "price_diff","new_date"]].rename(columns={
            "product_name": "المنتج", "competitor": "المنافس",
            "old_price": "السعر السابق", "new_price": "السعر الجديد",
            "price_diff": "التغيير", "new_date": "التاريخ"
        }).head(200), use_container_width=True, height=200)
        st.markdown("---")

    if st.session_state.results:
        r = st.session_state.results
        cols = st.columns(6)
        data = [
            ("🔴","سعر أعلى",  len(r.get("price_raise", pd.DataFrame())), COLORS["raise"]),
            ("🟢","سعر أقل",   len(r.get("price_lower", pd.DataFrame())), COLORS["lower"]),
            ("✅","موافق",     len(r.get("approved", pd.DataFrame())),     COLORS["approved"]),
            ("🔍","مفقود",     len(r.get("missing", pd.DataFrame())),      COLORS["missing"]),
            ("⚠️","مراجعة",   len(r.get("review", pd.DataFrame())),       COLORS["review"]),
            ("⚪","مستبعد",   len(r.get("excluded", pd.DataFrame())),     COLORS["excluded"]),
        ]
        for col, (icon, label, val, color) in zip(cols, data):
            col.markdown(stat_card(icon, label, val, color), unsafe_allow_html=True)

        # ملخص الثقة للمفقودات في لوحة التحكم
        _miss_dash = r.get("missing", pd.DataFrame())
        if not _miss_dash.empty and "مستوى_الثقة" in _miss_dash.columns:
            _g = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "green"])
            _y = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "yellow"])
            _rd = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "red"])
            st.markdown(
                f'<div style="display:flex;gap:12px;justify-content:center;padding:8px;'
                f'background:#1a1a2e;border-radius:8px;margin:8px 0">'
                f'<span style="color:#00C853">🟢 مؤكد: <b>{_g}</b></span>'
                f'<span style="color:#FFD600">🟡 محتمل: <b>{_y}</b></span>'
                f'<span style="color:#FF1744">🔴 مشكوك: <b>{_rd}</b></span>'
                f'</div>', unsafe_allow_html=True)

        st.markdown("---")
        cc1, cc2 = st.columns(2)
        with cc1:
            sheets = {}
            for key, name in [("price_raise","سعر_أعلى"),("price_lower","سعر_أقل"),
                               ("approved","موافق"),("missing","مفقود"),("review","مراجعة"),
                               ("excluded","مستبعد")]:
                if key in r and not r[key].empty:
                    df_ex = r[key].copy()
                    if "جميع المنافسين" in df_ex.columns:
                        df_ex = df_ex.drop(columns=["جميع المنافسين"])
                    sheets[name] = df_ex
            if sheets:
                excel_all = export_multiple_sheets(sheets)
                st.download_button("📥 تصدير كل الأقسام Excel",
                    data=excel_all, file_name="mahwous_all.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
        with cc2:
            if st.button("📤 إرسال كل شيء لـ Make (دفعات ذكية)"):
                _prog_all = st.progress(0, text="جاري الإرسال...")
                _status_all = st.empty()
                _sent_total = 0
                _fail_total = 0
                _sections = [
                    ("price_raise", "raise", "update", "🔴 سعر أعلى"),
                    ("price_lower", "lower", "update", "🟢 سعر أقل"),
                    ("approved",    "approved", "update", "✅ موافق"),
                    ("missing",     "missing", "new", "🔍 مفقودة"),
                ]
                for _si, (_key, _sec, _btype, _label) in enumerate(_sections):
                    if _key in r and not r[_key].empty:
                        _p = export_to_make_format(r[_key], _sec)
                        _res = send_batch_smart(_p, batch_type=_btype, batch_size=20, max_retries=3)
                        _sent_total += _res.get("sent", 0)
                        _fail_total += _res.get("failed", 0)
                        _status_all.caption(f"{_label}: ✅ {_res.get('sent',0)} | ❌ {_res.get('failed',0)}")
                    _prog_all.progress((_si + 1) / len(_sections), text=f"جاري: {_label}")
                _prog_all.progress(1.0, text="اكتمل")
                st.success(f"✅ تم إرسال {_sent_total} منتج لـ Make!" + (f" (فشل {_fail_total})" if _fail_total else ""))
    else:
        # استئناف آخر job؟
        last = get_last_job()
        if last and last["status"] == "done" and last.get("results"):
            st.info(f"💾 يوجد تحليل محفوظ من {last.get('updated_at','')}")
            if st.button("🔄 استعادة النتائج المحفوظة"):
                _restored_last = restore_results_from_json(last["results"])
                df_all = pd.DataFrame(_restored_last)
                if not df_all.empty:
                    missing_df = pd.DataFrame(last.get("missing", [])) if last.get("missing") else pd.DataFrame()
                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results     = _r
                    st.session_state.analysis_df = df_all
                    st.rerun()
        else:
            st.info("👈 ارفع الملفات في القسم أدناه ثم اضغط «بدء التحليل»")

    st.markdown("---")
    st.subheader("📂 رفع الملفات وبدء التحليل")

    our_file = st.file_uploader(
        "📦 ملف منتجاتنا (CSV/Excel)",
        type=["csv", "xlsx", "xls"],
        key="dash_our_file",
    )
    comp_files = st.file_uploader(
        "🏪 ملفات المنافسين (متعدد)",
        type=["csv", "xlsx", "xls"],
        accept_multiple_files=True,
        key="dash_comp_files",
    )

    if our_file is not None:
        try:
            our_file.seek(0)
        except Exception:
            pass
        _odf, _oe = read_file(our_file)
        try:
            our_file.seek(0)
        except Exception:
            pass
        if not _oe and _odf is not None:
            with st.expander("📋 تعرف تلقائي على أعمدة ملف المتجر", expanded=False):
                _render_column_mapping_expander(_odf, "dash_map_our")
    if comp_files:
        for _ci, cf in enumerate(comp_files):
            try:
                cf.seek(0)
            except Exception:
                pass
            _cdf, _ce = read_file(cf)
            try:
                cf.seek(0)
            except Exception:
                pass
            if not _ce and _cdf is not None:
                with st.expander(f"📋 تعرف تلقائي — {cf.name}", expanded=False):
                    _render_column_mapping_expander(_cdf, f"dash_map_comp_{_ci}")

    col_opt1, col_opt2 = st.columns(2)
    with col_opt1:
        bg_mode = st.checkbox(
            "⚡ معالجة خلفية (يمكنك التنقل أثناء التحليل)",
            value=True,
            key="dash_bg_mode",
        )
    with col_opt2:
        max_rows = st.number_input(
            "حد الصفوف للمعالجة (0=كل)", 0, step=500, key="dash_max_rows"
        )

    if st.button("🚀 بدء التحليل", type="primary", key="dash_btn_start_analysis"):
        if not our_file or not comp_files:
            st.warning("⚠️ ارفع ملف منتجاتنا وملف منافس واحد على الأقل")
        else:
            _prep_ok = False
            our_df = None
            comp_dfs = {}
            job_id = None
            comp_names = ""
            with st.spinner("⏳ جاري قراءة الملفات وتحديث الكتالوج..."):
                our_df, err = read_file(our_file)
                if err:
                    st.error(f"❌ {err}")
                else:
                    our_df = apply_user_column_map(our_df, **_effective_column_map(our_df, "dash_map_our"))
                    if max_rows > 0:
                        our_df = our_df.head(int(max_rows))

                    comp_dfs = {}
                    for _ci, cf in enumerate(comp_files):
                        cdf, cerr = read_file(cf)
                        if cerr:
                            st.warning(f"⚠️ {cf.name}: {cerr}")
                        else:
                            cdf = apply_user_column_map(
                                cdf, **_effective_column_map(cdf, f"dash_map_comp_{_ci}")
                            )
                            comp_dfs[cf.name] = cdf

                    if not comp_dfs:
                        st.error("❌ لم يُحمّل أي ملف منافس صالح")
                    else:
                        _catc = resolve_catalog_columns(our_df)
                        r_our = upsert_our_catalog(
                            our_df,
                            name_col=_catc["name"] or "اسم المنتج",
                            id_col=_catc["id"] or "رقم المنتج",
                            price_col=_catc["price"] or "سعر المنتج",
                        )
                        r_comp = upsert_comp_catalog(comp_dfs)
                        st.caption(
                            f"✅ كتالوجنا: {r_our['inserted']} جديد / {r_our['updated']} تحديث | "
                            f"المنافسين: {r_comp['new_products']} جديد / {r_comp.get('updated', 0)} تحديث"
                        )
                        st.session_state.our_df = our_df
                        st.session_state.comp_dfs = comp_dfs
                        job_id = str(uuid.uuid4())[:8]
                        st.session_state.job_id = job_id
                        comp_names = ",".join(comp_dfs.keys())
                        _prep_ok = True

            if _prep_ok and our_df is not None and comp_dfs:
                if bg_mode:
                    t = threading.Thread(
                        target=_run_analysis_background,
                        args=(job_id, our_df, comp_dfs, our_file.name, comp_names),
                        daemon=True,
                    )
                    add_script_run_ctx(t)
                    t.start()
                    st.session_state.job_running = True
                    st.success(f"✅ بدأ التحليل في الخلفية (Job: {job_id})")
                    st.rerun()
                else:
                    prog = st.progress(0, "جاري التحليل...")

                    def upd(p, _r=None):
                        prog.progress(min(float(p), 0.99), f"{float(p)*100:.0f}%")

                    df_all = run_full_analysis(our_df, comp_dfs, progress_callback=upd)
                    raw_missing_df = find_missing_products(our_df, comp_dfs)
                    missing_df = smart_missing_barrier(raw_missing_df, our_df)

                    for _, row in df_all.iterrows():
                        if row.get("نسبة_التطابق", 0) > 0:
                            upsert_price_history(
                                str(row.get("المنتج", "")),
                                str(row.get("المنافس", "")),
                                safe_float(row.get("سعر_المنافس", 0)),
                                safe_float(row.get("السعر", 0)),
                                safe_float(row.get("الفرق", 0)),
                                safe_float(row.get("نسبة_التطابق", 0)),
                                str(row.get("القرار", "")),
                            )

                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results = _r
                    st.session_state.analysis_df = df_all
                    log_analysis(
                        our_file.name,
                        comp_names,
                        len(our_df),
                        int((df_all.get("نسبة_التطابق", pd.Series(dtype=float)) > 0).sum()),
                        len(missing_df),
                    )
                    prog.progress(1.0, "✅ اكتمل!")
                    st.balloons()
                    st.rerun()


# ════════════════════════════════════════════════
#  2. سعر أعلى
# ════════════════════════════════════════════════
elif page == "🔴 سعر أعلى":
    st.header("🔴 منتجات سعرنا أعلى — فرصة خفض")
    db_log("price_raise", "view")
    if st.session_state.results and "price_raise" in st.session_state.results:
        df = st.session_state.results["price_raise"]
        if not df.empty:
            st.error(f"⚠️ {len(df)} منتج سعرنا أعلى من المنافسين")
            # AI تدريب لهذا القسم
            with st.expander("🤖 نصيحة AI لهذا القسم", expanded=False):
                if st.button("📡 احصل على تحليل شامل للقسم", key="ai_section_raise"):
                    with st.spinner("🤖 AI يحلل البيانات الفعلية..."):
                        _top = df.nlargest(min(15, len(df)), "الفرق") if "الفرق" in df.columns else df.head(15)
                        _lines = "\n".join(
                            f"- {r.get('المنتج','')}: سعرنا {safe_float(r.get('السعر',0)):.0f} | المنافس ({r.get('المنافس','')}) {safe_float(r.get('سعر_المنافس',0)):.0f} | فرق +{safe_float(r.get('الفرق',0)):.0f}"
                            for _, r in _top.iterrows())
                        _avg_diff = safe_float(df["الفرق"].mean()) if "الفرق" in df.columns else 0
                        _prompt = (f"عندي {len(df)} منتج سعرنا أعلى من المنافسين.\n"
                                   f"متوسط الفرق: {_avg_diff:.0f} ر.س\n"
                                   f"أعلى 15 فرق:\n{_lines}\n\n"
                                   f"أعطني:\n1. أي المنتجات يجب خفض سعرها فوراً (فرق>30)؟\n"
                                   f"2. أي المنتجات يمكن إبقاؤها (فرق<10)؟\n"
                                   f"3. استراتيجية تسعير مخصصة لكل ماركة")
                        r = call_ai(_prompt, "price_raise")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            render_pro_table(df, "raise", "raise")
        else:
            st.success("✅ ممتاز! لا توجد منتجات بسعر أعلى")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  4. سعر أقل
# ════════════════════════════════════════════════
elif page == "🟢 سعر أقل":
    st.header("🟢 منتجات سعرنا أقل — فرصة رفع")
    db_log("price_lower", "view")
    if st.session_state.results and "price_lower" in st.session_state.results:
        df = st.session_state.results["price_lower"]
        if not df.empty:
            st.info(f"💰 {len(df)} منتج يمكن رفع سعره لزيادة الهامش")
            with st.expander("🤖 نصيحة AI لهذا القسم", expanded=False):
                if st.button("📡 استراتيجية رفع الأسعار", key="ai_section_lower"):
                    with st.spinner("🤖 AI يحلل فرص الربح..."):
                        _top = df.nsmallest(min(15, len(df)), "الفرق") if "الفرق" in df.columns else df.head(15)
                        _lines = "\n".join(
                            f"- {r.get('المنتج','')}: سعرنا {safe_float(r.get('السعر',0)):.0f} | المنافس ({r.get('المنافس','')}) {safe_float(r.get('سعر_المنافس',0)):.0f} | فرق {safe_float(r.get('الفرق',0)):.0f}"
                            for _, r in _top.iterrows())
                        _total_lost = safe_float(df["الفرق"].sum()) if "الفرق" in df.columns else 0
                        _prompt = (f"عندي {len(df)} منتج سعرنا أقل من المنافسين.\n"
                                   f"إجمالي الأرباح الضائعة: {abs(_total_lost):.0f} ر.س\n"
                                   f"أكبر 15 فرصة ربح:\n{_lines}\n\n"
                                   f"أعطني:\n1. أي المنتجات يمكن رفع سعرها فوراً (فرق>50)؟\n"
                                   f"2. أي المنتجات نرفعها تدريجياً (فرق 10-50)؟\n"
                                   f"3. كم الربح المتوقع إذا رفعنا الأسعار؟")
                        r = call_ai(_prompt, "price_lower")
                        st.markdown(f'<div class="ai-box">{r["response"]}</div>', unsafe_allow_html=True)
            render_pro_table(df, "lower", "lower")
        else:
            st.info("لا توجد منتجات")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  5. موافق عليها
# ════════════════════════════════════════════════
elif page == "✅ موافق عليها":
    st.header("✅ منتجات موافق عليها")
    db_log("approved", "view")
    if st.session_state.results and "approved" in st.session_state.results:
        df = st.session_state.results["approved"]
        if not df.empty:
            st.success(f"✅ {len(df)} منتج بأسعار تنافسية مناسبة")
            render_pro_table(df, "approved", "approved")
        else:
            st.info("لا توجد منتجات موافق عليها")
    else:
        st.info("ارفع الملفات أولاً")


# ════════════════════════════════════════════════
#  6. منتجات مفقودة — v26 مع كشف التستر/الأساسي
# ════════════════════════════════════════════════
elif page == "🔍 منتجات مفقودة":
    st.header("🔍 منتجات المنافسين غير الموجودة عندنا")
    st.caption(
        "العدد هنا = **عناوين فريدة** بعد إزالة التكرار والمطابقة مع كتالوجنا — وليس بالضرورة كل صفوف ملف المنافس."
    )
    db_log("missing", "view")

    if st.session_state.results and "missing" in st.session_state.results:
        df = st.session_state.results["missing"]
        if df is not None and not df.empty:
            # ── إحصاءات سريعة ──────────────────────────────────────────────
            total_miss   = len(df)
            has_tester   = df["نوع_متاح"].str.contains("تستر", na=False).sum()    if "نوع_متاح" in df.columns else 0
            has_base     = df["نوع_متاح"].str.contains("العطر الأساسي", na=False).sum() if "نوع_متاح" in df.columns else 0
            pure_missing = total_miss - has_tester - has_base

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("🔍 مفقود فعلاً",    pure_missing)
            c2.metric("🏷️ يوجد تستر",      has_tester)
            c3.metric("✅ يوجد الأساسي",   has_base)
            c4.metric("📦 إجمالي المنافسين", total_miss)

            # ── تحليل AI الأولويات ────────────────────────────────────────
            with st.expander("🤖 تحليل AI — أولويات الإضافة", expanded=False):
                if st.button("📡 تحليل الأولويات", key="ai_missing_section"):
                    with st.spinner("🤖 AI يحلل أولويات الإضافة..."):
                        _pure = df[df["نوع_متاح"].str.strip() == ""] if "نوع_متاح" in df.columns else df
                        _brands = _pure["الماركة"].value_counts().head(10).to_dict() if "الماركة" in _pure.columns else {}
                        _summary = " | ".join(f"{b}:{c}" for b,c in _brands.items()) if _brands else "غير محدد"
                        _lines   = "\n".join(
                            f"- {r.get('منتج_المنافس','')}: {safe_float(r.get('سعر_المنافس',0)):.0f}ر.س ({r.get('الماركة','')}) — {r.get('المنافس','')}"
                            for _, r in _pure.head(20).iterrows())
                        _prompt = (
                            f"لديّ {len(_pure)} منتج مفقود فعلاً (بدون التستر/الأساسي المتاح).\n"
                            f"توزيع الماركات: {_summary}\nعينة:\n{_lines}\n\n"
                            "أعطني:\n1. ترتيب أولويات الإضافة (عالية/متوسطة/منخفضة) مع السبب\n"
                            "2. أي الماركات الأكثر ربحية؟\n"
                            "3. سعر مقترح (أقل من المنافس بـ5-10 ر.س)\n"
                            "4. منتجات لا تستحق الإضافة — ولماذا؟"
                        )
                        r_ai = call_ai(_prompt, "missing")
                        resp = r_ai["response"] if r_ai["success"] else "❌ فشل AI"
                        # تنظيف JSON من المخرجات
                        import re as _re
                        resp = _re.sub(r'```json.*?```', '', resp, flags=_re.DOTALL)
                        resp = _re.sub(r'```.*?```', '', resp, flags=_re.DOTALL)
                        st.markdown(f'<div class="ai-box">{resp}</div>', unsafe_allow_html=True)

            # ── فلاتر ─────────────────────────────────────────────────────
            opts = get_filter_options(df)
            with st.expander("🔍 فلاتر", expanded=False):
                c1,c2,c3,c4,c5 = st.columns(5)
                search   = c1.text_input("🔎 بحث", key="miss_s")
                brand_f  = c2.selectbox("الماركة", opts["brands"], key="miss_b")
                comp_f   = c3.selectbox("المنافس", opts["competitors"], key="miss_c")
                variant_f= c4.selectbox("النوع",
                    ["الكل","مفقود فعلاً","يوجد تستر","يوجد الأساسي"], key="miss_v")
                conf_f   = c5.selectbox("الثقة",
                    ["الكل","🟢 مؤكد","🟡 محتمل","🔴 مشكوك"], key="miss_conf_f")

            filtered = df.copy()
            if search:
                filtered = filtered[filtered.apply(lambda r: search.lower() in str(r.values).lower(), axis=1)]
            if brand_f != "الكل" and "الماركة" in filtered.columns:
                filtered = filtered[filtered["الماركة"].str.contains(brand_f, case=False, na=False, regex=False)]
            if comp_f != "الكل" and "المنافس" in filtered.columns:
                filtered = filtered[filtered["المنافس"].str.contains(comp_f, case=False, na=False, regex=False)]
            if variant_f == "مفقود فعلاً" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.strip() == ""]
            elif variant_f == "يوجد تستر" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("تستر", na=False)]
            elif variant_f == "يوجد الأساسي" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("الأساسي", na=False)]
            # فلتر الثقة
            if conf_f != "الكل" and "مستوى_الثقة" in filtered.columns:
                _conf_map = {"🟢 مؤكد": "green", "🟡 محتمل": "yellow", "🔴 مشكوك": "red"}
                _cv = _conf_map.get(conf_f, "")
                if _cv:
                    filtered = filtered[filtered["مستوى_الثقة"] == _cv]

            # ── ترتيب حسب الثقة (الأكثر ثقة أولاً) ─────────────────────
            if "مستوى_الثقة" in filtered.columns:
                _conf_order = {"green": 0, "yellow": 1, "red": 2}
                filtered = filtered.assign(
                    _conf_sort=filtered["مستوى_الثقة"].map(_conf_order).fillna(3)
                ).sort_values("_conf_sort").drop(columns=["_conf_sort"])

            _export_ok, _export_issues = validate_export_product_dataframe(filtered)
            if not _export_ok:
                with st.expander(
                    "⚠️ تنبيه جودة التصدير: صفوف لا تطابق معايير سلة (اسم/سعر) — راجع قبل الاستيراد",
                    expanded=False,
                ):
                    for _ei in _export_issues[:40]:
                        st.caption(_ei)

            # ── تصدير ─────────────────────────────────────────────────────
            cc1,cc2,cc3 = st.columns(3)
            with cc1:
                excel_m = export_to_excel(filtered, "مفقودة")
                st.download_button("📥 Excel", data=excel_m, file_name="missing.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="miss_dl")
            with cc2:
                _csv_m = filtered.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                st.download_button("📄 CSV", data=_csv_m, file_name="missing.csv", mime="text/csv", key="miss_csv")
            with cc3:
                _salla_fast = export_to_salla_shamel(filtered, generate_descriptions=False)
                st.download_button(
                    "📥 سلة الشامل",
                    data=_salla_fast,
                    file_name="mahwous_salla_shamel.csv",
                    mime="text/csv",
                    key="miss_salla_fast",
                    help="قالب استيراد سلة الشامل — صف «بيانات المنتج» ثم رؤوس الأعمدة",
                )
            if st.button("🤖 توليد ملف سلة الشامل + وصف AI (بطيء)", key="miss_salla_ai_run"):
                with st.spinner("جاري استدعاء الذكاء الاصطناعي لكل صف — قد يستغرق وقتاً..."):
                    st.session_state["miss_salla_ai_bytes"] = export_to_salla_shamel(
                        filtered, generate_descriptions=True,
                    )
            if st.session_state.get("miss_salla_ai_bytes"):
                st.download_button(
                    "📥 تحميل سلة الشامل (وصف AI)",
                    data=st.session_state["miss_salla_ai_bytes"],
                    file_name="mahwous_salla_shamel_ai.csv",
                    mime="text/csv",
                    key="miss_salla_ai_dl",
                )

            # ── خيارات الإرسال الذكي ─────────────────────────────
            _conf_opts = {"🟢 مؤكدة فقط": "green", "🟡 محتملة": "yellow", "🔵 الكل": ""}
            _conf_sel = st.selectbox("مستوى الثقة", list(_conf_opts.keys()), key="miss_conf_sel")
            _conf_val = _conf_opts[_conf_sel]
            if st.button("📤 إرسال بدفعات ذكية لـ Make", key="miss_make_all"):
                # فلتر المفقودة الفعلية فقط (بدون التستر/الأساسي المتاح)
                _to_send = filtered[filtered["نوع_متاح"].str.strip() == ""] if "نوع_متاح" in filtered.columns else filtered

                is_valid, issues = validate_export_product_dataframe(_to_send)
                if not is_valid:
                    st.error("❌ تم إيقاف الإرسال! البيانات لا تطابق معايير سلة الصارمة:")
                    for issue in issues:
                        st.warning(issue)
                else:
                    products = export_to_make_format(_to_send, "missing")
                    # إضافة مستوى الثقة لكل منتج
                    for _ip, _pr_row in enumerate(products):
                        if _ip < len(_to_send):
                            _pr_row["مستوى_الثقة"] = str(_to_send.iloc[_ip].get("مستوى_الثقة", "green"))
                    _prog_bar = st.progress(0, text="جاري الإرسال...")
                    _status_txt = st.empty()

                    def _miss_progress(sent, failed, total, cur_name):
                        pct = (sent + failed) / max(total, 1)
                        _prog_bar.progress(min(pct, 1.0), text=f"إرسال: {sent}/{total} | {cur_name}")
                        _status_txt.caption(f"✅ {sent} | ❌ {failed} | الإجمالي {total}")

                    res = send_batch_smart(
                        products,
                        batch_type="new",
                        batch_size=20,
                        max_retries=3,
                        progress_cb=_miss_progress,
                        confidence_filter=_conf_val,
                    )
                    _prog_bar.progress(1.0, text="اكتمل")
                    if res["success"]:
                        st.success(res["message"])
                        # v26: احفظ في قائمة المعالجة
                        for _, _pr in _to_send.iterrows():
                            _pk = f"miss_{str(_pr.get('منتج_المنافس',''))[:30]}_{str(_pr.get('المنافس',''))}"
                            save_processed(
                                _pk,
                                str(_pr.get('منتج_المنافس','')),
                                str(_pr.get('المنافس','')),
                                "send_missing",
                                new_price=safe_float(_pr.get('سعر_المنافس',0)),
                            )
                    else:
                        st.error(res["message"])
                    if res.get("errors"):
                        with st.expander(f"❌ منتجات فشلت ({len(res['errors'])})"):
                            for _en in res["errors"]:
                                st.caption(f"• {_en}")

            st.caption(f"{len(filtered)} منتج — {datetime.now().strftime('%Y-%m-%d %H:%M')}")

            # ── عرض المنتجات ──────────────────────────────────────────────
            PAGE_SIZE = 20
            total_p = len(filtered)
            tp = max(1, (total_p + PAGE_SIZE - 1) // PAGE_SIZE)
            pn = st.number_input("الصفحة", 1, tp, 1, key="miss_pg") if tp > 1 else 1
            page_df = filtered.iloc[(pn-1)*PAGE_SIZE : pn*PAGE_SIZE]

            for idx, row in page_df.iterrows():
                name  = str(row.get("منتج_المنافس", ""))
                _miss_key = f"missing_{name}_{idx}"
                if _miss_key in st.session_state.hidden_products:
                    continue

                price           = safe_float(row.get("سعر_المنافس", 0))
                brand           = str(row.get("الماركة", ""))
                comp            = str(row.get("المنافس", ""))
                size            = str(row.get("الحجم", ""))
                ptype           = str(row.get("النوع", ""))
                _comp_show = _humanize_competitor_upload(comp)
                _title_display = _display_name_for_missing_row(row)
                if not _title_display:
                    _u_title = competitor_product_url_from_row(row)
                    if not str(_u_title or "").strip().lower().startswith("http") and _is_http_url_text(name):
                        _u_title = name.strip()
                    if str(_u_title or "").strip().lower().startswith("http"):
                        _ft = _cached_title_from_product_url(str(_u_title).strip())
                        if _ft:
                            _title_display = _ft
                if _title_display:
                    nm_ai = _title_display
                elif not _is_http_url_text(name):
                    nm_ai = name
                else:
                    _fb = f"{brand} {size} {ptype}".strip()
                    if not _fb:
                        _fb = _comp_show if _comp_show != "—" else "منتج"
                    nm_ai = _fb
                note            = str(row.get("ملاحظة", ""))
                # استخراج معرف المنتج (SKU/الكود)
                _miss_pid_raw = (
                    row.get("معرف_المنافس", "") or
                    row.get("product_id", "") or
                    row.get("رقم المنتج", "") or
                    row.get("رقم_المنتج", "") or
                    row.get("SKU", "") or
                    row.get("sku", "") or
                    row.get("الكود", "") or
                    row.get("كود", "") or
                    row.get("الباركود", "") or ""
                )
                _miss_pid = ""
                if _miss_pid_raw and str(_miss_pid_raw) not in ("", "nan", "None", "0", "NaN"):
                    try: _miss_pid = str(int(float(str(_miss_pid_raw))))
                    except: _miss_pid = str(_miss_pid_raw).strip()
                variant_label   = str(row.get("نوع_متاح", ""))
                variant_product = str(row.get("منتج_متاح", ""))
                variant_score   = safe_float(row.get("نسبة_التشابه", 0))
                is_tester_flag  = bool(row.get("هو_تستر", False))
                conf_level      = str(row.get("مستوى_الثقة", "green"))
                conf_score      = safe_float(row.get("درجة_التشابه", 0))
                suggested_price = round(price - 1, 2) if price > 0 else 0

                _is_similar = "⚠️" in note
                _has_variant= bool(variant_label and variant_label.strip())
                _is_tester_type = "تستر" in variant_label if _has_variant else False

                # ── لون البطاقة حسب الحالة ────────────────────────────
                if _has_variant and _is_tester_type:
                    _border = "#ff980055"; _badge_bg = "#ff9800"
                elif _has_variant:
                    _border = "#4caf5055"; _badge_bg = "#4caf50"
                elif _is_similar:
                    _border = "#ff572255"; _badge_bg = "#ff5722"
                else:
                    _border = "#007bff44"; _badge_bg = "#007bff"

                # ── بادج النوع المتاح ──────────────────────────────────
                _variant_html = ""
                if _has_variant:
                    _variant_html = f"""
                    <div style="margin-top:6px;padding:5px 10px;border-radius:6px;
                                background:{_badge_bg}22;border:1px solid {_badge_bg}88;
                                font-size:.78rem;color:{_badge_bg};font-weight:700">
                        {variant_label}
                        <span style="font-weight:400;color:#aaa;margin-right:6px">
                            ({variant_score:.0f}%) → {variant_product[:50]}
                        </span>
                    </div>"""

                # ── بادج تستر ─────────────────────────────────────────
                _tester_badge = ""
                if is_tester_flag:
                    _tester_badge = '<span style="font-size:.68rem;padding:2px 7px;border-radius:10px;background:#9c27b022;color:#ce93d8;margin-right:6px">🏷️ تستر</span>'

                _miss_img = str(row.get("صورة_المنافس", "") or "").strip()
                if not _miss_img:
                    _miss_img = _first_image_url_from_row(row) or ""
                _miss_comp_url = competitor_product_url_from_row(row)
                if not _miss_comp_url and _is_http_url_text(name):
                    _miss_comp_url = name.strip()
                if not _miss_img and _miss_comp_url.startswith("http"):
                    _miss_img = _cached_thumb_from_product_url(_miss_comp_url)
                st.markdown(miss_card(
                    name=name, price=price, brand=brand, size=size,
                    ptype=ptype, comp=_comp_show, suggested_price=suggested_price,
                    note=note if _is_similar else "",
                    variant_html=_variant_html, tester_badge=_tester_badge,
                    border_color=_border,
                    confidence_level=conf_level, confidence_score=conf_score,
                    product_id=_miss_pid,
                    image_url=_miss_img,
                    comp_url=_miss_comp_url,
                    title_override=_title_display,
                ), unsafe_allow_html=True)

                # ── أدوات جمع المعلومات ───────────────────────────────
                t1, t2, t3, t4 = st.columns(4)
                with t1:
                    if st.button("🌸 مكونات", key=f"notes_{idx}", use_container_width=True):
                        with st.spinner("يجلب من Fragrantica Arabia..."):
                            fi = fetch_fragrantica_info(nm_ai)
                            if fi.get("success"):
                                top  = ", ".join(fi.get("top_notes",[])[:5])
                                mid  = ", ".join(fi.get("middle_notes",[])[:5])
                                base = ", ".join(fi.get("base_notes",[])[:5])
                                st.markdown(f"""
**🌸 هرم العطر:**
- **القمة:** {top or "—"}
- **القلب:** {mid or "—"}
- **القاعدة:** {base or "—"}
- **الماركة:** {fi.get('brand','—')} | **السنة:** {fi.get('year','—')} | **العائلة:** {fi.get('fragrance_family','—')}""")
                                if fi.get("fragrantica_url"):
                                    st.markdown(f"[🔗 Fragrantica Arabia]({fi['fragrantica_url']})")
                                st.session_state[f"frag_info_{idx}"] = fi
                            else:
                                st.warning("لم يتم العثور على بيانات")
                with t2:
                    if st.button("🖼️ صور المنتج", key=f"imgs_{idx}", use_container_width=True):
                        with st.spinner("🔍 يبحث عن صور..."):
                            img_result = fetch_product_images(nm_ai, brand)
                            images = img_result.get("images", [])
                            frag_url = img_result.get("fragrantica_url","")
                            if images:
                                img_cols = st.columns(min(len(images),3))
                                for ci, img_data in enumerate(images[:3]):
                                    url = img_data.get("url",""); src = img_data.get("source","")
                                    is_search = img_data.get("is_search", False)
                                    with img_cols[ci]:
                                        if not is_search and url.startswith("http") and any(
                                            ext in url.lower() for ext in [".jpg",".png",".webp",".jpeg"]):
                                            try:    st.image(url, caption=f"📸 {src}", use_container_width=True)
                                            except: st.markdown(f"[🔗 {src}]({url})")
                                        else:
                                            st.markdown(f"[🔍 ابحث في {src}]({url})")
                                if frag_url:
                                    st.markdown(f"[🔗 Fragrantica Arabia]({frag_url})")
                            else:
                                st.warning("لم يتم العثور على صور")

                with t3:
                    if st.button("🔎 تحقق مهووس", key=f"mhw_{idx}", use_container_width=True):
                        with st.spinner("يبحث في mahwous.com..."):
                            r_m = search_mahwous(nm_ai)
                            if r_m.get("success"):
                                avail = "✅ متوفر" if r_m.get("likely_available") else "❌ غير متوفر"
                                resp_text = str(r_m.get("reason",""))[:200]
                                # تنظيف JSON
                                import re as _re
                                resp_text = _re.sub(r'\{.*?\}', '', resp_text, flags=_re.DOTALL)
                                st.info(f"{avail} | أولوية: **{r_m.get('add_recommendation','—')}**\n{resp_text}")
                            else:
                                st.warning("تعذر البحث")

                with t4:
                    if st.button("💹 سعر السوق", key=f"mkt_m_{idx}", use_container_width=True):
                        with st.spinner("🌐 يبحث في السوق..."):
                            r_s = search_market_price(nm_ai, price)
                            if r_s.get("success"):
                                mp  = r_s.get("market_price", 0)
                                rng = r_s.get("price_range", {})
                                rec = str(r_s.get("recommendation",""))[:200]
                                # تنظيف JSON من الرد
                                import re as _re
                                rec = _re.sub(r'```.*?```','', rec, flags=_re.DOTALL).strip()
                                mn  = rng.get("min",0); mx = rng.get("max",0)
                                _gap = mp - price if mp > price else 0
                                st.markdown(f"""
<div style="background:#0e1a2e;border:1px solid #4fc3f744;border-radius:8px;padding:10px;">
  <div style="font-weight:700;color:#4fc3f7">💹 سعر السوق: {mp:,.0f} ر.س</div>
  <div style="color:#888;font-size:.8rem">النطاق: {mn:,.0f} – {mx:,.0f} ر.س</div>
  {"<div style='color:#4caf50;font-size:.82rem'>💰 هامش: ~" + f"{_gap:,.0f} ر.س</div>" if _gap > 10 else ""}
  <div style="color:#aaa;font-size:.82rem;margin-top:6px">{rec}</div>
</div>""", unsafe_allow_html=True)

                # ── إجراءات ───────────────────────────────────────────
                a1, a2, a3, a4 = st.columns(4)
                with a1:
                    if st.button("✍️ خبير الوصف", key=f"expert_{idx}", type="primary", use_container_width=True):
                        with st.spinner("🤖 خبير مهووس يكتب الوصف الكامل..."):
                            fi_cached = st.session_state.get(f"frag_info_{idx}")
                            if not fi_cached:
                                fi_cached = fetch_fragrantica_info(nm_ai)
                                st.session_state[f"frag_info_{idx}"] = fi_cached
                            desc = generate_mahwous_description(nm_ai, suggested_price, fi_cached)
                            desc, _seo_meta = _parse_seo_json_block(desc)
                            st.session_state[f"desc_{idx}"] = desc
                            st.success("✅ الوصف جاهز — راجع المحرر أدناه")
                with a2:
                    _has_desc = f"desc_{idx}" in st.session_state
                    _make_lbl = "📤 إرسال Make + وصف" if _has_desc else "📤 إرسال Make"
                    if st.button(_make_lbl, key=f"mk_m_{idx}", type="primary" if _has_desc else "secondary", use_container_width=True):
                        _desc_send  = st.session_state.get(f"desc_{idx}","")
                        _fi_send    = st.session_state.get(f"frag_info_{idx}",{})
                        _img_url    = _fi_send.get("image_url","") if _fi_send else ""
                        _size_val   = extract_size(nm_ai)
                        _size_str   = f"{int(_size_val)}ml" if _size_val else size
                        # إرسال مباشر سواء كان هناك وصف أم لا
                        with st.spinner("📤 يُرسل لـ Make..."):
                            res = send_new_products([{
                                "أسم المنتج":  nm_ai,
                                "سعر المنتج":  suggested_price,
                                "brand":       brand,
                                "الوصف":       _desc_send,
                                "image_url":   _img_url,
                                "الحجم":       _size_str,
                                "النوع":       ptype,
                                "المنافس":     comp,
                                "سعر_المنافس": price,
                            }])
                        if res["success"]:
                            _wc = len(_desc_send.split()) if _desc_send else 0
                            _wc_msg = f" — وصف {_wc} كلمة" if _wc > 0 else ""
                            st.success(f"✅ {res['message']}{_wc_msg}")
                            _mk = f"missing_{name}_{idx}"
                            st.session_state.hidden_products.add(_mk)
                            save_hidden_product(_mk, nm_ai, "sent_to_make")
                            save_processed(_mk, nm_ai, comp, "send_missing",
                                           new_price=suggested_price,
                                           notes=f"إضافة جديدة" + (f" + وصف {_wc} كلمة" if _wc > 0 else ""))
                            for k in [f"desc_{idx}",f"frag_info_{idx}"]:
                                if k in st.session_state: del st.session_state[k]
                            st.rerun()
                        else:
                            st.error(res["message"])

                with a3:
                    if st.button("🤖 تكرار؟", key=f"dup_{idx}", use_container_width=True):
                        with st.spinner("..."):
                            our_prods = []
                            if st.session_state.analysis_df is not None:
                                our_prods = st.session_state.analysis_df.get("المنتج", pd.Series()).tolist()[:50]
                            r_dup = check_duplicate(nm_ai, our_prods)
                            _dup_resp = str(r_dup.get("response",""))[:250]
                            # تنظيف JSON
                            import re as _re
                            _dup_resp = _re.sub(r'```.*?```','', _dup_resp, flags=_re.DOTALL).strip()
                            _dup_resp = _re.sub(r'\{[^}]{0,200}\}','[بيانات]', _dup_resp)
                            st.info(_dup_resp if r_dup.get("success") else "فشل")

                with a4:
                    if st.button("🗑️ تجاهل", key=f"ign_{idx}", use_container_width=True):
                        log_decision(nm_ai,"missing","ignored","تجاهل",0,price,-price,comp)
                        _ign = f"missing_{name}_{idx}"
                        st.session_state.hidden_products.add(_ign)
                        save_hidden_product(_ign, nm_ai, "ignored")
                        save_processed(_ign, nm_ai, comp, "ignored",
                                       new_price=price,
                                       notes="تجاهل من قسم المفقودة")
                        st.rerun()

                if f"desc_{idx}" in st.session_state:
                    with st.expander("📄 الوصف الكامل — خبير مهووس", expanded=True):
                        edited_desc = st.text_area(
                            "راجع وعدّل الوصف قبل الإرسال:",
                            value=st.session_state[f"desc_{idx}"],
                            height=400,
                            key=f"desc_edit_{idx}",
                        )
                        st.session_state[f"desc_{idx}"] = edited_desc
                        _wc = len(edited_desc.split())
                        _col = "#4caf50" if _wc >= 1000 else "#ff9800"
                        st.markdown(
                            f'<span style="color:{_col};font-size:.8rem">📊 {_wc} كلمة</span>',
                            unsafe_allow_html=True,
                        )

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:8px 0">', unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات مفقودة!")
    else:
        st.info("ارفع الملفات أولاً")
# ════════════════════════════════════════════════
#  مستبعد — لا تطابق كافٍ في الفهارس (Zero Data Drop)
# ════════════════════════════════════════════════
elif page == "⚪ مستبعد (لا يوجد تطابق)":
    st.header("⚪ منتجات مستبعدة — لا يوجد تطابق مناسب مع منافس")
    st.caption(
        "منتجاتنا التي لم يُعثر لها على مرشح منافس بدرجة كافية، أو بلا أي مرشح في الفهارس. "
        "ليس نفس قسم «مفقود» (منتج عند المنافس ولا يوجد عندنا)."
    )
    db_log("excluded", "view")
    if st.session_state.results and "excluded" in st.session_state.results:
        df = st.session_state.results["excluded"]
        if df is not None and not df.empty:
            st.info(f"⚪ {len(df)} منتج مستبعد — يمكن مراجعة الأسباب في عمود القرار والمصدر")
            render_pro_table(df, "excluded", "excluded")
        else:
            st.success("✅ لا توجد منتجات مستبعدة — كل المنتجات لها مسار مطابقة أو مراجعة")
    else:
        st.info("ارفع الملفات وأجرِ التحليل أولاً")
# ════════════════════════════════════════════════
#  7. تحت المراجعة — v26 مقارنة جنباً إلى جنب
# ════════════════════════════════════════════════
elif page == "⚠️ تحت المراجعة":
    st.header("⚠️ منتجات تحت المراجعة — مطابقة غير مؤكدة")
    db_log("review", "view")

    if st.session_state.results and "review" in st.session_state.results:
        df = st.session_state.results["review"]
        if df is not None and not df.empty:
            st.warning(f"⚠️ {len(df)} منتج بمطابقة غير مؤكدة — يحتاج مراجعة بشرية أو AI")

            # ── تصنيف تلقائي بـ AI ────────────────────────────────────────
            col_r1, col_r2 = st.columns([2, 1])
            with col_r1:
                if st.button("🤖 إعادة تصنيف بالذكاء الاصطناعي", type="primary", key="reclassify_review"):
                    with st.spinner("🤖 AI يعيد تصنيف المنتجات..."):
                        _items_rc = []
                        for _, rr in df.head(30).iterrows():
                            _items_rc.append({
                                "our":       str(rr.get("المنتج","")),
                                "comp":      str(rr.get("منتج_المنافس","")),
                                "our_price": safe_float(rr.get("السعر",0)),
                                "comp_price":safe_float(rr.get("سعر_المنافس",0)),
                            })
                        _rc_results = reclassify_review_items(_items_rc)
                        _adf = st.session_state.get("analysis_df")
                        if _rc_results and _adf is not None and not _adf.empty:
                            _new_adf, _st = _apply_reclassify_to_analysis(_adf, df, _rc_results)
                            st.session_state.analysis_df = _new_adf
                            _r2 = _split_results(_new_adf)
                            _prev_miss = st.session_state.results.get("missing") if st.session_state.results else None
                            if _prev_miss is not None and not (isinstance(_prev_miss, pd.DataFrame) and _prev_miss.empty):
                                _r2["missing"] = _prev_miss
                            else:
                                _r2["missing"] = pd.DataFrame()
                            st.session_state.results = _r2
                            _persist_analysis_after_reclassify(_new_adf)
                            _moved = int(_st.get("applied", 0))
                            st.success(
                                f"✅ تم تطبيق {_moved} تعديلاً على جدول التحليل "
                                f"(تجاهل: ثقة {_st.get('skip_conf',0)}، مراجعة {_st.get('skip_review',0)}، "
                                f"idx {_st.get('skip_idx',0)}، بدون صف {_st.get('skip_no_row',0)})"
                            )
                            st.rerun()
                        elif _rc_results:
                            st.warning("لا يوجد جدول تحليل (analysis_df) — لم يُحفظ التصنيف")
                        else:
                            st.warning("لم يتمكن AI من إعادة التصنيف")
            with col_r2:
                excel_rv = export_to_excel(df, "مراجعة")
                st.download_button("📥 Excel", data=excel_rv, file_name="review.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="rv_dl")

            # ── فلتر بحث ──────────────────────────────────────────────────
            search_rv = st.text_input("🔎 بحث في المنتجات", key="rv_search")
            df_rv = df.copy()
            if search_rv:
                df_rv = df_rv[df_rv.apply(lambda r: search_rv.lower() in str(r.values).lower(), axis=1)]

            st.caption(f"{len(df_rv)} منتج للمراجعة")

            # ── عرض المقارنة جنباً إلى جنب ────────────────────────────────
            PAGE_SIZE = 15
            tp = max(1, (len(df_rv) + PAGE_SIZE - 1) // PAGE_SIZE)
            pn = st.number_input("الصفحة", 1, tp, 1, key="rv_pg") if tp > 1 else 1
            page_rv = df_rv.iloc[(pn-1)*PAGE_SIZE : pn*PAGE_SIZE]

            for idx, row in page_rv.iterrows():
                our_name   = str(row.get("المنتج",""))
                comp_name  = str(row.get("منتج_المنافس","—"))
                our_price  = safe_float(row.get("السعر",0))
                comp_price = safe_float(row.get("سعر_المنافس",0))
                score      = safe_float(row.get("نسبة_التطابق",0))
                brand      = str(row.get("الماركة",""))
                size       = str(row.get("الحجم",""))
                comp_name_s= str(row.get("المنافس",""))
                diff       = our_price - comp_price

                _rv_key = f"review_{our_name}_{idx}"
                if _rv_key in st.session_state.hidden_products:
                    continue

                # لون الثقة
                _score_color = "#4caf50" if score >= 85 else "#ff9800" if score >= 70 else "#f44336"
                _diff_color  = "#f44336" if diff > 10 else "#4caf50" if diff < -10 else "#888"
                _diff_label  = f"+{diff:.0f}" if diff > 0 else f"{diff:.0f}"

                _rv_our_img, _rv_comp_img = row_media_urls_from_analysis(row)
                _rv_our_thumb = lazy_img_tag(_rv_our_img, 56, 56, our_name[:40]) if _rv_our_img else ""
                _rv_comp_thumb = lazy_img_tag(_rv_comp_img, 56, 56, comp_name[:40]) if _rv_comp_img else ""
                _rv_our_url = our_product_url_from_row(row)
                _rv_comp_url = competitor_product_url_from_row(row)
                _rv_our_title = linked_product_title(
                    our_name[:60], _rv_our_url, color="#fff", font_size=".88rem",
                )
                _rv_comp_title = linked_product_title(
                    comp_name[:60], _rv_comp_url, color="#fff", font_size=".88rem",
                )

                # ── بطاقة المقارنة (مع صور عند التوفر) ─────────────────
                st.markdown(f"""
                <div style="border:1px solid #ff980055;border-radius:10px;padding:12px;
                            margin:6px 0;background:linear-gradient(135deg,#0a1628,#0e1a30);">
                  <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                    <span style="font-size:.75rem;color:#888">🏷️ {brand} | 📏 {size}</span>
                    <span style="font-size:.75rem;padding:2px 8px;border-radius:10px;
                                 background:{_score_color}22;color:{_score_color};font-weight:700">
                      نسبة المطابقة: {score:.0f}%
                    </span>
                  </div>
                  <div style="display:grid;grid-template-columns:1fr 60px 1fr;gap:8px;align-items:stretch">
                    <div style="background:#0d2040;border-radius:8px;padding:10px;border:1px solid #4fc3f733;
                                display:flex;align-items:flex-start;gap:10px;flex-direction:row-reverse">
                      <div style="flex:1;min-width:0">
                        <div style="font-size:.65rem;color:#4fc3f7;margin-bottom:4px">📦 منتجنا</div>
                        <div style="line-height:1.35">{_rv_our_title}</div>
                        <div style="font-size:1.1rem;font-weight:900;color:#4caf50;margin-top:6px">{our_price:,.0f} ر.س</div>
                      </div>
                      <div style="flex-shrink:0">{_rv_our_thumb}</div>
                    </div>
                    <div style="text-align:center;display:flex;flex-direction:column;justify-content:center">
                      <div style="font-size:1.2rem;color:{_diff_color};font-weight:900">{_diff_label}</div>
                      <div style="font-size:.6rem;color:#555">ر.س</div>
                    </div>
                    <div style="background:#1a0d20;border-radius:8px;padding:10px;border:1px solid #ff572233;
                                display:flex;align-items:flex-start;gap:10px">
                      <div style="flex-shrink:0">{_rv_comp_thumb}</div>
                      <div style="flex:1;min-width:0">
                        <div style="font-size:.65rem;color:#ff5722;margin-bottom:4px">🏪 {comp_name_s}</div>
                        <div style="line-height:1.35">{_rv_comp_title}</div>
                        <div style="font-size:1.1rem;font-weight:900;color:#ff9800;margin-top:6px">{comp_price:,.0f} ر.س</div>
                      </div>
                    </div>
                  </div>
                </div>""", unsafe_allow_html=True)

                # ── أزرار المراجعة ─────────────────────────────────────
                ba,bb,bc,bd,be = st.columns(5)

                with ba:
                    if st.button("🤖 تحقق AI", key=f"rv_verify_{idx}"):
                        with st.spinner("..."):
                            r_v = verify_match(our_name, comp_name, our_price, comp_price)
                            if r_v.get("success"):
                                conf = r_v.get("confidence",0)
                                match = r_v.get("match", False)
                                reason = str(r_v.get("reason",""))[:200]
                                # تنظيف JSON
                                import re as _re
                                reason = _re.sub(r'```.*?```','', reason, flags=_re.DOTALL)
                                reason = _re.sub(r'\{[^}]{0,200}\}','', reason).strip()
                                _lbl = "✅ نفس المنتج" if match else "❌ مختلف"
                                st.info(f"**{_lbl}** ({conf}%)\n{reason[:150]}")
                            else:
                                st.warning("فشل التحقق")

                with bb:
                    if st.button("✅ موافق", key=f"rv_approve_{idx}"):
                        log_decision(our_name,"review","approved","موافق",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "approved_from_review")
                        save_processed(_rv_key, our_name, comp_name_s, "approved",
                                       old_price=our_price, new_price=our_price,
                                       notes="موافق من تحت المراجعة")
                        st.rerun()

                with bc:
                    if st.button("🔴 سعر أعلى", key=f"rv_raise_{idx}"):
                        log_decision(our_name,"review","price_raise","سعر أعلى",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "moved_price_raise")
                        save_processed(_rv_key, our_name, comp_name_s, "send_price",
                                       old_price=our_price, new_price=comp_price - 1 if comp_price > 0 else our_price,
                                       notes="نُقل من المراجعة → سعر أعلى")
                        st.rerun()

                with bd:
                    if st.button("🔵 مفقود", key=f"rv_missing_{idx}"):
                        log_decision(our_name,"review","missing","مفقود",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "moved_missing")
                        save_processed(_rv_key, our_name, comp_name_s, "send_missing",
                                       new_price=comp_price,
                                       notes="نُقل من المراجعة → مفقود")
                        st.rerun()

                with be:
                    if st.button("🗑️ تجاهل", key=f"rv_ign_{idx}"):
                        log_decision(our_name,"review","ignored","تجاهل",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "ignored_review")
                        save_processed(_rv_key, our_name, comp_name_s, "ignored",
                                       old_price=our_price,
                                       notes="تجاهل من تحت المراجعة")
                        st.rerun()

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:6px 0">',
                            unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات تحت المراجعة!")
    else:
        st.info("ارفع الملفات أولاً")

# ════════════════════════════════════════════════
#  تمت المعالجة — v26
# ════════════════════════════════════════════════
elif page == "✔️ تمت المعالجة":
    st.header("✔️ المنتجات المعالجة")
    st.caption("جميع المنتجات التي تم ترحيلها أو تحديث سعرها أو إضافتها")
    db_log("processed", "view")

    processed = get_processed(limit=500)
    if not processed:
        st.info("📭 لا توجد منتجات معالجة بعد")
    else:
        df_proc = pd.DataFrame(processed)

        # إحصاء
        actions = df_proc["action"].value_counts()
        cols_p = st.columns(len(actions) + 1)
        for i, (act, cnt) in enumerate(actions.items()):
            icon = {"send_price":"💰","send_missing":"📦","approved":"✅","removed":"🗑️"}.get(act,"📌")
            cols_p[i].metric(f"{icon} {act}", cnt)
        cols_p[-1].metric("📦 الإجمالي", len(df_proc))

        # فلتر
        act_filter = st.selectbox("نوع الإجراء", ["الكل"] + list(actions.index))
        show_df = df_proc if act_filter == "الكل" else df_proc[df_proc["action"] == act_filter]

        st.markdown("---")

        for _, row in show_df.iterrows():
            p_key  = str(row.get("product_key",""))
            p_name = str(row.get("product_name",""))
            p_act  = str(row.get("action",""))
            p_ts   = str(row.get("timestamp",""))
            p_price_old = safe_float(row.get("old_price",0))
            p_price_new = safe_float(row.get("new_price",0))
            p_notes = str(row.get("notes",""))
            p_comp  = str(row.get("competitor",""))

            icon_map = {"send_price":"💰","send_missing":"📦","approved":"✅","removed":"🗑️"}
            icon = icon_map.get(p_act, "📌")

            col_a, col_b = st.columns([5, 1])
            with col_a:
                price_info = ""
                if p_price_old > 0 and p_price_new > 0:
                    price_info = f" | {p_price_old:.0f} → {p_price_new:.0f} ر.س"
                elif p_price_new > 0:
                    price_info = f" | {p_price_new:.0f} ر.س"
                _notes_html = ("<br><span style='color:#aaa;font-size:.73rem'>" + p_notes[:80] + "</span>") if p_notes else ""
                _arow = _find_analysis_row_for_processed(p_name)
                _p_our_u, _p_comp_u = _lookup_product_urls_from_analysis_session(p_name)
                _url_chips_html = _processed_row_url_chips_html(_p_our_u, _p_comp_u)
                _po, _pc = (
                    row_media_urls_from_analysis(_arow)
                    if _arow is not None
                    else ("", "")
                )
                # إن وُجد رابط صفحة بلا صورة في الجدول — جرّب og:image / أيقونة الموقع
                if (not _po) and (_p_our_u or "").strip().lower().startswith("http"):
                    _po = _cached_thumb_from_product_url(_p_our_u) or ""
                if (not _pc) and (_p_comp_u or "").strip().lower().startswith("http"):
                    _pc = _cached_thumb_from_product_url(_p_comp_u) or ""
                _comp_disp = (
                    str(_arow.get("منتج_المنافس", "") or "").strip()
                    if _arow is not None
                    else ""
                )
                if not _comp_disp:
                    _comp_disp = p_comp or "منافس"
                _thumb_cell = _processed_dual_image_html(_po, _pc, p_name[:100], _comp_disp[:100])
                st.markdown(
                    f'<div style="display:flex;align-items:center;gap:10px;padding:6px 10px;border-radius:6px;background:#0a1628;'
                    f'border:1px solid #1a2a44;font-size:.85rem">'
                    f'{_thumb_cell}'
                    f'<div style="flex:1;min-width:0">'
                    f'<span style="color:#888;font-size:.75rem">{p_ts[:16]}</span> &nbsp;'
                    f'{icon} <b style="color:#4fc3f7">{p_name[:60]}</b>'
                    f'<span style="color:#888"> — {p_act}{price_info}</span>'
                    f'{_notes_html}{_url_chips_html}</div></div>',
                    unsafe_allow_html=True
                )
            with col_b:
                if st.button("↩️ تراجع", key=f"undo_{p_key}"):
                    undo_processed(p_key)
                    # أعد للقائمة النشطة
                    if p_key in st.session_state.hidden_products:
                        st.session_state.hidden_products.discard(p_key)
                    st.success(f"✅ تم التراجع: {p_name[:40]}")
                    st.rerun()

        # تصدير
        st.markdown("---")
        csv_proc = df_proc.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📥 تصدير CSV", data=csv_proc,
                           file_name="processed_products.csv", mime="text/csv")



# ════════════════════════════════════════════════
#  9. أتمتة Make
# ════════════════════════════════════════════════
elif page == "⚡ أتمتة Make":
    st.header("⚡ أتمتة Make.com")
    db_log("make", "view")

    tab1, tab2, tab3 = st.tabs(["🔗 حالة الاتصال", "📤 إرسال", "📦 القرارات المعلقة"])

    with tab1:
        if st.button("🔍 فحص الاتصال"):
            with st.spinner("..."):
                results = verify_webhook_connection()
                for name, r in results.items():
                    if name != "all_connected":
                        color = "🟢" if r["success"] else "🔴"
                        st.markdown(f"{color} **{name}:** {r['message']}")
                if results.get("all_connected"):
                    st.success("✅ جميع الاتصالات تعمل")

    with tab2:
        if st.session_state.results:
            wh = st.selectbox("نوع الإرسال", ["سعر أعلى (تخفيض)","سعر أقل (رفع)","موافق عليها","مفقودة"])
            key_map = {
                "سعر أعلى (تخفيض)": "price_raise",
                "سعر أقل (رفع)":    "price_lower",
                "موافق عليها":      "approved",
                "مفقودة":           "missing",
            }
            section_type_map = {
                "price_raise": "raise",
                "price_lower": "lower",
                "approved":    "approved",
                "missing":     "missing",
            }
            sec_key  = key_map[wh]
            sec_type = section_type_map[sec_key]
            df_s     = st.session_state.results.get(sec_key, pd.DataFrame())

            if not df_s.empty:
                # معاينة ما سيُرسل
                _prev_cols = ["المنتج","السعر","سعر_المنافس","الماركة"]
                _prev_cols = [c for c in _prev_cols if c in df_s.columns]
                if _prev_cols:
                    st.dataframe(df_s[_prev_cols].head(10), use_container_width=True)

                products = export_to_make_format(df_s, sec_type)
                _sendable = [p for p in products if p.get("name") and p.get("price",0) > 0]
                st.info(f"سيتم إرسال {len(_sendable)} منتج → Make (Payload: product_id + name + price)")

                if st.button("📤 إرسال الآن", type="primary"):
                    if sec_type == "missing":
                        res = send_missing_products(_sendable)
                    else:
                        res = send_price_updates(_sendable)
                    st.success(res["message"]) if res["success"] else st.error(res["message"])
            else:
                st.info("لا توجد بيانات في هذا القسم")

    with tab3:
        pending = st.session_state.decisions_pending
        if pending:
            st.info(f"📦 {len(pending)} قرار معلق")
            df_p = pd.DataFrame([
                {"المنتج": k, "القرار": v["action"],
                 "وقت القرار": v.get("ts",""), "المنافس": v.get("competitor","")}
                for k, v in pending.items()
            ])
            st.dataframe(df_p.head(200), use_container_width=True)

            c1, c2 = st.columns(2)
            with c1:
                if st.button("📤 إرسال كل القرارات لـ Make"):
                    to_send = [{"name": k, **v} for k, v in pending.items()]
                    res = send_price_updates(to_send)
                    st.success(res["message"])
                    st.session_state.decisions_pending = {}
                    st.rerun()
            with c2:
                if st.button("🗑️ مسح القرارات"):
                    st.session_state.decisions_pending = {}
                    st.rerun()
        else:
            st.info("لا توجد قرارات معلقة")


# ════════════════════════════════════════════════
#  10. الإعدادات
# ════════════════════════════════════════════════
elif page == "⚙️ الإعدادات":
    st.header("⚙️ الإعدادات")
    db_log("settings", "view")

    tab1, tab2, tab3, tab4 = st.tabs(
        ["🔑 المفاتيح", "⚙️ المطابقة", "📋 قرارات المنتجات", "📜 السجل الكامل"]
    )

    with tab1:
        # ── الحالة الحالية ────────────────────────────────────────────────
        gemini_s = f"✅ {len(GEMINI_API_KEYS)} مفتاح" if GEMINI_API_KEYS else "❌ لا توجد مفاتيح"
        or_s     = "✅ مفعل" if OPENROUTER_API_KEY else "❌ غير موجود"
        co_s     = "✅ مفعل" if COHERE_API_KEY else "❌ غير موجود"
        st.info(f"Gemini API: {gemini_s}")
        st.info(f"OpenRouter: {or_s}")
        st.info(f"Cohere:     {co_s}")
        st.info(f"Webhook أسعار:   {'✅' if WEBHOOK_UPDATE_PRICES else '❌'}")
        st.info(f"Webhook منتجات:  {'✅' if WEBHOOK_NEW_PRODUCTS else '❌'}")

        st.markdown("---")

        # ── تشخيص شامل ───────────────────────────────────────────────────
        st.subheader("🔬 تشخيص AI")
        st.caption("يختبر الاتصال الفعلي بكل مزود ويُظهر الخطأ الحقيقي")

        if st.button("🔬 تشخيص شامل لجميع المزودين", type="primary"):
            with st.spinner("يختبر الاتصال بـ Gemini, OpenRouter, Cohere..."):
                from engines.ai_engine import diagnose_ai_providers
                diag = diagnose_ai_providers()

            # ── نتائج Gemini ──────────────────────────────────────────────
            st.markdown("**Gemini API:**")
            any_gemini_ok = False
            for g in diag.get("gemini", []):
                status = g["status"]
                if "✅" in status:
                    st.success(f"مفتاح {g['key']}: {status}")
                    any_gemini_ok = True
                elif "⚠️" in status:
                    st.warning(f"مفتاح {g['key']}: {status}")
                else:
                    st.error(f"مفتاح {g['key']}: {status}")
                _gd = (g.get("detail") or "").strip()
                if _gd and ("❌" in status or "⚠️" in status):
                    st.caption(f"تفاصيل API: {_gd[:500]}")

            # ── نتائج OpenRouter ──────────────────────────────────────────
            or_res = diag.get("openrouter","")
            st.markdown("**OpenRouter:**")
            if "✅" in or_res: st.success(or_res)
            elif "⚠️" in or_res: st.warning(or_res)
            else: st.error(or_res)

            # ── نتائج Cohere ──────────────────────────────────────────────
            co_res = diag.get("cohere","")
            st.markdown("**Cohere:**")
            if "✅" in co_res: st.success(co_res)
            elif "⚠️" in co_res: st.warning(co_res)
            else: st.error(co_res)

            # ── تحليل وتوصية ─────────────────────────────────────────────
            or_ok = "✅" in or_res
            co_ok = "✅" in co_res

            _recs = diag.get("recommendations") or []
            if _recs:
                st.markdown("**💡 توصيات تلقائية (حسب نتيجة التشخيص)**")
                for _r in _recs:
                    st.info(_r)

            st.markdown("---")
            if any_gemini_ok or or_ok or co_ok:
                working = []
                if any_gemini_ok: working.append("Gemini")
                if or_ok: working.append("OpenRouter")
                if co_ok: working.append("Cohere")
                st.success(f"✅ AI يعمل عبر: {' + '.join(working)}")
            else:
                st.error("❌ جميع المزودين فاشلون")
                # تحليل السبب
                _all_errs = [g["status"] for g in diag.get("gemini",[]) if "❌" in g.get("status","")]
                if any("اتصال" in e or "ConnectionError" in e or "Pool" in e for e in _all_errs + [or_res, co_res]):
                    st.warning("""
**🔴 السبب المحتمل: Streamlit Cloud يحجب الطلبات الخارجية**

الحل: في صفحة تطبيقك على Streamlit Cloud:
1. اذهب إلى ⚙️ Settings → General
2. ابحث عن **"Network"** أو **"Egress"**
3. تأكد أن Outbound connections مسموح بها

أو جرب نشر التطبيق على **Railway** بدلاً من Streamlit Cloud.
                    """)
                elif any("403" in e or "IP" in e for e in _all_errs):
                    st.warning("🔴 مفاتيح Gemini محظورة من IP هذا الخادم — جرب OpenRouter")
                elif any("401" in e for e in _all_errs + [or_res, co_res]):
                    st.warning("🔴 مفتاح غير صحيح — تحقق من المفاتيح في Secrets")

        st.markdown("---")

        # ── سجل الأخطاء الأخيرة ──────────────────────────────────────────
        st.subheader("📋 آخر أخطاء AI")
        from engines.ai_engine import get_last_errors
        errs = get_last_errors()
        if errs:
            for e in errs:
                st.code(e, language=None)
        else:
            st.caption("لا أخطاء مسجلة بعد — جرب أي زر AI ثم ارجع هنا")

        st.markdown("---")

        # ── اختبار سريع ──────────────────────────────────────────────────
        if st.button("🧪 اختبار سريع"):
            with st.spinner("يتصل بـ AI..."):
                r = call_ai("أجب بكلمة واحدة فقط: يعمل", "general")
            if r["success"]:
                st.success(f"✅ AI يعمل عبر {r['source']}: {r['response'][:80]}")
            else:
                st.error("❌ فشل — اضغط 'تشخيص شامل' لمعرفة السبب الدقيق")
                from engines.ai_engine import get_last_errors
                for e in get_last_errors()[:5]:
                    st.code(e, language=None)

    with tab2:
        st.info(f"حد التطابق الأدنى: {MIN_MATCH_SCORE}%")
        st.info(f"حد التطابق العالي: {HIGH_MATCH_SCORE}%")
        st.info(f"هامش فرق السعر: {PRICE_DIFF_THRESHOLD} ر.س")

    with tab3:
        decisions = get_decisions(limit=30)
        if decisions:
            df_dec = pd.DataFrame(decisions)
            st.dataframe(df_dec[["timestamp","product_name","old_status",
                                  "new_status","reason","competitor"]].rename(columns={
                "timestamp":"التاريخ","product_name":"المنتج",
                "old_status":"من","new_status":"إلى",
                "reason":"السبب","competitor":"المنافس"
            }).head(200), use_container_width=True)
        else:
            st.info("لا توجد قرارات مسجلة")

    with tab4:
        db_log("settings", "full_log")
        st.caption("سجل التحليلات، تتبع الأسعار، وأحداث التنقل — مدمج مع الإعدادات")
        log_t1, log_t2, log_t3 = st.tabs(["📊 التحليلات", "💰 تغييرات الأسعار", "📝 الأحداث"])

        with log_t1:
            history = get_analysis_history(20)
            if history:
                df_h = pd.DataFrame(history)
                st.dataframe(df_h[["timestamp","our_file","comp_file",
                                    "total_products","matched","missing"]].rename(columns={
                    "timestamp":"التاريخ","our_file":"ملف منتجاتنا",
                    "comp_file":"ملف المنافس","total_products":"الإجمالي",
                    "matched":"متطابق","missing":"مفقود"
                }).head(200), use_container_width=True)
            else:
                st.info("لا يوجد تاريخ")

        with log_t2:
            days = st.slider("آخر X يوم", 1, 30, 7, key="settings_price_changes_days")
            changes = get_price_changes(days)
            if changes:
                df_c = pd.DataFrame(changes)
                st.dataframe(df_c.rename(columns={
                    "product_name":"المنتج","competitor":"المنافس",
                    "old_price":"السعر السابق","new_price":"السعر الجديد",
                    "price_diff":"التغيير","new_date":"تاريخ التغيير"
                }).head(200), use_container_width=True)
            else:
                st.info(f"لا توجد تغييرات في آخر {days} يوم")

        with log_t3:
            events = get_events(limit=50)
            if events:
                df_e = pd.DataFrame(events)
                st.dataframe(df_e[["timestamp","page","event_type","details"]].rename(columns={
                    "timestamp":"التاريخ","page":"الصفحة",
                    "event_type":"الحدث","details":"التفاصيل"
                }).head(200), use_container_width=True)
            else:
                st.info("لا توجد أحداث")


# ════════════════════════════════════════════════
#  12. الأتمتة الذكية (v26.0 — متصل بالتنقل)
# ════════════════════════════════════════════════
elif page == "🔄 الأتمتة الذكية":
    st.header("🔄 الأتمتة الذكية — محرك القرارات التلقائية")
    db_log("automation", "view")

    # ── إنشاء محرك الأتمتة ──
    if "auto_engine" not in st.session_state:
        st.session_state.auto_engine = AutomationEngine()
    if "search_manager" not in st.session_state:
        st.session_state.search_manager = ScheduledSearchManager()

    engine = st.session_state.auto_engine
    search_mgr = st.session_state.search_manager

    tab_a1, tab_a2, tab_a3, tab_a4 = st.tabs([
        "🤖 تشغيل الأتمتة", "⚙️ قواعد التسعير", "🔍 البحث الدوري", "📊 سجل القرارات"
    ])

    # ── تاب 1: تشغيل الأتمتة ──
    with tab_a1:
        st.subheader("تطبيق القواعد التلقائية على نتائج التحليل")

        if st.session_state.results and st.session_state.analysis_df is not None:
            adf = st.session_state.analysis_df
            matched_df = adf[adf["نسبة_التطابق"].apply(lambda x: safe_float(x)) >= 85].copy()
            st.info(f"📦 {len(matched_df)} منتج مؤكد المطابقة جاهز للتقييم التلقائي")

            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("🚀 تشغيل الأتمتة الآن", type="primary", key="run_auto"):
                    with st.spinner("⚙️ محرك الأتمتة يقيّم المنتجات..."):
                        engine.clear_log()
                        decisions = engine.evaluate_batch(matched_df)
                        st.session_state._auto_decisions = decisions

                        # تسجيل كل قرار في قاعدة البيانات
                        for d in decisions:
                            log_automation_decision(d)

                    if decisions:
                        summary = engine.get_summary()
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("إجمالي القرارات", summary["total"])
                        c2.metric("⬇️ خفض سعر", summary["lower"])
                        c3.metric("⬆️ رفع سعر", summary["raise"])
                        c4.metric("✅ إبقاء", summary["keep"])

                        if summary["net_impact"] > 0:
                            st.success(f"💰 الأثر المالي المتوقع: +{summary['net_impact']:.0f} ر.س (صافي ربح إضافي)")
                        elif summary["net_impact"] < 0:
                            st.warning(f"📉 الأثر المالي: {summary['net_impact']:.0f} ر.س (خفض لتحقيق التنافسية)")

                        # عرض القرارات في جدول
                        dec_df = pd.DataFrame(decisions)
                        display_cols = ["product_name", "action", "old_price", "new_price",
                                        "comp_price", "competitor", "match_score", "reason"]
                        available = [c for c in display_cols if c in dec_df.columns]
                        st.dataframe(dec_df[available].rename(columns={
                            "product_name": "المنتج", "action": "الإجراء",
                            "old_price": "السعر الحالي", "new_price": "السعر الجديد",
                            "comp_price": "سعر المنافس", "competitor": "المنافس",
                            "match_score": "نسبة التطابق", "reason": "السبب"
                        }), use_container_width=True)
                    else:
                        st.info("لم يتم اتخاذ أي قرارات — جميع الأسعار ضمن الهامش المقبول")

            with col_b:
                auto_decisions = st.session_state.get("_auto_decisions", [])
                push_eligible = [d for d in auto_decisions
                                 if d.get("action") in ("lower_price", "raise_price")
                                 and d.get("product_id")]
                if push_eligible:
                    st.warning(f"📤 {len(push_eligible)} قرار جاهز للإرسال إلى Make.com/سلة")
                    if st.button("📤 إرسال القرارات إلى Make.com", key="push_auto"):
                        with st.spinner("يُرسل إلى Make.com..."):
                            result = auto_push_decisions(auto_decisions)
                        if result.get("success"):
                            st.success(result["message"])
                        else:
                            st.error(result["message"])
                else:
                    st.caption("لا توجد قرارات جاهزة للإرسال — شغّل الأتمتة أولاً")

        else:
            st.warning("⚠️ لا توجد نتائج تحليل — ارفع الملفات من أسفل «لوحة التحكم» ثم ابدأ التحليل")

        # ── معالجة قسم المراجعة تلقائياً ──
        st.divider()
        st.subheader("🔄 معالجة قسم المراجعة تلقائياً")
        st.caption("يستخدم AI للتحقق المزدوج من المطابقات غير المؤكدة")

        if st.session_state.results and "review" in st.session_state.results:
            rev_df = st.session_state.results.get("review", pd.DataFrame())
            if not rev_df.empty:
                st.info(f"📋 {len(rev_df)} منتج تحت المراجعة")
                if st.button("🤖 تحقق AI تلقائي لقسم المراجعة", key="auto_review"):
                    with st.spinner("🤖 AI يتحقق من المطابقات..."):
                        confirmed = auto_process_review_items(rev_df.head(15))
                    if not confirmed.empty:
                        st.success(f"✅ تم تأكيد {len(confirmed)} منتج من أصل {min(15, len(rev_df))}")
                        st.dataframe(confirmed[["المنتج", "منتج_المنافس", "القرار"]].head(20),
                                     use_container_width=True)
                    else:
                        st.info("لم يتم تأكيد أي مطابقة — المنتجات تحتاج مراجعة يدوية")
            else:
                st.success("لا توجد منتجات تحت المراجعة")

    # ── تاب 2: قواعد التسعير ──
    with tab_a2:
        st.subheader("⚙️ قواعد التسعير النشطة")
        st.caption("القواعد تُطبّق بالترتيب — أول قاعدة تنطبق تُنفَّذ")

        for i, rule in enumerate(engine.rules):
            with st.expander(f"{'✅' if rule.enabled else '⬜'} {rule.name}", expanded=False):
                st.write(f"**الإجراء:** {rule.action}")
                st.write(f"**حد التطابق الأدنى:** {rule.min_match_score}%")
                for k, v in rule.params.items():
                    if k not in ("name", "enabled", "action", "min_match_score", "condition"):
                        st.write(f"**{k}:** {v}")

        st.divider()
        st.subheader("📝 تخصيص القواعد")
        st.caption("يمكنك تعديل القواعد من ملف config.py → AUTOMATION_RULES_DEFAULT")
        st.code("""
# مثال: إضافة قاعدة جديدة في config.py
AUTOMATION_RULES_DEFAULT.append({
    "name": "خفض عدواني",
    "enabled": True,
    "action": "undercut",
    "min_diff": 5,
    "undercut_amount": 2,
    "min_match_score": 95,
    "max_loss_pct": 10,
})
        """, language="python")

    # ── تاب 3: البحث الدوري ──
    with tab_a3:
        st.subheader("🔍 البحث الدوري عن أسعار المنافسين")

        c1, c2 = st.columns(2)
        c1.metric("⏱️ البحث القادم", search_mgr.time_until_next())
        c2.metric("📊 آخر نتائج", f"{len(search_mgr.last_results)} منتج")

        if st.session_state.analysis_df is not None:
            scan_count = st.slider("عدد المنتجات للمسح", 5, 50, 15, key="scan_n")
            if st.button("🔍 مسح السوق الآن", type="primary", key="scan_now"):
                with st.spinner(f"يبحث عن أسعار {scan_count} منتج في السوق..."):
                    scan_results = search_mgr.run_scan(st.session_state.analysis_df, scan_count)
                if scan_results:
                    st.success(f"✅ تم مسح {len(scan_results)} منتج بنجاح")
                    for sr in scan_results[:10]:
                        md = sr.get("market_data", {})
                        rec = md.get("recommendation", md.get("market_price", "—"))
                        st.markdown(f"**{sr['product']}** — سعرنا: {sr['our_price']:.0f} | السوق: {rec}")
                else:
                    st.warning("لم يتم العثور على نتائج — تحقق من اتصال AI")
        else:
            st.warning("ارفع ملفات التحليل أولاً")

    # ── تاب 4: سجل القرارات ──
    with tab_a4:
        st.subheader("📊 سجل قرارات الأتمتة")
        days_filter = st.selectbox("الفترة", [7, 14, 30], index=0, key="auto_log_days")

        stats = get_automation_stats(days_filter)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("إجمالي", stats["total"])
        c2.metric("خفض", stats["lower"])
        c3.metric("رفع", stats["raise"])
        c4.metric("أُرسل لـ Make", stats["pushed"])

        log_data = get_automation_log(limit=100)
        if log_data:
            log_df = pd.DataFrame(log_data)
            display = ["timestamp", "product_name", "action", "old_price",
                        "new_price", "competitor", "match_score", "pushed_to_make"]
            available = [c for c in display if c in log_df.columns]
            st.dataframe(log_df[available].rename(columns={
                "timestamp": "التاريخ", "product_name": "المنتج",
                "action": "الإجراء", "old_price": "السعر القديم",
                "new_price": "السعر الجديد", "competitor": "المنافس",
                "match_score": "التطابق%", "pushed_to_make": "أُرسل؟"
            }), use_container_width=True)
        else:
            st.info("لا توجد قرارات مسجلة بعد — شغّل الأتمتة من التاب الأول")
