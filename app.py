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
import copy
import hashlib
from html import escape as _html_escape
from textwrap import dedent as _dedent
import json
import logging
import os
import pickle
import streamlit as st
import pandas as pd
import threading

_logger = logging.getLogger(__name__)
import time
import uuid
from datetime import datetime
from urllib.parse import urlparse

from async_scraper import (
    merge_scraper_bg_state,
    read_scraper_bg_state,
    run_scraper_sync,
    _load_sitemap_seeds,
)
from sitemap_resolve import resolve_store_to_sitemap_url

try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx
except ImportError:
    try:
        from streamlit.scriptrunner import add_script_run_ctx
    except ImportError:
        def add_script_run_ctx(t): return t

from config import *
from styles import get_styles, stat_card, vs_card, comp_strip, miss_card, get_sidebar_toggle_js
from engines.engine import (read_file, run_full_analysis, find_missing_products,
                             extract_brand, extract_size, extract_type, is_sample,
                             smart_missing_barrier)
from engines.mahwous_core import validate_export_product_dataframe
from engines.ai_engine import (call_ai, gemini_chat, chat_with_ai,
                                verify_match, analyze_product,
                                bulk_verify, suggest_price,
                                search_market_price, search_mahwous,
                                check_duplicate, process_paste,
                                fetch_fragrantica_info, fetch_product_images,
                                generate_mahwous_description,
                                analyze_paste, reclassify_review_items,
                                ai_deep_analysis,
                                apply_gemini_reclassify_to_analysis_df)
from engines.automation import (AutomationEngine, ScheduledSearchManager,
                                 auto_push_decisions, auto_process_review_items,
                                 log_automation_decision, get_automation_log,
                                 get_automation_stats)
from utils.helpers import (apply_filters, get_filter_options, export_to_excel,
                            export_multiple_sheets, parse_pasted_text,
                            safe_float, format_price, format_diff,
                            export_missing_products_to_salla_csv_bytes,
                            make_salla_desc_fn)
from utils.make_helper import (send_price_updates, send_new_products,
                                send_missing_products, send_single_product,
                                verify_webhook_connection, export_to_make_format,
                                send_batch_smart)
from utils.db_manager import (init_db, log_event, log_decision,
                               log_analysis, get_events, get_decisions,
                               get_analysis_history, upsert_price_history,
                               get_price_history, get_price_changes,
                               save_job_progress, get_job_progress, get_last_job,
                               save_hidden_product, get_hidden_product_keys,
                               init_db_v26, upsert_our_catalog, upsert_comp_catalog,
                               merged_comp_dfs_for_analysis, load_all_comp_catalog_as_comp_dfs,
                               save_processed, get_processed, undo_processed,
                               get_processed_keys, migrate_db_v26)


def _ui_autorefresh_interval(ms_default: int) -> int:
    """فاصل التحديث الحي (ملّي ث). يمكن رفعه عبر MAHWOUS_UI_LIVE_REFRESH_MS لتخفيف ثقل الواجهة أثناء الكشط."""
    v = os.environ.get("MAHWOUS_UI_LIVE_REFRESH_MS", "").strip()
    if v.isdigit():
        return max(2500, int(v))
    return ms_default


def _format_elapsed_compact(sec: float | int) -> str:
    """عرض مدة قصيرة للواجهة: ١٢٣ث أو ٤:٠٥."""
    try:
        s = int(float(sec))
    except (TypeError, ValueError):
        return "—"
    if s < 0:
        s = 0
    if s < 3600:
        m, r = divmod(s, 60)
        return f"{m}:{r:02d}" if m else f"{r}ث"
    h, r2 = divmod(s, 3600)
    m, r = divmod(r2, 60)
    return f"{h}:{m:02d}:{r:02d}"


def _missing_df_fingerprint(edf: pd.DataFrame) -> str:
    """بصمة جدول المفقودات لتتبّع ما إذا تغيّر العرض بعد التجهيز."""
    try:
        return hashlib.sha256(edf.to_csv(index=False).encode("utf-8", errors="replace")).hexdigest()
    except Exception:
        return str(int(edf.shape[0]))


@st.cache_data(ttl=300, show_spinner=False)
def _cached_filter_options(df: pd.DataFrame):
    """خيارات الفلاتر — تُخزَّن مؤقتاً لتخفيف إعادة الحساب عند كل تفاعل."""
    return get_filter_options(df)


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
    "chat_history": [], "job_id": None, "job_running": False,
    "decisions_pending": {},   # {product_name: action}
    "our_df": None, "comp_dfs": None,  # حفظ الملفات للمنتجات المفقودة
    "hidden_products": set(),  # منتجات أُرسلت لـ Make أو أُزيلت
    "scrape_preset_selection": [],  # أسماء منافسين من preset_competitors.json
    "brands_df": None,   # من data/brands.csv — إثراء المفقودات
    "categories_df": None,  # من data/categories.csv
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


def _enrich_missing_df(missing_df: pd.DataFrame) -> pd.DataFrame:
    """
    يطبّق إثراء الماركات (brand_page_url / brand_description) والتصنيف التلقائي
    بعد جدول المفقودات — دون تعديل mahwous_core أو المحرك.
    """
    if missing_df is None or missing_df.empty:
        return missing_df
    try:
        from engines.pipeline_enrichment import (
            apply_missing_pipeline_enrichment,
            load_brands_categories_from_disk,
        )

        bdf = st.session_state.get("brands_df")
        cdf = st.session_state.get("categories_df")
        if not isinstance(bdf, pd.DataFrame) or bdf.empty:
            bdf, cdf = load_brands_categories_from_disk()
            st.session_state["brands_df"] = bdf
            st.session_state["categories_df"] = cdf
        elif not isinstance(cdf, pd.DataFrame) or cdf.empty:
            _, cdf = load_brands_categories_from_disk()
            st.session_state["categories_df"] = cdf
        return apply_missing_pipeline_enrichment(missing_df, bdf, cdf)
    except Exception:
        return missing_df


# تحميل المنتجات المخفية من قاعدة البيانات عند كل تشغيل
_db_hidden = get_hidden_product_keys()
st.session_state.hidden_products = st.session_state.hidden_products | _db_hidden

# ── نتائج جزئية أثناء الكشط → بطاقات الأقسام (ملف pickle + لقطة JSON) ──
_LIVE_SNAP_PATH = os.path.join("data", "scrape_live_snapshot.json")
_LIVE_SESSION_PKL = os.path.join("data", "live_session_results.pkl")
# خيط التحليل يكتب اللقطة بشكل متكرر؛ القفل + كتابة ذرية تمنع تداخل الكتابات وملفات تالفة
_LIVE_SESSION_PKL_LOCK = threading.Lock()


def _atomic_write_live_session_pkl(payload: dict) -> None:
    """كتابة pickle ذرية: ملف مؤقت ثم os.replace حتى لا يقرأ القارئ نصف ملف."""
    os.makedirs("data", exist_ok=True)
    tmp = _LIVE_SESSION_PKL + ".tmp"
    with _LIVE_SESSION_PKL_LOCK:
        with open(tmp, "wb") as f:
            pickle.dump(payload, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, _LIVE_SESSION_PKL)


def _hydrate_live_session_results_early():
    """يحمّل نتائج التحليل المترافق إلى session أثناء الكشط حتى تُعرض البطاقات في الأقسام."""
    if not os.path.isfile(_LIVE_SESSION_PKL) or not os.path.isfile(_LIVE_SNAP_PATH):
        return
    try:
        with open(_LIVE_SNAP_PATH, encoding="utf-8") as f:
            snap = json.load(f)
    except Exception:
        return
    run_ok = snap.get("running") and not snap.get("done")
    done_gap = snap.get("done") and snap.get("success") and st.session_state.get("results") is None
    if not (run_ok or done_gap):
        return
    try:
        with _LIVE_SESSION_PKL_LOCK:
            with open(_LIVE_SESSION_PKL, "rb") as f:
                blob = pickle.load(f)
        st.session_state.results = blob["results"]
        st.session_state.analysis_df = blob.get("analysis_df")
        st.session_state.comp_dfs = blob.get("comp_dfs")
        if blob.get("our_df") is not None:
            st.session_state.our_df = blob["our_df"]
    except Exception:
        pass


_hydrate_live_session_results_early()


# ── مؤشرات API (ألوان + أيقونات) — يُحدَّث بالكامل بعد «تشخيص شامل» في الإعدادات ──
_API_STATUS_HINT = {
    "ok": ("#00C853", "يعمل"),
    "warn": ("#FF9800", "حد/بطء (429)"),
    "bill": ("#E65100", "رصيد/فاتورة منتهية (402)"),
    "bad": ("#FF1744", "رفض/خطأ"),
    "absent": ("#78909C", "غير مضاف"),
    "unknown": ("#5C6BC0", "لم يُختبر بعد"),
}


def _classify_provider_line(status: str) -> str:
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


def _infer_api_diag_summary(diag: dict) -> dict:
    """تلخيص نتيجة diagnose_ai_providers → حالة لكل مزود."""
    out: dict = {}
    if not get_gemini_api_keys():
        out["gemini"] = "absent"
    else:
        gs = diag.get("gemini") or []
        if not gs:
            out["gemini"] = "unknown"
        else:
            parts = [_classify_provider_line(g.get("status", "")) for g in gs]
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
        out["openrouter"] = _classify_provider_line(diag.get("openrouter", ""))
    if not get_cohere_api_key():
        out["cohere"] = "absent"
    else:
        out["cohere"] = _classify_provider_line(diag.get("cohere", ""))
    out["wh_price"] = "ok" if WEBHOOK_UPDATE_PRICES else "absent"
    out["wh_new"] = "ok" if WEBHOOK_NEW_PRODUCTS else "absent"
    return out


def _presence_api_summary() -> dict:
    """بدون تشخيص — الوجود فقط (مفتاح مضاف أم لا)."""
    return {
        "gemini": "ok" if get_gemini_api_keys() else "absent",
        "openrouter": "ok" if get_openrouter_api_key() else "absent",
        "cohere": "ok" if get_cohere_api_key() else "absent",
        "wh_price": "ok" if WEBHOOK_UPDATE_PRICES else "absent",
        "wh_new": "ok" if WEBHOOK_NEW_PRODUCTS else "absent",
    }


def _merged_api_summary() -> dict:
    d = st.session_state.get("api_diag_summary")
    if isinstance(d, dict) and d.get("_from_diag"):
        return {k: v for k, v in d.items() if not str(k).startswith("_")}
    return _presence_api_summary()


def _api_badges_html() -> str:
    m = _merged_api_summary()
    wh_has = bool(WEBHOOK_UPDATE_PRICES or WEBHOOK_NEW_PRODUCTS)
    wh_st = "ok" if wh_has else "absent"
    items = [
        ("✨", "Gemini", m.get("gemini", "unknown")),
        ("🔀", "OpenRouter", m.get("openrouter", "unknown")),
        ("◎", "Cohere", m.get("cohere", "unknown")),
        ("🔗", "Make", wh_st),
    ]
    chips = []
    for icon, label, stt in items:
        col, hint = _API_STATUS_HINT.get(stt, ("#9E9E9E", "?"))
        chips.append(
            f'<span title="{_html_escape(hint)}" style="display:inline-flex;align-items:center;gap:3px;'
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


def _settings_api_card_html(name: str, icon: str, stt: str) -> str:
    col, hint = _API_STATUS_HINT.get(stt, ("#9E9E9E", "?"))
    return (
        f'<div style="border-right:4px solid {col};background:{col}10;border-radius:8px;'
        f'padding:10px 12px;margin-bottom:8px">'
        f'<div style="font-weight:800;color:{col};font-size:1rem">{icon} {_html_escape(name)}</div>'
        f'<div style="color:#666;font-size:0.85rem">{_html_escape(hint)}</div></div>'
    )


def _clear_live_session_pkl():
    try:
        with _LIVE_SESSION_PKL_LOCK:
            if os.path.isfile(_LIVE_SESSION_PKL):
                os.remove(_LIVE_SESSION_PKL)
            _tmp = _LIVE_SESSION_PKL + ".tmp"
            if os.path.isfile(_tmp):
                os.remove(_tmp)
    except Exception:
        pass


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
        "all":         df,
    }


def _merge_verified_review_into_session(confirmed: pd.DataFrame) -> int:
    """يدمج صفوفاً مؤكدة من تحت المراجعة في analysis_df ويعيد تقسيم الأقسام."""
    if confirmed is None or confirmed.empty:
        return 0
    adf = st.session_state.get("analysis_df")
    if adf is None or getattr(adf, "empty", True):
        return 0
    adf = adf.copy()
    n = 0
    for _, crow in confirmed.iterrows():
        our_n = str(crow.get("المنتج", ""))
        comp_n = str(crow.get("منتج_المنافس", ""))
        new_dec = str(crow.get("القرار", "")).strip()
        if not our_n or not new_dec:
            continue
        try:
            mask = (adf["المنتج"].astype(str) == our_n) & (adf["منتج_المنافس"].astype(str) == comp_n)
        except Exception:
            continue
        for ri in adf.index[mask]:
            adf.at[ri, "القرار"] = new_dec
            n += 1
    st.session_state.analysis_df = adf
    r_new = _split_results(adf)
    prev = st.session_state.results or {}
    if prev.get("missing") is not None:
        r_new["missing"] = prev["missing"]
    st.session_state.results = r_new
    return n


def _first_section_with_results(r: dict) -> str | None:
    """أول قسم (تسمية الشريط الجانبي) يحتوي صفوفاً — لقفز المستخدم مباشرة لبطاقات المنتجات."""
    if not r:
        return None
    priority = [
        ("price_raise", "🔴 سعر أعلى"),
        ("price_lower", "🟢 سعر أقل"),
        ("review", "⚠️ تحت المراجعة"),
        ("approved", "✅ موافق عليها"),
        ("missing", "🔍 منتجات مفقودة"),
    ]
    for key, label in priority:
        df = r.get(key)
        if isinstance(df, pd.DataFrame) and not df.empty and label in SECTIONS:
            return label
    return "📊 لوحة التحكم" if "📊 لوحة التحكم" in SECTIONS else None


def _focus_sidebar_on_analysis_results(r: dict) -> None:
    target = _first_section_with_results(r)
    if target:
        st.session_state.sidebar_page_radio = target


def _safe_results_for_json(results_list):
    """تحويل النتائج لصيغة آمنة للحفظ في JSON/SQLite — يحول القوائم المتداخلة"""
    safe = []
    for r in results_list:
        row = {}
        for k, v in (r.items() if isinstance(r, dict) else {}):
            if isinstance(v, list):
                # تحويل قوائم المنافسين لنص JSON
                try:
                    import json as _j
                    row[k] = _j.dumps(v, ensure_ascii=False, default=str)
                except Exception:
                    row[k] = str(v)
            elif pd.isna(v) if isinstance(v, float) else False:
                row[k] = 0
            else:
                row[k] = v
        safe.append(row)
    return safe


def _restore_results_from_json(results_list):
    """استعادة النتائج من JSON — يحول نصوص القوائم لقوائم فعلية"""
    import json as _j
    restored = []
    for r in results_list:
        row = dict(r) if isinstance(r, dict) else {}
        for k in ["جميع_المنافسين", "جميع المنافسين"]:
            v = row.get(k)
            if isinstance(v, str):
                try:
                    row[k] = _j.loads(v)
                except Exception:
                    row[k] = []
            elif v is None:
                row[k] = []
        restored.append(row)
    return restored


# ── تحميل تلقائي للنتائج المحفوظة عند فتح التطبيق ──
_skip_last_job = False
if os.path.isfile(_LIVE_SNAP_PATH):
    try:
        with open(_LIVE_SNAP_PATH, encoding="utf-8") as f:
            _ls = json.load(f)
        if _ls.get("running") and not _ls.get("done"):
            _skip_last_job = True
    except Exception:
        pass

if st.session_state.results is None and not st.session_state.job_running and not _skip_last_job:
    _auto_job = get_last_job()
    if _auto_job and _auto_job["status"] == "done" and _auto_job.get("results"):
        _auto_records = _restore_results_from_json(_auto_job["results"])
        _auto_df = pd.DataFrame(_auto_records)
        if not _auto_df.empty:
            _auto_miss = pd.DataFrame(_auto_job.get("missing", [])) if _auto_job.get("missing") else pd.DataFrame()
            _auto_r = _split_results(_auto_df)
            _auto_r["missing"] = _auto_miss
            st.session_state.results     = _auto_r
            st.session_state.analysis_df = _auto_df
            st.session_state.job_id      = _auto_job.get("job_id")
            try:
                _cdf_all = load_all_comp_catalog_as_comp_dfs()
                if _cdf_all:
                    st.session_state.comp_dfs = _cdf_all
            except Exception:
                _logger.exception(
                    "restore comp_dfs from load_all_comp_catalog_as_comp_dfs failed"
                )


# ── دوال مساعدة ───────────────────────────
def db_log(page, action, details=""):
    try:
        log_event(page, action, details)
    except Exception:
        _logger.exception("db_log failed page=%s action=%s", page, action)

def ts_badge(ts_str=""):
    """شارة تاريخ مصغرة جميلة"""
    if not ts_str:
        ts_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    return f'<span style="font-size:.65rem;color:#555;background:#1a1a2e;padding:1px 6px;border-radius:8px;margin-right:4px">🕐 {ts_str}</span>'

def decision_badge(action):
    colors = {
        "approved": ("#00C853", "✅ موافق"),
        "deferred": ("#FFD600", "⏸️ مؤجل"),
        "removed":  ("#FF1744", "🗑️ محذوف"),
    }
    c, label = colors.get(action, ("#666", action))
    return f'<span style="font-size:.7rem;color:{c};font-weight:700">{label}</span>'


def _derive_competitor_display_name(user_label: str, store_urls: list[str]) -> str:
    """اسم يظهر في عمود «المنافس» والبطاقات: من إدخال المستخدم أو من نطاق الرابط."""
    t = (user_label or "").strip()
    if t:
        return t[:120]
    for raw in store_urls or []:
        u = (raw or "").strip()
        if not u:
            continue
        try:
            if not u.startswith(("http://", "https://")):
                u = "https://" + u
            p = urlparse(u)
            host = (p.netloc or "").strip().lower()
            if not host and p.path:
                host = p.path.split("/")[0].strip().lower()
            if host.startswith("www."):
                host = host[4:]
            if host:
                return host[:120]
        except Exception:
            continue
    return "Scraped_Competitor"


def _host_from_url(url: str) -> str:
    """نطاق (host) من رابط المتجر أو الخريطة."""
    try:
        u = (url or "").strip()
        if not u:
            return "competitor"
        if not u.startswith(("http://", "https://")):
            u = "https://" + u
        p = urlparse(u)
        h = (p.netloc or "").strip().lower()
        if h.startswith("www."):
            h = h[4:]
        return h[:120] if h else "competitor"
    except Exception:
        return "competitor"


def _comp_key_for_queue_entry(source_url: str, user_label: str, single_store: bool) -> str:
    """مفتاح منافس فريد لكل متجر في الطابور. متجر واحد: يحترم تسمية المستخدم؛ عدة متاجر: نطاق + تسمية اختيارية."""
    if single_store:
        return _derive_competitor_display_name(user_label, [source_url])
    host = _host_from_url(source_url)
    t = (user_label or "").strip()
    if t:
        return f"{t} | {host}"[:120]
    return host


def _parse_bulk_competitor_urls(text: str) -> list[str]:
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


def _parse_competitor_bulk_entries(text: str) -> list[dict]:
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
        for u in _parse_bulk_competitor_urls(line):
            out.append({"label": "", "store_url": u, "sitemap_url": None})
    return out


def _dedupe_competitor_entries(entries: list[dict]) -> list[dict]:
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


def load_preset_competitors() -> list[dict]:
    """قائمة المنافسين الثابتة من `data/preset_competitors.json` (اسم، متجر، sitemap)."""
    path = PRESET_COMPETITORS_PATH
    if not os.path.isfile(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
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


def _comp_key_for_scrape_entry(
    explicit_name: str,
    source_url: str,
    user_label: str,
    single_store: bool,
) -> str:
    """اسم المنافس من عمود الجدول يتقدّم على الاشتقاق من النطاق."""
    ex = (explicit_name or "").strip()
    if ex:
        return ex[:120]
    return _comp_key_for_queue_entry(source_url, user_label, single_store)


def _comp_incremental_catalog_flush(comp_key: str = "Scraped_Competitor"):
    """يُرجع دالة تُحدّث كتالوج المنافس على دفعات أثناء الكشط (مجموع الصفوف حتى الآن)."""

    def _flush(rows_snap: list) -> None:
        if not rows_snap:
            return
        cdf = pd.DataFrame(rows_snap)
        if cdf.empty:
            return
        try:
            upsert_comp_catalog({comp_key: cdf})
        except Exception:
            pass

    return _flush


def _persist_analysis_after_match(
    job_id, our_df, comp_dfs, analysis_df, our_file_name, comp_names
):
    """بعد توفر جدول المطابقة: تاريخ أسعار، مفقود، حفظ job_progress، سجل التحليل."""
    total = len(our_df)
    processed = total
    try:
        apply_gemini_reclassify_to_analysis_df(analysis_df)
    except Exception:
        pass
    try:
        for _, row in analysis_df.iterrows():
            if safe_float(row.get("نسبة_التطابق", 0)) > 0:
                upsert_price_history(
                    str(row.get("المنتج", "")),
                    str(row.get("المنافس", "")),
                    safe_float(row.get("سعر_المنافس", 0)),
                    safe_float(row.get("السعر", 0)),
                    safe_float(row.get("الفرق", 0)),
                    safe_float(row.get("نسبة_التطابق", 0)),
                    str(row.get("القرار", "")),
                )
    except Exception:
        pass
    try:
        raw_missing_df = find_missing_products(our_df, comp_dfs)
        missing_df = smart_missing_barrier(raw_missing_df, our_df)
        missing_df = _enrich_missing_df(missing_df)
    except Exception as e:
        import traceback

        traceback.print_exc()
        missing_df = pd.DataFrame()
    try:
        safe_records = _safe_results_for_json(analysis_df.to_dict("records"))
        safe_missing = missing_df.to_dict("records") if not missing_df.empty else []

        save_job_progress(
            job_id,
            total,
            total,
            safe_records,
            "done",
            our_file_name,
            comp_names,
            missing=safe_missing,
        )
        log_analysis(
            our_file_name,
            comp_names,
            total,
            int((analysis_df.get("نسبة_التطابق", pd.Series(dtype=float)) > 0).sum()),
            len(missing_df),
        )
    except Exception as e:
        import traceback

        traceback.print_exc()
        try:
            save_job_progress(
                job_id,
                total,
                total,
                _safe_results_for_json(analysis_df.to_dict("records")),
                "done",
                our_file_name,
                comp_names,
                missing=[],
            )
        except Exception:
            save_job_progress(
                job_id,
                total,
                processed,
                [],
                f"error: فشل الحفظ النهائي — {str(e)[:200]}",
                our_file_name,
                comp_names,
            )


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
                safe_res = _safe_results_for_json(current_results)
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
            our_df,
            comp_dfs,
            progress_callback=progress_cb,
            use_ai=True,
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

    _persist_analysis_after_match(
        job_id, our_df, comp_dfs, analysis_df, our_file_name, comp_names
    )


SCRAPE_BG_CONTEXT = os.path.join("data", "scrape_bg_context.pkl")
SCRAPE_LIVE_SNAPSHOT = os.path.join("data", "scrape_live_snapshot.json")
# تزامن بين خيط الكشط وخيط مسار التحليل عند كتابة اللقطة JSON
_LIVE_SNAPSHOT_LOCK = threading.Lock()


def _default_scrape_live_snapshot():
    return {
        "running": False,
        "done": False,
        "success": False,
        "error": None,
        "scrape": {
            "current": 0,
            "total": 1,
            "label": "",
            "elapsed_sec": 0,
            "urls_per_min": 0.0,
            "products_per_min": 0.0,
        },
        "analysis": {
            "phase": "idle",
            "progress_pct": 0.0,
            "ai_mode": "",
            "counts": {
                "price_raise": 0,
                "price_lower": 0,
                "approved": 0,
                "review": 0,
                "missing": 0,
            },
            "scraped_rows": 0,
        },
    }


def _read_scrape_live_snapshot_inner():
    d = _default_scrape_live_snapshot()
    if not os.path.isfile(SCRAPE_LIVE_SNAPSHOT):
        return d
    try:
        with open(SCRAPE_LIVE_SNAPSHOT, encoding="utf-8") as f:
            loaded = json.load(f)
        if not isinstance(loaded, dict):
            return d
        for k, v in d.items():
            if k not in loaded:
                loaded[k] = v
        if isinstance(loaded.get("scrape"), dict):
            loaded["scrape"] = {**d["scrape"], **loaded["scrape"]}
        if isinstance(loaded.get("analysis"), dict):
            ac = loaded["analysis"].get("counts") or {}
            merged_c = {**d["analysis"]["counts"], **ac} if isinstance(ac, dict) else d["analysis"]["counts"]
            loaded["analysis"] = {**d["analysis"], **loaded["analysis"], "counts": merged_c}
        return loaded
    except Exception:
        return d


def _read_scrape_live_snapshot():
    with _LIVE_SNAPSHOT_LOCK:
        return _read_scrape_live_snapshot_inner()


def _merge_scrape_live_snapshot(**kwargs):
    analysis_reset = kwargs.pop("analysis_reset", False)
    with _LIVE_SNAPSHOT_LOCK:
        cur = _read_scrape_live_snapshot_inner()
        if analysis_reset:
            cur["analysis"] = copy.deepcopy(_default_scrape_live_snapshot()["analysis"])
            cur["analysis"]["phase"] = "بدء"
            cur["analysis"]["ai_mode"] = "—"
            cur["analysis"]["progress_pct"] = 0.0
            cur["analysis"]["scraped_rows"] = 0
        for k, v in kwargs.items():
            if k == "scrape" and isinstance(v, dict) and isinstance(cur.get("scrape"), dict):
                cur["scrape"].update(v)
            elif k == "analysis" and isinstance(v, dict) and isinstance(cur.get("analysis"), dict):
                if "counts" in v and isinstance(v["counts"], dict):
                    cur["analysis"].setdefault("counts", {})
                    cur["analysis"]["counts"].update(v["counts"])
                for kk, vv in v.items():
                    if kk != "counts":
                        cur["analysis"][kk] = vv
            else:
                cur[k] = v
        cur["updated_at"] = datetime.now().isoformat()
        os.makedirs(os.path.dirname(SCRAPE_LIVE_SNAPSHOT), exist_ok=True)
        tmp = SCRAPE_LIVE_SNAPSHOT + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(cur, f, ensure_ascii=False, indent=2)
            os.replace(tmp, SCRAPE_LIVE_SNAPSHOT)
        except Exception:
            pass


def _clear_scrape_live_snapshot():
    try:
        if os.path.isfile(SCRAPE_LIVE_SNAPSHOT):
            os.remove(SCRAPE_LIVE_SNAPSHOT)
    except Exception:
        pass


def _live_scrape_thread_done(success: bool, error=None):
    _merge_scrape_live_snapshot(
        running=False,
        done=True,
        success=success,
        error=error,
    )
    if not success:
        _clear_live_session_pkl()


def _make_scrape_rows_tick_fn():
    """يحدّث عدد الصفوف المكسوبة أثناء الكشط دون انتظار انتهاء دورة المحرك."""

    def _tick(n: int):
        if n <= 0:
            return
        _merge_scrape_live_snapshot(
            analysis={
                "scraped_rows": n,
                "phase": f"🕸️ كشط: {n} صف — جاري الفرز عند كل دفعة",
            }
        )

    return _tick


def _make_on_pipeline_before_analysis():
    """يُعلّم قبل run_full_analysis حتى لا تبدو أشرطة الفرز ثابتة أثناء المطابقة."""

    def _before(rows_snap, is_final: bool):
        if not rows_snap:
            return
        snap = _read_scrape_live_snapshot()
        t = float((snap.get("scrape") or {}).get("total") or 1)
        n = len(rows_snap)
        prog_a = min(1.0, float(n) / max(t, 1.0))
        _merge_scrape_live_snapshot(
            analysis={
                "scraped_rows": n,
                "phase": (
                    "⚙️ جاري المطابقة والفرز (قد يستغرق وقتاً)…"
                    if not is_final
                    else "⚙️ جولة فرز نهائية…"
                ),
                "progress_pct": prog_a,
            }
        )

    return _before


def _make_on_analysis_snapshot(
    our_df,
    use_ai_partial: bool = False,
    comp_key: str = "Scraped_Competitor",
):
    ck = (comp_key or "Scraped_Competitor").strip() or "Scraped_Competitor"

    def _cb(rows_snap, analysis_df, is_final):
        try:
            apply_gemini_reclassify_to_analysis_df(analysis_df)
        except Exception:
            pass
        r = _split_results(analysis_df)
        missing_df = pd.DataFrame()
        try:
            cdf = pd.DataFrame(rows_snap)
            comp_dfs = merged_comp_dfs_for_analysis(ck, cdf)
            raw_m = find_missing_products(our_df, comp_dfs)
            missing_df = smart_missing_barrier(raw_m, our_df)
            missing_df = _enrich_missing_df(missing_df)
            missing_n = len(missing_df)
        except Exception:
            missing_df = pd.DataFrame()
            comp_dfs = merged_comp_dfs_for_analysis(ck, pd.DataFrame(rows_snap))
            missing_n = 0
        _r = dict(r)
        _r["missing"] = missing_df
        try:
            _atomic_write_live_session_pkl(
                {
                    "results": _r,
                    "analysis_df": analysis_df,
                    "comp_dfs": comp_dfs,
                    "our_df": our_df,
                    "is_partial": not is_final,
                    "comp_key": ck,
                    "updated_at": datetime.now().isoformat(),
                }
            )
        except Exception:
            pass
        snap = _read_scrape_live_snapshot()
        t = float((snap.get("scrape") or {}).get("total") or 1)
        prog_a = min(1.0, float(len(rows_snap)) / max(t, 1.0))
        if is_final:
            prog_a = 1.0
        if use_ai_partial:
            ai_hint = "محرك + Gemini (لقطات جزئية)"
        elif is_final:
            ai_hint = "محرك + Gemini (جولة نهائية دقيقة)"
        else:
            ai_hint = "محرك مطابقة سريع — AI في الجولة النهائية"
        _merge_scrape_live_snapshot(
            analysis={
                "phase": "نهائي" if is_final else "لقطة دورية",
                "progress_pct": prog_a,
                "ai_mode": ai_hint,
                "counts": {
                    "price_raise": len(r["price_raise"]),
                    "price_lower": len(r["price_lower"]),
                    "approved": len(r["approved"]),
                    "review": len(r["review"]),
                    "missing": missing_n,
                },
                "scraped_rows": len(rows_snap),
            },
        )

    return _cb


def _render_live_scrape_dashboard(snap: dict):
    sc = snap.get("scrape") or {}
    an = snap.get("analysis") or {}
    counts = an.get("counts") or {}
    pct = float(sc.get("current", 0)) / max(float(sc.get("total", 1)), 1.0)
    st.progress(min(pct, 1.0), sc.get("label") or "🕸️ جاري الكشط...")
    _es = int(sc.get("elapsed_sec") or 0)
    _upm = sc.get("urls_per_min")
    _ppm = sc.get("products_per_min")
    _rate = ""
    if _upm:
        _rate += f" · ~{float(_upm):.1f} صفحة/د"
    if _ppm:
        _rate += f" · ~{float(_ppm):.1f} منتج/د"
    st.caption(
        f"⏱️ **{_format_elapsed_compact(_es)}**{_rate} — "
        f"**التحليل:** {an.get('phase', '—')} — "
        f"صفوف مكسوبة: **{an.get('scraped_rows', 0)}**"
    )
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("🔴 سعر أعلى", int(counts.get("price_raise", 0)))
    c2.metric("🟢 سعر أقل", int(counts.get("price_lower", 0)))
    c3.metric("✅ موافق عليها", int(counts.get("approved", 0)))
    c4.metric("🔍 منتجات مفقودة", int(counts.get("missing", 0)))
    c5.metric("⚠️ تحت المراجعة", int(counts.get("review", 0)))
    st.caption(
        "**🔴🟢…** تُحدَّث بعد مرور الصفوف على **محرك الفرز** (لقطة دورية/نهائية)، وليس عند عدّ روابط الكشط فقط. "
        "طابور عدة متاجر: غالباً تبقى الأصفار حتى ينتهي كشط كل المتاجر ثم تبدأ المقارنة الشاملة. "
        "لقطة تحليل — الجولة النهائية والـ Job عبر الشريط الجانبي."
    )


def _run_scrape_chain_background():
    """كشط في الخيط: طابور متاجر بالتسلسل، ثم تحليل يشمل جميع المنافسين في الكتالوج."""
    try:
        with open(SCRAPE_BG_CONTEXT, "rb") as f:
            ctx = pickle.load(f)
    except Exception as e:
        merge_scraper_bg_state(
            active=False,
            phase="error",
            error=f"تعذر تحميل سياق الكشط: {e}",
            progress=0.0,
            message="",
        )
        _live_scrape_thread_done(False, f"سياق: {e}")
        return
    try:
        os.remove(SCRAPE_BG_CONTEXT)
    except Exception:
        pass

    scrape_bg = bool(ctx.get("scrape_bg", False))
    our_df = ctx["our_df"]
    user_label = str(ctx.get("user_comp_label") or "").strip()
    # اسمح بالتحديث اللحظي حتى عند تشغيل الكشط في الخلفية.
    pipeline_inline = bool(ctx.get("pipeline_inline", True))
    pl_every = int(ctx.get("pl_every") or 100)
    use_ai_partial = bool(ctx.get("use_ai_partial"))
    our_file_name = str(ctx.get("our_file_name") or "mahwous_catalog.csv")
    _raw_inc = os.environ.get("SCRAPER_INCREMENTAL_EVERY", "").strip()
    inc_every = int(_raw_inc) if _raw_inc.isdigit() else pl_every

    scrape_queue = ctx.get("scrape_queue")
    if not scrape_queue:
        seeds = _load_sitemap_seeds()
        n_seeds = len(seeds)
        scrape_queue = [
            {
                "sitemap": s,
                "comp_key": _comp_key_for_queue_entry(s, user_label, n_seeds <= 1),
                "source_url": s,
            }
            for s in seeds
            if isinstance(s, str) and s.startswith("http")
        ]
    if not scrape_queue:
        merge_scraper_bg_state(
            active=False,
            phase="error",
            error="لا توجد خرائط مواقع في الطابور.",
        )
        _live_scrape_thread_done(False, "طابور الكشط فارغ.")
        return

    total_stores = len(scrape_queue)
    pipeline_inline_effective = bool(pipeline_inline)

    _merge_scrape_live_snapshot(
        analysis_reset=True,
        running=True,
        done=False,
        success=False,
        scrape={"current": 0, "total": 1, "label": f"🕸️ طابور: 0/{total_stores} متجر..."},
    )
    if scrape_bg:
        merge_scraper_bg_state(
            active=True,
            phase="scrape",
            progress=0.0,
            message=f"🕸️ طابور {total_stores} متجر — يبدأ الأول...",
            error=None,
            job_id=None,
            rows=0,
        )

    _last_merge = [0.0]
    _last_live = [0.0]
    pl_dict_last: dict | None = None
    last_comp_df_ok: pd.DataFrame | None = None
    last_comp_key_ok: str | None = None
    stores_completed = 0
    total_rows_across = 0
    chain_t0 = time.time()

    for store_idx, job in enumerate(scrape_queue):
        comp_key = str(job.get("comp_key") or "Scraped_Competitor").strip() or "Scraped_Competitor"
        sm = str(job.get("sitemap") or "").strip()
        if not sm.startswith("http"):
            continue

        os.makedirs("data", exist_ok=True)
        with open("data/competitors_list.json", "w", encoding="utf-8") as f:
            json.dump([sm], f, ensure_ascii=False)

        flush_cb = _comp_incremental_catalog_flush(comp_key)
        if pipeline_inline_effective:
            pl_dict: dict | None = {
                "our_df": our_df,
                "comp_key": comp_key,
                "every": pl_every,
                "use_ai_partial": use_ai_partial,
                "incremental_every": max(1, inc_every),
                "on_incremental_flush": flush_cb,
                "on_analysis_snapshot": _make_on_analysis_snapshot(
                    our_df, use_ai_partial, comp_key
                ),
                "on_scrape_rows_tick": _make_scrape_rows_tick_fn(),
                "on_pipeline_before_analysis": _make_on_pipeline_before_analysis(),
            }
        else:
            pl_dict = {
                "incremental_every": max(1, inc_every),
                "on_incremental_flush": flush_cb,
                "on_scrape_rows_tick": _make_scrape_rows_tick_fn(),
            }

        def scrape_cb(current, total, last_name, _si=store_idx, _ts=total_stores, _t0=chain_t0):
            now = time.time()
            elapsed = max(0.001, now - _t0)
            elapsed_i = int(elapsed)
            urls_pm = 0.0
            ppm = 0.0
            if elapsed > 2.0 and current > 0:
                urls_pm = (float(current) / elapsed) * 60.0
            try:
                _snap_r = _read_scrape_live_snapshot()
                sr = int((_snap_r.get("analysis") or {}).get("scraped_rows") or 0)
                if elapsed > 2.0 and sr > 0:
                    ppm = (float(sr) / elapsed) * 60.0
            except Exception:
                pass
            span = 1.0 / max(_ts, 1)
            base = _si / max(_ts, 1)
            pct = base + span * (current / max(total, 1))
            nm = (last_name or "")[:80]
            lbl = f"🏪 متجر {_si + 1}/{_ts} | 🕸️ {current}/{total} | {nm}"
            if scrape_bg:
                if now - _last_merge[0] >= 1.2 or current >= total:
                    _last_merge[0] = now
                    merge_scraper_bg_state(
                        progress=min(pct, 0.998),
                        message=lbl[:220],
                        elapsed_sec=elapsed_i,
                        urls_per_min=round(urls_pm, 1),
                        products_per_min=round(ppm, 1),
                    )
            if now - _last_live[0] >= 0.35 or current >= total:
                _last_live[0] = now
                _merge_scrape_live_snapshot(
                    scrape={
                        "current": current,
                        "total": total,
                        "label": lbl[:240],
                        "elapsed_sec": elapsed_i,
                        "urls_per_min": round(urls_pm, 1),
                        "products_per_min": round(ppm, 1),
                    }
                )

        try:
            nrows = run_scraper_sync(progress_cb=scrape_cb, pipeline=pl_dict)
        except Exception as e:
            import traceback

            traceback.print_exc()
            if scrape_bg:
                merge_scraper_bg_state(
                    message=f"⚠️ متجر {store_idx + 1}/{total_stores}: {str(e)[:180]} — يُكمل للتالي",
                )
            continue

        pl_dict_last = pl_dict

        if not nrows or not os.path.isfile("data/competitors_latest.csv"):
            continue

        try:
            comp_df = pd.read_csv("data/competitors_latest.csv")
        except Exception:
            continue

        if comp_df.empty:
            continue

        try:
            upsert_comp_catalog({comp_key: comp_df})
        except Exception:
            continue

        stores_completed += 1
        total_rows_across += int(nrows)
        last_comp_df_ok = comp_df
        last_comp_key_ok = comp_key

    try:
        all_smaps = [j.get("sitemap") for j in scrape_queue if j.get("sitemap")]
        if all_smaps:
            with open("data/competitors_list.json", "w", encoding="utf-8") as f:
                json.dump(all_smaps, f, ensure_ascii=False)
    except Exception:
        pass

    if stores_completed == 0:
        merge_scraper_bg_state(
            active=False,
            phase="error",
            error="لم يُكمل أي متجر في الطابور (تحقق من الخرائط والكشط).",
        )
        _live_scrape_thread_done(False, "فشل كشط كل المتاجر في الطابور.")
        return

    try:
        upsert_our_catalog(
            our_df,
            name_col="اسم المنتج",
            id_col="رقم المنتج",
            price_col="السعر",
        )
        comp_dfs = load_all_comp_catalog_as_comp_dfs()
        if not comp_dfs and last_comp_df_ok is not None:
            _lck = str(last_comp_key_ok or "").strip() or "Scraped_Competitor"
            comp_dfs = merged_comp_dfs_for_analysis(_lck, last_comp_df_ok)
    except Exception as e:
        merge_scraper_bg_state(active=False, phase="error", error=f"الكتالوج: {e}")
        _live_scrape_thread_done(False, str(e))
        return

    pl_out = (pl_dict_last or {}).get("out") or {}
    comp_names = ",".join(sorted(comp_dfs.keys()))
    _scrape_elapsed_total = int(time.time() - chain_t0)
    # متجر واحد فقط: يمكن الاعتماد على لقطة الـ pipeline النهائية. عدة متاجر: دائماً تحليل شامل على كل الكتالوج.
    if (
        total_stores == 1
        and pipeline_inline_effective
        and pl_out.get("analysis_df") is not None
        and not pl_out.get("error")
        and pl_out.get("is_final")
    ):
        job_id = str(uuid.uuid4())[:8]
        merge_scraper_bg_state(
            progress=1.0,
            message=(
                f"✅ كشط {_scrape_elapsed_total}ث — حفظ مقارنة المتجر ({total_rows_across} صف)…"
            ),
            rows=total_rows_across,
            phase="analysis",
            job_id=job_id,
            active=True,
            elapsed_sec=_scrape_elapsed_total,
        )
        t_done = threading.Thread(
            target=_persist_analysis_after_match,
            args=(
                job_id,
                our_df,
                comp_dfs,
                pl_out["analysis_df"],
                our_file_name,
                comp_names,
            ),
            daemon=True,
        )
        add_script_run_ctx(t_done)
        t_done.start()
        _live_scrape_thread_done(True)
        return

    job_id = str(uuid.uuid4())[:8]
    merge_scraper_bg_state(
        progress=1.0,
        message=(
            f"✅ كشط {stores_completed}/{total_stores} متجراً في {_scrape_elapsed_total}ث "
            f"(~{total_rows_across} صف) — جاري المقارنة الشاملة على كل المنافسين…"
        ),
        rows=total_rows_across,
        phase="analysis",
        job_id=job_id,
        active=True,
        elapsed_sec=_scrape_elapsed_total,
    )

    t2 = threading.Thread(
        target=_run_analysis_background,
        args=(job_id, our_df, comp_dfs, our_file_name, comp_names),
        daemon=True,
    )
    add_script_run_ctx(t2)
    t2.start()
    _live_scrape_thread_done(True)


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
    opts = _cached_filter_options(df)
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
        if st.button("📤 إرسال كل لـ Make", key=f"{prefix}_make_all"):
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
    PAGE_SIZE = 50
    total_pages = max(1, (len(filtered) + PAGE_SIZE - 1) // PAGE_SIZE)
    _pg_key = f"{prefix}_pg"
    if _pg_key in st.session_state and int(st.session_state[_pg_key]) > total_pages:
        st.session_state[_pg_key] = total_pages
    if total_pages > 1:
        c_prev, c_num, c_next = st.columns([1, 3, 1])
        with c_prev:
            _cur = int(st.session_state.get(_pg_key, 1))
            if st.button("◀ السابق", key=f"{prefix}_pg_prev", disabled=_cur <= 1):
                st.session_state[_pg_key] = max(1, _cur - 1)
                st.rerun()
        with c_next:
            _cur = int(st.session_state.get(_pg_key, 1))
            if st.button("التالي ▶", key=f"{prefix}_pg_next", disabled=_cur >= total_pages):
                st.session_state[_pg_key] = min(total_pages, _cur + 1)
                st.rerun()
        with c_num:
            page_num = st.number_input("الصفحة", 1, total_pages, key=_pg_key)
    else:
        page_num = 1
    start = (page_num - 1) * PAGE_SIZE
    page_df = filtered.iloc[start:start + PAGE_SIZE]
    _deep_section_for_prefix = {
        "raise": "🔴 سعر أعلى",
        "lower": "🟢 سعر أقل",
        "review": "⚠️ تحت المراجعة",
        "approved": "✅ موافق",
    }.get(prefix, "⚠️ تحت المراجعة")

    # ── الجدول البصري ─────────────────────
    # row_i + page_num يضمنان مفاتيح session_state فريدة حتى مع تكرار index في DataFrame
    for row_i, (idx, row) in enumerate(page_df.iterrows()):
        price_input_key = f"input_price_{prefix}_p{page_num}_r{row_i}"
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

        # بطاقة VS مع رقم المنتج
        our_img = str(row.get("صورة_منتجنا", "") or "")
        comp_img = str(row.get("صورة_المنافس", "") or "")
        st.markdown(vs_card(our_name, our_price, comp_name,
                            comp_price, diff, comp_src, _pid_str,
                            our_img=our_img, comp_img=comp_img),
                    unsafe_allow_html=True)

        # شريط المعلومات
        match_color = ("#00C853" if match_pct >= 90
                       else "#FFD600" if match_pct >= 70 else "#FF1744")
        risk_html = ""
        if risk:
            rc = {"حرج": "#FF1744", "عالي": "#FF1744", "متوسط": "#FFD600", "منخفض": "#00C853", "عادي": "#00C853"}.get(risk.replace("🔴 ","").replace("🟡 ","").replace("🟢 ",""), "#888")
            risk_html = f'<span style="color:{rc};font-size:.75rem;font-weight:700">⚡{risk}</span>'

        # تتبع سعر المنافس: أخضر ↓ خفض المنافس | أحمر ↑ رفع المنافس | رمادي ثابت
        ph = get_price_history(our_name, comp_src, limit=2)
        price_change_html = ""
        if len(ph) >= 2:
            try:
                old_p = float(ph[1]["price"])
                new_p = float(ph[0]["price"])
            except Exception:
                old_p = new_p = 0.0
            chg = new_p - old_p
            if abs(chg) < 0.02:
                price_change_html = (
                    '<span style="color:#9E9E9E;font-size:.7rem">⚪ سعر المنافس ثابت</span>'
                )
            elif chg > 0:
                price_change_html = (
                    f'<span style="color:#FF1744;font-size:.7rem;font-weight:700" title="فرصة رفع سعرك">'
                    f"🔴 سعر المنافس ↑ +{chg:.0f} ر.س</span>"
                )
            else:
                price_change_html = (
                    f'<span style="color:#00C853;font-size:.7rem;font-weight:700" title="المنافس خفض سعره">'
                    f"🟢 سعر المنافس ↓ {abs(chg):.0f} ر.س</span>"
                )
        elif len(ph) == 1:
            price_change_html = '<span style="color:#888;font-size:.65rem">أول رصد لسعر المنافس</span>'

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
            st.markdown(
                comp_strip(all_comps, our_price=our_price, rank_by_threat=True),
                unsafe_allow_html=True,
            )

        # ── أزرار لكل منتج ─────────────────────
        b1, b2, b3, b4, b5, b6, b7, b8, b9, b10 = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1, 1])

        with b1:  # AI تحقق ذكي — يُصحح القسم
            _ai_label = {"raise": "🤖 هل نخفض؟", "lower": "🤖 هل نرفع؟",
                         "review": "🤖 هل يطابق؟", "approved": "🤖 تحقق"}.get(prefix, "🤖 تحقق")
            if st.button(_ai_label, key=f"v_{prefix}_p{page_num}_r{row_i}"):
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
                            "review": "⚠️ تحت المراجعة"
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
            if st.button(_mkt_label, key=f"mkt_{prefix}_p{page_num}_r{row_i}"):
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
            if st.button("✅ موافق", key=f"ok_{prefix}_p{page_num}_r{row_i}"):
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
            if st.button("⏸️ تأجيل", key=f"df_{prefix}_p{page_num}_r{row_i}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "deferred", "reason": "تأجيل",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "deferred",
                             "تأجيل", our_price, comp_price, diff, comp_src)
                st.warning("⏸️")

        with b5:  # إزالة
            if st.button("🗑️ إزالة", key=f"rm_{prefix}_p{page_num}_r{row_i}"):
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
            _auto_price = round(comp_price - 1, 2) if comp_price > 0 else our_price
            st.number_input(
                "سعر", value=_auto_price, min_value=0.0,
                step=1.0, key=price_input_key,
                label_visibility="collapsed"
            )

        with b7:  # تصدير Make
            if st.button("📤 Make", key=f"mk_{prefix}_p{page_num}_r{row_i}"):
                # سحب رقم المنتج من جميع الأعمدة المحتملة
                _pid_raw = (
                    row.get("معرف_المنتج", "") or
                    row.get("product_id", "") or
                    row.get("رقم المنتج", "") or
                    row.get("رقم_المنتج", "") or
                    row.get("معرف المنتج", "") or ""
                )
                # تحويل float إلى int (مثل 1081786650.0 → 1081786650)
                try:
                    _fv = float(_pid_raw)
                    _pid = str(int(_fv)) if _fv == int(_fv) else str(_pid_raw)
                except (ValueError, TypeError):
                    _pid = str(_pid_raw).strip()
                if _pid in ("nan", "None", "NaN", ""): _pid = ""
                try:
                    _raw_p = st.session_state.get(price_input_key)
                    _final_price = float(_raw_p) if _raw_p is not None else _auto_price
                except (TypeError, ValueError):
                    _final_price = _auto_price
                if _final_price <= 0:
                    _final_price = _auto_price
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

        with b8:  # تحقق AI — يُصحح القسم
            if st.button("🔍 تحقق", key=f"vrf_{prefix}_p{page_num}_r{row_i}"):
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
            if st.button("📈 تاريخ", key=f"ph_{prefix}_p{page_num}_r{row_i}"):
                history = get_price_history(our_name, comp_src)
                if history:
                    rows_h = [f"📅 {h['date']}: {h['price']:,.0f} ر.س" for h in history[:5]]
                    st.info("\n".join(rows_h))
                else:
                    st.info("لا يوجد تاريخ بعد")

        with b10:  # تحليل عميق (سوق + Gemini)
            if st.button("🔬 عميق", key=f"deep_{prefix}_p{page_num}_r{row_i}"):
                with st.spinner("🔬 تحليل عميق..."):
                    r_deep = ai_deep_analysis(
                        our_name, our_price, comp_name, comp_price,
                        section=_deep_section_for_prefix, brand=brand,
                    )
                    if r_deep.get("success"):
                        st.markdown(
                            f'<div class="ai-box">{r_deep.get("response", "")}</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.warning(str(r_deep.get("response", "فشل التحليل")))

        st.markdown('<hr style="border:none;border-top:1px solid #1a1a2e;margin:6px 0">', unsafe_allow_html=True)


# ════════════════════════════════════════════════
#  الشريط الجانبي
# ════════════════════════════════════════════════
with st.sidebar:
    st.markdown(f"## {APP_ICON} {APP_TITLE}")
    st.caption(f"الإصدار {APP_VERSION}")

    # حالة AI — إعادة قراءة من البيئة (Railway Variables وليس فقط st.secrets)
    _keys_live = get_gemini_api_keys()
    ai_ok = bool(_keys_live)
    if ai_ok:
        ai_color = "#00C853"
        ai_label = f"🤖 Gemini ✅ ({len(_keys_live)} مفتاح)"
    else:
        ai_color = "#FF1744"
        ai_label = "🔴 AI غير متصل — تحقق من Secrets"

    st.markdown(
        f'<div style="background:{ai_color}22;border:1px solid {ai_color};'
        f'border-radius:6px;padding:6px;text-align:center;color:{ai_color};'
        f'font-weight:700;font-size:.85rem">{ai_label}</div>',
        unsafe_allow_html=True
    )
    st.markdown(_api_badges_html(), unsafe_allow_html=True)

    # زر تشخيص سريع
    if not ai_ok:
        if st.button("🔍 تشخيص المشكلة", key="diag_btn"):
            st.write("**متغيرات البيئة (Railway / Docker):**")
            for key_name in [
                "GEMINI_API_KEY", "GEMINI_API_KEYS", "GEMINI_KEY_1",
                "GOOGLE_API_KEY", "GOOGLE_AI_API_KEY",
            ]:
                v = os.environ.get(key_name, "")
                if v:
                    masked = (v[:8] + "…" + v[-4:]) if len(v) > 12 else "***"
                    st.success(f"✅ `{key_name}` موجود (طول {len(v)}) — `{masked}`")
                else:
                    st.caption(f"— `{key_name}` غير معرّف")
            st.write("**Streamlit secrets (محلي / Cloud فقط):**")
            try:
                available = list(st.secrets.keys())
                for k in available:
                    val = str(st.secrets[k])
                    masked = val[:8] + "..." if len(val) > 8 else val
                    st.write(f"  `{k}` = `{masked}`")
            except Exception as e:
                st.caption(f"لا secrets.toml: {e}")
            st.info(
                "على Railway: أضف المتغير **لنفس الخدمة** (Variables → New Variable). "
                "إذا استخدمت Shared Variable اضغط **Add** حتى يصبح «in use». "
                "الاسم الموصى به: `GEMINI_API_KEY`."
            )

    # كشط خلفي — التنقل بين الأقسام أثناء الجلب
    _sbg = read_scraper_bg_state()
    if _sbg.get("phase") == "error" and _sbg.get("error"):
        st.error(f"❌ كشط خلفي: {str(_sbg['error'])[:220]}")
        if st.button("✓ تجاهل الرسالة", key="dismiss_scrape_bg_err"):
            merge_scraper_bg_state(
                phase="idle",
                error=None,
                active=False,
                progress=0.0,
                message="",
            )
            st.rerun()

    _live_sb = _read_scrape_live_snapshot()
    _live_run = _live_sb.get("running") and not _live_sb.get("done")

    if _live_run:
        st.markdown(
            '<div style="background:#1B5E2022;border:1px solid #4CAF50;'
            'border-radius:8px;padding:8px;margin-bottom:8px;font-size:.78rem">'
            "<b>⚡ كشط + تحليل متزامنان</b> — يعملان في الخلفية دون إيقاف الواجهة.</div>",
            unsafe_allow_html=True,
        )
        _sc = _live_sb.get("scrape") or {}
        _an = _live_sb.get("analysis") or {}
        _pct_s = float(_sc.get("current", 0)) / max(float(_sc.get("total", 1)), 1.0)
        st.caption("🕸️ **1 — جلب صفحات المنافس**")
        st.progress(
            min(_pct_s, 0.99),
            _sc.get("label") or f"🕸️ {_sc.get('current', 0)}/{_sc.get('total', 1)}",
        )
        _el = int(_sc.get("elapsed_sec") or 0)
        _um = _sc.get("urls_per_min") or 0
        _pm = _sc.get("products_per_min") or 0
        _line = f"⏱️ {_format_elapsed_compact(_el)}"
        if _um:
            _line += f" · ~{_um} صفحة/د"
        if _pm:
            _line += f" · ~{_pm} منتج/د"
        st.caption(_line)
        _pct_a = float(_an.get("progress_pct") or 0)
        if _pct_a <= 0 and _sc.get("total"):
            _pct_a = min(
                1.0,
                float(_an.get("scraped_rows", 0)) / max(float(_sc.get("total", 1)), 1.0),
            )
        _ai_cap = _an.get("ai_mode") or "محرك المطابقة + فرز الأقسام"
        st.caption(
            f"⚙️ **2 — تحليل وفرز المنتجات** — {_ai_cap}"
            + (
                f" | **{_an.get('phase', '—')}**"
                if _an.get("phase") and str(_an.get("phase")) != "idle"
                else ""
            )
        )
        st.progress(
            min(_pct_a, 0.99),
            f"فرز ← 🔴{int((_an.get('counts') or {}).get('price_raise', 0))} "
            f"🟢{int((_an.get('counts') or {}).get('price_lower', 0))} "
            f"✅{int((_an.get('counts') or {}).get('approved', 0))} "
            f"🔍{int((_an.get('counts') or {}).get('missing', 0))} "
            f"⚠️{int((_an.get('counts') or {}).get('review', 0))}",
        )
        if "جاري المطابقة" in str(_an.get("phase", "")):
            st.caption(
                "⏳ **الأرقام أعلاه** تتحدّث بعد انتهاء المحرك من الدفعة الحالية — "
                "شريط التقدم و«صفوف مكسوبة» يتحركان أثناء الكشط والمطابقة."
            )
        try:
            from streamlit_autorefresh import st_autorefresh

            st_autorefresh(interval=_ui_autorefresh_interval(2500), key="sidebar_live_dual_refresh")
        except ImportError:
            time.sleep(2.5)
            st.rerun()
    elif _sbg.get("active") and _sbg.get("phase") == "scrape":
        st.markdown(
            '<div style="background:#1565C022;border:1px solid #42A5F5;'
            'border-radius:6px;padding:8px;font-size:.78rem;margin-bottom:6px">'
            "🌐 <b>كشط في الخلفية</b> — يمكنك فتح أي قسم؛ يتم تحديث التقدم تلقائياً.</div>",
            unsafe_allow_html=True,
        )
        st.progress(
            min(float(_sbg.get("progress", 0)), 0.99),
            _sbg.get("message") or "🕸️ جاري الكشط...",
        )
        _sbg_es = int(_sbg.get("elapsed_sec") or 0)
        if _sbg_es or _sbg.get("urls_per_min") or _sbg.get("products_per_min"):
            _ln = f"⏱️ {_format_elapsed_compact(_sbg_es)}"
            if _sbg.get("urls_per_min"):
                _ln += f" · ~{_sbg.get('urls_per_min')} صفحة/د"
            if _sbg.get("products_per_min"):
                _ln += f" · ~{_sbg.get('products_per_min')} منتج/د"
            st.caption(_ln)
        try:
            from streamlit_autorefresh import st_autorefresh

            st_autorefresh(interval=_ui_autorefresh_interval(3000), key="scrape_bg_refresh")
        except ImportError:
            time.sleep(3)
            st.rerun()

    if _sbg.get("active") and _sbg.get("phase") == "analysis" and _sbg.get("job_id"):
        if st.session_state.get("job_id") != _sbg["job_id"]:
            st.session_state.job_id = _sbg["job_id"]
            st.session_state.job_running = True

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
                    st_autorefresh(interval=_ui_autorefresh_interval(4000), key="progress_refresh")
                except ImportError:
                    # fallback: rerun عادي إذا لم تكن المكتبة موجودة
                    time.sleep(4)
                    st.rerun()
            elif job["status"] == "done" and st.session_state.job_running:
                # اكتمل — حمّل النتائج تلقائياً مع استعادة القوائم
                if job.get("results"):
                    _restored = _restore_results_from_json(job["results"])
                    df_all = pd.DataFrame(_restored)
                    missing_df = pd.DataFrame(job.get("missing", [])) if job.get("missing") else pd.DataFrame()
                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results     = _r
                    st.session_state.analysis_df = df_all
                    try:
                        _cdf_done = load_all_comp_catalog_as_comp_dfs()
                        if _cdf_done:
                            st.session_state.comp_dfs = _cdf_done
                    except Exception:
                        pass
                    _focus_sidebar_on_analysis_results(_r)
                _sbg_done = read_scraper_bg_state()
                if _sbg_done.get("job_id") and _sbg_done.get("job_id") == st.session_state.job_id:
                    merge_scraper_bg_state(
                        active=False,
                        phase="idle",
                        job_id=None,
                        progress=0.0,
                        message="",
                        error=None,
                    )
                st.session_state.job_running = False
                _clear_live_session_pkl()
                st.balloons()
                st.rerun()
            elif job["status"].startswith("error"):
                st.error(f"❌ فشل: {job['status'][7:80]}")
                _sbg_e = read_scraper_bg_state()
                if _sbg_e.get("job_id") == st.session_state.job_id:
                    merge_scraper_bg_state(
                        active=False,
                        phase="idle",
                        job_id=None,
                    )
                st.session_state.job_running = False

    page = st.radio(
        "الأقسام",
        SECTIONS,
        label_visibility="collapsed",
        key="sidebar_page_radio",
    )

    st.markdown("---")
    if st.session_state.results:
        r = st.session_state.results
        st.markdown("**📊 ملخص:**")
        for key, icon, label in [
            ("price_raise","🔴","أعلى"), ("price_lower","🟢","أقل"),
            ("approved","✅","موافق"), ("missing","🔍","مفقود"),
            ("review","⚠️","مراجعة")
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
        cols = st.columns(5)
        data = [
            ("🔴","سعر أعلى",  len(r.get("price_raise", pd.DataFrame())), COLORS["raise"]),
            ("🟢","سعر أقل",   len(r.get("price_lower", pd.DataFrame())), COLORS["lower"]),
            ("✅","موافق",     len(r.get("approved", pd.DataFrame())),     COLORS["approved"]),
            ("🔍","مفقود",     len(r.get("missing", pd.DataFrame())),      COLORS["missing"]),
            ("⚠️","مراجعة",   len(r.get("review", pd.DataFrame())),       COLORS["review"]),
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
                               ("approved","موافق"),("missing","مفقود"),("review","مراجعة")]:
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
                _restored_last = _restore_results_from_json(last["results"])
                df_all = pd.DataFrame(_restored_last)
                if not df_all.empty:
                    missing_df = pd.DataFrame(last.get("missing", [])) if last.get("missing") else pd.DataFrame()
                    _r = _split_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results     = _r
                    st.session_state.analysis_df = df_all
                    try:
                        _cdf_last = load_all_comp_catalog_as_comp_dfs()
                        if _cdf_last:
                            st.session_state.comp_dfs = _cdf_last
                    except Exception:
                        pass
                    _focus_sidebar_on_analysis_results(_r)
                    st.rerun()
        else:
            st.info("👈 ارفع ملفاتك من قسم 'رفع الملفات'")


# ════════════════════════════════════════════════
#  2. رفع الملفات — كشط الويب + تحليل
# ════════════════════════════════════════════════
elif page == "📂 رفع الملفات":
    _snap_live = _read_scrape_live_snapshot()
    if _snap_live.get("running") and not _snap_live.get("done"):
        with st.container(border=True):
            st.markdown("### 📡 مباشر — الكشط والتحليل على الدفعات")
            _render_live_scrape_dashboard(_snap_live)
        try:
            from streamlit_autorefresh import st_autorefresh

            st_autorefresh(interval=_ui_autorefresh_interval(2000), key="scrape_live_dashboard_refresh")
        except ImportError:
            time.sleep(2)
            st.rerun()
        st.markdown("---")
    elif _snap_live.get("done"):
        # يجب تحميل pickle قبل حذف اللقطة/الملفات — وإلا تُفقد النتائج إذا اكتمل الكشط أثناء الجلسة
        # (كان التحميل يحدث مرة عند استيراد الوحدة فقط).
        _hydrate_live_session_results_early()
        if not _snap_live.get("success") and _snap_live.get("error"):
            st.error(f"❌ {_snap_live['error'][:400]}")
        else:
            st.success(
                "✅ انتهت مرحلة الكشط والتحضير — راقب **الشريط الجانبي** لاكتمال التحليل (Job) أو النتائج."
            )
        _clear_scrape_live_snapshot()
        _clear_live_session_pkl()
        st.rerun()

    st.header("🕸️ كشط الويب والتحليل")
    db_log("upload", "view")
    our_path = os.path.join("data", "mahwous_catalog.csv")

    # اظهر رافع الكتالوج دائماً حتى تتمكن الاختبارات/المستخدم من رفع الملف دون الدخول في مسار البدء.
    st.markdown("### 📦 كتالوج منتجاتنا")
    if os.path.isfile(our_path):
        st.caption("✅ الملف الحالي: `data/mahwous_catalog.csv` (يمكنك استبداله برفع جديد)")
    else:
        st.warning("⚠️ لم يُعثر على كتالوج المنتجات — ارفعه أولاً قبل بدء الكشط.")
    uploaded_catalog_main = st.file_uploader(
        "📂 ارفع ملف كتالوج منتجاتك (mahwous_catalog.csv)",
        type=["csv"],
        key="catalog_uploader_main",
    )
    if uploaded_catalog_main:
        try:
            os.makedirs("data", exist_ok=True)
            with open(our_path, "wb") as f:
                f.write(uploaded_catalog_main.read())
            _tmp_df = pd.read_csv(our_path)
            if "no" not in _tmp_df.columns:
                st.error("❌ الملف رُفع لكن عمود المعرف الإلزامي `no` غير موجود.")
            else:
                st.success(f"✅ تم حفظ الكتالوج بنجاح — عدد الصفوف: {len(_tmp_df):,}")
        except Exception as e:
            st.error(f"❌ تعذر حفظ/قراءة الكتالوج: {e}")

    if "scraper_urls" not in st.session_state:
        st.session_state.scraper_urls = ["https://worldgivenchy.com/ar/"]

    st.markdown("🔗 **روابط متاجر المنافسين (سلة أو زد)**")
    st.caption(
        "مضاد الحظر: يُفضَّل تثبيت `curl-cffi` و`playwright` ثم `playwright install chromium`. "
        "عند الحظر يُعاد البحث تلقائيًا عبر Chromium. يمكن أيضًا لصق رابط .xml أو استيراد CSV."
    )

    _presets_ui = load_preset_competitors()
    with st.expander("📌 **المنافسون المحفوظون** (ملف `data/preset_competitors.json`)", expanded=True):
        if not _presets_ui:
            st.warning(
                "تعذر تحميل القائمة. أنشئ أو راجع الملف: **`data/preset_competitors.json`** "
                "(مصفوفة JSON: `name`, `store_url`, `sitemap_url` لكل متجر)."
            )
        else:
            _plabels = [p["name"] for p in _presets_ui]
            _bp1, _bp2, _bp3 = st.columns([1, 1, 2])
            with _bp1:
                if st.button(
                    "✅ تحديد الكل",
                    key="preset_select_all_btn",
                    help=f"اختيار كل المنافسين ({len(_plabels)})",
                ):
                    st.session_state.scrape_preset_selection = list(_plabels)
                    st.rerun()
            with _bp2:
                if st.button("⏹️ مسح التحديد", key="preset_clear_btn"):
                    st.session_state.scrape_preset_selection = []
                    st.rerun()
            st.multiselect(
                "اختر منافساً واحداً أو عدة منافسين من القائمة",
                options=_plabels,
                key="scrape_preset_selection",
                help="يُدمج مع المربع المجمّع والحقول أدناه عند **بدء الكشط**. لمتجر واحد فقط اختر اسماً واحداً.",
            )
            st.caption(
                f"**{len(_presets_ui)}** متجر في القائمة — عدّل الملف لتغيير الروابط الدائمة دون تعديل الكود."
            )

    st.text_area(
        "روابط مجمّعة أو جدول منسوخ (Excel / Sheets)",
        key="bulk_competitor_urls",
        height=140,
        placeholder=(
            "سطر لكل متجر — إما رابط فقط، أو ثلاثة أعمدة مفصولة بـ Tab:\n"
            "الاسم العربي\thttps://المتجر/\thttps://المتجر/sitemap.xml"
        ),
        help=(
            "يدعم لصق جدول ثلاثي الأعمدة: اسم المنافس، رابط المتجر، رابط sitemap.xml "
            "— يُستخدم الـ sitemap مباشرة دون بحث. يُدمج مع حقول «متجر 1، 2…»."
        ),
    )
    st.caption(
        "**عدة متاجر:** يُكشط كل متجر **بالتسلسل** ويُسجَّل في الكتالوج تحت مفتاحه، مع **Preview مترافق لكل متجر** "
        "أثناء الجلب، ثم يعمل **تحليل نهائي موحّد** يشمل جميع المنافسين في النهاية."
    )
    for i in range(len(st.session_state.scraper_urls)):
        st.caption(f"متجر {i+1}")
        st.text_input(
            "رابط",
            key=f"comp_url_{i}",
            placeholder="https://worldgivenchy.com/ar/",
            label_visibility="collapsed",
        )

    if st.button("➕ إضافة متجر آخر"):
        st.session_state.scraper_urls.append("")
        st.rerun()

    st.text_input(
        "اسم المنافس للعرض (في البطاقات والجداول)",
        key="competitor_display_name",
        placeholder="مثال: عالم جيفنشي — يُشتق تلقائياً من نطاق الرابط إذا تُرك فارغاً",
        help="يُمرَّر إلى المحرك وعمود «المنافس» بدل الاسم البرمجي. عند الفراغ يُستخدم النطاق من الرابط (مثل worldgivenchy.com).",
    )

    col_opt1, col_opt2, col_opt3 = st.columns(3)
    with col_opt1:
        scrape_bg = st.checkbox(
            "🌐 كشط في الخلفية (التنقل أثناء الكشط)",
            value=False,
            help="يُكمِل الجلب في خيط؛ مع حفظ CSV وتحديث كتالوج المنافس على دفعات، ومسار تحليل مترافق داخل الخيط. بعد الانتهاء يُحفظ التحليل كـ job.",
        )
    with col_opt2:
        pipeline_inline = st.checkbox(
            "⚡ تحليل مترافق مع الكشط (مطابقة أثناء الجلب — أسرع للنهاية)",
            value=True,
            disabled=scrape_bg,
            help="للكشط على الصفحة فقط: مطابقة على لقطات تراكمية أثناء الجلب ثم جولة نهائية. الكشط الخلفي يفعّل مساراً مماثلاً تلقائياً.",
        )
    with col_opt3:
        max_rows = st.number_input("حد الصفوف للمعالجة (0=كل)", 0, step=500)

    st.caption(
        "بعد انتهاء الكشط يُجدول **التحليل** تلقائياً (Job في الشريط الجانبي) — يمكنك التنقل أثناء التحليل."
    )
    st.caption(
        "💾 **دفعات أثناء الكشط:** يُحدَّث `data/competitors_latest.csv` وكتالوج المنافس كلّما تجاوز العدد "
        "`SCRAPER_INCREMENTAL_EVERY` (أو نفس خطوة المسار المترافق `SCRAPER_PIPELINE_EVERY`). "
        "المسار المترافق يُعيد المطابقة على **جميع** المنتجات المكسوبة حتى تلك اللحظة — دون انتظار نهاية الكشط."
    )

    pipeline_inline = bool(pipeline_inline) and (not scrape_bg)

    _snap_busy = _read_scrape_live_snapshot()
    _scrape_busy = _snap_busy.get("running") and not _snap_busy.get("done")

    if st.button("🚀 بدء الكشط والتحليل", type="primary", disabled=_scrape_busy):
        entries: list[dict] = []
        _preset_map = {p["name"]: p for p in load_preset_competitors()}
        for _pname in st.session_state.get("scrape_preset_selection") or []:
            _pr = _preset_map.get(_pname)
            if _pr:
                entries.append(
                    {
                        "label": _pr["name"],
                        "store_url": str(_pr.get("store_url") or ""),
                        "sitemap_url": str(_pr.get("sitemap_url") or ""),
                    }
                )
        entries.extend(
            _parse_competitor_bulk_entries(
                str(st.session_state.get("bulk_competitor_urls") or "")
            )
        )
        for i in range(len(st.session_state.scraper_urls)):
            v = (st.session_state.get(f"comp_url_{i}") or "").strip()
            if not v:
                continue
            if not v.startswith(("http://", "https://")):
                v = "https://" + v.lstrip("/")
            entries.append({"label": "", "store_url": v, "sitemap_url": None})
        entries = _dedupe_competitor_entries(entries)
        if not entries:
            st.warning(
                "⚠️ اختر منافساً من **المنافسين المحفوظين**، أو أدخل رابطاً في الحقول / المربع المجمّع."
            )
        else:
            resolved_triples: list[tuple[str, str, str]] = []
            prog_resolve = st.progress(0, "🔍 جاري تجهيز خرائط المواقع...")
            n_entries = len(entries)

            for i, e in enumerate(entries):
                label = str(e.get("label") or "")
                store = str(e.get("store_url") or "").strip()
                sm_direct = str(e.get("sitemap_url") or "").strip()
                hint = (label[:28] + "…") if len(label) > 28 else (label or store[:48])
                prog_resolve.progress(
                    (i) / max(n_entries, 1),
                    f"({i + 1}/{n_entries}) {hint}",
                )
                if sm_direct.startswith(("http://", "https://")):
                    src = store if store.startswith(("http://", "https://")) else sm_direct
                    resolved_triples.append((label, src, sm_direct))
                    continue
                if store.startswith(("http://", "https://")):
                    sitemap_url, msg = resolve_store_to_sitemap_url(store)
                    if sitemap_url:
                        resolved_triples.append((label, store, sitemap_url))
                    else:
                        st.error(f"❌ {hint or store}: {msg}")
                    continue
                st.error(f"❌ سطر بدون رابط صالح: {hint}")

            prog_resolve.progress(1.0, "✅ اكتمل تجهيز الخرائط")

            if not resolved_triples:
                st.error("❌ لم يتم العثور على أي خريطة موقع صالحة. لا يمكن بدء الكشط.")
            else:
                _fail_n = n_entries - len(resolved_triples)
                if _fail_n:
                    st.warning(
                        f"⚠️ تُجاهل {_fail_n} سطراً دون خريطة صالحة — يُكشط **{len(resolved_triples)}** متجراً في الطابور."
                    )
                our_df_pre = None
                if not os.path.isfile(our_path):
                    st.warning("⚠️ لم يُعثر على كتالوج المنتجات — يرجى رفع ملف `mahwous_catalog.csv`")
                    uploaded_catalog = st.file_uploader(
                        "📂 ارفع ملف كتالوج منتجاتك (mahwous_catalog.csv)",
                        type=["csv"],
                        key="catalog_uploader",
                    )
                    if uploaded_catalog:
                        os.makedirs("data", exist_ok=True)
                        with open(our_path, "wb") as f:
                            f.write(uploaded_catalog.read())
                        st.success("✅ تم حفظ الكتالوج — اضغط 'بدء الكشط والتحليل' الآن")
                        st.rerun()
                    st.stop()
                else:
                    try:
                        our_df_pre = pd.read_csv(our_path)
                    except Exception as e:
                        st.error(f"❌ تعذر قراءة الكتالوج المحلي: {e}")
                        our_df_pre = None

                if our_df_pre is not None:
                    if max_rows > 0:
                        our_df_pre = our_df_pre.head(int(max_rows))
                    os.makedirs("data", exist_ok=True)
                    _all_smaps = [p[2] for p in resolved_triples]
                    with open("data/competitors_list.json", "w", encoding="utf-8") as f:
                        json.dump(_all_smaps, f, ensure_ascii=False)

                    _comp_label = str(
                        st.session_state.get("competitor_display_name") or ""
                    ).strip()
                    _n_res = len(resolved_triples)
                    _single = _n_res <= 1
                    _scrape_queue = [
                        {
                            "sitemap": sm,
                            "comp_key": _comp_key_for_scrape_entry(
                                lbl, src, _comp_label, _single
                            ),
                            "source_url": src,
                        }
                        for lbl, src, sm in resolved_triples
                    ]
                    ctx = {
                        "our_df": our_df_pre,
                        "pipeline_inline": True if scrape_bg else pipeline_inline,
                        "pl_every": int(os.environ.get("SCRAPER_PIPELINE_EVERY", "100")),
                        "use_ai_partial": os.environ.get(
                            "SCRAPER_PIPELINE_AI_PARTIAL", ""
                        ).strip().lower()
                        in ("1", "true", "yes"),
                        "our_file_name": "mahwous_catalog.csv",
                        "scrape_bg": scrape_bg,
                        "user_comp_label": _comp_label,
                        "scrape_queue": _scrape_queue,
                    }
                    try:
                        with open(SCRAPE_BG_CONTEXT, "wb") as fctx:
                            pickle.dump(ctx, fctx)
                    except Exception as e:
                        st.error(f"❌ تعذر حفظ سياق الكشط: {e}")
                    else:
                        _clear_live_session_pkl()
                        # لا تُفرغ نتائج الأقسام — تبقى معروضة حتى تُستبدل بنتائج التحليل المندمج بعد اكتمال الجولة
                        _merge_scrape_live_snapshot(
                            analysis_reset=True,
                            running=True,
                            done=False,
                            success=False,
                            scrape={"current": 0, "total": 1, "label": "🕸️ يبدأ الكشط..."},
                        )
                        t_sc = threading.Thread(
                            target=_run_scrape_chain_background,
                            daemon=True,
                        )
                        add_script_run_ctx(t_sc)
                        t_sc.start()
                        _qk = "، ".join([str(j.get("comp_key", ""))[:40] for j in _scrape_queue[:5]])
                        _more = f" (+{_n_res - 5})" if _n_res > 5 else ""
                        if scrape_bg:
                            st.success(
                                "✅ **الكشط** يعمل في الخيط — يمكنك التنقل. "
                                "اللوحة المباشرة أدناه عند العودة لـ «رفع الملفات»؛ الشريط الجانبي يعرض التقدم."
                                f" — الطابور (**{_n_res}**): {_qk}{_more}"
                            )
                        else:
                            st.success(
                                "✅ **الكشط** يعمل — **اللوحة المباشرة** تُحدَّث دورياً (تقدّم لكل متجر بالتسلسل)."
                                f" — الطابور (**{_n_res}**): {_qk}{_more}"
                            )
                        st.rerun()

                if our_df_pre is None:
                    pass


# ════════════════════════════════════════════════
#  3. سعر أعلى
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
                        r_ai = call_ai(_prompt, "missing_analysis")
                        resp = r_ai["response"] if r_ai["success"] else "❌ فشل AI"
                        # تنظيف JSON من المخرجات
                        import re as _re
                        resp = _re.sub(r'```json.*?```', '', resp, flags=_re.DOTALL)
                        resp = _re.sub(r'```.*?```', '', resp, flags=_re.DOTALL)
                        st.markdown(f'<div class="ai-box">{resp}</div>', unsafe_allow_html=True)

            # ── فلاتر ─────────────────────────────────────────────────────
            opts = _cached_filter_options(df)
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

            # ── تصدير + مدقق سلة صارم ────────────────────────────────────
            _export_df = filtered.copy()
            _dropped_zero = 0
            if "سعر_المنافس" in _export_df.columns:
                _before_n = len(_export_df)
                _export_df = _export_df[pd.to_numeric(_export_df["سعر_المنافس"], errors="coerce").fillna(0) > 0]
                _dropped_zero = max(0, _before_n - len(_export_df))
            _export_ok, _export_issues = validate_export_product_dataframe(_export_df)
            if _dropped_zero > 0:
                st.info(f"ℹ️ تم استبعاد {_dropped_zero} صف بسعر منافس غير صالح (<= 0) من التصدير فقط.")
            if not _export_ok:
                st.error("❌ التصدير معطل مؤقتاً: البيانات لا تطابق معايير سلة الصارمة:")
                for _iss in _export_issues[:25]:
                    st.warning(_iss)

            _salla_ai = st.checkbox(
                "🤖 وصف «خبير مهووس» بالذكاء الاصطناعي في ملف استيراد سلة (عمود الوصف HTML)",
                value=False,
                key="miss_salla_ai_desc",
                help="يستخرج مكونات الهرم العطري من الويب (Fragrantica عبر fetch_fragrantica_info) ثم يدمجها مع وصف AI. يُلحق قسماً مرجعياً بالمكونات في HTML. يستهلك رصيد API.",
            )
            _salla_ai_n = 500
            if _salla_ai:
                _salla_ai_n = int(
                    st.number_input(
                        "أقصى عدد منتجات يُوصَف بالذكاء الاصطناعي (الباقي قالب HTML ثابت)",
                        min_value=1,
                        max_value=2000,
                        value=min(500, max(1, len(_export_df) if _export_ok and len(_export_df) > 0 else 500)),
                        key="miss_salla_ai_n",
                        help="زر «تجهيز ملف سلة» يولّد وصف AI حتى هذا الحد، ثم قالب ثابت لباقي الصفوف.",
                    )
                )

            _miss_fp = _missing_df_fingerprint(_export_df) if _export_ok and len(_export_df) > 0 else ""

            cc1, cc2, cc3, cc4 = st.columns(4)
            with cc1:
                if _export_ok:
                    excel_m = export_to_excel(_export_df, "مفقودة")
                    st.download_button("📥 Excel", data=excel_m, file_name="missing.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="miss_dl")
                else:
                    st.caption("📥 Excel — يتطلب إصلاح الأخطاء أعلاه")
            with cc2:
                if _export_ok:
                    _csv_m = _export_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                    st.download_button("📄 CSV", data=_csv_m, file_name="missing.csv", mime="text/csv", key="miss_csv")
                else:
                    st.caption("📄 CSV — يتطلب إصلاح الأخطاء أعلاه")
            with cc3:
                if _export_ok and len(_export_df) > 0:
                    st.markdown("**استيراد سلة**")
                    if st.button(
                        "⚙️ تجهيز ملف سلة (كل المنتجات المعروضة)",
                        key="miss_salla_prepare",
                        help="يبني ملف CSV جاهز للاستيراد في سلة ثم يمكنك تحميله من الزر التالي.",
                    ):
                        _n = len(_export_df)
                        _ai_n = min(_n, _salla_ai_n) if _salla_ai else 0
                        if _salla_ai and _ai_n > 60:
                            st.info(
                                f"ℹ️ سيتم توليد وصف AI لـ {_ai_n} منتجاً (من أصل {_n}) — قد يستغرق وقتاً ويستهلك رصيد API."
                            )
                        with st.spinner(f"جاري تجهيز {_n} منتجاً لملف سلة…"):
                            _salla_kw = {}
                            if _salla_ai and _ai_n > 0:
                                _salla_kw["generate_description"] = make_salla_desc_fn(True, _ai_n)
                            _blob = export_missing_products_to_salla_csv_bytes(_export_df, **_salla_kw)
                            st.session_state["missing_salla_csv_blob"] = _blob
                            st.session_state["missing_salla_csv_src_fp"] = _miss_fp
                        st.success(f"✅ تم تجهيز {_n} منتجاً — استخدم زر التحميل أدناه.")

                    _blob_ok = st.session_state.get("missing_salla_csv_blob")
                    _fp_saved = st.session_state.get("missing_salla_csv_src_fp")
                    if _blob_ok and _fp_saved == _miss_fp:
                        st.download_button(
                            "📥 تحميل ملف سلة CSV",
                            data=_blob_ok,
                            file_name="missing_salla_import.csv",
                            mime="text/csv; charset=utf-8",
                            key="miss_salla_csv_dl",
                            help="UTF-8 BOM — جاهز للاستيراد الجماعي في سلة.",
                        )
                    elif _blob_ok and _fp_saved != _miss_fp:
                        st.warning("⚠️ الفلاتر أو البيانات تغيّرت — اضغط «تجهيز» من جديد قبل التحميل.")
                else:
                    st.caption("🛒 سلة — يتطلب بيانات صالحة")
            with cc4:
                # ── خيارات الإرسال الذكي ─────────────────────────────
                _conf_opts = {"🟢 مؤكدة فقط": "green", "🟡 محتملة": "yellow", "🔵 الكل": ""}
                _conf_sel = st.selectbox("مستوى الثقة", list(_conf_opts.keys()), key="miss_conf_sel")
                _conf_val = _conf_opts[_conf_sel]
                if st.button("📤 إرسال بدفعات ذكية لـ Make", key="miss_make_all"):
                    _to_send = _export_df[_export_df["نوع_متاح"].str.strip() == ""] if "نوع_متاح" in _export_df.columns else _export_df
                    is_valid, issues = validate_export_product_dataframe(_to_send)
                    if not is_valid:
                        st.error("❌ تم إيقاف الإرسال! البيانات لا تطابق معايير سلة الصارمة:")
                        for issue in issues[:40]:
                            st.warning(issue)
                    else:
                        products = export_to_make_format(_to_send, "missing")
                        for _ip, _pr_row in enumerate(products):
                            if _ip < len(_to_send):
                                _pr_row["مستوى_الثقة"] = str(_to_send.iloc[_ip].get("مستوى_الثقة", "green"))
                        _prog_bar = st.progress(0, text="جاري الإرسال...")
                        _status_txt = st.empty()
                        def _miss_progress(sent, failed, total, cur_name):
                            pct = (sent + failed) / max(total, 1)
                            _prog_bar.progress(min(pct, 1.0), text=f"إرسال: {sent}/{total} | {cur_name}")
                            _status_txt.caption(f"✅ {sent} | ❌ {failed} | الإجمالي {total}")
                        res = send_batch_smart(products, batch_type="new",
                                               batch_size=20, max_retries=3,
                                               progress_cb=_miss_progress,
                                               confidence_filter=_conf_val)
                        _prog_bar.progress(1.0, text="اكتمل")
                        if res["success"]:
                            st.success(res["message"])
                            for _, _pr in _to_send.iterrows():
                                _pk = f"miss_{str(_pr.get('منتج_المنافس',''))[:30]}_{str(_pr.get('المنافس',''))}"
                                save_processed(_pk, str(_pr.get('منتج_المنافس','')),
                                             str(_pr.get('المنافس','')), "send_missing",
                                             new_price=safe_float(_pr.get('سعر_المنافس',0)))
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

            for row_i, (idx, row) in enumerate(page_df.iterrows()):
                name  = str(row.get("منتج_المنافس", ""))
                _row_slot = f"miss_p{pn}_r{row_i}"
                miss_price_key = f"input_price_{_row_slot}"
                _miss_key = f"missing_{name}_{idx}"
                if _miss_key in st.session_state.hidden_products:
                    continue

                price           = safe_float(row.get("سعر_المنافس", 0))
                brand           = str(row.get("الماركة", ""))
                comp            = str(row.get("المنافس", ""))
                size            = str(row.get("الحجم", ""))
                ptype           = str(row.get("النوع", ""))
                note            = str(row.get("ملاحظة", ""))
                _img_miss = str(row.get("صورة_المنافس", "") or row.get("image_url", "") or "").strip()
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
                    _variant_label_safe = _html_escape(str(variant_label or ""))
                    _variant_product_safe = _html_escape(str(variant_product or ""))
                    _variant_html = _dedent(
                        f"""
                        <div style="margin-top:6px;padding:5px 10px;border-radius:6px;
                                    background:{_badge_bg}22;border:1px solid {_badge_bg}88;
                                    font-size:.78rem;color:{_badge_bg};font-weight:700">
                            {_variant_label_safe}
                            <span style="font-weight:400;color:#aaa;margin-right:6px">
                                ({variant_score:.0f}%) → {_variant_product_safe[:50]}
                            </span>
                        </div>"""
                    ).strip()

                # ── بادج تستر ─────────────────────────────────────────
                _tester_badge = ""
                if is_tester_flag:
                    _tester_badge = '<span style="font-size:.68rem;padding:2px 7px;border-radius:10px;background:#9c27b022;color:#ce93d8;margin-right:6px">🏷️ تستر</span>'

                st.markdown(miss_card(
                    name=name, price=price, brand=brand, size=size,
                    ptype=ptype, comp=comp, suggested_price=suggested_price,
                    note=note if _is_similar else "",
                    variant_html=_variant_html, tester_badge=_tester_badge,
                    border_color=_border,
                    confidence_level=conf_level, confidence_score=conf_score,
                    image_url=_img_miss,
                ), unsafe_allow_html=True)

                _cpx, _ = st.columns([1, 5])
                with _cpx:
                    st.number_input(
                        "المقترح للإضافة (ر.س)",
                        value=float(suggested_price),
                        min_value=0.0,
                        step=1.0,
                        key=miss_price_key,
                        label_visibility="collapsed",
                        format="%.2f",
                    )

                # ── الأزرار — صف 1 ────────────────────────────────────
                b1,b2,b3,b4 = st.columns(4)

                with b1:
                    if st.button("🖼️ صور المنتج", key=f"imgs_{_row_slot}"):
                        with st.spinner("🔍 يبحث عن صور..."):
                            img_result = fetch_product_images(name, brand)
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

                with b2:
                    if st.button("🌸 مكونات", key=f"notes_{_row_slot}"):
                        with st.spinner("يجلب من Fragrantica Arabia..."):
                            fi = fetch_fragrantica_info(name)
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
                                st.session_state[f"frag_info_{_row_slot}"] = fi
                            else:
                                st.warning("لم يتم العثور على بيانات")

                with b3:
                    if st.button("🔎 تحقق مهووس", key=f"mhw_{_row_slot}"):
                        with st.spinner("يبحث في mahwous.com..."):
                            r_m = search_mahwous(name)
                            if r_m.get("success"):
                                avail = "✅ متوفر" if r_m.get("likely_available") else "❌ غير متوفر"
                                resp_text = str(r_m.get("reason",""))[:200]
                                # تنظيف JSON
                                import re as _re
                                resp_text = _re.sub(r'\{.*?\}', '', resp_text, flags=_re.DOTALL)
                                st.info(f"{avail} | أولوية: **{r_m.get('add_recommendation','—')}**\n{resp_text}")
                            else:
                                st.warning("تعذر البحث")

                with b4:
                    if st.button("💹 سعر السوق", key=f"mkt_m_{_row_slot}"):
                        with st.spinner("🌐 يبحث في السوق..."):
                            r_s = search_market_price(name, price)
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

                # ── الأزرار — صف 2 ────────────────────────────────────
                st.markdown('<div style="margin-top:6px"></div>', unsafe_allow_html=True)
                b5,b6,b7,b8 = st.columns(4)

                with b5:
                    if st.button("✍️ خبير الوصف", key=f"expert_{_row_slot}", type="primary"):
                        with st.spinner("🤖 خبير مهووس يكتب الوصف الكامل..."):
                            fi_cached = st.session_state.get(f"frag_info_{_row_slot}")
                            if not fi_cached:
                                fi_cached = fetch_fragrantica_info(name)
                                st.session_state[f"frag_info_{_row_slot}"] = fi_cached
                            desc = generate_mahwous_description(name, suggested_price, fi_cached)
                            # تنظيف أي JSON عارض
                            import re as _re
                            desc = _re.sub(r'```json.*?```','', desc, flags=_re.DOTALL)
                            st.session_state[f"desc_{_row_slot}"] = desc
                            st.success("✅ الوصف جاهز!")

                    if f"desc_{_row_slot}" in st.session_state:
                        with st.expander("📄 الوصف الكامل — خبير مهووس", expanded=True):
                            edited_desc = st.text_area(
                                "راجع وعدّل الوصف قبل الإرسال:",
                                value=st.session_state[f"desc_{_row_slot}"],
                                height=400,
                                key=f"desc_edit_{_row_slot}"
                            )
                            st.session_state[f"desc_{_row_slot}"] = edited_desc
                            _wc = len(edited_desc.split())
                            _col = "#4caf50" if _wc >= 1000 else "#ff9800"
                            st.markdown(f'<span style="color:{_col};font-size:.8rem">📊 {_wc} كلمة</span>', unsafe_allow_html=True)

                with b6:
                    _has_desc = f"desc_{_row_slot}" in st.session_state
                    _make_lbl = "📤 إرسال Make + وصف" if _has_desc else "📤 إرسال Make"
                    if st.button(_make_lbl, key=f"mk_m_{_row_slot}", type="primary" if _has_desc else "secondary"):
                        _desc_send = st.session_state.get(
                            f"desc_edit_{_row_slot}",
                            st.session_state.get(f"desc_{_row_slot}", ""),
                        )
                        _fi_send    = st.session_state.get(f"frag_info_{_row_slot}",{})
                        _img_url    = _fi_send.get("image_url","") if _fi_send else ""
                        _size_val   = extract_size(name)
                        _size_str   = f"{int(_size_val)}ml" if _size_val else size
                        try:
                            _send_price = float(st.session_state.get(miss_price_key, suggested_price))
                        except (TypeError, ValueError):
                            _send_price = suggested_price
                        if _send_price <= 0:
                            _send_price = suggested_price
                        # إرسال مباشر سواء كان هناك وصف أم لا
                        with st.spinner("📤 يُرسل لـ Make..."):
                            res = send_new_products([{
                                "أسم المنتج":  name,
                                "سعر المنتج":  _send_price,
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
                            save_hidden_product(_mk, name, "sent_to_make")
                            save_processed(_mk, name, comp, "send_missing",
                                           new_price=_send_price,
                                           notes=f"إضافة جديدة" + (f" + وصف {_wc} كلمة" if _wc > 0 else ""))
                            for k in [f"desc_{_row_slot}", f"frag_info_{_row_slot}", f"desc_edit_{_row_slot}"]:
                                if k in st.session_state: del st.session_state[k]
                            st.rerun()
                        else:
                            st.error(res["message"])

                with b7:
                    if st.button("🤖 تكرار؟", key=f"dup_{_row_slot}"):
                        with st.spinner("..."):
                            our_prods = []
                            if st.session_state.analysis_df is not None:
                                our_prods = st.session_state.analysis_df.get("المنتج", pd.Series()).tolist()[:50]
                            r_dup = check_duplicate(name, our_prods)
                            _dup_resp = str(r_dup.get("response",""))[:250]
                            # تنظيف JSON
                            import re as _re
                            _dup_resp = _re.sub(r'```.*?```','', _dup_resp, flags=_re.DOTALL).strip()
                            _dup_resp = _re.sub(r'\{[^}]{0,200}\}','[بيانات]', _dup_resp)
                            st.info(_dup_resp if r_dup.get("success") else "فشل")

                with b8:
                    if st.button("🗑️ تجاهل", key=f"ign_{_row_slot}"):
                        log_decision(name,"missing","ignored","تجاهل",0,price,-price,comp)
                        _ign = f"missing_{name}_{idx}"
                        st.session_state.hidden_products.add(_ign)
                        save_hidden_product(_ign, name, "ignored")
                        save_processed(_ign, name, comp, "ignored",
                                       new_price=price,
                                       notes="تجاهل من قسم المفقودة")
                        st.rerun()

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:8px 0">', unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات مفقودة!")
    else:
        st.info("ارفع الملفات أولاً")
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
            st.caption(
                "بعد كل جولة تحليل يُعاد تصنيف المراجعة تلقائياً عبر Gemini (ثقة ≥ 75%). "
                "الزر أدناه يعيد التشغيل يدوياً على أول 30 صفاً ويحدّث الجدول."
            )

            # ── تصنيف تلقائي بـ AI ────────────────────────────────────────
            col_r1, col_r2 = st.columns([2, 1])
            with col_r1:
                if st.button("🤖 إعادة تصنيف يدوي (أول 30)", type="primary", key="reclassify_review"):
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
                        if _rc_results:
                            _moved = 0
                            adf = st.session_state.get("analysis_df")
                            if adf is not None and not getattr(adf, "empty", True):
                                for rc in _rc_results:
                                    _sec = str(rc.get("section", "") or "")
                                    try:
                                        _conf = float(rc.get("confidence", 0) or 0)
                                    except (TypeError, ValueError):
                                        _conf = 0.0
                                    if not _sec or "مراجعة" in _sec or _conf < 75:
                                        continue
                                    try:
                                        _ixi = int(rc.get("idx"))
                                    except (TypeError, ValueError):
                                        continue
                                    if _ixi < 1 or _ixi > len(_items_rc):
                                        continue
                                    _it = _items_rc[_ixi - 1]
                                    try:
                                        _mask = (
                                            adf["المنتج"].astype(str) == _it["our"]
                                        ) & (
                                            adf["منتج_المنافس"].astype(str) == _it["comp"]
                                        )
                                    except Exception:
                                        continue
                                    for _ri in adf.index[_mask]:
                                        if "مراجعة" in str(adf.at[_ri, "القرار"]):
                                            adf.at[_ri, "القرار"] = _sec
                                            _moved += 1
                                            break
                                st.session_state.analysis_df = adf
                                _r_new = _split_results(adf)
                                _miss = st.session_state.results.get("missing")
                                if _miss is not None:
                                    _r_new["missing"] = _miss
                                st.session_state.results = _r_new
                                st.success(f"✅ حُدّث الجدول: نقل {_moved} صفاً بحسب Gemini")
                                st.rerun()
                            else:
                                for rc in _rc_results:
                                    _sec = rc.get("section", "")
                                    if _sec and "مراجعة" not in _sec and rc.get("confidence", 0) >= 75:
                                        _moved += 1
                                st.warning(
                                    f"اقتراحات AI: {_moved} — حمّل تحليلاً كاملاً من «رفع الملفات» لتطبيقها على الأقسام."
                                )
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
                our_img_rv = str(row.get("صورة_منتجنا", "") or "").strip()
                comp_img_rv = str(row.get("صورة_المنافس", "") or "").strip()

                _rv_key = f"review_{our_name}_{idx}"
                if _rv_key in st.session_state.hidden_products:
                    continue

                # لون الثقة
                _score_color = "#4caf50" if score >= 85 else "#ff9800" if score >= 70 else "#f44336"
                _diff_color  = "#f44336" if diff > 10 else "#4caf50" if diff < -10 else "#888"
                _diff_label  = f"+{diff:.0f}" if diff > 0 else f"{diff:.0f}"

                def _rv_img_tag(url: str, border_hex: str) -> str:
                    u = (url or "").strip()
                    if not u or u.lower() in ("nan", "none"):
                        return (
                            '<div style="min-height:76px;display:flex;align-items:center;justify-content:center;'
                            'color:#555;font-size:.62rem;border-radius:8px;background:#0a1424;border:1px dashed #333">لا صورة</div>'
                        )
                    eu = _html_escape(u, quote=True)
                    return (
                        f'<div style="text-align:center;margin-bottom:6px">'
                        f'<img src="{eu}" alt="" style="width:76px;height:76px;max-width:100%;object-fit:cover;'
                        f'border-radius:10px;border:1px solid {border_hex};background:#0e1628" '
                        f'loading="lazy" referrerpolicy="no-referrer" '
                        f"onerror=\"this.style.display='none'\" />"
                        f"</div>"
                    )

                _on = _html_escape(our_name[:120])
                _cn = _html_escape(comp_name[:120])
                _bs = _html_escape(brand)
                _sz = _html_escape(size)
                _cs = _html_escape(comp_name_s)

                # ── بطاقة المقارنة (مع صور) ─────────────────────────────────────
                st.markdown(f"""
                <div style="border:1px solid #ff980055;border-radius:10px;padding:12px;
                            margin:6px 0;background:linear-gradient(135deg,#0a1628,#0e1a30);">
                  <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                    <span style="font-size:.75rem;color:#888">🏷️ {_bs} | 📏 {_sz}</span>
                    <span style="font-size:.75rem;padding:2px 8px;border-radius:10px;
                                 background:{_score_color}22;color:{_score_color};font-weight:700">
                      نسبة المطابقة: {score:.0f}%
                    </span>
                  </div>
                  <div style="display:grid;grid-template-columns:1fr 60px 1fr;gap:10px;align-items:start">
                    <!-- منتجنا -->
                    <div style="background:#0d2040;border-radius:8px;padding:10px;border:1px solid #4fc3f733">
                      {_rv_img_tag(our_img_rv, "#4fc3f766")}
                      <div style="font-size:.65rem;color:#4fc3f7;margin-bottom:4px">📦 منتجنا</div>
                      <div style="font-weight:700;color:#fff;font-size:.88rem">{_on}</div>
                      <div style="font-size:1.1rem;font-weight:900;color:#4caf50;margin-top:6px">{our_price:,.0f} ر.س</div>
                    </div>
                    <!-- الفرق -->
                    <div style="text-align:center;padding-top:28px">
                      <div style="font-size:1.2rem;color:{_diff_color};font-weight:900">{_diff_label}</div>
                      <div style="font-size:.6rem;color:#555">ر.س</div>
                    </div>
                    <!-- منتج المنافس -->
                    <div style="background:#1a0d20;border-radius:8px;padding:10px;border:1px solid #ff572233">
                      {_rv_img_tag(comp_img_rv, "#ff572266")}
                      <div style="font-size:.65rem;color:#ff5722;margin-bottom:4px">🏪 {_cs}</div>
                      <div style="font-weight:700;color:#fff;font-size:.88rem">{_cn}</div>
                      <div style="font-size:1.1rem;font-weight:900;color:#ff9800;margin-top:6px">{comp_price:,.0f} ر.س</div>
                    </div>
                  </div>
                </div>""", unsafe_allow_html=True)

                # ── أزرار المراجعة ─────────────────────────────────────
                ba, bb, bc, bd, be, bf = st.columns(6)

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

                with bf:
                    if st.button("🔬 عميق", key=f"rv_deep_{idx}"):
                        with st.spinner("🔬 تحليل عميق..."):
                            r_d = ai_deep_analysis(
                                our_name, our_price, comp_name, comp_price,
                                section="⚠️ تحت المراجعة", brand=brand,
                            )
                            if r_d.get("success"):
                                st.markdown(
                                    f'<div class="ai-box">{r_d.get("response", "")}</div>',
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.warning(str(r_d.get("response", "فشل")))

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:6px 0">',
                            unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات تحت المراجعة!")
    else:
        st.info("ارفع الملفات أولاً")
# ════════════════════════════════════════════════
#  8. الذكاء الاصطناعي — Gemini مباشر
# ════════════════════════════════════════════════

# ════════════════════════════════════════════════
#  7b. تمت المعالجة — v26
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
                st.markdown(
                    f'<div style="padding:6px 10px;border-radius:6px;background:#0a1628;'
                    f'border:1px solid #1a2a44;font-size:.85rem">'
                    f'<span style="color:#888;font-size:.75rem">{p_ts[:16]}</span> &nbsp;'
                    f'{icon} <b style="color:#4fc3f7">{p_name[:60]}</b>'
                    f'<span style="color:#888"> — {p_act}{price_info}</span>'
                    f'{_notes_html}</div>',
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


elif page == "🤖 الذكاء الصناعي":
    db_log("ai", "view")

    # ── شريط الحالة ──
    if GEMINI_API_KEYS:
        st.markdown(f'''<div style="background:linear-gradient(90deg,#051505,#030d1f);
            border:1px solid #00C853;border-radius:10px;padding:10px 18px;
            margin-bottom:12px;display:flex;align-items:center;gap:10px;">
          <div style="width:10px;height:10px;border-radius:50%;background:#00C853;
                      box-shadow:0 0 8px #00C853;animation:pulse 2s infinite"></div>
          <span style="color:#00C853;font-weight:800;font-size:1rem">Gemini Flash — متصل مباشرة</span>
          <span style="color:#555;font-size:.78rem"> | {len(GEMINI_API_KEYS)} مفاتيح | {GEMINI_MODEL}</span>
        </div>''', unsafe_allow_html=True)
    else:
        st.error("❌ Gemini غير متصل — أضف GEMINI_API_KEYS في Streamlit Secrets")

    # ── سياق البيانات ──
    _ctx = []
    if st.session_state.results:
        _r = st.session_state.results
        _ctx = [
            f"المنتجات الكلية: {len(_r.get('all', pd.DataFrame()))}",
            f"سعر أعلى: {len(_r.get('price_raise', pd.DataFrame()))}",
            f"سعر أقل: {len(_r.get('price_lower', pd.DataFrame()))}",
            f"موافق: {len(_r.get('approved', pd.DataFrame()))}",
            f"مراجعة: {len(_r.get('review', pd.DataFrame()))}",
            f"مفقود: {len(_r.get('missing', pd.DataFrame()))}",
        ]
    _ctx_str = " | ".join(_ctx) if _ctx else "لم يتم تحليل بيانات بعد"

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "💬 دردشة مباشرة", "📋 لصق وتحليل", "🔍 تحقق منتج", "💹 بحث سوق", "📊 أوامر مجمعة"
    ])

    # ═══ TAB 1: دردشة Gemini مباشرة ═══════════
    with tab1:
        st.caption(f"📊 البيانات: {_ctx_str}")

        # صندوق المحادثة
        _chat_h = 430
        _msgs_html = ""
        if not st.session_state.chat_history:
            _msgs_html = """<div style="text-align:center;padding:60px 20px;color:#333">
              <div style="font-size:3rem">🤖</div>
              <div style="color:#666;margin-top:10px;font-size:1rem">Gemini Flash جاهز للمساعدة</div>
              <div style="color:#444;margin-top:6px;font-size:.82rem">
                اسأل عن الأسعار · المنتجات · توصيات التسعير · تحليل المنافسين
              </div>
            </div>"""
        else:
            for h in st.session_state.chat_history[-15:]:
                _msgs_html += f"""
                <div style="display:flex;justify-content:flex-end;margin:5px 0">
                  <div style="background:#1e1e3f;color:#B8B4FF;padding:8px 14px;
                              border-radius:14px 14px 2px 14px;max-width:82%;font-size:.88rem;
                              line-height:1.5">{h['user']}</div>
                </div>
                <div style="display:flex;justify-content:flex-start;margin:4px 0 10px 0">
                  <div style="background:#080f1e;border:1px solid #1a3050;color:#d0d0d0;
                              padding:10px 14px;border-radius:14px 14px 14px 2px;
                              max-width:88%;font-size:.88rem;line-height:1.65">
                    <span style="color:#00C853;font-size:.65rem;font-weight:700">
                      ● {h.get('source','Gemini')} · {h.get('ts','')}</span><br>
                    {h['ai'].replace(chr(10),'<br>')}
                  </div>
                </div>"""

        st.markdown(
            f'''<div style="background:#050b14;border:1px solid #1a3050;border-radius:12px;
                padding:14px;height:{_chat_h}px;overflow-y:auto;direction:rtl">
              {_msgs_html}
            </div>''', unsafe_allow_html=True)

        # إدخال
        _mc1, _mc2 = st.columns([5, 1])
        with _mc1:
            _user_in = st.text_input("اكتب رسالتك", key="gem_in",
                placeholder="اسأل Gemini — عن المنتجات، الأسعار، التوصيات...",
                label_visibility="collapsed")
        with _mc2:
            _send = st.button("➤ إرسال", key="gem_send", type="primary", use_container_width=True)

        # أزرار سريعة
        _qc = st.columns(4)
        _quick = None
        _quick_labels = [
            ("📉 أولويات الخفض", "بناءً على البيانات المحملة أعطني أولويات خفض الأسعار مع الأرقام"),
            ("📈 فرص الرفع", "حلّل فرص رفع الأسعار وأعطني توصية مرتبة"),
            ("🔍 أولويات المفقودات", "حلّل المنتجات المفقودة وأعطني أولويات الإضافة"),
            ("📊 ملخص شامل", f"أعطني ملخصاً تنفيذياً: {_ctx_str}"),
        ]
        for i, (lbl, q) in enumerate(_quick_labels):
            with _qc[i]:
                if st.button(lbl, key=f"q{i}", use_container_width=True):
                    _quick = q

        _msg_to_send = _quick or (_user_in if _send and _user_in else None)
        if _msg_to_send:
            _full = f"سياق البيانات: {_ctx_str}\n\n{_msg_to_send}"
            with st.spinner("🤖 Gemini يفكر..."):
                _res = gemini_chat(_full, st.session_state.chat_history)
            if _res["success"]:
                st.session_state.chat_history.append({
                    "user": _msg_to_send, "ai": _res["response"],
                    "source": _res.get("source","Gemini"),
                    "ts": datetime.now().strftime("%H:%M")
                })
                st.session_state.chat_history = st.session_state.chat_history[-40:]
                st.rerun()
            else:
                st.error(_res["response"])

        _dc1, _dc2 = st.columns([4,1])
        with _dc2:
            if st.session_state.chat_history:
                if st.button("🗑️ مسح", key="clr_chat"):
                    st.session_state.chat_history = []
                    st.rerun()

    # ═══ TAB 2: لصق وتحليل ══════════════════════
    with tab2:
        st.markdown("**الصق منتجات أو بيانات أو أوامر — Gemini سيحللها فوراً:**")

        _paste = st.text_area(
            "الصق هنا:",
            height=200, key="paste_box",
            placeholder="""يمكنك لصق:
• قائمة منتجات من Excel (Ctrl+C ثم Ctrl+V)
• أوامر: "خفّض كل منتج فرقه أكثر من 30 ريال"
• CSV مباشرة
• أي نص تريد تحليله""")

        _pc1, _pc2 = st.columns(2)
        with _pc1:
            if st.button("🤖 تحليل بـ Gemini", key="paste_go", type="primary", use_container_width=True):
                if _paste:
                    # إضافة سياق البيانات الحالية
                    _ctx_data = ""
                    if st.session_state.results:
                        _r2 = st.session_state.results
                        _all = _r2.get("all", pd.DataFrame())
                        if not _all.empty and len(_all) > 0:
                            cols = [c for c in ["المنتج","السعر","منتج_المنافس","سعر_المنافس","القرار"] if c in _all.columns]
                            if cols:
                                _ctx_data = "\n\nعينة من بيانات التطبيق:\n" + _all[cols].head(15).to_string(index=False)
                    with st.spinner("🤖 Gemini يحلل..."):
                        _pr = analyze_paste(_paste, _ctx_data)
                    st.markdown(f'<div class="ai-box">{_pr["response"]}</div>', unsafe_allow_html=True)
        with _pc2:
            if st.button("📊 تحويل لجدول", key="paste_table", use_container_width=True):
                if _paste:
                    try:
                        import io as _io
                        _df_p = pd.read_csv(_io.StringIO(_paste), sep=None, engine='python')
                        st.dataframe(_df_p.head(200), use_container_width=True)
                        _csv_p = _df_p.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                        st.download_button("📄 تحميل CSV", data=_csv_p,
                            file_name="pasted.csv", mime="text/csv", key="paste_dl")
                    except Exception:
                        st.warning("تعذر التحويل لجدول — جرب تنسيق CSV أو TSV")

    # ═══ TAB 3: تحقق منتج ══════════════════════
    with tab3:
        st.markdown("**تحقق من تطابق منتجين بدقة 100%:**")
        _vc1, _vc2 = st.columns(2)
        _vp1 = _vc1.text_input("🏷️ منتجنا:", key="v_our", placeholder="Dior Sauvage EDP 100ml")
        _vp2 = _vc2.text_input("🏪 المنافس:", key="v_comp", placeholder="ديور سوفاج بارفان 100 مل")
        _vc3, _vc4 = st.columns(2)
        _vpr1 = _vc3.number_input("💰 سعرنا:", 0.0, key="v_p1")
        _vpr2 = _vc4.number_input("💰 سعر المنافس:", 0.0, key="v_p2")
        if st.button("🔍 تحقق الآن", key="vbtn", type="primary"):
            if _vp1 and _vp2:
                with st.spinner("🤖 AI يتحقق..."):
                    _vr = verify_match(_vp1, _vp2, _vpr1, _vpr2)
                if _vr["success"]:
                    _mc = "#00C853" if _vr.get("match") else "#FF1744"
                    _ml = "✅ متطابقان" if _vr.get("match") else "❌ غير متطابقان"
                    st.markdown(f'''<div style="background:{_mc}22;border:1px solid {_mc};
                        border-radius:8px;padding:12px;margin:8px 0">
                      <div style="color:{_mc};font-weight:800;font-size:1.1rem">{_ml}</div>
                      <div style="color:#aaa;margin-top:4px">ثقة: <b>{_vr.get("confidence",0)}%</b></div>
                      <div style="color:#888;font-size:.88rem;margin-top:6px">{_vr.get("reason","")}</div>
                    </div>''', unsafe_allow_html=True)
                    if _vr.get("suggestion"):
                        st.info(f"💡 {_vr['suggestion']}")
                else:
                    st.error("فشل الاتصال")

    # ═══ TAB 4: بحث السوق ══════════════════════
    with tab4:
        st.markdown("**ابحث عن سعر السوق الحقيقي لأي منتج:**")
        _ms1, _ms2 = st.columns([3,1])
        with _ms1:
            _mprod = st.text_input("🔎 اسم المنتج:", key="mkt_prod",
                                    placeholder="Dior Sauvage EDP 100ml")
        with _ms2:
            _mcur = st.number_input("💰 سعرنا:", 0.0, key="mkt_price")

        if st.button("🌐 ابحث في السوق", key="mkt_btn", type="primary"):
            if _mprod:
                with st.spinner("🌐 Gemini يبحث في السوق..."):
                    _mr = search_market_price(_mprod, _mcur)
                if _mr.get("success"):
                    _mp = _mr.get("market_price", 0)
                    _rng = _mr.get("price_range", {})
                    _comps = _mr.get("competitors", [])
                    _rec = _mr.get("recommendation","")
                    _diff_v = _mp - _mcur if _mcur > 0 else 0
                    _diff_c = "#00C853" if _diff_v > 0 else "#FF1744" if _diff_v < 0 else "#888"

                    _src1, _src2 = st.columns(2)
                    with _src1:
                        st.metric("💹 سعر السوق", f"{_mp:,.0f} ر.س",
                                  delta=f"{_diff_v:+.0f} ر.س" if _mcur > 0 else None)
                    with _src2:
                        _mn = _rng.get("min",0); _mx = _rng.get("max",0)
                        st.metric("📊 نطاق السعر", f"{_mn:,.0f} - {_mx:,.0f} ر.س")

                    if _comps:
                        st.markdown("**🏪 منافسون في السوق:**")
                        for _c in _comps[:5]:
                            _cpv = float(_c.get("price",0))
                            _dv = _cpv - _mcur if _mcur > 0 else 0
                            st.markdown(
                                f"• **{_c.get('name','')}**: {_cpv:,.0f} ر.س "
                                f"({'أعلى' if _dv>0 else 'أقل'} بـ {abs(_dv):.0f}ر.س)" if _dv != 0 else
                                f"• **{_c.get('name','')}**: {_cpv:,.0f} ر.س"
                            )
                    if _rec:
                        st.markdown(f'<div class="ai-box">💡 {_rec}</div>', unsafe_allow_html=True)

        # صورة المنتج من Fragrantica
        with st.expander("🖼️ صورة ومكونات من Fragrantica Arabia", expanded=False):
            _fprod = st.text_input("اسم العطر:", key="frag_prod",
                                    placeholder="Dior Sauvage EDP")
            if st.button("🔍 ابحث في Fragrantica", key="frag_btn"):
                if _fprod:
                    with st.spinner("يجلب من Fragrantica Arabia..."):
                        _fi = fetch_fragrantica_info(_fprod)
                    if _fi.get("success"):
                        _fic1, _fic2 = st.columns([1,2])
                        with _fic1:
                            _img_url = _fi.get("image_url","")
                            if _img_url and _img_url.startswith("http"):
                                st.image(_img_url, width=240, caption=_fprod)
                            else:
                                st.markdown(f"[🔗 Fragrantica Arabia]({_FR}/search/?query={_fprod.replace(' ','+')})")
                        with _fic2:
                            _top = ", ".join(_fi.get("top_notes",[])[:5])
                            _mid = ", ".join(_fi.get("middle_notes",[])[:5])
                            _base = ", ".join(_fi.get("base_notes",[])[:5])
                            st.markdown(f"""
🌸 **القمة:** {_top or "—"}
💐 **القلب:** {_mid or "—"}
🌿 **القاعدة:** {_base or "—"}
📝 **{_fi.get('description_ar','')}**""")
                        if _fi.get("fragrantica_url"):
                            st.markdown(f"[🌐 صفحة العطر في Fragrantica]({_fi['fragrantica_url']})")
                    else:
                        st.info("لم يتم العثور على بيانات — تحقق من اسم العطر")

    # ═══ TAB 5: أوامر مجمعة ════════════════════
    with tab5:
        st.markdown("**نفّذ أوامر مجمعة على بياناتك:**")
        st.caption(f"📊 البيانات: {_ctx_str}")

        _cmd_section = st.selectbox(
            "اختر القسم:", ["الكل", "سعر أعلى", "سعر أقل", "موافق", "مراجعة", "مفقود"],
            key="cmd_sec"
        )
        _cmd_text = st.text_area(
            "الأمر أو السؤال:", height=120, key="cmd_area",
            placeholder="""أمثلة:
• حلّل المنتجات التي فرقها أكثر من 30 ريال وأعطني توصية
• رتّب المنتجات حسب الأولوية
• ما المنتجات التي تحتاج خفض سعر فوري؟
• أعطني ملخص مقارنة مع المنافسين"""
        )

        if st.button("⚡ تنفيذ الأمر", key="cmd_run", type="primary"):
            if _cmd_text and st.session_state.results:
                _sec_map = {
                    "سعر أعلى":"price_raise","سعر أقل":"price_lower",
                    "موافق":"approved","مراجعة":"review","مفقود":"missing"
                }
                _df_sec = None
                if _cmd_section != "الكل":
                    _k = _sec_map.get(_cmd_section)
                    _df_sec = st.session_state.results.get(_k, pd.DataFrame())
                else:
                    _df_sec = st.session_state.results.get("all", pd.DataFrame())

                if _df_sec is not None and not _df_sec.empty:
                    _cols = [c for c in ["المنتج","السعر","منتج_المنافس","سعر_المنافس","القرار","الفرق"] if c in _df_sec.columns]
                    _sample = _df_sec[_cols].head(25).to_string(index=False) if _cols else ""
                    _full_cmd = f"""البيانات ({_cmd_section}) - {len(_df_sec)} منتج:
{_sample}

الأمر: {_cmd_text}"""
                    with st.spinner("⚡ Gemini ينفذ الأمر..."):
                        _cr = call_ai(_full_cmd, "general")
                    st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)
                else:
                    with st.spinner("🤖"):
                        _cr = call_ai(f"{_ctx_str}\n\n{_cmd_text}", "general")
                    st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)
            elif _cmd_text:
                with st.spinner("🤖"):
                    _cr = call_ai(_cmd_text, "general")
                st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)


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

    tab1, tab2, tab3 = st.tabs(["🔑 المفاتيح", "⚙️ المطابقة", "📜 السجل"])

    with tab1:
        # ── الحالة الحالية ────────────────────────────────────────────────
        _ms = _merged_api_summary()
        st.markdown("##### لوحة المزودين (لون = الحالة)")
        st.markdown(
            _settings_api_card_html("Google Gemini", "✨", _ms.get("gemini", "unknown")),
            unsafe_allow_html=True,
        )
        st.markdown(
            _settings_api_card_html("OpenRouter", "🔀", _ms.get("openrouter", "unknown")),
            unsafe_allow_html=True,
        )
        st.markdown(
            _settings_api_card_html("Cohere", "◎", _ms.get("cohere", "unknown")),
            unsafe_allow_html=True,
        )
        _whp = "ok" if WEBHOOK_UPDATE_PRICES else "absent"
        _whn = "ok" if WEBHOOK_NEW_PRODUCTS else "absent"
        _wh = "ok" if (_whp == "ok" or _whn == "ok") else "absent"
        st.markdown(
            _settings_api_card_html("Make.com (Webhooks)", "🔗", _wh),
            unsafe_allow_html=True,
        )
        st.caption(
            "بعد «تشخيص شامل» تظهر هنا **فاتورة/رصيد منتهٍ (402)** و**تجاوز حد (429)** بألوان مميزة. "
            "بدون تشخيص: يُعرض وجود المفتاح فقط."
        )

        st.markdown("---")

        # ── تشخيص شامل ───────────────────────────────────────────────────
        st.subheader("🔬 تشخيص AI")
        st.caption("يختبر الاتصال الفعلي بكل مزود ويُظهر الخطأ الحقيقي")

        if st.button("🔬 تشخيص شامل لجميع المزودين", type="primary"):
            with st.spinner("يختبر الاتصال بـ Gemini, OpenRouter, Cohere..."):
                from engines.ai_engine import diagnose_ai_providers
                diag = diagnose_ai_providers()
            _summ = _infer_api_diag_summary(diag)
            _summ["_from_diag"] = True
            st.session_state["api_diag_summary"] = _summ

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

            # ── نتائج OpenRouter ──────────────────────────────────────────
            or_res = diag.get("openrouter","")
            st.markdown("**OpenRouter:**")
            if "✅" in or_res: st.success(or_res)
            elif "⚠️" in or_res: st.warning(or_res)
            else: st.error(or_res)

            # ── نتائج Cohere (احتياطي — التطبيق يعمل بدونها) ───────────────
            co_res = diag.get("cohere","")
            st.markdown("**Cohere:**")
            if "✅" in co_res:
                st.success(co_res)
            elif "⚠️" in co_res:
                st.warning(co_res)
            else:
                st.error(co_res)

            # ── تحليل وتوصية ─────────────────────────────────────────────
            or_ok = "✅" in or_res
            co_ok = "✅" in co_res

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


# ════════════════════════════════════════════════
#  11. السجل
# ════════════════════════════════════════════════
elif page == "📜 السجل":
    st.header("📜 السجل الكامل")
    db_log("log", "view")

    tab1, tab2, tab3 = st.tabs(["📊 التحليلات", "💰 تغييرات الأسعار", "📝 الأحداث"])

    with tab1:
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

    with tab2:
        days = st.slider("آخر X يوم", 1, 30, 7)
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

    with tab3:
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
#  13. الأتمتة الذكية (v26.0 — متصل بالتنقل)
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
                        if summary.get("review", 0) > 0:
                            st.info(
                                f"⚠️ **{summary['review']}** قرار أُحيل لمراجعة يدوية "
                                "(خفض يتجاوز أقصى نزول آمن 25٪ — لن يُرسل تلقائياً إلى Make/سلة)."
                            )

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
            st.warning("⚠️ لا توجد نتائج تحليل — ارفع الملفات أولاً من صفحة 'رفع الملفات'")

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
                        _n_applied = _merge_verified_review_into_session(confirmed)
                        st.success(
                            f"✅ دُمج {_n_applied} صفاً في التحليل — {len(confirmed)} مؤكّداً من AI. انتقل للأقسام المحدَّثة."
                        )
                        st.dataframe(confirmed[["المنتج", "منتج_المنافس", "القرار"]].head(20),
                                     use_container_width=True)
                        st.rerun()
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
