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
from html import escape as _html_escape
from textwrap import dedent as _dedent
import json
import logging
import os
import streamlit as st
import pandas as pd
import threading

_logger = logging.getLogger(__name__)

from mahwous_logging import configure_logging

configure_logging()

import time
import uuid
from datetime import datetime
from urllib.parse import urlparse

from async_scraper import (
    merge_scraper_bg_state,
    read_scraper_bg_state,
    run_scraper_sync,
    _load_sitemap_seeds,
    load_checkpoint_rows_ignore_fingerprint,
    get_scraper_sitemap_seeds,
    get_checkpoint_recovery_status,
)
from utils.session_pickle import (
    atomic_write_pickle,
    load_pickle,
    remove_pickle_and_tmp,
    safe_remove,
)

try:
    from streamlit.runtime.scriptrunner import add_script_run_ctx
except ImportError:
    try:
        from streamlit.scriptrunner import add_script_run_ctx
    except ImportError:
        def add_script_run_ctx(t): return t

from config import *
from styles import get_styles, stat_card, vs_card, comp_strip, miss_card, get_sidebar_toggle_js

from utils.analysis_sections import split_analysis_results
from utils.results_io import restore_results_from_json, safe_results_for_json
from mahwous_ui.sidebar_nav import focus_sidebar_on_analysis_results
from mahwous_ui.dashboard_page import render_dashboard_page
from utils.api_providers_ui import api_badges_html
from mahwous_ui.upload_page import UploadPageDeps, render_upload_page
from mahwous_ui.settings_page import render_settings_page
from mahwous_ui.history_log_page import render_history_log_page
from mahwous_ui.missing_products_page import render_missing_products_page
from mahwous_ui.review_page import render_review_page
from mahwous_ui.processed_page import render_processed_page
from mahwous_ui.ai_page import render_ai_page
from mahwous_ui.make_automation_page import render_make_automation_page
from mahwous_ui.pro_table import render_pro_table as _render_pro_table_impl
from mahwous_ui.automation_page import render_automation_page
from mahwous_ui.quick_add_page import render_quick_add_page
from mahwous_ui.price_decision_pages import (
    render_approved_decision_page,
    render_price_lower_decision_page,
    render_price_raise_decision_page,
)
from engines.engine import (read_file, run_full_analysis, find_missing_products,
                             extract_brand, extract_size, extract_type, is_sample,
                             smart_missing_barrier)
from engines.mahwous_core import ensure_export_brands, validate_export_product_dataframe
from engines.ai_engine import (call_ai,
                                verify_match, analyze_product,
                                bulk_verify, suggest_price,
                                search_market_price, search_mahwous,
                                check_duplicate, process_paste,
                                fetch_fragrantica_info, fetch_product_images,
                                generate_mahwous_description,
                                ai_deep_analysis,
                                apply_gemini_reclassify_to_analysis_df,
                                USER_MSG_AI_UNAVAILABLE)
from engines.automation import (AutomationEngine, ScheduledSearchManager,
                                 auto_push_decisions, auto_process_review_items,
                                 log_automation_decision, get_automation_log,
                                 get_automation_stats)
from utils.helpers import safe_float
from utils.make_helper import (send_price_updates, send_new_products,
                                send_single_product,
                                export_to_make_format)
from utils.db_manager import (init_db, log_event, log_decision,
                               log_analysis, upsert_price_history,
                               get_price_history,
                               save_job_progress, get_job_progress, get_last_job,
                               save_hidden_product, get_hidden_product_keys,
                               init_db_v26, upsert_our_catalog, upsert_comp_catalog,
                               merged_comp_dfs_for_analysis, load_all_comp_catalog_as_comp_dfs,
                               save_processed,
                               get_processed_keys, migrate_db_v26)


def _ui_autorefresh_interval(ms_default: int) -> int:
    """فاصل التحديث الحي (ملّي ث). يمكن رفعه عبر MAHWOUS_UI_LIVE_REFRESH_MS لتخفيف ثقل الواجهة أثناء الكشط."""
    v = os.environ.get("MAHWOUS_UI_LIVE_REFRESH_MS", "").strip()
    if v.isdigit():
        return max(2500, int(v))
    return ms_default


def _scrape_live_snapshot_min_interval_sec(total_urls: int) -> float:
    """
    أقل فاصل بين كتابات لقطة الكشط الحية — يمنع قراءة/كتابة JSON آلاف المرات.
    يُعدّل عبر MAHWOUS_SCRAPE_UI_MIN_INTERVAL_SEC (ثوانٍ).
    """
    env = (os.environ.get("MAHWOUS_SCRAPE_UI_MIN_INTERVAL_SEC") or "").strip()
    if env:
        try:
            return max(0.2, float(env.replace(",", ".")))
        except ValueError:
            pass
    t = int(total_urls or 0)
    if t > 1200:
        return 1.8
    if t > 600:
        return 1.45
    if t > 250:
        return 0.95
    return 0.5


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
    "audit_tools_mode": False,  # صفحة التدقيق والتحسين (mahwous_ui.audit_tools_page)
}
if "legacy_tools_mode" in st.session_state:
    _lm = bool(st.session_state.get("legacy_tools_mode"))
    del st.session_state["legacy_tools_mode"]
    if "audit_tools_mode" not in st.session_state:
        st.session_state["audit_tools_mode"] = _lm
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v


def _ensure_make_webhooks_session():
    """
    مزامنة روابط Make: Secrets/البيئة → جلسة Streamlit → os.environ
    (يستخدمها utils/make_helper عند الإرسال).
    • WEBHOOK_UPDATE_PRICES → تعديل أسعار (🔴 أعلى 🟢 أقل ✅ موافق)
    • WEBHOOK_MISSING_PRODUCTS → مفقودات فقط (سيناريو أتمتة التسعير)
    """
    if "WEBHOOK_UPDATE_PRICES" not in st.session_state:
        st.session_state["WEBHOOK_UPDATE_PRICES"] = get_webhook_update_prices()
    if "WEBHOOK_MISSING_PRODUCTS" not in st.session_state:
        if st.session_state.get("WEBHOOK_NEW_PRODUCTS"):
            st.session_state["WEBHOOK_MISSING_PRODUCTS"] = st.session_state["WEBHOOK_NEW_PRODUCTS"]
        else:
            st.session_state["WEBHOOK_MISSING_PRODUCTS"] = get_webhook_missing_products()
    os.environ["WEBHOOK_UPDATE_PRICES"] = (
        st.session_state.get("WEBHOOK_UPDATE_PRICES") or ""
    ).strip()
    _miss = (st.session_state.get("WEBHOOK_MISSING_PRODUCTS") or "").strip()
    os.environ["WEBHOOK_MISSING_PRODUCTS"] = _miss
    os.environ["WEBHOOK_NEW_PRODUCTS"] = _miss


_ensure_make_webhooks_session()


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
# خيط التحليل يكتب اللقطة بشكل متكرر؛ كتابة pickle عبر utils.session_pickle (قفل + مسار موثوق)
_CHECKPOINT_SORT_BG_LOCK = threading.Lock()


def _default_checkpoint_sort() -> dict:
    return {
        "active": False,
        "progress": 0.0,
        "phase": "",
        "error": None,
        "pending_hydrate": False,
    }


def _atomic_write_live_session_pkl(payload: dict) -> None:
    """كتابة pickle ذرية تحت data/ فقط (انظر utils.session_pickle)."""
    try:
        atomic_write_pickle(_LIVE_SESSION_PKL, payload)
    except Exception:
        _logger.exception("فشل كتابة live_session_results.pkl")


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
        blob = load_pickle(_LIVE_SESSION_PKL)
        st.session_state.results = blob["results"]
        st.session_state.analysis_df = blob.get("analysis_df")
        st.session_state.comp_dfs = blob.get("comp_dfs")
        if blob.get("our_df") is not None:
            st.session_state.our_df = blob["our_df"]
    except Exception:
        _logger.exception("فشل تحميل live_session_results.pkl للجلسة المبكرة")


_hydrate_live_session_results_early()


def _clear_live_session_pkl():
    try:
        remove_pickle_and_tmp(_LIVE_SESSION_PKL)
    except Exception as e:
        _logger.warning("تعذر مسح live_session_results.pkl: %s", e)


# ════════════════════════════════════════════════
#  دوال المعالجة — يجب تعريفها قبل استخدامها
#  (split_analysis_results واستعادة JSON في utils/)
# ════════════════════════════════════════════════
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
    r_new = split_analysis_results(adf)
    prev = st.session_state.results or {}
    if prev.get("missing") is not None:
        r_new["missing"] = prev["missing"]
    st.session_state.results = r_new
    return n


def _hydrate_checkpoint_sort_pending() -> bool:
    """بعد اكتمال فرز النقطة في الخلفية: تحميل pickle إلى الجلسة وإعادة التشغيل مرة واحدة."""
    if not os.path.isfile(_LIVE_SNAP_PATH):
        return False
    try:
        with open(_LIVE_SNAP_PATH, encoding="utf-8") as f:
            snap = json.load(f)
    except Exception:
        return False
    ck = snap.get("checkpoint_sort") or {}
    if not ck.get("pending_hydrate"):
        return False
    hydrated = False
    try:
        blob = load_pickle(_LIVE_SESSION_PKL)
        st.session_state.results = blob["results"]
        st.session_state.analysis_df = blob.get("analysis_df")
        st.session_state.comp_dfs = blob.get("comp_dfs")
        if blob.get("our_df") is not None:
            st.session_state.our_df = blob["our_df"]
        focus_sidebar_on_analysis_results(blob["results"])
        hydrated = True
    except Exception:
        _merge_scrape_live_snapshot(
            checkpoint_sort={
                "pending_hydrate": False,
                "error": "تعذر تحميل نتائج الفرز من الملف المؤقت.",
            },
        )
        return False
    _merge_scrape_live_snapshot(
        checkpoint_sort={"pending_hydrate": False, "phase": "✅ تم تحميل النتائج", "error": None},
    )
    if hydrated:
        st.rerun()
    return hydrated


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
        _auto_records = restore_results_from_json(_auto_job["results"])
        _auto_df = pd.DataFrame(_auto_records)
        if not _auto_df.empty:
            _auto_miss = pd.DataFrame(_auto_job.get("missing", [])) if _auto_job.get("missing") else pd.DataFrame()
            _auto_r = split_analysis_results(_auto_df)
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
        safe_records = safe_results_for_json(analysis_df.to_dict("records"))
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
                safe_results_for_json(analysis_df.to_dict("records")),
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
        "checkpoint_sort": _default_checkpoint_sort(),
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
        _d_ck = d.get("checkpoint_sort") or _default_checkpoint_sort()
        if isinstance(loaded.get("checkpoint_sort"), dict):
            loaded["checkpoint_sort"] = {**_d_ck, **loaded["checkpoint_sort"]}
        else:
            loaded["checkpoint_sort"] = dict(_d_ck)
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
            elif k == "checkpoint_sort" and isinstance(v, dict):
                cur.setdefault("checkpoint_sort", _default_checkpoint_sort())
                cur["checkpoint_sort"].update(v)
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
        r = split_analysis_results(analysis_df)
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
    try:
        _pe = int(os.environ.get("SCRAPER_PIPELINE_EVERY", "100") or 100)
    except ValueError:
        _pe = 100
    if _pe <= 0:
        _pe = 100
    try:
        _n_ck_disk = int(get_checkpoint_recovery_status().get("raw_row_count") or 0)
    except Exception:
        _n_ck_disk = 0
    st.caption(
        f"**🔴🟢…** تُحدَّث آلياً كل **~{_pe}** صف منتج في النقطة (وليس مع كل رابط كشط). "
        f"عدد «صفوف مكسوبة» قد يتقدّم على آخر فرز — اضغط الزر لفرز **كل** "
        f"**{_n_ck_disk:,}** صف محفوظ الآن وتحديث العدادات (المتبقي)."
    )
    _flush = st.button(
        "🔄 تحديث الفرز لكل الصفوف في النقطة الآن (المتبقي)",
        key="btn_live_flush_full_checkpoint",
        disabled=_n_ck_disk == 0,
        help="يشغّل المطابقة على كل صفوف scraper_checkpoint.json فوراً دون انتظار الدفعة التالية.",
        use_container_width=True,
    )
    if _flush:
        ok, err = _start_checkpoint_sort_background(log_action="live_flush_sort")
        if not ok:
            st.error(f"❌ {err}")
        else:
            st.success(
                f"✅ **بدأ** فرز **{_n_ck_disk:,}** صف في الخلفية — راقب التقدم في الشريط الجانبي والعدادات."
            )
            st.rerun()
    st.caption(
        "طابور عدة متاجر: قد تتأخر بعض الأقسام حتى اكتمال المتاجر. الجولة النهائية والـ Job من الشريط الجانبي."
    )


def _infer_comp_key_for_checkpoint_recovery() -> str:
    """يستنتج مفتاح المنافس كما في بدء الكشط — متجر واحد يحترم اسم العرض."""
    seeds = get_scraper_sitemap_seeds()
    if not seeds:
        return "Scraped_Competitor"
    user_label = str(st.session_state.get("competitor_display_name") or "").strip()
    return _comp_key_for_queue_entry(seeds[0], user_label, len(seeds) <= 1)


def _checkpoint_sort_progress_cb(pct, _results) -> None:
    """تحديث لقطة JSON أثناء run_full_analysis (شريط تقدّم + الشريط الجانبي)."""
    try:
        p = float(pct)
    except (TypeError, ValueError):
        p = 0.0
    p = max(0.0, min(1.0, p))
    overall = 0.04 + 0.82 * p
    _merge_scrape_live_snapshot(
        checkpoint_sort={
            "active": True,
            "progress": min(0.87, overall),
            "phase": f"⚙️ مطابقة المحرك والذكاء ({int(p * 100)}٪ من الكتالوج)",
            "error": None,
        },
        analysis={
            "phase": "فرز من نقطة الحفظ",
            "progress_pct": p,
            "ai_mode": "محرك + Gemini — تحقق مزدوج للمراجعة",
        },
    )


def _run_checkpoint_sort_pipeline(
    *,
    log_action: str,
    comp_key: str,
    emit_progress: bool,
    strict_verify: bool,
    update_session_state: bool,
) -> tuple[bool, str, int]:
    """
    فرز كامل من `scraper_checkpoint.json` مع تقدّم اختياري وتحقق Gemini أدق لصفوف المراجعة.
    `update_session_state=False` للخيط الخلفي — يكتب pickle ويضع pending_hydrate.
    """
    try:
        ck_rows = load_checkpoint_rows_ignore_fingerprint()
    except Exception as e:
        return False, f"قراءة النقطة: {e}", 0
    n_ck = len(ck_rows)
    if n_ck == 0:
        return False, "لا توجد صفوف في نقطة الحفظ بعد.", 0
    our_path = os.path.join("data", "mahwous_catalog.csv")
    if not os.path.isfile(our_path):
        return False, "لا يوجد `data/mahwous_catalog.csv` — ارفع الكتالوج أولاً.", 0
    try:
        our_df = pd.read_csv(our_path)
    except Exception as e:
        return False, f"قراءة الكتالوج: {e}", 0
    if our_df.empty:
        return False, "كتالوج منتجاتنا فارغ.", 0
    if update_session_state:
        st.session_state.our_df = our_df
    cdf = pd.DataFrame(ck_rows)
    try:
        comp_dfs = merged_comp_dfs_for_analysis(comp_key, cdf)
        _pcb = _checkpoint_sort_progress_cb if emit_progress else None
        analysis_df = run_full_analysis(
            our_df,
            comp_dfs,
            progress_callback=_pcb,
            use_ai=True,
        )
        if emit_progress:
            _merge_scrape_live_snapshot(
                checkpoint_sort={
                    "active": True,
                    "progress": 0.88,
                    "phase": "🔍 تحقق Gemini — إعادة تصنيف «تحت المراجعة» (دفعات دقيقة)",
                },
            )
        try:
            if strict_verify:
                apply_gemini_reclassify_to_analysis_df(
                    analysis_df, min_confidence=82.0, batch_size=12,
                )
                apply_gemini_reclassify_to_analysis_df(
                    analysis_df, min_confidence=74.0, batch_size=10,
                )
            else:
                apply_gemini_reclassify_to_analysis_df(analysis_df)
        except Exception:
            pass
        if emit_progress:
            _merge_scrape_live_snapshot(
                checkpoint_sort={
                    "active": True,
                    "progress": 0.93,
                    "phase": "📊 حساب المفقودات وتجميع الأقسام",
                },
            )
        r = split_analysis_results(analysis_df)
        missing_df = pd.DataFrame()
        try:
            raw_m = find_missing_products(our_df, comp_dfs)
            missing_df = smart_missing_barrier(raw_m, our_df)
            missing_df = _enrich_missing_df(missing_df)
        except Exception:
            missing_df = pd.DataFrame()
        r["missing"] = missing_df
        missing_n = len(missing_df)
        payload = {
            "results": r,
            "analysis_df": analysis_df,
            "comp_dfs": comp_dfs,
            "our_df": our_df,
            "is_partial": True,
            "comp_key": comp_key,
            "updated_at": datetime.now().isoformat(),
        }
        try:
            _atomic_write_live_session_pkl(payload)
        except Exception:
            pass
        snap = _read_scrape_live_snapshot()
        t = float((snap.get("scrape") or {}).get("total") or 1)
        prog_a = min(1.0, float(n_ck) / max(t, 1.0))
        _merge_scrape_live_snapshot(
            analysis={
                "phase": "🔄 فرز من النقطة — اكتمل",
                "progress_pct": prog_a,
                "ai_mode": "محرك + Gemini — تحقق مزدوج" if strict_verify else "محرك + Gemini",
                "counts": {
                    "price_raise": len(r["price_raise"]),
                    "price_lower": len(r["price_lower"]),
                    "approved": len(r["approved"]),
                    "review": len(r["review"]),
                    "missing": missing_n,
                },
                "scraped_rows": n_ck,
            },
            checkpoint_sort={
                "active": False,
                "progress": 1.0,
                "phase": "✅ اكتمل الفرز",
                "error": None,
                "pending_hydrate": not update_session_state,
            },
        )
        if update_session_state:
            st.session_state.results = r
            st.session_state.analysis_df = analysis_df
            st.session_state.comp_dfs = comp_dfs
            focus_sidebar_on_analysis_results(r)
        db_log("upload", log_action, f"rows={n_ck} comp={comp_key[:80]}")
        return True, "", n_ck
    except Exception as e:
        return False, str(e), 0


def _checkpoint_sort_worker(*, log_action: str, comp_key: str) -> None:
    try:
        ok, err, _n = _run_checkpoint_sort_pipeline(
            log_action=log_action,
            comp_key=comp_key,
            emit_progress=True,
            strict_verify=True,
            update_session_state=False,
        )
        if not ok:
            _merge_scrape_live_snapshot(
                checkpoint_sort={
                    "active": False,
                    "progress": 0.0,
                    "phase": "",
                    "error": err[:500] if err else "فشل غير معروف",
                    "pending_hydrate": False,
                },
            )
    except Exception as e:
        _merge_scrape_live_snapshot(
            checkpoint_sort={
                "active": False,
                "progress": 0.0,
                "phase": "",
                "error": str(e)[:500],
                "pending_hydrate": False,
            },
        )


def _start_checkpoint_sort_background(*, log_action: str) -> tuple[bool, str]:
    """يبدأ فرز النقطة في خيط خلفي مع تقدّم في scrape_live_snapshot.json."""
    snap = _read_scrape_live_snapshot()
    ck0 = snap.get("checkpoint_sort") or {}
    if ck0.get("active"):
        return False, "يوجد بالفعل فرز من نقطة الحفظ قيد التنفيذ — انتظر اكتماله."
    try:
        ck_rows = load_checkpoint_rows_ignore_fingerprint()
    except Exception as e:
        return False, f"قراءة النقطة: {e}"
    if not ck_rows:
        return False, "لا توجد صفوف في نقطة الحفظ بعد."
    our_path = os.path.join("data", "mahwous_catalog.csv")
    if not os.path.isfile(our_path):
        return False, "لا يوجد `data/mahwous_catalog.csv` — ارفع الكتالوج أولاً."
    comp_key = _infer_comp_key_for_checkpoint_recovery()
    with _CHECKPOINT_SORT_BG_LOCK:
        snap2 = _read_scrape_live_snapshot()
        ck1 = snap2.get("checkpoint_sort") or {}
        if ck1.get("active"):
            return False, "يوجد بالفعل فرز من نقطة الحفظ قيد التنفيذ."
        _merge_scrape_live_snapshot(
            checkpoint_sort={
                "active": True,
                "progress": 0.02,
                "phase": "⏳ جاري تجهيز الفرز في الخلفية…",
                "error": None,
                "pending_hydrate": False,
            },
        )
        t = threading.Thread(
            target=_checkpoint_sort_worker,
            kwargs={"log_action": log_action, "comp_key": comp_key},
            daemon=True,
            name="checkpoint-sort-bg",
        )
        t.start()
    return True, ""


def _render_checkpoint_recovery_panel(snap_live: dict) -> None:
    """فرز ومقارنة من `scraper_checkpoint.json` — لا يعتمد على استمرار الكشط ولا على بصمة الخرائط."""
    try:
        st_ck = get_checkpoint_recovery_status()
    except Exception:
        st_ck = {
            "file_exists": False,
            "raw_row_count": 0,
            "usable_row_count": 0,
            "fingerprint_match": False,
            "has_seeds_json": False,
            "checkpoint_path": os.path.join("data", "scraper_checkpoint.json"),
        }
    try:
        ck_rows = load_checkpoint_rows_ignore_fingerprint()
    except Exception:
        ck_rows = []

    busy = bool(snap_live.get("running")) and not bool(snap_live.get("done"))
    n_ck = len(ck_rows)
    n_raw = int(st_ck.get("raw_row_count") or 0)
    fp_ok = bool(st_ck.get("fingerprint_match"))
    _ck_live = (snap_live.get("checkpoint_sort") or {})

    with st.container(border=True):
        st.markdown("#### 🛟 طوارئ — فرز ومقارنة من نقطة الحفظ")
        st.caption(
            "يحمّل **كل الصفوف** من `data/scraper_checkpoint.json` ويشغّل **الفرز والمقارنة** فقط "
            "(محرك المطابقة + كتالوجك). **لا حاجة** لتطابق بصمة الخرائط مع الجلسة الحالية ولا لإيقاف الكشط."
        )
        _cp = st_ck.get("checkpoint_path") or "data/scraper_checkpoint.json"
        if not st_ck.get("file_exists"):
            st.info(
                f"📭 لا يوجد ملف (`{_cp}`). يُنشأ أثناء جلسة كشط؛ بدون ملف لا يوجد ما يُفرَز."
            )
        elif n_ck > 0:
            st.success(f"📦 **{n_ck:,}** صف في الملف — جاهز للفرز والمقارنة.")
            if not fp_ok and n_raw > 0:
                st.caption(
                    "ℹ️ بصمة `competitors_list.json` الحالية تختلف عن جلسة حفظ الملف — **لا يمنع الفرز**؛ "
                    "اسم مفتاح المنافس يُشتق من إعداداتك الحالية."
                )
        else:
            st.caption("📭 الملف موجود لكن لا توجد صفوف صالحة داخله.")

        if busy and n_ck > 0:
            st.caption(
                "⏳ **لقطة الواجهة** تُظهر «كشطاً قيد التشغيل» — يمكنك الضغط على الفرز إن أردت المقارنة فقط؛ "
                "إن كان الكشط عالقاً أعد تحميل الصفحة."
            )

        do_recover = st.button(
            "⚙️ تشغيل الفرز والمقارنة من نقطة الحفظ",
            key="btn_checkpoint_force_recovery",
            disabled=n_ck == 0 or bool(_ck_live.get("active")),
            help="فرز كامل في الخلفية مع شريط تقدّم وتحقق Gemini مزدوج لصفوف المراجعة.",
            use_container_width=True,
        )

        if _ck_live.get("active"):
            st.progress(
                min(float(_ck_live.get("progress") or 0), 0.99),
                _ck_live.get("phase") or "جاري الفرز…",
            )
            st.caption("⏳ يعمل في الخلفية دون تجميد الصفحة — راقب أيضاً الشريط الجانبي.")
            try:
                from streamlit_autorefresh import st_autorefresh

                st_autorefresh(interval=_ui_autorefresh_interval(2000), key="checkpoint_recovery_panel_refresh")
            except ImportError:
                time.sleep(2)
                st.rerun()

        if _ck_live.get("error") and not _ck_live.get("active"):
            st.error(f"❌ {_ck_live['error'][:400]}")

        if not do_recover:
            return

        ok, err = _start_checkpoint_sort_background(log_action="checkpoint_recovery")
        if not ok:
            st.error(f"❌ {err}")
            return
        st.success(
            f"✅ **بدأ** فرز **{n_ck:,}** صف في الخلفية — راقب التقدم هنا أو في الشريط الجانبي؛ "
            "عند الانتهاء تُحمَّل النتائج تلقائياً."
        )
        st.rerun()


def _run_scrape_chain_background():
    """كشط في الخيط: طابور متاجر بالتسلسل، ثم تحليل يشمل جميع المنافسين في الكتالوج."""
    try:
        ctx = load_pickle(SCRAPE_BG_CONTEXT)
    except Exception as e:
        _logger.exception("تعذر تحميل سياق الكشط من pickle")
        merge_scraper_bg_state(
            active=False,
            phase="error",
            error=f"تعذر تحميل سياق الكشط: {e}",
            progress=0.0,
            message="",
        )
        _live_scrape_thread_done(False, f"سياق: {e}")
        return
    if not safe_remove(SCRAPE_BG_CONTEXT):
        _logger.warning("تعذر حذف scrape_bg_context.pkl بعد التحميل")

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
            span = 1.0 / max(_ts, 1)
            base = _si / max(_ts, 1)
            pct = base + span * (current / max(total, 1))
            nm = (last_name or "")[:80]
            lbl = f"🏪 متجر {_si + 1}/{_ts} | 🕸️ {current}/{total} | {nm}"
            live_iv = _scrape_live_snapshot_min_interval_sec(int(total or 0))
            need_live = (now - _last_live[0] >= live_iv) or (current >= total)
            need_bg = scrape_bg and ((now - _last_merge[0] >= 1.35) or (current >= total))
            if need_live or need_bg:
                try:
                    _snap_r = _read_scrape_live_snapshot()
                    sr = int((_snap_r.get("analysis") or {}).get("scraped_rows") or 0)
                    if elapsed > 2.0 and sr > 0:
                        ppm = (float(sr) / elapsed) * 60.0
                except Exception:
                    pass
            if need_bg:
                _last_merge[0] = now
                merge_scraper_bg_state(
                    progress=min(pct, 0.998),
                    message=lbl[:220],
                    elapsed_sec=elapsed_i,
                    urls_per_min=round(urls_pm, 1),
                    products_per_min=round(ppm, 1),
                )
            if need_live:
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
#  مكوّن جدول المقارنة البصري (مشترك) → mahwous_ui/pro_table.py
# ════════════════════════════════════════════════
def render_pro_table(df, prefix, section_type="update", show_search=True):
    return _render_pro_table_impl(
        df, prefix, section_type=section_type, show_search=show_search, db_log=db_log
    )

#  الشريط الجانبي
# ════════════════════════════════════════════════
with st.sidebar:
    _hydrate_checkpoint_sort_pending()
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
        ai_label = "⚠️ لم يُضبط مفتاح API — راجع الإعدادات أو Secrets"

    st.markdown(
        f'<div style="background:{ai_color}22;border:1px solid {ai_color};'
        f'border-radius:6px;padding:6px;text-align:center;color:{ai_color};'
        f'font-weight:700;font-size:.85rem">{ai_label}</div>',
        unsafe_allow_html=True
    )
    st.markdown(api_badges_html(), unsafe_allow_html=True)

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

    elif (not _live_run) and (_live_sb.get("checkpoint_sort") or {}).get("active"):
        st.markdown(
            '<div style="background:#4A148C22;border:1px solid #7B1FA2;'
            'border-radius:8px;padding:8px;margin-bottom:8px;font-size:.78rem">'
            "<b>⚙️ فرز من نقطة الحفظ</b> — خلفية + تحقق Gemini مزدوج للمراجعة.</div>",
            unsafe_allow_html=True,
        )
        _ck_sb = _live_sb.get("checkpoint_sort") or {}
        st.progress(
            min(float(_ck_sb.get("progress") or 0), 0.99),
            _ck_sb.get("phase") or "جاري الفرز…",
        )
        _an_ck = _live_sb.get("analysis") or {}
        if _an_ck.get("phase") and str(_an_ck.get("phase")) != "idle":
            st.caption(f"📊 {_an_ck.get('phase', '')} — {_an_ck.get('ai_mode', '')}")
        try:
            from streamlit_autorefresh import st_autorefresh

            st_autorefresh(interval=_ui_autorefresh_interval(2000), key="sidebar_checkpoint_sort_refresh")
        except ImportError:
            time.sleep(2)
            st.rerun()

    _ck_err = (_live_sb.get("checkpoint_sort") or {}).get("error")
    if _ck_err and not (_live_sb.get("checkpoint_sort") or {}).get("active"):
        st.caption(f"⚠️ آخر فرز من النقطة: {_ck_err[:180]}")

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
                    _restored = restore_results_from_json(job["results"])
                    df_all = pd.DataFrame(_restored)
                    missing_df = pd.DataFrame(job.get("missing", [])) if job.get("missing") else pd.DataFrame()
                    _r = split_analysis_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results     = _r
                    st.session_state.analysis_df = df_all
                    try:
                        _cdf_done = load_all_comp_catalog_as_comp_dfs()
                        if _cdf_done:
                            st.session_state.comp_dfs = _cdf_done
                    except Exception:
                        pass
                    focus_sidebar_on_analysis_results(_r)
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

    st.markdown("---")
    try:
        _nav_audit_clicked = st.button(
            "🛠️ التدقيق والتحسين",
            key="nav_audit_tools",
            use_container_width=True,
            type="tertiary",
        )
    except TypeError:
        _nav_audit_clicked = st.button(
            "🛠️ التدقيق والتحسين",
            key="nav_audit_tools",
            use_container_width=True,
        )
    if _nav_audit_clicked:
        st.session_state.audit_tools_mode = True
        st.rerun()

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
#  التدقيق والتحسين (مقارنة / مدقق متجر / SEO)
# ════════════════════════════════════════════════
if st.session_state.get("audit_tools_mode"):
    from mahwous_ui.audit_tools_page import render_audit_tools_page

    render_audit_tools_page()
    st.stop()


# ════════════════════════════════════════════════
#  1. لوحة التحكم
# ════════════════════════════════════════════════
if page == "📊 لوحة التحكم":
    render_dashboard_page(db_log=db_log)


# ════════════════════════════════════════════════
#  2. رفع الملفات — كشط الويب + تحليل
# ════════════════════════════════════════════════
elif page == "📂 رفع الملفات":
    render_upload_page(
        UploadPageDeps(
            db_log=db_log,
            read_scrape_live_snapshot=_read_scrape_live_snapshot,
            render_live_scrape_dashboard=_render_live_scrape_dashboard,
            hydrate_live_session_results_early=_hydrate_live_session_results_early,
            clear_scrape_live_snapshot=_clear_scrape_live_snapshot,
            clear_live_session_pkl=_clear_live_session_pkl,
            merge_scrape_live_snapshot=_merge_scrape_live_snapshot,
            run_scrape_chain_background=_run_scrape_chain_background,
            render_checkpoint_recovery_panel=_render_checkpoint_recovery_panel,
            comp_key_for_scrape_entry=_comp_key_for_scrape_entry,
            scrape_bg_context_path=SCRAPE_BG_CONTEXT,
            ui_autorefresh_interval=_ui_autorefresh_interval,
            add_script_run_ctx=add_script_run_ctx,
            logger=_logger,
        )
    )


# ════════════════════════════════════════════════
#  2b. منتج سريع — صف واحد لاستيراد سلة
# ════════════════════════════════════════════════
elif page == "➕ منتج سريع":
    render_quick_add_page(db_log=db_log)


# ════════════════════════════════════════════════
#  3. سعر أعلى
# ════════════════════════════════════════════════
elif page == "🔴 سعر أعلى":
    render_price_raise_decision_page(db_log=db_log, render_pro_table=render_pro_table)


# ════════════════════════════════════════════════
#  4. سعر أقل
# ════════════════════════════════════════════════
elif page == "🟢 سعر أقل":
    render_price_lower_decision_page(db_log=db_log, render_pro_table=render_pro_table)


# ════════════════════════════════════════════════
#  5. موافق عليها
# ════════════════════════════════════════════════
elif page == "✅ موافق عليها":
    render_approved_decision_page(db_log=db_log, render_pro_table=render_pro_table)


# ════════════════════════════════════════════════
#  6. منتجات مفقودة — v26 مع كشف التستر/الأساسي
# ════════════════════════════════════════════════
elif page == "🔍 منتجات مفقودة":
    render_missing_products_page(db_log=db_log)


# ════════════════════════════════════════════════
#  7. تحت المراجعة — v26 مقارنة جنباً إلى جنب
# ════════════════════════════════════════════════
elif page == "⚠️ تحت المراجعة":
    render_review_page(db_log=db_log)

# ════════════════════════════════════════════════
#  7b. تمت المعالجة — v26
# ════════════════════════════════════════════════
elif page == "✔️ تمت المعالجة":
    render_processed_page(db_log=db_log)


# ════════════════════════════════════════════════
#  8. الذكاء الاصطناعي — Gemini مباشر
# ════════════════════════════════════════════════
elif page == "🤖 الذكاء الصناعي":
    render_ai_page(db_log=db_log)

# ════════════════════════════════════════════════
#  9. أتمتة Make
# ════════════════════════════════════════════════
elif page == "⚡ أتمتة Make":
    render_make_automation_page(db_log=db_log)


# ════════════════════════════════════════════════
#  10. الإعدادات
# ════════════════════════════════════════════════
elif page == "⚙️ الإعدادات":
    render_settings_page(
        db_log=db_log,
        ensure_make_webhooks_session=_ensure_make_webhooks_session,
    )


# ════════════════════════════════════════════════
#  11. السجل
# ════════════════════════════════════════════════
elif page == "📜 السجل":
    render_history_log_page(db_log=db_log)


# ════════════════════════════════════════════════
#  12. الأتمتة الذكية (v26.0 — متصل بالتنقل)
# ════════════════════════════════════════════════
elif page == "🔄 الأتمتة الذكية":
    render_automation_page(
        db_log=db_log,
        merge_verified_review_into_session=_merge_verified_review_into_session,
    )


