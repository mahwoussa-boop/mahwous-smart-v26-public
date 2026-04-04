"""استعادة نتائج التحليل إلى جلسة Streamlit — مسار واحد للواجهة والبدء الاختياري."""
from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd
import streamlit as st

from mahwous_ui.sidebar_nav import focus_sidebar_on_analysis_results
from utils.analysis_sections import split_analysis_results
from utils.db_manager import get_last_job, load_all_comp_catalog_as_comp_dfs
from utils.results_io import restore_results_from_json

_logger = logging.getLogger(__name__)


def should_auto_restore_last_job() -> bool:
    """
    الاستعادة التلقائية عند فتح التطبيق معطّلة افتراضياً.
    لتفعيل السلوك القديم: MAHWOUS_AUTO_RESTORE_LAST_JOB=1
    """
    v = (os.environ.get("MAHWOUS_AUTO_RESTORE_LAST_JOB") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def apply_completed_job_to_session(
    job: dict[str, Any] | None,
    *,
    set_job_id: bool = True,
    focus_sidebar: bool = False,
) -> bool:
    """
    يحمّل وظيفة تحليل مكتملة من السجل إلى الجلسة.
    يعيد True إذا وُجدت نتائج صالحة وطُبّقت.
    """
    if not job or job.get("status") != "done" or not job.get("results"):
        return False
    records = restore_results_from_json(job["results"])
    df_all = pd.DataFrame(records)
    if df_all.empty:
        return False
    miss = pd.DataFrame(job.get("missing", [])) if job.get("missing") else pd.DataFrame()
    r = split_analysis_results(df_all)
    r["missing"] = miss
    st.session_state.results = r
    st.session_state.analysis_df = df_all
    if set_job_id:
        st.session_state.job_id = job.get("job_id")
    try:
        cdf = load_all_comp_catalog_as_comp_dfs()
        if cdf:
            st.session_state.comp_dfs = cdf
    except Exception:
        _logger.exception("load_all_comp_catalog_as_comp_dfs during session restore")
    if focus_sidebar:
        focus_sidebar_on_analysis_results(r)
    return True


def maybe_auto_restore_last_job(*, skip_due_to_live_scrape: bool) -> None:
    """استدعاء مرة عند بدء التطبيق — فقط إذا فُعّلت عبر البيئة وليست هناك جلسة حية."""
    if not should_auto_restore_last_job():
        return
    if st.session_state.results is not None:
        return
    if st.session_state.job_running:
        return
    if skip_due_to_live_scrape:
        return
    job = get_last_job()
    apply_completed_job_to_session(job, set_job_id=True, focus_sidebar=False)


def clear_analysis_session() -> None:
    """إزالة نتائج التحليل من الجلسة دون مسح كتالوج المنافسين المحمّل."""
    st.session_state.results = None
    st.session_state.analysis_df = None
    st.session_state.job_id = None
    st.session_state.job_running = False
