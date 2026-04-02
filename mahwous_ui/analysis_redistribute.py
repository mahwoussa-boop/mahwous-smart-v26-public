"""إعادة توزيع صف تحليل يدوياً بين الأقسام — يعتمد على جلسة Streamlit."""
from __future__ import annotations

from typing import Any, Callable

import pandas as pd
import streamlit as st

from utils.analysis_sections import MANUAL_BUCKET_DECISION, split_analysis_results


def apply_redistribute_analysis_row(
    our_name: str,
    comp_name: str,
    target_bucket: str,
    *,
    log_event_fn: Callable[..., Any],
) -> tuple[bool, str]:
    """نقل صف المطابقة إلى قسم آخر بتصحيح عمود القرار وإعادة split_results."""
    if target_bucket not in MANUAL_BUCKET_DECISION:
        return False, "قسم غير صالح"
    adf = st.session_state.get("analysis_df")
    if adf is None or getattr(adf, "empty", True):
        return False, "لا يوجد تحليل محمّل — شغّل المقارنة أولاً"
    dec = MANUAL_BUCKET_DECISION[target_bucket]
    try:
        adf = adf.copy()
        m = (adf["المنتج"].astype(str) == str(our_name).strip()) & (
            adf["منتج_المنافس"].astype(str) == str(comp_name).strip()
        )
        if not m.any():
            return False, "لم يُعثر على الصف في جدول التحليل (تحقق من اسم المنتج والمنافس)"
        adf.loc[m, "القرار"] = dec
        st.session_state.analysis_df = adf
        r_new = split_analysis_results(adf)
        prev = st.session_state.get("results") or {}
        if isinstance(prev.get("missing"), pd.DataFrame):
            r_new["missing"] = prev["missing"]
        st.session_state.results = r_new
        log_event_fn(
            "redistribute",
            "manual_bucket",
            f"{str(our_name)[:50]} → {target_bucket}",
        )
        return True, ""
    except Exception as e:
        return False, str(e)
