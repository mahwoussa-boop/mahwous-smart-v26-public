"""
تنقل الشريط الجانبي بعد التحليل — منفصل عن app.py لتقليل الازدواجية.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from config import SECTIONS


def first_section_with_results(r: dict) -> str | None:
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


def focus_sidebar_on_analysis_results(r: dict) -> None:
    target = first_section_with_results(r)
    if target:
        st.session_state.sidebar_page_radio = target
