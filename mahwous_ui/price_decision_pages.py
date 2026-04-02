"""صفحات أقسام القرار (سعر أعلى / أقل / موافق) — مُستخرَجة من app.py."""
from __future__ import annotations

from typing import Callable

import streamlit as st

from engines.ai_engine import call_ai
from utils.helpers import safe_float


def render_price_raise_decision_page(
    *,
    db_log: Callable[..., None],
    render_pro_table: Callable[..., None],
) -> None:
    st.header("🔴 منتجات سعرنا أعلى — فرصة خفض")
    db_log("price_raise", "view")
    if st.session_state.results and "price_raise" in st.session_state.results:
        df = st.session_state.results["price_raise"]
        if not df.empty:
            st.error(f"⚠️ {len(df)} منتج سعرنا أعلى من المنافسين")
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


def render_price_lower_decision_page(
    *,
    db_log: Callable[..., None],
    render_pro_table: Callable[..., None],
) -> None:
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


def render_approved_decision_page(
    *,
    db_log: Callable[..., None],
    render_pro_table: Callable[..., None],
) -> None:
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
