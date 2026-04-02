"""صفحة أتمتة Make.com — مُستخرَجة من app.py."""
from __future__ import annotations

from typing import Callable

import pandas as pd
import streamlit as st

from utils.make_helper import (
    export_to_make_format,
    send_missing_products,
    send_price_updates,
    verify_webhook_connection,
)


def render_make_automation_page(*, db_log: Callable[..., None]) -> None:
    st.header("⚡ أتمتة Make.com")
    db_log("make", "view")
    st.caption(
        "**تعديل أسعار** (🔴 أعلى 🟢 أقل ✅ موافق) ← `WEBHOOK_UPDATE_PRICES`. "
        "**مفقودات** ← `WEBHOOK_MISSING_PRODUCTS` (سيناريو أتمتة التسعير فقط)."
    )

    tab1, tab2, tab3 = st.tabs(["🔗 حالة الاتصال", "📤 إرسال", "📦 القرارات المعلقة"])

    with tab1:
        if st.button("🔍 فحص الاتصال"):
            with st.spinner("..."):
                results = verify_webhook_connection()
                _wh_labels = {
                    "update_prices": "تعديل الأسعار (🔴🟢✅)",
                    "new_products": "مفقودات / أتمتة التسعير",
                }
                for name, r in results.items():
                    if name != "all_connected":
                        color = "🟢" if r["success"] else "🔴"
                        _lbl = _wh_labels.get(name, name)
                        st.markdown(f"{color} **{_lbl}:** {r['message']}")
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