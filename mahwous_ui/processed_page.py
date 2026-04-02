"""صفحة المنتجات المعالجة — مُستخرَجة من app.py."""
from __future__ import annotations

from typing import Callable

import pandas as pd
import streamlit as st

from utils.helpers import safe_float
from utils.db_manager import get_processed, undo_processed


def render_processed_page(*, db_log: Callable[..., None]) -> None:
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