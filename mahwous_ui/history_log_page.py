"""صفحة السجل الكامل — مُستخرَجة من app.py."""
from __future__ import annotations

from typing import Callable

import pandas as pd
import streamlit as st

from utils.db_manager import (
    get_analysis_history,
    get_events,
    get_price_changes,
)


def render_history_log_page(*, db_log: Callable[..., None]) -> None:
    st.header("📜 السجل الكامل")
    db_log("log", "view")

    tab1, tab2, tab3 = st.tabs(["📊 التحليلات", "💰 تغييرات الأسعار", "📝 الأحداث"])

    with tab1:
        history = get_analysis_history(20)
        if history:
            df_h = pd.DataFrame(history)
            st.dataframe(
                df_h[
                    ["timestamp", "our_file", "comp_file", "total_products", "matched", "missing"]
                ]
                .rename(
                    columns={
                        "timestamp": "التاريخ",
                        "our_file": "ملف منتجاتنا",
                        "comp_file": "ملف المنافس",
                        "total_products": "الإجمالي",
                        "matched": "متطابق",
                        "missing": "مفقود",
                    }
                )
                .head(200),
                use_container_width=True,
            )
        else:
            st.info("لا يوجد تاريخ")

    with tab2:
        days = st.slider("آخر X يوم", 1, 30, 7)
        changes = get_price_changes(days)
        if changes:
            df_c = pd.DataFrame(changes)
            st.dataframe(
                df_c.rename(
                    columns={
                        "product_name": "المنتج",
                        "competitor": "المنافس",
                        "old_price": "السعر السابق",
                        "new_price": "السعر الجديد",
                        "price_diff": "التغيير",
                        "new_date": "تاريخ التغيير",
                    }
                ).head(200),
                use_container_width=True,
            )
        else:
            st.info(f"لا توجد تغييرات في آخر {days} يوم")

    with tab3:
        events = get_events(limit=50)
        if events:
            df_e = pd.DataFrame(events)
            st.dataframe(
                df_e[["timestamp", "page", "event_type", "details"]]
                .rename(
                    columns={
                        "timestamp": "التاريخ",
                        "page": "الصفحة",
                        "event_type": "الحدث",
                        "details": "التفاصيل",
                    }
                )
                .head(200),
                use_container_width=True,
            )
        else:
            st.info("لا توجد أحداث")
