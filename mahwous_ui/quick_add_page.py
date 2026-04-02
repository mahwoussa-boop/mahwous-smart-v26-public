"""صفحة «منتج سريع» — مُستخرَجة من app.py."""
from __future__ import annotations

from typing import Callable

import streamlit as st

from utils.quick_add import render_quick_add_tab


def render_quick_add_page(*, db_log: Callable[..., None]) -> None:
    st.header("➕ منتج سريع")
    st.caption(
        "إضافة صف واحد بصيغة **بيانات المنتج** لسلة (40 عموداً كما في `export_missing_products_to_salla_csv_bytes`). "
        "التحقق عبر `validate_export_product_dataframe` في `mahwous_core`."
    )
    db_log("quick_add", "view")
    render_quick_add_tab()
