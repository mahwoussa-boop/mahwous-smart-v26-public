"""خيارات فلاتر مُخزَّنة مؤقتاً — مشتركة بين أقسام الجداول."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from utils.helpers import get_filter_options


@st.cache_data(ttl=300, show_spinner=False)
def cached_filter_options(df: pd.DataFrame):
    """خيارات الفلاتر — تُخزَّن مؤقتاً لتخفيف إعادة الحساب عند كل تفاعل."""
    return get_filter_options(df)
