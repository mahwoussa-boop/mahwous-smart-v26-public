"""
تقسيم جدول التحليل إلى أقسام (سعر أعلى / أقل / موافق / مراجعة).
يُستخدم من المحرك وواجهة Streamlit — مصدر واحد لقيم «القرار» النصية.

تُبقى هذه الوحدة خالية من تبعية Streamlit؛ مناورات الجلسة (إعادة التوزيع) في
`mahwous_ui/analysis_redistribute.py`.
"""
from __future__ import annotations

import pandas as pd

# قيم «القرار» يجب أن تبقى متوافقة مع split_analysis_results (النص المفتاحي في str.contains)
MANUAL_BUCKET_DECISION = {
    "price_raise": "🔴 سعر أعلى",
    "price_lower": "🟢 سعر أقل",
    "approved": "✅ موافق",
    "review": "⚠️ تحت المراجعة",
}

RENDER_PREFIX_TO_BUCKET = {
    "raise": "price_raise",
    "lower": "price_lower",
    "approved": "approved",
    "review": "review",
}


def split_analysis_results(df: pd.DataFrame) -> dict:
    """تقسيم نتائج التحليل على الأقسام بأمان تام."""

    def _contains(col, txt):
        try:
            return df[col].str.contains(txt, na=False, regex=False)
        except Exception:
            return pd.Series([False] * len(df))

    return {
        "price_raise": df[_contains("القرار", "أعلى")].reset_index(drop=True),
        "price_lower": df[_contains("القرار", "أقل")].reset_index(drop=True),
        "approved": df[_contains("القرار", "موافق")].reset_index(drop=True),
        "review": df[_contains("القرار", "مراجعة")].reset_index(drop=True),
        "all": df,
    }
