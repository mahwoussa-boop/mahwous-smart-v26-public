"""
نواة فلاتر الأتمتة الصارمة — متوافقة مع سلة / تصدير Make.
يُستورد من محرك الحاجز الذكي ومن مدقق الإرسال النهائي.
"""
from __future__ import annotations

import re
from typing import Any

import pandas as pd

try:
    from config import REJECT_KEYWORDS
except Exception:
    REJECT_KEYWORDS = [
        "sample", "عينة", "عينه", "decant", "تقسيم", "split", "miniature",
        "مينياتشر", "travel size", "travel", "فial", "vial",
    ]

# كلمات إضافية للحاجز قبل قسم المفقودات
_EXTRA_BANNED = (
    "tester", "تستر", "تيستر", "عينة مجانية", "مجانية", "free sample",
)

# أحجام تعتبر عينات/تجارب (صارمة)
_SMALL_ML_RE = re.compile(
    r"\b(5|7|8|10|12|15|20)\s*(ml|مل|ملي|milliliter)\b",
    re.IGNORECASE,
)


def apply_strict_pipeline_filters(
    df: pd.DataFrame,
    name_col: str = "منتج_المنافس",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    فلترة صارمة: عينات، أحجام صغيرة جداً، كلمات ممنوعة.
    يعيد (dataframe_مفلتر، تقرير).
    """
    if df is None or df.empty:
        return df.copy() if df is not None else pd.DataFrame(), {"dropped": 0}

    work = df.copy()
    col = name_col
    if col not in work.columns:
        for alt in ("منتج_المنافس", "المنتج", "اسم المنتج", "Product", "name"):
            if alt in work.columns:
                col = alt
                break
        else:
            return work, {"dropped": 0, "warning": "no_name_column"}

    n0 = len(work)
    s = work[col].fillna("").astype(str)

    mask = pd.Series(True, index=work.index)
    for kw in list(REJECT_KEYWORDS) + list(_EXTRA_BANNED):
        if not kw:
            continue
        mask &= ~s.str.lower().str.contains(kw.lower(), regex=False, na=False)

    mask &= ~s.apply(lambda x: bool(_SMALL_ML_RE.search(str(x))))

    filtered = work[mask].copy()
    dropped = n0 - len(filtered)
    return filtered, {"dropped": int(dropped), "name_col_used": col}


def validate_export_product_dataframe(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """
    مدقق التصدير قبل Make / CSV — أسماء غير فارغة، أسعار رقمية معقولة، طول الاسم.
    يعيد (سليم؟، قائمة مشاكل).
    """
    issues: list[str] = []
    if df is None or df.empty:
        return False, ["لا توجد بيانات للتصدير أو الإرسال"]

    for i, (_, row) in enumerate(df.iterrows()):
        name = (
            str(row.get("منتج_المنافس", "") or "").strip()
            or str(row.get("المنتج", "") or "").strip()
            or str(row.get("اسم المنتج", "") or "").strip()
        )
        if not name or name.lower() in ("nan", "none", "—", "-"):
            issues.append(f"صف {i + 1}: اسم المنتج فارغ أو غير صالح")
            continue

        if len(name) > 500:
            issues.append(f"صف {i + 1}: الاسم يتجاوز 500 حرفاً (سلة)")

        raw_p = row.get("سعر_المنافس", row.get("السعر", row.get("price", 0)))
        try:
            p = float(str(raw_p).replace(",", "").replace("ر.س", "").strip())
            if p <= 0:
                issues.append(f"صف {i + 1}: السعر يجب أن يكون أكبر من صفر (القيمة: {raw_p})")
        except Exception:
            issues.append(f"صف {i + 1}: السعر غير رقمي ({raw_p})")

        if len(issues) >= 100:
            issues.append("... توقف جمع المشاكل عند 100 بند — راجع الدفعة يدوياً")
            break

    return (len(issues) == 0, issues)
