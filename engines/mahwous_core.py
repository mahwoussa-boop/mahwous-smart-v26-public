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
        "مينياتشر", "travel size", "travel", "fial", "vial",
    ]

# كلمات إضافية للحاجز قبل قسم المفقودات
_EXTRA_BANNED = (
    "tester", "تستر", "تيستر", "عينة مجانية", "مجانية", "free sample",
    "سمبل", "vial",
)

# إكسسوارات — استبعاد فوري من قائمة المفقودات
_ACCESSORY_BANNED = (
    "حقيبة",
    "مِرشة",
    "مرشة",
    "علبة فارغة",
    "كيس حماية",
    "travel case",
    "empty box",
    "dust bag",
    "pouch only",
)

# حجم عينة: ≤ 8 مل (أو ما يعادلها تقريباً في الأونصة)
_ML_SAMPLE_RE = re.compile(
    r"(?P<n>\d+(?:[.,]\d+)?)\s*(ml|مل|ملي|milliliter)\b",
    re.IGNORECASE,
)
_OZ_SMALL_RE = re.compile(
    r"(?P<n>\d+(?:[.,]\d+)?)\s*(oz|أونصة|ounce)\b",
    re.IGNORECASE,
)


def _volume_indicates_sample_size(text: str) -> bool:
    """يُستبعد إن وُجد في الاسم حجم ≤ 8 مل، أو أونصة صغيرة جداً (≤0.28 oz ≈ 8 مل)."""
    if not text:
        return False
    s = str(text)
    for m in _ML_SAMPLE_RE.finditer(s):
        try:
            v = float(m.group("n").replace(",", "."))
            if 0 < v <= 8.0:
                return True
        except Exception:
            continue
    for m in _OZ_SMALL_RE.finditer(s):
        try:
            oz = float(m.group("n").replace(",", "."))
            if 0 < oz <= 0.28:
                return True
        except Exception:
            continue
    return False


def _combined_name_text(row: pd.Series, name_col: str, desc_col: str | None) -> str:
    parts = [str(row.get(name_col, "") or "")]
    if desc_col and desc_col in row.index:
        parts.append(str(row.get(desc_col, "") or ""))
    return " ".join(parts)


def product_has_volume_hint(text: str) -> bool:
    """يتحقق من وجود حجم (ml / oz / مل) في النص."""
    if not text or not str(text).strip():
        return False
    t = str(text).lower()
    if _ML_SAMPLE_RE.search(t) or _OZ_SMALL_RE.search(t):
        return True
    if re.search(r"\b\d+\s*(مل|ملي)\b", t):
        return True
    return False


def tag_missing_volume_status(
    df: pd.DataFrame,
    name_col: str = "منتج_المنافس",
    desc_col: str | None = None,
) -> pd.DataFrame:
    """
    يضيف عمود حالة_البيانات: «كامل» إن وُجد حجم في الاسم أو الوصف، وإلا «بيانات ناقصة».
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if desc_col and desc_col not in out.columns:
        desc_col = None
    if name_col not in out.columns:
        for alt in ("منتج_المنافس", "المنتج", "اسم المنتج", "Product", "name"):
            if alt in out.columns:
                name_col = alt
                break
    statuses = []
    for _, row in out.iterrows():
        blob = _combined_name_text(row, name_col, desc_col)
        statuses.append("كامل" if product_has_volume_hint(blob) else "بيانات ناقصة")
    out["حالة_البيانات"] = statuses
    return out


def apply_strict_pipeline_filters(
    df: pd.DataFrame,
    name_col: str = "منتج_المنافس",
    desc_col: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    فلترة صارمة: عينات (≤8مل + كلمات)، إكسسوارات، كلمات ممنوعة.
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

    dcol = desc_col if (desc_col and desc_col in work.columns) else None

    n0 = len(work)
    mask = pd.Series(True, index=work.index)

    def row_text(i) -> str:
        r = work.loc[i]
        return _combined_name_text(r, col, dcol)

    for kw in list(REJECT_KEYWORDS) + list(_EXTRA_BANNED):
        if not kw:
            continue
        mask &= ~work.index.to_series().map(
            lambda i: kw.lower() in row_text(i).lower()
        )

    for kw in _ACCESSORY_BANNED:
        if not kw:
            continue
        mask &= ~work.index.to_series().map(
            lambda i: kw.lower() in row_text(i).lower()
        )

    mask &= ~work.index.to_series().map(lambda i: _volume_indicates_sample_size(row_text(i)))

    filtered = work[mask].copy()
    dropped = n0 - len(filtered)
    return filtered, {
        "dropped": int(dropped),
        "name_col_used": col,
        "desc_col_used": dcol or "",
    }


def validate_export_product_dataframe(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """
    مدقق التصدير قبل Make / CSV — اسم، سعر، ماركة، تاريخ YYYY-MM-DD عند وجوده.
    يعيد (سليم؟، قائمة مشاكل).
    """
    issues: list[str] = []
    if df is None or df.empty:
        return False, ["لا توجد بيانات للتصدير أو الإرسال"]

    date_re = re.compile(r"^\d{4}-\d{2}-\d{2}$")

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

        brand = str(row.get("الماركة", "") or "").strip()
        if not brand or brand.lower() in ("nan", "none", "—", "-"):
            issues.append(f"صف {i + 1}: الماركة مطلوبة للتصدير")

        raw_p = row.get("سعر_المنافس", row.get("السعر", row.get("price", 0)))
        try:
            p = float(str(raw_p).replace(",", "").replace("ر.س", "").strip())
            if p <= 0:
                issues.append(f"صف {i + 1}: السعر يجب أن يكون أكبر من صفر (القيمة: {raw_p})")
        except Exception:
            issues.append(f"صف {i + 1}: السعر غير رقمي ({raw_p})")

        raw_date = row.get("تاريخ_الرصد", row.get("تاريخ", ""))
        if raw_date is not None and str(raw_date).strip():
            if hasattr(raw_date, "strftime"):
                ds = raw_date.strftime("%Y-%m-%d")
            else:
                ds = str(raw_date).strip()
            if not date_re.match(ds):
                issues.append(
                    f"صف {i + 1}: التاريخ يجب أن يكون بصيغة YYYY-MM-DD (القيمة: {raw_date})"
                )

        if len(issues) >= 100:
            issues.append("... توقف جمع المشاكل عند 100 بند — راجع الدفعة يدوياً")
            break

    return (len(issues) == 0, issues)
