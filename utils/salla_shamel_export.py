"""
تصدير منتجات مفقودة بتنسيق CSV سلة الشامل (صف «بيانات المنتج» + أعمدة المرجع).
"""
import html
import io
import csv
import re

import pandas as pd

from engines.ai_engine import auto_infer_category, generate_mahwous_description
from utils.helpers import safe_float

_HTML_TAG_RE = re.compile(r"<[^>]+>")

# قالب «منتجات مهووس بتنسيق الشامل» — الصف الثاني (رؤوس الحقول) كما في سلة
SALLA_SHAMEL_COLUMNS = [
    "النوع",
    "أسم المنتج",
    "تصنيف المنتج",
    "صورة المنتج",
    "وصف صورة المنتج",
    "نوع المنتج",
    "سعر المنتج",
    "الكمية المتوفرة",
    "الوصف",
    "هل يتطلب شحن؟",
    "رمز المنتج sku",
    "سعر التكلفة",
    "السعر المخفض",
    "تاريخ بداية التخفيض",
    "تاريخ نهاية التخفيض",
    "اقصي كمية لكل عميل",
    "إخفاء خيار تحديد الكمية",
    "اضافة صورة عند الطلب",
    "الوزن",
    "وحدة الوزن",
    "الماركة",
    "العنوان الترويجي",
    "تثبيت المنتج",
    "الباركود",
    "السعرات الحرارية",
    "MPN",
    "GTIN",
    "خاضع للضريبة ؟",
    "سبب عدم الخضوع للضريبة",
    "[1] الاسم",
    "[1] النوع",
    "[1] القيمة",
    "[1] الصورة / اللون",
    "[2] الاسم",
    "[2] النوع",
    "[2] القيمة",
    "[2] الصورة / اللون",
    "[3] الاسم",
    "[3] النوع",
    "[3] القيمة",
    "[3] الصورة / اللون",
]


def _strip_html_visible(s: str) -> str:
    if not s:
        return ""
    t = _HTML_TAG_RE.sub(" ", str(s))
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


def _is_url_text(s: str) -> bool:
    t = str(s or "").strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _plain_missing_product_name(r: dict) -> str:
    """
    اسم منتج نصي للتصدير — يطابق منطق عرض المفقودات: لا رابط خام كاسم، ولا HTML.
    """
    def _clean(v):
        x = _strip_html_visible(str(v or "").strip())
        if not x or x.lower() in ("nan", "none", "<na>"):
            return ""
        return x

    for key in (
        "المنتج",
        "اسم المنتج",
        "اسم_المنتج",
        "Product",
        "Name",
        "name",
        "title",
        "الاسم",
        "منتج_المنافس",
    ):
        v = _clean(r.get(key))
        if v and not _is_url_text(v):
            return v

    br = _clean(r.get("الماركة"))
    sz = _clean(r.get("الحجم"))
    pt = _clean(r.get("النوع"))
    chunks = [c for c in (br, sz, pt) if c]
    if chunks:
        return " · ".join(chunks)

    return ""


def _real_price(r: dict) -> float:
    """سعر من مصدر البيانات (منافس) — أعمدة متعددة احتياطاً."""
    for k in (
        "سعر_المنافس",
        "سعر المنافس",
        "السعر",
        "سعر المنتج",
        "Price",
        "price",
        "PRICE",
    ):
        if k not in r:
            continue
        p = safe_float(r.get(k), 0.0)
        if p > 0:
            return round(p, 2)
    return 0.0


def _real_sku(r: dict) -> str:
    for k in (
        "معرف_المنافس",
        "رمز المنتج sku",
        "رمز_المنتج_sku",
        "SKU",
        "sku",
        "Sku",
        "رمز المنتج",
        "رمز_المنتج",
        "رقم المنتج",
        "Barcode",
        "barcode",
        "الباركود",
        "الكود",
        "كود",
        "Code",
        "code",
    ):
        v = r.get(k)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if not s or s.lower() in ("nan", "none", "<na>"):
            continue
        if s.startswith("http://") or s.startswith("https://"):
            continue
        try:
            fv = float(s.replace(",", ""))
            if fv == int(fv):
                return str(int(fv))
        except (ValueError, TypeError):
            pass
        return s
    return ""


def _real_qty(r: dict) -> int:
    for k in ("الكمية المتوفرة", "الكمية", "stock", "Stock", "المخزون", "الكميه"):
        if k not in r:
            continue
        v = r.get(k)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        try:
            q = int(float(str(v).replace(",", "").strip()))
            if q >= 0:
                return q
        except (ValueError, TypeError):
            continue
    return 10


def _weight_and_unit(r: dict) -> tuple:
    w = safe_float(r.get("الوزن"), 0.0)
    unit = str(r.get("وحدة الوزن") or r.get("weight_unit") or "").strip()
    if w <= 0:
        w = 0.2
    if not unit:
        unit = "kg"
    return round(w, 4), unit


def _placeholder_description(row: dict) -> str:
    name = str(row.get("منتج_المنافس", "") or "")
    brand = str(row.get("الماركة", "") or "")
    return (
        f"【مسودة سريعة】عطر أصلي 100% — {name} — {brand}. "
        f"استخدم زر «خبير الوصف» في صفحة المفقودات لتوليد وصف كامل بلهجة مهووس، "
        f"أو صدّر مع خيار «توليد وصف AI»."
    )


def export_to_salla_shamel(missing_df: pd.DataFrame, generate_descriptions: bool = False) -> bytes:
    """
    يُنشئ ملف CSV بترميز UTF-8 مع BOM لاستيراد سلة الشامل.
    الصف الأول: «بيانات المنتج»، ثم صف رؤوس الأعمدة، ثم البيانات.
    """
    ncols = len(SALLA_SHAMEL_COLUMNS)

    if missing_df is None or missing_df.empty:
        out = io.StringIO(newline="")
        w = csv.writer(out)
        w.writerow(["بيانات المنتج"] + [""] * (ncols - 1))
        w.writerow(SALLA_SHAMEL_COLUMNS)
        return ("\ufeff" + out.getvalue()).encode("utf-8")

    rows_out = []
    for _, row in missing_df.iterrows():
        r = row.to_dict()
        pname = _plain_missing_product_name(r)
        if not pname:
            pname = _strip_html_visible(str(r.get("منتج_المنافس", "") or "")) or "منتج"

        comp_price = _real_price(r)
        list_price = comp_price if comp_price > 0 else 1.0

        brand = str(r.get("الماركة", "") or "").strip()
        gender = str(r.get("الجنس", "") or "").strip()
        img = str(r.get("صورة_المنافس", "") or r.get("image_url", "") or "").strip()
        sku = _real_sku(r)
        qty = _real_qty(r)
        w_val, w_unit = _weight_and_unit(r)

        extra = f"الحجم: {r.get('الحجم', '')} | النوع: {r.get('النوع', '')} | الجنس: {gender}"
        seo = {}
        if generate_descriptions:
            res = generate_mahwous_description(
                pname, comp_price if comp_price > 0 else list_price,
                fragrantica_data=None, extra_info=extra, return_seo=True,
            )
            if isinstance(res, dict):
                desc_text = res.get("body") or ""
                seo = res.get("seo") or {}
            else:
                desc_text = str(res)
        else:
            desc_text = _placeholder_description(r)

        category = auto_infer_category(pname, gender)
        alt_txt = (seo.get("alt_text") or "").strip() or f"زجاجة عطر {pname} الأصلية"
        promo = (seo.get("page_title") or "").strip() or f"{pname} — {brand}".strip(" —")
        meta = (seo.get("meta_description") or "").strip()
        if meta and meta not in desc_text:
            desc_text = f"{desc_text}\n\n---\n{meta}"

        row_csv = {
            "النوع": "منتج",
            "أسم المنتج": pname,
            "تصنيف المنتج": category,
            "صورة المنتج": img,
            "وصف صورة المنتج": alt_txt,
            "نوع المنتج": "منتج جاهز",
            "سعر المنتج": list_price,
            "الكمية المتوفرة": qty,
            "الوصف": desc_text,
            "هل يتطلب شحن؟": "نعم",
            "رمز المنتج sku": sku,
            "سعر التكلفة": "",
            "السعر المخفض": "",
            "تاريخ بداية التخفيض": "",
            "تاريخ نهاية التخفيض": "",
            "اقصي كمية لكل عميل": 0,
            "إخفاء خيار تحديد الكمية": 0,
            "اضافة صورة عند الطلب": 0,
            "الوزن": w_val,
            "وحدة الوزن": w_unit,
            "الماركة": brand,
            "العنوان الترويجي": promo[:500],
            "تثبيت المنتج": "",
            "الباركود": str(r.get("الباركود") or r.get("Barcode") or "").strip(),
            "السعرات الحرارية": "",
            "MPN": "",
            "GTIN": "",
            "خاضع للضريبة ؟": "نعم",
            "سبب عدم الخضوع للضريبة": "",
            "[1] الاسم": "",
            "[1] النوع": "",
            "[1] القيمة": "",
            "[1] الصورة / اللون": "",
            "[2] الاسم": "",
            "[2] النوع": "",
            "[2] القيمة": "",
            "[2] الصورة / اللون": "",
            "[3] الاسم": "",
            "[3] النوع": "",
            "[3] القيمة": "",
            "[3] الصورة / اللون": "",
        }
        rows_out.append([row_csv[c] for c in SALLA_SHAMEL_COLUMNS])

    buf = io.StringIO(newline="")
    writer = csv.writer(buf)
    writer.writerow(["بيانات المنتج"] + [""] * (ncols - 1))
    writer.writerow(SALLA_SHAMEL_COLUMNS)
    for line in rows_out:
        writer.writerow(line)
    return ("\ufeff" + buf.getvalue()).encode("utf-8")
