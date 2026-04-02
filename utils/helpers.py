"""
utils/helpers.py - دوال مساعدة v17.2
الملف الذي كان مفقوداً - يحتوي على جميع الدوال المستوردة في app.py
"""
import csv
import html
import io
import json
import os
import re
import pandas as pd
from rapidfuzz import fuzz, process as rf_process
from typing import Any, Callable, Dict, List, Optional, Tuple, Union


# ===== safe_float =====
def safe_float(val, default=0.0) -> float:
    """تحويل قيمة إلى float بأمان"""
    try:
        if val is None or val == "" or (isinstance(val, float) and pd.isna(val)):
            return default
        return float(val)
    except (ValueError, TypeError):
        return default


# ===== format_price =====
def format_price(price, currency="ر.س") -> str:
    """تنسيق عرض السعر"""
    try:
        return f"{float(price):,.0f} {currency}"
    except (ValueError, TypeError):
        return f"0 {currency}"


# ===== format_diff =====
def format_diff(diff) -> str:
    """تنسيق عرض فرق السعر"""
    try:
        d = float(diff)
        sign = "+" if d > 0 else ""
        return f"{sign}{d:,.0f} ر.س"
    except (ValueError, TypeError):
        return "0 ر.س"


# ===== get_filter_options =====
def get_filter_options(df: pd.DataFrame) -> dict:
    """استخراج خيارات الفلاتر من DataFrame"""
    opts = {
        "brands": ["الكل"],
        "competitors": ["الكل"],
        "types": ["الكل"],
    }
    if df is None or df.empty:
        return opts

    # الماركات
    if "الماركة" in df.columns:
        brands = df["الماركة"].dropna().unique().tolist()
        brands = sorted([str(b) for b in brands if str(b).strip() and str(b) != "nan"])
        opts["brands"] = ["الكل"] + brands

    # المنافسون
    if "المنافس" in df.columns:
        comps = df["المنافس"].dropna().unique().tolist()
        comps = sorted([str(c) for c in comps if str(c).strip() and str(c) != "nan"])
        opts["competitors"] = ["الكل"] + comps

    # الأنواع
    if "النوع" in df.columns:
        types = df["النوع"].dropna().unique().tolist()
        types = sorted([str(t) for t in types if str(t).strip() and str(t) != "nan"])
        opts["types"] = ["الكل"] + types

    return opts


# ===== apply_filters =====
def apply_filters(df: pd.DataFrame, filters: dict) -> pd.DataFrame:
    """تطبيق الفلاتر على DataFrame"""
    if df is None or df.empty:
        return df

    result = df.copy()

    # بحث نصي
    search = filters.get("search", "").strip()
    if search:
        mask = pd.Series([False] * len(result))
        for col in ["المنتج", "منتج_المنافس", "الماركة"]:
            if col in result.columns:
                mask = mask | result[col].astype(str).str.contains(search, case=False, na=False)
        result = result[mask]

    # فلتر الماركة
    brand = filters.get("brand", "الكل")
    if brand and brand != "الكل" and "الماركة" in result.columns:
        result = result[result["الماركة"].astype(str) == brand]

    # فلتر المنافس
    competitor = filters.get("competitor", "الكل")
    if competitor and competitor != "الكل" and "المنافس" in result.columns:
        result = result[result["المنافس"].astype(str) == competitor]

    # فلتر النوع
    ptype = filters.get("type", "الكل")
    if ptype and ptype != "الكل" and "النوع" in result.columns:
        result = result[result["النوع"].astype(str) == ptype]

    # فلتر نسبة التطابق
    match_min = filters.get("match_min")
    if match_min and "نسبة_التطابق" in result.columns:
        result = result[result["نسبة_التطابق"] >= float(match_min)]

    # فلتر أقل سعر
    price_min = filters.get("price_min", 0.0)
    if price_min and price_min > 0 and "السعر" in result.columns:
        result = result[result["السعر"] >= float(price_min)]

    # فلتر أعلى سعر
    price_max = filters.get("price_max")
    if price_max and price_max > 0 and "السعر" in result.columns:
        result = result[result["السعر"] <= float(price_max)]

    return result.reset_index(drop=True)


# ===== export_to_excel =====
def export_to_excel(df: pd.DataFrame, sheet_name: str = "النتائج") -> bytes:
    """تصدير DataFrame إلى Excel"""
    output = io.BytesIO()
    export_df = df.copy()

    # إزالة الأعمدة غير القابلة للتسلسل
    for col in ["جميع المنافسين", "جميع_المنافسين"]:
        if col in export_df.columns:
            export_df = export_df.drop(columns=[col])

    safe_name = sheet_name[:31]
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        export_df.to_excel(writer, sheet_name=safe_name, index=False)

        # تنسيق العمود
        ws = writer.sheets[safe_name]
        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)

    return output.getvalue()


# ===== export_multiple_sheets =====
def export_multiple_sheets(sheets: Dict[str, pd.DataFrame]) -> bytes:
    """تصدير عدة DataFrames في ملف Excel متعدد الأوراق"""
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in sheets.items():
            export_df = df.copy()
            for col in ["جميع المنافسين", "جميع_المنافسين"]:
                if col in export_df.columns:
                    export_df = export_df.drop(columns=[col])

            safe_name = str(sheet_name)[:31]
            export_df.to_excel(writer, sheet_name=safe_name, index=False)

            # تنسيق تلقائي
            ws = writer.sheets[safe_name]
            for col in ws.columns:
                max_len = max(len(str(cell.value or "")) for cell in col)
                ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 50)

    return output.getvalue()


# ===== parse_pasted_text =====
def parse_pasted_text(text: str):
    """
    تحليل نص ملصوق وتحويله إلى DataFrame
    يدعم: CSV، TSV، جداول مفصولة بـ |
    """
    if not text or not text.strip():
        return None, "النص فارغ"

    lines = [l.strip() for l in text.strip().split('\n') if l.strip()]

    if not lines:
        return None, "لا توجد بيانات"

    # محاولة 1: مفصول بـ |
    if '|' in lines[0]:
        rows = []
        for line in lines:
            if set(line.replace(' ', '').replace('-', '')) == {'|'}:
                continue  # تخطي خطوط الفاصل
            cells = [c.strip() for c in line.split('|') if c.strip()]
            if cells:
                rows.append(cells)

        if len(rows) >= 2:
            try:
                df = pd.DataFrame(rows[1:], columns=rows[0])
                return df, f"✅ تم تحليل {len(df)} صف"
            except Exception:
                pass

    # محاولة 2: TSV (tabs)
    if '\t' in lines[0]:
        try:
            df = pd.read_csv(io.StringIO(text), sep='\t')
            return df, f"✅ تم تحليل {len(df)} صف (TSV)"
        except Exception:
            pass

    # محاولة 3: CSV
    try:
        df = pd.read_csv(io.StringIO(text))
        return df, f"✅ تم تحليل {len(df)} صف (CSV)"
    except Exception:
        pass

    # محاولة 4: كل سطر منتج
    if len(lines) >= 2:
        df = pd.DataFrame({"البيانات": lines})
        return df, f"✅ تم تحليل {len(df)} سطر"

    return None, "❌ لا يمكن تحليل الصيغة. جرب CSV أو جدول مفصول بـ |"


# ===== BackgroundTask (stub) =====
class BackgroundTask:
    """
    محاكاة معالجة في الخلفية
    ملاحظة: Streamlit لا يدعم true background threads بشكل كامل
    هذا placeholder وظيفي
    """
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.done = False
        self.error = None

    def run(self):
        """تشغيل المهمة مباشرة (synchronous)"""
        try:
            self.result = self.func(*self.args, **self.kwargs)
            self.done = True
        except Exception as e:
            self.error = str(e)
            self.done = True
        return self.result

    def is_done(self):
        return self.done


# ---------------------------------------------------------------------------
# تصدير منتجات مفقودة → CSV سلة (استيراد جماعي)
# ---------------------------------------------------------------------------

_SALLA_MISSING_HEADER_ROW2_RAW = (
    "النوع ,أسم المنتج,تصنيف المنتج,صورة المنتج,وصف صورة المنتج,نوع المنتج,سعر المنتج,الوصف,هل يتطلب شحن؟,"
    "رمز المنتج sku,سعر التكلفة,السعر المخفض,تاريخ بداية التخفيض,تاريخ نهاية التخفيض,اقصي كمية لكل عميل,"
    "إخفاء خيار تحديد الكمية,اضافة صورة عند الطلب,الوزن,وحدة الوزن,الماركة,العنوان الترويجي,تثبيت المنتج,"
    "الباركود,السعرات الحرارية,MPN,GTIN,خاضع للضريبة ؟,سبب عدم الخضوع للضريبة,"
    "[1] الاسم,[1] النوع,[1] القيمة,[1] الصورة / اللون,"
    "[2] الاسم,[2] النوع,[2] القيمة,[2] الصورة / اللون,"
    "[3] الاسم,[3] النوع,[3] القيمة,[3] الصورة / اللون"
)
SALLA_MISSING_HEADER_ROW2: List[str] = _SALLA_MISSING_HEADER_ROW2_RAW.split(",")
_SALLA_COL_COUNT = 40
assert len(SALLA_MISSING_HEADER_ROW2) == _SALLA_COL_COUNT


def _normalize_missing_rows_for_salla(
    missing_products_list: Union[pd.DataFrame, List[Dict[str, Any]], Any],
) -> List[Dict[str, Any]]:
    if missing_products_list is None:
        return []
    if hasattr(missing_products_list, "to_dict"):
        recs = missing_products_list.to_dict("records")
        return [dict(r) for r in recs]
    return [dict(r) for r in list(missing_products_list)]


def _salla_price_numeric(row: Dict[str, Any]) -> str:
    v = safe_float(row.get("سعر_المنافس", row.get("price", 0)), 0.0)
    if abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    s = f"{v:.2f}".rstrip("0").rstrip(".")
    return s if s else "0"


def _salla_plain_image_alt_text(s: str) -> str:
    """
    سلة ترفض «وصف صورة المنتج» إن وُجدت HTML أو أحرف خاصة — نص عربي/لاتيني بسيط فقط.
    """
    t = str(s or "")
    t = re.sub(r"<[^>]*>", " ", t)
    t = re.sub(r"[<>&\[\]{}\\|`#*_{}\"]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return (t[:480] if t else "صورة المنتج").strip()


def _load_valid_salla_category_paths() -> List[str]:
    """مسارات «أب > فرع» من data/categories.csv كما في لوحة سلة."""
    try:
        from engines.pipeline_enrichment import _build_category_rows
        from engines.reference_data import CATEGORIES_CSV
    except Exception:
        return []
    cdf = None
    for enc in ("utf-8-sig", "utf-8"):
        try:
            cdf = pd.read_csv(CATEGORIES_CSV, encoding=enc, on_bad_lines="skip")
            break
        except Exception:
            continue
    if cdf is None or cdf.empty:
        return []
    try:
        all_rows, sub_rows = _build_category_rows(cdf)
        pool = sub_rows if sub_rows else all_rows
        return [str(p["path"]).strip() for p in pool if p.get("path")]
    except Exception:
        return []


def _pick_default_salla_category(valid_paths: List[str]) -> str:
    """مسار افتراضي يطابق أقسام المتجر الفعلية (ليس «عطور للجنسين» إن لم تكن في CSV)."""
    env = (os.environ.get("SALLA_IMPORT_DEFAULT_CATEGORY") or "").strip()
    if env:
        for p in valid_paths:
            if p.replace(" ", "") == env.replace(" ", "") or p == env:
                return p
        if env in valid_paths:
            return env
    for key in ("عطور فرمونية", "عطور رجالية", "عطور نسائية", "عطور التستر"):
        for p in valid_paths:
            if key in p:
                return p
    return valid_paths[0] if valid_paths else "العطور > عطور رجالية"


def _resolve_category_for_salla(
    cat_raw: str,
    valid_paths: List[str],
    fallback: str,
) -> str:
    c = (cat_raw or "").strip()
    if not c:
        return fallback
    c = re.sub(r"\s*>\s*", " > ", c)
    for p in valid_paths:
        if p.strip() == c or p.replace(" ", "") == c.replace(" ", ""):
            return p
    best_p, best_sc = None, 0
    for p in valid_paths:
        sc = fuzz.token_set_ratio(c, p)
        if sc > best_sc:
            best_sc, best_p = sc, p
    if best_p and best_sc >= 86:
        return best_p
    # مسارات قديمة/غير موجودة في المتجر
    if any(x in c for x in ("للجنسين", "جنسين", "يونيسكس")):
        for p in valid_paths:
            if "فرمون" in p or "للجنسين" in p or "جنسين" in p:
                return p
        return fallback
    return fallback


def _build_salla_brand_lookup(brands_df: pd.DataFrame) -> List[Tuple[str, List[str]]]:
    out: List[Tuple[str, List[str]]] = []
    if brands_df is None or brands_df.empty:
        return out
    col = None
    for c in ("اسم الماركة", "الماركة", "brand", "Brand"):
        if c in brands_df.columns:
            col = c
            break
    if not col:
        return out
    for _, r in brands_df.iterrows():
        cell = str(r.get(col, "") or "").strip()
        if not cell or cell.lower() == "nan":
            continue
        tokens = [p.strip().lower() for p in cell.split("|") if p.strip()]
        out.append((cell, tokens))
    return out


def _resolve_brand_for_salla(
    raw_brand: str,
    product_name: str,
    lookup: List[Tuple[str, List[str]]],
) -> str:
    """
    يطابق اسم الماركة مع عمود «اسم الماركة» في brands.csv (عربي | English)
    كما تتوقع سلة — لا يُرسل «Gucci» فقط إن كان الملف يستخدم «غوتشي | Gucci».
    """
    canonical = [x[0] for x in lookup]
    raw = (raw_brand or "").strip()
    pname = (product_name or "").strip()

    def _infer_from_product() -> str:
        try:
            from engines.mahwous_core import _infer_brand_from_name
            from config import KNOWN_BRANDS

            kb = list(KNOWN_BRANDS) if isinstance(KNOWN_BRANDS, (list, tuple)) else []
            inf = _infer_brand_from_name(pname, kb)
            if not inf:
                return ""
            for cell, tokens in lookup:
                if inf.lower() in cell.lower():
                    return cell
                for tok in tokens:
                    if fuzz.token_set_ratio(inf.lower(), tok) >= 88:
                        return cell
            m = rf_process.extractOne(inf, canonical, scorer=fuzz.token_set_ratio)
            if m and m[1] >= 78:
                return m[0]
        except Exception:
            pass
        return ""

    if raw.lower() in ("غير محدد", "غير محدد.", "", "nan", "none"):
        fb = (os.environ.get("SALLA_IMPORT_FALLBACK_BRAND") or "").strip()
        if fb and canonical:
            m = rf_process.extractOne(fb, canonical, scorer=fuzz.token_set_ratio)
            if m and m[1] >= 70:
                return m[0]
        inferred = _infer_from_product()
        if inferred:
            return inferred
        if canonical:
            return canonical[0]
        return "غير محدد"

    if canonical:
        m = rf_process.extractOne(raw, canonical, scorer=fuzz.token_set_ratio)
        if m and m[1] >= 82:
            return m[0]
    for cell, tokens in lookup:
        for tok in tokens:
            if fuzz.token_set_ratio(raw.lower(), tok) >= 88:
                return cell
        if fuzz.token_set_ratio(raw.lower(), cell.lower()) >= 85:
            return cell
    return raw


def _salla_export_context() -> Dict[str, Any]:
    """يُحمَّل مرة لكل تصدير: مسارات تصنيف + ماركات مرجعية."""
    valid_paths = _load_valid_salla_category_paths()
    fallback_cat = _pick_default_salla_category(valid_paths)
    try:
        from engines.reference_data import BRANDS_CSV

        bdf = pd.read_csv(BRANDS_CSV, encoding="utf-8-sig", on_bad_lines="skip")
    except Exception:
        try:
            from engines.reference_data import BRANDS_CSV

            bdf = pd.read_csv(BRANDS_CSV, encoding="utf-8", on_bad_lines="skip")
        except Exception:
            bdf = pd.DataFrame()
    brand_lookup = _build_salla_brand_lookup(bdf)
    return {
        "valid_paths": valid_paths,
        "fallback_cat": fallback_cat,
        "brand_lookup": brand_lookup,
    }


def default_salla_missing_html_description(row: Dict[str, Any]) -> str:
    """وصف HTML افتراضي لقالب سلة (بدون استدعاء ذكاء اصطناعي)."""
    name = str(row.get("منتج_المنافس", "") or row.get("name", "") or "").strip()
    brand = str(row.get("الماركة", "") or "").strip()
    ctype = str(row.get("النوع", "") or "").strip()
    size = str(row.get("الحجم", "") or "").strip()
    he = html.escape
    name_e, brand_e, ctype_e, size_e = he(name), he(brand), he(ctype), he(size)
    parts = [f"<h2>{name_e}</h2>", "<p>"]
    if brand:
        parts.append(f"عطر من <strong>{brand_e}</strong>")
    if ctype:
        parts.append(f" بتركيز <strong>{ctype_e}</strong>")
    if size:
        parts.append(f"، حجم العبوة <strong>{size_e}</strong>")
    parts.append(".</p><ul>")
    if name:
        parts.append(f"<li>الاسم: {name_e}</li>")
    if brand:
        parts.append(f"<li>الماركة: {brand_e}</li>")
    if ctype:
        parts.append(f"<li>التركيز: {ctype_e}</li>")
    if size:
        parts.append(f"<li>الحجم: {size_e}</li>")
    parts.append("</ul><p>منتج أصلي من متجرنا — جودة عالية وتوصيل سريع.</p>")
    return "".join(parts)


def _strip_trailing_seo_json_from_mahwous_text(text: str) -> str:
    """يزيل كتلة JSON الختامية من رد خبير مهووس (MISSING_PAGE_SYSTEM)."""
    s = (text or "").strip()
    if not s:
        return ""
    if "\n\n{" in s:
        head, tail = s.rsplit("\n\n{", 1)
        tail = "{" + tail
        try:
            json.loads(tail)
            return head.strip()
        except Exception:
            pass
    m = re.search(r"\n\s*(\{[\s\S]*\})\s*$", s)
    if m:
        try:
            json.loads(m.group(1))
            return s[: m.start()].strip()
        except Exception:
            pass
    return s


def rough_markdown_to_html_for_salla(md: str) -> str:
    """تحويل تقريبي من Markdown (عناوين، قوائم، **غامق**) إلى HTML آمن."""
    md = _strip_trailing_seo_json_from_mahwous_text(md)
    if not md:
        return ""
    lines = md.replace("\r\n", "\n").split("\n")
    out: List[str] = []
    in_ul = False

    def close_ul() -> None:
        nonlocal in_ul
        if in_ul:
            out.append("</ul>")
            in_ul = False

    def inline_bold(s: str) -> str:
        parts = re.split(r"(\*\*.+?\*\*)", s)
        buf: List[str] = []
        for p in parts:
            if p.startswith("**") and p.endswith("**") and len(p) > 4:
                inner = html.escape(p[2:-2].strip())
                buf.append(f"<strong>{inner}</strong>")
            else:
                buf.append(html.escape(p))
        return "".join(buf)

    for line in lines:
        raw = line.rstrip()
        if not raw.strip():
            close_ul()
            continue
        if raw.startswith("### "):
            close_ul()
            out.append(f"<h3>{inline_bold(raw[4:].strip())}</h3>")
        elif raw.startswith("## "):
            close_ul()
            out.append(f"<h2>{inline_bold(raw[3:].strip())}</h2>")
        elif raw.startswith("# "):
            close_ul()
            out.append(f"<h2>{inline_bold(raw[2:].strip())}</h2>")
        elif raw.strip().startswith(("- ", "* ")):
            if not in_ul:
                out.append("<ul>")
                in_ul = True
            item = raw.strip()[2:].strip()
            out.append(f"<li>{inline_bold(item)}</li>")
        else:
            close_ul()
            out.append(f"<p>{inline_bold(raw.strip())}</p>")
    close_ul()
    return "".join(out)


def _normalize_note_list(val: Any) -> List[str]:
    if val is None:
        return []
    if isinstance(val, str):
        s = val.strip()
        return [s] if s else []
    if isinstance(val, list):
        out: List[str] = []
        for x in val[:24]:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out
    return []


def _fragrance_web_data_to_ai_context(frag: Dict[str, Any]) -> str:
    """نص عربي يُحقَن في برومبت الوصف ليلزم النموذج بمكونات مستخرجة من الويب."""
    if not frag.get("success"):
        return ""
    top = _normalize_note_list(frag.get("top_notes"))
    mid = _normalize_note_list(frag.get("middle_notes"))
    base = _normalize_note_list(frag.get("base_notes"))
    fam = str(frag.get("fragrance_family") or "").strip()
    des = str(frag.get("description_ar") or "").strip()
    yr = str(frag.get("year") or "").strip()
    des_snip = des[:900] if des else ""
    lines = [
        "【بيانات مرجعية مستخرجة من مواقع عطور (مثل Fragrantica Arabia) — التزم بها في قسم الهرم العطري ولا تخترع مكونات متعارضة معها. إن غابت قائمة فاذكر أن التفاصيل غير مكتملة】",
        f"- النفحات العليا: { '، '.join(top) if top else 'غير متوفرة في المصدر' }",
        f"- النفحات الوسطى: { '، '.join(mid) if mid else 'غير متوفرة في المصدر' }",
        f"- النفحات الأساسية: { '، '.join(base) if base else 'غير متوفرة في المصدر' }",
    ]
    if fam:
        lines.append(f"- العائلة العطرية: {fam}")
    if yr:
        lines.append(f"- سنة الإصدار (إن وُجدت): {yr}")
    if des_snip:
        lines.append(f"- وصف مرجعي مختصر: {des_snip}")
    u = str(frag.get("fragrantica_url") or "").strip()
    if u:
        lines.append(f"- رابط المصدر: {u}")
    return "\n".join(lines)


def _fragrance_web_data_to_html_appendix(frag: Dict[str, Any]) -> str:
    """قسم HTML يُلحق بالوصف: هرم عطري صريح + ملخص مرجعي + رابط المصدر."""
    if not frag.get("success"):
        return ""
    top = _normalize_note_list(frag.get("top_notes"))
    mid = _normalize_note_list(frag.get("middle_notes"))
    base = _normalize_note_list(frag.get("base_notes"))
    if not (top or mid or base):
        return ""
    chunks: List[str] = [
        '<h3>الهرم العطري (مصادر مرجعية من الويب)</h3>',
        "<ul>",
    ]
    if top:
        chunks.append(
            f"<li><strong>النفحات العليا:</strong> {html.escape('، '.join(top))}</li>"
        )
    if mid:
        chunks.append(
            f"<li><strong>النفحات الوسطى:</strong> {html.escape('، '.join(mid))}</li>"
        )
    if base:
        chunks.append(
            f"<li><strong>النفحات الأساسية:</strong> {html.escape('، '.join(base))}</li>"
        )
    chunks.append("</ul>")
    des = str(frag.get("description_ar") or "").strip()
    if des:
        chunks.append(
            f"<p><strong>ملخص مرجعي:</strong> {html.escape(des[:1200])}</p>"
        )
    url = str(frag.get("fragrantica_url") or "").strip()
    if url:
        chunks.append(f'<p><small>مصدر المكونات: {html.escape(url)}</small></p>')
    return "".join(chunks)


def ai_salla_description_for_missing_row(row: Dict[str, Any]) -> str:
    """
    يستدعي خبير مهووس (MISSING_PAGE_SYSTEM + generate_missing_product_description)
    ويُرجع HTML مناسباً لعمود «الوصف» في استيراد سلة.

    عند تفعيل الجلب من الويب (افتراضي): يستدعي fetch_fragrantica_info لاستخراج مكونات حقيقية
    من مواقع مرجعية، يدمجها في سياق الـ AI، ويُلحق قسماً HTML بالمكونات الصريحة.
    """
    try:
        from engines.ai_engine import fetch_fragrantica_info, generate_missing_product_description

        name = str(row.get("منتج_المنافس", "") or row.get("name", "") or "").strip()
        if not name:
            return default_salla_missing_html_description(row)
        brand = str(row.get("الماركة", "") or "").strip()
        sz = str(row.get("الحجم", "") or "").strip()
        ctype = str(row.get("النوع", "") or "").strip()
        size_conc = " ".join(x for x in (sz, ctype) if x).strip()
        comp = str(row.get("المنافس", "") or "").strip()
        price = safe_float(row.get("سعر_المنافس", 0), 0.0)
        extra = str(row.get("ملاحظة", "") or "").strip()

        fetch_web = (os.environ.get("SALLA_EXPORT_FETCH_WEB_NOTES") or "1").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        frag: Dict[str, Any] = {}
        if fetch_web:
            q = f"{brand} {name}".strip() if brand else name
            try:
                frag = fetch_fragrantica_info(q) or {}
            except Exception:
                frag = {}

        ctx_bits: List[str] = []
        if extra:
            ctx_bits.append(extra)
        web_ctx = _fragrance_web_data_to_ai_context(frag)
        if web_ctx:
            ctx_bits.append(web_ctx)
        extra_merged = "\n\n".join(ctx_bits)

        res = generate_missing_product_description(
            name,
            brand=brand,
            size_concentration=size_conc,
            competitor_price=price,
            competitor_name=comp,
            extra_context=extra_merged,
        )
        txt = ""
        if isinstance(res, dict):
            txt = str(res.get("response") or res.get("text") or "")
        if not txt.strip():
            base_fallback = default_salla_missing_html_description(row)
            appendix = _fragrance_web_data_to_html_appendix(frag)
            return base_fallback + appendix if appendix else base_fallback
        html_out = rough_markdown_to_html_for_salla(txt)
        if not html_out.strip():
            base_fallback = default_salla_missing_html_description(row)
            appendix = _fragrance_web_data_to_html_appendix(frag)
            return base_fallback + appendix if appendix else base_fallback
        appendix = _fragrance_web_data_to_html_appendix(frag)
        if appendix:
            html_out = html_out + appendix
        return html_out
    except Exception:
        return default_salla_missing_html_description(row)


def make_salla_desc_fn(
    use_ai: bool,
    max_ai_rows: int,
) -> Callable[[Dict[str, Any]], str]:
    """
    يُبنى دالة وصف لـ export_missing_products_to_salla_csv*:
    أول max_ai_rows صفوفاً بالذكاء الاصطناعي (إن فعّلت)، ثم القالب الثابت.
    """
    cap = max(0, int(max_ai_rows))
    n = [0]

    def _fn(row: Dict[str, Any]) -> str:
        if use_ai and cap > 0 and n[0] < cap:
            n[0] += 1
            return ai_salla_description_for_missing_row(row)
        return default_salla_missing_html_description(row)

    return _fn


def _missing_row_to_salla_cells(
    row: Dict[str, Any],
    default_category: Optional[str],
    desc_fn: Callable[[Dict[str, Any]], str],
    ctx: Dict[str, Any],
) -> List[str]:
    valid_paths: List[str] = ctx.get("valid_paths") or []
    fallback_cat: str = ctx.get("fallback_cat") or "العطور > عطور رجالية"
    brand_lookup: List[Tuple[str, List[str]]] = ctx.get("brand_lookup") or []

    name = str(row.get("منتج_المنافس", "") or row.get("name", "") or "").strip()
    brand_raw = str(row.get("الماركة", "") or "").strip()
    cat_raw = str(
        row.get("تصنيف_مرجعي", "")
        or row.get("category_auto_path", "")
        or ""
    ).strip()
    if not cat_raw:
        cat_raw = (default_category or "").strip()
    if not cat_raw:
        cat_raw = fallback_cat
    cat = _resolve_category_for_salla(cat_raw, valid_paths, fallback_cat)
    brand = _resolve_brand_for_salla(brand_raw, name, brand_lookup)

    img = str(row.get("صورة_المنافس", "") or row.get("image_url", "") or "").strip()
    alt = _salla_plain_image_alt_text(f"{name} الأصلية" if name else "صورة المنتج")
    desc_html = desc_fn(row)
    out: List[str] = [""] * _SALLA_COL_COUNT
    out[0] = "منتج"
    out[1] = name
    out[2] = cat
    out[3] = img
    out[4] = alt
    out[5] = "منتج جاهز"
    out[6] = _salla_price_numeric(row)
    out[7] = desc_html
    out[8] = "نعم"
    # 9–16 فارغة
    out[17] = "0.2"
    out[18] = "kg"
    out[19] = brand
    # 20–25 فارغة
    out[26] = "نعم"
    # 27–39 فارغة
    return out


def export_missing_products_to_salla_csv(
    missing_products_list: Union[pd.DataFrame, List[Dict[str, Any]]],
    output_filepath: str,
    *,
    default_category: Optional[str] = None,
    generate_description: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> str:
    """
    يكتب ملف CSV بصيغة استيراد سلة لمنتجات مفقودة: UTF-8 مع BOM، صفّا رأس مطابقان، ثم البيانات.

    generate_description: دالة اختيارية تُرجع HTML للوصف؛ الافتراضي قالب HTML ثابت (بدون AI).

    متغيرات بيئة اختيارية:
    - SALLA_IMPORT_DEFAULT_CATEGORY: مسار تصنيف افتراضي يطابق لوحة سلة (مثل: العطور > عطور فرمونية)
    - SALLA_IMPORT_FALLBACK_BRAND: ماركة احتياط عند «غير محدد» (نص كما في brands.csv)
    """
    rows = _normalize_missing_rows_for_salla(missing_products_list)
    desc_fn = generate_description or default_salla_missing_html_description
    ctx = _salla_export_context()
    dc = (default_category or "").strip() or None
    _dir = os.path.dirname(os.path.abspath(output_filepath))
    if _dir:
        os.makedirs(_dir, exist_ok=True)

    # utf-8-sig يضيف BOM في بداية الملف
    with open(output_filepath, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        w.writerow(["بيانات المنتج"] + [""] * (_SALLA_COL_COUNT - 1))
        w.writerow(SALLA_MISSING_HEADER_ROW2)
        for row in rows:
            w.writerow(_missing_row_to_salla_cells(row, dc, desc_fn, ctx))
    return os.path.abspath(output_filepath)


def export_missing_products_to_salla_csv_bytes(
    missing_products_list: Union[pd.DataFrame, List[Dict[str, Any]]],
    *,
    default_category: Optional[str] = None,
    generate_description: Optional[Callable[[Dict[str, Any]], str]] = None,
) -> bytes:
    """
    نفس منطق التصدير إلى ملف، لكن يُرجع بايتات للتنزيل في Streamlit.

    يُطابق التصنيف والماركة مع data/categories.csv و data/brands.csv لتقليل أخطاء الاستيراد في سلة.
    """
    buf = io.StringIO()
    rows = _normalize_missing_rows_for_salla(missing_products_list)
    desc_fn = generate_description or default_salla_missing_html_description
    ctx = _salla_export_context()
    dc = (default_category or "").strip() or None
    w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    w.writerow(["بيانات المنتج"] + [""] * (_SALLA_COL_COUNT - 1))
    w.writerow(SALLA_MISSING_HEADER_ROW2)
    for row in rows:
        w.writerow(_missing_row_to_salla_cells(row, dc, desc_fn, ctx))
    return buf.getvalue().encode("utf-8-sig")
