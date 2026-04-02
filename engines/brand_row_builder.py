"""
توليد صف ماركة جديد لملف data/brands.csv بنفس تنسيق مهووس.
- أعمدة تُقرأ من الملف الحالي إن وُجد.
- مقترحات شعار: Clearbit + Google favicons عند إدخال نطاق الموقع (بدون API مفتاح).
- تعبئة SEO اختيارية عبر call_ai.
"""
from __future__ import annotations

import csv
import io
import json
import os
import re
from typing import Any
from urllib.parse import urlparse

# نفس ترتيب ملف مهووس الافتراضي إن تعذر القراءة
DEFAULT_BRAND_COLUMNS: tuple[str, ...] = (
    "اسم الماركة",
    "وصف مختصر عن الماركة",
    "صورة شعار الماركة",
    "(إختياري) صورة البانر",
    "(Page Title) عنوان صفحة العلامة التجارية",
    "(SEO Page URL) رابط صفحة العلامة التجارية",
    "(Page Description) وصف صفحة العلامة التجارية",
)


def load_brands_csv_columns(brands_csv_path: str) -> list[str]:
    """يقرأ صف العناوين من brands.csv."""
    if not brands_csv_path or not os.path.isfile(brands_csv_path):
        return list(DEFAULT_BRAND_COLUMNS)
    for enc in ("utf-8-sig", "utf-8"):
        try:
            with open(brands_csv_path, encoding=enc) as f:
                r = csv.reader(f)
                row = next(r, None)
                if row and len(row) >= 5:
                    return [str(c).strip() for c in row]
        except OSError:
            continue
        except StopIteration:
            break
    return list(DEFAULT_BRAND_COLUMNS)


def normalize_domain(url_or_domain: str) -> str:
    """يستخرج نطاقاً مثل chanel.com من رابط كامل أو نص."""
    s = (url_or_domain or "").strip()
    if not s:
        return ""
    if "://" not in s and "/" not in s.replace(".", ""):
        # قد يكون مجرد اسم نطاق
        if "." in s:
            sl = s.lower()
            return sl[4:] if sl.startswith("www.") else sl
        return ""
    if "://" not in s:
        s = "https://" + s
    try:
        p = urlparse(s)
        host = (p.netloc or p.path.split("/")[0]).strip().lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def suggest_logo_urls(domain: str) -> list[str]:
    """روابط شعار شائعة (لا ضمان توفر الصورة — جرّب في المتصفح)."""
    raw = (domain or "").strip().lower()
    if raw.startswith("www."):
        raw = raw[4:]
    d = normalize_domain(domain) or raw
    if not d or "." not in d:
        return []
    return [
        f"https://logo.clearbit.com/{d}",
        f"https://www.google.com/s2/favicons?domain={d}&sz=256",
    ]


def slugify_seo_latin(name_en: str) -> str:
    """مقطع لاتيني بسيط لعمود SEO URL."""
    s = (name_en or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s, flags=re.IGNORECASE)
    s = re.sub(r"-+", "-", s).strip("-")
    return (s[:80] if s else "brand").strip("-")


def build_empty_brand_row(columns: list[str]) -> dict[str, str]:
    return {c: "" for c in columns}


def build_brand_row(
    *,
    name_bilingual: str,
    short_description: str = "",
    logo_url: str = "",
    banner_url: str = "",
    page_title: str = "",
    seo_slug_latin: str = "",
    page_description: str = "",
    columns: list[str] | None = None,
) -> dict[str, str]:
    """يملأ القاموس بمفاتيح مطابقة لعناوين الأعمدة."""
    cols = columns or list(DEFAULT_BRAND_COLUMNS)
    row = build_empty_brand_row(cols)
    mapping = {
        "اسم الماركة": name_bilingual,
        "وصف مختصر عن الماركة": short_description,
        "صورة شعار الماركة": logo_url,
        "(إختياري) صورة البانر": banner_url,
        "(Page Title) عنوان صفحة العلامة التجارية": page_title,
        "(SEO Page URL) رابط صفحة العلامة التجارية": seo_slug_latin,
        "(Page Description) وصف صفحة العلامة التجارية": page_description,
    }
    for k, v in mapping.items():
        if k in row:
            row[k] = v or ""
    # إن كانت أسماء الأعمدة مختلفة قليلاً — طابق أول عمود يشبه الاسم
    if cols and cols[0] not in row and len(cols) >= 1:
        row[cols[0]] = name_bilingual
    return row


def brand_row_to_csv_bytes(columns: list[str], row: dict[str, Any]) -> bytes:
    """سطر واحد + رأس UTF-8 BOM لدمج يدوي في Excel/سلة."""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=columns, extrasaction="ignore", quoting=csv.QUOTE_MINIMAL)
    w.writeheader()
    w.writerow({c: (row.get(c, "") or "") for c in columns})
    return buf.getvalue().encode("utf-8-sig")


def ai_fill_brand_seo_fields(brand_display_name: str) -> dict[str, str]:
    """
    يطلب من AI وصفاً قصيراً وعنوان صفحة ووصف ميتا بأسلوب مهووس.
    يعيد مفاتيح بالعربية كما في CSV.
    """
    out: dict[str, str] = {}
    try:
        from engines.ai_engine import call_ai
        try:
            from engines.engine import _clean_ai_json
        except ImportError:
            from engine import _clean_ai_json  # type: ignore
    except Exception:
        return out

    prompt = f"""ماركة عطور جديدة لمتجر مهووس (السعودية): "{brand_display_name}"

أجب JSON فقط (بدون markdown) بهذا الشكل:
{{
  "وصف مختصر عن الماركة": "نص عربي 180-280 حرفاً: من نحن، التخصص، سنة إن عرفت",
  "(Page Title) عنوان صفحة العلامة التجارية": "حتى 65 حرفاً عربي | مهووس",
  "(Page Description) وصف صفحة العلامة التجارية": "حتى 160 حرفاً عربي تسويقي"
}}
لا تخترع حقائق تاريخية محددة إن لم تكن متأكداً؛ اكتب بلغة عامة مهنية."""
    r = call_ai(prompt, "general")
    txt = (r or {}).get("response") or ""
    if not txt:
        return out
    try:
        clean = _clean_ai_json(txt)
        data = json.loads(clean)
        if isinstance(data, dict):
            for k in (
                "وصف مختصر عن الماركة",
                "(Page Title) عنوان صفحة العلامة التجارية",
                "(Page Description) وصف صفحة العلامة التجارية",
            ):
                if data.get(k):
                    out[k] = str(data[k]).strip()
    except Exception:
        pass
    return out
