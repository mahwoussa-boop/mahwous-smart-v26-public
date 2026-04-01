"""
مرجعية مهووس: ماركات وتصنيفات معتمدة (من CSV أو من mahwous_catalog.csv).
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any

import pandas as pd
from rapidfuzz import fuzz, process as rf_process

logger = logging.getLogger(__name__)

_BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(_BASE, "data")
BRANDS_CSV = os.path.join(DATA_DIR, "brands.csv")
CATEGORIES_CSV = os.path.join(DATA_DIR, "categories.csv")
OUR_CATALOG_CSV = os.path.join(DATA_DIR, "mahwous_catalog.csv")
# اسم بديل كما في الدليل الهندسي (نفس المحتوى)
OUR_CATALOG_ALIAS = os.path.join(DATA_DIR, "our_catalog.csv")


def _norm_brand_token(s: str) -> str:
    if not s or not isinstance(s, str):
        return ""
    t = s.strip().lower()
    for ch in "\u0640\u200c":
        t = t.replace(ch, "")
    return t


def _split_brand_aliases(cell: str) -> list[str]:
    """'لطافة | Latafa' → ['لطافة', 'latafa']."""
    if not cell or not str(cell).strip():
        return []
    parts = [p.strip() for p in str(cell).split("|")]
    return [p for p in parts if p]


@lru_cache(maxsize=1)
def load_reference_brands_list() -> list[str]:
    """قائمة نصوص ماركات (للمطابقة الضبابية)."""
    seen: dict[str, None] = {}
    catalog_paths = []
    if os.path.isfile(OUR_CATALOG_CSV):
        catalog_paths.append((OUR_CATALOG_CSV, ("الماركة",)))
    if os.path.isfile(OUR_CATALOG_ALIAS):
        catalog_paths.append((OUR_CATALOG_ALIAS, ("الماركة",)))

    for path, col_candidates in (
        (BRANDS_CSV, ("الماركة", "brand", "Brand", "name")),
        *catalog_paths,
    ):
        if not os.path.isfile(path):
            continue
        try:
            df = pd.read_csv(path, encoding="utf-8", on_bad_lines="skip")
        except Exception:
            try:
                df = pd.read_csv(path, encoding="utf-8-sig", on_bad_lines="skip")
            except Exception as e:
                logger.warning("reference_data: read failed %s: %s", path, e)
                continue
        col = None
        for c in col_candidates:
            if c in df.columns:
                col = c
                break
        if not col:
            continue
        for v in df[col].dropna().astype(str):
            for alias in _split_brand_aliases(v):
                n = _norm_brand_token(alias)
                if len(n) >= 2:
                    seen[n] = None
    return list(seen.keys())


@lru_cache(maxsize=1)
def load_reference_category_paths() -> list[str]:
    paths: list[str] = []
    cat_paths = [(CATEGORIES_CSV, None)]
    if os.path.isfile(OUR_CATALOG_CSV):
        cat_paths.append((OUR_CATALOG_CSV, "تصنيف المنتج"))
    if os.path.isfile(OUR_CATALOG_ALIAS):
        cat_paths.append((OUR_CATALOG_ALIAS, "تصنيف المنتج"))

    for path, col in cat_paths:
        if not os.path.isfile(path):
            continue
        try:
            df = pd.read_csv(path, encoding="utf-8", on_bad_lines="skip")
        except Exception:
            try:
                df = pd.read_csv(path, encoding="utf-8-sig", on_bad_lines="skip")
            except Exception as e:
                logger.warning("reference_data: categories read failed %s: %s", path, e)
                continue
        if path == CATEGORIES_CSV:
            col = None
            for c in ("تصنيف", "تصنيف المنتج", "category", "Category"):
                if c in df.columns:
                    col = c
                    break
            if not col:
                continue
        elif col not in df.columns:
            continue
        for v in df[col].dropna().astype(str):
            s = v.strip()
            if s and s not in paths:
                paths.append(s)
    return paths


def classify_brand_vs_reference(brand_raw: str, threshold: int = 85) -> tuple[str, str]:
    """
    يعيد (حالة_الماركة, ماركة_مرجعية_مطابقة).
    معتمدة: تطابق ضبابي ≥ threshold مع قائمة المرجعية.
    وإلا: ماركة جديدة محتملة (لا يُرفض المنتج).
    """
    if not brand_raw or not str(brand_raw).strip():
        return "غير محدد", ""
    brands = load_reference_brands_list()
    if not brands:
        return "ماركة جديدة محتملة", ""
    b = str(brand_raw).strip()
    nb = _norm_brand_token(b)
    if nb in brands:
        return "معتمدة", b
    match = rf_process.extractOne(nb, brands, scorer=fuzz.token_set_ratio)
    if match and match[1] >= threshold:
        return "معتمدة", match[0]
    return "ماركة جديدة محتملة", ""


def guess_reference_category(row: dict[str, Any]) -> str:
    """تخمين تصنيف سلة من الجدول المرجعي حسب الجنس/النوع."""
    paths = load_reference_category_paths()
    if not paths:
        return "العطور > عطور للجنسين"
    gender = str(row.get("الجنس", "") or "").lower()
    name = str(row.get("منتج_المنافس", "") or row.get("المنتج", "") or "")

    def pick(keywords: tuple[str, ...]) -> str:
        for p in paths:
            pl = p.lower()
            if any(k in pl for k in keywords):
                return p
        return ""

    if any(x in gender for x in ("نساء", "نسائي", "female")) or "نسائ" in name:
        g = pick(("نسائ", "نسائية"))
        if g:
            return g
    if any(x in gender for x in ("رجال", "رجالي", "male")) or "رجال" in name:
        g = pick(("رجال", "رجالية"))
        if g:
            return g
    g = pick(("جنسين", "unisex"))
    if g:
        return g
    return paths[0]


def enrich_missing_reference_columns(df: pd.DataFrame) -> pd.DataFrame:
    """يضيف: تصنيف_مرجعي، حالة_الماركة، ماركة_مرجعية_مطابقة."""
    if df is None or df.empty:
        return df
    out = df.copy()
    cats: list[str] = []
    states: list[str] = []
    ref_hits: list[str] = []
    for _, row in out.iterrows():
        r = row.to_dict()
        brand = str(r.get("الماركة", "") or "").strip()
        st, hit = classify_brand_vs_reference(brand)
        states.append(st)
        ref_hits.append(hit)
        cats.append(guess_reference_category(r))
    out["حالة_الماركة"] = states
    out["ماركة_مرجعية_مطابقة"] = ref_hits
    out["تصنيف_مرجعي"] = cats
    return out


def clear_reference_cache() -> None:
    load_reference_brands_list.cache_clear()
    load_reference_category_paths.cache_clear()
