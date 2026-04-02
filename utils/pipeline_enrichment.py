"""
إثراء المسار الآلي: ماركات من brands.csv، تصنيفات من categories.csv،
وصف كامل عبر AI، وحل المشبوهين بدون API — دوال مستقلة عن Streamlit.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Optional

import pandas as pd
from rapidfuzz import fuzz, process as rf_process

logger = logging.getLogger(__name__)

# ── أسماء أعمدة مرنة لـ brands.csv ─────────────────────────────────────
_BRAND_NAME_CANDS = ("الماركة", "brand", "Brand", "name", "العلامة")
_BRAND_DESC_CANDS = (
    "وصف مختصر عن الماركة",
    "وصف_مختصر_عن_الماركة",
    "description",
    "وصف",
    "Brand Description",
)
_BRAND_URL_CANDS = (
    "(SEO Page URL) رابط صفحة العلامة التجارية",
    "رابط صفحة العلامة التجارية",
    "SEO Page URL",
    "brand_page_url",
    "رابط_الصفحة",
)

# ── أعمدة categories.csv ────────────────────────────────────────────────
_CAT_NAME_CANDS = ("التصنيفات", "تصنيف", "category", "Category", "الاسم")
_CAT_SUB_CANDS = ("هل التصنيف فرعي ام لا", "فرعي", "is_sub", "sub")
_CAT_PARENT_CANDS = ("التصنيف الاساسي", "التصنيف الأساسي", "parent", "الأب")


def _pick_col(df: pd.DataFrame, candidates: tuple[str, ...]) -> Optional[str]:
    if df is None or df.empty:
        return None
    cols = list(df.columns)
    for c in candidates:
        if c in cols:
            return c
    cl = [str(x).strip().lower() for x in cols]
    for c in candidates:
        lc = c.lower().strip()
        for i, col in enumerate(cols):
            if lc in str(col).lower() or str(col).lower() in lc:
                return cols[i]
    return None


def _norm_brand_cell(s: str) -> str:
    if not s or not isinstance(s, str):
        return ""
    t = s.strip().lower()
    for ch in "\u0640\u200c":
        t = t.replace(ch, "")
    return t


def enrich_with_brand_data(
    df: pd.DataFrame,
    brands_df: Optional[pd.DataFrame] = None,
    *,
    fuzzy_threshold: int = 85,
) -> pd.DataFrame:
    """
    يُطابق عمود الماركة في df مع brands_df (rapidfuzz ≥ threshold)
    ويضيف: brand_page_url, brand_description
    """
    try:
        if df is None or df.empty:
            return df
        out = df.copy()
        if brands_df is None or brands_df.empty:
            out["brand_page_url"] = ""
            out["brand_description"] = ""
            return out

        bcol = _pick_col(brands_df, _BRAND_NAME_CANDS)
        dcol = _pick_col(brands_df, _BRAND_DESC_CANDS)
        ucol = _pick_col(brands_df, _BRAND_URL_CANDS)
        if not bcol:
            out["brand_page_url"] = ""
            out["brand_description"] = ""
            return out

        brand_labels: list[str] = []
        iloc_list: list[int] = []
        for bi in range(len(brands_df)):
            r = brands_df.iloc[bi]
            raw = str(r.get(bcol, "") or "").strip()
            if raw and raw.lower() not in ("nan", "none"):
                brand_labels.append(raw)
                iloc_list.append(bi)

        if not brand_labels:
            out["brand_page_url"] = ""
            out["brand_description"] = ""
            return out

        urls: list[str] = []
        descs: list[str] = []
        for _, row in out.iterrows():
            b = str(row.get("الماركة", "") or row.get("brand", "") or "").strip()
            if not b:
                urls.append("")
                descs.append("")
                continue
            match = rf_process.extractOne(b, brand_labels, scorer=fuzz.token_set_ratio)
            if match and match[1] >= fuzzy_threshold:
                lbl = match[0]
                pos = brand_labels.index(lbl)
                br = brands_df.iloc[iloc_list[pos]]
                u = str(br.get(ucol, "") or "").strip() if ucol else ""
                d = str(br.get(dcol, "") or "").strip() if dcol else ""
                urls.append(u)
                descs.append(d)
            else:
                urls.append("")
                descs.append("")

        out["brand_page_url"] = urls
        out["brand_description"] = descs
        return out
    except Exception:
        logger.exception("enrich_with_brand_data failed")
        try:
            out = df.copy()
            out["brand_page_url"] = ""
            out["brand_description"] = ""
            return out
        except Exception:
            return df


def _build_category_rows(categories_df: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """صفوف التصنيف: الكل، والفرعية فقط (مفضّلة)."""
    rows: list[dict] = []
    ncol = _pick_col(categories_df, _CAT_NAME_CANDS)
    if not ncol:
        return [], []
    subcol = _pick_col(categories_df, _CAT_SUB_CANDS)
    parcol = _pick_col(categories_df, _CAT_PARENT_CANDS)

    for _, r in categories_df.iterrows():
        name = str(r.get(ncol, "") or "").strip()
        if not name or name.lower() == "nan":
            continue
        is_sub = False
        if subcol:
            sv = str(r.get(subcol, "") or "").strip().lower()
            is_sub = sv in ("نعم", "yes", "y", "true", "1", "فرعي")
        parent = str(r.get(parcol, "") or "").strip() if parcol else ""
        rows.append(
            {
                "name": name,
                "is_sub": is_sub,
                "parent": parent,
                "path": f"{parent} > {name}" if parent else name,
            }
        )
    sub_only = [x for x in rows if x["is_sub"]]
    return rows, sub_only


def auto_assign_category(
    product_name: str,
    brand: str,
    categories_df: Optional[pd.DataFrame],
    *,
    fuzzy_threshold: int = 80,
) -> dict[str, Any]:
    """
    مرحلة 1: token_set_ratio على أسماء التصنيفات الفرعية (≥80%).
    يُفضّل دائماً تصنيفاً فرعياً إن وُجد في الجدول.
    """
    out: dict[str, Any] = {
        "category_path": "",
        "category_name": "",
        "method": "",
        "score": 0.0,
    }
    try:
        if not product_name or categories_df is None or categories_df.empty:
            return out
        all_rows, sub_rows = _build_category_rows(categories_df)
        pool = sub_rows if sub_rows else all_rows
        if not pool:
            return out
        names = [p["name"] for p in pool]
        q = f"{brand} {product_name}".strip() or product_name
        best = None
        best_sc = 0.0
        for p in pool:
            s = float(fuzz.token_set_ratio(q, p["name"]))
            if s > best_sc:
                best_sc = s
                best = p
        if best and best_sc >= fuzzy_threshold:
            out["category_path"] = best["path"]
            out["category_name"] = best["name"]
            out["method"] = "fuzzy_local"
            out["score"] = best_sc
        return out
    except Exception:
        logger.exception("auto_assign_category failed")
        return out


def _parse_category_assign_json(txt: str) -> Optional[dict]:
    try:
        from engines.engine import _clean_ai_json
    except ImportError:
        from engine import _clean_ai_json  # type: ignore
    try:
        clean = _clean_ai_json(txt)
        return json.loads(clean)
    except Exception:
        return None


def auto_assign_category_batch(
    df: pd.DataFrame,
    categories_df: Optional[pd.DataFrame],
    *,
    use_api: bool = True,
    fuzzy_threshold: int = 80,
) -> pd.DataFrame:
    """
    يطبّق auto_assign_category على كل صف؛ عند use_api يستدعي دفعة واحدة للمنتجات
    التي لم تُعيَّن محلياً.
    """
    try:
        if df is None or df.empty:
            return df
        out = df.copy()
        all_rows, sub_rows = (
            _build_category_rows(categories_df)
            if categories_df is not None and not categories_df.empty
            else ([], [])
        )
        pool = sub_rows if sub_rows else all_rows
        subcat_lines = "\n".join(f"- {p['path']}" for p in pool[:400])

        row_order = list(out.iterrows())
        n = len(row_order)
        paths = [""] * n
        names = [""] * n
        methods = [""] * n
        scores = [0.0] * n
        need_api: list[tuple[int, str, str]] = []

        for pos, (_idx, row) in enumerate(row_order):
            pn = str(
                row.get("منتج_المنافس", "")
                or row.get("المنتج", "")
                or row.get("اسم المنتج", "")
                or ""
            ).strip()
            br = str(row.get("الماركة", "") or row.get("brand", "") or "").strip()
            loc = auto_assign_category(pn, br, categories_df, fuzzy_threshold=fuzzy_threshold)
            if loc.get("category_path"):
                paths[pos] = loc["category_path"]
                names[pos] = loc["category_name"]
                methods[pos] = loc["method"]
                scores[pos] = float(loc.get("score") or 0)
            elif pn and pool and use_api:
                need_api.append((pos, pn, br))

        if need_api and use_api and pool:
            try:
                from engines.ai_engine import call_ai

                batch_lines = "\n".join(
                    f"[{j}] {x[1]} | ماركة: {x[2]}" for j, x in enumerate(need_api)
                )
                prompt = f"""أنت مصنّف منتجات عطور لمتجر مهووس.
قائمة التصنيفات الفرعية المعتمدة (اختر واحداً فقط لكل رقم من القائمة الثانية):
{subcat_lines}

المنتجات المراد تصنيفها:
{batch_lines}

أجب JSON فقط بهذا الشكل:
{{"assignments":[{{"index":0,"category_path":"مسار بالضبط من القائمة أعلاه"}}]}}
حيث index يطابق رقم [j] في القائمة."""
                r = call_ai(prompt, "general")
                txt = (r or {}).get("response") or ""
                data = _parse_category_assign_json(txt)
                assigns = (data or {}).get("assignments") or []
                by_idx = {int(a.get("index", -1)): a.get("category_path", "") for a in assigns if isinstance(a, dict)}
                for j, (pos, _pn, _br) in enumerate(need_api):
                    cp = str(by_idx.get(j, "") or "").strip()
                    if cp:
                        paths[pos] = cp
                        names[pos] = cp.split(">")[-1].strip() if ">" in cp else cp
                        methods[pos] = "api_batch"
                        scores[pos] = 75.0
            except Exception:
                logger.exception("auto_assign_category_batch API phase failed")

        for col, series in [
            ("category_auto_path", paths),
            ("category_auto_name", names),
            ("category_auto_method", methods),
            ("category_auto_score", scores),
        ]:
            out[col] = series

        # توحيد مع تصنيف_مرجعي إن وُجد
        if "تصنيف_مرجعي" not in out.columns:
            out["تصنيف_مرجعي"] = out["category_auto_path"]
        else:
            out["تصنيف_مرجعي"] = out["تصنيف_مرجعي"].where(
                out["تصنيف_مرجعي"].astype(str).str.strip() != "",
                out["category_auto_path"],
            )
        return out
    except Exception:
        logger.exception("auto_assign_category_batch failed")
        return df


_FULL_EXPERT_SYSTEM = """أنت خبير عطور محترف مع 15+ سنة خبرة في صناعة العطور الفاخرة، متخصص في SEO و Generative Engine Optimization (GEO). تعمل حصرياً لمتجر "مهووس" (Mahwous) - الوجهة الأولى للعطور الفاخرة في السعودية.

مهمتك: كتابة وصف منتج عطور شامل واحترافي (1200-1500 كلمة) يتضمن:

1. فقرة افتتاحية عاطفية (الكلمة الرئيسية في أول 50 كلمة)
2. تفاصيل المنتج (نقاط نقطية: الماركة، الجنس، العائلة العطرية، الحجم، التركيز، سنة الإصدار)
3. رحلة العطر - الهرم العطري (Top Notes → Heart Notes → Base Notes) بلغة حسية عاطفية
4. لماذا تختار هذا العطر؟ (4-6 نقاط تركز على الفوائد)
5. متى وأين ترتدي هذا العطر؟ (الفصول، الأوقات، المناسبات)
6. لمسة خبير من مهووس (تحليل حسي + الأداء + المقارنات + التوصية)
7. الأسئلة الشائعة FAQ (6-8 أسئلة بإجابات 50-80 كلمة)
8. روابط داخلية (3-5 روابط لتصنيفات مهووس)
9. فقرة ختامية بالشعار: "عالمك العطري يبدأ من مهووس"

القواعد الصارمة:
- الكلمة الرئيسية: "عطر [الماركة] [اسم العطر] [التركيز] [الحجم] [للجنس]" تتكرر 5-7 مرات
- لا إيموجي، لا شرح، لا تعليمات — فقط الوصف الجاهز
- أسلوب: 40% راقٍ + 25% ودود + 20% عاطفي + 15% تسويقي
- ابحث في Fragrantica Arabia عن المكونات الحقيقية للعطر
- أكد دائماً: "أصلي 100%"، "ضمان الأصالة"
"""


def generate_full_product_description(
    product_name: str,
    brand: str,
    category: str,
    price: float,
) -> dict[str, Any]:
    """
    يُرجع JSON: description, page_title, meta_description, url_slug, tags
    عند فشل API يُعاد هيكل بقيم فارغة/احتياطية.
    """
    default: dict[str, Any] = {
        "description": "",
        "page_title": (product_name or "")[:60],
        "meta_description": "",
        "url_slug": re.sub(r"[^\w\-]+", "-", (product_name or "product").lower())[:80].strip("-"),
        "tags": [brand] if brand else [],
    }
    try:
        from engines.ai_engine import _call_gemini, _call_openrouter
    except ImportError:
        try:
            from ai_engine import _call_gemini, _call_openrouter  # type: ignore
        except ImportError:
            return default

    user = f"""بيانات المنتج:
- الاسم: {product_name}
- الماركة: {brand}
- التصنيف: {category}
- السعر المرجعي: {price}

أجب JSON فقط (بدون markdown) بهذا الشكل:
{{
  "description": "النص الكامل 1200-1500 كلمة",
  "page_title": "حتى 60 حرف",
  "meta_description": "حتى 160 حرف",
  "url_slug": "english-slug-lowercase",
  "tags": ["ماركة","جنس","فصل"]
}}
"""
    txt = _call_gemini(user, system=_FULL_EXPERT_SYSTEM, grounding=True, max_tokens=8192)
    if not txt:
        txt = _call_openrouter(user, system=_FULL_EXPERT_SYSTEM)
    if not txt:
        return default
    try:
        from engines.engine import _clean_ai_json
    except ImportError:
        from engine import _clean_ai_json  # type: ignore
    try:
        clean = _clean_ai_json(txt)
        data = json.loads(clean)
        if isinstance(data, dict):
            for k in default:
                if k in data and data[k]:
                    default[k] = data[k]
            return default
    except Exception:
        logger.exception("generate_full_product_description parse failed")
    default["description"] = txt[:50000] if txt else ""
    return default


_CONC_RE = re.compile(
    r"\b(edp|edt|edc|extrait|parfum|بارفيوم|تواليت|كولون|او\s*دي\s*بارفان|أو\s*دي\s*تواليت)\b",
    re.I,
)


def resolve_suspicious_no_api(
    suspect_row: dict[str, Any],
    store_names: Optional[list[str]] = None,
    brands_list: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    قواعد بدون API: مكرر / جديد / مراجعة — مستقل عن Streamlit.
    """
    try:
        try:
            from engines.engine import extract_brand, extract_size, normalize
        except ImportError:
            extract_brand = lambda s: ""  # type: ignore
            extract_size = lambda s: 0.0  # type: ignore
            normalize = lambda s: str(s or "").lower()  # type: ignore

        comp_name = str(suspect_row.get("الاسم الجديد", "") or "").strip()
        closest = str(suspect_row.get("أقرب تطابق في المتجر", "") or "").strip()
        score = suspect_row.get("نسبة التطابق", 0)
        try:
            score = float(score)
        except Exception:
            score = 0.0

        if not comp_name:
            return {"classification": "review", "reason": "اسم جديد فارغ"}

        b_new = extract_brand(comp_name) or (comp_name.split()[0] if comp_name else "")
        b_old = extract_brand(closest) if closest else ""
        blist = brands_list or []

        def _in_brands(b: str) -> bool:
            if not b or not blist:
                return False
            nb = normalize(b)
            for x in blist:
                if fuzz.token_set_ratio(nb, normalize(str(x))) >= 85:
                    return True
            return False

        if not _in_brands(b_new):
            return {"classification": "review", "reason": "ماركة غير مدرجة في القائمة المعتمدة"}

        sz_n = extract_size(comp_name)
        sz_o = extract_size(closest) if closest else 0.0
        if sz_n > 0 and sz_o > 0 and abs(sz_n - sz_o) > 5:
            return {"classification": "new", "reason": "اختلاف حجم واضح بين المنتج الجديد والمخزون"}

        def _conc(s: str) -> str:
            m = _CONC_RE.search(s or "")
            return (m.group(1) or "").lower() if m else ""

        c1, c2 = _conc(comp_name), _conc(closest)
        if c1 and c2 and c1 != c2 and {c1, c2} != {"edp", "parfum"}:
            return {"classification": "new", "reason": "اختلاف تركيز (مثلاً EDP vs EDT)"}

        if score >= 85 and b_old and normalize(b_new) == normalize(b_old):
            return {"classification": "duplicate", "reason": "تطابق ماركة عالٍ ونسبة تطابق ≥85%"}

        if score >= 85:
            return {"classification": "duplicate", "reason": "نسبة تطابق ≥85%"}

        return {"classification": "new", "reason": "تطابق منخفض — يُعامل كمنتج جديد"}
    except Exception:
        logger.exception("resolve_suspicious_no_api failed")
        return {"classification": "review", "reason": "خطأ داخلي"}


def apply_missing_pipeline_enrichment(
    missing_df: pd.DataFrame,
    brands_df: Optional[pd.DataFrame] = None,
    categories_df: Optional[pd.DataFrame] = None,
    *,
    use_category_api: bool = True,
) -> pd.DataFrame:
    """
    تسلسل موحّد: إثراء ماركة + تصنيف — يُستدعى من app بعد جدول المفقودات.
    """
    try:
        m = enrich_with_brand_data(missing_df, brands_df)
        m = auto_assign_category_batch(m, categories_df, use_api=use_category_api)
        return m
    except Exception:
        logger.exception("apply_missing_pipeline_enrichment failed")
        return missing_df


def load_brands_categories_from_disk() -> tuple[pd.DataFrame, pd.DataFrame]:
    """تحميل آمن من data/ — يُرجع DataFrame فارغ عند الفشل."""
    try:
        from engines.reference_data import BRANDS_CSV, CATEGORIES_CSV
    except ImportError:
        from reference_data import BRANDS_CSV, CATEGORIES_CSV  # type: ignore
    bdf = pd.DataFrame()
    cdf = pd.DataFrame()
    try:
        bdf = pd.read_csv(BRANDS_CSV, encoding="utf-8-sig", on_bad_lines="skip")
    except Exception:
        try:
            bdf = pd.read_csv(BRANDS_CSV, encoding="utf-8", on_bad_lines="skip")
        except Exception:
            pass
    try:
        cdf = pd.read_csv(CATEGORIES_CSV, encoding="utf-8-sig", on_bad_lines="skip")
    except Exception:
        try:
            cdf = pd.read_csv(CATEGORIES_CSV, encoding="utf-8", on_bad_lines="skip")
        except Exception:
            pass
    return bdf, cdf
