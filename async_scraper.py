"""
كشط غير متزامن — Phase 2: I/O بـ asyncio فقط (لا ThreadPoolExecutor).
HTTP: جلسة واحدة ``AsyncScraperHTTP`` تُغلق في ``__aexit__`` (لا تسرّب مقابس).
حدود: asyncio.Semaphore لمنع فتح اتصالات لا نهائية على Railway.
"""
from __future__ import annotations

import asyncio
import copy
import csv
from collections import deque
import hashlib
import logging
import os
import queue
import random
import re
import threading
import time as _time
import xml.etree.ElementTree as ET
from html import unescape
from typing import Any

from urllib.parse import urljoin, urlparse

from browser_like_http import AsyncScraperHTTP
from utils.jsonfast import dump as json_dump, load as json_load, loads as json_loads

logger = logging.getLogger(__name__)

# uvloop على Linux/macOS فقط — لا يُفعّل تلقائياً مع Streamlit؛ للمهام asyncio المستقبلية
try:
    import sys as _sys

    if _sys.platform != "win32":
        import uvloop  # noqa: F401 — اختياري، يُستورد للتحقق من التثبيت
except ImportError:
    pass


def _is_ld_product_group_node(node: dict) -> bool:
    raw = node.get("@type")
    parts = raw if isinstance(raw, list) else [raw]
    for p in parts:
        if p is None:
            continue
        s = str(p)
        if re.search(r"(^|/|#)ProductGroup$", s, re.I):
            return True
        if str(p).strip().lower() == "productgroup":
            return True
    return False


def _is_ld_product_node(node: dict) -> bool:
    """Product في JSON-LD بما فيها http://schema.org/Product."""
    raw = node.get("@type")
    parts = raw if isinstance(raw, list) else [raw]
    for p in parts:
        if p is None:
            continue
        s = str(p)
        if re.search(r"(^|/|#)Product$", s, re.I):
            return True
        if str(p).strip().lower() == "product":
            return True
    return False


DATA_DIR = "data"
LIST_PATH = os.path.join(DATA_DIR, "competitors_list.json")
OUT_CSV = os.path.join(DATA_DIR, "competitors_latest.csv")
_COMP_CSV_FIELDS = ["اسم المنتج", "السعر", "رقم المنتج", "رابط_الصورة"]
SCRAPER_BG_STATE_PATH = os.path.join(DATA_DIR, "scraper_bg_state.json")
CHECKPOINT_JSON = os.path.join(DATA_DIR, "scraper_checkpoint.json")
CHECKPOINT_CSV = os.path.join(DATA_DIR, "competitors_checkpoint.csv")

def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# 0 = بلا حد. فصل واضح:
# - SCRAPER_MAX_FETCH_URLS: أقصى عدد روابط يُجلب ويُعالج (يتضمّن تخطّي المُعالَج سابقاً من checkpoint)
# - SCRAPER_MAX_PRODUCT_ROWS: أقصى عدد صفوف منتجات تُضاف إلى rows
# SCRAPER_MAX_URLS (قديم): يُطبَّق على جمع عناوين الـ sitemap فقط إن لم تُضبط SCRAPER_MAX_FETCH_URLS صراحةً
_LEGACY_MAX = _env_int("SCRAPER_MAX_URLS", 0)
_MAX_FETCH_URLS = _env_int("SCRAPER_MAX_FETCH_URLS", 0)
if _MAX_FETCH_URLS <= 0 and _LEGACY_MAX > 0:
    _MAX_FETCH_URLS = _LEGACY_MAX
# افتراضي مرتفع للمتاجر الكبيرة (~8000+ منتج) — 0 يعني بلا حد
_MAX_PRODUCT_ROWS = _env_int("SCRAPER_MAX_PRODUCT_ROWS", 0)
# حدّ جمع عناوين URL من ملفات sitemap (صفحات وليست ملفات الفهرس)
_SITEMAP_LOC_CAP = _env_int("SCRAPER_SITEMAP_LOC_CAP", 200000)
# أقصى عدد ملفات sitemap مميّزة في فهرس (sitemapindex) — كان 400 ويُعطّل المتاجر الكبيرة
_MAX_SITEMAP_INDEX_ENTRIES = _env_int("SCRAPER_SITEMAP_INDEX_CAP", 200000)
# حجم استجابة XML واحدة قبل التخطي (متاجر ضخمة قد تولّد ملفات > 8 ميجا)
_MAX_SITEMAP_BYTES = _env_int("SCRAPER_MAX_SITEMAP_BYTES", 32 * 1024 * 1024)
# متاجر كبيرة (آلاف الملفات داخل sitemap index) تحتاج مهلة أعلى افتراضياً.
_SITEMAP_EXPAND_TIMEOUT_SEC = _env_int("SCRAPER_SITEMAP_EXPAND_TIMEOUT_SEC", 600)
_CHECKPOINT_EVERY = _env_int("SCRAPER_CHECKPOINT_EVERY", 100)
_CLEAR_CK = os.environ.get("SCRAPER_CLEAR_CHECKPOINT", "").strip() in ("1", "true", "yes")
_MAX_CONCURRENT_FETCH = max(1, min(64, _env_int("SCRAPER_MAX_CONCURRENT_FETCH", 28)))
_HEURISTIC_MODE = (os.environ.get("SCRAPER_HEURISTIC_MODE", "loose") or "loose").strip().lower()
_PIPELINE_EVERY = int(os.environ.get("SCRAPER_PIPELINE_EVERY", "100"))
_PIPELINE_AI_PARTIAL = os.environ.get("SCRAPER_PIPELINE_AI_PARTIAL", "").strip().lower() in (
    "1",
    "true",
    "yes",
)

_PIPELINE_STOP = object()
_RECENT_HTTP_STATUS = deque(maxlen=10)
_SALLA_FAST_PATH = (os.environ.get("SCRAPER_SALLA_FAST_PATH") or "1").strip().lower() not in (
    "0",
    "false",
    "no",
)
_SALLA_MAX_PAGES = max(1, min(2000, _env_int("SCRAPER_SALLA_MAX_PAGES", 500)))
_SALLA_MERGE_SITEMAP = os.environ.get("SCRAPER_SALLA_MERGE_SITEMAP", "").strip().lower() in (
    "1",
    "true",
    "yes",
)
_NON_PRODUCT_URL_TOKENS = (
    "/privacy", "/policy", "/policies", "/terms", "/shipping", "/returns",
    "/refund", "/contact", "/about", "/faq", "/blog", "/track", "/cart",
    "/checkout", "/account", "/login", "/register", "/wishlist",
)
_NON_PRODUCT_NAME_TOKENS = (
    "سياسة", "الخصوصية", "الشحن", "التوصيل", "الشروط", "الاحكام",
    "طرق الدفع", "الاستبدال", "الاسترجاع", "اتصل بنا", "من نحن",
    "المدونة", "تتبع الطلب", "الأسئلة الشائعة",
    "privacy", "policy", "terms", "shipping", "returns", "refund",
    "contact us", "about us", "blog", "faq",
)


def _max_sitemap_urls_reached(n: int) -> bool:
    """سقف جمع عناوين URL من الـ sitemap (متوافق مع المتغير القديم SCRAPER_MAX_URLS)."""
    return _LEGACY_MAX > 0 and n >= _LEGACY_MAX


def _max_fetch_urls_reached(n_processed_urls: int) -> bool:
    """سقف عدد الصفحات المستخرجة (بعد checkpoint)."""
    return _MAX_FETCH_URLS > 0 and n_processed_urls >= _MAX_FETCH_URLS


def _max_product_rows_reached(n_rows: int) -> bool:
    """سقف عدد المنتجات المخزّنة في CSV/الدفعة."""
    return _MAX_PRODUCT_ROWS > 0 and n_rows >= _MAX_PRODUCT_ROWS


_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]


def read_scraper_bg_state() -> dict[str, Any]:
    """حالة الكشط/التحليل الخلفي للعرض في الشريط الجانبي (ملف JSON)."""
    default: dict[str, Any] = {
        "active": False,
        "phase": "idle",
        "progress": 0.0,
        "message": "",
        "error": None,
        "job_id": None,
        "rows": 0,
    }
    if not os.path.isfile(SCRAPER_BG_STATE_PATH):
        return dict(default)
    try:
        with open(SCRAPER_BG_STATE_PATH, "r", encoding="utf-8") as f:
            data = json_load(f)
        out = dict(default)
        if isinstance(data, dict):
            out.update(data)
        return out
    except Exception:
        logger.exception(
            "read_scraper_bg_state: failed to read/parse %s", SCRAPER_BG_STATE_PATH
        )
        return dict(default)


def merge_scraper_bg_state(**kwargs) -> None:
    cur = read_scraper_bg_state()
    cur.update(kwargs)
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = SCRAPER_BG_STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json_dump(cur, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SCRAPER_BG_STATE_PATH)


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


async def _async_jitter_sleep() -> None:
    await asyncio.sleep(random.uniform(0.5, 1.5))


async def _async_backoff_sleep(attempt: int) -> None:
    """Exponential backoff + jitter لـ 403/429/5xx."""
    base = min(5.0 * (2.0 ** attempt), 60.0)
    jitter = random.uniform(0.5, 2.5)
    await asyncio.sleep(base + jitter)


async def _async_http_get_armored(
    fetcher: AsyncScraperHTTP,
    url: str,
    timeout: float = 25.0,
    max_attempts: int = 6,
) -> tuple[int, str] | None:
    """
    GET مع backoff أسي + jitter عند 403/429/5xx أو أخطاء الشبكة.
    يعيد (200, text) أو None. يُسجّل آخر رموز HTTP في _RECENT_HTTP_STATUS.
    """
    last_status = 0
    for attempt in range(max_attempts):
        await _async_jitter_sleep()
        try:
            code, text = await fetcher.get_text_once(url, timeout=timeout)
            last_status = int(code or 0)
            _RECENT_HTTP_STATUS.append(last_status)
            if last_status in (429, 403, 503, 502, 500, 504):
                await _async_backoff_sleep(attempt)
                continue
            if last_status == 200 and text:
                return 200, text
            if last_status == 200 and not text:
                await _async_backoff_sleep(attempt)
                continue
            # غير 200: أعد المحاولة للأخطاء العابرة
            if last_status > 0:
                await _async_backoff_sleep(attempt)
                continue
            await _async_backoff_sleep(attempt)
        except Exception:
            logger.warning(
                "async HTTP GET failed url=%s attempt=%s/%s",
                url[:200],
                attempt + 1,
                max_attempts,
                exc_info=True,
            )
            _RECENT_HTTP_STATUS.append(0)
            await _async_backoff_sleep(attempt)
    return None


def _strip_ns(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def _parse_sitemap_xml(content: bytes) -> tuple[list[str], bool]:
    urls: list[str] = []
    is_index = False
    try:
        root = ET.fromstring(content)
    except Exception:
        logger.exception(
            "sitemap XML parse failed (ElementTree) content_len=%s",
            len(content) if content else 0,
        )
        return [], False
    root_tag = _strip_ns(root.tag).lower()
    if root_tag == "sitemapindex":
        is_index = True
    for el in root.iter():
        t = _strip_ns(el.tag).lower()
        if t == "loc" and el.text:
            urls.append(el.text.strip())
    return urls, is_index


async def _expand_sitemap_to_page_urls_async(
    fetcher: AsyncScraperHTTP,
    start_url: str,
    progress_cb=None,
) -> list[str]:
    page_urls: list[str] = []
    seen_sm: set[str] = set()
    q: list[str] = [start_url]
    t0 = _time.time()
    _sm_prog_last = [0.0]  # خفض تكرار progress أثناء فهرسة sitemap (كان يُستدعى كل ملف)
    while q and len(page_urls) < _SITEMAP_LOC_CAP:
        if _SITEMAP_EXPAND_TIMEOUT_SEC > 0 and (_time.time() - t0) > _SITEMAP_EXPAND_TIMEOUT_SEC:
            break
        sm_url = q.pop(0)
        if sm_url in seen_sm:
            continue
        if len(seen_sm) >= _MAX_SITEMAP_INDEX_ENTRIES:
            continue
        seen_sm.add(sm_url)
        if progress_cb:
            now_sm = _time.time()
            if len(seen_sm) <= 1 or (now_sm - _sm_prog_last[0]) >= 0.55:
                _sm_prog_last[0] = now_sm
                try:
                    progress_cb(len(seen_sm), len(q), len(page_urls))
                except Exception:
                    logger.exception(
                        "sitemap progress_cb failed sm_url=%s seen_sm=%s",
                        sm_url,
                        len(seen_sm),
                    )
        await _async_jitter_sleep()
        got = await _async_http_get_armored(fetcher, sm_url, timeout=30.0)
        if got is None:
            continue
        _code, body = got
        if _code != 200 or not body:
            continue
        raw = body.encode("utf-8", errors="replace")
        if len(raw) > _MAX_SITEMAP_BYTES:
            continue
        locs, is_index = _parse_sitemap_xml(raw)
        if is_index:
            for loc in locs:
                if loc.startswith("http") and loc not in seen_sm:
                    q.append(loc)
        else:
            for loc in locs:
                if loc.startswith("http"):
                    page_urls.append(loc.strip())
                    if len(page_urls) >= _SITEMAP_LOC_CAP:
                        break
    return page_urls


def _product_url_heuristic(url: str) -> bool:
    """يقدّر إن كان الرابط صفحة منتج (سلة: .../اسم-المنتج/p123 وليس /p/صفحة-ثابتة)."""
    try:
        path = urlparse(url).path
    except Exception:
        logger.exception("urlparse failed for product URL heuristic url=%r", url)
        path = ""
    pl = path.rstrip("/")
    # سلة / زد الشائع: المسار ينتهي بـ /p وأرقام معرّف المنتج
    if re.search(r"/p\d+$", pl, re.I):
        return True
    u = url.lower()
    if any(tok in u for tok in _NON_PRODUCT_URL_TOKENS):
        return False
    if any(x in u for x in ("/product/", "/products/", "/item/", "/perfume")):
        return True
    if "عطر" in u and "/c" not in u:
        return True
    if re.search(r"/[^/]+-\d{3,}", u):
        return True
    return False


def _looks_non_product_name(name: str) -> bool:
    n = (name or "").strip().lower()
    if not n:
        return True
    return any(tok in n for tok in _NON_PRODUCT_NAME_TOKENS)


def _ld_pick_first_image(val: Any) -> str | None:
    """يستخرج رابط صورة من حقول JSON-LD (نص، ImageObject، قائمة، زد/سلة/Shopify)."""
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        return s if s else None
    if isinstance(val, dict):
        u = val.get("url") or val.get("contentUrl") or val.get("contentURL")
        if isinstance(u, str) and u.strip():
            return u.strip()
        if isinstance(u, list) and u:
            return _ld_pick_first_image(u[0])
        nested = val.get("image")
        if nested is not None and nested is not val:
            got = _ld_pick_first_image(nested)
            if got:
                return got
        uid = val.get("@id")
        if isinstance(uid, str) and uid.startswith("http"):
            return uid.strip()
        return None
    if isinstance(val, list):
        for x in val:
            got = _ld_pick_first_image(x)
            if got:
                return got
    return None


def _iter_ld_product_dicts(node: Any, out: list[dict[str, Any]]) -> None:
    """يجمع كائنات Product من JSON-LD (بما فيها @graph) دون الخلط مع ProductGroup."""
    if isinstance(node, dict):
        if _is_ld_product_group_node(node):
            for sub in node.get("hasVariant") or []:
                _iter_ld_product_dicts(sub, out)
        elif _is_ld_product_node(node):
            out.append(node)
        g = node.get("@graph")
        if isinstance(g, list):
            for x in g:
                _iter_ld_product_dicts(x, out)
    elif isinstance(node, list):
        for x in node:
            _iter_ld_product_dicts(x, out)


def _extract_from_json_ld(html: str, page_url: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    fallback_img: str | None = None
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.I | re.DOTALL,
    ):
        raw = m.group(1).strip()
        try:
            data = json_loads(raw)
        except Exception:
            logger.warning(
                "JSON-LD script parse failed (step=json_ld_loads skip block) page_url=%s raw_len=%s",
                page_url or "",
                len(raw),
                exc_info=True,
            )
            continue
        products: list[dict[str, Any]] = []
        _iter_ld_product_dicts(data, products)
        for it in products:
            name = it.get("name")
            if isinstance(name, str) and name.strip():
                out.setdefault("name", unescape(name.strip()))
            img = _ld_pick_first_image(it.get("image"))
            if img:
                fallback_img = fallback_img or img
                out.setdefault("image", img)
            offers = it.get("offers")
            if isinstance(offers, dict):
                p = offers.get("price") or offers.get("lowPrice")
                if p is not None:
                    try:
                        out.setdefault(
                            "price",
                            float(str(p).replace(",", "").replace("\u00a0", "")),
                        )
                    except Exception:
                        logger.warning(
                            "JSON-LD offer price parse failed (step=json_ld_offer_dict) page_url=%s p=%r",
                            page_url or "",
                            p,
                            exc_info=True,
                        )
            elif isinstance(offers, list) and offers:
                o0 = offers[0]
                if isinstance(o0, dict):
                    p = o0.get("price") or o0.get("lowPrice")
                    if p is not None:
                        try:
                            out.setdefault(
                                "price",
                                float(str(p).replace(",", "").replace("\u00a0", "")),
                            )
                        except Exception:
                            logger.warning(
                                "JSON-LD list offer price parse failed (step=json_ld_offer_list) page_url=%s p=%r",
                                page_url or "",
                                p,
                                exc_info=True,
                            )
    if not out.get("image") and fallback_img:
        out["image"] = fallback_img
    return out


def _extract_meta_fallback(html: str, page_url: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    m = re.search(
        r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']',
        html,
        re.I,
    )
    if m:
        out["name"] = unescape(m.group(1))
    for pat in (
        r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image["\']',
        r'<meta\s+property=["\']og:image:secure_url["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:image:secure_url["\']',
        r'<meta\s+name=["\']twitter:image["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+name=["\']twitter:image:src["\']\s+content=["\']([^"\']+)["\']',
    ):
        m = re.search(pat, html, re.I)
        if m:
            out["image"] = m.group(1).strip()
            break
    if not out.get("image"):
        m = re.search(
            r'<link[^>]+rel=["\']image_src["\'][^>]+href=["\']([^"\']+)["\']',
            html,
            re.I,
        )
        if m:
            out["image"] = m.group(1).strip()
    for pat in (
        r'"price"\s*:\s*([\d.]+)',
        r'itemprop=["\']price["\']\s+content=["\']([\d.]+)',
        r'data-price=["\']([\d.]+)',
    ):
        m = re.search(pat, html, re.I)
        if m:
            try:
                out["price"] = float(m.group(1))
                break
            except Exception:
                logger.warning(
                    "meta fallback regex price parse failed (step=meta_price_regex) page_url=%s m=%r",
                    page_url or "",
                    m.group(0)[:80] if m else None,
                    exc_info=True,
                )
    return out


def _absolutize_image_url(page_url: str, img: str | None) -> str | None:
    """يحوّل روابط الصور النسبية أو // إلى رابط مطلق يعمل في <img src>."""
    if not img:
        return None
    u = str(img).strip()
    if not u or u.lower() in ("none", "null", "undefined"):
        return None
    if u.startswith("//"):
        return "https:" + u
    if u.startswith(("http://", "https://")):
        return u
    try:
        return urljoin(page_url, u)
    except Exception:
        logger.exception(
            "absolutize image urljoin failed page_url=%r img=%r",
            page_url,
            (u[:120] + "…") if len(u) > 120 else u,
        )
        return u


async def _scrape_url_async(
    fetcher: AsyncScraperHTTP,
    page_url: str,
) -> dict[str, Any] | None:
    got = await _async_http_get_armored(fetcher, page_url, timeout=22.0)
    if got is None:
        return None
    _code, html = got
    if _code != 200 or not html:
        return None
    data = _extract_from_json_ld(html, page_url=page_url)
    fb = _extract_meta_fallback(html, page_url=page_url)
    if not data.get("name"):
        data.update({k: v for k, v in fb.items() if v is not None})
    if not data.get("name"):
        return None
    if data.get("price") is None and fb.get("price") is not None:
        data["price"] = fb["price"]
    if not data.get("image") and fb.get("image"):
        data["image"] = fb["image"]
    if data.get("image"):
        abs_u = _absolutize_image_url(page_url, data.get("image"))
        if abs_u:
            data["image"] = abs_u
    data["url"] = page_url
    return data


def _load_sitemap_seeds() -> list[str]:
    os.makedirs(DATA_DIR, exist_ok=True)
    if not os.path.isfile(LIST_PATH):
        return []
    try:
        with open(LIST_PATH, encoding="utf-8") as f:
            raw = json_load(f)
    except Exception:
        logger.exception("load sitemap seeds failed path=%s", LIST_PATH)
        return []
    seeds: list[str] = []
    if isinstance(raw, list):
        for x in raw:
            if isinstance(x, str) and x.startswith("http"):
                seeds.append(x.strip())
            elif isinstance(x, dict):
                d = x.get("domain") or x.get("url")
                if isinstance(d, str) and d.startswith("http"):
                    seeds.append(d.strip())
    return seeds


def _seeds_fingerprint(seeds: list[str]) -> str:
    h = hashlib.sha256("|".join(sorted(seeds)).encode("utf-8")).hexdigest()[:16]
    return h


def _clear_checkpoint_files() -> None:
    for p in (CHECKPOINT_JSON, CHECKPOINT_CSV):
        try:
            if os.path.isfile(p):
                os.remove(p)
        except Exception:
            logger.exception("clear checkpoint file failed path=%s", p)


def get_scraper_sitemap_seeds() -> list[str]:
    """نفس مصدر روابط الكشط (`data/competitors_list.json`) — للاستعادة من نقطة الحفظ."""
    return list(_load_sitemap_seeds())


def load_checkpoint_rows_if_any() -> list[dict[str, Any]]:
    """صفوف محفوظة في نقطة الاستعادة إذا تطابقت بصمة الخرائط مع الجلسة الحالية؛ وإلا قائمة فارغة."""
    seeds = _load_sitemap_seeds()
    if not seeds:
        return []
    seeds_fp = _seeds_fingerprint(seeds)
    _done, rows = _load_checkpoint(seeds_fp)
    if not isinstance(rows, list):
        return []
    return [r for r in rows if isinstance(r, dict)]


def load_checkpoint_rows_ignore_fingerprint() -> list[dict[str, Any]]:
    """كل صفوف المنتج من `scraper_checkpoint.json` — بدون شرط بصمة الخرائط (فرز/مقارنة فقط)."""
    if not os.path.isfile(CHECKPOINT_JSON):
        return []
    try:
        with open(CHECKPOINT_JSON, encoding="utf-8") as f:
            d = json_load(f)
        rows = d.get("rows", [])
        if not isinstance(rows, list):
            return []
        return [r for r in rows if isinstance(r, dict)]
    except Exception:
        logger.exception("load_checkpoint_rows_ignore_fingerprint failed path=%s", CHECKPOINT_JSON)
        return []


def get_checkpoint_recovery_status() -> dict[str, Any]:
    """للواجهة: هل يوجد ملف، عدد الصفوف، وتطابق بصمة `competitors_list.json` مع جلسة النقطة."""
    seeds = _load_sitemap_seeds()
    seeds_fp = _seeds_fingerprint(seeds) if seeds else ""
    raw_rows: list = []
    ck_fp = ""
    file_exists = os.path.isfile(CHECKPOINT_JSON)
    fp_match = False
    if file_exists:
        try:
            with open(CHECKPOINT_JSON, encoding="utf-8") as f:
                d = json_load(f)
            ck_fp = str(d.get("seeds_fp") or "")
            raw = d.get("rows", [])
            raw_rows = raw if isinstance(raw, list) else []
            fp_match = bool(seeds_fp) and (ck_fp == seeds_fp)
        except Exception:
            logger.exception("get_checkpoint_recovery_status: read checkpoint failed path=%s", CHECKPOINT_JSON)
            raw_rows = []
    usable = (
        [r for r in raw_rows if isinstance(r, dict)]
        if fp_match
        else []
    )
    return {
        "file_exists": file_exists,
        "raw_row_count": len(raw_rows),
        "usable_row_count": len(usable),
        "fingerprint_match": fp_match,
        "has_seeds_json": bool(seeds),
        "checkpoint_path": CHECKPOINT_JSON,
    }


def _load_checkpoint(seeds_fp: str) -> tuple[set[str], list[dict[str, Any]]]:
    if not os.path.isfile(CHECKPOINT_JSON):
        return set(), []
    try:
        with open(CHECKPOINT_JSON, encoding="utf-8") as f:
            d = json_load(f)
    except Exception:
        logger.exception("load checkpoint failed path=%s", CHECKPOINT_JSON)
        return set(), []
    if d.get("seeds_fp") != seeds_fp:
        return set(), []
    done = set(d.get("processed_urls", []))
    rows = d.get("rows", [])
    if not isinstance(rows, list):
        rows = []
    return done, rows


def write_competitors_csv(rows: list[dict[str, Any]]) -> None:
    """كتابة جميع صفوف المنافس المكسوبة حتى الآن إلى CSV (للدفعات أثناء الكشط)."""
    if not rows:
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_COMP_CSV_FIELDS)
        w.writeheader()
        w.writerows(rows)


def _save_checkpoint(seeds_fp: str, processed: set[str], rows: list[dict[str, Any]]) -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        with open(CHECKPOINT_JSON, "w", encoding="utf-8") as f:
            json_dump(
                {
                    "seeds_fp": seeds_fp,
                    "processed_urls": list(processed),
                    "rows": rows,
                    "updated_at": _time.strftime("%Y-%m-%dT%H:%M:%S"),
                },
                f,
                ensure_ascii=False,
            )
        if rows:
            with open(CHECKPOINT_CSV, "w", encoding="utf-8-sig", newline="") as f:
                w = csv.DictWriter(
                    f,
                    fieldnames=["اسم المنتج", "السعر", "رقم المنتج", "رابط_الصورة"],
                )
                w.writeheader()
                w.writerows(rows)
    except Exception:
        logger.exception(
            "save checkpoint failed seeds_fp=%s rows=%s",
            seeds_fp,
            len(rows),
        )


def _pipeline_analysis_worker(
    q: queue.Queue,
    out: dict[str, Any],
    our_df: Any,
    comp_key: str,
    use_ai_partial: bool,
    on_analysis_snapshot: Any = None,
    on_pipeline_before_analysis: Any = None,
) -> None:
    """يستهلك لقطات صفوف المنافس ويشغّل run_full_analysis — الوسطى بدون AI افتراضياً."""
    import pandas as pd

    from engines.engine import run_full_analysis

    # أحداث الكشط المُصدّرة من engines.scrape_event تُسجّل عند الاستخراج؛ لا تزال هذه الدالة
    # تعمل على صفوف CSV كما في السابق (استبدال الوسيط لاحقاً دون تغيير التوقيع هنا).

    while True:
        item = q.get()
        if item is _PIPELINE_STOP:
            break
        rows_snap, is_final = item
        if not rows_snap:
            continue
        cdf = pd.DataFrame(rows_snap)
        if cdf.empty:
            continue
        if on_pipeline_before_analysis:
            try:
                on_pipeline_before_analysis(rows_snap, bool(is_final))
            except Exception:
                logger.exception(
                    "on_pipeline_before_analysis failed rows=%s is_final=%s",
                    len(rows_snap),
                    is_final,
                )
        use_ai = True if is_final else use_ai_partial
        try:
            from utils.db_manager import merged_comp_dfs_for_analysis

            _comp_dfs = merged_comp_dfs_for_analysis(comp_key, cdf)
            df = run_full_analysis(
                our_df,
                _comp_dfs,
                progress_callback=None,
                use_ai=use_ai,
            )
            out["analysis_df"] = df
            out["analyzed_rows"] = len(rows_snap)
            out["is_final"] = bool(is_final)
            out["error"] = None
            if on_analysis_snapshot:
                try:
                    on_analysis_snapshot(rows_snap, df, bool(is_final))
                except Exception:
                    logger.exception(
                        "on_analysis_snapshot failed rows=%s analyzed=%s",
                        len(rows_snap),
                        len(df) if df is not None else 0,
                    )
        except Exception as e:
            logger.exception(
                "pipeline run_full_analysis failed comp_key=%s rows_snap=%s",
                comp_key,
                len(rows_snap),
            )
            out["error"] = str(e)
            out["is_final"] = False


def _pipeline_maybe_enqueue(
    pipeline_q: queue.Queue | None,
    rows: list[dict[str, Any]],
    every: int,
) -> None:
    """لقطات وسيطة فقط (كل every صف). الجولة النهائية تُرسل يدوياً."""
    if pipeline_q is None or not rows or every <= 0:
        return
    if len(rows) % every != 0:
        return
    pipeline_q.put((copy.deepcopy(rows), False))


async def _run_scraper_async(
    progress_cb=None,
    pipeline: dict[str, Any] | None = None,
) -> int:
    """منطق الكشط بالكامل — asyncio + Semaphore + جلسة HTTP واحدة مغلقة بـ async with."""
    seeds = _load_sitemap_seeds()
    if not seeds:
        return 0

    scrape_wall_t0 = _time.time()
    seeds_fp = _seeds_fingerprint(seeds)
    if _CLEAR_CK:
        _clear_checkpoint_files()

    processed_urls, rows = _load_checkpoint(seeds_fp)
    seen_names: set[str] = {str(r.get("اسم المنتج", "")).strip() for r in rows if r.get("اسم المنتج")}
    stats = {
        "sitemap_total": 0,
        "heuristic_accepted": 0,
        "heuristic_rejected": 0,
        "extract_ok": 0,
        "extract_fail": 0,
        "dup_name": 0,
        "skip_non_product": 0,
        "skip_zero_price": 0,
        "salla_fast": 0,
    }

    async with AsyncScraperHTTP() as fetcher:
        from engines.salla_storefront import collect_salla_products_fast_path

        seeds_for_sitemap: list[str] = list(seeds)
        salla_pre: list[tuple[str, dict[str, Any]]] = []
        if _SALLA_FAST_PATH:
            seeds_for_sitemap = []
            for seed in seeds:
                batch: list[dict[str, Any]] = []
                try:
                    batch = await collect_salla_products_fast_path(
                        fetcher,
                        seed,
                        max_pages=_SALLA_MAX_PAGES,
                    )
                except Exception:
                    logger.exception("salla fast path failed seed=%s", (seed or "")[:200])
                if batch:
                    stats["salla_fast"] += len(batch)
                    for p in batch:
                        u = str(p.get("url") or seed)
                        salla_pre.append(
                            (
                                u,
                                {
                                    "name": p["name"],
                                    "price": p["price"],
                                    "image": str(p.get("image") or ""),
                                    "url": u,
                                },
                            )
                        )
                    if _SALLA_MERGE_SITEMAP:
                        seeds_for_sitemap.append(seed)
                else:
                    seeds_for_sitemap.append(seed)

        all_page_urls: list[str] = []
        seen_u: set[str] = set()
        for si, seed in enumerate(seeds_for_sitemap):
            if progress_cb:
                try:
                    progress_cb(
                        si + 1,
                        max(1, len(seeds)),
                        f"🔍 فهرسة sitemap للمتجر {si + 1}/{len(seeds)}...",
                    )
                except Exception:
                    logger.exception(
                        "progress_cb failed during seed expand si=%s seed=%s",
                        si,
                        seed[:200] if seed else "",
                    )
            expanded = await _expand_sitemap_to_page_urls_async(
                fetcher,
                seed,
                progress_cb=lambda seen_sm, queued, found: (
                    progress_cb(
                        si + 1,
                        max(1, len(seeds)),
                        f"🔍 sitemap {si + 1}/{len(seeds)} | خرائط:{seen_sm} | queued:{queued} | روابط:{found}",
                    )
                    if progress_cb
                    else None
                ),
            )
            products = [x for x in expanded if _product_url_heuristic(x)]
            stats["sitemap_total"] += len(expanded)
            stats["heuristic_accepted"] += len(products)
            stats["heuristic_rejected"] += max(0, len(expanded) - len(products))
            accept_ratio = (len(products) / len(expanded)) if expanded else 1.0
            prod_set = set(products)
            rest = [x for x in expanded if x not in prod_set]
            include_all_urls = False
            if _HEURISTIC_MODE == "off":
                merged = expanded
                include_all_urls = True
            elif _HEURISTIC_MODE == "loose" and accept_ratio < 0.50:
                merged = expanded
                include_all_urls = True
            else:
                merged = products + rest
            for u in merged:
                if u in seen_u:
                    continue
                seen_u.add(u)
                if include_all_urls or _product_url_heuristic(u):
                    all_page_urls.append(u)
                elif not products and len(all_page_urls) < 80:
                    all_page_urls.append(u)
                if _max_sitemap_urls_reached(len(all_page_urls)):
                    break
            if _max_sitemap_urls_reached(len(all_page_urls)):
                break

        total_urls = len(all_page_urls) + len(salla_pre)
        last_name = "جاري البحث..."

        pipeline_q: queue.Queue | None = None
        pipeline_thread: threading.Thread | None = None
        pipe_every = max(0, _PIPELINE_EVERY)
        use_ai_partial = _PIPELINE_AI_PARTIAL
        if pipeline and pipeline.get("our_df") is not None:
            pipe_every = max(0, int(pipeline.get("every") or pipe_every))
            use_ai_partial = bool(pipeline.get("use_ai_partial", use_ai_partial))
            pipeline_q = queue.Queue()
            out = pipeline.setdefault("out", {})
            comp_key = str(pipeline.get("comp_key") or "Scraped_Competitor")
            our_df_pl = pipeline["our_df"]
            on_snap = pipeline.get("on_analysis_snapshot")
            on_before = pipeline.get("on_pipeline_before_analysis")
            pipeline_thread = threading.Thread(
                target=_pipeline_analysis_worker,
                args=(pipeline_q, out, our_df_pl, comp_key, use_ai_partial, on_snap, on_before),
                daemon=True,
            )
            pipeline_thread.start()

        inc_cb = pipeline.get("on_incremental_flush") if pipeline else None
        inc_ev = 0
        if pipeline and pipeline.get("incremental_every") is not None:
            inc_ev = max(0, int(pipeline["incremental_every"]))
        env_inc = os.environ.get("SCRAPER_INCREMENTAL_EVERY", "").strip()
        if env_inc.isdigit():
            inc_ev = max(1, int(env_inc))
        elif inc_ev == 0 and (inc_cb or (pipeline and pipeline.get("our_df") is not None)):
            inc_ev = pipe_every if pipe_every > 0 else _CHECKPOINT_EVERY

        _cid = (
            str((pipeline or {}).get("comp_key") or "").strip()
            or os.environ.get("SCRAPE_COMPETITOR_ID", "").strip()
            or "Scraped_Competitor"
        )

        def _emit_scrape_event(page_url: str, name: str, price_f: float, img: str) -> None:
            """عقد حدث داخلي — راجع engines/scrape_event.py (لاحقاً: Redis Streams)."""
            try:
                from engines.scrape_event import build_scrape_event, maybe_append_ndjson_event

                ev = build_scrape_event(
                    competitor_id=_cid,
                    source_url=page_url,
                    name=name,
                    price_sar=price_f,
                    image_url=img,
                )
                if pipeline and callable(pipeline.get("on_scrape_event")):
                    try:
                        pipeline["on_scrape_event"](ev)
                    except Exception:
                        logger.exception("on_scrape_event callback failed")
                maybe_append_ndjson_event(ev)
            except Exception:
                logger.exception("emit scrape event failed")

        _scrape_tick = [0, 0.0]
        # تخفيف progress_cb: آلاف الاستدعاءات تُبطئ الواجهة (قراءة/كتابة لقطة JSON في app)
        _prog_emit = [0.0, -1]

        def _emit_url_progress(i_pos: int, total_n: int, name_hint: str) -> None:
            if not progress_cb or total_n <= 0:
                return
            now_p = _time.time()
            step = max(1, total_n // 120) if total_n > 120 else 1
            min_dt = 0.95 if total_n > 500 else 0.5
            last_t, last_ip = _prog_emit[0], _prog_emit[1]
            at_end = (i_pos + 1) >= total_n
            at_start = i_pos <= 0
            due_time = (now_p - last_t) >= min_dt
            due_step = (i_pos - last_ip) >= step
            if not (at_start or at_end or (due_time and due_step)):
                return
            _prog_emit[0] = now_p
            _prog_emit[1] = i_pos
            try:
                progress_cb(
                    i_pos + 1,
                    total_n,
                    name_hint[:80] if name_hint else "جاري البحث...",
                )
            except Exception:
                logger.exception("progress_cb failed i_pos=%s total=%s", i_pos, total_n)

        def _consume_row(u: str, row: dict[str, Any] | None, i_pos: int):
            nonlocal last_name
            if row:
                name = str(row.get("name", "")).strip()
                if name:
                    last_name = name
                stats["extract_ok"] += 1
                if name:
                    if name in seen_names:
                        stats["dup_name"] += 1
                        return None
                    seen_names.add(name)
                    price = row.get("price")
                    if price is None:
                        price = 0.0
                    try:
                        price_f = float(price)
                    except (TypeError, ValueError):
                        price_f = 0.0
                    if _looks_non_product_name(name):
                        stats["skip_non_product"] += 1
                        return None
                    if price_f <= 0:
                        stats["skip_zero_price"] += 1
                        return None
                    img = str(row.get("image", "") or "")
                    rows.append(
                        {
                            "اسم المنتج": name,
                            "السعر": price_f,
                            "رقم المنتج": "",
                            "رابط_الصورة": img,
                        }
                    )
                    _emit_scrape_event(u, name, price_f, img)
                else:
                    stats["extract_fail"] += 1
            else:
                stats["extract_fail"] += 1
            _pipeline_maybe_enqueue(pipeline_q, rows, pipe_every)
            on_tick = pipeline.get("on_scrape_rows_tick") if pipeline else None
            if on_tick and rows:
                now = _time.time()
                n = len(rows)
                if n == 1 or n - _scrape_tick[0] >= 4 or now - _scrape_tick[1] >= 1.4:
                    _scrape_tick[0] = n
                    _scrape_tick[1] = now
                    try:
                        on_tick(n)
                    except Exception:
                        logger.exception("on_scrape_rows_tick failed n_rows=%s", n)
            if inc_ev > 0 and len(rows) % inc_ev == 0 and rows:
                write_competitors_csv(rows)
                if inc_cb:
                    try:
                        inc_cb(copy.deepcopy(rows))
                    except Exception:
                        logger.exception(
                            "on_incremental_flush failed rows=%s", len(rows)
                        )
            _emit_url_progress(
                i_pos,
                total_urls,
                last_name if last_name else "جاري البحث...",
            )
            if len(rows) % _CHECKPOINT_EVERY == 0 and rows:
                _save_checkpoint(seeds_fp, processed_urls, rows)
            if _max_product_rows_reached(len(rows)):
                return "stop_products"
            return None

        salla_stop_early = False
        for si, (u, row) in enumerate(salla_pre):
            if u in processed_urls:
                continue
            processed_urls.add(u)
            if _consume_row(u, row, si) == "stop_products":
                salla_stop_early = True
                break

        pending = [u for u in all_page_urls if u not in processed_urls]
        urls_processed_this_run = [len(salla_pre)]
        sem = asyncio.Semaphore(_MAX_CONCURRENT_FETCH)

        async def _bounded_fetch_one(page_url: str) -> tuple[str, dict[str, Any] | None]:
            async with sem:
                try:
                    row = await _scrape_url_async(fetcher, page_url)
                    return page_url, row
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("async scrape failed url=%s", page_url[:200])
                    return page_url, None

        if pending and not salla_stop_early:
            tasks = [asyncio.create_task(_bounded_fetch_one(u)) for u in pending]
            stop_early = False
            try:
                for fut in asyncio.as_completed(tasks):
                    try:
                        u, row = await fut
                    except asyncio.CancelledError:
                        break
                    except Exception:
                        logger.exception("as_completed task failed")
                        continue
                    if u in processed_urls:
                        continue
                    if _max_fetch_urls_reached(urls_processed_this_run[0]):
                        stop_early = True
                        break
                    processed_urls.add(u)
                    urls_processed_this_run[0] += 1
                    i_pos = max(0, urls_processed_this_run[0] - 1)
                    if _consume_row(u, row, i_pos) == "stop_products":
                        stop_early = True
                        break
            finally:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    if pipeline_q is not None and pipeline_thread is not None:
        _pout = pipeline.setdefault("out", {})
        if rows:
            pipeline_q.put((copy.deepcopy(rows), True))
        pipeline_q.put(_PIPELINE_STOP)
        pipeline_thread.join(timeout=7200)
        if pipeline_thread.is_alive():
            logger.error(
                "pipeline analysis worker did not finish within 7200s — "
                "downstream may use full analysis job instead of fast path"
            )
            _prev = str(_pout.get("error") or "").strip()
            _msg = "انتهت مهلة انتظار محرك الفرز أثناء الكشط — سيتم التحليل الشامل لاحقاً"
            _pout["error"] = f"{_prev} | {_msg}" if _prev else _msg
            _pout["is_final"] = False

    _wall_sec = _time.time() - scrape_wall_t0
    if not rows:
        print(
            f"[SCRAPER] sitemap_total={stats['sitemap_total']} accepted={stats['heuristic_accepted']} "
            f"rejected={stats['heuristic_rejected']} salla_fast={stats.get('salla_fast', 0)} "
            f"extract_ok={stats['extract_ok']} extract_fail={stats['extract_fail']} dup_name={stats['dup_name']} "
            f"skip_non_product={stats['skip_non_product']} skip_zero_price={stats['skip_zero_price']} "
            f"duration_sec={_wall_sec:.1f} product_rows=0",
            flush=True,
        )
        return 0

    write_competitors_csv(rows)
    _nrows = len(rows)
    _ppm = (_nrows / _wall_sec) * 60.0 if _wall_sec > 0.1 else 0.0
    print(
        f"[SCRAPER] sitemap_total={stats['sitemap_total']} accepted={stats['heuristic_accepted']} "
        f"rejected={stats['heuristic_rejected']} salla_fast={stats.get('salla_fast', 0)} "
        f"extract_ok={stats['extract_ok']} extract_fail={stats['extract_fail']} dup_name={stats['dup_name']} "
        f"skip_non_product={stats['skip_non_product']} skip_zero_price={stats['skip_zero_price']} "
        f"duration_sec={_wall_sec:.1f} product_rows={_nrows} products_per_min≈{_ppm:.1f}",
        flush=True,
    )

    # اكتمال ناجح → حذف نقاط الحفظ ليبدأ الجلسة القادمة من جديد
    _clear_checkpoint_files()

    return _nrows


def run_scraper_sync(
    progress_cb=None,
    pipeline: dict[str, Any] | None = None,
) -> int:
    """تشغيل الكشط — يعيد عدد الصفوف المكتوبة. ينفّذ محرك asyncio بالكامل (لا خيط جلب)."""
    try:
        return asyncio.run(_run_scraper_async(progress_cb, pipeline))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            asyncio.set_event_loop(loop)
            return loop.run_until_complete(_run_scraper_async(progress_cb, pipeline))
        finally:
            loop.close()


async def run_scraper_engine(progress_cb=None, pipeline: dict[str, Any] | None = None) -> int:
    """استدعاء async مباشر — نفس منطق run_scraper_sync دون ThreadPoolExecutor."""
    return await _run_scraper_async(progress_cb, pipeline)
