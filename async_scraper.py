"""
كشط غير متزامن (واجهة async + تنفيذ I/O عبر مؤشر ترابط).
مدرّع: تدوير User-Agent، Jitter، Exponential Backoff، نقاط حفظ (Checkpoint).
"""
from __future__ import annotations

import asyncio
import copy
import csv
from collections import deque
import hashlib
import json
import logging
import os
import queue
import random
import re
import threading
import time as _time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

import requests

from browser_like_http import create_scraper_session

logger = logging.getLogger(__name__)


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
_FETCH_WORKERS = max(1, min(16, int(os.environ.get("SCRAPER_FETCH_WORKERS", "3"))))
_MAX_ADAPTIVE_WORKERS = max(1, min(5, int(os.environ.get("SCRAPER_MAX_ADAPTIVE_WORKERS", "5"))))
_HEURISTIC_MODE = (os.environ.get("SCRAPER_HEURISTIC_MODE", "loose") or "loose").strip().lower()
_PIPELINE_EVERY = int(os.environ.get("SCRAPER_PIPELINE_EVERY", "100"))
_PIPELINE_AI_PARTIAL = os.environ.get("SCRAPER_PIPELINE_AI_PARTIAL", "").strip().lower() in (
    "1",
    "true",
    "yes",
)

_PIPELINE_STOP = object()
_HTTP_STATUS_LOCK = threading.Lock()
_RECENT_HTTP_STATUS = deque(maxlen=10)
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
            data = json.load(f)
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
        json.dump(cur, f, ensure_ascii=False, indent=2)
    os.replace(tmp, SCRAPER_BG_STATE_PATH)


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


def _jitter_sleep() -> None:
    _time.sleep(random.uniform(0.5, 1.5))


def _session() -> Any:
    """جلسة كشط: curl_cffi (بصمة Chrome) عند التثبيت، وإلا requests."""
    sess = create_scraper_session()
    # جلسة requests فقط — curl_cffi يثبّت UA مع impersonate
    if isinstance(sess, requests.Session):
        sess.headers.update(
            {
                "User-Agent": _random_ua(),
                "Accept": "text/html,application/xml,text/xml,application/json;q=0.9,*/*;q=0.8",
                "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
                "Accept-Encoding": "gzip, deflate",
                "DNT": "1",
                "Connection": "keep-alive",
            }
        )
    return sess


def _close_scraper_session(session: Any) -> None:
    """يغلق جلسة requests/curl_cffi ويحرر المقابس — ضروري للكشط عالي الحجم."""
    if session is None:
        return
    try:
        close = getattr(session, "close", None)
        if callable(close):
            close()
    except Exception:
        pass


@contextmanager
def _scraper_session_cm():
    """سياق جلسة كشط واحدة مع إغلاق مضمون في finally."""
    sess = _session()
    try:
        yield sess
    finally:
        _close_scraper_session(sess)


def _http_get_armored(session: Any, url: str, timeout: float = 25.0):
    """GET مع تدوير UA (requests فقط) و backoff أسي عند 403/429/5xx. curl_cffi يُترك ببصمة TLS ثابتة."""
    backoff = 5.0
    last_exc: Exception | None = None
    last_status = 0
    for attempt in range(6):
        if isinstance(session, requests.Session):
            session.headers["User-Agent"] = _random_ua()
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            last_status = int(r.status_code or 0)
            if r.status_code in (429, 403, 503, 502, 500, 504):
                _time.sleep(backoff)
                backoff = min(backoff * 2.0, 60.0)
                continue
            with _HTTP_STATUS_LOCK:
                _RECENT_HTTP_STATUS.append(last_status)
            return r
        except Exception as e:
            last_exc = e
            logger.warning(
                "HTTP GET attempt failed (step=http_get_armored) url=%s attempt=%s/6",
                url,
                attempt + 1,
                exc_info=True,
            )
            _time.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)
    with _HTTP_STATUS_LOCK:
        _RECENT_HTTP_STATUS.append(last_status if last_status else 0)
    if last_exc:
        logger.warning(
            "HTTP GET exhausted retries url=%s last_status=%s",
            url,
            last_status,
            exc_info=last_exc,
        )
        return None
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


def _expand_sitemap_to_page_urls(session: Any, start_url: str, progress_cb=None) -> list[str]:
    page_urls: list[str] = []
    seen_sm: set[str] = set()
    queue = [start_url]
    t0 = _time.time()
    while queue and len(page_urls) < _SITEMAP_LOC_CAP:
        if _SITEMAP_EXPAND_TIMEOUT_SEC > 0 and (_time.time() - t0) > _SITEMAP_EXPAND_TIMEOUT_SEC:
            break
        sm_url = queue.pop(0)
        if sm_url in seen_sm:
            continue
        if len(seen_sm) >= _MAX_SITEMAP_INDEX_ENTRIES:
            continue
        seen_sm.add(sm_url)
        if progress_cb:
            try:
                progress_cb(len(seen_sm), len(queue), len(page_urls))
            except Exception:
                logger.exception(
                    "sitemap progress_cb failed sm_url=%s seen_sm=%s",
                    sm_url,
                    len(seen_sm),
                )
        _jitter_sleep()
        r = _http_get_armored(session, sm_url, timeout=30.0)
        if r is None or r.status_code != 200 or not r.content:
            continue
        if len(r.content) > _MAX_SITEMAP_BYTES:
            continue
        locs, is_index = _parse_sitemap_xml(r.content)
        if is_index:
            for loc in locs:
                if loc.startswith("http") and loc not in seen_sm:
                    queue.append(loc)
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
            data = json.loads(raw)
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


def _scrape_url(session: Any, page_url: str) -> dict[str, Any] | None:
    _jitter_sleep()
    r = _http_get_armored(session, page_url, timeout=22.0)
    if r is None or r.status_code != 200 or not r.text:
        return None
    html = r.text
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
            raw = json.load(f)
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


def _load_checkpoint(seeds_fp: str) -> tuple[set[str], list[dict[str, Any]]]:
    if not os.path.isfile(CHECKPOINT_JSON):
        return set(), []
    try:
        with open(CHECKPOINT_JSON, encoding="utf-8") as f:
            d = json.load(f)
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
            json.dump(
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


def _fetch_url_row(u: str) -> tuple[str, dict[str, Any] | None]:
    """خيط منفصل لكل URL — جلسة مؤقتة تُغلق حتماً بعد الطلب."""
    _jitter_sleep()
    sess = _session()
    try:
        return u, _scrape_url(sess, u)
    except Exception:
        logger.exception("_fetch_url_row scrape failed url=%s", u)
        return u, None
    finally:
        _close_scraper_session(sess)


def _recent_block_ratio() -> float:
    with _HTTP_STATUS_LOCK:
        xs = list(_RECENT_HTTP_STATUS)
    if not xs:
        return 0.0
    blocked = sum(1 for s in xs if s in (403, 429))
    return float(blocked) / float(len(xs))


def run_scraper_sync(
    progress_cb=None,
    pipeline: dict[str, Any] | None = None,
) -> int:
    """تشغيل الكشط — يعيد عدد الصفوف المكتوبة.

    pipeline (اختياري): {"our_df": DataFrame, "comp_key": "Scraped_Competitor",
    "every": لقطات كل N صف، "use_ai_partial": bool،
    "incremental_every": حفظ CSV + استدعاء on_incremental_flush كل N صف (مجموع مكسوب حتى الآن)،
    "on_incremental_flush": دالة(rows) لتحديث كتالوج المنافس،
    "on_analysis_snapshot": دالة(rows_snap, analysis_df, is_final) للوحة مباشرة،
    "on_scrape_rows_tick": دالة(n_rows) أثناء الكشط لتحديث اللقطة دون انتظار المحرك،
    "on_pipeline_before_analysis": دالة(rows_snap, is_final) قبل run_full_analysis}
    يملأ pipeline["out"] بمفاتيح analysis_df / error عند التحليل المترافق.
    SCRAPER_INCREMENTAL_EVERY في البيئة يحدد الدفعة إن وُجدت.
    """
    seeds = _load_sitemap_seeds()
    if not seeds:
        return 0

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
    }

    with _scraper_session_cm() as session:
        all_page_urls: list[str] = []
        seen_u: set[str] = set()
        for si, seed in enumerate(seeds):
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
            expanded = _expand_sitemap_to_page_urls(
                session,
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
                    # لا توجد روابط تبدو كمنتجات — سلوك قديم: املأ حتى 80 رابطاً
                    all_page_urls.append(u)
                if _max_sitemap_urls_reached(len(all_page_urls)):
                    break
            if _max_sitemap_urls_reached(len(all_page_urls)):
                break

        total_urls = len(all_page_urls)
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

        _scrape_tick = [0, 0.0]

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
            if progress_cb:
                progress_cb(
                    i_pos + 1,
                    total_urls,
                    last_name[:80] if last_name else "جاري البحث...",
                )
            if len(rows) % _CHECKPOINT_EVERY == 0 and rows:
                _save_checkpoint(seeds_fp, processed_urls, rows)
            if _max_product_rows_reached(len(rows)):
                return "stop_products"
            return None

        pending = [u for u in all_page_urls if u not in processed_urls]
        urls_processed_this_run = [0]

        if _FETCH_WORKERS > 1 and pending:
            dyn_workers = max(1, min(_FETCH_WORKERS, _MAX_ADAPTIVE_WORKERS))
            high_success_since: float | None = None
            p = 0
            stop_early = False
            while p < len(pending):
                chunk = pending[p : p + max(dyn_workers * 4, dyn_workers)]
                p += len(chunk)
                with ThreadPoolExecutor(max_workers=dyn_workers) as ex:
                    fut_map = {ex.submit(_fetch_url_row, u): u for u in chunk}
                    for fut in as_completed(fut_map):
                        try:
                            u, row = fut.result()
                        except Exception:
                            u = fut_map[fut]
                            row = None
                            logger.exception(
                                "ThreadPoolExecutor future failed url=%s", u
                            )
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
                if stop_early:
                    break
                br = _recent_block_ratio()
                now = _time.time()
                with _HTTP_STATUS_LOCK:
                    xs = list(_RECENT_HTTP_STATUS)
                ok_ratio = 0.0
                if xs:
                    ok_ratio = sum(1 for s in xs if s == 200) / float(len(xs))
                if len(xs) >= 10 and br > 0.20:
                    if dyn_workers > 1:
                        dyn_workers -= 1
                    high_success_since = None
                else:
                    if len(xs) >= 10 and ok_ratio >= 0.95:
                        if high_success_since is None:
                            high_success_since = now
                        elif now - high_success_since >= 60.0 and dyn_workers < _MAX_ADAPTIVE_WORKERS:
                            dyn_workers += 1
                            high_success_since = now
                    else:
                        high_success_since = None
        else:
            for i, u in enumerate(all_page_urls):
                if u in processed_urls:
                    if progress_cb:
                        progress_cb(i + 1, total_urls, last_name)
                    continue
                if _max_fetch_urls_reached(urls_processed_this_run[0]):
                    break
                row = _scrape_url(session, u)
                processed_urls.add(u)
                urls_processed_this_run[0] += 1
                if _consume_row(u, row, i) == "stop_products":
                    break

    if pipeline_q is not None and pipeline_thread is not None:
        if rows:
            pipeline_q.put((copy.deepcopy(rows), True))
        pipeline_q.put(_PIPELINE_STOP)
        pipeline_thread.join(timeout=7200)

    if not rows:
        print(
            f"[SCRAPER] sitemap_total={stats['sitemap_total']} accepted={stats['heuristic_accepted']} "
            f"rejected={stats['heuristic_rejected']} extract_ok={stats['extract_ok']} "
            f"extract_fail={stats['extract_fail']} dup_name={stats['dup_name']} "
            f"skip_non_product={stats['skip_non_product']} skip_zero_price={stats['skip_zero_price']}",
            flush=True,
        )
        return 0

    write_competitors_csv(rows)
    print(
        f"[SCRAPER] sitemap_total={stats['sitemap_total']} accepted={stats['heuristic_accepted']} "
        f"rejected={stats['heuristic_rejected']} extract_ok={stats['extract_ok']} "
        f"extract_fail={stats['extract_fail']} dup_name={stats['dup_name']} "
        f"skip_non_product={stats['skip_non_product']} skip_zero_price={stats['skip_zero_price']}",
        flush=True,
    )

    # اكتمال ناجح → حذف نقاط الحفظ ليبدأ الجلسة القادمة من جديد
    _clear_checkpoint_files()

    return len(rows)


async def run_scraper_engine(progress_cb=None, pipeline: dict[str, Any] | None = None) -> int:
    """للتوافق مع استدعاءات async — يمرّر progress_cb إلى الكشط المتزامن."""
    import functools

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, functools.partial(run_scraper_sync, progress_cb, pipeline)
    )
