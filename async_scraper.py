"""
كشط غير متزامن (واجهة async + تنفيذ I/O عبر مؤشر ترابط).
مدرّع: تدوير User-Agent، Jitter، Exponential Backoff، نقاط حفظ (Checkpoint).
"""
from __future__ import annotations

import asyncio
import copy
import csv
import hashlib
import json
import os
import queue
import random
import re
import threading
import time as _time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from html import unescape
from typing import Any
from urllib.parse import urlparse

import requests

from browser_like_http import create_scraper_session

DATA_DIR = "data"
LIST_PATH = os.path.join(DATA_DIR, "competitors_list.json")
OUT_CSV = os.path.join(DATA_DIR, "competitors_latest.csv")
_COMP_CSV_FIELDS = ["اسم المنتج", "السعر", "رقم المنتج", "رابط_الصورة"]
SCRAPER_BG_STATE_PATH = os.path.join(DATA_DIR, "scraper_bg_state.json")
CHECKPOINT_JSON = os.path.join(DATA_DIR, "scraper_checkpoint.json")
CHECKPOINT_CSV = os.path.join(DATA_DIR, "competitors_checkpoint.csv")

_MAX_URLS = int(os.environ.get("SCRAPER_MAX_URLS", "2500"))
# حدّ جمع عناوين من ملفات sitemap فقط — منفصل عن حد الكشط حتى لا نُغلق الفهرس قبل sitemap-2.xml
_SITEMAP_LOC_CAP = int(os.environ.get("SCRAPER_SITEMAP_LOC_CAP", "200000"))
_MAX_SITEMAP_BYTES = 8 * 1024 * 1024
_CHECKPOINT_EVERY = int(os.environ.get("SCRAPER_CHECKPOINT_EVERY", "100"))
_CLEAR_CK = os.environ.get("SCRAPER_CLEAR_CHECKPOINT", "").strip() in ("1", "true", "yes")
_FETCH_WORKERS = max(1, min(16, int(os.environ.get("SCRAPER_FETCH_WORKERS", "1"))))
_PIPELINE_EVERY = int(os.environ.get("SCRAPER_PIPELINE_EVERY", "100"))
_PIPELINE_AI_PARTIAL = os.environ.get("SCRAPER_PIPELINE_AI_PARTIAL", "").strip().lower() in (
    "1",
    "true",
    "yes",
)

_PIPELINE_STOP = object()

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


def _http_get_armored(session: Any, url: str, timeout: float = 25.0):
    """GET مع تدوير UA (requests فقط) و backoff أسي عند 403/429/5xx. curl_cffi يُترك ببصمة TLS ثابتة."""
    backoff = 5.0
    last_exc: Exception | None = None
    for attempt in range(6):
        if isinstance(session, requests.Session):
            session.headers["User-Agent"] = _random_ua()
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code in (429, 403, 503, 502, 500, 504):
                _time.sleep(backoff)
                backoff = min(backoff * 2.0, 60.0)
                continue
            return r
        except Exception as e:
            last_exc = e
            _time.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)
    if last_exc:
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
        return [], False
    root_tag = _strip_ns(root.tag).lower()
    if root_tag == "sitemapindex":
        is_index = True
    for el in root.iter():
        t = _strip_ns(el.tag).lower()
        if t == "loc" and el.text:
            urls.append(el.text.strip())
    return urls, is_index


def _expand_sitemap_to_page_urls(session: Any, start_url: str) -> list[str]:
    page_urls: list[str] = []
    seen_sm: set[str] = set()
    queue = [start_url]
    while queue and len(page_urls) < _SITEMAP_LOC_CAP:
        sm_url = queue.pop(0)
        if sm_url in seen_sm or len(seen_sm) > 400:
            continue
        seen_sm.add(sm_url)
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
        path = ""
    pl = path.rstrip("/")
    # سلة / زد الشائع: المسار ينتهي بـ /p وأرقام معرّف المنتج
    if re.search(r"/p\d+$", pl, re.I):
        return True
    u = url.lower()
    if any(x in u for x in ("/product/", "/products/", "/item/", "/perfume")):
        return True
    if "عطر" in u and "/c" not in u:
        return True
    if re.search(r"/[^/]+-\d{3,}", u):
        return True
    return False


def _extract_from_json_ld(html: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for m in re.finditer(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.I | re.DOTALL,
    ):
        raw = m.group(1).strip()
        try:
            data = json.loads(raw)
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        for it in items:
            if not isinstance(it, dict):
                continue
            t = str(it.get("@type", "")).lower()
            if "product" not in t and "product" not in str(it.get("@graph", "")).lower():
                if "Product" not in str(it.get("@type", "")):
                    g = it.get("@graph")
                    if isinstance(g, list):
                        for g0 in g:
                            if isinstance(g0, dict) and "Product" in str(g0.get("@type", "")):
                                it = g0
                                break
            name = it.get("name")
            if isinstance(name, str) and name:
                out.setdefault("name", unescape(name))
            img = it.get("image")
            if isinstance(img, str) and img:
                out.setdefault("image", img)
            elif isinstance(img, list) and img and isinstance(img[0], str):
                out.setdefault("image", img[0])
            offers = it.get("offers")
            if isinstance(offers, dict):
                p = offers.get("price") or offers.get("lowPrice")
                if p is not None:
                    try:
                        out["price"] = float(str(p).replace(",", ""))
                    except Exception:
                        pass
            elif isinstance(offers, list) and offers:
                o0 = offers[0]
                if isinstance(o0, dict):
                    p = o0.get("price") or o0.get("lowPrice")
                    if p is not None:
                        try:
                            out["price"] = float(str(p).replace(",", ""))
                        except Exception:
                            pass
            if out.get("name") and out.get("price") is not None:
                break
        if out.get("name"):
            break
    return out


def _extract_meta_fallback(html: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    m = re.search(
        r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']',
        html,
        re.I,
    )
    if m:
        out["name"] = unescape(m.group(1))
    m = re.search(
        r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']',
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
                pass
    return out


def _scrape_url(session: Any, page_url: str) -> dict[str, Any] | None:
    _jitter_sleep()
    r = _http_get_armored(session, page_url, timeout=22.0)
    if r is None or r.status_code != 200 or not r.text:
        return None
    html = r.text
    data = _extract_from_json_ld(html)
    if not data.get("name"):
        data.update(_extract_meta_fallback(html))
    if not data.get("name"):
        return None
    if data.get("price") is None:
        fb = _extract_meta_fallback(html)
        if fb.get("price") is not None:
            data["price"] = fb["price"]
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
            pass


def _load_checkpoint(seeds_fp: str) -> tuple[set[str], list[dict[str, Any]]]:
    if not os.path.isfile(CHECKPOINT_JSON):
        return set(), []
    try:
        with open(CHECKPOINT_JSON, encoding="utf-8") as f:
            d = json.load(f)
    except Exception:
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
        pass


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
                pass
        use_ai = True if is_final else use_ai_partial
        try:
            df = run_full_analysis(
                our_df,
                {comp_key: cdf},
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
                    pass
        except Exception as e:
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
    _jitter_sleep()
    try:
        return u, _scrape_url(_session(), u)
    except Exception:
        return u, None


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

    session = _session()
    all_page_urls: list[str] = []
    seen_u: set[str] = set()
    for seed in seeds:
        expanded = _expand_sitemap_to_page_urls(session, seed)
        products = [x for x in expanded if _product_url_heuristic(x)]
        prod_set = set(products)
        rest = [x for x in expanded if x not in prod_set]
        merged = products + rest
        for u in merged:
            if u in seen_u:
                continue
            seen_u.add(u)
            if _product_url_heuristic(u):
                all_page_urls.append(u)
            elif not products and len(all_page_urls) < 80:
                # لا توجد روابط تبدو كمنتجات — سلوك قديم: املأ حتى 80 رابطاً
                all_page_urls.append(u)
            if len(all_page_urls) >= _MAX_URLS:
                break
        if len(all_page_urls) >= _MAX_URLS:
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

    def _consume_row(u: str, row: dict[str, Any] | None, i_pos: int) -> None:
        nonlocal last_name
        if row:
            name = str(row.get("name", "")).strip()
            if name:
                last_name = name
            if name and name not in seen_names:
                seen_names.add(name)
                price = row.get("price")
                if price is None:
                    price = 0.0
                img = str(row.get("image", "") or "")
                rows.append(
                    {
                        "اسم المنتج": name,
                        "السعر": price,
                        "رقم المنتج": "",
                        "رابط_الصورة": img,
                    }
                )
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
                    pass
        if inc_ev > 0 and len(rows) % inc_ev == 0 and rows:
            write_competitors_csv(rows)
            if inc_cb:
                try:
                    inc_cb(copy.deepcopy(rows))
                except Exception:
                    pass
        if progress_cb:
            progress_cb(
                i_pos + 1,
                total_urls,
                last_name[:80] if last_name else "جاري البحث...",
            )
        if len(rows) % _CHECKPOINT_EVERY == 0 and rows:
            _save_checkpoint(seeds_fp, processed_urls, rows)
        return None

    pending = [u for u in all_page_urls if u not in processed_urls]
    pref: dict[str, dict[str, Any] | None] = {}

    if _FETCH_WORKERS > 1 and pending:
        with ThreadPoolExecutor(max_workers=_FETCH_WORKERS) as ex:
            fut_map = {ex.submit(_fetch_url_row, u): u for u in pending}
            for fut in as_completed(fut_map):
                try:
                    u, row = fut.result()
                except Exception:
                    u = fut_map[fut]
                    row = None
                pref[u] = row
        for i, u in enumerate(all_page_urls):
            if u in processed_urls:
                if progress_cb:
                    progress_cb(i + 1, total_urls, last_name)
                continue
            row = pref.get(u)
            processed_urls.add(u)
            _consume_row(u, row, i)
            if len(rows) >= _MAX_URLS:
                break
    else:
        for i, u in enumerate(all_page_urls):
            if u in processed_urls:
                if progress_cb:
                    progress_cb(i + 1, total_urls, last_name)
                continue
            row = _scrape_url(session, u)
            processed_urls.add(u)
            _consume_row(u, row, i)
            if len(rows) >= _MAX_URLS:
                break

    if pipeline_q is not None and pipeline_thread is not None:
        if rows:
            pipeline_q.put((copy.deepcopy(rows), True))
        pipeline_q.put(_PIPELINE_STOP)
        pipeline_thread.join(timeout=7200)

    if not rows:
        return 0

    write_competitors_csv(rows)

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
