"""
كشط غير متزامن (واجهة async + تنفيذ I/O عبر مؤشر ترابط).
مدرّع: تدوير User-Agent، Jitter، Exponential Backoff، نقاط حفظ (Checkpoint).
"""
from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import os
import random
import re
import time as _time
import xml.etree.ElementTree as ET
from html import unescape
from typing import Any
from urllib.parse import urlparse

import requests

DATA_DIR = "data"
LIST_PATH = os.path.join(DATA_DIR, "competitors_list.json")
OUT_CSV = os.path.join(DATA_DIR, "competitors_latest.csv")
CHECKPOINT_JSON = os.path.join(DATA_DIR, "scraper_checkpoint.json")
CHECKPOINT_CSV = os.path.join(DATA_DIR, "competitors_checkpoint.csv")

_MAX_URLS = int(os.environ.get("SCRAPER_MAX_URLS", "2500"))
_MAX_SITEMAP_BYTES = 8 * 1024 * 1024
_CHECKPOINT_EVERY = int(os.environ.get("SCRAPER_CHECKPOINT_EVERY", "100"))
_CLEAR_CK = os.environ.get("SCRAPER_CLEAR_CHECKPOINT", "").strip() in ("1", "true", "yes")

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


def _jitter_sleep() -> None:
    _time.sleep(random.uniform(0.5, 1.5))


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": _random_ua(),
            "Accept": "text/html,application/xml,text/xml,application/json;q=0.9,*/*;q=0.8",
            "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "DNT": "1",
            "Connection": "keep-alive",
        }
    )
    return s


def _http_get_armored(session: requests.Session, url: str, timeout: float = 25.0):
    """GET مع تدوير UA و backoff أسي عند 403/429/5xx."""
    backoff = 5.0
    last_exc: Exception | None = None
    for attempt in range(6):
        session.headers["User-Agent"] = _random_ua()
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code in (429, 403, 503, 502, 500, 504):
                _time.sleep(backoff)
                backoff = min(backoff * 2.0, 60.0)
                continue
            return r
        except requests.RequestException as e:
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


def _expand_sitemap_to_page_urls(session: requests.Session, start_url: str) -> list[str]:
    page_urls: list[str] = []
    seen_sm: set[str] = set()
    queue = [start_url]
    while queue and len(page_urls) < _MAX_URLS * 3:
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
                    if len(page_urls) >= _MAX_URLS * 3:
                        break
    return page_urls


def _product_url_heuristic(url: str) -> bool:
    u = url.lower()
    if any(x in u for x in ("/product/", "/products/", "/p/", "/item/", "/perfume", "/عطر")):
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


def _scrape_url(session: requests.Session, page_url: str) -> dict[str, Any] | None:
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


def run_scraper_sync(progress_cb=None) -> int:
    """تشغيل الكشط — يعيد عدد الصفوف المكتوبة. progress_cb(current, total, last_name) اختياري."""
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
        for u in _expand_sitemap_to_page_urls(session, seed):
            if u in seen_u:
                continue
            seen_u.add(u)
            if _product_url_heuristic(u) or len(all_page_urls) < 80:
                all_page_urls.append(u)
            if len(all_page_urls) >= _MAX_URLS:
                break
        if len(all_page_urls) >= _MAX_URLS:
            break

    fieldnames = ["اسم المنتج", "السعر", "رقم المنتج", "رابط_الصورة"]

    total_urls = len(all_page_urls)
    last_name = "جاري البحث..."
    for i, u in enumerate(all_page_urls):
        if u in processed_urls:
            if progress_cb:
                progress_cb(i + 1, total_urls, last_name)
            continue
        row = _scrape_url(session, u)
        processed_urls.add(u)

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

        if progress_cb:
            progress_cb(
                i + 1,
                total_urls,
                last_name[:80] if last_name else "جاري البحث...",
            )

        if len(rows) % _CHECKPOINT_EVERY == 0 and rows:
            _save_checkpoint(seeds_fp, processed_urls, rows)

        if len(rows) >= _MAX_URLS:
            break

    if not rows:
        return 0

    os.makedirs(DATA_DIR, exist_ok=True)
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    # اكتمال ناجح → حذف نقاط الحفظ ليبدأ الجلسة القادمة من جديد
    _clear_checkpoint_files()

    return len(rows)


async def run_scraper_engine(progress_cb=None) -> int:
    """للتوافق مع استدعاءات async — يمرّر progress_cb إلى الكشط المتزامن."""
    import functools

    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, functools.partial(run_scraper_sync, progress_cb)
    )
