"""
مسار تسريع لمتاجر سلة: جلب JSON خفيف بدل صفحات HTML الثقيلة عند الإمكان.

الاستراتيجيات (بالترتيب):
  1) مسارات Next.js ‎/_next/data/{buildId}/…‎ مع pagination (page=)
  2) استخراج ‎__NEXT_DATA__‎ من صفحة /products أو الرئيسية
  3) محاولة مسارات API نسبية شائعة (/api/v1/products …)
  4) فشل كامل → يعيد القائمة الفارغة ويعود الكاشط لمسار الـ sitemap + HTML

لا يعتمد على مفاتيح OAuth لـ admin API — يقتصر على ما يعرضه المتجر للزائر.
"""
from __future__ import annotations

import logging
import re
from html import unescape
from typing import Any
from urllib.parse import urljoin, urlparse

from utils.jsonfast import loads as json_loads

logger = logging.getLogger(__name__)

_SALLA_MARKERS = (
    "cdn.salla",
    "salla.sa",
    "api.salla.dev",
    "salla.network",
    "@salla.sa",
    "salla",
)


def normalize_seed_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not u.lower().startswith("http"):
        u = "https://" + u
    return u


def origin_from_url(url: str) -> str:
    u = normalize_seed_url(url)
    if not u:
        return ""
    p = urlparse(u)
    if not p.netloc:
        return ""
    return f"{p.scheme}://{p.netloc}"


def is_salla_host(netloc: str) -> bool:
    nl = (netloc or "").lower()
    return nl.endswith(".salla.sa") or nl.endswith(".salla.dev")


def html_looks_like_salla_store(html: str) -> bool:
    if not html or len(html) < 80:
        return False
    h = html.lower()
    hits = sum(1 for m in _SALLA_MARKERS if m in h)
    return hits >= 2


def extract_build_id(html: str) -> str | None:
    if not html:
        return None
    for pat in (
        r'"buildId"\s*:\s*"([^"]+)"',
        r'"buildId":"([^"]+)"',
        r'buildId\\":\\"([^"\\]+)',
    ):
        m = re.search(pat, html[:500000])
        if m:
            return m.group(1).strip()
    return None


def extract_next_data_json(html: str) -> dict[str, Any] | None:
    if not html:
        return None
    m = re.search(
        r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(.*?)</script>',
        html,
        re.I | re.DOTALL,
    )
    if not m:
        return None
    raw = m.group(1).strip()
    try:
        data = json_loads(raw)
        return data if isinstance(data, dict) else None
    except Exception:
        logger.debug("salla: __NEXT_DATA__ parse failed", exc_info=True)
        return None


def _as_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("\u00a0", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _pick_name(d: dict[str, Any]) -> str:
    for k in ("name", "title", "product_name", "label"):
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return unescape(v.strip())
    return ""


def _pick_price(d: dict[str, Any]) -> float | None:
    for k in ("price", "sale_price", "regular_price", "amount", "final_price", "min_price"):
        p = _as_float(d.get(k))
        if p is not None and p > 0:
            return p
    pr = d.get("pricing")
    if isinstance(pr, dict):
        for k in ("price", "amount"):
            p = _as_float(pr.get(k))
            if p is not None and p > 0:
                return p
    return None


def _pick_image(d: dict[str, Any]) -> str:
    for k in ("image", "image_url", "main_image", "cover", "thumbnail"):
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
        if isinstance(v, dict):
            u = v.get("url") or v.get("src")
            if isinstance(u, str) and u.strip():
                return u.strip()
    imgs = d.get("images")
    if isinstance(imgs, list) and imgs:
        x0 = imgs[0]
        if isinstance(x0, str) and x0.strip():
            return x0.strip()
        if isinstance(x0, dict):
            u = x0.get("url") or x0.get("src")
            if isinstance(u, str):
                return u.strip()
    return ""


def _pick_slug_or_id(d: dict[str, Any]) -> str:
    for k in ("slug", "permalink", "url", "html_url", "id", "product_id", "sku"):
        v = d.get(k)
        if v is None:
            continue
        if isinstance(v, (int, float)):
            return str(int(v)) if float(v) == int(v) else str(v)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def normalize_salla_product(d: dict[str, Any], origin: str) -> dict[str, Any] | None:
    name = _pick_name(d)
    if not name or len(name) < 2:
        return None
    price = _pick_price(d)
    if price is None or price <= 0:
        return None
    img = _pick_image(d)
    slug = _pick_slug_or_id(d)
    url = ""
    for k in ("url", "permalink", "html_url"):
        v = d.get(k)
        if isinstance(v, str) and v.startswith("http"):
            url = v.strip()
            break
    if not url and slug:
        if slug.startswith("/"):
            url = urljoin(origin + "/", slug.lstrip("/"))
        elif slug.startswith("http"):
            url = slug
        else:
            url = urljoin(origin + "/", f"product/{slug}")
    if not url:
        url = origin + "/"
    out = {
        "name": name,
        "price": float(price),
        "image": img,
        "url": url,
    }
    return out


def _deep_collect_product_dicts(obj: Any, out: list[dict[str, Any]], depth: int = 0) -> None:
    if depth > 28:
        return
    if isinstance(obj, dict):
        if _pick_name(obj) and _pick_price(obj) is not None:
            out.append(obj)
            return
        for v in obj.values():
            _deep_collect_product_dicts(v, out, depth + 1)
    elif isinstance(obj, list):
        for it in obj[:800]:
            _deep_collect_product_dicts(it, out, depth + 1)


def products_from_arbitrary_json(data: Any, origin: str) -> list[dict[str, Any]]:
    raw: list[dict[str, Any]] = []
    _deep_collect_product_dicts(data, raw)
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for d in raw:
        norm = normalize_salla_product(d, origin)
        if not norm:
            continue
        key = (norm["name"], round(norm["price"], 2))
        if key in seen:
            continue
        seen.add(key)
        out.append(norm)
    return out


def _next_data_paths_to_try() -> list[str]:
    """مسارات شائعة في متاجر سلة (Next.js)."""
    return [
        "products.json",
        "ar/products.json",
        "en/products.json",
        "ar/منتجات.json",
    ]


async def collect_salla_products_fast_path(
    fetcher: Any,
    seed_url: str,
    *,
    max_pages: int = 500,
    per_page_hint: int = 48,
) -> list[dict[str, Any]]:
    """
    يحاول جمع منتجات من واجهات JSON/Next.js.
    يعيد قائمة {name, price, image, url} أو [] عند الفشل.
    """
    origin = origin_from_url(seed_url)
    if not origin:
        return []

    async def get(u: str) -> tuple[int, str | None]:
        if hasattr(fetcher, "get_text_once"):
            return await fetcher.get_text_once(u, timeout=28.0)
        return 0, None

    # صفحة رئيسية + صفحة منتجات لاكتشاف buildId وعلامات سلة
    home_r, home_html = await get(origin + "/")
    prod_r, prod_html = await get(origin.rstrip("/") + "/products")
    if (home_r != 200 or not home_html) and prod_r == 200 and prod_html:
        home_html = prod_html
    elif prod_r != 200 or not prod_html:
        prod_html = home_html or ""

    html_blob = (prod_html or "") + "\n" + (home_html or "")
    if not html_looks_like_salla_store(html_blob) and not is_salla_host(urlparse(origin).netloc):
        logger.info("salla fast path: no salla markers, skip origin=%s", origin)
        return []

    build_id = extract_build_id(html_blob) or extract_build_id(prod_html or "") or extract_build_id(
        home_html or ""
    )
    collected: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    def add_batch(items: list[dict[str, Any]]) -> None:
        for it in items:
            u = str(it.get("url") or "")
            if u and u in seen_urls:
                continue
            if u:
                seen_urls.add(u)
            collected.append(it)

    # 1) Next.js data routes
    if build_id:
        for rel in _next_data_paths_to_try():
            for page in range(1, max_pages + 1):
                u = f"{origin}/_next/data/{build_id}/{rel}?page={page}"
                code, txt = await get(u)
                if code != 200 or not txt:
                    if page == 1:
                        break
                    break
                try:
                    data = json_loads(txt)
                except Exception:
                    if page == 1:
                        break
                    break
                batch = products_from_arbitrary_json(data, origin)
                if not batch:
                    if page == 1:
                        break
                    break
                add_batch(batch)
                if len(batch) < max(8, per_page_hint // 4):
                    break

    # 2) __NEXT_DATA__ من صفحة المنتجات
    nd = extract_next_data_json(prod_html or "") or extract_next_data_json(home_html or "")
    if nd:
        add_batch(products_from_arbitrary_json(nd, origin))

    # 3) مسارات API نسبية (JSON عام)
    api_candidates = [
        f"{origin}/api/v1/products",
        f"{origin}/api/products",
        f"{origin}/api/store/products",
    ]
    for base in api_candidates:
        for page in range(1, min(max_pages, 200) + 1):
            sep = "&" if "?" in base else "?"
            u = f"{base}{sep}page={page}&per_page={per_page_hint}"
            code, txt = await get(u)
            if code != 200 or not txt or not txt.lstrip().startswith("{"):
                break
            try:
                data = json_loads(txt)
            except Exception:
                break
            inner: Any = data
            if isinstance(data, dict):
                inner = (
                    data.get("data")
                    or data.get("result")
                    or data.get("items")
                    or data.get("products")
                    or data
                )
            batch: list[dict[str, Any]] = []
            if isinstance(inner, list):
                for x in inner:
                    if isinstance(x, dict):
                        n = normalize_salla_product(x, origin)
                        if n:
                            batch.append(n)
            else:
                batch = products_from_arbitrary_json(inner if isinstance(inner, dict) else data, origin)
            if not batch:
                break
            add_batch(batch)

    # إزالة التكرار النهائي بالاسم
    seen_n: set[str] = set()
    uniq: list[dict[str, Any]] = []
    for it in collected:
        n = str(it.get("name", "")).strip().lower()
        if not n or n in seen_n:
            continue
        seen_n.add(n)
        uniq.append(it)

    if uniq:
        logger.info("salla fast path: collected=%s origin=%s", len(uniq), origin)
    return uniq
