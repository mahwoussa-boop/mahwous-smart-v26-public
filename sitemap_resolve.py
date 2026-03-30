"""
اكتشاف رابط خريطة الموقع (Sitemap) من رابط متجر.
يستخدم browser_like_http: TLS عبر curl_cffi، ثم Playwright عند الحظر إن وُجد.
"""
from __future__ import annotations

import os
import re
import traceback
from collections.abc import Callable
from urllib.parse import urljoin, urlparse

from browser_like_http import (
    create_scraper_session,
    fetch_url_bytes,
    playwright_browser_context,
    playwright_sub_fetch,
)

_UserFetch = Callable[[str, int], tuple[int, bytes, bool]]

_CF_MSG = (
    "تعذر التأكد من خريطة الموقع: الخادم يحدّ الطلبات الآلية (مثل Cloudflare 429/403). "
    "ثبّت: pip install curl-cffi playwright && playwright install chromium — ثم أعد المحاولة. "
    "أو الصق رابط ملف .xml يدويًا، أو استورد CSV."
)


def _normalize_origin(url: str) -> tuple[str, str]:
    u = (url or "").strip()
    if not u:
        return "", ""
    if not re.match(r"^https?://", u, re.I):
        u = "https://" + u
    p = urlparse(u)
    if not p.netloc:
        return "", ""
    origin = f"{p.scheme}://{p.netloc}"
    return u, origin


def _is_sitemap_body(body: bytes) -> bool:
    if not body:
        return False
    s = body.lstrip()
    if s.startswith(b"\xef\xbb\xbf"):
        s = s[3:]
    low = s[:16000].lower()
    if b"<html" in low[:3000] or b"<!doctype html" in low[:3000]:
        return False
    return b"urlset" in low or b"sitemapindex" in low


def _extract_sitemap_hrefs(html: str, origin: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()

    for m in re.finditer(
        r"""<link[^>]+rel\s*=\s*["']sitemap["'][^>]+href\s*=\s*["']([^"']+)["']""",
        html,
        re.I,
    ):
        u = urljoin(origin + "/", m.group(1).strip())
        if u not in seen:
            seen.add(u)
            out.append(u)

    for m in re.finditer(
        r"""href\s*=\s*["']([^"']*sitemap[^"']*\.xml[^"']*)["']""",
        html,
        re.I,
    ):
        u = urljoin(origin + "/", m.group(1).strip())
        if u not in seen and u.lower().endswith(".xml"):
            seen.add(u)
            out.append(u)

    for m in re.finditer(r"""https?://[^\s"'<>]+\.xml[^\s"'<>]*""", html, re.I):
        u = m.group(0).rstrip("'\"")
        if "sitemap" in u.lower() and u not in seen:
            seen.add(u)
            out.append(u)

    return out


def _format_playwright_error(exc: BaseException) -> str:
    """رسالة مفهومة حتى لو كان str(exc) فارغاً."""
    lines = traceback.format_exception_only(type(exc), exc)
    s = "".join(lines).strip().replace("\n", " ")
    if not s:
        s = repr(exc)
    return s


def _try_playwright_fallback() -> bool:
    v = os.environ.get("SCRAPER_USE_PLAYWRIGHT", "").strip().lower()
    if v in ("0", "false", "no"):
        return False
    try:
        import playwright  # noqa: F401

        return True
    except ImportError:
        return v in ("1", "true", "yes")


def _discover_with_fetch(full: str, origin: str, fetch: _UserFetch) -> tuple[str | None, str, bool]:
    """يعيد (رابط الخريطة، رسالة، hub_blocked_any)."""
    blocked_any = False
    consec_block = 0

    def wall_hit(http_code: int) -> bool:
        nonlocal consec_block
        if http_code in (429, 403):
            consec_block += 1
        elif http_code:
            consec_block = 0
        return consec_block >= 2

    code, prefix, bl = fetch(full, 24576)
    blocked_any = blocked_any or bl
    if code == 200 and _is_sitemap_body(prefix):
        return full, f"✅ الرابط يشير مباشرةً إلى خريطة موقع صالحة: {full}", blocked_any
    if wall_hit(code):
        return None, _CF_MSG, blocked_any

    robots_url = urljoin(origin + "/", "robots.txt")
    try:
        code_r, body_r, bl = fetch(robots_url, 65536)
        blocked_any = blocked_any or bl
        if wall_hit(code_r):
            return None, _CF_MSG, blocked_any
        if code_r == 200 and body_r:
            text = body_r.decode("utf-8", errors="ignore")
            sm_urls: list[str] = []
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                low = line.lower()
                if low.startswith("sitemap:"):
                    sm = line.split(":", 1)[1].strip()
                    if sm.startswith("http"):
                        sm_urls.append(sm)
            preferred = [u for u in sm_urls if "product" in u.lower()]
            rest = [u for u in sm_urls if u not in preferred]
            for sm in preferred + rest:
                c2, p2, bl2 = fetch(sm, 24576)
                blocked_any = blocked_any or bl2
                if c2 == 200 and _is_sitemap_body(p2):
                    return sm, f"✅ وُجدت خريطة الموقع من robots.txt: {sm}", blocked_any
                if wall_hit(c2):
                    return None, _CF_MSG, blocked_any
    except Exception:
        pass

    for page_url in (full, urljoin(origin + "/", "/")):
        code, htmlb, bl = fetch(page_url, 262144)
        blocked_any = blocked_any or bl
        if wall_hit(code):
            return None, _CF_MSG, blocked_any
        if code != 200 or not htmlb:
            continue
        try:
            html = htmlb.decode("utf-8", errors="ignore")
        except Exception:
            continue
        if "<html" not in html.lower()[:5000] and "<!doctype html" not in html.lower()[:5000]:
            continue
        for href in _extract_sitemap_hrefs(html, origin):
            c3, p3, bl3 = fetch(href, 24576)
            blocked_any = blocked_any or bl3
            if c3 == 200 and _is_sitemap_body(p3):
                return href, f"✅ وُجدت خريطة الموقع من الصفحة الرئيسية: {href}", blocked_any
            if wall_hit(c3):
                return None, _CF_MSG, blocked_any

    candidates = [
        "sitemap.xml",
        "sitemap_products.xml",
        "sitemap-products.xml",
        "sitemap_index.xml",
        "wp-sitemap.xml",
        "sitemaps/sitemap.xml",
        "product-sitemap.xml",
        "products.xml",
        "sitemap/products.xml",
        "sitemap1.xml",
    ]
    for path in candidates:
        c = urljoin(origin + "/", path)
        code, pref, bl = fetch(c, 24576)
        blocked_any = blocked_any or bl
        if code == 200 and _is_sitemap_body(pref):
            return c, f"✅ وُجدت خريطة الموقع: {c}", blocked_any
        if wall_hit(code):
            return None, _CF_MSG, blocked_any

    if blocked_any:
        return None, _CF_MSG, blocked_any
    return None, "لم يُعثر على خريطة موقع (جرب رابط المتجر الرئيسي أو تواصل مع الدعم).", blocked_any


def resolve_store_to_sitemap_url(store_url: str) -> tuple[str | None, str]:
    full, origin = _normalize_origin(store_url)
    if not origin:
        return None, "رابط غير صالح"

    try:
        session = create_scraper_session()

        def fetch_sess(url: str, mb: int) -> tuple[int, bytes, bool]:
            return fetch_url_bytes(
                session,
                url,
                timeout=22.0,
                max_body_bytes=mb,
                max_attempts=2,
            )

        found, msg, blocked = _discover_with_fetch(full, origin, fetch_sess)
        if found:
            return found, msg

        if blocked and _try_playwright_fallback():
            try:
                with playwright_browser_context(origin.rstrip("/"), warmup_url=full) as (
                    req,
                    page,
                ):

                    def fetch_pw(url: str, mb: int) -> tuple[int, bytes, bool]:
                        return playwright_sub_fetch(
                            req, url, mb, page=page, max_attempts=2
                        )

                    found2, msg2, _ = _discover_with_fetch(full, origin, fetch_pw)
                    if found2:
                        return found2, msg2 + " (عبر Playwright)"
            except Exception as e:
                detail = _format_playwright_error(e)
                hints: list[str] = []
                lowd = detail.lower()
                if "executable doesn't exist" in lowd or "browserType.launch" in lowd:
                    hints.append("شغّل في الطرفية: playwright install chromium")
                elif "timeout" in lowd:
                    hints.append(
                        "جرّب زيادة وقت الانتظار: SCRAPER_PW_SETTLE_MS=8000 أو تحقق من الشبكة"
                    )
                hint_txt = (" — " + " ".join(hints)) if hints else ""
                return None, (
                    f"❌ Playwright: {detail}{hint_txt}. "
                    f"الكشط الآلي ما زال قد يُحظر من الموقع؛ أو استخدم رابط .xml يدوياً أو CSV. "
                    f"({_CF_MSG})"
                )

        if blocked:
            return None, msg
        return None, msg
    except Exception as e:
        return None, str(e)
