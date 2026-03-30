"""
جلب HTTP بسلوك أقرب للمتصفح الحقيقي لتقليل حظر Cloudflare/WAF:
- curl_cffi: بصمة TLS/JA3 مثل Chrome (الأفضل دون تشغيل متصفح كامل).
- Playwright (اختياري): جلسة Chromium مع زيارة الصفحة الرئيسية ثم طلبات follow-up.

متغيرات البيئة:
  SCRAPER_IMPERSONATE   — تعريف curl_cffi (افتراضي: chrome131)
  SCRAPER_DISABLE_CURL_CFFI — 1 لتعطيل curl_cffi والاكتفاء بـ requests
  SCRAPER_USE_PLAYWRIGHT — 1 لتفعيل مسار Playwright في اكتشاف الخريطة عند الفشل
  SCRAPER_PW_SETTLE_MS — انتظار بعد تحميل الصفحة الرئيسية (افتراضي 2500)
"""
from __future__ import annotations

import os
import random
import time
from contextlib import contextmanager
from typing import Any
from urllib.parse import urlparse

import requests

_IMPERSONATE = (os.environ.get("SCRAPER_IMPERSONATE") or "chrome131").strip() or "chrome131"
_DISABLE_CURL = os.environ.get("SCRAPER_DISABLE_CURL_CFFI", "").lower() in ("1", "true", "yes")
_PW_SETTLE_MS = int(os.environ.get("SCRAPER_PW_SETTLE_MS", "2500"))

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]


def curl_cffi_available() -> bool:
    if _DISABLE_CURL:
        return False
    try:
        import curl_cffi  # noqa: F401

        return True
    except ImportError:
        return False


def create_requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": random.choice(_USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,text/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
        }
    )
    return s


def create_browser_tls_session() -> Any:
    """
    جلسة GET تشبه Chrome على مستوى TLS. يعيد كائن requests-compatible من curl_cffi.
    """
    from curl_cffi import requests as curl_requests

    s = curl_cffi_safe_session(curl_requests)
    return s


def curl_cffi_safe_session(curl_requests_module) -> Any:
    imp = _IMPERSONATE
    try:
        return curl_requests_module.Session(impersonate=imp)
    except Exception:
        for fallback in ("chrome124", "chrome120", "safari17_0"):
            if fallback == imp:
                continue
            try:
                return curl_requests_module.Session(impersonate=fallback)
            except Exception:
                continue
        return curl_requests_module.Session()


def create_scraper_session() -> Any:
    """للكشط: curl_cffi إن وُجد، وإلا requests."""
    if curl_cffi_available():
        try:
            s = create_browser_tls_session()
            # لا تُستبدل User-Agent عند impersonate؛ أضف لغات فقط إن لم تُضف تلقائياً
            h = getattr(s, "headers", None)
            if h is not None and not h.get("Accept-Language"):
                h["Accept-Language"] = "ar,en-US;q=0.9,en;q=0.8"
            return s
        except Exception:
            pass
    return create_requests_session()


def fetch_url_bytes(
    session: Any,
    url: str,
    *,
    timeout: float = 22.0,
    max_body_bytes: int | None = None,
    max_attempts: int = 2,
) -> tuple[int, bytes, bool]:
    """
    GET كامل ثم اقتطاع المحتوى (موثوق مع curl_cffi و requests).
    يعيد (الرمز، الجسم أو البادئة، هل واجهنا 429/403).
    """
    saw_block = False
    last_code = 0
    for attempt in range(max_attempts):
        try:
            time.sleep(0.2 + random.random() * 0.35)
            r = session.get(url, timeout=timeout, allow_redirects=True)
            last_code = getattr(r, "status_code", 0) or 0
            if last_code in (429, 403):
                saw_block = True
                if attempt + 1 < max_attempts:
                    time.sleep(1.0 + random.random())
                continue
            if last_code != 200:
                return last_code, b"", saw_block
            raw = r.content or b""
            if max_body_bytes is not None and len(raw) > max_body_bytes:
                raw = raw[:max_body_bytes]
            return 200, raw, saw_block
        except Exception:
            saw_block = True
            if attempt + 1 < max_attempts:
                time.sleep(1.0 + random.random())
            continue
    return last_code, b"", saw_block


@contextmanager
def playwright_browser_context(origin: str, warmup_url: str | None = None):
    """
    سياق Playwright: زيارة صفحة تمهيدية (رابط المتجر كما أدخله المستخدم إن وُجد)
    ثم إتاحة APIRequestContext + Page لطلبات المتابعة أو page.goto للـ XML.
    """
    from playwright.sync_api import sync_playwright

    origin = (origin or "").strip().rstrip("/")
    if not origin.startswith("http"):
        raise ValueError("invalid origin for playwright")

    warm = (warmup_url or "").strip()
    if not warm.startswith("http"):
        warm = origin + "/"

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = browser.new_context(
            locale="ar-SA",
            viewport={"width": 1280, "height": 900},
            user_agent=random.choice(_USER_AGENTS),
            extra_http_headers={
                "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        page = context.new_page()
        # domcontentloaded أقل عرضة للتعلّق من wait=load خلف طلبات طرف ثالث / Cloudflare
        try:
            page.goto(warm, wait_until="domcontentloaded", timeout=90000)
        except Exception:
            page.goto(warm, wait_until="commit", timeout=45000)
        settle = max(_PW_SETTLE_MS, 3500)
        page.wait_for_timeout(settle)
        req = context.request
        try:
            yield req, page
        finally:
            browser.close()


def playwright_fetch_bytes(url: str, max_bytes: int, timeout_ms: float = 90000) -> tuple[int, bytes]:
    """GET واحد عبر سياق Playwright (يزور أصل النطاق أولاً)."""
    p = urlparse(url)
    origin = f"{p.scheme}://{p.netloc}" if p.scheme and p.netloc else ""
    if not origin:
        return 0, b""
    try:
        with playwright_browser_context(origin, warmup_url=None) as (req, page):
            st, data, _ = playwright_sub_fetch(
                req, url, max_bytes, page=page, timeout_ms=timeout_ms
            )
            return st, data
    except Exception:
        return 0, b""


def playwright_sub_fetch(
    req: Any,
    url: str,
    max_bytes: int,
    *,
    page: Any | None = None,
    timeout_ms: float = 90000,
    max_attempts: int = 2,
) -> tuple[int, bytes, bool]:
    """
    طلبات من جلسة Playwright. إن فشل request.get (XHR)، يُجرّب page.goto
    كما يفعل المتصفح عند فتح رابط مباشر (مفيد مع بعض سياسات Cloudflare).
    """
    saw_block = False
    last_code = 0
    to = min(int(timeout_ms), 120000)

    def _clip(b: bytes) -> bytes:
        if len(b) > max_bytes:
            return b[:max_bytes]
        return b

    for attempt in range(max_attempts):
        try:
            time.sleep(0.2 + random.random() * 0.3)
            resp = req.get(url, timeout=timeout_ms)
            last_code = resp.status
            if last_code == 200:
                b = resp.body()
                if b:
                    return 200, _clip(b), saw_block
            if page is not None:
                try:
                    nav = page.goto(url, wait_until="commit", timeout=to)
                    if nav and nav.ok:
                        b2 = nav.body()
                        if b2:
                            return 200, _clip(b2), saw_block
                except Exception:
                    pass
            if last_code in (429, 403):
                saw_block = True
                if attempt + 1 < max_attempts:
                    time.sleep(1.2 + random.random())
                continue
            if last_code != 200:
                return last_code, b"", saw_block
        except Exception:
            saw_block = True
            if attempt + 1 < max_attempts:
                time.sleep(1.0 + random.random())
            continue
    return last_code, b"", saw_block
