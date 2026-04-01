"""
جلب HTTP بسلوك أقرب للمتصفح الحقيقي لتقليل حظر Cloudflare/WAF:
- curl_cffi: بصمة TLS/JA3 مثل Chrome (الأفضل دون تشغيل متصفح كامل).
- Playwright (اختياري): جلسة Chromium مع زيارة الصفحة الرئيسية ثم طلبات follow-up.

متغيرات البيئة:
  SCRAPER_IMPERSONATE   — تعريف curl_cffi (افتراضي: chrome131)
  SCRAPER_ASYNC_IMPERSONATE — curl_cffi للكشط async (افتراضي: chrome120)
  SCRAPER_DISABLE_CURL_CFFI — 1 لتعطيل curl_cffi والاكتفاء بـ requests
  SCRAPER_USE_PLAYWRIGHT — 1 لتفعيل مسار Playwright في اكتشاف الخريطة عند الفشل
  SCRAPER_PW_SETTLE_MS — انتظار بعد تحميل الصفحة الرئيسية (افتراضي 2500)
  SCRAPER_PROXY_LIST — قائمة بروكسيات مفصولة بفاصلة أو سطر (http://user:pass@host:port)
  SCRAPER_UA_ROTATE_EVERY — تدوير User-Agent كل N طلبات (0 = معطّل)
"""
from __future__ import annotations

import os
import random
import time
from contextlib import asynccontextmanager, contextmanager
from typing import Any
from urllib.parse import urlparse

import requests

_IMPERSONATE = (os.environ.get("SCRAPER_IMPERSONATE") or "chrome131").strip() or "chrome131"
_ASYNC_IMPERSONATE = (os.environ.get("SCRAPER_ASYNC_IMPERSONATE") or "chrome120").strip() or "chrome120"
_DISABLE_CURL = os.environ.get("SCRAPER_DISABLE_CURL_CFFI", "").lower() in ("1", "true", "yes")
_PW_SETTLE_MS = int(os.environ.get("SCRAPER_PW_SETTLE_MS", "2500"))

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "DNT": "1",
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


def _parse_proxy_list_env() -> list[str]:
    raw = (os.environ.get("SCRAPER_PROXY_LIST") or "").strip()
    if not raw:
        return []
    out: list[str] = []
    for part in raw.replace("\n", ",").split(","):
        p = part.strip()
        if p.startswith("http://") or p.startswith("https://"):
            out.append(p)
    return out


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
    max_attempts: int = 4,
) -> tuple[int, bytes, bool]:
    """
    GET كامل ثم اقتطاع المحتوى مع Exponential Backoff (موثوق مع curl_cffi و requests).
    يعيد (الرمز، الجسم أو البادئة، هل واجهنا 429/403/503).
    """
    saw_block = False
    last_code = 0
    for attempt in range(max_attempts):
        try:
            time.sleep(random.uniform(0.3, 0.8))
            r = session.get(url, timeout=timeout, allow_redirects=True)
            last_code = getattr(r, "status_code", 0) or 0

            if last_code in (429, 403, 503):
                saw_block = True
                if attempt + 1 < max_attempts:
                    backoff = (2**attempt) * 2.0 + random.uniform(0.5, 2.0)
                    time.sleep(backoff)
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
                backoff = (2**attempt) * 2.0 + random.uniform(0.5, 2.0)
                time.sleep(backoff)
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
    max_attempts: int = 4,
) -> tuple[int, bytes, bool]:
    """
    طلبات من جلسة Playwright مع Exponential Backoff عند الحظر.
    إن فشل request.get (XHR)، يُجرّب page.goto كما يفعل المتصفح عند فتح رابط مباشر.
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
            time.sleep(random.uniform(0.3, 0.8))
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
            if last_code in (429, 403, 503):
                saw_block = True
                if attempt + 1 < max_attempts:
                    backoff = (2**attempt) * 2.5 + random.uniform(1.0, 3.0)
                    time.sleep(backoff)
                continue
            if last_code != 200:
                return last_code, b"", saw_block
        except Exception:
            saw_block = True
            if attempt + 1 < max_attempts:
                backoff = (2**attempt) * 2.0 + random.uniform(1.0, 2.0)
                time.sleep(backoff)
            continue
    return last_code, b"", saw_block


# ═══════════════════════════════════════════════════════════════════════════════
#  Phase 2 — كشط async: curl_cffi.AsyncSession + httpx.AsyncClient (احتياطي)
# ═══════════════════════════════════════════════════════════════════════════════


def curl_cffi_async_available() -> bool:
    """هل يتوفر AsyncSession من curl_cffi (مع احترام SCRAPER_DISABLE_CURL_CFFI)."""
    if _DISABLE_CURL:
        return False
    try:
        from curl_cffi.requests import AsyncSession  # noqa: F401

        return True
    except ImportError:
        return False


async def _maybe_await_close(obj: Any) -> None:
    if obj is None:
        return
    try:
        c = getattr(obj, "close", None)
        if callable(c):
            out = c()
            import asyncio

            if asyncio.iscoroutine(out):
                await out
    except Exception:
        pass


class AsyncScraperHTTP:
    """
    جلسة HTTP للكشط غير المتزامن:
    - أساسي: ``curl_cffi.requests.AsyncSession(impersonate=…)`` لتجاوز WAF/Cloudflare.
    - احتياطي: ``httpx.AsyncClient`` عند فشل curl أو غيابه.
    يُغلق كلاهما في ``__aexit__`` لتفادي تسرّب المقابس.
    """

    def __init__(self) -> None:
        self._curl: Any = None
        self._httpx: Any = None
        self._curl_entered = False
        self._proxy_urls = _parse_proxy_list_env()
        self._proxy_idx = 0
        self._request_seq = 0

    async def __aenter__(self) -> "AsyncScraperHTTP":
        import httpx

        timeout = httpx.Timeout(30.0, connect=20.0)
        headers = {
            "User-Agent": random.choice(_USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ar,en-US;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "DNT": "1",
        }
        self._httpx = httpx.AsyncClient(
            headers=headers,
            timeout=timeout,
            follow_redirects=True,
            http2=False,
        )
        await self._httpx.__aenter__()

        if curl_cffi_async_available():
            try:
                from curl_cffi.requests import AsyncSession

                imp = _ASYNC_IMPERSONATE
                session: Any = None
                for cand in (imp, "chrome120", "chrome124", "chrome131", "safari17_0"):
                    try:
                        session = AsyncSession(impersonate=cand)
                        break
                    except Exception:
                        continue
                if session is not None:
                    aenter = getattr(session, "__aenter__", None)
                    if callable(aenter):
                        await aenter()
                        self._curl_entered = True
                    self._curl = session
            except Exception:
                self._curl = None
        return self

    def _next_proxy_url(self) -> str | None:
        if not self._proxy_urls:
            return None
        u = self._proxy_urls[self._proxy_idx % len(self._proxy_urls)]
        self._proxy_idx += 1
        return u

    def _rotate_ua_headers(self) -> dict[str, str]:
        every = int(os.environ.get("SCRAPER_UA_ROTATE_EVERY") or "0")
        if every <= 0:
            return {}
        self._request_seq += 1
        if self._request_seq % every != 0:
            return {}
        return {"User-Agent": random.choice(_USER_AGENTS)}

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._curl is not None:
            try:
                if self._curl_entered:
                    aexit = getattr(self._curl, "__aexit__", None)
                    if callable(aexit):
                        await aexit(exc_type, exc, tb)
                    else:
                        await _maybe_await_close(self._curl)
                else:
                    await _maybe_await_close(self._curl)
            except Exception:
                pass
            self._curl = None

        if self._httpx is not None:
            try:
                await self._httpx.__aexit__(exc_type, exc, tb)
            except Exception:
                pass
            self._httpx = None

    async def get_text_once(self, url: str, timeout: float = 25.0) -> tuple[int, str | None]:
        """طلب GET واحد: curl أولاً ثم httpx. يعيد (status_code, نص أو None)."""
        t = float(timeout)
        px = self._next_proxy_url()
        curl_proxies = {"http": px, "https": px} if px else None
        extra_h = self._rotate_ua_headers()
        if self._curl is not None:
            try:
                kwargs: dict[str, Any] = {"timeout": t, "allow_redirects": True}
                if curl_proxies:
                    kwargs["proxies"] = curl_proxies
                if extra_h:
                    kwargs["headers"] = extra_h
                r = await self._curl.get(url, **kwargs)
                code = int(getattr(r, "status_code", 0) or 0)
                body = getattr(r, "text", None)
                if body is None and getattr(r, "content", None) is not None:
                    raw = r.content
                    if isinstance(raw, (bytes, bytearray)):
                        body = raw.decode("utf-8", errors="replace")
                    else:
                        body = str(raw)
                return code, (body if body else None)
            except Exception:
                pass
        try:
            kw: dict[str, Any] = {"timeout": t}
            if extra_h:
                kw["headers"] = extra_h
            if px:
                kw["proxy"] = px
            r = await self._httpx.get(url, **kw)
            code = int(r.status_code)
            body = r.text or ""
            return code, (body if body else None)
        except Exception:
            return 0, None


@asynccontextmanager
async def async_scraper_http_stack():
    """سياق ``async with async_scraper_http_stack() as fetcher:`` — إغلاق مضمون."""
    f = AsyncScraperHTTP()
    await f.__aenter__()
    try:
        yield f
    finally:
        await f.__aexit__(None, None, None)
