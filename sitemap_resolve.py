"""
اكتشاف رابط خريطة الموقع (Sitemap) من رابط متجر (سلة / زد / ووردبريس وغيرها).
"""
from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

import requests

_DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
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


def _head_or_ok(session: requests.Session, url: str, timeout: float = 12.0) -> bool:
    try:
        r = session.head(url, timeout=timeout, allow_redirects=True)
        if r.status_code == 200:
            return True
        if r.status_code in (405, 501):
            r2 = session.get(url, timeout=timeout, allow_redirects=True, stream=True)
            ok = r2.status_code == 200
            r2.close()
            return ok
    except Exception:
        return False
    return False


def resolve_store_to_sitemap_url(store_url: str) -> tuple[str | None, str]:
    """
    يعيد (رابط أول sitemap صالح، رسالة للمستخدم) أو (None، سبب الفشل).
    """
    full, origin = _normalize_origin(store_url)
    if not origin:
        return None, "رابط غير صالح"

    headers = {"User-Agent": _DEFAULT_UA, "Accept": "text/html,application/xml,text/xml,*/*"}

    try:
        session = requests.Session()
        session.headers.update(headers)

        # 1) robots.txt
        robots_url = urljoin(origin + "/", "robots.txt")
        try:
            rr = session.get(robots_url, timeout=12)
            if rr.status_code == 200:
                for line in rr.text.splitlines():
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    low = line.lower()
                    if low.startswith("sitemap:"):
                        sm = line.split(":", 1)[1].strip()
                        if sm.startswith("http") and _head_or_ok(session, sm):
                            return sm, f"✅ وُجدت خريطة الموقع من robots.txt: {sm}"
        except Exception:
            pass

        # 2) مسارات شائعة
        candidates = [
            urljoin(origin + "/", "sitemap.xml"),
            urljoin(origin + "/", "sitemap_index.xml"),
            urljoin(origin + "/", "wp-sitemap.xml"),
            urljoin(origin + "/", "sitemaps/sitemap.xml"),
        ]
        for c in candidates:
            if _head_or_ok(session, c):
                return c, f"✅ وُجدت خريطة الموقع: {c}"

        return None, "لم يُعثر على خريطة موقع (جرب رابط المتجر الرئيسي أو تواصل مع الدعم)."
    except Exception as e:
        return None, str(e)
