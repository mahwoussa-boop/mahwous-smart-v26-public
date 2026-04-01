# ==============================================================================
# Mahwous Ultimate Scraper — محرك مستقل لمتاجر سلة (Salla)
# ==============================================================================
# • curl_cffi + impersonate لتقليد TLS المتصفح
# • asyncio + Semaphore للتحكم بالتزامن (تجنّب إغراق السيرفر والحظر)
# • استخراج Product من JSON-LD مع fallback لميتا
# • إخراج NDJSON لدمج لاحق مع Streamlit / Polling
#
# التثبيت:
#   pip install curl_cffi beautifulsoup4 lxml aiofiles
#
# التشغيل:
#   python mahwous_ultimate_scraper.py https://your-store.salla.sa
#
# اختياري:
#   set MAHWOUS_ULTIMATE_CONCURRENCY=32
#   set MAHWOUS_ULTIMATE_OUTPUT=data/mahwous_fast_results.ndjson
# ==============================================================================
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from urllib.parse import urlparse

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# -----------------------------------------------------------------------------
# تبعيات
# -----------------------------------------------------------------------------
try:
    from curl_cffi.requests import AsyncSession
except ImportError:
    print("⚠️ ثبّت: pip install curl_cffi", file=sys.stderr)
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("⚠️ ثبّت: pip install beautifulsoup4 lxml", file=sys.stderr)
    sys.exit(1)

try:
    import aiofiles
    HAS_AIOFILES = True
except ImportError:
    HAS_AIOFILES = False

_IMPERSONATE_FALLBACKS = (
    os.environ.get("MAHWOUS_ULTIMATE_IMPERSONATE", "").strip() or "chrome120",
    "chrome124",
    "chrome131",
    "safari17_0",
)

TIMEOUT = float(os.environ.get("MAHWOUS_ULTIMATE_TIMEOUT", "25"))
OUTPUT_FILE = os.environ.get(
    "MAHWOUS_ULTIMATE_OUTPUT", "mahwous_fast_results.ndjson"
).strip() or "mahwous_fast_results.ndjson"
_DEFAULT_CONC = 32
_MAX_CONC = int(os.environ.get("MAHWOUS_ULTIMATE_CONCURRENCY") or _DEFAULT_CONC)
_GATHER_CHUNK = int(os.environ.get("MAHWOUS_ULTIMATE_GATHER_CHUNK", "400"))


def _safe_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = re.sub(r"[^\d.]", "", str(v).replace(",", ""))
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


class ScraperStats:
    def __init__(self):
        self.start_time = time.time()
        self.processed_pages = 0
        self.success_count = 0
        self.failed_count = 0
        self.total_pages = 0
        self._lock = asyncio.Lock()

    @property
    def pages_per_minute(self) -> float:
        elapsed = time.time() - self.start_time
        if elapsed <= 0:
            return 0.0
        return round((self.processed_pages / elapsed) * 60, 2)

    async def print_progress(self) -> None:
        async with self._lock:
            elapsed = time.time() - self.start_time
            mins, secs = divmod(int(elapsed), 60)
            line = (
                f"\r⚡ {self.pages_per_minute} صفحة/د | "
                f"✅ {self.success_count}/{self.total_pages} | ❌ {self.failed_count} | "
                f"⏱️ {mins}:{secs:02d}"
            )
            sys.stdout.write(line)
            sys.stdout.flush()


def extract_salla_product_data(html: str, url: str) -> dict:
    """استخراج حقول المنتج من JSON-LD (سلة) + fallback OG."""
    soup = BeautifulSoup(html, "lxml")

    product_data = {
        "url": url,
        "name": "",
        "brand": "",
        "price": 0.0,
        "original_price": 0.0,
        "in_stock": True,
        "image_url": "",
        "sku": "",
        "description": "",
    }

    for script in soup.find_all("script", type="application/ld+json"):
        raw = script.string or script.get_text() or ""
        raw = raw.strip()
        if not raw:
            continue
        try:
            raw_data = json.loads(raw)
        except json.JSONDecodeError:
            continue

        candidates = raw_data if isinstance(raw_data, list) else [raw_data]
        data = None
        for item in candidates:
            if isinstance(item, dict) and item.get("@type") == "Product":
                data = item
                break
        if not data:
            continue

        product_data["name"] = (data.get("name") or "") or product_data["name"]

        img = data.get("image")
        if isinstance(img, list) and img:
            product_data["image_url"] = str(img[0])
        elif isinstance(img, str):
            product_data["image_url"] = img

        product_data["description"] = (data.get("description") or "")[:2000]
        product_data["sku"] = str(data.get("sku") or "")

        brand = data.get("brand")
        if isinstance(brand, dict):
            product_data["brand"] = brand.get("name", "") or product_data["brand"]
        elif isinstance(brand, str):
            product_data["brand"] = brand

        offers = data.get("offers")
        if isinstance(offers, dict):
            product_data["price"] = _safe_float(offers.get("price"))
            product_data["in_stock"] = "InStock" in str(
                offers.get("availability", "")
            )
        elif isinstance(offers, list) and offers:
            product_data["price"] = _safe_float(offers[0].get("price"))
            product_data["in_stock"] = "InStock" in str(
                offers[0].get("availability", "")
            )

    if not product_data["name"]:
        og = soup.find("meta", property="og:title")
        if og and og.get("content"):
            product_data["name"] = og["content"].strip()

    if not product_data["image_url"]:
        og = soup.find("meta", property="og:image")
        if og and og.get("content"):
            product_data["image_url"] = og["content"].strip()

    try:
        old_el = soup.select_one(".price-before, s")
        if old_el and old_el.text:
            op = _safe_float(re.sub(r"[^\d.]", "", old_el.text))
            if op > 0:
                product_data["original_price"] = op
    except Exception:
        pass

    if product_data["original_price"] == 0.0 and product_data["price"] > 0:
        product_data["original_price"] = product_data["price"]

    return product_data


async def append_ndjson(lock: asyncio.Lock, path: str, data: dict) -> None:
    line = json.dumps(data, ensure_ascii=False) + "\n"
    if HAS_AIOFILES:
        async with lock:
            async with aiofiles.open(path, "a", encoding="utf-8") as f:
                await f.write(line)
    else:
        async with lock:

            def _w():
                with open(path, "a", encoding="utf-8") as f:
                    f.write(line)

            await asyncio.to_thread(_w)


async def fetch_product(
    session: AsyncSession,
    url: str,
    semaphore: asyncio.Semaphore,
    stats: ScraperStats,
    lock: asyncio.Lock,
    out_path: str,
) -> None:
    async with semaphore:
        try:
            response = await session.get(url, timeout=TIMEOUT, allow_redirects=True)
            if response.status_code == 200:
                data = extract_salla_product_data(response.text, url)
                if data.get("name") or data.get("price", 0) > 0:
                    await append_ndjson(lock, out_path, data)
                    async with stats._lock:
                        stats.success_count += 1
                else:
                    async with stats._lock:
                        stats.failed_count += 1
            else:
                async with stats._lock:
                    stats.failed_count += 1
        except Exception:
            async with stats._lock:
                stats.failed_count += 1
        finally:
            async with stats._lock:
                stats.processed_pages += 1
                done = stats.processed_pages
                total = stats.total_pages
            if done % 5 == 0 or done >= total:
                await stats.print_progress()


async def fetch_sitemap(session: AsyncSession, sitemap_url: str) -> list[str]:
    urls: list[str] = []
    print(f"\n🕷️ قراءة Sitemap: {sitemap_url}")
    try:
        response = await session.get(sitemap_url, timeout=TIMEOUT, allow_redirects=True)
        if response.status_code != 200:
            print("❌ لا يمكن الوصول لـ sitemap.xml")
            return urls

        soup = BeautifulSoup(response.text, "xml")
        locs = [loc.get_text(strip=True) for loc in soup.find_all("loc") if loc.get_text(strip=True)]

        sub_sitemaps = [u for u in locs if "sitemap-products" in u.lower()]

        if sub_sitemaps:
            print(f"📦 خرائط منتجات داخلية: {len(sub_sitemaps)}")
            for sub_url in sub_sitemaps:
                sub_res = await session.get(sub_url, timeout=TIMEOUT, allow_redirects=True)
                if sub_res.status_code != 200:
                    continue
                sub_soup = BeautifulSoup(sub_res.text, "xml")
                urls.extend(
                    loc.get_text(strip=True)
                    for loc in sub_soup.find_all("loc")
                    if loc.get_text(strip=True)
                )
        else:
            urls = [
                u
                for u in locs
                if "/p/" in u.lower() or "/product" in u.lower() or len(locs) < 2000
            ]

        uniq = list(dict.fromkeys(urls))
        print(f"✅ روابط منتجات مكتشفة: {len(uniq)}")
        return uniq
    except Exception as e:
        print(f"❌ خطأ Sitemap: {e}")
        return []


def _make_session_cm():
    last_err = None
    for imp in _IMPERSONATE_FALLBACKS:
        try:
            return AsyncSession(impersonate=imp)
        except Exception as e:
            last_err = e
    raise RuntimeError(f"تعذر فتح AsyncSession curl_cffi: {last_err}")


async def main(store_url: str) -> None:
    print("=" * 65)
    print("🚀 Mahwous Ultimate Scraper — سلة (curl_cffi + asyncio)")
    print(f"🎯 المتجر: {store_url}")
    print(f"📁 الإخراج: {OUTPUT_FILE} | تزامن: {_MAX_CONC}")
    print("=" * 65)

    su = (store_url or "").strip()
    if not su.startswith("http"):
        su = "https://" + su

    parsed = urlparse(su)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    sitemap_url = f"{base_url}/sitemap.xml"

    os.makedirs(os.path.dirname(OUTPUT_FILE) or ".", exist_ok=True)
    open(OUTPUT_FILE, "w", encoding="utf-8").close()

    file_lock = asyncio.Lock()
    stats = ScraperStats()
    semaphore = asyncio.Semaphore(max(1, min(_MAX_CONC, 200)))

    async with _make_session_cm() as session:
        product_urls = await fetch_sitemap(session, sitemap_url)
        if not product_urls:
            print("❌ لا توجد روابط منتجات. تحقق من الرابط أو من sitemap.")
            return

        stats.total_pages = len(product_urls)
        stats.start_time = time.time()
        print(f"\n⚡ بدء الكشط (حتى {_MAX_CONC} طلباً متزامناً، دفعات {_GATHER_CHUNK})…")

        for i in range(0, len(product_urls), _GATHER_CHUNK):
            chunk = product_urls[i : i + _GATHER_CHUNK]
            await asyncio.gather(
                *[
                    fetch_product(session, u, semaphore, stats, file_lock, OUTPUT_FILE)
                    for u in chunk
                ]
            )

    await stats.print_progress()
    print("\n\n" + "=" * 65)
    print("🎉 انتهى الكشط")
    print(f"📄 ناجح: {stats.success_count} / {stats.total_pages}")
    print(f"⏱️ معدل: {stats.pages_per_minute} صفحة/د")
    print(f"💾 {OUTPUT_FILE}")
    print("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="كاشط سلة مستقل — مهووس")
    parser.add_argument("store", type=str, help="رابط المتجر، مثل https://xxx.salla.sa")
    args = parser.parse_args()

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main(args.store))
    except KeyboardInterrupt:
        print("\n🛑 أوقفه المستخدم.", file=sys.stderr)
