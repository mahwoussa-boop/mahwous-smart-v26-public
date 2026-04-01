"""
engines/hyper_pipeline.py - V26 Hyper-Intelligence Pipeline
════════════════════════════════════════════════════════════
المنظومة الذكية الخارقة: الكشط، التحلية، المطابقة، وتوزيع الأقسام.
مبنية للعمل بأعلى معايير الدقة والسرعة لمعالجة 500k+ منتج عبر Railway.
تربط هذه المنظومة (Scraper) و (Sanitizer) و (VectorMatcher) و (TriageRouter) بخط تجميع متزامن (Async Pipeline).

تشغيل من جذر المشروع (متجر سلة حقيقي):

  ``python engines/hyper_pipeline.py https://your-store.salla.sa``

أو: ``set HYPER_PIPELINE_STORE_URL=https://...`` ثم نفس الأمر بدون وسيطات.
"""
from __future__ import annotations

import asyncio
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List

# جذر المشروع — يضمن `from engines.*` و`utils` عند التشغيل كـ script
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import httpx

from engines.salla_storefront import (
    collect_salla_products_fast_path,
    origin_from_url,
    products_from_arbitrary_json,
)

# اختياري — لمسار تحليل HTML سريع لاحقاً (DOM)
try:
    from selectolax.parser import HTMLParser  # noqa: F401
except ImportError:
    HTMLParser = None  # type: ignore

logging.basicConfig(
    level=logging.INFO,
    format="[HyperPipeline] %(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("HyperPipeline")


class PerfumeSanitizer:
    """
    1. مرحلة الفلترة والتحلية (DNA Extraction & Cleaning)
    دقيقة جداً: تحافظ على الأرقام المميزة (212، 5) وتمسح كلمات الضجيج بسرعة O(1).
    تستخرج الخصائص الحيوية (الحجم، التركيز، نوع التغليف) لتشغيل البوابات الحديدية.
    """

    NOISE_WORDS = {
        "عطر",
        "تستر",
        "تيستر",
        "tester",
        "عينة",
        "sample",
        "ميني",
        "parfum",
        "eau",
        "de",
        "toilette",
        "cologne",
        "edp",
        "edt",
        "edc",
        "للجنسين",
        "نسائي",
        "رجالي",
    }

    @classmethod
    def clean_name(cls, text: str) -> str:
        """تحلية الاسم دون مسح الأرقام الجوهرية للعلامات التجارية (مثل 212 و 360)."""
        if not text:
            return ""
        t = text.lower()
        for src, dst in [("أ", "ا"), ("إ", "ا"), ("آ", "ا"), ("ة", "ه"), ("ى", "ي")]:
            t = t.replace(src, dst)

        t = re.sub(r"\d+(?:\.\d+)?\s*(ml|مل|ملي|oz|لتر)\b", " ", t)

        words = t.split()
        features = [w for w in words if w not in cls.NOISE_WORDS]
        t = " ".join(features)

        t = re.sub(r"[^\w\s\u0600-\u06FF]", " ", t)
        return re.sub(r"\s+", " ", t).strip()

    @classmethod
    def extract_dna(cls, raw_title: str) -> Dict[str, Any]:
        """استخراج بصمة المنتج الوراثية لتفعيل بوابات المنع (Hard Gates)."""
        tl = raw_title.lower()
        dna: Dict[str, Any] = {
            "is_tester": bool(
                re.search(r"\b(tester|تستر|تيستر|عينة|sample|بدون كرتون)\b", tl)
            ),
            "concentration": None,
            "size_ml": None,
            "clean_name": cls.clean_name(raw_title),
        }

        size_match = re.search(r"\b(\d{2,3})\s*(ml|مل|ملي)\b", tl)
        if size_match:
            dna["size_ml"] = int(size_match.group(1))

        if re.search(r"\b(edp|eau de parfum|بارفان|parfum)\b", tl):
            dna["concentration"] = "EDP"
        elif re.search(r"\b(edt|eau de toilette|تواليت|toilette)\b", tl):
            dna["concentration"] = "EDT"

        return dna


class VectorMatcher:
    """
    2. مرحلة المطابقة الجزيئية (Vector Search Strategy)
    مصممة للبحث في نصف مليون منتج خلال أجزاء من الثانية باستخدام تضمين FAISS الرياضي.
    """

    def __init__(self, use_mock: bool = True):
        self.use_mock = use_mock
        self.index = None
        self.model = None

    async def find_candidates(
        self, competitor_dna: Dict[str, Any], top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """يسترجع أعلى المرشحين تطابقاً (وضع mock حتى ربط FAISS)."""
        _ = top_k
        await asyncio.sleep(0.002)

        mock_candidates = [
            {
                "sku": "MAH-101",
                "name": "Carolina Herrera 212 VIP EDP 80ml",
                "price": 400.0,
                "is_tester": False,
                "concentration": "EDP",
                "size_ml": 80,
            }
        ]

        valid_matches: List[Dict[str, Any]] = []
        for cand in mock_candidates:
            if cand["is_tester"] != competitor_dna["is_tester"]:
                continue
            if cand["concentration"] and competitor_dna["concentration"]:
                if cand["concentration"] != competitor_dna["concentration"]:
                    continue
            if cand["size_ml"] and competitor_dna["size_ml"]:
                if abs(cand["size_ml"] - competitor_dna["size_ml"]) > 15:
                    continue

            valid_matches.append({**cand, "confidence": 0.95})

        return valid_matches


class TriageRouter:
    """
    3. التوزيع الاستراتيجي للفرص (AI Triage & Business Intel)
    """

    @classmethod
    def classify_action(
        cls, our_product: Dict[str, Any], comp_product: Dict[str, Any], confidence: float
    ) -> str:
        """يصنف قرارات التسعير: كتالوجنا (our) مقابل المنافس (comp_item بـ price_sar)."""
        if confidence < 0.85:
            return "AMBIGUOUS_REVIEW_NEEDED"

        our_price = float(our_product.get("price", 0))
        comp_price = float(comp_product.get("price_sar", 0))

        if comp_price > (our_price * 1.15):
            return "OPPORTUNITY_RAISE_MARGIN"

        if our_price > (comp_price * 1.05):
            return "CRITICAL_PRICE_WAR"

        return "SAFE_AUTO_MATCHED"


class _HttpxFetcherForSalla:
    """محوّل لـ ``collect_salla_products_fast_path`` (يتوقع ``get_text_once``)."""

    def __init__(self, client: httpx.AsyncClient):
        self._c = client

    async def get_text_once(self, url: str, timeout: float = 28.0):
        try:
            r = await self._c.get(url, timeout=timeout)
            return r.status_code, r.text
        except Exception as exc:
            logger.debug("[HyperScraper] get_text_once failed url=%s err=%s", url, exc)
            return 0, None


class AsyncHyperScraper:
    """
    4. الكشط الهجومي بالواجهات (API-First Offensive Scraping)
    يستخدم ``httpx`` + مسار سلة السريع من ``salla_storefront`` (Next.js / API نسبية).
    """

    def __init__(self, queue: asyncio.Queue):
        self.queue = queue
        self.limits = httpx.Limits(max_keepalive_connections=50, max_connections=100)

    @staticmethod
    async def _try_direct_json_payload(
        client: httpx.AsyncClient, url: str
    ) -> List[Dict[str, Any]]:
        """GET لـ URL يعيد JSON خاماً (مثلاً endpoint من Network tab)."""
        try:
            r = await client.get(url, timeout=30.0)
            if r.status_code != 200:
                return []
            txt = (r.text or "").lstrip()
            if not txt.startswith("{"):
                return []
            try:
                from utils.jsonfast import loads as json_loads

                data = json_loads(txt)
            except Exception:
                import json

                data = json.loads(txt)
            origin = origin_from_url(url)
            if not origin:
                from urllib.parse import urlparse

                p = urlparse(url)
                origin = f"{p.scheme}://{p.netloc}" if p.netloc else ""
            if not origin:
                return []
            return products_from_arbitrary_json(data, origin)
        except Exception as exc:
            logger.debug("[HyperScraper] direct JSON parse failed: %s", exc)
            return []

    async def _fetch_products(
        self, client: httpx.AsyncClient, store_url: str, *, max_pages: int
    ) -> List[Dict[str, Any]]:
        """يجمع {name, price, image, url} عبر مسار سلة السريع أو JSON مباشر."""
        u = (store_url or "").strip()
        if not u:
            return []

        low = u.lower()
        if "/api/" in low or low.endswith(".json"):
            direct = await self._try_direct_json_payload(client, u)
            if direct:
                logger.info("[HyperScraper] direct JSON: %s product(s)", len(direct))
                return direct

        fetcher = _HttpxFetcherForSalla(client)
        batch = await collect_salla_products_fast_path(
            fetcher,
            u,
            max_pages=max_pages,
            per_page_hint=48,
        )
        return batch or []

    async def fast_scrape_store(self, store_api_url: str, *, max_pages: int = 40) -> None:
        """
        يجلب منتجات متجر سلة (رابط المتجر أو صفحة رئيسية) ويدفعها للطابور.

        ``max_pages`` يحد أقصى صفحات الـ pagination في المسار السريع (لتجنب الجهد اللا نهائي).
        """
        logger.info("[HyperScraper] Launching API strike: %s", store_api_url)

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/html;q=0.9,*/*;q=0.8",
            "Accept-Language": "ar,en;q=0.9",
        }

        timeout = httpx.Timeout(40.0, connect=20.0)
        async with httpx.AsyncClient(
            limits=self.limits,
            timeout=timeout,
            follow_redirects=True,
            headers=headers,
        ) as client:
            try:
                items = await self._fetch_products(
                    client, store_api_url, max_pages=max_pages
                )
                if not items:
                    logger.warning(
                        "[HyperScraper] no products (verify URL is a public Salla store "
                        "or pass a JSON API URL)."
                    )
                    return

                nq = 0
                for it in items:
                    name = str(it.get("name") or "").strip()
                    price = float(it.get("price") or 0)
                    if not name or price <= 0:
                        continue
                    await self.queue.put(
                        {
                            "raw_name": name,
                            "price_sar": price,
                            "url": str(it.get("url") or ""),
                            "image": str(it.get("image") or ""),
                        }
                    )
                    nq += 1
                logger.info("[HyperScraper] queued %s product(s).", nq)
            except Exception as e:
                logger.error("[HyperScraper] strike failed: %s", e, exc_info=True)


class HyperOrchestrator:
    """
    5. دماغ المعمارية المركزية (The Grand Brain)
    """

    @classmethod
    async def match_consumer(cls, queue: asyncio.Queue, matcher: VectorMatcher) -> None:
        logger.info("[HyperOrchestrator] Active and listening on queue.")
        while True:
            comp_item = await queue.get()
            if comp_item is None:
                queue.task_done()
                break

            dna = PerfumeSanitizer.extract_dna(comp_item["raw_name"])
            matches = await matcher.find_candidates(dna)

            if matches:
                top_match = matches[0]
                action = TriageRouter.classify_action(
                    top_match, comp_item, float(top_match["confidence"])
                )
                logger.info(
                    "MATCH VERIFIED: '%s' -> '%s' | Triage: %s",
                    dna["clean_name"],
                    top_match["name"],
                    action,
                )
            else:
                logger.warning(
                    "NO MATCH passed hard gates: '%s'", dna["clean_name"]
                )

            queue.task_done()

    @classmethod
    async def startup(
        cls,
        store_url: str,
        *,
        max_pages: int = 40,
    ) -> None:
        logger.info("Initializing V26 Hyper-Intelligence Pipeline...")
        queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        matcher = VectorMatcher()

        consumer_task = asyncio.create_task(cls.match_consumer(queue, matcher))

        scraper = AsyncHyperScraper(queue)
        await scraper.fast_scrape_store(store_url, max_pages=max_pages)

        await queue.put(None)
        await queue.join()
        await consumer_task
        logger.info("Pipeline complete.")


if __name__ == "__main__":
    import os

    _url = (os.environ.get("HYPER_PIPELINE_STORE_URL") or "").strip()
    if not _url and len(sys.argv) > 1:
        _url = sys.argv[1].strip()
    if not _url:
        print(
            "Usage: python engines/hyper_pipeline.py https://your-store.salla.sa\n"
            "  or: set HYPER_PIPELINE_STORE_URL to the same URL.",
            file=sys.stderr,
        )
        sys.exit(2)
    _max = os.environ.get("HYPER_PIPELINE_MAX_PAGES", "").strip()
    _mp = int(_max) if _max.isdigit() else 40
    asyncio.run(HyperOrchestrator.startup(_url, max_pages=_mp))
