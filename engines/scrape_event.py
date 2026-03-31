"""
عقد حدث كشط داخلي (نسخة 1) — جاهز لاحقاً لاستبدال الوسيط (مثل Redis Streams)
دون تغيير مسار CSV أو التحليل الحالي.

يُنشأ حدث واحد لكل منتج يُستخرج بنجاح من صفحة منافس.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 1
_ENV_NDJSON = "MAHWOUS_SCRAPE_EVENTS_NDJSON"
_DEFAULT_NDJSON_PATH = os.path.join("data", "scrape_events.ndjson")
_ndjson_lock = threading.Lock()


def utc_now_iso_z() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def build_scrape_event(
    *,
    competitor_id: str,
    source_url: str,
    name: str,
    price_sar: float,
    image_url: str = "",
    product_sku: str = "",
    availability_status: str = "IN_STOCK",
    extraction_confidence: float | None = None,
    proxy_used: str | None = None,
) -> dict[str, Any]:
    """
    يبني dict مطابقاً لمخطط الحمولة الاستراتيجي (مع schema_version إضافي للتطور).
    السكربر لا يضيف منطق أعمال — فقط حقول مستخرجة ومعرّفات.
    """
    meta: dict[str, Any] = {}
    if proxy_used:
        meta["proxy_used"] = str(proxy_used)
    if extraction_confidence is not None:
        meta["extraction_confidence"] = float(extraction_confidence)

    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "timestamp_utc": utc_now_iso_z(),
        "competitor_id": competitor_id,
        "source_url": source_url,
        "raw_product": {
            "name": name,
            "price_sar": float(price_sar),
            "availability_status": availability_status,
            "image_url": image_url or "",
            "product_sku": product_sku or "",
        },
        "scraper_metadata": meta,
    }


def validate_event(ev: Any) -> bool:
    """تحقق خفيف قبل إعادة التشغيل أو ربط وسيط خارجي."""
    if not isinstance(ev, dict):
        return False
    if int(ev.get("schema_version") or 0) != SCHEMA_VERSION:
        return False
    if not ev.get("event_id") or not ev.get("timestamp_utc"):
        return False
    rp = ev.get("raw_product")
    if not isinstance(rp, dict):
        return False
    if not rp.get("name"):
        return False
    try:
        float(rp.get("price_sar", 0))
    except (TypeError, ValueError):
        return False
    return True


def maybe_append_ndjson_event(
    ev: dict[str, Any],
    path: str | None = None,
) -> None:
    """
    إن فُعّل عبر MAHWOUS_SCRAPE_EVENTS_NDJSON=1|true|yes يُلحق سطر JSON واحد
    (للتصحيح، أو كمسار تدريجي قبل Redis). يُنشئ مجلد data تلقائياً.
    """
    raw = (os.environ.get(_ENV_NDJSON) or "").strip().lower()
    if raw not in ("1", "true", "yes", "on"):
        return
    out_path = path or _DEFAULT_NDJSON_PATH
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    line = json.dumps(ev, ensure_ascii=False) + "\n"
    with _ndjson_lock:
        with open(out_path, "a", encoding="utf-8") as f:
            f.write(line)
