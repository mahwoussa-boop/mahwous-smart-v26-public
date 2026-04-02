"""
إعداد logging موحّد للمشروع.
المستوى: MAHWOUS_LOG_LEVEL (افتراضي INFO) — DEBUG للتشخيص العميق.
"""
from __future__ import annotations

import logging
import os

_CONFIGURED = False


def configure_logging() -> None:
    """يُستدعى مرة واحدة عند بدء التطبيق (app.py)."""
    global _CONFIGURED
    if _CONFIGURED:
        return
    raw = (os.environ.get("MAHWOUS_LOG_LEVEL") or "INFO").strip().upper()
    level = getattr(logging, raw, logging.INFO)
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    try:
        logging.basicConfig(level=level, format=fmt, datefmt=datefmt, force=True)
    except TypeError:
        logging.basicConfig(level=level, format=fmt, datefmt=datefmt)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    _CONFIGURED = True
