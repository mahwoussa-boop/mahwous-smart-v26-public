"""
تحميل/حفظ pickle لملفات الجلسة تحت data/ فقط (تقليل خطر تحميل ملف خبيث من مسار غير متوقع).
"""
from __future__ import annotations

import logging
import os
import pickle
import threading
from typing import Any

logger = logging.getLogger("mahwous.session_pickle")

# قفل واحد لكل عمليات pickle تحت data/ (يتجنب قراءة نصف ملف أثناء الكتابة)
DATA_PICKLE_LOCK = threading.Lock()


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _trusted_data_dir() -> str:
    return os.path.realpath(os.path.join(_project_root(), "data"))


def resolve_trusted_pickle_path(path: str) -> str:
    """
    يحوّل المسار إلى مطلق ويتحقق أنه داخل مجلد data/ للمشروع.
    يفشل إذا حاول أحد تمرير مسار خارج data (مثل symlink أو ..).
    """
    if not path or not str(path).strip():
        raise ValueError("empty pickle path")
    if os.path.isabs(path):
        abs_path = os.path.realpath(path)
    else:
        abs_path = os.path.realpath(os.path.join(_project_root(), path))
    data_dir = _trusted_data_dir()
    try:
        common = os.path.commonpath([abs_path, data_dir])
    except ValueError:
        common = ""
    if common != data_dir:
        logger.error("رفض مسار pickle خارج data/: %s", abs_path)
        raise ValueError("pickle path must be under project data/ directory")
    return abs_path


def atomic_write_pickle(rel_or_abs: str, payload: Any) -> None:
    p = resolve_trusted_pickle_path(rel_or_abs)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    tmp = p + ".tmp"
    with DATA_PICKLE_LOCK:
        with open(tmp, "wb") as f:
            pickle.dump(payload, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)


def load_pickle(rel_or_abs: str) -> Any:
    p = resolve_trusted_pickle_path(rel_or_abs)
    with DATA_PICKLE_LOCK:
        with open(p, "rb") as f:
            return pickle.load(f)


def safe_remove(rel_or_abs: str) -> bool:
    """حذف ملف تحت data/ إن وُجد."""
    try:
        p = resolve_trusted_pickle_path(rel_or_abs)
    except ValueError:
        return False
    try:
        if os.path.isfile(p):
            os.remove(p)
        return True
    except OSError as e:
        logger.warning("تعذر حذف %s: %s", p, e)
        return False


def remove_pickle_and_tmp(rel_or_abs: str) -> None:
    """يحذف الملف والملف المؤقت .tmp المرتبط به (كتابة ذرية سابقة)."""
    p = resolve_trusted_pickle_path(rel_or_abs)
    with DATA_PICKLE_LOCK:
        for fp in (p, p + ".tmp"):
            try:
                if os.path.isfile(fp):
                    os.remove(fp)
            except OSError as e:
                logger.warning("تعذر حذف %s: %s", fp, e)
