"""بصمة جدول المفقودات لتتبّع تغيّر العرض بعد تجهيز ملف سلة."""
from __future__ import annotations

import hashlib

import pandas as pd


def missing_df_fingerprint(edf: pd.DataFrame) -> str:
    try:
        return hashlib.sha256(edf.to_csv(index=False).encode("utf-8", errors="replace")).hexdigest()
    except Exception:
        return str(int(edf.shape[0]))
