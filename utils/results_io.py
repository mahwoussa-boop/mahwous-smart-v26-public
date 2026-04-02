"""
تحويل نتائج التحليل لـ JSON آمن للتخزين واستعادتها.
"""
from __future__ import annotations

import json

import pandas as pd


def safe_results_for_json(results_list):
    """تحويل النتائج لصيغة آمنة للحفظ في JSON/SQLite — يحول القوائم المتداخلة"""
    safe = []
    for r in results_list:
        row = {}
        for k, v in (r.items() if isinstance(r, dict) else {}):
            if isinstance(v, list):
                try:
                    row[k] = json.dumps(v, ensure_ascii=False, default=str)
                except Exception:
                    row[k] = str(v)
            elif pd.isna(v) if isinstance(v, float) else False:
                row[k] = 0
            else:
                row[k] = v
        safe.append(row)
    return safe


def restore_results_from_json(results_list):
    """استعادة النتائج من JSON — يحول نصوص القوائم لقوائم فعلية"""
    restored = []
    for r in results_list:
        row = dict(r) if isinstance(r, dict) else {}
        for k in ["جميع_المنافسين", "جميع المنافسين"]:
            v = row.get(k)
            if isinstance(v, str):
                try:
                    row[k] = json.loads(v)
                except Exception:
                    row[k] = []
            elif v is None:
                row[k] = []
        restored.append(row)
    return restored
