"""
Weighted Threat Index (WTI) — ترتيب المنافسين حسب خطر السعر + الثقة + حداثة البيانات.
متوافق مع مفاتيح المحرك الحالية: price / score ويمكن تمرير comp_price / match_score / last_seen.
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import TYPE_CHECKING, Any, List, Mapping, MutableMapping, Optional, Union

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "rank_competitors_for_ui",
    "parse_last_seen",
    "DEFAULT_WEIGHT_PRICE",
    "DEFAULT_WEIGHT_CONFIDENCE",
    "DEFAULT_DECAY_LAMBDA",
]

DEFAULT_WEIGHT_PRICE = 0.7
DEFAULT_WEIGHT_CONFIDENCE = 0.3
DEFAULT_DECAY_LAMBDA = 0.1


def parse_last_seen(
    raw: Union[None, datetime, str, float, int],
    *,
    fallback: Optional[datetime] = None,
) -> datetime:
    """يحوّل last_seen إلى datetime؛ القيم غير المعروفة → fallback أو الآن."""
    fb = fallback if fallback is not None else datetime.now()
    if raw is None:
        return fb
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw))
        except (OSError, OverflowError, ValueError):
            return fb
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "null"):
        return fb
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt)
        except ValueError:
            continue
    return fb


def _comp_price(comp: Mapping[str, Any]) -> float:
    v = comp.get("comp_price", comp.get("price", 0))
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _match_score(comp: Mapping[str, Any]) -> float:
    v = comp.get("match_score", comp.get("score", 0))
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def rank_competitors_for_ui(
    competitors_list: Union[List[MutableMapping[str, Any]], "pd.DataFrame"],
    our_price: float,
    *,
    weight_price: float = DEFAULT_WEIGHT_PRICE,
    weight_confidence: float = DEFAULT_WEIGHT_CONFIDENCE,
    decay_lambda: float = DEFAULT_DECAY_LAMBDA,
    exclude_oos_from_threat: bool = True,
    now: Optional[datetime] = None,
) -> List[MutableMapping[str, Any]]:
    """
    يرتّب المنافسين حسب threat_score تنازلياً.

    يُحدّث كل عنصر بمفتاح ``threat_score`` (0–100 تقريباً).

    - **السعر**: يُعتبر تهديداً فقط إذا كان المنافس **أرخص** منّا (فرق إيجابي).
    - **الثقة**: من درجة المطابقة 0–100.
    - **الحداثة**: تضاؤل أسي حسب عمر ``last_seen`` (بالأيام).
    - **OOS**: إن ``out_of_stock`` / ``oos`` = True → threat = 0 (يُرتّب في الذيل).

    يقبل أيضاً ``pandas.DataFrame`` (يُحوَّل إلى سجلات ثم يُعاد كـ ``list[dict]``).
    """
    try:
        import pandas as pd
    except ImportError:
        pd = None  # type: ignore
    if pd is not None and isinstance(competitors_list, pd.DataFrame):
        if competitors_list.empty:
            return []
        competitors_list = competitors_list.to_dict("records")

    if not competitors_list:
        return competitors_list

    if our_price is None or float(our_price) <= 0:
        for c in competitors_list:
            c["threat_score"] = 0.0
        return competitors_list

    our_p = float(our_price)
    anchor = now or datetime.now()
    if anchor.tzinfo is not None:
        anchor = anchor.replace(tzinfo=None)

    for comp in competitors_list:
        if exclude_oos_from_threat and (
            comp.get("out_of_stock") is True or comp.get("oos") is True
        ):
            comp["threat_score"] = 0.0
            continue

        cp = _comp_price(comp)
        conf = max(0.0, min(100.0, _match_score(comp))) / 100.0
        ls = parse_last_seen(comp.get("last_seen"), fallback=anchor)
        if ls.tzinfo is not None:
            ls = ls.replace(tzinfo=None)

        days_old = max(0.0, (anchor - ls).total_seconds() / 86400.0)
        freshness = math.exp(-decay_lambda * days_old)

        price_diff_pct = (our_p - cp) / our_p
        price_factor = max(0.0, price_diff_pct)

        base = (weight_price * price_factor) + (weight_confidence * conf)
        comp["threat_score"] = round(base * freshness * 100.0, 2)

    return sorted(competitors_list, key=lambda x: float(x.get("threat_score", 0) or 0), reverse=True)


def _demo_verify() -> None:
    """سيناريوهات تحقق سريعة (تشغيل: python -m utils.threat_score)."""
    now = datetime(2026, 3, 31, 12, 0, 0)
    our = 400.0

    # أرخص قليلاً + تطابق عالٍ + بيانات ساعة → يجب أن يتفوق على أرخص بكثير لكن ضعيف/قديم
    a: List[MutableMapping[str, Any]] = [
        {
            "competitor": "A",
            "name": "p1",
            "comp_price": 390.0,
            "match_score": 96.0,
            "last_seen": datetime(2026, 3, 31, 11, 0, 0),
        },
        {
            "competitor": "B",
            "name": "p2",
            "price": 300.0,
            "score": 55.0,
            "last_seen": datetime(2026, 3, 20, 12, 0, 0),
        },
        {
            "competitor": "C",
            "name": "p3",
            "comp_price": 380.0,
            "match_score": 90.0,
            "out_of_stock": True,
        },
    ]
    out = rank_competitors_for_ui([dict(x) for x in a], our, now=now)
    assert out[0]["competitor"] == "A", out
    assert out[-1]["competitor"] == "C" and out[-1]["threat_score"] == 0.0
    print("threat_score demo OK:", [(x.get("competitor"), x.get("threat_score")) for x in out])


if __name__ == "__main__":
    _demo_verify()
