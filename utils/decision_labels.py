# توحيد تسميات القرار بين verify_match والمحرك و_app (_split_results)
# نصوص القرار في النتائج يجب أن تحتوي: أعلى | أقل | موافق | مراجعة كما تتوقع دالة التقسيم.

UI_RAISE = "🔴 سعر أعلى"
UI_LOWER = "🟢 سعر أقل"
UI_APPROVED = "✅ موافق"
UI_REVIEW = "⚠️ تحت المراجعة"


def ui_decision_from_verify_section(correct_section: str, match: bool) -> str:
    """
    يحوّل correct_section القادم من verify_match (سعر اعلى / سعر اقل / موافق / مفقود ...)
    إلى نفس السلاسل التي يكتبها محرك المطابقة في عمود القرار.
    """
    if not match:
        return UI_REVIEW
    sec = correct_section or ""
    if "مفقود" in sec:
        return UI_REVIEW
    if "مراجعة" in sec or "تحت" in sec:
        return UI_REVIEW
    if "اعلى" in sec or "أعلى" in sec:
        return UI_RAISE
    if "اقل" in sec or "أقل" in sec:
        return UI_LOWER
    if "موافق" in sec:
        return UI_APPROVED
    return UI_REVIEW
