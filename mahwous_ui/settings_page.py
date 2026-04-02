"""صفحة الإعدادات — مفاتيح، مطابقة، سجل قرارات، صف ماركة."""
from __future__ import annotations

from typing import Callable

import pandas as pd
import streamlit as st

from config import (
    HIGH_MATCH_SCORE,
    MAKE_DOCS_SCENARIO_PRICING_AUTOMATION,
    MAKE_DOCS_SCENARIO_UPDATE_PRICES,
    MIN_MATCH_SCORE,
    PRICE_DIFF_THRESHOLD,
    get_webhook_missing_products,
    get_webhook_update_prices,
)
from engines.ai_engine import USER_MSG_AI_UNAVAILABLE, call_ai
from utils.api_providers_ui import (
    infer_api_diag_summary,
    merged_api_summary,
    settings_api_card_html,
)
from utils.db_manager import get_decisions


def render_settings_page(
    *,
    db_log: Callable[..., None],
    ensure_make_webhooks_session: Callable[[], None],
) -> None:
    st.header("⚙️ الإعدادات")
    db_log("settings", "view")

    tab1, tab2, tab3, tab4 = st.tabs(["🔑 المفاتيح", "⚙️ المطابقة", "📜 السجل", "🏷️ صف ماركة جديدة"])

    with tab1:
        _ms = merged_api_summary()
        st.markdown("##### لوحة المزودين (لون = الحالة)")
        st.markdown(
            settings_api_card_html("Google Gemini", "✨", _ms.get("gemini", "unknown")),
            unsafe_allow_html=True,
        )
        st.markdown(
            settings_api_card_html("OpenRouter", "🔀", _ms.get("openrouter", "unknown")),
            unsafe_allow_html=True,
        )
        st.markdown(
            settings_api_card_html("Cohere", "◎", _ms.get("cohere", "unknown")),
            unsafe_allow_html=True,
        )
        _whp = "ok" if get_webhook_update_prices() else "absent"
        _whn = "ok" if get_webhook_missing_products() else "absent"
        _wh = "ok" if (_whp == "ok" or _whn == "ok") else "absent"
        st.markdown(
            settings_api_card_html("Make.com (Webhooks)", "🔗", _wh),
            unsafe_allow_html=True,
        )
        st.caption(
            "بعد «تشخيص شامل» تظهر هنا **فاتورة/رصيد منتهٍ (402)** و**تجاوز حد (429)** بألوان مميزة. "
            "بدون تشخيص: يُعرض وجود المفتاح فقط."
        )

        with st.expander("🔗 ربط Make.com — لصق روابط الـ Webhook (ليس رابط المشاركة العامة)", expanded=False):
            st.markdown(
                f"**أ)** **تعديل أسعار المنتجات الموجودة** — 🔴 سعر أعلى، 🟢 سعر أقل، ✅ موافق عليها فقط. "
                f"سيناريو مرجعي: [Integration Webhooks, Salla]({MAKE_DOCS_SCENARIO_UPDATE_PRICES})"
            )
            st.markdown(
                f"**ب)** **قسم المفقودات فقط** (القسم + بطاقة كل منتج مفقود). "
                f"سيناريو: [mahwous-pricing-automation-salla]({MAKE_DOCS_SCENARIO_PRICING_AUTOMATION})"
            )
            st.caption(
                "في Make: استنسخ السيناريو → **Custom Webhook** → انسخ `https://hook...` وليس رابط المشاركة. "
                "للإنتاج: `WEBHOOK_UPDATE_PRICES` و `WEBHOOK_MISSING_PRODUCTS` في Railway أو Secrets. "
                "المتغير القديم `WEBHOOK_NEW_PRODUCTS` ما زال يعمل كاحتياط لنفس رابط **ب**."
            )
            st.text_input(
                "WEBHOOK_UPDATE_PRICES — 🔴🟢✅ تعديل الأسعار",
                placeholder="https://hook.eu2.make.com/...",
                key="WEBHOOK_UPDATE_PRICES",
                help="يُستخدم لإرسال {\"products\": [...]} من أقسام سعر أعلى / أقل / موافق فقط.",
            )
            st.text_input(
                "WEBHOOK_MISSING_PRODUCTS — 🔍 مفقودات فقط (أتمتة التسعير)",
                placeholder="https://hook.eu2.make.com/...",
                key="WEBHOOK_MISSING_PRODUCTS",
                help='يُستخدم لقسم المفقودات وبطاقات الإرسال إلى Make فقط — Payload {"data": [...]}.',
            )
            if st.button("🔄 مزامنة الروابط مع الإرسال الآن", key="btn_sync_make_webhooks"):
                ensure_make_webhooks_session()
                st.success("تمت المزامنة — شريط «🔗 Make» في الشريط الجانبي يتحدّث بعد إعادة التحميل.")
                st.rerun()

        st.markdown("---")

        st.subheader("🔬 تشخيص AI")
        st.caption("يختبر الاتصال الفعلي بكل مزود ويُظهر الخطأ الحقيقي")

        if st.button("🔬 تشخيص شامل لجميع المزودين", type="primary"):
            with st.spinner("يختبر الاتصال بـ Gemini, OpenRouter, Cohere..."):
                from engines.ai_engine import diagnose_ai_providers

                diag = diagnose_ai_providers()
            _summ = infer_api_diag_summary(diag)
            _summ["_from_diag"] = True
            st.session_state["api_diag_summary"] = _summ

            st.markdown("**Gemini API:**")
            any_gemini_ok = False
            for g in diag.get("gemini", []):
                status = g["status"]
                if "✅" in status:
                    st.success(f"مفتاح {g['key']}: {status}")
                    any_gemini_ok = True
                elif "⚠️" in status:
                    st.warning(f"مفتاح {g['key']}: {status}")
                else:
                    st.error(f"مفتاح {g['key']}: {status}")

            or_res = diag.get("openrouter", "")
            st.markdown("**OpenRouter:**")
            if "✅" in or_res:
                st.success(or_res)
            elif "⚠️" in or_res:
                st.warning(or_res)
            else:
                st.error(or_res)

            co_res = diag.get("cohere", "")
            st.markdown("**Cohere:**")
            if "✅" in co_res:
                st.success(co_res)
            elif "⚠️" in co_res:
                st.warning(co_res)
            else:
                st.error(co_res)

            or_ok = "✅" in or_res
            co_ok = "✅" in co_res

            st.markdown("---")
            if any_gemini_ok or or_ok or co_ok:
                working = []
                if any_gemini_ok:
                    working.append("Gemini")
                if or_ok:
                    working.append("OpenRouter")
                if co_ok:
                    working.append("Cohere")
                st.success(f"✅ AI يعمل عبر: {' + '.join(working)}")
            else:
                st.warning(
                    "تعذّر إكمال الاتصال بأي مزود AI حالياً — راجع المفاتيح والشبكة أو جرّب من بيئة أخرى."
                )
                _all_errs = [g["status"] for g in diag.get("gemini", []) if "❌" in g.get("status", "")]
                if any("اتصال" in e or "ConnectionError" in e or "Pool" in e for e in _all_errs + [or_res, co_res]):
                    st.warning("""
**🔴 السبب المحتمل: Streamlit Cloud يحجب الطلبات الخارجية**

الحل: في صفحة تطبيقك على Streamlit Cloud:
1. اذهب إلى ⚙️ Settings → General
2. ابحث عن **"Network"** أو **"Egress"**
3. تأكد أن Outbound connections مسموح بها

أو جرب نشر التطبيق على **Railway** بدلاً من Streamlit Cloud.
                    """)
                elif any("403" in e or "IP" in e for e in _all_errs):
                    st.warning("🔴 مفاتيح Gemini محظورة من IP هذا الخادم — جرب OpenRouter")
                elif any("401" in e for e in _all_errs + [or_res, co_res]):
                    st.warning("🔴 مفتاح غير صحيح — تحقق من المفاتيح في Secrets")

        st.markdown("---")

        st.subheader("📋 آخر أخطاء AI")
        from engines.ai_engine import get_last_errors

        errs = get_last_errors()
        if errs:
            for e in errs:
                st.code(e, language=None)
        else:
            st.caption("لا أخطاء مسجلة بعد — جرب أي زر AI ثم ارجع هنا")

        st.markdown("---")

        if st.button("🧪 اختبار سريع"):
            with st.spinner("يتصل بـ AI..."):
                r = call_ai("أجب بكلمة واحدة فقط: يعمل", "general")
            if r["success"]:
                st.success(f"✅ AI يعمل عبر {r['source']}: {r['response'][:80]}")
            else:
                st.warning(
                    f"{USER_MSG_AI_UNAVAILABLE} — يمكنك تشغيل **تشخيص شامل** أدناه للتفاصيل."
                )
                from engines.ai_engine import get_last_errors as _gle

                for e in _gle()[:5]:
                    st.code(e, language=None)

    with tab2:
        st.info(f"حد التطابق الأدنى: {MIN_MATCH_SCORE}%")
        st.info(f"حد التطابق العالي: {HIGH_MATCH_SCORE}%")
        st.info(f"هامش فرق السعر: {PRICE_DIFF_THRESHOLD} ر.س")

    with tab3:
        decisions = get_decisions(limit=30)
        if decisions:
            df_dec = pd.DataFrame(decisions)
            st.dataframe(
                df_dec[
                    ["timestamp", "product_name", "old_status", "new_status", "reason", "competitor"]
                ]
                .rename(
                    columns={
                        "timestamp": "التاريخ",
                        "product_name": "المنتج",
                        "old_status": "من",
                        "new_status": "إلى",
                        "reason": "السبب",
                        "competitor": "المنافس",
                    }
                )
                .head(200),
                use_container_width=True,
            )
        else:
            st.info("لا توجد قرارات مسجلة")

    with tab4:
        from engines.brand_row_builder import (
            ai_fill_brand_seo_fields,
            brand_row_to_csv_bytes,
            build_brand_row,
            load_brands_csv_columns,
            slugify_seo_latin,
            suggest_logo_urls,
        )
        from engines.reference_data import BRANDS_CSV

        st.subheader("🏷️ تجهيز صف ماركة لـ brands.csv")
        st.caption(
            "عندما لا توجد الماركة في `data/brands.csv`، ولّد صفاً بنفس أعمدة الملف، وادمجها يدوياً أو عبر استيراد سلة. "
            "جلب الشعار: يُقترح رابط Clearbit أو أيقونة Google فقط عند إدخال **نطاق** الموقع (بدون API)."
        )
        _bcols = load_brands_csv_columns(BRANDS_CSV)
        st.caption(f"الأعمدة المقروءة من الملف: {len(_bcols)} عموداً.")

        _bn = st.text_input(
            "اسم الماركة (عربي | English)",
            placeholder="مثال: دار عطر جديدة | New House",
            key="new_brand_name_bilingual",
        )
        _ben = st.text_input(
            "مقطع SEO بالإنجليزية (اختياري — للعمود رابط الصفحة)",
            placeholder="new-house",
            key="new_brand_name_en_slug",
        )
        _dom = st.text_input(
            "موقع الماركة (نطاق أو رابط) لاقتراح شعار",
            placeholder="example.com",
            key="new_brand_domain",
        )
        _logo_manual = st.text_input(
            "أو الصق رابط شعار جاهز (CDN)",
            placeholder="https://...",
            key="new_brand_logo_url",
        )
        _use_ai = st.checkbox(
            "تعبئة الوصف وعنوان الصفحة ووصف الميتا بالذكاء الاصطناعي",
            value=False,
            key="new_brand_use_ai",
        )

        if st.button("🧩 تجميع صف ماركة", key="new_brand_build_btn"):
            if not (_bn or "").strip():
                st.warning("أدخل اسم الماركة أولاً.")
            else:
                _logo = (_logo_manual or "").strip()
                if not _logo and (_dom or "").strip():
                    _cands = suggest_logo_urls(_dom)
                    _logo = _cands[0] if _cands else ""
                    if _cands:
                        st.info(
                            f"مقترح شعار (جرّب الرابط؛ إن تعذّر التحميل استخدم البديل أو الصق رابطاً): `{_cands[0]}`"
                        )
                        if len(_cands) > 1:
                            st.caption(f"بديل: {_cands[1]}")

                _slug = (_ben or "").strip()
                if not _slug:
                    _part = _bn.split("|")[-1].strip() if "|" in _bn else _bn
                    _slug = slugify_seo_latin(_part)

                _short = ""
                _title = ""
                _pdesc = ""
                if _use_ai:
                    with st.spinner("جاري طلب الذكاء الاصطناعي…"):
                        _ai = ai_fill_brand_seo_fields(_bn.strip())
                    _short = _ai.get("وصف مختصر عن الماركة", "")
                    _title = _ai.get("(Page Title) عنوان صفحة العلامة التجارية", "")
                    _pdesc = _ai.get("(Page Description) وصف صفحة العلامة التجارية", "")
                    if not _ai:
                        st.warning("لم يُرجع AI حقولاً — املأ الوصف يدوياً أو جرّب لاحقاً.")

                if not _title:
                    _title = f"{_bn.split('|')[0].strip()} | عطور فاخرة — مهووس"[:120]

                _row = build_brand_row(
                    name_bilingual=_bn.strip(),
                    short_description=_short,
                    logo_url=_logo,
                    banner_url="",
                    page_title=_title,
                    seo_slug_latin=_slug,
                    page_description=_pdesc,
                    columns=_bcols,
                )
                st.session_state["new_brand_row_preview"] = _row
                st.session_state["new_brand_row_cols"] = _bcols

        _prev = st.session_state.get("new_brand_row_preview")
        _pcols = st.session_state.get("new_brand_row_cols")
        if _prev and isinstance(_prev, dict) and _pcols:
            st.dataframe(pd.DataFrame([_prev]), use_container_width=True)
            _blob = brand_row_to_csv_bytes(_pcols, _prev)
            st.download_button(
                "📥 تحميل صف ماركة (CSV لدمج)",
                data=_blob,
                file_name="brand_row_new.csv",
                mime="text/csv; charset=utf-8",
                key="dl_new_brand_row",
            )
            st.caption("افتح `data/brands.csv` في Excel، انسخ الصف الجديد تحت آخر صف، أو استورد الدفعة من لوحة سلة.")
