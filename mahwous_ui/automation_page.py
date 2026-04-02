"""صفحة الأتمتة الذكية — مُستخرَجة من app.py."""
from __future__ import annotations

from typing import Callable

import pandas as pd
import streamlit as st

from engines.automation import (
    AutomationEngine,
    ScheduledSearchManager,
    auto_process_review_items,
    auto_push_decisions,
    get_automation_log,
    get_automation_stats,
    log_automation_decision,
)
from utils.helpers import safe_float


def render_automation_page(
    *,
    db_log: Callable[..., None],
    merge_verified_review_into_session: Callable[[pd.DataFrame], int],
) -> None:
    st.header("🔄 الأتمتة الذكية — محرك القرارات التلقائية")
    db_log("automation", "view")

    # ── إنشاء محرك الأتمتة ──
    if "auto_engine" not in st.session_state:
        st.session_state.auto_engine = AutomationEngine()
    if "search_manager" not in st.session_state:
        st.session_state.search_manager = ScheduledSearchManager()

    engine = st.session_state.auto_engine
    search_mgr = st.session_state.search_manager

    tab_a1, tab_a2, tab_a3, tab_a4 = st.tabs([
        "🤖 تشغيل الأتمتة", "⚙️ قواعد التسعير", "🔍 البحث الدوري", "📊 سجل القرارات"
    ])

    # ── تاب 1: تشغيل الأتمتة ──
    with tab_a1:
        st.subheader("تطبيق القواعد التلقائية على نتائج التحليل")

        if st.session_state.results and st.session_state.analysis_df is not None:
            adf = st.session_state.analysis_df
            matched_df = adf[adf["نسبة_التطابق"].apply(lambda x: safe_float(x)) >= 85].copy()
            st.info(f"📦 {len(matched_df)} منتج مؤكد المطابقة جاهز للتقييم التلقائي")

            col_a, col_b = st.columns(2)
            with col_a:
                if st.button("🚀 تشغيل الأتمتة الآن", type="primary", key="run_auto"):
                    with st.spinner("⚙️ محرك الأتمتة يقيّم المنتجات..."):
                        engine.clear_log()
                        decisions = engine.evaluate_batch(matched_df)
                        st.session_state._auto_decisions = decisions

                        # تسجيل كل قرار في قاعدة البيانات
                        for d in decisions:
                            log_automation_decision(d)

                    if decisions:
                        summary = engine.get_summary()
                        c1, c2, c3, c4 = st.columns(4)
                        c1.metric("إجمالي القرارات", summary["total"])
                        c2.metric("⬇️ خفض سعر", summary["lower"])
                        c3.metric("⬆️ رفع سعر", summary["raise"])
                        c4.metric("✅ إبقاء", summary["keep"])
                        if summary.get("review", 0) > 0:
                            st.info(
                                f"⚠️ **{summary['review']}** قرار أُحيل لمراجعة يدوية "
                                "(خفض يتجاوز أقصى نزول آمن 25٪ — لن يُرسل تلقائياً إلى Make/سلة)."
                            )

                        if summary["net_impact"] > 0:
                            st.success(f"💰 الأثر المالي المتوقع: +{summary['net_impact']:.0f} ر.س (صافي ربح إضافي)")
                        elif summary["net_impact"] < 0:
                            st.warning(f"📉 الأثر المالي: {summary['net_impact']:.0f} ر.س (خفض لتحقيق التنافسية)")

                        # عرض القرارات في جدول
                        dec_df = pd.DataFrame(decisions)
                        display_cols = ["product_name", "action", "old_price", "new_price",
                                        "comp_price", "competitor", "match_score", "reason"]
                        available = [c for c in display_cols if c in dec_df.columns]
                        st.dataframe(dec_df[available].rename(columns={
                            "product_name": "المنتج", "action": "الإجراء",
                            "old_price": "السعر الحالي", "new_price": "السعر الجديد",
                            "comp_price": "سعر المنافس", "competitor": "المنافس",
                            "match_score": "نسبة التطابق", "reason": "السبب"
                        }), use_container_width=True)
                    else:
                        st.info("لم يتم اتخاذ أي قرارات — جميع الأسعار ضمن الهامش المقبول")

            with col_b:
                auto_decisions = st.session_state.get("_auto_decisions", [])
                push_eligible = [d for d in auto_decisions
                                 if d.get("action") in ("lower_price", "raise_price")
                                 and d.get("product_id")]
                if push_eligible:
                    st.warning(f"📤 {len(push_eligible)} قرار جاهز للإرسال إلى Make.com/سلة")
                    if st.button("📤 إرسال القرارات إلى Make.com", key="push_auto"):
                        with st.spinner("يُرسل إلى Make.com..."):
                            result = auto_push_decisions(auto_decisions)
                        if result.get("success"):
                            st.success(result["message"])
                        else:
                            st.error(result["message"])
                else:
                    st.caption("لا توجد قرارات جاهزة للإرسال — شغّل الأتمتة أولاً")

        else:
            st.warning("⚠️ لا توجد نتائج تحليل — ارفع الملفات أولاً من صفحة 'رفع الملفات'")

        # ── معالجة قسم المراجعة تلقائياً ──
        st.divider()
        st.subheader("🔄 معالجة قسم المراجعة تلقائياً")
        st.caption("يستخدم AI للتحقق المزدوج من المطابقات غير المؤكدة")

        if st.session_state.results and "review" in st.session_state.results:
            rev_df = st.session_state.results.get("review", pd.DataFrame())
            if not rev_df.empty:
                st.info(f"📋 {len(rev_df)} منتج تحت المراجعة")
                if st.button("🤖 تحقق AI تلقائي لقسم المراجعة", key="auto_review"):
                    with st.spinner("🤖 AI يتحقق من المطابقات..."):
                        confirmed = auto_process_review_items(rev_df.head(15))
                    if not confirmed.empty:
                        _n_applied = merge_verified_review_into_session(confirmed)
                        st.success(
                            f"✅ دُمج {_n_applied} صفاً في التحليل — {len(confirmed)} مؤكّداً من AI. انتقل للأقسام المحدَّثة."
                        )
                        st.dataframe(confirmed[["المنتج", "منتج_المنافس", "القرار"]].head(20),
                                     use_container_width=True)
                        st.rerun()
                    else:
                        st.info("لم يتم تأكيد أي مطابقة — المنتجات تحتاج مراجعة يدوية")
            else:
                st.success("لا توجد منتجات تحت المراجعة")

    # ── تاب 2: قواعد التسعير ──
    with tab_a2:
        st.subheader("⚙️ قواعد التسعير النشطة")
        st.caption("القواعد تُطبّق بالترتيب — أول قاعدة تنطبق تُنفَّذ")

        for i, rule in enumerate(engine.rules):
            with st.expander(f"{'✅' if rule.enabled else '⬜'} {rule.name}", expanded=False):
                st.write(f"**الإجراء:** {rule.action}")
                st.write(f"**حد التطابق الأدنى:** {rule.min_match_score}%")
                for k, v in rule.params.items():
                    if k not in ("name", "enabled", "action", "min_match_score", "condition"):
                        st.write(f"**{k}:** {v}")

        st.divider()
        st.subheader("📝 تخصيص القواعد")
        st.caption("يمكنك تعديل القواعد من ملف config.py → AUTOMATION_RULES_DEFAULT")
        st.code("""
# مثال: إضافة قاعدة جديدة في config.py
AUTOMATION_RULES_DEFAULT.append({
    "name": "خفض عدواني",
    "enabled": True,
    "action": "undercut",
    "min_diff": 5,
    "undercut_amount": 2,
    "min_match_score": 95,
    "max_loss_pct": 10,
})
        """, language="python")

    # ── تاب 3: البحث الدوري ──
    with tab_a3:
        st.subheader("🔍 البحث الدوري عن أسعار المنافسين")

        c1, c2 = st.columns(2)
        c1.metric("⏱️ البحث القادم", search_mgr.time_until_next())
        c2.metric("📊 آخر نتائج", f"{len(search_mgr.last_results)} منتج")

        if st.session_state.analysis_df is not None:
            scan_count = st.slider("عدد المنتجات للمسح", 5, 50, 15, key="scan_n")
            if st.button("🔍 مسح السوق الآن", type="primary", key="scan_now"):
                with st.spinner(f"يبحث عن أسعار {scan_count} منتج في السوق..."):
                    scan_results = search_mgr.run_scan(st.session_state.analysis_df, scan_count)
                if scan_results:
                    st.success(f"✅ تم مسح {len(scan_results)} منتج بنجاح")
                    for sr in scan_results[:10]:
                        md = sr.get("market_data", {})
                        rec = md.get("recommendation", md.get("market_price", "—"))
                        st.markdown(f"**{sr['product']}** — سعرنا: {sr['our_price']:.0f} | السوق: {rec}")
                else:
                    st.warning("لم يتم العثور على نتائج — تحقق من اتصال AI")
        else:
            st.warning("ارفع ملفات التحليل أولاً")

    # ── تاب 4: سجل القرارات ──
    with tab_a4:
        st.subheader("📊 سجل قرارات الأتمتة")
        days_filter = st.selectbox("الفترة", [7, 14, 30], index=0, key="auto_log_days")

        stats = get_automation_stats(days_filter)
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("إجمالي", stats["total"])
        c2.metric("خفض", stats["lower"])
        c3.metric("رفع", stats["raise"])
        c4.metric("أُرسل لـ Make", stats["pushed"])

        log_data = get_automation_log(limit=100)
        if log_data:
            log_df = pd.DataFrame(log_data)
            display = ["timestamp", "product_name", "action", "old_price",
                        "new_price", "competitor", "match_score", "pushed_to_make"]
            available = [c for c in display if c in log_df.columns]
            st.dataframe(log_df[available].rename(columns={
                "timestamp": "التاريخ", "product_name": "المنتج",
                "action": "الإجراء", "old_price": "السعر القديم",
                "new_price": "السعر الجديد", "competitor": "المنافس",
                "match_score": "التطابق%", "pushed_to_make": "أُرسل؟"
            }), use_container_width=True)
        else:
            st.info("لا توجد قرارات مسجلة بعد — شغّل الأتمتة من التاب الأول")

