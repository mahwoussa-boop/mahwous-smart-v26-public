"""صفحة منتجات المنافسين المفقودة — مُستخرَجة من app.py."""
from __future__ import annotations

from datetime import datetime
from html import escape as html_escape
from textwrap import dedent as _dedent
from typing import Callable

import pandas as pd
import streamlit as st

from config import MAKE_DOCS_SCENARIO_PRICING_AUTOMATION
from engines.ai_engine import (
    USER_MSG_AI_UNAVAILABLE,
    call_ai,
    check_duplicate,
    fetch_fragrantica_info,
    fetch_product_images,
    generate_mahwous_description,
    search_market_price,
    search_mahwous,
)
from engines.engine import extract_size
from engines.mahwous_core import ensure_export_brands, validate_export_product_dataframe
from styles import miss_card
from utils.db_manager import log_decision, save_hidden_product, save_processed
from utils.filter_ui import cached_filter_options
from utils.helpers import (
    export_missing_products_to_salla_csv_bytes,
    export_to_excel,
    make_salla_desc_fn,
    safe_float,
)
from utils.make_helper import export_to_make_format, send_batch_smart, send_new_products
from utils.missing_fingerprint import missing_df_fingerprint


def render_missing_products_page(*, db_log: Callable[..., None]) -> None:
    st.header("🔍 منتجات المنافسين غير الموجودة عندنا")
    db_log("missing", "view")

    if st.session_state.results and "missing" in st.session_state.results:
        df = st.session_state.results["missing"]
        if df is not None and not df.empty:
            # ── إحصاءات سريعة ──────────────────────────────────────────────
            total_miss   = len(df)
            has_tester   = df["نوع_متاح"].str.contains("تستر", na=False).sum()    if "نوع_متاح" in df.columns else 0
            has_base     = df["نوع_متاح"].str.contains("العطر الأساسي", na=False).sum() if "نوع_متاح" in df.columns else 0
            pure_missing = total_miss - has_tester - has_base

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("🔍 مفقود فعلاً",    pure_missing)
            c2.metric("🏷️ يوجد تستر",      has_tester)
            c3.metric("✅ يوجد الأساسي",   has_base)
            c4.metric("📦 إجمالي المنافسين", total_miss)

            # ── تحليل AI الأولويات ────────────────────────────────────────
            with st.expander("🤖 تحليل AI — أولويات الإضافة", expanded=False):
                if st.button("📡 تحليل الأولويات", key="ai_missing_section"):
                    with st.spinner("🤖 AI يحلل أولويات الإضافة..."):
                        _pure = df[df["نوع_متاح"].str.strip() == ""] if "نوع_متاح" in df.columns else df
                        _brands = _pure["الماركة"].value_counts().head(10).to_dict() if "الماركة" in _pure.columns else {}
                        _summary = " | ".join(f"{b}:{c}" for b,c in _brands.items()) if _brands else "غير محدد"
                        _lines   = "\n".join(
                            f"- {r.get('منتج_المنافس','')}: {safe_float(r.get('سعر_المنافس',0)):.0f}ر.س ({r.get('الماركة','')}) — {r.get('المنافس','')}"
                            for _, r in _pure.head(20).iterrows())
                        _prompt = (
                            f"لديّ {len(_pure)} منتج مفقود فعلاً (بدون التستر/الأساسي المتاح).\n"
                            f"توزيع الماركات: {_summary}\nعينة:\n{_lines}\n\n"
                            "أعطني:\n1. ترتيب أولويات الإضافة (عالية/متوسطة/منخفضة) مع السبب\n"
                            "2. أي الماركات الأكثر ربحية؟\n"
                            "3. سعر مقترح (أقل من المنافس بـ5-10 ر.س)\n"
                            "4. منتجات لا تستحق الإضافة — ولماذا؟"
                        )
                        r_ai = call_ai(_prompt, "missing_analysis")
                        resp = r_ai["response"] if r_ai["success"] else USER_MSG_AI_UNAVAILABLE
                        # تنظيف JSON من المخرجات
                        import re as _re
                        resp = _re.sub(r'```json.*?```', '', resp, flags=_re.DOTALL)
                        resp = _re.sub(r'```.*?```', '', resp, flags=_re.DOTALL)
                        st.markdown(f'<div class="ai-box">{resp}</div>', unsafe_allow_html=True)

            # ── فلاتر ─────────────────────────────────────────────────────
            opts = cached_filter_options(df)
            with st.expander("🔍 فلاتر", expanded=False):
                c1,c2,c3,c4,c5 = st.columns(5)
                search   = c1.text_input("🔎 بحث", key="miss_s")
                brand_f  = c2.selectbox("الماركة", opts["brands"], key="miss_b")
                comp_f   = c3.selectbox("المنافس", opts["competitors"], key="miss_c")
                variant_f= c4.selectbox("النوع",
                    ["الكل","مفقود فعلاً","يوجد تستر","يوجد الأساسي"], key="miss_v")
                conf_f   = c5.selectbox("الثقة",
                    ["الكل","🟢 مؤكد","🟡 محتمل","🔴 مشكوك"], key="miss_conf_f")

            filtered = df.copy()
            if search:
                filtered = filtered[filtered.apply(lambda r: search.lower() in str(r.values).lower(), axis=1)]
            if brand_f != "الكل" and "الماركة" in filtered.columns:
                filtered = filtered[filtered["الماركة"].str.contains(brand_f, case=False, na=False, regex=False)]
            if comp_f != "الكل" and "المنافس" in filtered.columns:
                filtered = filtered[filtered["المنافس"].str.contains(comp_f, case=False, na=False, regex=False)]
            if variant_f == "مفقود فعلاً" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.strip() == ""]
            elif variant_f == "يوجد تستر" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("تستر", na=False)]
            elif variant_f == "يوجد الأساسي" and "نوع_متاح" in filtered.columns:
                filtered = filtered[filtered["نوع_متاح"].str.contains("الأساسي", na=False)]
            # فلتر الثقة
            if conf_f != "الكل" and "مستوى_الثقة" in filtered.columns:
                _conf_map = {"🟢 مؤكد": "green", "🟡 محتمل": "yellow", "🔴 مشكوك": "red"}
                _cv = _conf_map.get(conf_f, "")
                if _cv:
                    filtered = filtered[filtered["مستوى_الثقة"] == _cv]

            # ── ترتيب حسب الثقة (الأكثر ثقة أولاً) ─────────────────────
            if "مستوى_الثقة" in filtered.columns:
                _conf_order = {"green": 0, "yellow": 1, "red": 2}
                filtered = filtered.assign(
                    _conf_sort=filtered["مستوى_الثقة"].map(_conf_order).fillna(3)
                ).sort_values("_conf_sort").drop(columns=["_conf_sort"])

            # ── تصدير + مدقق سلة صارم ────────────────────────────────────
            _export_df = filtered.copy()
            _dropped_zero = 0
            if "سعر_المنافس" in _export_df.columns:
                _before_n = len(_export_df)
                _export_df = _export_df[pd.to_numeric(_export_df["سعر_المنافس"], errors="coerce").fillna(0) > 0]
                _dropped_zero = max(0, _before_n - len(_export_df))
            # ملء الماركة الفارغة (استيراد سلة يتطلب عموداً غير فارغ)
            _export_df = ensure_export_brands(_export_df)
            _export_ok, _export_issues = validate_export_product_dataframe(_export_df)
            if _dropped_zero > 0:
                st.info(f"ℹ️ تم استبعاد {_dropped_zero} صف بسعر منافس غير صالح (<= 0) من التصدير فقط.")
            if not _export_ok:
                st.error("❌ التصدير معطل مؤقتاً: البيانات لا تطابق معايير سلة الصارمة:")
                for _iss in _export_issues[:25]:
                    st.warning(_iss)

            _salla_ai = st.checkbox(
                "🤖 وصف «خبير مهووس» بالذكاء الاصطناعي في ملف استيراد سلة (عمود الوصف HTML)",
                value=False,
                key="miss_salla_ai_desc",
                help="يستخرج مكونات الهرم العطري من الويب (Fragrantica عبر fetch_fragrantica_info) ثم يدمجها مع وصف AI. يُلحق قسماً مرجعياً بالمكونات في HTML. يستهلك رصيد API.",
            )
            _salla_ai_n = 500
            if _salla_ai:
                _salla_ai_n = int(
                    st.number_input(
                        "أقصى عدد منتجات يُوصَف بالذكاء الاصطناعي (الباقي قالب HTML ثابت)",
                        min_value=1,
                        max_value=2000,
                        value=min(500, max(1, len(_export_df) if _export_ok and len(_export_df) > 0 else 500)),
                        key="miss_salla_ai_n",
                        help="زر «تجهيز ملف سلة» يولّد وصف AI حتى هذا الحد، ثم قالب ثابت لباقي الصفوف.",
                    )
                )

            _miss_fp = missing_df_fingerprint(_export_df) if _export_ok and len(_export_df) > 0 else ""

            cc1, cc2, cc3, cc4 = st.columns(4)
            with cc1:
                if _export_ok:
                    excel_m = export_to_excel(_export_df, "مفقودة")
                    st.download_button("📥 Excel", data=excel_m, file_name="missing.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="miss_dl")
                else:
                    st.caption("📥 Excel — يتطلب إصلاح الأخطاء أعلاه")
            with cc2:
                if _export_ok:
                    _csv_m = _export_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                    st.download_button("📄 CSV", data=_csv_m, file_name="missing.csv", mime="text/csv", key="miss_csv")
                else:
                    st.caption("📄 CSV — يتطلب إصلاح الأخطاء أعلاه")
            with cc3:
                if _export_ok and len(_export_df) > 0:
                    st.markdown("**استيراد سلة**")
                    if st.button(
                        "⚙️ تجهيز ملف سلة (كل المنتجات المعروضة)",
                        key="miss_salla_prepare",
                        help="يبني ملف CSV جاهز للاستيراد في سلة ثم يمكنك تحميله من الزر التالي.",
                    ):
                        _n = len(_export_df)
                        _ai_n = min(_n, _salla_ai_n) if _salla_ai else 0
                        if _salla_ai and _ai_n > 60:
                            st.info(
                                f"ℹ️ سيتم توليد وصف AI لـ {_ai_n} منتجاً (من أصل {_n}) — قد يستغرق وقتاً ويستهلك رصيد API."
                            )
                        with st.spinner(f"جاري تجهيز {_n} منتجاً لملف سلة…"):
                            _salla_kw = {}
                            if _salla_ai and _ai_n > 0:
                                _salla_kw["generate_description"] = make_salla_desc_fn(True, _ai_n)
                            _blob = export_missing_products_to_salla_csv_bytes(_export_df, **_salla_kw)
                            st.session_state["missing_salla_csv_blob"] = _blob
                            st.session_state["missing_salla_csv_src_fp"] = _miss_fp
                        st.success(f"✅ تم تجهيز {_n} منتجاً — استخدم زر التحميل أدناه.")

                    _blob_ok = st.session_state.get("missing_salla_csv_blob")
                    _fp_saved = st.session_state.get("missing_salla_csv_src_fp")
                    if _blob_ok and _fp_saved == _miss_fp:
                        st.download_button(
                            "📥 تحميل ملف سلة CSV",
                            data=_blob_ok,
                            file_name="missing_salla_import.csv",
                            mime="text/csv; charset=utf-8",
                            key="miss_salla_csv_dl",
                            help="UTF-8 BOM — جاهز للاستيراد الجماعي في سلة.",
                        )
                    elif _blob_ok and _fp_saved != _miss_fp:
                        st.warning("⚠️ الفلاتر أو البيانات تغيّرت — اضغط «تجهيز» من جديد قبل التحميل.")
                else:
                    st.caption("🛒 سلة — يتطلب بيانات صالحة")
            with cc4:
                # ── خيارات الإرسال الذكي ─────────────────────────────
                st.caption(
                    f"📎 Webhook المفقودات = سيناريو [أتمتة التسعير]({MAKE_DOCS_SCENARIO_PRICING_AUTOMATION}) "
                    "(ليس سيناريو تعديل الأسعار 🔴🟢✅)."
                )
                _conf_opts = {"🟢 مؤكدة فقط": "green", "🟡 محتملة": "yellow", "🔵 الكل": ""}
                _conf_sel = st.selectbox("مستوى الثقة", list(_conf_opts.keys()), key="miss_conf_sel")
                _conf_val = _conf_opts[_conf_sel]
                if st.button("📤 إرسال بدفعات ذكية لـ Make", key="miss_make_all"):
                    _to_send = _export_df[_export_df["نوع_متاح"].str.strip() == ""] if "نوع_متاح" in _export_df.columns else _export_df
                    is_valid, issues = validate_export_product_dataframe(_to_send)
                    if not is_valid:
                        st.error("❌ تم إيقاف الإرسال! البيانات لا تطابق معايير سلة الصارمة:")
                        for issue in issues[:40]:
                            st.warning(issue)
                    else:
                        products = export_to_make_format(_to_send, "missing")
                        for _ip, _pr_row in enumerate(products):
                            if _ip < len(_to_send):
                                _pr_row["مستوى_الثقة"] = str(_to_send.iloc[_ip].get("مستوى_الثقة", "green"))
                        _prog_bar = st.progress(0, text="جاري الإرسال...")
                        _status_txt = st.empty()
                        def _miss_progress(sent, failed, total, cur_name):
                            pct = (sent + failed) / max(total, 1)
                            _prog_bar.progress(min(pct, 1.0), text=f"إرسال: {sent}/{total} | {cur_name}")
                            _status_txt.caption(f"✅ {sent} | ❌ {failed} | الإجمالي {total}")
                        res = send_batch_smart(products, batch_type="new",
                                               batch_size=20, max_retries=3,
                                               progress_cb=_miss_progress,
                                               confidence_filter=_conf_val)
                        _prog_bar.progress(1.0, text="اكتمل")
                        if res["success"]:
                            st.success(res["message"])
                            for _, _pr in _to_send.iterrows():
                                _pk = f"miss_{str(_pr.get('منتج_المنافس',''))[:30]}_{str(_pr.get('المنافس',''))}"
                                save_processed(_pk, str(_pr.get('منتج_المنافس','')),
                                             str(_pr.get('المنافس','')), "send_missing",
                                             new_price=safe_float(_pr.get('سعر_المنافس',0)))
                        else:
                            st.error(res["message"])
                        if res.get("errors"):
                            with st.expander(f"❌ منتجات فشلت ({len(res['errors'])})"):
                                for _en in res["errors"]:
                                    st.caption(f"• {_en}")

            st.caption(f"{len(filtered)} منتج — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
            st.caption(
                "📤 «إرسال Make» في كل بطاقة أدناه يذهب إلى **مفقودات** (سيناريو أتمتة التسعير) — "
                "وليس إلى تعديل أسعار 🔴🟢✅."
            )

            # ── عرض المنتجات ──────────────────────────────────────────────
            PAGE_SIZE = 20
            total_p = len(filtered)
            tp = max(1, (total_p + PAGE_SIZE - 1) // PAGE_SIZE)
            pn = st.number_input("الصفحة", 1, tp, 1, key="miss_pg") if tp > 1 else 1
            page_df = filtered.iloc[(pn-1)*PAGE_SIZE : pn*PAGE_SIZE]

            for row_i, (idx, row) in enumerate(page_df.iterrows()):
                name  = str(row.get("منتج_المنافس", ""))
                _row_slot = f"miss_p{pn}_r{row_i}"
                miss_price_key = f"input_price_{_row_slot}"
                _miss_key = f"missing_{name}_{idx}"
                if _miss_key in st.session_state.hidden_products:
                    continue

                price           = safe_float(row.get("سعر_المنافس", 0))
                brand           = str(row.get("الماركة", ""))
                comp            = str(row.get("المنافس", ""))
                size            = str(row.get("الحجم", ""))
                ptype           = str(row.get("النوع", ""))
                note            = str(row.get("ملاحظة", ""))
                _img_miss = str(row.get("صورة_المنافس", "") or row.get("image_url", "") or "").strip()
                variant_label   = str(row.get("نوع_متاح", ""))
                variant_product = str(row.get("منتج_متاح", ""))
                variant_score   = safe_float(row.get("نسبة_التشابه", 0))
                is_tester_flag  = bool(row.get("هو_تستر", False))
                conf_level      = str(row.get("مستوى_الثقة", "green"))
                conf_score      = safe_float(row.get("درجة_التشابه", 0))
                suggested_price = round(price - 1, 2) if price > 0 else 0

                _is_similar = "⚠️" in note
                _has_variant= bool(variant_label and variant_label.strip())
                _is_tester_type = "تستر" in variant_label if _has_variant else False

                # ── لون البطاقة حسب الحالة ────────────────────────────
                if _has_variant and _is_tester_type:
                    _border = "#ff980055"; _badge_bg = "#ff9800"
                elif _has_variant:
                    _border = "#4caf5055"; _badge_bg = "#4caf50"
                elif _is_similar:
                    _border = "#ff572255"; _badge_bg = "#ff5722"
                else:
                    _border = "#007bff44"; _badge_bg = "#007bff"

                # ── بادج النوع المتاح ──────────────────────────────────
                _variant_html = ""
                if _has_variant:
                    _variant_label_safe = html_escape(str(variant_label or ""))
                    _variant_product_safe = html_escape(str(variant_product or ""))
                    _variant_html = _dedent(
                        f"""
                        <div style="margin-top:6px;padding:5px 10px;border-radius:6px;
                                    background:{_badge_bg}22;border:1px solid {_badge_bg}88;
                                    font-size:.78rem;color:{_badge_bg};font-weight:700">
                            {_variant_label_safe}
                            <span style="font-weight:400;color:#aaa;margin-right:6px">
                                ({variant_score:.0f}%) → {_variant_product_safe[:50]}
                            </span>
                        </div>"""
                    ).strip()

                # ── بادج تستر ─────────────────────────────────────────
                _tester_badge = ""
                if is_tester_flag:
                    _tester_badge = '<span style="font-size:.68rem;padding:2px 7px;border-radius:10px;background:#9c27b022;color:#ce93d8;margin-right:6px">🏷️ تستر</span>'

                st.markdown(miss_card(
                    name=name, price=price, brand=brand, size=size,
                    ptype=ptype, comp=comp, suggested_price=suggested_price,
                    note=note if _is_similar else "",
                    variant_html=_variant_html, tester_badge=_tester_badge,
                    border_color=_border,
                    confidence_level=conf_level, confidence_score=conf_score,
                    image_url=_img_miss,
                ), unsafe_allow_html=True)

                _cpx, _ = st.columns([1, 5])
                with _cpx:
                    st.number_input(
                        "المقترح للإضافة (ر.س)",
                        value=float(suggested_price),
                        min_value=0.0,
                        step=1.0,
                        key=miss_price_key,
                        label_visibility="collapsed",
                        format="%.2f",
                    )

                # ── الأزرار — صف 1 ────────────────────────────────────
                b1,b2,b3,b4 = st.columns(4)

                with b1:
                    if st.button("🖼️ صور المنتج", key=f"imgs_{_row_slot}"):
                        with st.spinner("🔍 يبحث عن صور..."):
                            img_result = fetch_product_images(name, brand)
                            images = img_result.get("images", [])
                            frag_url = img_result.get("fragrantica_url","")
                            if images:
                                img_cols = st.columns(min(len(images),3))
                                for ci, img_data in enumerate(images[:3]):
                                    url = img_data.get("url",""); src = img_data.get("source","")
                                    is_search = img_data.get("is_search", False)
                                    with img_cols[ci]:
                                        if not is_search and url.startswith("http") and any(
                                            ext in url.lower() for ext in [".jpg",".png",".webp",".jpeg"]):
                                            try:    st.image(url, caption=f"📸 {src}", use_container_width=True)
                                            except: st.markdown(f"[🔗 {src}]({url})")
                                        else:
                                            st.markdown(f"[🔍 ابحث في {src}]({url})")
                                if frag_url:
                                    st.markdown(f"[🔗 Fragrantica Arabia]({frag_url})")
                            else:
                                st.warning("لم يتم العثور على صور")

                with b2:
                    if st.button("🌸 مكونات", key=f"notes_{_row_slot}"):
                        with st.spinner("يجلب من Fragrantica Arabia..."):
                            fi = fetch_fragrantica_info(name)
                            if fi.get("success"):
                                top  = ", ".join(fi.get("top_notes",[])[:5])
                                mid  = ", ".join(fi.get("middle_notes",[])[:5])
                                base = ", ".join(fi.get("base_notes",[])[:5])
                                st.markdown(f"""
**🌸 هرم العطر:**
- **القمة:** {top or "—"}
- **القلب:** {mid or "—"}
- **القاعدة:** {base or "—"}
- **الماركة:** {fi.get('brand','—')} | **السنة:** {fi.get('year','—')} | **العائلة:** {fi.get('fragrance_family','—')}""")
                                if fi.get("fragrantica_url"):
                                    st.markdown(f"[🔗 Fragrantica Arabia]({fi['fragrantica_url']})")
                                st.session_state[f"frag_info_{_row_slot}"] = fi
                            else:
                                st.warning("لم يتم العثور على بيانات")

                with b3:
                    if st.button("🔎 تحقق مهووس", key=f"mhw_{_row_slot}"):
                        with st.spinner("يبحث في mahwous.com..."):
                            r_m = search_mahwous(name)
                            if r_m.get("success"):
                                avail = "✅ متوفر" if r_m.get("likely_available") else "❌ غير متوفر"
                                resp_text = str(r_m.get("reason",""))[:200]
                                # تنظيف JSON
                                import re as _re
                                resp_text = _re.sub(r'\{.*?\}', '', resp_text, flags=_re.DOTALL)
                                st.info(f"{avail} | أولوية: **{r_m.get('add_recommendation','—')}**\n{resp_text}")
                            else:
                                st.warning("تعذر البحث")

                with b4:
                    if st.button("💹 سعر السوق", key=f"mkt_m_{_row_slot}"):
                        with st.spinner("🌐 يبحث في السوق..."):
                            r_s = search_market_price(name, price)
                            if r_s.get("success"):
                                mp  = r_s.get("market_price", 0)
                                rng = r_s.get("price_range", {})
                                rec = str(r_s.get("recommendation",""))[:200]
                                # تنظيف JSON من الرد
                                import re as _re
                                rec = _re.sub(r'```.*?```','', rec, flags=_re.DOTALL).strip()
                                mn  = rng.get("min",0); mx = rng.get("max",0)
                                _gap = mp - price if mp > price else 0
                                st.markdown(f"""
<div style="background:#0e1a2e;border:1px solid #4fc3f744;border-radius:8px;padding:10px;">
  <div style="font-weight:700;color:#4fc3f7">💹 سعر السوق: {mp:,.0f} ر.س</div>
  <div style="color:#888;font-size:.8rem">النطاق: {mn:,.0f} – {mx:,.0f} ر.س</div>
  {"<div style='color:#4caf50;font-size:.82rem'>💰 هامش: ~" + f"{_gap:,.0f} ر.س</div>" if _gap > 10 else ""}
  <div style="color:#aaa;font-size:.82rem;margin-top:6px">{rec}</div>
</div>""", unsafe_allow_html=True)

                # ── الأزرار — صف 2 ────────────────────────────────────
                st.markdown('<div style="margin-top:6px"></div>', unsafe_allow_html=True)
                b5,b6,b7,b8 = st.columns(4)

                with b5:
                    if st.button("✍️ خبير الوصف", key=f"expert_{_row_slot}", type="primary"):
                        with st.spinner("🤖 خبير مهووس يكتب الوصف الكامل..."):
                            fi_cached = st.session_state.get(f"frag_info_{_row_slot}")
                            if not fi_cached:
                                fi_cached = fetch_fragrantica_info(name)
                                st.session_state[f"frag_info_{_row_slot}"] = fi_cached
                            desc = generate_mahwous_description(name, suggested_price, fi_cached)
                            # تنظيف أي JSON عارض
                            import re as _re
                            desc = _re.sub(r'```json.*?```','', desc, flags=_re.DOTALL)
                            st.session_state[f"desc_{_row_slot}"] = desc
                            st.success("✅ الوصف جاهز!")

                    if f"desc_{_row_slot}" in st.session_state:
                        with st.expander("📄 الوصف الكامل — خبير مهووس", expanded=True):
                            edited_desc = st.text_area(
                                "راجع وعدّل الوصف قبل الإرسال:",
                                value=st.session_state[f"desc_{_row_slot}"],
                                height=400,
                                key=f"desc_edit_{_row_slot}"
                            )
                            st.session_state[f"desc_{_row_slot}"] = edited_desc
                            _wc = len(edited_desc.split())
                            _col = "#4caf50" if _wc >= 1000 else "#ff9800"
                            st.markdown(f'<span style="color:{_col};font-size:.8rem">📊 {_wc} كلمة</span>', unsafe_allow_html=True)

                with b6:
                    _has_desc = f"desc_{_row_slot}" in st.session_state
                    _make_lbl = "📤 إرسال Make + وصف" if _has_desc else "📤 إرسال Make"
                    if st.button(_make_lbl, key=f"mk_m_{_row_slot}", type="primary" if _has_desc else "secondary"):
                        _desc_send = st.session_state.get(
                            f"desc_edit_{_row_slot}",
                            st.session_state.get(f"desc_{_row_slot}", ""),
                        )
                        _fi_send    = st.session_state.get(f"frag_info_{_row_slot}",{})
                        _img_url    = _fi_send.get("image_url","") if _fi_send else ""
                        _size_val   = extract_size(name)
                        _size_str   = f"{int(_size_val)}ml" if _size_val else size
                        try:
                            _send_price = float(st.session_state.get(miss_price_key, suggested_price))
                        except (TypeError, ValueError):
                            _send_price = suggested_price
                        if _send_price <= 0:
                            _send_price = suggested_price
                        # إرسال مباشر سواء كان هناك وصف أم لا
                        with st.spinner("📤 يُرسل لـ Make..."):
                            res = send_new_products([{
                                "أسم المنتج":  name,
                                "سعر المنتج":  _send_price,
                                "brand":       brand,
                                "الوصف":       _desc_send,
                                "image_url":   _img_url,
                                "الحجم":       _size_str,
                                "النوع":       ptype,
                                "المنافس":     comp,
                                "سعر_المنافس": price,
                            }])
                        if res["success"]:
                            _wc = len(_desc_send.split()) if _desc_send else 0
                            _wc_msg = f" — وصف {_wc} كلمة" if _wc > 0 else ""
                            st.success(f"✅ {res['message']}{_wc_msg}")
                            _mk = f"missing_{name}_{idx}"
                            st.session_state.hidden_products.add(_mk)
                            save_hidden_product(_mk, name, "sent_to_make")
                            save_processed(_mk, name, comp, "send_missing",
                                           new_price=_send_price,
                                           notes=f"إضافة جديدة" + (f" + وصف {_wc} كلمة" if _wc > 0 else ""))
                            for k in [f"desc_{_row_slot}", f"frag_info_{_row_slot}", f"desc_edit_{_row_slot}"]:
                                if k in st.session_state: del st.session_state[k]
                            st.rerun()
                        else:
                            st.error(res["message"])

                with b7:
                    if st.button("🤖 تكرار؟", key=f"dup_{_row_slot}"):
                        with st.spinner("..."):
                            our_prods = []
                            if st.session_state.analysis_df is not None:
                                our_prods = st.session_state.analysis_df.get("المنتج", pd.Series()).tolist()[:50]
                            r_dup = check_duplicate(name, our_prods)
                            _dup_resp = str(r_dup.get("response",""))[:250]
                            # تنظيف JSON
                            import re as _re
                            _dup_resp = _re.sub(r'```.*?```','', _dup_resp, flags=_re.DOTALL).strip()
                            _dup_resp = _re.sub(r'\{[^}]{0,200}\}','[بيانات]', _dup_resp)
                            st.info(
                                _dup_resp
                                if r_dup.get("success")
                                else USER_MSG_AI_UNAVAILABLE
                            )

                with b8:
                    if st.button("🗑️ تجاهل", key=f"ign_{_row_slot}"):
                        log_decision(name,"missing","ignored","تجاهل",0,price,-price,comp)
                        _ign = f"missing_{name}_{idx}"
                        st.session_state.hidden_products.add(_ign)
                        save_hidden_product(_ign, name, "ignored")
                        save_processed(_ign, name, comp, "ignored",
                                       new_price=price,
                                       notes="تجاهل من قسم المفقودة")
                        st.rerun()

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:8px 0">', unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات مفقودة!")
    else:
        st.info("ارفع الملفات أولاً")

