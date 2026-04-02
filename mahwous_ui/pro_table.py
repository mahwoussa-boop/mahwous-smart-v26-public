"""جدول المقارنة البصري المشترك — مُستخرَج من app.py."""
from __future__ import annotations

from datetime import datetime
from typing import Callable, Any

import pandas as pd
import streamlit as st

from engines.ai_engine import (
    USER_MSG_AI_UNAVAILABLE,
    ai_deep_analysis,
    bulk_verify,
    search_market_price,
    verify_match,
)
from styles import comp_strip, vs_card
from .analysis_redistribute import apply_redistribute_analysis_row
from utils.analysis_sections import MANUAL_BUCKET_DECISION, RENDER_PREFIX_TO_BUCKET
from utils.db_manager import (
    get_price_history,
    log_decision,
    save_hidden_product,
    save_processed,
)
from utils.filter_ui import cached_filter_options
from utils.helpers import apply_filters, export_to_excel, safe_float
from utils.make_helper import (
    export_to_make_format,
    send_new_products,
    send_price_updates,
    send_single_product,
)


def ts_badge(ts_str: str = "") -> str:
    if not ts_str:
        ts_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    return (
        '<span style="font-size:.65rem;color:#555;background:#1a1a2e;padding:1px 6px;'
        'border-radius:8px;margin-right:4px">🕐 ' + ts_str + "</span>"
    )


def decision_badge(action: Any) -> str:
    colors = {
        "approved": ("#00C853", "✅ موافق"),
        "deferred": ("#FFD600", "⏸️ مؤجل"),
        "removed": ("#FF1744", "🗑️ محذوف"),
    }
    c, label = colors.get(action, ("#666", action))
    return f'<span style="font-size:.7rem;color:{c};font-weight:700">{label}</span>'


def render_pro_table(
    df,
    prefix,
    section_type="update",
    show_search=True,
    *,
    db_log: Callable[..., None],
):
    """
    جدول احترافي بصري مع:
    - فلاتر ذكية (أو بدون واجهة فلاتر عند ``show_search=False``)
    - أزرار AI + قرار لكل منتج
    - تصدير Make
    - Pagination

    ``show_search``: عند False تُخفى واجهة الفلاتر ويُعرض الجدول كاملاً دون تصفية.
    """
    if df is None or df.empty:
        st.info("لا توجد منتجات")
        return

    # ── فلاتر ─────────────────────────────────
    if show_search:
        opts = cached_filter_options(df)
        with st.expander("🔍 فلاتر متقدمة", expanded=False):
            c1, c2, c3, c4 = st.columns(4)
            search = c1.text_input("🔎 بحث", key=f"{prefix}_s")
            brand_f = c2.selectbox("🏷️ الماركة", opts["brands"], key=f"{prefix}_b")
            comp_f = c3.selectbox("🏪 المنافس", opts["competitors"], key=f"{prefix}_c")
            type_f = c4.selectbox("🧴 النوع", opts["types"], key=f"{prefix}_t")
            c5, c6, c7 = st.columns(3)
            match_min = c5.slider("أقل تطابق%", 0, 100, 0, key=f"{prefix}_m")
            price_min = c6.number_input("سعر من", 0.0, key=f"{prefix}_p1")
            price_max = c7.number_input("سعر لـ", 0.0, key=f"{prefix}_p2")

        filters = {
            "search": search,
            "brand": brand_f,
            "competitor": comp_f,
            "type": type_f,
            "match_min": match_min if match_min > 0 else None,
            "price_min": price_min if price_min > 0 else 0.0,
            "price_max": price_max if price_max > 0 else None,
        }
    else:
        filters = {
            "search": "",
            "brand": "الكل",
            "competitor": "الكل",
            "type": "الكل",
            "match_min": None,
            "price_min": 0.0,
            "price_max": None,
        }
    filtered = apply_filters(df, filters)

    # ── شريط الأدوات ───────────────────────────
    ac1, ac2, ac3, ac4, ac5 = st.columns(5)
    with ac1:
        _exdf = filtered.copy()
        if "جميع المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _exdf.columns: _exdf = _exdf.drop(columns=["جميع_المنافسين"])
        excel_data = export_to_excel(_exdf, prefix)
        st.download_button("📥 Excel", data=excel_data,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key=f"{prefix}_xl")
    with ac2:
        _csdf = filtered.copy()
        if "جميع المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع المنافسين"])
        if "جميع_المنافسين" in _csdf.columns: _csdf = _csdf.drop(columns=["جميع_المنافسين"])
        _csv_bytes = _csdf.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button("📄 CSV", data=_csv_bytes,
            file_name=f"{prefix}_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv", key=f"{prefix}_csv")
    with ac3:
        _bulk_labels = {"raise": "🤖 تحليل ذكي — خفض (أول 20)",
                        "lower": "🤖 تحليل ذكي — رفع (أول 20)",
                        "review": "🤖 تحقق جماعي (أول 20)",
                        "approved": "🤖 مراجعة (أول 20)"}
        if st.button(_bulk_labels.get(prefix, "🤖 AI جماعي (أول 20)"), key=f"{prefix}_bulk"):
            with st.spinner("🤖 AI يحلل البيانات..."):
                _section_map = {"raise": "price_raise", "lower": "price_lower",
                                "review": "review", "approved": "approved"}
                items = [{
                    "our": str(r.get("المنتج", "")),
                    "comp": str(r.get("منتج_المنافس", "")),
                    "our_price": safe_float(r.get("السعر", 0)),
                    "comp_price": safe_float(r.get("سعر_المنافس", 0))
                } for _, r in filtered.head(20).iterrows()]
                res = bulk_verify(items, _section_map.get(prefix, "general"))
                st.markdown(f'<div class="ai-box">{res["response"]}</div>',
                            unsafe_allow_html=True)
    with ac4:
        if st.button("📤 إرسال كل لـ Make", key=f"{prefix}_make_all"):
            products = export_to_make_format(filtered, section_type)
            if section_type in ("missing", "new"):
                res = send_new_products(products)
            else:
                res = send_price_updates(products)
            if res["success"]:
                st.success(res["message"])
                # v26: سجّل كل منتج في processed_products
                for _i, (_idx, _r) in enumerate(filtered.iterrows()):
                    _pname = str(_r.get("المنتج", _r.get("منتج_المنافس", "")))
                    _pkey  = f"{prefix}_{_pname}_{_i}"
                    _pid_r = str(_r.get("معرف_المنتج", _r.get("معرف_المنافس", "")))
                    _comp  = str(_r.get("المنافس",""))
                    _op    = safe_float(_r.get("السعر", _r.get("سعر_المنافس", 0)))
                    _np    = safe_float(_r.get("سعر_المنافس", _r.get("السعر", 0)))
                    st.session_state.hidden_products.add(_pkey)
                    save_hidden_product(_pkey, _pname, "sent_to_make_bulk")
                    save_processed(_pkey, _pname, _comp, "send_price",
                                   old_price=_op, new_price=_np,
                                   product_id=_pid_r,
                                   notes=f"إرسال جماعي ← {prefix}")
                st.rerun()
            else:
                st.error(res["message"])
    with ac5:
        # جمع القرارات المعلقة وإرسالها
        pending = {k: v for k, v in st.session_state.decisions_pending.items()
                   if v["action"] in ["approved", "deferred", "removed"]}
        if pending and st.button(f"📦 ترحيل {len(pending)} قرار → Make", key=f"{prefix}_send_decisions"):
            to_send = [{"name": k, "action": v["action"], "reason": v.get("reason", "")}
                       for k, v in pending.items()]
            res = send_price_updates(to_send)
            st.success(f"✅ تم إرسال {len(to_send)} قرار لـ Make")
            # v26: سجّل القرارات المعلقة في processed_products
            for k, v in pending.items():
                _pkey = f"decision_{k}"
                _act  = v.get("action","approved")
                save_processed(_pkey, k, v.get("competitor",""), _act,
                               old_price=safe_float(v.get("our_price",0)),
                               new_price=safe_float(v.get("comp_price",0)),
                               notes=f"قرار معلق → Make | {v.get('reason','')}")
            st.session_state.decisions_pending = {}
            st.rerun()

    st.caption(
        "📤 أزرار Make في هذا الجدول تُرسل إلى **تعديل الأسعار** (🔴 أعلى / 🟢 أقل / ✅ موافق) — "
        "وليس إلى سيناريو المفقودات."
    )
    st.caption(f"عرض {len(filtered)} من {len(df)} منتج — {datetime.now().strftime('%H:%M:%S')}")

    # ── Pagination ─────────────────────────────
    PAGE_SIZE = 50
    total_pages = max(1, (len(filtered) + PAGE_SIZE - 1) // PAGE_SIZE)
    _pg_key = f"{prefix}_pg"
    if _pg_key in st.session_state and int(st.session_state[_pg_key]) > total_pages:
        st.session_state[_pg_key] = total_pages
    if total_pages > 1:
        c_prev, c_num, c_next = st.columns([1, 3, 1])
        with c_prev:
            _cur = int(st.session_state.get(_pg_key, 1))
            if st.button("◀ السابق", key=f"{prefix}_pg_prev", disabled=_cur <= 1):
                st.session_state[_pg_key] = max(1, _cur - 1)
                st.rerun()
        with c_next:
            _cur = int(st.session_state.get(_pg_key, 1))
            if st.button("التالي ▶", key=f"{prefix}_pg_next", disabled=_cur >= total_pages):
                st.session_state[_pg_key] = min(total_pages, _cur + 1)
                st.rerun()
        with c_num:
            page_num = st.number_input("الصفحة", 1, total_pages, key=_pg_key)
    else:
        page_num = 1
    start = (page_num - 1) * PAGE_SIZE
    page_df = filtered.iloc[start:start + PAGE_SIZE]
    _deep_section_for_prefix = {
        "raise": "🔴 سعر أعلى",
        "lower": "🟢 سعر أقل",
        "review": "⚠️ تحت المراجعة",
        "approved": "✅ موافق",
    }.get(prefix, "⚠️ تحت المراجعة")

    # ── الجدول البصري ─────────────────────
    # row_i + page_num يضمنان مفاتيح session_state فريدة حتى مع تكرار index في DataFrame
    for row_i, (idx, row) in enumerate(page_df.iterrows()):
        price_input_key = f"input_price_{prefix}_p{page_num}_r{row_i}"
        our_name   = str(row.get("المنتج", "—"))
        # تخطي المنتجات التي أُرسلت لـ Make أو أُزيلت
        _hide_key = f"{prefix}_{our_name}_{idx}"
        if _hide_key in st.session_state.hidden_products:
            continue
        comp_name  = str(row.get("منتج_المنافس", "—"))
        our_price  = safe_float(row.get("السعر", 0))
        comp_price = safe_float(row.get("سعر_المنافس", 0))
        diff       = safe_float(row.get("الفرق", our_price - comp_price))
        match_pct  = safe_float(row.get("نسبة_التطابق", 0))
        comp_src   = str(row.get("المنافس", ""))
        brand      = str(row.get("الماركة", ""))
        size       = row.get("الحجم", "")
        ptype      = str(row.get("النوع", ""))
        risk       = str(row.get("الخطورة", ""))
        decision   = str(row.get("القرار", ""))
        ts_now     = datetime.now().strftime("%Y-%m-%d %H:%M")

        # سحب رقم المنتج من جميع الأعمدة المحتملة
        _pid_raw = (
            row.get("معرف_المنتج", "") or
            row.get("product_id", "") or
            row.get("رقم المنتج", "") or
            row.get("رقم_المنتج", "") or
            row.get("معرف المنتج", "") or ""
        )
        _pid_str = ""
        if _pid_raw and str(_pid_raw) not in ("", "nan", "None", "0"):
            try: _pid_str = str(int(float(str(_pid_raw))))
            except: _pid_str = str(_pid_raw)

        # بطاقة VS مع رقم المنتج
        our_img = str(row.get("صورة_منتجنا", "") or "")
        comp_img = str(row.get("صورة_المنافس", "") or "")
        st.markdown(vs_card(our_name, our_price, comp_name,
                            comp_price, diff, comp_src, _pid_str,
                            our_img=our_img, comp_img=comp_img),
                    unsafe_allow_html=True)

        # شريط المعلومات
        match_color = ("#00C853" if match_pct >= 90
                       else "#FFD600" if match_pct >= 70 else "#FF1744")
        risk_html = ""
        if risk:
            rc = {"حرج": "#FF1744", "عالي": "#FF1744", "متوسط": "#FFD600", "منخفض": "#00C853", "عادي": "#00C853"}.get(risk.replace("🔴 ","").replace("🟡 ","").replace("🟢 ",""), "#888")
            risk_html = f'<span style="color:{rc};font-size:.75rem;font-weight:700">⚡{risk}</span>'

        # تتبع سعر المنافس: أخضر ↓ خفض المنافس | أحمر ↑ رفع المنافس | رمادي ثابت
        ph = get_price_history(our_name, comp_src, limit=2)
        price_change_html = ""
        if len(ph) >= 2:
            try:
                old_p = float(ph[1]["price"])
                new_p = float(ph[0]["price"])
            except Exception:
                old_p = new_p = 0.0
            chg = new_p - old_p
            if abs(chg) < 0.02:
                price_change_html = (
                    '<span style="color:#9E9E9E;font-size:.7rem">⚪ سعر المنافس ثابت</span>'
                )
            elif chg > 0:
                price_change_html = (
                    f'<span style="color:#FF1744;font-size:.7rem;font-weight:700" title="فرصة رفع سعرك">'
                    f"🔴 سعر المنافس ↑ +{chg:.0f} ر.س</span>"
                )
            else:
                price_change_html = (
                    f'<span style="color:#00C853;font-size:.7rem;font-weight:700" title="المنافس خفض سعره">'
                    f"🟢 سعر المنافس ↓ {abs(chg):.0f} ر.س</span>"
                )
        elif len(ph) == 1:
            price_change_html = '<span style="color:#888;font-size:.65rem">أول رصد لسعر المنافس</span>'

        # قرار معلق؟
        pend = st.session_state.decisions_pending.get(our_name, {})
        pend_html = decision_badge(pend.get("action", "")) if pend else ""

        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;
                    padding:3px 12px;font-size:.8rem;flex-wrap:wrap;gap:4px;">
          <span>🏷️ <b>{brand}</b> {size} {ptype}</span>
          <span>تطابق: <b style="color:{match_color}">{match_pct:.0f}%</b></span>
          {risk_html}
          {price_change_html}
          {pend_html}
          {ts_badge(ts_now)}
        </div>""", unsafe_allow_html=True)

        # شريط المنافسين المصغر — يعرض كل المنافسين بأسعارهم
        all_comps = row.get("جميع_المنافسين", row.get("جميع المنافسين", []))
        if isinstance(all_comps, list) and len(all_comps) > 0:
            st.markdown(
                comp_strip(all_comps, our_price=our_price, rank_by_threat=True),
                unsafe_allow_html=True,
            )

        # ── أزرار لكل منتج ─────────────────────
        b1, b2, b3, b4, b5, b6, b7, b8, b9, b10 = st.columns([1, 1, 1, 1, 1, 1, 1, 1, 1, 1])

        with b1:  # AI تحقق ذكي — يُصحح القسم
            _ai_label = {"raise": "🤖 هل نخفض؟", "lower": "🤖 هل نرفع؟",
                         "review": "🤖 هل يطابق؟", "approved": "🤖 تحقق"}.get(prefix, "🤖 تحقق")
            if st.button(_ai_label, key=f"v_{prefix}_p{page_num}_r{row_i}"):
                with st.spinner("🤖 AI يحلل ويتحقق..."):
                    r = verify_match(our_name, comp_name, our_price, comp_price)
                    if r.get("success"):
                        icon = "✅" if r.get("match") else "❌"
                        conf = r.get("confidence", 0)
                        reason = r.get("reason","")[:200]
                        correct_sec = r.get("correct_section","")
                        suggested_price = r.get("suggested_price", 0)

                        # تحديد القسم الحالي من prefix
                        current_sec_map = {
                            "raise": "🔴 سعر أعلى",
                            "lower": "🟢 سعر أقل",
                            "approved": "✅ موافق",
                            "review": "⚠️ تحت المراجعة"
                        }
                        current_sec = current_sec_map.get(prefix, "")

                        # هل AI يوافق على القسم الحالي؟
                        section_ok = True
                        if correct_sec and current_sec:
                            # مقارنة مبسطة
                            if ("اعلى" in correct_sec or "أعلى" in correct_sec) and prefix != "raise":
                                section_ok = False
                            elif ("اقل" in correct_sec or "أقل" in correct_sec) and prefix != "lower":
                                section_ok = False
                            elif "موافق" in correct_sec and prefix != "approved":
                                section_ok = False
                            elif ("مفقود" in correct_sec or "🔵" in correct_sec) and r.get("match") == False:
                                section_ok = False

                        if r.get("match"):
                            # مطابقة صحيحة — عرض نتيجة السعر
                            diff_info = ""
                            if prefix == "raise":
                                diff_info = f"\n\n💡 **توصية:** {'خفض السعر' if diff > 20 else 'إبقاء السعر'}"
                            elif prefix == "lower":
                                diff_info = f"\n\n💡 **توصية:** {'رفع السعر' if abs(diff) > 20 else 'إبقاء السعر'}"
                            if suggested_price > 0:
                                diff_info += f"\n💰 **السعر المقترح: {suggested_price:,.0f} ر.س**"

                            st.success(f"{icon} **تطابق {conf}%** — المطابقة صحيحة\n\n{reason}{diff_info}")

                            if not section_ok:
                                st.warning(f"⚠️ AI يرى أن هذا المنتج يجب أن يكون في قسم: **{correct_sec}**")
                        else:
                            # مطابقة خاطئة — تنبيه
                            st.error(f"{icon} **المطابقة خاطئة** ({conf}%)\n\n{reason}")
                            st.warning("🔵 هذا المنتج يجب أن يكون في **المنتجات المفقودة**")
                    else:
                        st.warning(r.get("reason") or USER_MSG_AI_UNAVAILABLE)

        with b2:  # بحث سعر السوق ذكي
            _mkt_label = {"raise": "🌐 سعر عادل؟", "lower": "🌐 فرصة رفع؟"}.get(prefix, "🌐 سوق")
            if st.button(_mkt_label, key=f"mkt_{prefix}_p{page_num}_r{row_i}"):
                with st.spinner("🌐 يبحث في السوق السعودي..."):
                    r = search_market_price(our_name, our_price)
                    if r.get("success"):
                        mp  = r.get("market_price", 0)
                        rng = r.get("price_range", {})
                        rec = r.get("recommendation", "")[:250]
                        web_ctx = r.get("web_context","")
                        comps = r.get("competitors", [])
                        conf = r.get("confidence", 0)

                        _verdict = ""
                        if prefix == "raise" and mp > 0:
                            _verdict = "✅ سعرنا ضمن السوق" if our_price <= mp * 1.1 else "⚠️ سعرنا أعلى من السوق — يُنصح بالخفض"
                        elif prefix == "lower" and mp > 0:
                            _gap = mp - our_price
                            _verdict = f"💰 فرصة رفع ~{_gap:.0f} ر.س" if _gap > 10 else "✅ سعرنا قريب من السوق"

                        _comps_txt = ""
                        if comps:
                            _comps_txt = "\n\n**منافسون:**\n" + "\n".join(
                                f"• {c.get('name','')}: {c.get('price',0):,.0f} ر.س" for c in comps[:3]
                            )

                        _price_range = f"{rng.get('min',0):.0f}–{rng.get('max',0):.0f}" if rng else "—"
                        st.info(
                            f"💹 **سعر السوق: {mp:,.0f} ر.س** ({_price_range} ر.س)\n\n"
                            f"{rec}{_comps_txt}\n\n{'**' + _verdict + '**' if _verdict else ''}"
                        )
                        if web_ctx:
                            with st.expander("🔍 مصادر البحث"):
                                st.caption(web_ctx)
                    else:
                        st.warning("تعذر البحث في السوق")

        with b3:  # موافق
            if st.button("✅ موافق", key=f"ok_{prefix}_p{page_num}_r{row_i}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "approved", "reason": "موافقة يدوية",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "approved",
                             "موافقة يدوية", our_price, comp_price, diff, comp_src)
                _hk3 = f"{prefix}_{our_name}_{idx}"
                st.session_state.hidden_products.add(_hk3)
                save_hidden_product(_hk3, our_name, "approved")
                save_processed(_hk3, our_name, comp_src, "approved",
                               old_price=our_price, new_price=our_price,
                               product_id=str(row.get("معرف_المنتج","")),
                               notes=f"موافق من {prefix} | منافس: {comp_src}")
                st.rerun()

        with b4:  # تأجيل
            if st.button("⏸️ تأجيل", key=f"df_{prefix}_p{page_num}_r{row_i}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "deferred", "reason": "تأجيل",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "deferred",
                             "تأجيل", our_price, comp_price, diff, comp_src)
                st.warning("⏸️")

        with b5:  # إزالة
            if st.button("🗑️ إزالة", key=f"rm_{prefix}_p{page_num}_r{row_i}"):
                st.session_state.decisions_pending[our_name] = {
                    "action": "removed", "reason": "إزالة",
                    "our_price": our_price, "comp_price": comp_price,
                    "diff": diff, "competitor": comp_src,
                    "ts": datetime.now().strftime("%Y-%m-%d %H:%M")
                }
                log_decision(our_name, prefix, "removed",
                             "إزالة", our_price, comp_price, diff, comp_src)
                _hk = f"{prefix}_{our_name}_{idx}"
                st.session_state.hidden_products.add(_hk)
                save_hidden_product(_hk, our_name, "removed")
                save_processed(_hk, our_name, comp_src, "removed",
                               old_price=our_price, new_price=our_price,
                               product_id=str(row.get("معرف_المنتج","")),
                               notes=f"إزالة من {prefix}")
                st.rerun()

        with b6:  # سعر يدوي
            _auto_price = round(comp_price - 1, 2) if comp_price > 0 else our_price
            st.number_input(
                "سعر", value=_auto_price, min_value=0.0,
                step=1.0, key=price_input_key,
                label_visibility="collapsed"
            )

        with b7:  # تصدير Make
            if st.button("📤 Make", key=f"mk_{prefix}_p{page_num}_r{row_i}"):
                # سحب رقم المنتج من جميع الأعمدة المحتملة
                _pid_raw = (
                    row.get("معرف_المنتج", "") or
                    row.get("product_id", "") or
                    row.get("رقم المنتج", "") or
                    row.get("رقم_المنتج", "") or
                    row.get("معرف المنتج", "") or ""
                )
                # تحويل float إلى int (مثل 1081786650.0 → 1081786650)
                try:
                    _fv = float(_pid_raw)
                    _pid = str(int(_fv)) if _fv == int(_fv) else str(_pid_raw)
                except (ValueError, TypeError):
                    _pid = str(_pid_raw).strip()
                if _pid in ("nan", "None", "NaN", ""): _pid = ""
                try:
                    _raw_p = st.session_state.get(price_input_key)
                    _final_price = float(_raw_p) if _raw_p is not None else _auto_price
                except (TypeError, ValueError):
                    _final_price = _auto_price
                if _final_price <= 0:
                    _final_price = _auto_price
                res = send_single_product({
                    "product_id": _pid,
                    "name": our_name, "price": _final_price,
                    "comp_name": comp_name, "comp_price": comp_price,
                    "diff": diff, "decision": decision, "competitor": comp_src
                })
                if res["success"]:
                    _hk = f"{prefix}_{our_name}_{idx}"
                    st.session_state.hidden_products.add(_hk)
                    save_hidden_product(_hk, our_name, "sent_to_make")
                    save_processed(_hk, our_name, comp_src, "send_price",
                                   old_price=our_price, new_price=_final_price,
                                   product_id=_pid,
                                   notes=f"Make ← {prefix} | منافس: {comp_src} | {comp_price:.0f}→{_final_price:.0f}ر.س")
                    st.rerun()

        with b8:  # تحقق AI — يُصحح القسم
            if st.button("🔍 تحقق", key=f"vrf_{prefix}_p{page_num}_r{row_i}"):
                with st.spinner("🤖 يتحقق..."):
                    _vr2 = verify_match(our_name, comp_name, our_price, comp_price)
                    if _vr2.get("success"):
                        _mc2 = "✅ متطابق" if _vr2.get("match") else "❌ غير متطابق"
                        _conf2 = _vr2.get("confidence",0)
                        _sec2 = _vr2.get("correct_section","")
                        _reason2 = _vr2.get("reason","")[:150]
                        st.markdown(f"{_mc2} {_conf2}%\n\n{_reason2}")
                        if _sec2 and not _vr2.get("match"):
                            st.warning(f"يجب نقله → **{_sec2}**")

        with b9:  # تاريخ السعر
            if st.button("📈 تاريخ", key=f"ph_{prefix}_p{page_num}_r{row_i}"):
                history = get_price_history(our_name, comp_src)
                if history:
                    rows_h = [f"📅 {h['date']}: {h['price']:,.0f} ر.س" for h in history[:5]]
                    st.info("\n".join(rows_h))
                else:
                    st.info("لا يوجد تاريخ بعد")

        with b10:  # تحليل عميق (سوق + Gemini)
            if st.button("🔬 عميق", key=f"deep_{prefix}_p{page_num}_r{row_i}"):
                with st.spinner("🔬 تحليل عميق..."):
                    r_deep = ai_deep_analysis(
                        our_name, our_price, comp_name, comp_price,
                        section=_deep_section_for_prefix, brand=brand,
                    )
                    if r_deep.get("success"):
                        st.markdown(
                            f'<div class="ai-box">{r_deep.get("response", "")}</div>',
                            unsafe_allow_html=True,
                        )
                    else:
                        st.warning(str(r_deep.get("response", USER_MSG_AI_UNAVAILABLE)))

        _cur_buck = RENDER_PREFIX_TO_BUCKET.get(prefix)
        if _cur_buck and prefix in ("raise", "lower", "approved", "review"):
            _opts = [k for k in MANUAL_BUCKET_DECISION if k != _cur_buck]
            _lbl = {k: v for k, v in MANUAL_BUCKET_DECISION.items()}
            with st.expander("↩️ إعادة توزيع — تصحيح قسم الفرز", expanded=False):
                st.caption(
                    "إذا وضع المحرك المنتج في القسم الخطأ، اختر القسم الصحيح — يُحدَّث `القرار` في التحليل دون إعادة كشط."
                )
                _pick = st.selectbox(
                    "انقل إلى",
                    options=_opts,
                    format_func=lambda k: _lbl.get(k, k),
                    key=f"redist_pick_{prefix}_p{page_num}_r{row_i}",
                    label_visibility="collapsed",
                )
                if st.button(
                    "✓ تطبيق إعادة التوزيع",
                    key=f"redist_apply_{prefix}_p{page_num}_r{row_i}",
                ):
                    _ok_r, _err_r = apply_redistribute_analysis_row(
                        our_name, comp_name, _pick, log_event_fn=db_log
                    )
                    if not _ok_r:
                        st.error(_err_r)
                    else:
                        st.success(
                            f"✅ نُقل إلى **{_lbl.get(_pick, _pick)}** — انتقل للقسم من الشريط أو حدّث الصفحة."
                        )
                        st.rerun()

        st.markdown('<hr style="border:none;border-top:1px solid #1a1a2e;margin:6px 0">', unsafe_allow_html=True)
