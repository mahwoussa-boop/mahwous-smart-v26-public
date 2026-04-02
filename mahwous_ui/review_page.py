"""صفحة تحت المراجعة — مُستخرَجة من app.py."""
from __future__ import annotations

from html import escape as html_escape
from typing import Callable

import pandas as pd
import streamlit as st

from utils.analysis_sections import split_analysis_results
from utils.helpers import export_to_excel, safe_float
from engines.ai_engine import (
    USER_MSG_AI_UNAVAILABLE,
    ai_deep_analysis,
    reclassify_review_items,
    verify_match,
)
from utils.db_manager import log_decision, save_hidden_product, save_processed


def render_review_page(*, db_log: Callable[..., None]) -> None:
    st.header("⚠️ منتجات تحت المراجعة — مطابقة غير مؤكدة")
    db_log("review", "view")

    if st.session_state.results and "review" in st.session_state.results:
        df = st.session_state.results["review"]
        if df is not None and not df.empty:
            st.warning(f"⚠️ {len(df)} منتج بمطابقة غير مؤكدة — يحتاج مراجعة بشرية أو AI")
            st.caption(
                "بعد كل جولة تحليل يُعاد تصنيف المراجعة تلقائياً عبر Gemini (ثقة ≥ 75%). "
                "الزر أدناه يعيد التشغيل يدوياً على أول 30 صفاً ويحدّث الجدول."
            )

            # ── تصنيف تلقائي بـ AI ────────────────────────────────────────
            col_r1, col_r2 = st.columns([2, 1])
            with col_r1:
                if st.button("🤖 إعادة تصنيف يدوي (أول 30)", type="primary", key="reclassify_review"):
                    with st.spinner("🤖 AI يعيد تصنيف المنتجات..."):
                        _items_rc = []
                        for _, rr in df.head(30).iterrows():
                            _items_rc.append({
                                "our":       str(rr.get("المنتج","")),
                                "comp":      str(rr.get("منتج_المنافس","")),
                                "our_price": safe_float(rr.get("السعر",0)),
                                "comp_price":safe_float(rr.get("سعر_المنافس",0)),
                            })
                        _rc_results = reclassify_review_items(_items_rc)
                        if _rc_results:
                            _moved = 0
                            adf = st.session_state.get("analysis_df")
                            if adf is not None and not getattr(adf, "empty", True):
                                for rc in _rc_results:
                                    _sec = str(rc.get("section", "") or "")
                                    try:
                                        _conf = float(rc.get("confidence", 0) or 0)
                                    except (TypeError, ValueError):
                                        _conf = 0.0
                                    if not _sec or "مراجعة" in _sec or _conf < 75:
                                        continue
                                    try:
                                        _ixi = int(rc.get("idx"))
                                    except (TypeError, ValueError):
                                        continue
                                    if _ixi < 1 or _ixi > len(_items_rc):
                                        continue
                                    _it = _items_rc[_ixi - 1]
                                    try:
                                        _mask = (
                                            adf["المنتج"].astype(str) == _it["our"]
                                        ) & (
                                            adf["منتج_المنافس"].astype(str) == _it["comp"]
                                        )
                                    except Exception:
                                        continue
                                    for _ri in adf.index[_mask]:
                                        if "مراجعة" in str(adf.at[_ri, "القرار"]):
                                            adf.at[_ri, "القرار"] = _sec
                                            _moved += 1
                                            break
                                st.session_state.analysis_df = adf
                                _r_new = split_analysis_results(adf)
                                _miss = st.session_state.results.get("missing")
                                if _miss is not None:
                                    _r_new["missing"] = _miss
                                st.session_state.results = _r_new
                                st.success(f"✅ حُدّث الجدول: نقل {_moved} صفاً بحسب Gemini")
                                st.rerun()
                            else:
                                for rc in _rc_results:
                                    _sec = rc.get("section", "")
                                    if _sec and "مراجعة" not in _sec and rc.get("confidence", 0) >= 75:
                                        _moved += 1
                                st.warning(
                                    f"اقتراحات AI: {_moved} — حمّل تحليلاً كاملاً من «رفع الملفات» لتطبيقها على الأقسام."
                                )
                        else:
                            st.warning("لم يتمكن AI من إعادة التصنيف")
            with col_r2:
                excel_rv = export_to_excel(df, "مراجعة")
                st.download_button("📥 Excel", data=excel_rv, file_name="review.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", key="rv_dl")

            # ── فلتر بحث ──────────────────────────────────────────────────
            search_rv = st.text_input("🔎 بحث في المنتجات", key="rv_search")
            df_rv = df.copy()
            if search_rv:
                df_rv = df_rv[df_rv.apply(lambda r: search_rv.lower() in str(r.values).lower(), axis=1)]

            st.caption(f"{len(df_rv)} منتج للمراجعة")

            # ── عرض المقارنة جنباً إلى جنب ────────────────────────────────
            PAGE_SIZE = 15
            tp = max(1, (len(df_rv) + PAGE_SIZE - 1) // PAGE_SIZE)
            pn = st.number_input("الصفحة", 1, tp, 1, key="rv_pg") if tp > 1 else 1
            page_rv = df_rv.iloc[(pn-1)*PAGE_SIZE : pn*PAGE_SIZE]

            for idx, row in page_rv.iterrows():
                our_name   = str(row.get("المنتج",""))
                comp_name  = str(row.get("منتج_المنافس","—"))
                our_price  = safe_float(row.get("السعر",0))
                comp_price = safe_float(row.get("سعر_المنافس",0))
                score      = safe_float(row.get("نسبة_التطابق",0))
                brand      = str(row.get("الماركة",""))
                size       = str(row.get("الحجم",""))
                comp_name_s= str(row.get("المنافس",""))
                diff       = our_price - comp_price
                our_img_rv = str(row.get("صورة_منتجنا", "") or "").strip()
                comp_img_rv = str(row.get("صورة_المنافس", "") or "").strip()

                _rv_key = f"review_{our_name}_{idx}"
                if _rv_key in st.session_state.hidden_products:
                    continue

                # لون الثقة
                _score_color = "#4caf50" if score >= 85 else "#ff9800" if score >= 70 else "#f44336"
                _diff_color  = "#f44336" if diff > 10 else "#4caf50" if diff < -10 else "#888"
                _diff_label  = f"+{diff:.0f}" if diff > 0 else f"{diff:.0f}"

                def _rv_img_tag(url: str, border_hex: str) -> str:
                    u = (url or "").strip()
                    if not u or u.lower() in ("nan", "none"):
                        return (
                            '<div style="min-height:76px;display:flex;align-items:center;justify-content:center;'
                            'color:#555;font-size:.62rem;border-radius:8px;background:#0a1424;border:1px dashed #333">لا صورة</div>'
                        )
                    eu = html_escape(u, quote=True)
                    return (
                        f'<div style="text-align:center;margin-bottom:6px">'
                        f'<img src="{eu}" alt="" style="width:76px;height:76px;max-width:100%;object-fit:cover;'
                        f'border-radius:10px;border:1px solid {border_hex};background:#0e1628" '
                        f'loading="lazy" referrerpolicy="no-referrer" '
                        f"onerror=\"this.style.display='none'\" />"
                        f"</div>"
                    )

                _on = html_escape(our_name[:120])
                _cn = html_escape(comp_name[:120])
                _bs = html_escape(brand)
                _sz = html_escape(size)
                _cs = html_escape(comp_name_s)

                # ── بطاقة المقارنة (مع صور) ─────────────────────────────────────
                st.markdown(f"""
                <div style="border:1px solid #ff980055;border-radius:10px;padding:12px;
                            margin:6px 0;background:linear-gradient(135deg,#0a1628,#0e1a30);">
                  <div style="display:flex;justify-content:space-between;margin-bottom:8px;">
                    <span style="font-size:.75rem;color:#888">🏷️ {_bs} | 📏 {_sz}</span>
                    <span style="font-size:.75rem;padding:2px 8px;border-radius:10px;
                                 background:{_score_color}22;color:{_score_color};font-weight:700">
                      نسبة المطابقة: {score:.0f}%
                    </span>
                  </div>
                  <div style="display:grid;grid-template-columns:1fr 60px 1fr;gap:10px;align-items:start">
                    <!-- منتجنا -->
                    <div style="background:#0d2040;border-radius:8px;padding:10px;border:1px solid #4fc3f733">
                      {_rv_img_tag(our_img_rv, "#4fc3f766")}
                      <div style="font-size:.65rem;color:#4fc3f7;margin-bottom:4px">📦 منتجنا</div>
                      <div style="font-weight:700;color:#fff;font-size:.88rem">{_on}</div>
                      <div style="font-size:1.1rem;font-weight:900;color:#4caf50;margin-top:6px">{our_price:,.0f} ر.س</div>
                    </div>
                    <!-- الفرق -->
                    <div style="text-align:center;padding-top:28px">
                      <div style="font-size:1.2rem;color:{_diff_color};font-weight:900">{_diff_label}</div>
                      <div style="font-size:.6rem;color:#555">ر.س</div>
                    </div>
                    <!-- منتج المنافس -->
                    <div style="background:#1a0d20;border-radius:8px;padding:10px;border:1px solid #ff572233">
                      {_rv_img_tag(comp_img_rv, "#ff572266")}
                      <div style="font-size:.65rem;color:#ff5722;margin-bottom:4px">🏪 {_cs}</div>
                      <div style="font-weight:700;color:#fff;font-size:.88rem">{_cn}</div>
                      <div style="font-size:1.1rem;font-weight:900;color:#ff9800;margin-top:6px">{comp_price:,.0f} ر.س</div>
                    </div>
                  </div>
                </div>""", unsafe_allow_html=True)

                # ── أزرار المراجعة ─────────────────────────────────────
                ba, bb, bc, bd, be, bf = st.columns(6)

                with ba:
                    if st.button("🤖 تحقق AI", key=f"rv_verify_{idx}"):
                        with st.spinner("..."):
                            r_v = verify_match(our_name, comp_name, our_price, comp_price)
                            if r_v.get("success"):
                                conf = r_v.get("confidence",0)
                                match = r_v.get("match", False)
                                reason = str(r_v.get("reason",""))[:200]
                                # تنظيف JSON
                                import re as _re
                                reason = _re.sub(r'```.*?```','', reason, flags=_re.DOTALL)
                                reason = _re.sub(r'\{[^}]{0,200}\}','', reason).strip()
                                _lbl = "✅ نفس المنتج" if match else "❌ مختلف"
                                st.info(f"**{_lbl}** ({conf}%)\n{reason[:150]}")
                            else:
                                st.warning(
                                    str(r_v.get("reason") or USER_MSG_AI_UNAVAILABLE)
                                )

                with bb:
                    if st.button("✅ موافق", key=f"rv_approve_{idx}"):
                        log_decision(our_name,"review","approved","موافق",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "approved_from_review")
                        save_processed(_rv_key, our_name, comp_name_s, "approved",
                                       old_price=our_price, new_price=our_price,
                                       notes="موافق من تحت المراجعة")
                        st.rerun()

                with bc:
                    if st.button("🔴 سعر أعلى", key=f"rv_raise_{idx}"):
                        log_decision(our_name,"review","price_raise","سعر أعلى",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "moved_price_raise")
                        save_processed(_rv_key, our_name, comp_name_s, "send_price",
                                       old_price=our_price, new_price=comp_price - 1 if comp_price > 0 else our_price,
                                       notes="نُقل من المراجعة → سعر أعلى")
                        st.rerun()

                with bd:
                    if st.button("🔵 مفقود", key=f"rv_missing_{idx}"):
                        log_decision(our_name,"review","missing","مفقود",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "moved_missing")
                        save_processed(_rv_key, our_name, comp_name_s, "send_missing",
                                       new_price=comp_price,
                                       notes="نُقل من المراجعة → مفقود")
                        st.rerun()

                with be:
                    if st.button("🗑️ تجاهل", key=f"rv_ign_{idx}"):
                        log_decision(our_name,"review","ignored","تجاهل",our_price,comp_price,diff,comp_name_s)
                        st.session_state.hidden_products.add(_rv_key)
                        save_hidden_product(_rv_key, our_name, "ignored_review")
                        save_processed(_rv_key, our_name, comp_name_s, "ignored",
                                       old_price=our_price,
                                       notes="تجاهل من تحت المراجعة")
                        st.rerun()

                with bf:
                    if st.button("🔬 عميق", key=f"rv_deep_{idx}"):
                        with st.spinner("🔬 تحليل عميق..."):
                            r_d = ai_deep_analysis(
                                our_name, our_price, comp_name, comp_price,
                                section="⚠️ تحت المراجعة", brand=brand,
                            )
                            if r_d.get("success"):
                                st.markdown(
                                    f'<div class="ai-box">{r_d.get("response", "")}</div>',
                                    unsafe_allow_html=True,
                                )
                            else:
                                st.warning(str(r_d.get("response", USER_MSG_AI_UNAVAILABLE)))

                st.markdown('<hr style="border:none;border-top:1px solid #0d1a2e;margin:6px 0">',
                            unsafe_allow_html=True)
        else:
            st.success("✅ لا توجد منتجات تحت المراجعة!")
    else:
        st.info("ارفع الملفات أولاً")