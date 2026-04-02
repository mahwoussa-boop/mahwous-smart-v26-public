"""صفحة الذكاء الصناعي (Gemini) — مُستخرَجة من app.py."""
from __future__ import annotations

from datetime import datetime
from typing import Callable

import pandas as pd
import streamlit as st

from config import GEMINI_API_KEYS, GEMINI_MODEL
from engines.ai_engine import (
    USER_MSG_AI_UNAVAILABLE,
    analyze_paste,
    call_ai,
    fetch_fragrantica_info,
    gemini_chat,
    search_market_price,
    verify_match,
)


def render_ai_page(*, db_log: Callable[..., None]) -> None:
    db_log("ai", "view")

    # ── شريط الحالة ──
    if GEMINI_API_KEYS:
        st.markdown(f'''<div style="background:linear-gradient(90deg,#051505,#030d1f);
            border:1px solid #00C853;border-radius:10px;padding:10px 18px;
            margin-bottom:12px;display:flex;align-items:center;gap:10px;">
          <div style="width:10px;height:10px;border-radius:50%;background:#00C853;
                      box-shadow:0 0 8px #00C853;animation:pulse 2s infinite"></div>
          <span style="color:#00C853;font-weight:800;font-size:1rem">Gemini Flash — متصل مباشرة</span>
          <span style="color:#555;font-size:.78rem"> | {len(GEMINI_API_KEYS)} مفاتيح | {GEMINI_MODEL}</span>
        </div>''', unsafe_allow_html=True)
    else:
        st.warning(
            "لم يُضبط مفتاح API بعد — أضف **GEMINI_API_KEYS** في Streamlit Secrets أو متغيرات البيئة."
        )

    # ── سياق البيانات ──
    _ctx = []
    if st.session_state.results:
        _r = st.session_state.results
        _ctx = [
            f"المنتجات الكلية: {len(_r.get('all', pd.DataFrame()))}",
            f"سعر أعلى: {len(_r.get('price_raise', pd.DataFrame()))}",
            f"سعر أقل: {len(_r.get('price_lower', pd.DataFrame()))}",
            f"موافق: {len(_r.get('approved', pd.DataFrame()))}",
            f"مراجعة: {len(_r.get('review', pd.DataFrame()))}",
            f"مفقود: {len(_r.get('missing', pd.DataFrame()))}",
        ]
    _ctx_str = " | ".join(_ctx) if _ctx else "لم يتم تحليل بيانات بعد"

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "💬 دردشة مباشرة", "📋 لصق وتحليل", "🔍 تحقق منتج", "💹 بحث سوق", "📊 أوامر مجمعة"
    ])

    # ═══ TAB 1: دردشة Gemini مباشرة ═══════════
    with tab1:
        st.caption(f"📊 البيانات: {_ctx_str}")

        # صندوق المحادثة
        _chat_h = 430
        _msgs_html = ""
        if not st.session_state.chat_history:
            _msgs_html = """<div style="text-align:center;padding:60px 20px;color:#333">
              <div style="font-size:3rem">🤖</div>
              <div style="color:#666;margin-top:10px;font-size:1rem">Gemini Flash جاهز للمساعدة</div>
              <div style="color:#444;margin-top:6px;font-size:.82rem">
                اسأل عن الأسعار · المنتجات · توصيات التسعير · تحليل المنافسين
              </div>
            </div>"""
        else:
            for h in st.session_state.chat_history[-15:]:
                _msgs_html += f"""
                <div style="display:flex;justify-content:flex-end;margin:5px 0">
                  <div style="background:#1e1e3f;color:#B8B4FF;padding:8px 14px;
                              border-radius:14px 14px 2px 14px;max-width:82%;font-size:.88rem;
                              line-height:1.5">{h['user']}</div>
                </div>
                <div style="display:flex;justify-content:flex-start;margin:4px 0 10px 0">
                  <div style="background:#080f1e;border:1px solid #1a3050;color:#d0d0d0;
                              padding:10px 14px;border-radius:14px 14px 14px 2px;
                              max-width:88%;font-size:.88rem;line-height:1.65">
                    <span style="color:#00C853;font-size:.65rem;font-weight:700">
                      ● {h.get('source','Gemini')} · {h.get('ts','')}</span><br>
                    {h['ai'].replace(chr(10),'<br>')}
                  </div>
                </div>"""

        st.markdown(
            f'''<div style="background:#050b14;border:1px solid #1a3050;border-radius:12px;
                padding:14px;height:{_chat_h}px;overflow-y:auto;direction:rtl">
              {_msgs_html}
            </div>''', unsafe_allow_html=True)

        # إدخال
        _mc1, _mc2 = st.columns([5, 1])
        with _mc1:
            _user_in = st.text_input("اكتب رسالتك", key="gem_in",
                placeholder="اسأل Gemini — عن المنتجات، الأسعار، التوصيات...",
                label_visibility="collapsed")
        with _mc2:
            _send = st.button("➤ إرسال", key="gem_send", type="primary", use_container_width=True)

        # أزرار سريعة
        _qc = st.columns(4)
        _quick = None
        _quick_labels = [
            ("📉 أولويات الخفض", "بناءً على البيانات المحملة أعطني أولويات خفض الأسعار مع الأرقام"),
            ("📈 فرص الرفع", "حلّل فرص رفع الأسعار وأعطني توصية مرتبة"),
            ("🔍 أولويات المفقودات", "حلّل المنتجات المفقودة وأعطني أولويات الإضافة"),
            ("📊 ملخص شامل", f"أعطني ملخصاً تنفيذياً: {_ctx_str}"),
        ]
        for i, (lbl, q) in enumerate(_quick_labels):
            with _qc[i]:
                if st.button(lbl, key=f"q{i}", use_container_width=True):
                    _quick = q

        _msg_to_send = _quick or (_user_in if _send and _user_in else None)
        if _msg_to_send:
            _full = f"سياق البيانات: {_ctx_str}\n\n{_msg_to_send}"
            with st.spinner("جاري المعالجة..."):
                _res = gemini_chat(_full, st.session_state.chat_history)
            if _res["success"]:
                st.session_state.chat_history.append({
                    "user": _msg_to_send, "ai": _res["response"],
                    "source": _res.get("source","Gemini"),
                    "ts": datetime.now().strftime("%H:%M")
                })
                st.session_state.chat_history = st.session_state.chat_history[-40:]
                st.rerun()
            else:
                st.warning(_res["response"])

        _dc1, _dc2 = st.columns([4,1])
        with _dc2:
            if st.session_state.chat_history:
                if st.button("🗑️ مسح", key="clr_chat"):
                    st.session_state.chat_history = []
                    st.rerun()

    # ═══ TAB 2: لصق وتحليل ══════════════════════
    with tab2:
        st.markdown("**الصق منتجات أو بيانات أو أوامر — Gemini سيحللها فوراً:**")

        _paste = st.text_area(
            "الصق هنا:",
            height=200, key="paste_box",
            placeholder="""يمكنك لصق:
• قائمة منتجات من Excel (Ctrl+C ثم Ctrl+V)
• أوامر: "خفّض كل منتج فرقه أكثر من 30 ريال"
• CSV مباشرة
• أي نص تريد تحليله""")

        _pc1, _pc2 = st.columns(2)
        with _pc1:
            if st.button("🤖 تحليل بـ Gemini", key="paste_go", type="primary", use_container_width=True):
                if _paste:
                    # إضافة سياق البيانات الحالية
                    _ctx_data = ""
                    if st.session_state.results:
                        _r2 = st.session_state.results
                        _all = _r2.get("all", pd.DataFrame())
                        if not _all.empty and len(_all) > 0:
                            cols = [c for c in ["المنتج","السعر","منتج_المنافس","سعر_المنافس","القرار"] if c in _all.columns]
                            if cols:
                                _ctx_data = "\n\nعينة من بيانات التطبيق:\n" + _all[cols].head(15).to_string(index=False)
                    with st.spinner("🤖 Gemini يحلل..."):
                        _pr = analyze_paste(_paste, _ctx_data)
                    st.markdown(f'<div class="ai-box">{_pr["response"]}</div>', unsafe_allow_html=True)
        with _pc2:
            if st.button("📊 تحويل لجدول", key="paste_table", use_container_width=True):
                if _paste:
                    try:
                        import io as _io
                        _df_p = pd.read_csv(_io.StringIO(_paste), sep=None, engine='python')
                        st.dataframe(_df_p.head(200), use_container_width=True)
                        _csv_p = _df_p.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
                        st.download_button("📄 تحميل CSV", data=_csv_p,
                            file_name="pasted.csv", mime="text/csv", key="paste_dl")
                    except Exception:
                        st.warning("تعذر التحويل لجدول — جرب تنسيق CSV أو TSV")

    # ═══ TAB 3: تحقق منتج ══════════════════════
    with tab3:
        st.markdown("**تحقق من تطابق منتجين بدقة 100%:**")
        _vc1, _vc2 = st.columns(2)
        _vp1 = _vc1.text_input("🏷️ منتجنا:", key="v_our", placeholder="Dior Sauvage EDP 100ml")
        _vp2 = _vc2.text_input("🏪 المنافس:", key="v_comp", placeholder="ديور سوفاج بارفان 100 مل")
        _vc3, _vc4 = st.columns(2)
        _vpr1 = _vc3.number_input("💰 سعرنا:", 0.0, key="v_p1")
        _vpr2 = _vc4.number_input("💰 سعر المنافس:", 0.0, key="v_p2")
        if st.button("🔍 تحقق الآن", key="vbtn", type="primary"):
            if _vp1 and _vp2:
                with st.spinner("🤖 AI يتحقق..."):
                    _vr = verify_match(_vp1, _vp2, _vpr1, _vpr2)
                if _vr["success"]:
                    _mc = "#00C853" if _vr.get("match") else "#FF1744"
                    _ml = "✅ متطابقان" if _vr.get("match") else "❌ غير متطابقان"
                    st.markdown(f'''<div style="background:{_mc}22;border:1px solid {_mc};
                        border-radius:8px;padding:12px;margin:8px 0">
                      <div style="color:{_mc};font-weight:800;font-size:1.1rem">{_ml}</div>
                      <div style="color:#aaa;margin-top:4px">ثقة: <b>{_vr.get("confidence",0)}%</b></div>
                      <div style="color:#888;font-size:.88rem;margin-top:6px">{_vr.get("reason","")}</div>
                    </div>''', unsafe_allow_html=True)
                    if _vr.get("suggestion"):
                        st.info(f"💡 {_vr['suggestion']}")
                else:
                    st.warning(
                        _vr.get("reason") or USER_MSG_AI_UNAVAILABLE
                    )

    # ═══ TAB 4: بحث السوق ══════════════════════
    with tab4:
        st.markdown("**ابحث عن سعر السوق الحقيقي لأي منتج:**")
        _ms1, _ms2 = st.columns([3,1])
        with _ms1:
            _mprod = st.text_input("🔎 اسم المنتج:", key="mkt_prod",
                                    placeholder="Dior Sauvage EDP 100ml")
        with _ms2:
            _mcur = st.number_input("💰 سعرنا:", 0.0, key="mkt_price")

        if st.button("🌐 ابحث في السوق", key="mkt_btn", type="primary"):
            if _mprod:
                with st.spinner("🌐 جاري البحث في السوق..."):
                    _mr = search_market_price(_mprod, _mcur)
                if _mr.get("success"):
                    _mp = _mr.get("market_price", 0)
                    _rng = _mr.get("price_range", {})
                    _comps = _mr.get("competitors", [])
                    _rec = _mr.get("recommendation","")
                    _diff_v = _mp - _mcur if _mcur > 0 else 0
                    _diff_c = "#00C853" if _diff_v > 0 else "#FF1744" if _diff_v < 0 else "#888"

                    _src1, _src2 = st.columns(2)
                    with _src1:
                        st.metric("💹 سعر السوق", f"{_mp:,.0f} ر.س",
                                  delta=f"{_diff_v:+.0f} ر.س" if _mcur > 0 else None)
                    with _src2:
                        _mn = _rng.get("min",0); _mx = _rng.get("max",0)
                        st.metric("📊 نطاق السعر", f"{_mn:,.0f} - {_mx:,.0f} ر.س")

                    if _comps:
                        st.markdown("**🏪 منافسون في السوق:**")
                        for _c in _comps[:5]:
                            _cpv = float(_c.get("price",0))
                            _dv = _cpv - _mcur if _mcur > 0 else 0
                            st.markdown(
                                f"• **{_c.get('name','')}**: {_cpv:,.0f} ر.س "
                                f"({'أعلى' if _dv>0 else 'أقل'} بـ {abs(_dv):.0f}ر.س)" if _dv != 0 else
                                f"• **{_c.get('name','')}**: {_cpv:,.0f} ر.س"
                            )
                    if _rec:
                        st.markdown(f'<div class="ai-box">💡 {_rec}</div>', unsafe_allow_html=True)

        # صورة المنتج من Fragrantica
        with st.expander("🖼️ صورة ومكونات من Fragrantica Arabia", expanded=False):
            _fprod = st.text_input("اسم العطر:", key="frag_prod",
                                    placeholder="Dior Sauvage EDP")
            if st.button("🔍 ابحث في Fragrantica", key="frag_btn"):
                if _fprod:
                    with st.spinner("يجلب من Fragrantica Arabia..."):
                        _fi = fetch_fragrantica_info(_fprod)
                    if _fi.get("success"):
                        _fic1, _fic2 = st.columns([1,2])
                        with _fic1:
                            _img_url = _fi.get("image_url","")
                            if _img_url and _img_url.startswith("http"):
                                st.image(_img_url, width=240, caption=_fprod)
                            else:
                                st.markdown(f"[🔗 Fragrantica Arabia](https://www.fragranticarabia.com/?s={_fprod.replace(' ','+')})")
                        with _fic2:
                            _top = ", ".join(_fi.get("top_notes",[])[:5])
                            _mid = ", ".join(_fi.get("middle_notes",[])[:5])
                            _base = ", ".join(_fi.get("base_notes",[])[:5])
                            st.markdown(f"""
🌸 **القمة:** {_top or "—"}
💐 **القلب:** {_mid or "—"}
🌿 **القاعدة:** {_base or "—"}
📝 **{_fi.get('description_ar','')}**""")
                        if _fi.get("fragrantica_url"):
                            st.markdown(f"[🌐 صفحة العطر في Fragrantica]({_fi['fragrantica_url']})")
                    else:
                        st.info("لم يتم العثور على بيانات — تحقق من اسم العطر")

    # ═══ TAB 5: أوامر مجمعة ════════════════════
    with tab5:
        st.markdown("**نفّذ أوامر مجمعة على بياناتك:**")
        st.caption(f"📊 البيانات: {_ctx_str}")

        _cmd_section = st.selectbox(
            "اختر القسم:", ["الكل", "سعر أعلى", "سعر أقل", "موافق", "مراجعة", "مفقود"],
            key="cmd_sec"
        )
        _cmd_text = st.text_area(
            "الأمر أو السؤال:", height=120, key="cmd_area",
            placeholder="""أمثلة:
• حلّل المنتجات التي فرقها أكثر من 30 ريال وأعطني توصية
• رتّب المنتجات حسب الأولوية
• ما المنتجات التي تحتاج خفض سعر فوري؟
• أعطني ملخص مقارنة مع المنافسين"""
        )

        if st.button("⚡ تنفيذ الأمر", key="cmd_run", type="primary"):
            if _cmd_text and st.session_state.results:
                _sec_map = {
                    "سعر أعلى":"price_raise","سعر أقل":"price_lower",
                    "موافق":"approved","مراجعة":"review","مفقود":"missing"
                }
                _df_sec = None
                if _cmd_section != "الكل":
                    _k = _sec_map.get(_cmd_section)
                    _df_sec = st.session_state.results.get(_k, pd.DataFrame())
                else:
                    _df_sec = st.session_state.results.get("all", pd.DataFrame())

                if _df_sec is not None and not _df_sec.empty:
                    _cols = [c for c in ["المنتج","السعر","منتج_المنافس","سعر_المنافس","القرار","الفرق"] if c in _df_sec.columns]
                    _sample = _df_sec[_cols].head(25).to_string(index=False) if _cols else ""
                    _full_cmd = f"""البيانات ({_cmd_section}) - {len(_df_sec)} منتج:
{_sample}

الأمر: {_cmd_text}"""
                    with st.spinner("⚡ Gemini ينفذ الأمر..."):
                        _cr = call_ai(_full_cmd, "general")
                    st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)
                else:
                    with st.spinner("🤖"):
                        _cr = call_ai(f"{_ctx_str}\n\n{_cmd_text}", "general")
                    st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)
            elif _cmd_text:
                with st.spinner("🤖"):
                    _cr = call_ai(_cmd_text, "general")
                st.markdown(f'<div class="ai-box">{_cr["response"]}</div>', unsafe_allow_html=True)