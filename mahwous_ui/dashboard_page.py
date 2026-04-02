"""صفحة لوحة التحكم — مُستخرَجة من app.py."""
from __future__ import annotations

from typing import Callable

import pandas as pd
import streamlit as st

from config import COLORS
from styles import stat_card
from utils.analysis_sections import split_analysis_results
from utils.db_manager import get_last_job, get_price_changes, load_all_comp_catalog_as_comp_dfs
from utils.helpers import export_multiple_sheets
from utils.make_helper import export_to_make_format, send_batch_smart
from utils.results_io import restore_results_from_json

from mahwous_ui.sidebar_nav import focus_sidebar_on_analysis_results


def render_dashboard_page(*, db_log: Callable[..., None]) -> None:
    st.header("📊 لوحة التحكم")
    db_log("dashboard", "view")

    changes = get_price_changes(7)
    if changes:
        st.markdown("#### 🔔 تغييرات أسعار آخر 7 أيام")
        c_df = pd.DataFrame(changes)
        st.dataframe(
            c_df[
                ["product_name", "competitor", "old_price", "new_price", "price_diff", "new_date"]
            ]
            .rename(
                columns={
                    "product_name": "المنتج",
                    "competitor": "المنافس",
                    "old_price": "السعر السابق",
                    "new_price": "السعر الجديد",
                    "price_diff": "التغيير",
                    "new_date": "التاريخ",
                }
            )
            .head(200),
            use_container_width=True,
            height=200,
        )
        st.markdown("---")

    if st.session_state.results:
        r = st.session_state.results
        cols = st.columns(5)
        data = [
            ("🔴", "سعر أعلى", len(r.get("price_raise", pd.DataFrame())), COLORS["raise"]),
            ("🟢", "سعر أقل", len(r.get("price_lower", pd.DataFrame())), COLORS["lower"]),
            ("✅", "موافق", len(r.get("approved", pd.DataFrame())), COLORS["approved"]),
            ("🔍", "مفقود", len(r.get("missing", pd.DataFrame())), COLORS["missing"]),
            ("⚠️", "مراجعة", len(r.get("review", pd.DataFrame())), COLORS["review"]),
        ]
        for col, (icon, label, val, color) in zip(cols, data):
            col.markdown(stat_card(icon, label, val, color), unsafe_allow_html=True)

        _miss_dash = r.get("missing", pd.DataFrame())
        if not _miss_dash.empty and "مستوى_الثقة" in _miss_dash.columns:
            _g = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "green"])
            _y = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "yellow"])
            _rd = len(_miss_dash[_miss_dash["مستوى_الثقة"] == "red"])
            st.markdown(
                f'<div style="display:flex;gap:12px;justify-content:center;padding:8px;'
                f'background:#1a1a2e;border-radius:8px;margin:8px 0">'
                f'<span style="color:#00C853">🟢 مؤكد: <b>{_g}</b></span>'
                f'<span style="color:#FFD600">🟡 محتمل: <b>{_y}</b></span>'
                f'<span style="color:#FF1744">🔴 مشكوك: <b>{_rd}</b></span>'
                f"</div>",
                unsafe_allow_html=True,
            )

        st.markdown("---")
        cc1, cc2 = st.columns(2)
        with cc1:
            sheets = {}
            for key, name in [
                ("price_raise", "سعر_أعلى"),
                ("price_lower", "سعر_أقل"),
                ("approved", "موافق"),
                ("missing", "مفقود"),
                ("review", "مراجعة"),
            ]:
                if key in r and not r[key].empty:
                    df_ex = r[key].copy()
                    if "جميع المنافسين" in df_ex.columns:
                        df_ex = df_ex.drop(columns=["جميع المنافسين"])
                    sheets[name] = df_ex
            if sheets:
                excel_all = export_multiple_sheets(sheets)
                st.download_button(
                    "📥 تصدير كل الأقسام Excel",
                    data=excel_all,
                    file_name="mahwous_all.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )
        with cc2:
            st.caption(
                "الدفعات: 🔴🟢✅ → تعديل أسعار؛ 🔍 مفقودة → Webhook المفقودات (أتمتة التسعير)."
            )
            if st.button("📤 إرسال كل شيء لـ Make (دفعات ذكية)"):
                _prog_all = st.progress(0, text="جاري الإرسال...")
                _status_all = st.empty()
                _sent_total = 0
                _fail_total = 0
                _sections = [
                    ("price_raise", "raise", "update", "🔴 سعر أعلى"),
                    ("price_lower", "lower", "update", "🟢 سعر أقل"),
                    ("approved", "approved", "update", "✅ موافق"),
                    ("missing", "missing", "new", "🔍 مفقودة"),
                ]
                for _si, (_key, _sec, _btype, _label) in enumerate(_sections):
                    if _key in r and not r[_key].empty:
                        _p = export_to_make_format(r[_key], _sec)
                        _res = send_batch_smart(_p, batch_type=_btype, batch_size=20, max_retries=3)
                        _sent_total += _res.get("sent", 0)
                        _fail_total += _res.get("failed", 0)
                        _status_all.caption(f"{_label}: ✅ {_res.get('sent',0)} | ❌ {_res.get('failed',0)}")
                    _prog_all.progress((_si + 1) / len(_sections), text=f"جاري: {_label}")
                _prog_all.progress(1.0, text="اكتمل")
                st.success(
                    f"✅ تم إرسال {_sent_total} منتج لـ Make!"
                    + (f" (فشل {_fail_total})" if _fail_total else "")
                )
    else:
        last = get_last_job()
        if last and last["status"] == "done" and last.get("results"):
            st.info(f"💾 يوجد تحليل محفوظ من {last.get('updated_at','')}")
            if st.button("🔄 استعادة النتائج المحفوظة"):
                _restored_last = restore_results_from_json(last["results"])
                df_all = pd.DataFrame(_restored_last)
                if not df_all.empty:
                    missing_df = (
                        pd.DataFrame(last.get("missing", [])) if last.get("missing") else pd.DataFrame()
                    )
                    _r = split_analysis_results(df_all)
                    _r["missing"] = missing_df
                    st.session_state.results = _r
                    st.session_state.analysis_df = df_all
                    try:
                        _cdf_last = load_all_comp_catalog_as_comp_dfs()
                        if _cdf_last:
                            st.session_state.comp_dfs = _cdf_last
                    except Exception:
                        pass
                    focus_sidebar_on_analysis_results(_r)
                    st.rerun()
        else:
            st.info("👈 ارفع ملفاتك من قسم 'رفع الملفات'")
