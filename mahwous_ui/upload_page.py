"""صفحة «رفع الملفات» — كشط الويب والتحليل — مُستخرَجة من app.py."""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

import pandas as pd
import streamlit as st

from sitemap_resolve import resolve_store_to_sitemap_url
from utils.competitor_entries import dedupe_competitor_entries, parse_competitor_bulk_entries
from utils.preset_competitors import load_preset_competitors
from utils.session_pickle import atomic_write_pickle


@dataclass
class UploadPageDeps:
    """تبعيات من app.py لتفادي استيراد دائري."""

    db_log: Callable[..., None]
    read_scrape_live_snapshot: Callable[[], dict]
    render_live_scrape_dashboard: Callable[[dict], None]
    hydrate_live_session_results_early: Callable[[], None]
    clear_scrape_live_snapshot: Callable[[], None]
    clear_live_session_pkl: Callable[[], None]
    merge_scrape_live_snapshot: Callable[..., None]
    run_scrape_chain_background: Callable[[], None]
    render_checkpoint_recovery_panel: Callable[[dict], None]
    comp_key_for_scrape_entry: Callable[..., str]
    scrape_bg_context_path: str
    ui_autorefresh_interval: Callable[[int], int]
    add_script_run_ctx: Callable[[threading.Thread], None]
    logger: logging.Logger


def render_upload_page(deps: UploadPageDeps) -> None:
    _read = deps.read_scrape_live_snapshot
    _snap_live = _read()
    if _snap_live.get("running") and not _snap_live.get("done"):
        with st.container(border=True):
            st.markdown("### 📡 مباشر — الكشط والتحليل على الدفعات")
            deps.render_live_scrape_dashboard(_snap_live)
        try:
            from streamlit_autorefresh import st_autorefresh

            st_autorefresh(
                interval=deps.ui_autorefresh_interval(2000),
                key="scrape_live_dashboard_refresh",
            )
        except ImportError:
            time.sleep(2)
            st.rerun()
        st.markdown("---")
    elif _snap_live.get("done"):
        deps.hydrate_live_session_results_early()
        if not _snap_live.get("success") and _snap_live.get("error"):
            st.error(f"❌ {_snap_live['error'][:400]}")
        else:
            st.success(
                "✅ انتهت مرحلة الكشط والتحضير — راقب **الشريط الجانبي** لاكتمال التحليل (Job) أو النتائج."
            )
        deps.clear_scrape_live_snapshot()
        deps.clear_live_session_pkl()
        st.rerun()

    st.header("🕸️ كشط الويب والتحليل")
    deps.db_log("upload", "view")
    deps.render_checkpoint_recovery_panel(_snap_live)
    our_path = os.path.join("data", "mahwous_catalog.csv")

    st.markdown("### 📦 كتالوج منتجاتنا")
    if os.path.isfile(our_path):
        st.caption("✅ الملف الحالي: `data/mahwous_catalog.csv` (يمكنك استبداله برفع جديد)")
    else:
        st.warning("⚠️ لم يُعثر على كتالوج المنتجات — ارفعه أولاً قبل بدء الكشط.")
    uploaded_catalog_main = st.file_uploader(
        "📂 ارفع ملف كتالوج منتجاتك (mahwous_catalog.csv)",
        type=["csv"],
        key="catalog_uploader_main",
    )
    if uploaded_catalog_main:
        try:
            os.makedirs("data", exist_ok=True)
            with open(our_path, "wb") as f:
                f.write(uploaded_catalog_main.read())
            _tmp_df = pd.read_csv(our_path)
            if "no" not in _tmp_df.columns:
                st.error("❌ الملف رُفع لكن عمود المعرف الإلزامي `no` غير موجود.")
            else:
                st.success(f"✅ تم حفظ الكتالوج بنجاح — عدد الصفوف: {len(_tmp_df):,}")
        except Exception as e:
            st.error(f"❌ تعذر حفظ/قراءة الكتالوج: {e}")

    if "scraper_urls" not in st.session_state:
        st.session_state.scraper_urls = ["https://worldgivenchy.com/ar/"]

    st.markdown("🔗 **روابط متاجر المنافسين (سلة أو زد)**")
    st.caption(
        "مضاد الحظر: يُفضَّل تثبيت `curl-cffi` و`playwright` ثم `playwright install chromium`. "
        "عند الحظر يُعاد البحث تلقائيًا عبر Chromium. يمكن أيضًا لصق رابط .xml أو استيراد CSV."
    )

    _presets_ui = load_preset_competitors()
    with st.expander("📌 **المنافسون المحفوظون** (ملف `data/preset_competitors.json`)", expanded=True):
        if not _presets_ui:
            st.warning(
                "تعذر تحميل القائمة. أنشئ أو راجع الملف: **`data/preset_competitors.json`** "
                "(مصفوفة JSON: `name`, `store_url`, `sitemap_url` لكل متجر)."
            )
        else:
            _plabels = [p["name"] for p in _presets_ui]
            _bp1, _bp2, _bp3 = st.columns([1, 1, 2])
            with _bp1:
                if st.button(
                    "✅ تحديد الكل",
                    key="preset_select_all_btn",
                    help=f"اختيار كل المنافسين ({len(_plabels)})",
                ):
                    st.session_state.scrape_preset_selection = list(_plabels)
                    st.rerun()
            with _bp2:
                if st.button("⏹️ مسح التحديد", key="preset_clear_btn"):
                    st.session_state.scrape_preset_selection = []
                    st.rerun()
            st.multiselect(
                "اختر منافساً واحداً أو عدة منافسين من القائمة",
                options=_plabels,
                key="scrape_preset_selection",
                help="يُدمج مع المربع المجمّع والحقول أدناه عند **بدء الكشط**. لمتجر واحد فقط اختر اسماً واحداً.",
            )
            st.caption(
                f"**{len(_presets_ui)}** متجر في القائمة — عدّل الملف لتغيير الروابط الدائمة دون تعديل الكود."
            )

    st.text_area(
        "روابط مجمّعة أو جدول منسوخ (Excel / Sheets)",
        key="bulk_competitor_urls",
        height=140,
        placeholder=(
            "سطر لكل متجر — إما رابط فقط، أو ثلاثة أعمدة مفصولة بـ Tab:\n"
            "الاسم العربي\thttps://المتجر/\thttps://المتجر/sitemap.xml"
        ),
        help=(
            "يدعم لصق جدول ثلاثي الأعمدة: اسم المنافس، رابط المتجر، رابط sitemap.xml "
            "— يُستخدم الـ sitemap مباشرة دون بحث. يُدمج مع حقول «متجر 1، 2…»."
        ),
    )
    st.caption(
        "**عدة متاجر:** يُكشط كل متجر **بالتسلسل** ويُسجَّل في الكتالوج تحت مفتاحه، مع **Preview مترافق لكل متجر** "
        "أثناء الجلب، ثم يعمل **تحليل نهائي موحّد** يشمل جميع المنافسين في النهاية."
    )
    for i in range(len(st.session_state.scraper_urls)):
        st.caption(f"متجر {i+1}")
        st.text_input(
            "رابط",
            key=f"comp_url_{i}",
            placeholder="https://worldgivenchy.com/ar/",
            label_visibility="collapsed",
        )

    if st.button("➕ إضافة متجر آخر"):
        st.session_state.scraper_urls.append("")
        st.rerun()

    st.text_input(
        "اسم المنافس للعرض (في البطاقات والجداول)",
        key="competitor_display_name",
        placeholder="مثال: عالم جيفنشي — يُشتق تلقائياً من نطاق الرابط إذا تُرك فارغاً",
        help="يُمرَّر إلى المحرك وعمود «المنافس» بدل الاسم البرمجي. عند الفراغ يُستخدم النطاق من الرابط (مثل worldgivenchy.com).",
    )

    col_opt1, col_opt2, col_opt3 = st.columns(3)
    with col_opt1:
        scrape_bg = st.checkbox(
            "🌐 كشط في الخلفية (التنقل أثناء الكشط)",
            value=False,
            help="يُكمِل الجلب في خيط؛ مع حفظ CSV وتحديث كتالوج المنافس على دفعات، ومسار تحليل مترافق داخل الخيط. بعد الانتهاء يُحفظ التحليل كـ job.",
        )
    with col_opt2:
        pipeline_inline = st.checkbox(
            "⚡ تحليل مترافق مع الكشط (مطابقة أثناء الجلب — أسرع للنهاية)",
            value=True,
            disabled=scrape_bg,
            help="للكشط على الصفحة فقط: مطابقة على لقطات تراكمية أثناء الجلب ثم جولة نهائية. الكشط الخلفي يفعّل مساراً مماثلاً تلقائياً.",
        )
    with col_opt3:
        max_rows = st.number_input("حد الصفوف للمعالجة (0=كل)", 0, step=500)

    st.caption(
        "بعد انتهاء الكشط يُجدول **التحليل** تلقائياً (Job في الشريط الجانبي) — يمكنك التنقل أثناء التحليل."
    )
    st.caption(
        "💾 **دفعات أثناء الكشط:** يُحدَّث `data/competitors_latest.csv` وكتالوج المنافس كلّما تجاوز العدد "
        "`SCRAPER_INCREMENTAL_EVERY` (أو نفس خطوة المسار المترافق `SCRAPER_PIPELINE_EVERY`). "
        "المسار المترافق يُعيد المطابقة على **جميع** المنتجات المكسوبة حتى تلك اللحظة — دون انتظار نهاية الكشط."
    )

    pipeline_inline = bool(pipeline_inline) and (not scrape_bg)

    _snap_busy = _read()
    _scrape_busy = _snap_busy.get("running") and not _snap_busy.get("done")

    if st.button("🚀 بدء الكشط والتحليل", type="primary", disabled=_scrape_busy):
        entries: list[dict] = []
        _preset_map = {p["name"]: p for p in load_preset_competitors()}
        for _pname in st.session_state.get("scrape_preset_selection") or []:
            _pr = _preset_map.get(_pname)
            if _pr:
                entries.append(
                    {
                        "label": _pr["name"],
                        "store_url": str(_pr.get("store_url") or ""),
                        "sitemap_url": str(_pr.get("sitemap_url") or ""),
                    }
                )
        entries.extend(
            parse_competitor_bulk_entries(
                str(st.session_state.get("bulk_competitor_urls") or "")
            )
        )
        for i in range(len(st.session_state.scraper_urls)):
            v = (st.session_state.get(f"comp_url_{i}") or "").strip()
            if not v:
                continue
            if not v.startswith(("http://", "https://")):
                v = "https://" + v.lstrip("/")
            entries.append({"label": "", "store_url": v, "sitemap_url": None})
        entries = dedupe_competitor_entries(entries)
        if not entries:
            st.warning(
                "⚠️ اختر منافساً من **المنافسين المحفوظين**، أو أدخل رابطاً في الحقول / المربع المجمّع."
            )
        else:
            resolved_triples: list[tuple[str, str, str]] = []
            prog_resolve = st.progress(0, "🔍 جاري تجهيز خرائط المواقع...")
            n_entries = len(entries)

            for i, e in enumerate(entries):
                label = str(e.get("label") or "")
                store = str(e.get("store_url") or "").strip()
                sm_direct = str(e.get("sitemap_url") or "").strip()
                hint = (label[:28] + "…") if len(label) > 28 else (label or store[:48])
                prog_resolve.progress(
                    (i) / max(n_entries, 1),
                    f"({i + 1}/{n_entries}) {hint}",
                )
                if sm_direct.startswith(("http://", "https://")):
                    src = store if store.startswith(("http://", "https://")) else sm_direct
                    resolved_triples.append((label, src, sm_direct))
                    continue
                if store.startswith(("http://", "https://")):
                    sitemap_url, msg = resolve_store_to_sitemap_url(store)
                    if sitemap_url:
                        resolved_triples.append((label, store, sitemap_url))
                    else:
                        st.error(f"❌ {hint or store}: {msg}")
                    continue
                st.error(f"❌ سطر بدون رابط صالح: {hint}")

            prog_resolve.progress(1.0, "✅ اكتمل تجهيز الخرائط")

            if not resolved_triples:
                st.error("❌ لم يتم العثور على أي خريطة موقع صالحة. لا يمكن بدء الكشط.")
            else:
                _fail_n = n_entries - len(resolved_triples)
                if _fail_n:
                    st.warning(
                        f"⚠️ تُجاهل {_fail_n} سطراً دون خريطة صالحة — يُكشط **{len(resolved_triples)}** متجراً في الطابور."
                    )
                our_df_pre = None
                if not os.path.isfile(our_path):
                    st.warning("⚠️ لم يُعثر على كتالوج المنتجات — يرجى رفع ملف `mahwous_catalog.csv`")
                    uploaded_catalog = st.file_uploader(
                        "📂 ارفع ملف كتالوج منتجاتك (mahwous_catalog.csv)",
                        type=["csv"],
                        key="catalog_uploader",
                    )
                    if uploaded_catalog:
                        os.makedirs("data", exist_ok=True)
                        with open(our_path, "wb") as f:
                            f.write(uploaded_catalog.read())
                        st.success("✅ تم حفظ الكتالوج — اضغط 'بدء الكشط والتحليل' الآن")
                        st.rerun()
                    st.stop()
                else:
                    try:
                        our_df_pre = pd.read_csv(our_path)
                    except Exception as e:
                        st.error(f"❌ تعذر قراءة الكتالوج المحلي: {e}")
                        our_df_pre = None

                if our_df_pre is not None:
                    if max_rows > 0:
                        our_df_pre = our_df_pre.head(int(max_rows))
                    os.makedirs("data", exist_ok=True)
                    _all_smaps = [p[2] for p in resolved_triples]
                    with open("data/competitors_list.json", "w", encoding="utf-8") as f:
                        json.dump(_all_smaps, f, ensure_ascii=False)

                    _comp_label = str(
                        st.session_state.get("competitor_display_name") or ""
                    ).strip()
                    _n_res = len(resolved_triples)
                    _single = _n_res <= 1
                    _scrape_queue = [
                        {
                            "sitemap": sm,
                            "comp_key": deps.comp_key_for_scrape_entry(
                                lbl, src, _comp_label, _single
                            ),
                            "source_url": src,
                        }
                        for lbl, src, sm in resolved_triples
                    ]
                    ctx: dict[str, Any] = {
                        "our_df": our_df_pre,
                        "pipeline_inline": True if scrape_bg else pipeline_inline,
                        "pl_every": int(os.environ.get("SCRAPER_PIPELINE_EVERY", "100")),
                        "use_ai_partial": os.environ.get(
                            "SCRAPER_PIPELINE_AI_PARTIAL", ""
                        ).strip().lower()
                        in ("1", "true", "yes"),
                        "our_file_name": "mahwous_catalog.csv",
                        "scrape_bg": scrape_bg,
                        "user_comp_label": _comp_label,
                        "scrape_queue": _scrape_queue,
                    }
                    try:
                        atomic_write_pickle(deps.scrape_bg_context_path, ctx)
                    except Exception as e:
                        deps.logger.exception("تعذر حفظ سياق الكشط")
                        st.error(f"❌ تعذر حفظ سياق الكشط: {e}")
                    else:
                        deps.clear_live_session_pkl()
                        deps.merge_scrape_live_snapshot(
                            analysis_reset=True,
                            running=True,
                            done=False,
                            success=False,
                            scrape={"current": 0, "total": 1, "label": "🕸️ يبدأ الكشط..."},
                        )
                        t_sc = threading.Thread(
                            target=deps.run_scrape_chain_background,
                            daemon=True,
                        )
                        deps.add_script_run_ctx(t_sc)
                        t_sc.start()
                        _qk = "، ".join([str(j.get("comp_key", ""))[:40] for j in _scrape_queue[:5]])
                        _more = f" (+{_n_res - 5})" if _n_res > 5 else ""
                        if scrape_bg:
                            st.success(
                                "✅ **الكشط** يعمل في الخيط — يمكنك التنقل. "
                                "اللوحة المباشرة أدناه عند العودة لـ «رفع الملفات»؛ الشريط الجانبي يعرض التقدم."
                                f" — الطابور (**{_n_res}**): {_qk}{_more}"
                            )
                        else:
                            st.success(
                                "✅ **الكشط** يعمل — **اللوحة المباشرة** تُحدَّث دورياً (تقدّم لكل متجر بالتسلسل)."
                                f" — الطابور (**{_n_res}**): {_qk}{_more}"
                            )
                        st.rerun()

                if our_df_pre is None:
                    pass
