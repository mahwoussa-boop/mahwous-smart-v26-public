"""
styles.py - التصميم v20.0 — بطاقات محسنة + عرض المنافسين
"""
from html import escape as _html_escape
from textwrap import dedent


def get_styles():
    return get_main_css()

def get_main_css():
    return """<style>
@import url('https://fonts.googleapis.com/css2?family=Tajawal:wght@400;700;900&display=swap');
*{font-family:'Tajawal',sans-serif!important}
.main .block-container{max-width:1400px;padding:1rem 2rem}
.stat-card{background:#1A1A2E;border-radius:12px;padding:16px;text-align:center;border:1px solid #333344}
.stat-card:hover{box-shadow:0 4px 16px rgba(108,99,255,.15);border-color:#6C63FF}
.stat-card .num{font-size:2.2rem;font-weight:900;margin:4px 0}
.stat-card .lbl{font-size:.85rem;color:#8B8B8B}
.cmp-table{width:100%;border-collapse:separate;border-spacing:0;border-radius:8px;overflow:hidden;font-size:.88rem}
.cmp-table thead th{background:#16213e;color:#fff;padding:10px 8px;font-weight:700;text-align:center;border-bottom:2px solid #6C63FF;position:sticky;top:0;z-index:10}
.cmp-table tbody tr:nth-child(even){background:rgba(26,26,46,.4)}
.cmp-table tbody tr:hover{background:rgba(108,99,255,.1)!important}
.cmp-table td{padding:8px 6px;text-align:center;border-bottom:1px solid rgba(51,51,68,.4);vertical-align:middle}
.td-our{background:rgba(108,99,255,.06)!important;border-right:3px solid #6C63FF;text-align:right!important;font-weight:600;color:#B8B4FF;max-width:250px;word-wrap:break-word}
.td-comp{background:rgba(255,152,0,.06)!important;border-left:3px solid #ff9800;text-align:right!important;font-weight:600;color:#FFD180;max-width:250px;word-wrap:break-word}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:.75rem;font-weight:700}
.b-high{background:rgba(255,23,68,.15);color:#FF1744;border:1px solid #FF1744}
.b-med{background:rgba(255,214,0,.15);color:#FFD600;border:1px solid #FFD600}
.b-low{background:rgba(0,200,83,.15);color:#00C853;border:1px solid #00C853}
.conf-bar{width:100%;height:6px;background:rgba(255,255,255,.08);border-radius:3px;overflow:hidden}
.conf-fill{height:100%;border-radius:3px}
/* ── بطاقة VS المحسنة مع المنافسين ── */
.vs-row{display:grid;grid-template-columns:1fr 36px 1fr;gap:10px;align-items:center;padding:12px;background:#1A1A2E;border-radius:8px 8px 0 0;margin:5px 0 0 0;border:1px solid #333344;border-bottom:none}
.vs-badge{background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.7rem}
.our-s{text-align:right;padding:8px;background:rgba(108,99,255,.04);border-radius:6px;border-right:3px solid #6C63FF}
.comp-s{text-align:left;padding:8px;background:rgba(255,152,0,.04);border-radius:6px;border-left:3px solid #ff9800}
.action-btn{display:inline-block;padding:4px 10px;border-radius:6px;font-size:.75rem;font-weight:700;cursor:pointer;margin:2px;border:1px solid}
.btn-approve{background:rgba(0,200,83,.1);color:#00C853;border-color:#00C853}
.btn-remove{background:rgba(255,23,68,.1);color:#FF1744;border-color:#FF1744}
.btn-delay{background:rgba(255,152,0,.1);color:#ff9800;border-color:#ff9800}
.btn-export{background:rgba(108,99,255,.1);color:#6C63FF;border-color:#6C63FF}
.ai-box{background:#1A1A2E;padding:12px;border-radius:8px;border:1px solid #333344;margin:6px 0}
.paste-area{background:#0E1117;border:2px dashed #333344;border-radius:8px;padding:12px;min-height:80px}
.multi-comp{background:rgba(0,123,255,.06);border:1px solid rgba(0,123,255,.2);border-radius:6px;padding:8px;margin:4px 0}
/* ── شريط المنافسين المصغر ── */
.comp-strip{background:#0e1628;border:1px solid #333344;border-top:none;border-radius:0 0 8px 8px;padding:8px 12px;margin:0 0 2px 0;display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.comp-chip{display:inline-flex;align-items:center;gap:4px;padding:3px 8px;border-radius:14px;font-size:.72rem;font-weight:600;border:1px solid;white-space:nowrap}
.comp-chip.leader{background:rgba(255,152,0,.12);border-color:#ff9800;color:#ffb74d}
.comp-chip.normal{background:rgba(108,99,255,.08);border-color:#333366;color:#9e9eff}
.comp-chip .cp-name{max-width:100px;overflow:hidden;text-overflow:ellipsis}
.comp-chip .cp-price{font-weight:900}
/* ── بطاقة المنتج المفقود المحسنة ── */
.miss-card{border-radius:10px;padding:14px;margin:6px 0;background:linear-gradient(135deg,#0a1628,#0e1a30)}
.miss-card .miss-header{display:flex;justify-content:space-between;align-items:flex-start;gap:12px}
.miss-card .miss-info{flex:1;min-width:0}
.miss-card .miss-thumb{flex-shrink:0}
.miss-card .miss-name{font-weight:700;color:#4fc3f7;font-size:1rem}
.miss-card .miss-meta{font-size:.75rem;color:#888;margin-top:4px}
.miss-card .miss-prices{text-align:left;min-width:120px}
.miss-card .miss-comp-price{font-size:1.2rem;font-weight:900;color:#ff9800}
.miss-card .miss-suggested{font-size:.72rem;color:#4caf50}
/* ── شارات الثقة ── */
.trust-badge{display:inline-block;padding:2px 8px;border-radius:10px;font-size:.68rem;font-weight:700;margin-right:4px}
.trust-green{background:rgba(0,200,83,.15);color:#00C853;border:1px solid #00C85366}
.trust-yellow{background:rgba(255,214,0,.15);color:#FFD600;border:1px solid #FFD60066}
.trust-red{background:rgba(255,23,68,.15);color:#FF1744;border:1px solid #FF174466}
section[data-testid="stSidebar"]{background:linear-gradient(180deg,#0E1117,#1A1A2E);transition:all .3s ease}
#MainMenu,footer{visibility:hidden}
/* header يبقى ظاهراً لأنه يحتوي على زر إظهار القائمة الجانبية */
header[data-testid="stHeader"] {
    background: transparent !important;
    backdrop-filter: none !important;
}
/* إصلاح أيقونات Streamlit */
[data-testid="stExpander"] summary svg,
[data-testid="stSelectbox"] svg[data-testid="stExpanderToggleIcon"],
details summary span[data-testid] svg {
    font-family: system-ui, -apple-system, sans-serif !important;
}
[data-testid="stExpander"] summary {
    direction: rtl;
    font-family: 'Tajawal', sans-serif !important;
}
.stSelectbox label, .stMultiSelect label {
    direction: rtl;
    font-family: 'Tajawal', sans-serif !important;
}
/* ── زر القائمة الجانبية ── منقول إلى get_sidebar_toggle_js */
</style>"""


def get_sidebar_toggle_js():
    """CSS فقط لزر إخفاء/إظهار القائمة الجانبية — متوافق مع Streamlit Cloud"""
    return """<style>
/* زر إخفاء/إظهار القائمة الجانبية — يستخدم الزر المدمج في Streamlit */
[data-testid="collapsedControl"] {
    color: #6C63FF !important;
    background: linear-gradient(180deg,#6C63FF22,#4a42cc22) !important;
    border: 1px solid #6C63FF44 !important;
    border-radius: 0 8px 8px 0 !important;
    transition: all .25s ease !important;
}
[data-testid="collapsedControl"]:hover {
    background: linear-gradient(180deg,#6C63FF44,#4a42cc44) !important;
    box-shadow: 3px 0 10px rgba(108,99,255,.4) !important;
}
</style>
"""


def stat_card(icon, label, value, color="#6C63FF"):
    return f'<div class="stat-card" style="border-top:3px solid {color}"><div style="font-size:1.3rem">{icon}</div><div class="num" style="color:{color}">{value}</div><div class="lbl">{label}</div></div>'


def vs_card(our_name, our_price, comp_name, comp_price, diff, comp_source="", product_id="", our_img="", comp_img=""):
    """بطاقة VS الأساسية — المنافس الرئيسي (الأقل سعراً) + صور اختيارية"""
    dc = "#FF1744" if diff > 0 else "#00C853" if diff < 0 else "#FFD600"
    src = f'<div style="font-size:.65rem;color:#666">{comp_source}</div>' if comp_source else ""
    pid = str(product_id) if product_id and str(product_id) not in ("", "nan", "None", "0") else ""
    pid_html = f'<div style="font-size:.65rem;color:#6C63FF99;margin-top:1px">#{pid}</div>' if pid else ""
    # السعر المقترح = أقل من أقل منافس بريال
    suggested = comp_price - 1 if comp_price > 0 else 0
    sugg_html = ""
    if suggested > 0 and diff > 10:
        sugg_html = f'<div style="font-size:.7rem;color:#4caf50;margin-top:2px">مقترح: {suggested:,.0f} ر.س</div>'
    ou = str(our_img or "").strip()
    cu = str(comp_img or "").strip()
    our_img_html = (
        f'<img src="{_html_escape(ou, quote=True)}" style="width:44px;height:44px;border-radius:6px;object-fit:cover;margin-bottom:6px;border:1px solid #6C63FF">'
        if ou and ou.lower() not in ("nan", "none")
        else ""
    )
    comp_img_html = (
        f'<img src="{_html_escape(cu, quote=True)}" style="width:44px;height:44px;border-radius:6px;object-fit:cover;margin-bottom:6px;border:1px solid #ff9800">'
        if cu and cu.lower() not in ("nan", "none")
        else ""
    )
    return f'''<div class="vs-row">
<div class="our-s">{our_img_html}<div style="font-size:.7rem;color:#8B8B8B">منتجنا</div><div style="font-weight:700;color:#B8B4FF;font-size:.9rem">{our_name}</div>{pid_html}<div style="font-size:1.1rem;font-weight:900;color:#6C63FF;margin-top:2px">{our_price:.0f} ر.س</div>{sugg_html}</div>
<div class="vs-badge">VS</div>
<div class="comp-s">{comp_img_html}<div style="font-size:.7rem;color:#8B8B8B">المنافس المتصدر</div><div style="font-weight:700;color:#FFD180;font-size:.9rem">{comp_name}</div><div style="font-size:1.1rem;font-weight:900;color:#ff9800;margin-top:2px">{comp_price:.0f} ر.س</div>{src}</div>
</div><div style="text-align:center;background:#1A1A2E;padding:4px;border-left:1px solid #333344;border-right:1px solid #333344;margin:0"><span style="color:{dc};font-weight:700;font-size:.9rem">الفرق: {diff:+.0f} ر.س</span></div>'''


def comp_strip(all_comps):
    """شريط المنافسين المصغر — يعرض كل المنافسين بأسعارهم واسم المنتج لديهم مرتبين من الأقل"""
    if not all_comps or not isinstance(all_comps, list) or len(all_comps) == 0:
        return ""
    # ترتيب من الأقل سعراً
    sorted_comps = sorted(all_comps, key=lambda c: float(c.get("price", 0) or 0))
    rows = []
    for i, cm in enumerate(sorted_comps):
        c_store = str(cm.get("competitor", "")).strip()
        c_price = float(cm.get("price", 0) or 0)
        c_pname = str(cm.get("name", "")).strip()
        c_score = float(cm.get("score", 0) or 0)
        c_img = str(cm.get("image_url", "") or cm.get("image", "") or "").strip()
        is_leader = (i == 0)
        crown = "👑" if is_leader else ""
        bg = "rgba(255,152,0,.10)" if is_leader else "rgba(108,99,255,.05)"
        border = "#ff9800" if is_leader else "#333366"
        name_color = "#ffb74d" if is_leader else "#9e9eff"
        # اسم المنتج لدى المنافس (مختصر)
        short_pname = c_pname[:50] + ".." if len(c_pname) > 50 else c_pname
        score_html = f'<span style="color:#888;font-size:.62rem">{c_score:.0f}%</span>' if c_score > 0 else ""
        img_html = (
            f'<img src="{_html_escape(c_img, quote=True)}" '
            f'style="width:50px;height:50px;border-radius:10px;object-fit:cover;'
            f'border:1px solid {border};background:#0e1628;flex:0 0 50px" '
            f'onerror="this.style.display=\'none\'" />'
            if c_img and c_img.lower() not in ("nan", "none")
            else ""
        )
        rows.append(
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'padding:5px 10px;background:{bg};border:1px solid {border};border-radius:8px;'
            f'margin:2px 0;gap:8px;flex-wrap:wrap">'
            f'<div style="display:flex;align-items:center;gap:6px;flex:1;min-width:0">'
            f'{img_html}'
            f'<span style="font-weight:900;font-size:.8rem">{crown}</span>'
            f'<span style="font-weight:700;color:{name_color};font-size:.75rem;white-space:nowrap">{c_store}</span>'
            f'<span style="color:#aaa;font-size:.7rem;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:300px" title="{c_pname}">{short_pname}</span>'
            f'{score_html}'
            f'</div>'
            f'<span style="font-weight:900;color:{"#ff9800" if is_leader else "#9e9eff"};font-size:.85rem;white-space:nowrap">{c_price:,.0f} ر.س</span>'
            f'</div>'
        )
    return f'<div class="comp-strip" style="flex-direction:column;gap:2px">{chr(10).join(rows)}</div>'


def miss_card(name, price, brand, size, ptype, comp, suggested_price,
              note="", variant_html="", tester_badge="", border_color="#007bff44",
              confidence_level="green", confidence_score=0, image_url=""):
    """بطاقة المنتج المفقود — HTML بدون مسافات بادئة (تجنب تفسير Markdown ككتلة كود)."""
    safe_name = _html_escape(str(name or ""))
    safe_brand = _html_escape(str(brand or "—"))
    safe_size = _html_escape(str(size or "—"))
    safe_ptype = _html_escape(str(ptype or "—"))
    safe_comp = _html_escape(str(comp or "—"))
    safe_note = _html_escape(str(note or ""))
    trust_map = {
        "green":  ("trust-green",  "مؤكد"),
        "yellow": ("trust-yellow", "محتمل"),
        "red":    ("trust-red",    "مشكوك"),
    }
    t_cls, t_lbl = trust_map.get(confidence_level, ("trust-green", "مؤكد"))
    trust_html = f'<span class="trust-badge {t_cls}">{t_lbl}</span>' if confidence_level != "green" else ""

    note_html = f'<div style="font-size:.72rem;color:#ff9800;margin-top:4px">{safe_note}</div>' if safe_note and "⚠️" in safe_note else ""

    u = str(image_url or "").strip()
    img_html = ""
    if u.lower().startswith("http"):
        eu = _html_escape(u, quote=True)
        img_html = (
            f'<div class="miss-thumb"><img src="{eu}" alt="" '
            'style="width:76px;height:76px;border-radius:10px;object-fit:cover;'
            'border:1px solid #444466;background:#0e1628" loading="lazy" '
            'referrerpolicy="no-referrer" onerror="this.style.display=\'none\'" /></div>'
        )

    inner = f"""<div class="miss-card" style="border:1px solid {border_color};margin:8px 0;border-radius:10px;padding:12px;background:linear-gradient(135deg,#0a1628,#0e1a30)">
<div style="display:flex;gap:14px;align-items:flex-start;direction:rtl;flex-wrap:wrap">
{img_html}
<div style="flex:1;min-width:0">
<div class="miss-name" style="font-weight:700;color:#4fc3f7;font-size:1rem;line-height:1.35">{trust_html}{tester_badge}{safe_name}</div>
<div class="miss-meta" style="font-size:.75rem;color:#888;margin-top:6px;line-height:1.5">🏷️ {safe_brand} &nbsp;|&nbsp; 📏 {safe_size} &nbsp;|&nbsp; 🧴 {safe_ptype} &nbsp;|&nbsp; 🏪 {safe_comp}</div>
{variant_html}
{note_html}
</div>
<div class="miss-prices" style="text-align:left;min-width:108px;flex-shrink:0">
<div class="miss-comp-price" style="font-size:1.15rem;font-weight:900;color:#ff9800">{price:,.0f} ر.س</div>
<div class="miss-suggested" style="font-size:.72rem;color:#4caf50;margin-top:4px">مقترح: {suggested_price:,.0f} ر.س</div>
</div>
</div>
</div>"""
    return dedent(inner).strip()
