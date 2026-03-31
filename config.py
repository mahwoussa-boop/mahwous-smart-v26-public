"""
config.py - الإعدادات المركزية v19.0
المفاتيح محمية عبر Streamlit Secrets
"""
import json as _json
import os as _os

# ===== معلومات التطبيق =====
APP_TITLE   = "نظام التسعير الذكي - مهووس"
APP_NAME    = APP_TITLE
APP_VERSION = "v26.0"
APP_ICON    = "🧪"
GEMINI_MODEL = "gemini-2.0-flash"   # النموذج المستقر الموصى به

# ══════════════════════════════════════════════
#  قراءة Secrets بطريقة آمنة 100%
#  تدعم 3 أساليب Streamlit
# ══════════════════════════════════════════════
def _s(key, default=""):
    """
    يقرأ Secret بـ 3 طرق:
    1. st.secrets[key]         الطريقة المباشرة (Streamlit Cloud)
    2. os.environ              Railway Environment Variables
    3. default                 القيمة الافتراضية
    """
    # 1. Railway / os.environ أولاً (يعمل في البناء والتشغيل)
    v = _os.environ.get(key, "")
    if v:
        return v
    # 2. st.secrets (Streamlit Cloud فقط - يُستدعى عند التشغيل)
    try:
        import streamlit as st
        v = st.secrets[key]
        if v is not None:
            return str(v) if not isinstance(v, (list, dict)) else v
    except Exception:
        pass
    return default


def _parse_gemini_keys():
    """
    يجمع مفاتيح Gemini من أي صيغة:
    • GEMINI_API_KEYS = '["key1","key2","key3"]'  (JSON string)
    • GEMINI_API_KEYS = ["key1","key2"]            (TOML array)
    • GEMINI_API_KEY  = "key1"                     (مفتاح واحد)
    • GEMINI_KEY_1 / GEMINI_KEY_2 / ...           (مفاتيح منفصلة)
    """
    keys = []

    # ─── المحاولة 1: GEMINI_API_KEYS (JSON string أو TOML array) ───
    raw = _s("GEMINI_API_KEYS", "")

    if isinstance(raw, list):
        # TOML array مباشرة
        keys = [k for k in raw if k and isinstance(k, str)]
    elif raw and isinstance(raw, str):
        raw = raw.strip()
        # قد تكون JSON string
        if raw.startswith('['):
            try:
                parsed = _json.loads(raw)
                if isinstance(parsed, list):
                    keys = [k for k in parsed if k]
            except Exception:
                # ربما string بدون quotes صحيحة → نظفها
                clean = raw.strip("[]").replace('"','').replace("'",'')
                keys = [k.strip() for k in clean.split(',') if k.strip()]
        elif raw:
            keys = [raw]

    # ─── المحاولة 2: GEMINI_API_KEY (مفتاح واحد) ───
    single = _s("GEMINI_API_KEY", "")
    # Railway/UI أحياناً يحفظان Gemini_API_Key (حالة أحرف مختلفة؛ Linux حساس)
    if not single:
        single = (_os.environ.get("Gemini_API_Key", "") or _os.environ.get("GEMINI_KEY", "")).strip()
    if single and single not in keys:
        keys.append(single)

    # ─── المحاولة 3: مفاتيح منفصلة ───
    for n in ["GEMINI_KEY_1","GEMINI_KEY_2","GEMINI_KEY_3",
              "GEMINI_KEY_4","GEMINI_KEY_5"]:
        k = _s(n, "")
        if k and k not in keys:
            keys.append(k)

    # ─── أسماء بديلة شائعة (Railway / Google AI Studio) ───
    for n in ("GOOGLE_API_KEY", "GOOGLE_AI_API_KEY", "GENERATIVE_AI_API_KEY"):
        k = _s(n, "")
        if k and k not in keys:
            keys.append(k)

    # تنظيف نهائي: مفاتيح Google عادة ≥30 حرفاً؛ الحد الأدنى 12 لتجنب القيم الوهمية
    keys = [k.strip() for k in keys if k and len(k.strip()) >= 12]
    return keys


def get_gemini_api_keys():
    """إعادة قراءة المفاتيح من البيئة (مفيد للعرض بعد تغيير Variables دون إعادة تشغيل العملية)."""
    return _parse_gemini_keys()


# ══════════════════════════════════════════════
#  المفاتيح الفعلية
# ══════════════════════════════════════════════
GEMINI_API_KEYS    = _parse_gemini_keys()
GEMINI_API_KEY     = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else ""
OPENROUTER_API_KEY = _s("OPENROUTER_API_KEY") or _s("OPENROUTER_KEY") or ""
COHERE_API_KEY     = _s("COHERE_API_KEY") or ""
EXTRA_API_KEY      = _s("EXTRA_API_KEY")

# ══════════════════════════════════════════════
#  Make Webhooks
# ══════════════════════════════════════════════
WEBHOOK_UPDATE_PRICES = _s("WEBHOOK_UPDATE_PRICES") or ""
WEBHOOK_NEW_PRODUCTS = _s("WEBHOOK_NEW_PRODUCTS") or ""

# ══════════════════════════════════════════════
#  ألوان
# ══════════════════════════════════════════════
COLORS = {
    "raise": "#dc3545", "lower": "#ffc107", "approved": "#28a745",
    "missing": "#007bff", "review": "#ff9800", "primary": "#6C63FF",
}

# ══════════════════════════════════════════════
#  إعدادات المطابقة
# ══════════════════════════════════════════════
MATCH_THRESHOLD    = 85
HIGH_CONFIDENCE    = 95
REVIEW_THRESHOLD   = 75
PRICE_TOLERANCE    = 5
MIN_MATCH_SCORE    = MATCH_THRESHOLD
HIGH_MATCH_SCORE   = HIGH_CONFIDENCE
PRICE_DIFF_THRESHOLD = PRICE_TOLERANCE

# ══════════════════════════════════════════════
#  فلاتر المنتجات
# ══════════════════════════════════════════════
REJECT_KEYWORDS = [
    "sample","عينة","عينه","decant","تقسيم","تقسيمة",
    "split","miniature","0.5ml","1ml","2ml","3ml",
    "vial","سمبل",
]
TESTER_KEYWORDS = ["tester","تستر","تيستر"]
SET_KEYWORDS    = ["set","gift set","طقم","مجموعة","coffret"]

# ══════════════════════════════════════════════
#  العلامات التجارية
# ══════════════════════════════════════════════
KNOWN_BRANDS = [
    "Dior","Chanel","Gucci","Tom Ford","Versace","Armani","YSL","Prada",
    "Burberry","Givenchy","Hermes","Creed","Montblanc","Calvin Klein",
    "Hugo Boss","Dolce & Gabbana","Valentino","Bvlgari","Cartier","Lancome",
    "Jo Malone","Amouage","Rasasi","Lattafa","Arabian Oud","Ajmal",
    "Al Haramain","Afnan","Armaf","Nishane","Xerjoff","Parfums de Marly",
    "Initio","Byredo","Le Labo","Mancera","Montale","Kilian","Roja",
    "Carolina Herrera","Jean Paul Gaultier","Narciso Rodriguez",
    "Paco Rabanne","Mugler","Chloe","Coach","Michael Kors","Ralph Lauren",
    "Maison Margiela","Memo Paris","Penhaligons","Serge Lutens","Diptyque",
    "Frederic Malle","Francis Kurkdjian","Floris","Clive Christian",
    "Ormonde Jayne","Zoologist","Tauer","Lush","The Different Company",
    "Missoni","Juicy Couture","Moschino","Dunhill","Bentley","Jaguar",
    "Boucheron","Chopard","Elie Saab","Escada","Ferragamo","Fendi",
    "Kenzo","Lacoste","Loewe","Rochas","Roberto Cavalli","Tiffany",
    "Van Cleef","Azzaro","Banana Republic","Benetton","Bottega Veneta",
    "Celine","Dsquared2","Ed Hardy","Elizabeth Arden","Ermenegildo Zegna",
    "Swiss Arabian","Ard Al Zaafaran","Nabeel","Asdaaf","Maison Alhambra",
    "لطافة","العربية للعود","رصاصي","أجمل","الحرمين","أرماف",
    "أمواج","كريد","توم فورد","ديور","شانيل","غوتشي","برادا",
    "ميسوني","جوسي كوتور","موسكينو","دانهيل","بنتلي",
    "كينزو","لاكوست","فندي","ايلي صعب","ازارو",
    "Guerlain","Givenchy","Sisley","Issey Miyake","Davidoff","Mexx",
]

# ══════════════════════════════════════════════
#  استبدالات التطبيع
# ══════════════════════════════════════════════
WORD_REPLACEMENTS = {
    'او دو بارفان':'edp','أو دو بارفان':'edp','او دي بارفان':'edp',
    'او دو تواليت':'edt','أو دو تواليت':'edt','او دي تواليت':'edt',
    'مل':'ml','ملي':'ml',
    'سوفاج':'sauvage','ديور':'dior','شانيل':'chanel',
    'توم فورد':'tom ford','أرماني':'armani','غيرلان':'guerlain',
}

# ══════════════════════════════════════════════
#  إعدادات الأتمتة الذكية v26.0
# ══════════════════════════════════════════════
AUTOMATION_RULES_DEFAULT = [
    {
        "name": "خفض السعر تلقائياً",
        "enabled": True,
        "condition": "our_price > comp_price",
        "min_diff": 10,       # فرق أدنى بالريال لتفعيل القاعدة
        "action": "undercut",  # خفض ليصبح أقل من المنافس
        "undercut_amount": 1,  # أقل بكم ريال
        "min_match_score": 90, # حد أدنى لنسبة التطابق
        "max_loss_pct": 15,    # أقصى نسبة خسارة مقبولة من سعر التكلفة
    },
    {
        "name": "رفع السعر عند فرصة ربح",
        "enabled": True,
        "condition": "our_price < comp_price",
        "min_diff": 15,
        "action": "raise_to_match",
        "margin_below": 5,     # أقل من المنافس بكم ريال
        "min_match_score": 90,
    },
    {
        "name": "إبقاء السعر إذا تنافسي",
        "enabled": True,
        "condition": "abs(our_price - comp_price) <= threshold",
        "threshold": 10,
        "action": "keep",
        "min_match_score": 85,
    },
]

# جدولة البحث الدوري (بالدقائق)
AUTO_SEARCH_INTERVAL_MINUTES = 60 * 6   # كل 6 ساعات
AUTO_PUSH_TO_MAKE = False               # إرسال تلقائي لـ Make.com (يتطلب تفعيل يدوي)
AUTO_DECISION_CONFIDENCE = 92           # حد الثقة للقرار التلقائي (تسعير/رفع-خفض)
# حاجز المفقودات: تطابق نصي مع كتالوجنا (token_set_ratio) — يُستبعد عند ≥88%
SMART_MISSING_FUZZ_THRESHOLD = 88
# تحقق AI لقسم المراجعة — واقعي مع مخرجات verify_match (غالباً 65–90)
REVIEW_VERIFY_MIN_CONFIDENCE = 72

# ══════════════════════════════════════════════
#  أقسام التطبيق (v26.0 — مع لوحة الأتمتة)
# ══════════════════════════════════════════════
SECTIONS = [
    "📊 لوحة التحكم",
    "📂 رفع الملفات",
    "🔴 سعر أعلى",
    "🟢 سعر أقل",
    "✅ موافق عليها",
    "🔍 منتجات مفقودة",
    "⚠️ تحت المراجعة",
    "✔️ تمت المعالجة",
    "🤖 الذكاء الصناعي",
    "⚡ أتمتة Make",
    "🔄 الأتمتة الذكية",
    "⚙️ الإعدادات",
    "📜 السجل",
]
SIDEBAR_SECTIONS = SECTIONS
PAGES_PER_TABLE  = 25
# مسار SQLite — معرّف مباشرة لتجنب circular import مع db_manager
DB_PATH = _os.path.join("/tmp", "pricing_v18.db")

# قائمة المنافسين الافتراضية للكشط — يُحمَّل من الملف؛ يمكن تعديل JSON دون المساس بالكود
PRESET_COMPETITORS_PATH = _os.path.join("data", "preset_competitors.json")
