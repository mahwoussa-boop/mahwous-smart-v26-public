"""
config.py - الإعدادات المركزية (v26.0)
المفاتيح عبر Streamlit Secrets / متغيرات البيئة
"""
import json as _json
import os as _os
import shutil as _shutil
import tempfile

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


def get_openrouter_api_key() -> str:
    """إعادة قراءة المفتاح من البيئة (Railway / Secrets دون إعادة تشغيل العملية)."""
    return _s("OPENROUTER_API_KEY") or _s("OPENROUTER_KEY") or ""


def get_cohere_api_key() -> str:
    return _s("COHERE_API_KEY") or ""


# ══════════════════════════════════════════════
#  المفاتيح الفعلية
# ══════════════════════════════════════════════
GEMINI_API_KEYS    = _parse_gemini_keys()
GEMINI_API_KEY     = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else ""
OPENROUTER_API_KEY = get_openrouter_api_key()
COHERE_API_KEY     = get_cohere_api_key()
EXTRA_API_KEY      = _s("EXTRA_API_KEY")

# ══════════════════════════════════════════════
#  Make Webhooks (يُفضَّل الدوال — تقرأ البيئة/Secrets بعد مزامنة الجلسة في app.py)
# ══════════════════════════════════════════════
def get_webhook_update_prices() -> str:
    return (_s("WEBHOOK_UPDATE_PRICES") or "").strip()


def get_webhook_missing_products() -> str:
    """
    سيناريو «أتمتة التسعير» / إضافة المفقودات في سلة فقط.
    يفضّل WEBHOOK_MISSING_PRODUCTS؛ إن وُجد WEBHOOK_NEW_PRODUCTS قديماً يُستخدم كاحتياط.
    """
    v = (_s("WEBHOOK_MISSING_PRODUCTS") or "").strip()
    if v:
        return v
    return (_s("WEBHOOK_NEW_PRODUCTS") or "").strip()


def get_webhook_new_products() -> str:
    """توافق خلفي — نفس دالة المفقودات."""
    return get_webhook_missing_products()


# توثيق روابط سيناريوهات Make المشتركة (الاستنساخ من المتصفح — الرابط الفعلي للـ Webhook من لوحتك)
MAKE_DOCS_SCENARIO_UPDATE_PRICES = (
    "https://eu2.make.com/public/shared-scenario/9uue7ENfzO5/integration-webhooks-salla"
)
MAKE_DOCS_SCENARIO_PRICING_AUTOMATION = (
    "https://eu2.make.com/public/shared-scenario/UsesKnA62xy/mahwous-pricing-automation-salla"
)

# ══════════════════════════════════════════════
#  كشط (async_scraper.py) — تُقرأ من os.environ على التشغيل
#  • SCRAPER_MAX_CONCURRENT_FETCH (افتراضي 28، حد أعلى 64) — تزيد السرعة؛ خفّضها عند الحظر
#  • SCRAPER_PIPELINE_EVERY — فاصل لقطات الفرز أثناء الكشط (افتراضي 100 صف)
#  • MAHWOUS_UI_LIVE_REFRESH_MS — تبطئة تحديث واجهة Streamlit أثناء الكشط الطويل
#  • MAHWOUS_SCRAPE_UI_MIN_INTERVAL_SEC — أقل فاصل (ثوانٍ) بين كتابات لقطة JSON للتقدم الحي (افتراضي حسب حجم الطابور)
#  • MAHWOUS_DB_PATH — مسار ملف SQLite كاملاً (تجاوز data/pricing_v18.db الافتراضي)
#  • MAHWOUS_LOG_LEVEL — مستوى التسجيل: DEBUG / INFO / WARNING / ERROR (افتراضي INFO)
#  استيراد سلة (utils/helpers.py export_missing_products_to_salla_csv_bytes):
#  • SALLA_IMPORT_DEFAULT_CATEGORY — مسار تصنيف افتراضي يطابق categories.csv / لوحة سلة
#  • SALLA_IMPORT_FALLBACK_BRAND — ماركة احتياط عند «غير محدد» (نص كما في brands.csv)
#  • WEBHOOK_UPDATE_PRICES — تعديل أسعار (🔴 أعلى 🟢 أقل ✅ موافق)؛ WEBHOOK_MISSING_PRODUCTS — مفقودات فقط
#  • WEBHOOK_NEW_PRODUCTS — اسم قديم؛ يُقرأ كاحتياط إن لم يُضبط WEBHOOK_MISSING_PRODUCTS
#  AI (engines/ai_engine.py):
#  • OPENROUTER_MODELS — معرّفات نماذج OpenRouter مفصولة بفواصل (تجاوز القائمة الافتراضية)
#  • احذف COHERE_API_KEY من Secrets إذا كان 401 لتقليل الضوضاء (Cohere اختياري)
# ══════════════════════════════════════════════

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
    "➕ منتج سريع",
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


def _project_root_dir() -> str:
    """مجلد جذر المشروع (حيث يقع config.py)."""
    return _os.path.dirname(_os.path.abspath(__file__))


def _resolve_db_path() -> str:
    """
    مسار SQLite موحّد لكل الوحدات.
    - الأولوية: متغير البيئة MAHWOUS_DB_PATH (مسار كامل للملف).
    - الافتراضي: data/pricing_v18.db تحت جذر المشروع (دائم أكثر من مجلد temp).
    - احتياطي: مجلد temp النظام إن تعذّر إنشاء/الكتابة في data/.
    - ترحيل لمرة واحدة: إن وُجدت قاعدة قديمة في temp ولم يُنشأ الملف في data بعد، تُنسخ.
    """
    env = (_os.environ.get("MAHWOUS_DB_PATH") or "").strip()
    if env:
        return env
    legacy_temp = _os.path.join(tempfile.gettempdir(), "pricing_v18.db")
    data_dir = _os.path.join(_project_root_dir(), "data")
    try:
        _os.makedirs(data_dir, exist_ok=True)
        candidate = _os.path.join(data_dir, "pricing_v18.db")
        if not _os.path.isfile(candidate) and _os.path.isfile(legacy_temp):
            try:
                _shutil.copy2(legacy_temp, candidate)
            except Exception:
                pass
        return candidate
    except Exception:
        return legacy_temp if _os.path.isfile(legacy_temp) else _os.path.join(
            tempfile.gettempdir(), "pricing_v18.db"
        )


# نفس الاسم في config و db_manager (استورد من هنا فقط)
DB_PATH = _resolve_db_path()

# قائمة المنافسين الافتراضية للكشط — يُحمَّل من الملف؛ يمكن تعديل JSON دون المساس بالكود
PRESET_COMPETITORS_PATH = _os.path.join("data", "preset_competitors.json")
