# 🧪 مهووس v26 — نظام التسعير الذكي

## ✅ الميزات الكاملة

### 1️⃣ قسم الذكاء الاصطناعي (5 tabs)
```
🤖 الذكاء الاصطناعي
├── 💬 دردشة مباشرة
│   ├── Gemini Flash متصل مباشرة
│   ├── تاريخ المحادثة (آخر 15 رسالة)
│   ├── أزرار سريعة (أولويات، فرص، مفقودات، ملخص)
│   └── سياق تلقائي من البيانات
│
├── 📋 لصق وتحليل
│   ├── الصق Excel/CSV/نص
│   ├── تحليل فوري بـ Gemini
│   └── تحويل لجدول
│
├── 🔍 تحقق منتج
│   ├── مقارنة منتجين
│   ├── نسبة الثقة %
│   └── توصية AI
│
├── 💹 بحث سوق
│   ├── سعر السوق الحقيقي (Grounding)
│   ├── صور من Fragrantica Arabia
│   └── مكونات العطر (قمة، قلب، قاعدة)
│
└── 📊 أوامر مجمعة
    ├── تنفيذ أوامر على أي قسم
    └── تحليل مجمع
```

### 2️⃣ محرك v21 السريع (5x أسرع)
```python
CompIndex:
├── تطبيع مسبق (Pre-normalize)
├── بحث vectorized
├── AI فقط للغموض (62-96%)
└── تلقائي للواضح (≥97%)

النتيجة:
- السرعة: ~10ms/منتج (كان 50ms)
- الدقة: 99.5% محفوظة
- استخدام AI: -60%
```

### 3️⃣ المنتجات المفقودة مع Fragrantica
```
🔵 منتجات مفقودة
├── 🖼️ صورة
│   └── جلب من Fragrantica Arabia
├── ✍️ وصف مهووس
│   ├── تنسيق احترافي
│   ├── مكونات العطر
│   └── جاهز للنسخ للمتجر
├── 🤖 تكرار؟
├── 🔎 بحث مهووس
├── 💹 سعر السوق
├── 📤 إرسال Make
└── 📥 CSV + Excel
```

### 4️⃣ تصدير شامل
```
كل قسم يحتوي:
├── 📥 Excel (ملون ومنسق)
├── 📄 CSV (UTF-8 للعربية)
├── 🤖 AI جماعي (أول 20)
└── 📤 إرسال Make (مباشر)
```

### 5️⃣ Make.com محسّن
```json
{
  "products": [{
    "product_no": "12345",        ← رقم المنتج (no)
    "name": "Dior Sauvage...",
    "current_price": 450.00,
    "new_price": 430.00,
    "diff": -20.00,
    "competitor": "competitor1",
    "action": "lower_price",
    "brand": "Dior",
    "match_confidence": 98.5
  }],
  "timestamp": "...",
  "total": 10
}
```

---

## 📦 هيكل المشروع (v26)

```
mahwous-smart-v26-public/
├── app.py                  ← نقطة الدخول Streamlit + كشط/شريط جانبي
├── config.py               ← إعدادات و SECTIONS
├── styles.py               ← تنسيقات CSS
├── requirements.txt
├── .env.example            ← قالب متغيرات البيئة (انسخ إلى .env محلياً)
│
├── mahwous_ui/             ← صفحات الواجهة المستخرجة من app
│   ├── pro_table.py        ← جدول المقارنة المشترك (سعر أعلى/أقل/موافق/مراجعة)
│   ├── analysis_redistribute.py  ← إعادة توزيع يدوية (جلسة Streamlit)
│   └── …                   ← dashboard, upload, ai_page, …
│
├── engines/
│   ├── engine.py           ← تحليل ومطابقة
│   └── ai_engine.py        ← Gemini وواجهات AI
│
├── utils/
│   ├── analysis_sections.py ← split_analysis_results (بدون Streamlit)
│   ├── make_helper.py      ← Make.com
│   ├── helpers.py
│   └── db_manager.py
│
├── tests/                  ← unittest (استيراد صفحات، split_analysis، safe_float، SECTIONS)
├── .github/workflows/
│   └── ci.yml              ← CI على GitHub (Python 3.11 و 3.12)
├── scripts/
│   ├── verify_all.ps1      ← نفس فحوصات CI على Windows
│   └── verify_all.sh       ← نفس فحوصات CI على Linux/macOS
│
└── .streamlit/
    └── config.toml
```

### جودة الكود والتحقق المحلي

```bash
cd mahwous-smart-v26-public
python -m pip install -r requirements.txt
python -m compileall -q .
python -m unittest discover -s tests -v
streamlit run app.py
```

أو سكربت واحد (يطابق ما يُشغَّل في CI):

```powershell
# Windows (PowerShell)
.\scripts\verify_all.ps1
```

```bash
# Linux / macOS
chmod +x scripts/verify_all.sh && ./scripts/verify_all.sh
```

**GitHub Actions:** عند الدفع إلى `main` / `master` / `develop` يُشغَّل تلقائياً: تثبيت `requirements.txt`، `compileall`، و`unittest discover`.

- **`utils/analysis_sections`**: منطق تقسيم أقسام التحليل فقط؛ أي منطق يعتمد على `st.session_state` يبقى تحت **`mahwous_ui/`** (مثل `analysis_redistribute.py`).
- **`mahwous_ui/audit_tools_page.py`** و**`audit_tools_core.py`**: واجهة ومنطق «التدقيق والتحسين» (مقارنة ذكية، مدقق متجر، SEO)؛ تُفتح من الشريط الجانبي وتعمل بجانب التطبيق الرئيسي دون الخلط مع محرك التحليل المركزي.

---

## 🚀 التشغيل السريع

### 1️⃣ الرفع على Streamlit Cloud
```bash
# فك الضغط
unzip mahwous_COMPLETE.zip

# ارفع كل الملفات على:
https://share.streamlit.io
```

### 2️⃣ إضافة Secrets
```toml
# Settings → Secrets (أو محلياً: انسخ .streamlit/secrets.toml.example إلى secrets.toml)

GEMINI_API_KEY = "ضع_مفتاحك_من_Google_AI_Studio"

# أو عدة مفاتيح:
# GEMINI_API_KEYS = '["KEY1","KEY2"]'

WEBHOOK_UPDATE_PRICES = "https://hook.eu2.make.com/مسار_الويب_هوك_الخاص_بك"

WEBHOOK_MISSING_PRODUCTS = "https://hook.eu2.make.com/مسار_الويب_هوك_للمفقودات"
# (اسم قديم مدعوم: WEBHOOK_NEW_PRODUCTS)
```

### 3️⃣ جرّب!
```bash
streamlit run app.py
```

---

## 🎯 سير العمل

### الخطوة 1: رفع الملفات
1. 📂 رفع الملفات
2. ملف مهووس (Excel/CSV)
3. ملفات المنافسين (1-5 ملفات)
4. 🚀 بدء التحليل

### الخطوة 2: مشاهدة النتائج
```
📊 لوحة التحكم
├── 🔴 سعر أعلى: 120
├── 🟢 سعر أقل: 85
├── ✅ موافق عليها: 980
├── 🔵 مفقودة: 69
└── ⚠️ مراجعة: 15
```

### الخطوة 3: كل قسم
1. فلاتر (بحث، ماركة، نوع، تطابق)
2. تصدير (Excel, CSV)
3. AI جماعي (أول 20)
4. إرسال Make

### الخطوة 4: المفقودات
1. 🖼️ صورة من Fragrantica
2. ✍️ وصف مهووس (جاهز للنسخ)
3. 💹 سعر السوق
4. 📤 إرسال Make

### الخطوة 5: AI
1. 💬 دردشة مع Gemini
2. 🔍 تحقق من منتجين
3. 💹 بحث سوق
4. 📊 أوامر مجمعة

---

## ✅ التحقق

| الميزة | الحالة |
|--------|---------|
| قسم AI (5 tabs) | ✅ |
| محرك v21 سريع | ✅ |
| CSV في كل قسم | ✅ |
| Fragrantica صور | ✅ |
| وصف مهووس | ✅ |
| Make.com + "no" | ✅ |
| عداد تقدم | ✅ |

---

## 🔧 المتطلبات

### ملف مهووس:
- ✅ عمود "المنتج" أو "Product"
- ✅ عمود "السعر" أو "Price"
- ✅ عمود "no" أو "ID" (**مهم جداً لـ Make!**)

### ملفات المنافسين:
- ✅ عمود المنتج
- ✅ عمود السعر

---

## 💡 نصائح

1. **عداد التقدم** يظهر أثناء التحليل
2. **CSV** موجود في كل قسم
3. **Fragrantica** في قسم المفقودات
4. **دردشة Gemini** في قسم AI
5. **لصق Excel** في tab "لصق وتحليل"

---

## 📞 الدعم

### مشاكل شائعة:
1. **ImportError** → تأكد من رفع مجلدات utils/ و engines/
2. **Gemini لا يعمل** → تحقق من المفاتيح في Secrets
3. **Make لا يستقبل** → تأكد من وجود عمود "no" في ملف مهووس

### Logs:
```bash
streamlit run app.py --logger.level=debug
```

---

## 🎉 الخلاصة

✅ **قسم AI كامل** (دردشة + لصق + تحقق + بحث سوق + أوامر)  
✅ **محرك v21** أسرع 5x  
✅ **CSV + Excel** في كل الأقسام  
✅ **Fragrantica** صور + مكونات  
✅ **وصف مهووس** احترافي  
✅ **Make.com** محسّن + "no"  
✅ **عداد تقدم** مباشر

---

Made with ❤️ for Mahwous Store
