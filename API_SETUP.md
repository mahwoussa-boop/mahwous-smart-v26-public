# 🤖 كيفية ربط الذكاء الاصطناعي (Google Gemini) بالمشروع

الكود يقرأ المفاتيح من `secrets.toml` أو متغيرات البيئة. **لا تضع مفاتيحاً حقيقية في هذا الملف عند المشاركة.**

### 1️⃣ الطريقة الأولى: عبر ملف `secrets.toml` (للتشغيل المحلي)
أنشئ مجلد `.streamlit` وانسخ `secrets.toml.example` إلى `secrets.toml` ثم عدّل القيم:
```toml
GEMINI_API_KEY = "YOUR_GEMINI_API_KEY_HERE"
```
*ملاحظة: `secrets.toml` مستبعد من Git عبر `.gitignore`.*

### 2️⃣ الطريقة الثانية: عبر متغيرات البيئة (Railway / Docker / إلخ)
- **Key:** `GEMINI_API_KEY`
- **Value:** مفتاحك من Google AI Studio

### 3️⃣ الطريقة الثالثة: عدة مفاتيح (لتقليل حدود الاستخدام)
```toml
GEMINI_API_KEYS = ["key1", "key2", "key3"]
```

---
**✅ `config.py` يقرأ هذه القيم بالترتيب الموثّق في الكود.**
