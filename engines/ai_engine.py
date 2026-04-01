"""
engines/ai_engine.py v26.0 — خبير مهووس الكامل
════════════════════════════════════════════════
✅ تسجيل الأخطاء الحقيقية (لا يبتلعها)
✅ تشخيص ذاتي لكل مزود AI
✅ خبير وصف منتجات مهووس الكامل (SEO + GEO)
✅ جلب صور المنتج من Fragrantica + Google
✅ بحث ويب DuckDuckGo + Gemini Grounding
✅ تحقق AI يُصحّح القسم الخاطئ
✅ تصنيف تلقائي لقسم "تحت المراجعة"
✅ v26.0: بحث أشمل في المتاجر السعودية مع تحليل JSON دقيق
"""
import base64
import os as _os
import requests, json, re, time, traceback
from config import get_gemini_api_keys, get_openrouter_api_key, get_cohere_api_key

try:
    from engines.engine import _clean_ai_json
except ImportError:
    from engine import _clean_ai_json

_GM  = "gemini-2.0-flash"  # ← النموذج المستقر الموصى به (يدعم الرؤية)
_GV = _os.environ.get("GEMINI_VISION_MODEL", _GM)
_GU  = f"https://generativelanguage.googleapis.com/v1beta/models/{_GM}:generateContent"
_GVU = f"https://generativelanguage.googleapis.com/v1beta/models/{_GV}:generateContent"
_OR  = "https://openrouter.ai/api/v1/chat/completions"

# نماذج افتراضية — بدون معرّفات أُزيلت من الطبقة المجانية (404 No endpoints).
# يمكن تجاوزها بالكامل عبر OPENROUTER_MODELS (مفصولة بفواصل) في البيئة.
_OPENROUTER_DEFAULT_MODELS = (
    "openrouter/free",
    "google/gemma-3-27b-it:free",
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemma-2-9b-it:free",
    "meta-llama/llama-3.2-3b-instruct:free",
)


def _openrouter_models_list():
    raw = _os.environ.get("OPENROUTER_MODELS", "").strip()
    if raw:
        return [m.strip() for m in raw.split(",") if m.strip()]
    return list(_OPENROUTER_DEFAULT_MODELS)


# توافق خلفي مع أي استيراد لاسم القائمة القديم
OPENROUTER_FALLBACK_MODELS = list(_OPENROUTER_DEFAULT_MODELS)
_CO  = "https://api.cohere.ai/v1/generate"

# Cohere اختياري — بعد 401 لا نكرر الاستدعاءات ولا نملأ سجل الأخطاء
_COHERE_KEY_INVALID = False

# ── سجل الأخطاء الأخيرة (يُعرض في صفحة التشخيص) ─────────────────────────
_LAST_ERRORS: list = []

def _log_err(source: str, msg: str):
    global _LAST_ERRORS
    entry = f"[{source}] {msg}"
    _LAST_ERRORS = ([entry] + _LAST_ERRORS)[:10]  # آخر 10 أخطاء

def get_last_errors() -> list:
    return _LAST_ERRORS.copy()

# ── تشخيص شامل لجميع مزودي AI ─────────────────────────────────────────────
def diagnose_ai_providers() -> dict:
    """
    يختبر كل مزود ويُعيد تقريراً مفصلاً بالأخطاء الحقيقية.
    يُستدعى من صفحة الإعدادات.
    """
    results = {}

    # ── Gemini ────────────────────────────────────────────────────────────
    gemini_results = []
    for i, key in enumerate(get_gemini_api_keys() or []):
        if not key:
            gemini_results.append({"key": i+1, "status": "❌ مفتاح فارغ"})
            continue
        try:
            payload = {
                "contents": [{"parts": [{"text": "test"}]}],
                "generationConfig": {"maxOutputTokens": 5}
            }
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=15)
            if r.status_code == 200:
                gemini_results.append({"key": i+1, "status": "✅ يعمل"})
            elif r.status_code == 400:
                try: msg = r.json().get("error",{}).get("message","Bad Request")
                except: msg = r.text[:100]
                gemini_results.append({"key": i+1, "status": f"❌ 400 — {msg[:80]}"})
            elif r.status_code == 403:
                gemini_results.append({"key": i+1, "status": "❌ 403 — مفتاح غير مصرح أو IP محظور"})
            elif r.status_code == 429:
                gemini_results.append({
                    "key": i+1,
                    "status": "⚠️ 429 — تجاوز الحد (انتظر أو جرّب مفتاحاً آخر / ارفع الحصة في Google AI)",
                })
            elif r.status_code == 404:
                gemini_results.append({"key": i+1, "status": f"❌ 404 — النموذج {_GM} غير متاح"})
            else:
                try: msg = r.json().get("error",{}).get("message","")
                except: msg = r.text[:100]
                gemini_results.append({"key": i+1, "status": f"❌ {r.status_code} — {msg[:80]}"})
        except requests.exceptions.ConnectionError as e:
            gemini_results.append({"key": i+1, "status": f"❌ لا يوجد اتصال بالإنترنت أو جدار حماية: {str(e)[:60]}"})
        except requests.exceptions.Timeout:
            gemini_results.append({"key": i+1, "status": "❌ انتهت المهلة (Timeout 15s)"})
        except Exception as e:
            gemini_results.append({"key": i+1, "status": f"❌ خطأ: {str(e)[:80]}"})
    results["gemini"] = gemini_results

    # ── OpenRouter — تجربة نماذج صالحة (المعرّفات القديمة مثل google/gemini-2.0-flash تُرفض بـ 400)
    _or_key = get_openrouter_api_key()
    if _or_key:
        or_ok = False
        last_or = ""
        for _model in _openrouter_models_list():
            try:
                r = requests.post(
                    _OR,
                    json={
                        "model": _model,
                        "messages": [{"role": "user", "content": "test"}],
                        "max_tokens": 5,
                    },
                    headers={
                        "Authorization": f"Bearer {_or_key}",
                        "HTTP-Referer": "https://mahwous.com",
                    },
                    timeout=15,
                )
                if r.status_code == 200:
                    results["openrouter"] = f"✅ يعمل (نموذج: {_model})"
                    or_ok = True
                    break
                if r.status_code == 401:
                    results["openrouter"] = "❌ 401 — مفتاح OpenRouter غير صحيح"
                    or_ok = True
                    break
                if r.status_code == 402:
                    results["openrouter"] = "❌ 402 — رصيد OpenRouter منتهٍ"
                    or_ok = True
                    break
                if r.status_code == 429:
                    last_or = f"⚠️ 429 — تجاوز الحد ({_model})"
                    continue
                try:
                    msg = r.json().get("error", {}).get("message", "")
                except Exception:
                    msg = r.text[:100]
                msg_l = (msg or "").lower()
                if r.status_code == 404 or "no endpoints" in msg_l:
                    continue
                last_or = f"❌ {r.status_code} — {msg[:100]} ({_model})"
            except requests.exceptions.ConnectionError:
                results["openrouter"] = "❌ لا اتصال بـ openrouter.ai — قد يكون محظوراً"
                or_ok = True
                break
            except requests.exceptions.Timeout:
                last_or = f"❌ Timeout ({_model})"
            except Exception as e:
                last_or = f"❌ {str(e)[:80]} ({_model})"
        if not or_ok:
            results["openrouter"] = last_or or (
                "❌ لا يوجد نموذج يعمل — راجع المعرفات على openrouter.ai أو OPENROUTER_MODELS"
            )
    else:
        results["openrouter"] = "⚠️ مفتاح غير موجود"

    # ── Cohere ────────────────────────────────────────────────────────────
    _ch_key = get_cohere_api_key()
    if _ch_key:
        try:
            r = requests.post("https://api.cohere.com/v2/chat", json={
                "model": "command-a-03-2025",
                "messages": [{"role": "user", "content": "test"}],
            }, headers={
                "Authorization": f"Bearer {_ch_key}",
                "Content-Type": "application/json",
            }, timeout=15)
            if r.status_code == 200:
                results["cohere"] = "✅ يعمل (command-a-03-2025)"
            elif r.status_code == 401:
                # اختياري — لا يُعرض كخطأ أحمر؛ التطبيق يعمل بدون Cohere
                results["cohere"] = (
                    "⚠️ 401 — مفتاح Cohere غير صالح (اختياري). "
                    "احذف COHERE_API_KEY من Secrets أو ضع مفتاحاً صحيحاً من dashboard.cohere.com"
                )
            elif r.status_code == 402:
                results["cohere"] = "❌ 402 — رصيد Cohere منتهٍ"
            else:
                try: msg = r.json().get("message","")
                except: msg = r.text[:100]
                results["cohere"] = f"❌ {r.status_code} — {msg[:80]}"
        except requests.exceptions.ConnectionError:
            results["cohere"] = "❌ لا اتصال بـ api.cohere.com"
        except Exception as e:
            results["cohere"] = f"❌ {str(e)[:80]}"
    else:
        results["cohere"] = "⚠️ مفتاح غير موجود"

    return results


def _vision_fetch_image(url: str):
    """تحميل صورة لـ Gemini inline_data. يعيد (bytes|None, mime)."""
    if not url or not str(url).startswith("http"):
        return None, "image/jpeg"
    try:
        r = requests.get(
            str(url).strip(),
            timeout=16,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            },
        )
        if r.status_code != 200 or not r.content:
            return None, "image/jpeg"
        ct = (r.headers.get("Content-Type") or "image/jpeg").split(";")[0].strip().lower()
        if "png" in ct:
            return r.content, "image/png"
        if "webp" in ct:
            return r.content, "image/webp"
        return r.content, "image/jpeg"
    except Exception:
        return None, "image/jpeg"


def vision_match_court(
    our_name: str,
    comp_name: str,
    our_price: float,
    comp_price: float,
    our_img_url: str,
    comp_img_url: str,
    fuzzy_score: float,
) -> dict:
    """
    المحكمة البصرية: مقارنة صورتي المنتج عبر Gemini Vision.
    يعيد: same_product (bool), reason (str), ok (نجح الطلب).
    """
    out: dict = {"same_product": False, "reason": "", "ok": False}
    keys = get_gemini_api_keys() or []
    if not keys:
        out["reason"] = "لا يوجد مفتاح Gemini"
        return out

    b1, m1 = _vision_fetch_image(our_img_url)
    b2, m2 = _vision_fetch_image(comp_img_url)
    if not b1 or not b2:
        out["reason"] = "تعذر تحميل إحدى الصورتين"
        return out

    prompt = (
        "بصفتك خبير عطور، انظر للصورتين واقرأ النصوص التالية.\n"
        f"منتجنا: {our_name} — السعر {our_price:.2f} ر.س\n"
        f"المنافس: {comp_name} — السعر {comp_price:.2f} ر.س\n"
        f"نسبة التطابق النصي السابقة: {fuzzy_score:.1f}%\n\n"
        "الصورة الأولى لمنتجنا، الثانية لمنتج المنافس.\n"
        "هل يمثلان نفس العطر بالتجزئة (نفس الحجم ml تقريباً، نفس التركيز EDP/EDT/EDC، وليس تستر مقابل عطر كامل)؟\n"
        'أجب JSON فقط بدون شرح إضافي: {"same_product":true أو false,"reason":"سبب مختصر بالعربية"}'
    )

    parts: list = [{"text": prompt}]
    parts.append({"inline_data": {"mime_type": m1, "data": base64.b64encode(b1).decode("ascii")}})
    parts.append({"inline_data": {"mime_type": m2, "data": base64.b64encode(b2).decode("ascii")}})

    payload = {
        "contents": [{"parts": parts}],
        "generationConfig": {"temperature": 0, "maxOutputTokens": 256, "topP": 1, "topK": 1},
    }

    for key in keys:
        if not key:
            continue
        try:
            r = requests.post(f"{_GVU}?key={key}", json=payload, timeout=45)
            if r.status_code != 200:
                continue
            txt = r.json()["candidates"][0]["content"]["parts"][0]["text"]
            clean = _clean_ai_json(txt)
            try:
                obj = json.loads(clean)
            except json.JSONDecodeError:
                continue
            if not isinstance(obj, dict):
                continue
            out["same_product"] = bool(obj.get("same_product"))
            out["reason"] = str(obj.get("reason", "")).strip() or "—"
            out["ok"] = True
            return out
        except Exception as e:
            _log_err("vision_match_court", str(e)[:120])
            continue

    out["reason"] = "فشل كل مفاتيح Gemini للمحكمة البصرية"
    return out


# ══ خبير وصف منتجات مهووس الكامل ══════════════════════════════════════════
MAHWOUS_EXPERT_SYSTEM = """أنت خبير عالمي في كتابة أوصاف منتجات العطور محسّنة لمحركات البحث التقليدية (Google SEO) ومحركات بحث الذكاء الصناعي (GEO/AIO). تعمل حصرياً لمتجر "مهووس" (Mahwous) - الوجهة الأولى للعطور الفاخرة في السعودية.---## هويتك ومهمتك**من أنت:**- خبير عطور محترف مع 15+ سنة خبرة في صناعة العطور الفاخرة- متخصص في SEO و Generative Engine Optimization (GEO)- كاتب محتوى عربي بارع بأسلوب راقٍ، ودود، عاطفي، وتسويقي مقنع- تمثل صوت متجر "مهووس" بكل احترافية وثقة**مهمتك:**كتابة أوصاف منتجات عطور شاملة، احترافية، ومحسّنة بشكل علمي صارم لتحقيق:1. تصدر نتائج البحث في Google (الصفحة الأولى)2. الظهور في إجابات محركات بحث الذكاء الصناعي (ChatGPT, Gemini, Perplexity)3. زيادة معدل التحويل (Conversion Rate) بنسبة 40-60%4. تعزيز ثقة العملاء (E-E-A-T: Experience, Expertise, Authoritativeness, Trustworthiness)---## القواعد العلمية الصارمة للكلمات المفتاحية### 1. هرمية الكلمات المفتاحية (إلزامية)**المستوى 1: الكلمة الرئيسية (Primary Keyword)**- الصيغة: "عطر [الماركة] [اسم العطر] [التركيز] [الحجم] [للجنس]"- مثال: "عطر أكوا دي بارما كولونيا إنتنسا أو دو كولون 180 مل للرجال"- التكرار: 5-7 مرات في وصف 1200 كلمة- الكثافة: 1.5-2%- المواقع الإلزامية:  * H1 (العنوان الرئيسي)  * أول 50 كلمة  * آخر 100 كلمة  * 2-3 عناوين فرعية  * قسم "لمسة خبير"**المستوى 2: الكلمات الثانوية (3 كلمات)**- أمثلة: "عطر رجالي خشبي"، "عطر فاخر ثابت"، "عطر رجالي للمكتب"- التكرار: 3-5 مرات لكل كلمة- الكثافة: 0.5-1% لكل كلمة- المواقع: العناوين الفرعية، النقاط النقطية، الفقرات الوصفية**المستوى 3: الكلمات الدلالية (LSI) (10-15 كلمة)**- الفئات:  * صفات: فاخر، راقٍ، أنيق، كلاسيكي، ثابت، فواح  * مكونات: برغموت، جلد، خشب الأرز، مسك، باتشولي  * أحاسيس: دافئ، منعش، حار، حمضي، ذكوري  * مناسبات: مكتب، رسمي، يومي، مساء، صيف، شتاء- التكرار: 2-3 مرات لكل كلمة- الكثافة: 0.3-0.5% لكل كلمة**المستوى 4: الكلمات الحوارية (5-8 عبارات)**- الأنماط:  * "أبحث عن عطر رجالي خشبي ثابت للعمل"  * "ما هو أفضل عطر رجالي حمضي للصيف"  * "هل يناسب [اسم العطر] الاستخدام اليومي"  * "الفرق بين EDC و EDP"- المواقع: FAQ، قسم "لمسة خبير"### 2. خريطة المواقع الاستراتيجية (إلزامية)**الأولوية القصوى (Critical Zones):****H1 (العنوان الرئيسي):**- يجب أن يطابق الكلمة الرئيسية 100%- صيغة: "عطر [الماركة] [اسم العطر] [التركيز] [الحجم] [للجنس]"**أول 100 كلمة (The Golden Paragraph):**- الكلمة الرئيسية في أول 50 كلمة- كلمة ثانوية واحدة على الأقل- 2-3 كلمات دلالية- أسلوب عاطفي جذاب- دعوة مبكرة للشراء- مثال: "قوة الحمضيات وعمق الجلد، توقيع خشبي فاخر للرجل الأنيق. عطر [الاسم الكامل] هو تحفة عطرية [جنسية الماركة] تجمع بين [مكون 1] و[مكون 2]. صدر عام [السنة] بتوقيع [المصمم]، ليمنحك حضوراً راقياً وثباتاً استثنائياً. هذا العطر [الجنس] الفاخر متوفر الآن حصرياً لدى مهووس بأفضل سعر. اشترِه الآن!"**العناوين الفرعية (H2/H3):**- 60% من العناوين يجب أن تحتوي على كلمات مفتاحية- أمثلة:  * "لماذا تختار عطر [الاسم] [الجنس]؟"  * "رحلة العطر: اكتشف الهرم العطري [العائلة العطرية] الفاخر"  * "متى وأين ترتدي هذا العطر [الجنس] الأنيق؟"  * "لمسة خبير من مهووس: تقييم احترافي لعطر [الاسم]"**النقاط النقطية:**- كل نقطة تبدأ بكلمة مفتاحية بولد- مثال: "**عطر رجالي خشبي فاخر:** يجمع بين..."**قسم FAQ:**- 6-8 أسئلة- كل سؤال = كلمة مفتاحية حوارية- الإجابة تكرر الكلمة المفتاحية مرة واحدة- الإجابة مفصلة (50-80 كلمة)**الفقرة الختامية (آخر 100 كلمة):**- الكلمة الرئيسية مرتين- كلمة ثانوية واحدة- دعوة قوية للشراء- تعزيز الثقة: "أصلي 100%"، "ضمان"، "آلاف العملاء"- الشعار: "عالمك العطري يبدأ من مهووس"---## بنية الوصف الإلزامية**الطول الإجمالي: 1200-1500 كلمة**### 1. الفقرة الافتتاحية (100-150 كلمة)- جملة افتتاحية عاطفية قوية- الكلمة الرئيسية كاملة في أول 50 كلمة- معلومات أساسية: الماركة، المصمم، سنة الإصدار، العائلة العطرية- دعوة مبكرة للشراء### 2. تفاصيل المنتج (نقاط نقطية)**العنوان:** "تفاصيل المنتج"- الماركة (مع رابط داخلي)- اسم العطر- المصمم/الموقّع- الجنس- العائلة العطرية- الحجم- التركيز- سنة الإصدار### 3. رحلة العطر: الهرم العطري (200-250 كلمة)**العنوان:** "رحلة العطر: اكتشف الهرم العطري [العائلة] الفاخر"- **النفحات العليا (Top Notes):** وصف حسي + المكونات- **النفحات الوسطى (Heart Notes):** وصف حسي + المكونات- **النفحات الأساسية (Base Notes):** وصف حسي + المكونات + معلومات الثبات**القاعدة:** استخدم لغة حسية عاطفية، ليس مجرد قائمة مكونات.### 4. لماذا تختار هذا العطر؟ (200-250 كلمة)**العنوان:** "لماذا تختار عطر [الاسم] [الجنس]؟"- 4-6 نقاط نقطية- كل نقطة تبدأ بكلمة مفتاحية بولد- تركز على الفوائد (Benefits) وليس الميزات (Features)- أمثلة:  * **توقيع عطري فريد:** ...  * **ثبات استثنائي طوال اليوم:** ...  * **حجم اقتصادي:** ...  * **مثالي للمكتب والمناسبات:** ...  * **عطر أصلي بسعر مميز:** ...### 5. متى وأين ترتدي هذا العطر؟ (150-200 كلمة) [جديد]**العنوان:** "متى وأين ترتدي عطر [الاسم] [الجنس]؟"- **الفصول المناسبة:** (مع تفسير)- **الأوقات المثالية:** (صباح، مساء، ليل)- **المناسبات:** (عمل، رسمي، كاجوال، رومانسي)- **الفئة العمرية:** (إن كان ذلك مناسباً)### 6. لمسة خبير من مهووس (200-250 كلمة) [إلزامي]**العنوان:** "لمسة خبير من مهووس: تقييمنا الاحترافي"- **الافتتاحية:** "بعد تجربتنا المعمقة لعطر [الاسم]، يمكننا القول بثقة..."- **التحليل الحسي:** وصف الافتتاحية، القلب، القاعدة من منظور الخبير- **الأداء:** الثبات (بالساعات)، الفوحان (ضعيف/متوسط/قوي)، الإسقاط- **المقارنات:** "إذا كنت من محبي [عطر مشابه 1] أو [عطر مشابه 2]، فإن [الاسم] سيكون..."- **التوصية:** "لمن نوصي به؟"- **نصيحة الخبير:** نصيحة عملية لأفضل استخدام**القاعدة:** استخدم ضمير "نحن"، اذكر تجربة فعلية، قدم نصيحة احترافية.### 7. الأسئلة الشائعة (FAQ) (250-300 كلمة)**العنوان:** "الأسئلة الشائعة حول عطر [الاسم]"- **6-8 أسئلة** (كل سؤال = كلمة مفتاحية حوارية)- أسئلة إلزامية:  1. "هل عطر [الاسم] مناسب للاستخدام اليومي في [المكان]؟"  2. "ما الفرق بين [التركيز الحالي] و[تركيز آخر]؟"  3. "ما هي مدة ثبات عطر [الاسم] على البشرة؟"  4. "هل يتوفر عطر [الاسم] كـ تستر؟"  5. "ما هو الفصل الأنسب لاستخدام عطر [الاسم]؟"  6. "هل عطر [الاسم] مناسب للمناسبات الرسمية؟"- أسئلة اختيارية:  7. "ما هي أفضل طريقة لرش عطر [الاسم] لأطول ثبات؟"  8. "هل يمكن دمج عطر [الاسم] مع عطور أخرى (Layering)؟"**القاعدة:** الإجابة 50-80 كلمة، تبدأ بـ "نعم/لا" عندما يكون مناسباً، تكرر الكلمة المفتاحية مرة واحدة.### 8. اكتشف أكثر من مهووس (100-120 كلمة)**العنوان:** "اكتشف المزيد من عطور [الجنس/الفئة]"- 3-5 روابط داخلية- كل رابط = Anchor Text محسّن (كلمة مفتاحية)- أمثلة:  * "تسوق المزيد من [عطور رجالية خشبية فاخرة](رابط)"  * "اكتشف [أفضل عطور [الماركة] للرجال](رابط)"  * "تصفح [عطور التستر الأصلية بأسعار مميزة](رابط)"  * "استكشف [عطور النيش الحصرية](رابط)"- **رابط خارجي واحد** (إلزامي):  * "اقرأ المزيد عن عطر [الاسم] على [Fragrantica Arabia](https://www.fragranticarabia.com/...)"### 9. الفقرة الختامية (80-100 كلمة)**العنوان:** "عالمك العطري يبدأ من مهووس"- الكلمة الرئيسية مرتين- كلمة ثانوية واحدة- تعزيز الثقة: "أصلي 100%"، "ضمان الأصالة"، "توصيل سريع"، "آلاف العملاء الراضين"- دعوة قوية للشراء: "اطلب الآن"، "اشترِ الآن"- الشعار: "عالمك العطري يبدأ من مهووس"---## الأسلوب الكتابي (إلزامي)### المزيج المطلوب:1. **راقٍ ومحترف** (40%): لغة فصحى سليمة، مصطلحات عطرية دقيقة2. **ودود وقريب** (25%): خطاب مباشر بضمير "أنت"، أسلوب محادثة3. **عاطفي ورومانسي** (20%): أوصاف حسية، استحضار مشاعر ومشاهد4. **تسويقي ومقنع** (15%): دعوات للشراء، تعزيز الثقة، خلق حاجة### القواعد الأسلوبية:- **لا تستخدم الإيموجي** (غير احترافي)- **استخدم Bold** للكلمات المفتاحية المهمة (لا تبالغ)- **تجنب التكرار الممل:** استخدم مرادفات- **اكتب بطبيعية:** لا حشو للكلمات المفتاحية- **استخدم أرقام وإحصائيات:** "ثبات 7-9 ساعات"، "فوحان متوسط إلى قوي"---## التعامل مع المدخلات### إذا أعطاك المستخدم:**1. معلومات كاملة (الاسم، الماركة، الحجم، السعر، الروابط):**- اكتب الوصف مباشرة بدون أسئلة- استخدم المعلومات المقدمة- ابحث في Fragrantica Arabia عن باقي التفاصيل**2. معلومات ناقصة (فقط الاسم والماركة):**- ابحث في Fragrantica Arabia عن:  * المصمم  * سنة الإصدار  * العائلة العطرية  * الهرم العطري  * الحجم الأكثر مبيعاً (إذا لم يحدد المستخدم)- ابحث في Google عن السعر التقريبي في السوق السعودي- اكتب الوصف بناءً على ما وجدته**3. فقط اسم العطر (بدون ماركة):**- ابحث في Google و Fragrantica لتحديد الماركة- ثم اتبع الخطوة 2### مصادر البحث (بالترتيب):1. **Fragrantica Arabia** (https://www.fragranticarabia.com/) - المصدر الأساسي2. **Google Search** - للأسعار والمعلومات الإضافية3. **موقع الماركة الرسمي** - للمعلومات الدقيقة---## التنسيق النهائي (إلزامي)### المخرجات يجب أن تكون:1. **جاهزة للنسخ واللصق مباشرة** (بدون شرح أو تعليمات)2. **بصيغة Markdown** مع العناوين والتنسيق3. **منظمة بالترتيب المذكور أعلاه**4. **الروابط جاهزة** (إذا قدمها المستخدم)### لا ترسل:- ❌ "هذا هو الوصف..."- ❌ "يمكنك نسخ..."- ❌ "ملاحظة: ..."- ❌ أي تعليمات أو شرح### فقط أرسل:- ✅ الوصف الكامل جاهز للاستخدام---## جدول التحقق النهائي (تحقق قبل الإرسال)قبل إرسال أي وصف، تأكد من:**الكلمات المفتاحية:**- [ ] الكلمة الرئيسية في H1- [ ] الكلمة الرئيسية في أول 50 كلمة- [ ] الكلمة الرئيسية في آخر 100 كلمة- [ ] الكلمة الرئيسية تكررت 5-7 مرات- [ ] 3 كلمات ثانوية (كل واحدة 3-5 مرات)- [ ] 10-15 كلمة دلالية (كل واحدة 2-3 مرات)- [ ] 5-8 عبارات حوارية في FAQ**البنية:**- [ ] الطول: 1200-1500 كلمة- [ ] 9 أقسام رئيسية (حسب البنية أعلاه)- [ ] قسم "لمسة خبير من مهووس" موجود- [ ] قسم "متى وأين ترتدي" موجود- [ ] FAQ يحتوي على 6-8 أسئلة- [ ] 3-5 روابط داخلية- [ ] 1 رابط خارجي (Fragrantica)**الأسلوب:**- [ ] مزيج: راقٍ + ودود + عاطفي + تسويقي- [ ] لا إيموجي- [ ] Bold للكلمات المهمة (بدون مبالغة)- [ ] 

## قواعد صارمة:
- اكتب باللغة العربية فقط
- الطول: 1200-1500 كلمة
- لا تختلق مكونات أو بيانات — ابنِ على الاسم فقط
- شخصيتك: الرجل الأنيق بالبدلة والغترة، خبير عطور متحمس
- لا تكتب JSON أو أكواد — نص مقروء فقط
"""

# ══ وصف منتجات مفقودة — «خبير مهووس» الموحّد + SEO (Gemini) ═════════════════
MISSING_PAGE_SYSTEM = """## الهوية والأسلوب (Persona)
أنت الآن خبير عطور سعودي محترف تعمل في متجر (مهووس - Mahwous). أسلوبك شاعري، واثق، دقيق تقنياً، ومقنع تسويقياً. لغتك العربية فصيحة بلمسة خليجية راقية، تركز على الأصالة والثبات والتميز. خاطب الذوق السعودي والخليجي (استخدم كلمات مثل: فوحان، ثبات، تميز).

## المهمة
توليد وصف تسويقي كامل لكل «عطر مفقود» (غير مدرج بعد في كتالوج مهووس). استخدم Markdown للعناوين والتنسيق. المحتوى العربي أولاً.

## الهيكل الإلزامي للوصف (Description Structure)
التزم بهذا الترتيب والعناوين:

### العنوان الجذاب
سطر واحد يجمع: اسم العطر + الماركة + الحجم + (تركيز العطر مثل EDP/EDT) حسب المدخلات.

### المقدمة الشاعرية
وصف حسي يلامس المشاعر (مثال أسلوبي: رحلة تأخذك إلى غابات الأرز…) دون مبالغة فارغة.

### الهرم العطري (الرحلة العطرية)
- **المقدمة (Top Notes):** المكونات الافتتاحية.
- **القلب (Heart Notes):** جوهر العطر.
- **القاعدة (Base Notes):** المكونات المستقرة والعميقة.

### لماذا تختار هذا العطر؟
أربع نقاط قوى واضحة تغطي: الثبات، الفوحان، التميز، القيمة.

### متى وأين ترتديه؟
الفصول المناسبة (صيف/شتاء)، الأوقات (صباح/مساء)، والمناسبات.

### لمسة خبير من مهووس
- تقييم **الفوحان** على مقياس من 1 إلى 10 مع جملة توضيحية قصيرة.
- تقييم **الثبات** على مقياس من 1 إلى 10 مع جملة توضيحية قصيرة.
- **نصيحة رش احترافية** (مثال: رشة على أماكن النبض لثبات يدوم).

### الأسئلة الشائعة
3 إلى 5 أسئلة تهم العميل السعودي عن العطر، مع إجابة مختصرة لكل سؤال.

## بيانات SEO الذكية (JSON)
بعد انتهاء الوصف بالكامل، اترك سطراً فارغاً ثم أخرج **كائناً JSON واحداً صالحاً فقط** (بدون ``` وبدون نص بعده) بالشكل:
{
  "page_title": "عنوان جذاب لمحركات البحث",
  "meta_description": "ملخص تسويقي حوالي 150 حرفاً",
  "url_slug": "perfume-slug-in-english-lowercase",
  "alt_text": "وصف مختصر للصورة بالعربية",
  "tags": ["ماركة", "عطر رجالي أو نسائي", "صيفي أو شتوي", "..."]
}

## قواعد صارمة (Strict Rules)
- **الأصالة:** أكد صراحةً في مكان مناسب أن العطر «أصلي 100%» من مصادر موثوقة عند التوريد من مهووس.
- **الدقة:** لا تخترع مكونات؛ استند إلى المعرفة الموثوقة عن العطر وإن وُجد بحث، وإن لم تتوفر تفاصيل الهرم بدقة فاذكر ذلك باختصار وصِغ وصفاً حسياً عاماً دون تسمية مكونات وهمية.
- **الجمهور:** لغة تناسب العميل السعودي والخليجي؛ ركز على فوحان وثبات وتميز.
- لا تستخدم إيموجي في الوصف النهائي. الـ url_slug وtags باللاتينية/الإنجليزية حيث يلزم للـ SEO."""


def _append_mahwous_kb(base: str) -> str:
    """يُلحق قاعدة روابط مهووس من data/mahwous_internal_links_kb.md عند وجود الملف."""
    path = _os.path.normpath(_os.path.join(_os.path.dirname(__file__), "..", "data", "mahwous_internal_links_kb.md"))
    try:
        with open(path, encoding="utf-8") as f:
            kb = f.read()
        if not kb.strip():
            return base
        return (
            base
            + "\n\n---\n## قاعدة روابط مهووس (استخدمها للروابط الداخلية في قسم «اكتشف المزيد»)\n"
            + kb
        )
    except OSError:
        return base


MAHWOUS_EXPERT_SYSTEM = _append_mahwous_kb(MAHWOUS_EXPERT_SYSTEM)
MISSING_PAGE_SYSTEM = _append_mahwous_kb(MISSING_PAGE_SYSTEM)

# ══ System Prompts للأقسام ══════════════════════════════════════════════════
PAGE_PROMPTS = {
"price_raise": """انت خبير تسعير عطور فاخرة (السوق السعودي) قسم سعر اعلى.
سعرنا اعلى من المنافس. قواعد: فرق<10 ابقاء | 10-30 مراجعة | >30 خفض فوري.
لكل منتج: 1.هل المطابقة صحيحة؟ 2.هل الفرق مبرر؟ 3.السعر المقترح.
اجب بالعربية بايجاز واحترافية.""",
"price_lower": """انت خبير تسعير عطور فاخرة (السوق السعودي) قسم سعر اقل.
سعرنا اقل من المنافس = فرصة ربح ضائعة. فرق<10 ابقاء | 10-50 رفع تدريجي | >50 رفع فوري.
لكل منتج: 1.هل يمكن رفع السعر؟ 2.السعر الامثل. اجب بالعربية بايجاز.""",
"approved": "انت خبير تسعير عطور. راجع المنتجات الموافق عليها وتاكد من استمرار صلاحيتها. اجب بالعربية.",
"missing": """أكمل المهمة بناءً على المدخلات. التزم بهيكل الوصف وSEO في النظام أعلاه.""",
"missing_analysis": """أنت مستشار تسعير واستيراد لمتجر مهووس. مهمتك تحليل قوائم المفقودات تشغيلياً (أولويات، ماركات، أسعار) وليس كتابة وصف منتج كامل.
أجب بالعربية بشكل منظم وواضح. لا تكرر هيكل «خبير الوصف» الطويل.""",
"review": """انت خبير تسعير عطور. هذه منتجات بمطابقة غير مؤكدة.
لكل منتج: هل هما نفس العطر فعلا؟ نعم / لا / غير متاكد. اشرح السبب. اجب بالعربية.""",
"general": """انت مساعد ذكاء اصطناعي متخصص في تسعير العطور الفاخرة والسوق السعودي.
خبرتك: تحليل الاسعار، المنافسة، استراتيجيات التسعير، مكونات العطور.
اجب بالعربية باحترافية وايجاز يمكنك استخدام markdown.""",
"verify": """انت خبير تحقق من منتجات العطور دقيق جدا.
تحقق من: الماركة + اسم المنتج + الحجم (ml) + النوع (EDP/EDT) + الجنس.
اجب JSON فقط بدون اي نص اضافي:
{"match":true/false,"confidence":0-100,"reason":"سبب واضح","correct_section":"احد الاقسام","suggested_price":0}""",
"market_search": """انت محلل اسعار عطور (السوق السعودي) تبحث في الانترنت.
اجب JSON فقط:
{"market_price":0,"price_range":{"min":0,"max":0},"competitors":[{"name":"","price":0}],"recommendation":"","confidence":0}""",
"reclassify": """انت نظام تصنيف دقيق لمنتجات العطور.
القسم الصحيح:
- سعر اعلى: نفس المنتج وسعرنا اعلى باكثر من 10 ريال
- سعر اقل: نفس المنتج وسعرنا اقل باكثر من 10 ريال
- موافق: الفرق 10 ريال او اقل + المطابقة صحيحة
- مفقود: المنتج غير موجود لدينا فعلا
اجب JSON فقط:
{"results":[{"idx":1,"section":"القسم","confidence":85,"match":true,"reason":""},...]}"""
}

# ══ استدعاءات AI ═══════════════════════════════════════════════════════════
def _call_gemini(prompt, system="", grounding=False, temperature=0.3, max_tokens=8192):
    full = f"{system}\n\n{prompt}" if system else prompt
    payload = {
        "contents": [{"parts": [{"text": full}]}],
        "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens, "topP": 0.85}
    }
    if grounding:
        payload["tools"] = [{"google_search": {}}]

    keys = get_gemini_api_keys()
    if not keys:
        _log_err("Gemini", "لا توجد مفاتيح API")
        return None

    for i, key in enumerate(keys):
        if not key:
            continue
        try:
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=45)
            if r.status_code == 200:
                data = r.json()
                if data.get("candidates"):
                    parts = data["candidates"][0]["content"]["parts"]
                    return "".join(p.get("text","") for p in parts)
                else:
                    # blocked / safety filter
                    reason = data.get("promptFeedback",{}).get("blockReason","")
                    _log_err("Gemini", f"مفتاح {i+1}: لا نتائج — {reason}")
            elif r.status_code == 429:
                _log_err("Gemini", f"مفتاح {i+1}: Rate Limit (429) — انتظار 2 ثانية")
                time.sleep(2)  # ← 2 ثانية للـ 429
                continue
            elif r.status_code == 403:
                _log_err("Gemini", f"مفتاح {i+1}: IP محظور أو مفتاح غير مصرح (403)")
            elif r.status_code == 404:
                _log_err("Gemini", f"مفتاح {i+1}: نموذج غير متاح {_GM} (404)")
            else:
                try:
                    msg = r.json().get("error",{}).get("message","")
                except Exception:
                    msg = r.text[:100]
                _log_err("Gemini", f"مفتاح {i+1}: {r.status_code} — {msg[:80]}")
        except requests.exceptions.ConnectionError as e:
            _log_err("Gemini", f"مفتاح {i+1}: لا اتصال — {str(e)[:80]}")
        except requests.exceptions.Timeout:
            _log_err("Gemini", f"مفتاح {i+1}: Timeout (45s)")
        except Exception as e:
            _log_err("Gemini", f"مفتاح {i+1}: {str(e)[:80]}")
    return None

def _call_openrouter(prompt, system=""):
    or_key = get_openrouter_api_key()
    if not or_key:
        return None

    msgs = []
    if system:
        msgs.append({"role": "system", "content": system})
    msgs.append({"role": "user", "content": prompt})

    models = _openrouter_models_list()
    logged_429 = False
    for model in models:
        try:
            r = requests.post(_OR, json={
                "model": model,
                "messages": msgs,
                "temperature": 0.3,
                "max_tokens": 8192
            }, headers={
                "Authorization": f"Bearer {or_key}",
                "HTTP-Referer": "https://mahwous.com",
                "X-Title": "Mahwous"
            }, timeout=45)

            if r.status_code == 200:
                content = r.json()["choices"][0]["message"]["content"]
                if content and content.strip():
                    return content
            elif r.status_code == 429:
                if not logged_429:
                    _log_err(
                        "OpenRouter",
                        "429 — تجاوز الحد؛ جرّب نموذجاً آخر في القائمة (بدون انتظار طويل)",
                    )
                    logged_429 = True
                # نموذج آخر قد يكون تحت حدّ مختلف — لا ننام هنا لتسريع السلسلة
                continue
            elif r.status_code == 402:
                _log_err("OpenRouter", f"{model}: رصيد منتهٍ (402) — جرب النموذج التالي")
                continue
            elif r.status_code == 401:
                _log_err("OpenRouter", "مفتاح غير صحيح (401)")
                return None  # لا فائدة من تجربة نماذج أخرى
            else:
                try:
                    msg = r.json().get("error", {}).get("message", "")
                except Exception:
                    msg = r.text[:100]
                msg_l = (msg or "").lower()
                if r.status_code == 404 or "no endpoints" in msg_l:
                    continue
                _log_err("OpenRouter", f"{model}: {r.status_code} — {msg[:80]}")
                continue

        except requests.exceptions.ConnectionError as e:
            _log_err("OpenRouter", f"لا اتصال — {str(e)[:80]}")
            return None  # إذا لا اتصال، لا فائدة من تجربة نماذج أخرى
        except requests.exceptions.Timeout:
            _log_err("OpenRouter", f"{model}: Timeout (45s)")
            continue
        except Exception as e:
            _log_err("OpenRouter", f"{model}: {str(e)[:80]}")
            continue

    return None

def _call_cohere(prompt, system=""):
    """
    Cohere — Fallback صامت فقط.
    أي خطأ (401/402/429/...) يُسجَّل ويُعاد None بدون إيقاف سير العمل.
    """
    global _COHERE_KEY_INVALID
    if _COHERE_KEY_INVALID:
        return None
    ch_key = get_cohere_api_key()
    if not ch_key:
        return None
    try:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        r = requests.post(
            "https://api.cohere.com/v2/chat",
            json={"model": "command-r-plus", "messages": messages, "temperature": 0.3},
            headers={"Authorization": f"Bearer {ch_key}",
                     "Content-Type": "application/json"},
            timeout=30
        )
        if r.status_code == 200:
            data = r.json()
            return data.get("message", {}).get("content", [{}])[0].get("text", "")
        elif r.status_code == 401:
            _COHERE_KEY_INVALID = True
            _log_err("Cohere", "مفتاح غير صحيح (401) — تجاوز Cohere")
            return None  # ← لا يوقف العمل، يمرر للـ fallback التالي
        elif r.status_code in (402, 403):
            _log_err("Cohere", f"غير مصرح ({r.status_code}) — تجاوز")
            return None
        elif r.status_code == 429:
            _log_err("Cohere", "Rate Limit (429) — انتظار 2 ثانية")
            time.sleep(2)
            return None
        else:
            try:   msg = r.json().get("message", "")
            except Exception: msg = r.text[:100]
            _log_err("Cohere", f"{r.status_code} — {msg[:80]}")
    except Exception as e:
        _log_err("Cohere", f"Fallback صامت — {str(e)[:60]}")
    return None

def _parse_json(txt):
    if not txt:
        return None
    try:
        clean = _clean_ai_json(txt)
        return json.loads(clean)
    except Exception as e:
        _log_err("_parse_json", str(e)[:120])
    return None

def _search_ddg(query, num_results=5):
    """بحث DuckDuckGo مجاني"""
    try:
        r = requests.get("https://api.duckduckgo.com/", params={
            "q": query, "format": "json", "no_html": "1", "skip_disambig": "1"
        }, timeout=8)
        if r.status_code == 200:
            data = r.json()
            results = []
            if data.get("AbstractText"):
                results.append({"snippet": data["AbstractText"], "url": data.get("AbstractURL","")})
            for rel in data.get("RelatedTopics", [])[:num_results]:
                if isinstance(rel, dict) and rel.get("Text"):
                    results.append({"snippet": rel.get("Text",""), "url": rel.get("FirstURL","")})
            return results
    except Exception:
        pass
    return []

def call_ai(prompt, page="general"):
    # «missing» فقط = وصف منتج مفقود كامل + SEO (البرومبت الموحّد لخبير مهووس)
    if page == "missing":
        sys = f"{MISSING_PAGE_SYSTEM}\n\nتعليمات إضافية:\n{PAGE_PROMPTS.get('missing', '')}"
    else:
        sys = PAGE_PROMPTS.get(page, PAGE_PROMPTS["general"])
    # بحث Google مع Gemini عند توليد وصف مفقود (مكونات موثوقة)
    _miss_ground = page == "missing"
    for fn, src in [
        (lambda: _call_gemini(prompt, sys, grounding=_miss_ground), "Gemini"),
        (lambda: _call_openrouter(prompt, sys), "OpenRouter"),
        (lambda: _call_cohere(prompt, sys), "Cohere")
    ]:
        r = fn()
        if r: return {"success":True,"response":r,"source":src}
    return {"success":False,"response":"فشل الاتصال بجميع مزودي AI","source":"none"}

# ══ Gemini Chat ══════════════════════════════════════════════════════════════
def gemini_chat(message, history=None, system_extra=""):
    sys = PAGE_PROMPTS["general"]
    if system_extra:
        sys = f"{sys}\n\nسياق: {system_extra}"
    needs_web = any(k in message.lower() for k in ["سعر","price","كم","متوفر","يباع","market","سوق","الان","اليوم","حالي","اخر","جديد"])
    contents = []
    for h in (history or [])[-12:]:
        contents.append({"role":"user","parts":[{"text":h["user"]}]})
        contents.append({"role":"model","parts":[{"text":h["ai"]}]})
    contents.append({"role":"user","parts":[{"text":f"{sys}\n\n{message}"}]})
    payload = {"contents":contents,
               "generationConfig":{"temperature":0.4,"maxOutputTokens":4096,"topP":0.9}}
    if needs_web:
        payload["tools"] = [{"google_search":{}}]
    for key in get_gemini_api_keys():
        if not key: continue
        try:
            r = requests.post(f"{_GU}?key={key}", json=payload, timeout=40)
            if r.status_code == 200:
                data = r.json()
                if data.get("candidates"):
                    parts = data["candidates"][0]["content"]["parts"]
                    text = "".join(p.get("text","") for p in parts)
                    return {"success":True,"response":text,
                            "source":"Gemini Flash" + (" + بحث ويب" if needs_web else "")}
            elif r.status_code == 429:
                time.sleep(1); continue
        except: continue
    r = _call_openrouter(message, sys)
    if r: return {"success":True,"response":r,"source":"OpenRouter"}
    return {"success":False,"response":"فشل الاتصال","source":"none"}

# ══ جلب صور المنتج من مصادر متعددة ══════════════════════════════════════════
def fetch_product_images(product_name, brand=""):
    """
    يجلب روابط صور المنتج من:
    1. Fragrantica Arabia (المصدر الأساسي)
    2. Google Images عبر Gemini Grounding
    3. DuckDuckGo كبديل
    يُرجع: {"images": [{"url":"...","source":"...","alt":"..."}], "fragrantica_url": "..."}
    """
    images = []
    fragrantica_url = ""

    # ── 1. Fragrantica Arabia (أفضل مصدر) ────────────────────────────────
    prompt_frag = f"""ابحث عن العطر "{product_name}" في موقع fragranticarabia.com وابحث أيضاً في fragrantica.com

أريد فقط:
1. رابط URL مباشر للصورة الرئيسية للعطر (يجب أن يكون رابط صورة حقيقي ينتهي بـ .jpg أو .png أو .webp)
2. روابط صور إضافية إذا وجدت (2-3 صور)
3. رابط صفحة المنتج على Fragrantica Arabia

أجب JSON فقط:
{{
  "main_image": "رابط URL الصورة الرئيسية المباشر",
  "extra_images": ["رابط2", "رابط3"],
  "fragrantica_url": "رابط الصفحة",
  "found": true/false
}}"""

    txt_frag = _call_gemini(prompt_frag, grounding=True)
    if txt_frag:
        data = _parse_json(txt_frag)
        if data and data.get("found") and data.get("main_image"):
            main = data["main_image"]
            if main and main.startswith("http") and any(ext in main.lower() for ext in [".jpg",".png",".webp",".jpeg"]):
                images.append({"url": main, "source": "Fragrantica Arabia", "alt": product_name})
            for extra in data.get("extra_images", []):
                if extra and extra.startswith("http") and len(images) < 4:
                    images.append({"url": extra, "source": "Fragrantica", "alt": product_name})
            fragrantica_url = data.get("fragrantica_url", "")

    # ── 2. Google Images عبر Gemini ───────────────────────────────────────
    if len(images) < 2:
        search_q = f"{product_name} {brand} perfume bottle official image site:sephora.com OR site:nocibé.fr OR site:parfumdreams.com"
        prompt_google = f"""ابحث عن صور المنتج: "{product_name}"
أريد روابط URL مباشرة لصور زجاجة العطر من المتاجر الرسمية مثل Sephora أو الموقع الرسمي للماركة.
الروابط يجب أن تنتهي بـ .jpg أو .png أو .webp وتكون صور حقيقية للمنتج.
أجب JSON: {{"images": ["رابط1","رابط2","رابط3"], "sources": ["مصدر1","مصدر2","مصدر3"]}}"""

        txt_google = _call_gemini(prompt_google, grounding=True)
        if txt_google:
            data2 = _parse_json(txt_google)
            if data2 and data2.get("images"):
                sources = data2.get("sources", [])
                for i, img_url in enumerate(data2["images"][:3]):
                    if img_url and img_url.startswith("http") and len(images) < 4:
                        src = sources[i] if i < len(sources) else "Google"
                        images.append({"url": img_url, "source": src, "alt": product_name})

    # ── 3. DuckDuckGo كبديل ───────────────────────────────────────────────
    if not images:
        ddg = _search_ddg(f"{product_name} perfume official image fragrantica")
        for r in ddg[:3]:
            url = r.get("url","")
            if url and any(ext in url.lower() for ext in [".jpg",".png",".webp"]):
                images.append({"url": url, "source": "DuckDuckGo", "alt": product_name})
                if len(images) >= 2: break

    # ── إذا لم نجد صور مباشرة، نُعيد رابط بحث ──────────────────────────
    if not images:
        search_url = f"https://www.fragranticarabia.com/?s={requests.utils.quote(product_name)}"
        images.append({
            "url": search_url,
            "source": "بحث Fragrantica",
            "alt": product_name,
            "is_search": True
        })

    return {
        "images": images,
        "fragrantica_url": fragrantica_url,
        "success": len(images) > 0
    }

# ══ جلب معلومات Fragrantica Arabia الكاملة ══════════════════════════════════
def fetch_fragrantica_info(product_name):
    """جلب صورة + مكونات + وصف من Fragrantica Arabia"""
    prompt = f"""ابحث عن العطر "{product_name}" في موقع fragranticarabia.com

احتاج:
1. رابط صورة المنتج المباشر (.jpg/.png/.webp)
2. مكونات العطر (top notes, middle notes, base notes)
3. وصف قصير بالعربية
4. الماركة والنوع (EDP/EDT) والحجم
5. رابط الصفحة

اجب JSON فقط:
{{
  "image_url": "رابط الصورة المباشر",
  "top_notes": ["مكون1","مكون2"],
  "middle_notes": ["مكون1","مكون2"],
  "base_notes": ["مكون1","مكون2"],
  "description_ar": "وصف قصير بالعربية",
  "brand": "",
  "type": "",
  "size": "",
  "year": "",
  "designer": "",
  "fragrance_family": "",
  "fragrantica_url": "رابط الصفحة"
}}"""

    txt = _call_gemini(prompt, grounding=True)
    if not txt: txt = _call_gemini(prompt)
    if not txt: return {"success":False}

    data = _parse_json(txt)
    if data: return {"success":True, **data}
    return {"success":False,"description_ar":txt[:200] if txt else ""}

# ══ خبير وصف مهووس الكامل (مع SEO + GEO) ══════════════════════════════════
def generate_mahwous_description(product_name, price, fragrantica_data=None, extra_info=None):
    """
    يولّد وصفاً احترافياً كاملاً بنظام خبير مهووس:
    - 1200-1500 كلمة
    - 9 أقسام: مقدمة + تفاصيل + هرم عطري + لماذا + متى/أين + لمسة خبير + FAQ + روابط + خاتمة
    - SEO محسّن + GEO محسّن
    - أسلوب مهووس: راقٍ + ودود + عاطفي + تسويقي
    """
    # جمع المعلومات المتاحة
    frag_info = ""
    if fragrantica_data and fragrantica_data.get("success"):
        top    = ", ".join(fragrantica_data.get("top_notes",[])[:5])
        mid    = ", ".join(fragrantica_data.get("middle_notes",[])[:5])
        base   = ", ".join(fragrantica_data.get("base_notes",[])[:5])
        desc   = fragrantica_data.get("description_ar","")
        brand  = fragrantica_data.get("brand","")
        ptype  = fragrantica_data.get("type","")
        size   = fragrantica_data.get("size","")
        year   = fragrantica_data.get("year","")
        designer = fragrantica_data.get("designer","")
        family = fragrantica_data.get("fragrance_family","")
        frag_url = fragrantica_data.get("fragrantica_url","")

        frag_info = f"""
معلومات من Fragrantica Arabia:
- الماركة: {brand}
- المصمم: {designer}
- سنة الإصدار: {year}
- العائلة العطرية: {family}
- الحجم: {size}
- التركيز: {ptype}
- النفحات العليا: {top}
- النفحات الوسطى: {mid}
- النفحات الأساسية: {base}
- الوصف: {desc}
- رابط Fragrantica: {frag_url}"""

    extra = ""
    if extra_info:
        extra = f"\nمعلومات إضافية: {extra_info}"

    prompt = f"""اكتب وصفاً احترافياً كاملاً لهذا العطر بتنسيق متجر مهووس:

**اسم المنتج:** {product_name}
**السعر:** {price:.0f} ريال سعودي
{frag_info}{extra}

اكتب وصفاً من 1200-1500 كلمة يتضمن الأقسام التسعة التالية:

## [عنوان المنتج — الكلمة الرئيسية الكاملة]

[فقرة افتتاحية عاطفية قوية — الكلمة الرئيسية في أول 50 كلمة — دعوة مبكرة للشراء]

## تفاصيل المنتج
[نقاط نقطية: الماركة، المصمم، الجنس، العائلة العطرية، الحجم، التركيز، سنة الإصدار]

## رحلة العطر: اكتشف الهرم العطري الفاخر
[النفحات العليا + الوسطى + الأساسية — وصف حسي عاطفي، ليس مجرد قائمة]

## لماذا تختار عطر {product_name}؟
[4-6 نقاط تبدأ بـ **كلمة مفتاحية بولد** — فوائد لا ميزات]

## متى وأين ترتدي هذا العطر؟
[الفصول + الأوقات المثالية + المناسبات + الفئة العمرية]

## لمسة خبير من مهووس: تقييمنا الاحترافي
[تحليل حسي بضمير "نحن" + الأداء بالساعات + مقارنات + توصية + نصيحة عملية]

## الأسئلة الشائعة حول عطر {product_name}
[6-8 أسئلة حوارية — كل سؤال = كلمة مفتاحية — إجابة 50-80 كلمة]

## اكتشف المزيد من عطور مهووس
[3-5 روابط داخلية + رابط Fragrantica Arabia]

## عالمك العطري يبدأ من مهووس
[الكلمة الرئيسية مرتين + تعزيز الثقة + دعوة قوية للشراء]

**ملاحظات مهمة:**
- لا تستخدم الإيموجي
- استخدم **Bold** للكلمات المفتاحية
- الكلمة الرئيسية 5-7 مرات في المجموع
- أسلوب: راقٍ + ودود + عاطفي + تسويقي
- اكتب الوصف مباشرة بدون أي شرح أو تعليمات"""

    # Gemini أولاً (مع Grounding إذا أمكن لجلب معلومات إضافية)
    txt = _call_gemini(prompt, MAHWOUS_EXPERT_SYSTEM, grounding=not bool(frag_info), max_tokens=8192)
    if not txt:
        txt = _call_gemini(prompt, MAHWOUS_EXPERT_SYSTEM, grounding=False, max_tokens=8192)
    if not txt:
        txt = _call_openrouter(prompt, MAHWOUS_EXPERT_SYSTEM)
    if not txt:
        txt = _call_cohere(prompt, MAHWOUS_EXPERT_SYSTEM)

    if txt:
        return txt
    return f"## {product_name}\n\nعطر فاخر من الدرجة الأولى متوفر الآن في مهووس.\n\n**السعر:** {price:.0f} ريال سعودي\n\nعالمك العطري يبدأ من مهووس!"

# ══ تحقق منتج + تحديد القسم الصحيح ════════════════════════════════════════
def verify_match(p1, p2, pr1=0, pr2=0):
    from utils.decision_labels import ui_decision_from_verify_section
    diff = pr1 - pr2 if pr1 > 0 and pr2 > 0 else 0
    if pr1 > 0 and pr2 > 0:
        if diff > 10:     expected = "سعر اعلى"
        elif diff < -10:  expected = "سعر اقل"
        else:             expected = "موافق"
    else:
        expected = "تحت المراجعة"

    prompt = f"""تحقق من تطابق هذين المنتجين بدقة متناهية (99.9%):
منتج 1 (مهووس): {p1} | السعر: {pr1:.0f} ريال
منتج 2 (المنافس): {p2} | السعر: {pr2:.0f} ريال

قواعد المطابقة الصارمة:
1. يجب أن تكون الماركة متطابقة تماماً.
2. يجب أن يكون اسم العطر متطابقاً (مثلاً: Sauvage ليس Sauvage Elixir).
3. يجب أن يكون الحجم متطابقاً (مثلاً: 100ml ليس 50ml).
4. يجب أن يكون التركيز متطابقاً (EDP ليس EDT).
5. يجب أن يكون الجنس متطابقاً (Men ليس Women).

إذا كانت كل الشروط أعلاه متوفرة، أجب بـ:
- القسم الصحيح = {expected}
خلاف ذلك، أجب بـ:
- القسم الصحيح = مفقود"""

    sys = PAGE_PROMPTS["verify"]
    txt = _call_gemini(prompt, sys, temperature=0.1) or _call_openrouter(prompt, sys)
    if not txt:
        return {"success":False,"match":False,"confidence":0,"reason":"فشل AI","correct_section":"تحت المراجعة","suggested_price":0,
                "ui_decision": ui_decision_from_verify_section("تحت المراجعة", False)}
    data = _parse_json(txt)
    if data:
        sec = data.get("correct_section","")
        if "اعلى" in sec or "أعلى" in sec: data["correct_section"] = "سعر اعلى"
        elif "اقل" in sec or "أقل" in sec:  data["correct_section"] = "سعر اقل"
        elif "موافق" in sec:                 data["correct_section"] = "موافق"
        elif "مفقود" in sec:                 data["correct_section"] = "مفقود"
        else: data["correct_section"] = expected if data.get("match") else "مفقود"
        data["ui_decision"] = ui_decision_from_verify_section(data.get("correct_section", ""), bool(data.get("match")))
        return {"success":True, **data}
    match = "true" in txt.lower() or "نعم" in txt
    cs = expected if match else "مفقود"
    return {"success":True,"match":match,"confidence":65,"reason":txt[:200],"correct_section":cs,"suggested_price":0,
            "ui_decision": ui_decision_from_verify_section(cs, match)}

# ══ إعادة تصنيف قسم "تحت المراجعة" ════════════════════════════════════════
def reclassify_review_items(items):
    if not items: return []
    lines = []
    for i, it in enumerate(items):
        diff = it.get("our_price",0) - it.get("comp_price",0)
        lines.append(f"[{i+1}] منتجنا: {it['our']} ({it.get('our_price',0):.0f}ر.س)"
                     f" vs منافس: {it['comp']} ({it.get('comp_price',0):.0f}ر.س) | فرق: {diff:+.0f}ر.س")
    prompt = f"""حلل هذه المنتجات وحدد القسم الصحيح لكل منها:
{chr(10).join(lines)}
- سعر اعلى: نفس المنتج + سعرنا اعلى بـ10+ ريال
- سعر اقل: نفس المنتج + سعرنا اقل بـ10+ ريال
- موافق: نفس المنتج + فرق 10 ريال او اقل
- مفقود: ليسا نفس المنتج"""
    sys = PAGE_PROMPTS["reclassify"]
    txt = _call_gemini(prompt, sys, temperature=0.1) or _call_openrouter(prompt, sys)
    if not txt: return []
    data = _parse_json(txt)
    if data and "results" in data:
        for r in data["results"]:
            sec = r.get("section","")
            if "اعلى" in sec or "أعلى" in sec: r["section"] = "🔴 سعر أعلى"
            elif "اقل" in sec or "أقل" in sec:  r["section"] = "🟢 سعر أقل"
            elif "موافق" in sec:                 r["section"] = "✅ موافق"
            elif "مفقود" in sec:                 r["section"] = "🔵 مفقود"
            else:                                 r["section"] = "⚠️ تحت المراجعة"
            try:
                r["confidence"] = float(r.get("confidence") or 0)
            except (TypeError, ValueError):
                r["confidence"] = 0.0
        return data["results"]
    return []


def apply_gemini_reclassify_to_analysis_df(analysis_df, min_confidence: float = 75.0, batch_size: int = 30):
    """
    بعد المطابقة: يمرّ على صفوف «⚠️ تحت المراجعة» ويطلب من Gemini إعادة التصنيف على دفعات.
    يحدّث عمود «القرار» في نفس DataFrame عند ثقة كافية (مثل زر المراجعة اليدوي).
    """
    if analysis_df is None or getattr(analysis_df, "empty", True):
        return analysis_df
    if "القرار" not in analysis_df.columns:
        return analysis_df
    try:
        mask = analysis_df["القرار"].astype(str).str.contains("مراجعة", na=False)
    except Exception:
        return analysis_df
    row_indices = analysis_df.index[mask].tolist()
    if not row_indices:
        return analysis_df

    for start in range(0, len(row_indices), batch_size):
        chunk_idx = row_indices[start : start + batch_size]
        items = []
        for ri in chunk_idx:
            r = analysis_df.loc[ri]
            try:
                op = float(r.get("السعر", 0) or 0)
            except (TypeError, ValueError):
                op = 0.0
            try:
                cp = float(r.get("سعر_المنافس", 0) or 0)
            except (TypeError, ValueError):
                cp = 0.0
            items.append({
                "our": str(r.get("المنتج", "")),
                "comp": str(r.get("منتج_المنافس", "")),
                "our_price": op,
                "comp_price": cp,
            })
        try:
            results = reclassify_review_items(items)
        except Exception:
            continue
        if not results:
            continue
        by_idx = {}
        for res in results:
            ix = res.get("idx")
            if ix is None:
                continue
            try:
                ix = int(ix)
            except (TypeError, ValueError):
                continue
            by_idx[ix] = res
        for pos, ri in enumerate(chunk_idx, start=1):
            res = by_idx.get(pos)
            if not res:
                continue
            sec = str(res.get("section", "") or "")
            try:
                conf = float(res.get("confidence", 0) or 0)
            except (TypeError, ValueError):
                conf = 0.0
            if sec and "مراجعة" not in sec and conf >= min_confidence:
                analysis_df.at[ri, "القرار"] = sec
    return analysis_df

# ══ بحث أسعار السوق ══════════════════════════════════════════════════════
def search_market_price(product_name, our_price=0):
    # البحث في أشهر المتاجر السعودية (سلة، زد، نايس ون، قولدن سنت، خبير العطور)
    queries = [
        f"سعر {product_name} السعودية نايس ون قولدن سنت سلة",
        f"سعر {product_name} في المتاجر السعودية 2026",
        f"مقارنة أسعار {product_name} السعودية",
        f"{product_name} price Saudi Arabia perfume shop",
    ]
    all_results = []
    for q in queries[:3]:  # استخدام أول 3 استعلامات
        ddg = _search_ddg(q)
        if ddg: all_results.extend(ddg[:3])
    
    web_ctx = "\n".join(f"- {r['title']}: {r['snippet'][:120]}" for r in all_results) if all_results else ""
    
    prompt = f"""تحليل سوق دقيق للمنتج في السعودية (مارس 2026):
المنتج: {product_name}
سعرنا الحالي: {our_price:.0f} ريال

المعلومات المستخرجة من الويب:
{web_ctx}

المطلوب تحليل JSON مفصل:
1. متوسط السعر في السوق السعودي.
2. أرخص سعر متاح حالياً واسم المتجر.
3. قائمة المنافسين المباشرين وأسعارهم (نايس ون، قولدن سنت، لودوريه، بيوتي ستور، إلخ).
4. حالة التوفر (متوفر/غير متوفر).
5. توصية تسعير ذكية لمتجر مهووس ليكون الأكثر تنافسية.
6. نسبة الثقة في البيانات (0-100)."""
    sys = PAGE_PROMPTS["market_search"]
    txt = _call_gemini(prompt, sys, grounding=True)
    if not txt: txt = _call_gemini(prompt, sys)
    if not txt: txt = _call_openrouter(prompt, sys)
    if not txt: return {"success":False,"market_price":0}
    data = _parse_json(txt)
    if data:
        data["web_context"] = web_ctx
        return {"success":True, **data}
    return {"success":True,"market_price":our_price,"recommendation":txt[:400],"web_context":web_ctx}

# ══ تحليل عميق ══════════════════════════════════════════════════════════════
def ai_deep_analysis(our_product, our_price, comp_product, comp_price, section="general", brand=""):
    diff = our_price - comp_price if our_price > 0 and comp_price > 0 else 0
    diff_pct = (abs(diff)/comp_price*100) if comp_price > 0 else 0
    ddg = _search_ddg(f"سعر {our_product} السعودية")
    web_ctx = "\n".join(f"- {r['snippet'][:80]}" for r in ddg[:2]) if ddg else ""
    guidance = {
        "🔴 سعر أعلى": f"سعرنا اعلى بـ{diff:.0f}ريال ({diff_pct:.1f}%). هل يجب خفضه؟",
        "🟢 سعر أقل":  f"سعرنا اقل بـ{abs(diff):.0f}ريال ({diff_pct:.1f}%). كم يمكن رفعه؟",
        "✅ موافق":     "السعر تنافسي. هل نحافظ عليه؟",
        "⚠️ تحت المراجعة": "المطابقة غير مؤكدة. هل هما نفس المنتج؟",
    }.get(section, "")
    prompt = f"""تحليل تسعير عميق:
منتجنا: {our_product} | سعرنا: {our_price:.0f} ريال
المنافس: {comp_product} | سعره: {comp_price:.0f} ريال
الفرق: {diff:+.0f} ريال | {diff_pct:.1f}% | {guidance}
{f"معلومات السوق:{chr(10)}{web_ctx}" if web_ctx else ""}
اجب بتقرير مختصر: هل المطابقة صحيحة؟ السعر المقترح بالرقم؟ الاجراء الفوري؟"""
    txt = _call_gemini(prompt, grounding=bool(web_ctx)) or _call_openrouter(prompt)
    if txt: return {"success":True,"response":txt,"source":"Gemini" + (" + ويب" if web_ctx else "")}
    return {"success":False,"response":"فشل التحليل"}

# ══ بحث mahwous.com ══════════════════════════════════════════════════════════
def search_mahwous(product_name):
    ddg = _search_ddg(f"site:mahwous.com {product_name}")
    web_ctx = "\n".join(r["snippet"][:100] for r in ddg[:2]) if ddg else ""
    prompt = f"""هل العطر {product_name} متوفر في متجر مهووس؟
{f"نتائج:{chr(10)}{web_ctx}" if web_ctx else ""}
اجب JSON: {{"likely_available":true/false,"confidence":0-100,"similar_products":[],
"add_recommendation":"عالية/متوسطة/منخفضة","reason":"","suggested_price":0}}"""
    txt = _call_gemini(prompt, grounding=True) or _call_gemini(prompt)
    if not txt: return {"success":False}
    data = _parse_json(txt)
    if data: return {"success":True, **data}
    return {"success":True,"likely_available":False,"confidence":50,"reason":txt[:150]}

# ══ تحقق مكرر ════════════════════════════════════════════════════════════════
def check_duplicate(product_name, our_products):
    if not our_products: return {"success":True,"response":"لا توجد بيانات"}
    prompt = f"""هل العطر {product_name} موجود بشكل مشابه في هذه القائمة؟
القائمة: {', '.join(str(p) for p in our_products[:30])}
اجب: نعم (وذكر اقرب مطابقة) او لا مع السبب."""
    return call_ai(prompt, "general")

# ══ وصف مفقود — خبير مهووس (Gemini + البرومبت الموحّد) ═══════════════════════
def generate_missing_product_description(
    product_name: str,
    *,
    brand: str = "",
    size_concentration: str = "",
    competitor_price: float = 0.0,
    competitor_name: str = "",
    extra_context: str = "",
) -> dict:
    """
    يولّد وصفاً كاملاً + JSON SEO عبر call_ai(page='missing') وMISSING_PAGE_SYSTEM.
    """
    prompt = f"""اكتب وصف المنتج المفقود التالي وفق النظام والهيكل المحددين.

**الاسم:** {product_name}
**الماركة:** {brand or "غير محدد"}
**الحجم والتركيز:** {size_concentration or "استخرجه من الاسم إن أمكن"}
**سعر مرجعي (منافس):** {competitor_price:.2f} ر.س إن وُجد
**اسم المنافس:** {competitor_name or "—"}
{f"**ملاحظات إضافية:** {extra_context}" if extra_context.strip() else ""}
"""
    return call_ai(prompt, "missing")

# ══ تحليل مجمع ════════════════════════════════════════════════════════════════
def bulk_verify(items, section="general"):
    if not items: return {"success":False,"response":"لا توجد منتجات"}
    lines = "\n".join(
        f"{i+1}. {it.get('our','')} vs {it.get('comp','')} | "
        f"سعرنا: {it.get('our_price',0):.0f} | منافس: {it.get('comp_price',0):.0f} | "
        f"فرق: {it.get('our_price',0)-it.get('comp_price',0):+.0f}"
        for i,it in enumerate(items))
    instructions = {
        "price_raise": "سعرنا اعلى. لكل منتج: هل المطابقة صحيحة؟ هل نخفض؟ السعر المقترح.",
        "price_lower": "سعرنا اقل = ربح ضائع. لكل منتج: هل يمكن رفعه؟ السعر الامثل.",
        "review": "مطابقات غير مؤكدة. لكل منتج: هل هما نفس العطر فعلا؟ نعم/لا/غير متاكد.",
        "approved": "منتجات موافق عليها. راجعها وتاكد انها لا تزال تنافسية.",
    }
    prompt = f"{instructions.get(section,'حلل واعط توصية.')}\n\nالمنتجات:\n{lines}"
    return call_ai(prompt, section)

# ══ معالجة النص الملصوق ═══════════════════════════════════════════════════
def analyze_paste(text, context=""):
    prompt = f"""المستخدم لصق هذا النص:
---
{text[:5000]}
---
حلل واستخرج: قائمة منتجات؟ اسعار؟ اوامر؟ اعط توصيات مفيدة. اجب بالعربية منظم."""
    return call_ai(prompt, "general")

# ══ دوال متوافقة مع app.py ════════════════════════════════════════════════
def chat_with_ai(msg, history=None, ctx=""): return gemini_chat(msg, history, ctx)
def analyze_product(p, price=0): return call_ai(f"حلل: {p} ({price:.0f}ريال)", "general")
def suggest_price(p, comp_price): return call_ai(f"اقترح سعرا لـ {p} بدلا من {comp_price:.0f}ريال", "general")
def process_paste(text): return analyze_paste(text)
