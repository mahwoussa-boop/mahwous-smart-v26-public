"""
utils/db_manager.py - v18.0
- تتبع تاريخ الأسعار (يحدث السعر إذا تغير)
- حفظ نقاط استئناف للمعالجة الخلفية
- قرارات لكل منتج (موافق/تأجيل/إزالة)
- سجل كامل بالتاريخ والوقت

مسار قاعدة البيانات (DB_PATH) — المصدر الوحيد: يُستورد من هنا في config.py
للأتمتة وباقي الوحدات حتى لا يُكتب سجل الأتمتة في ملف منفصل.
"""
import hashlib
import sqlite3, json, os
from datetime import datetime

# استخدام /tmp لضمان الكتابة على Streamlit Cloud (مجلد الكود read-only)
_DB_NAME = "pricing_v18.db"
DB_PATH = os.path.join("/tmp", _DB_NAME)


def _log_db_err(where: str, err: Exception) -> None:
    """تسجيل أخطاء المسارات الحرجة — لا يُبتلع الخطأ بصمت."""
    try:
        print(f"[ERROR] db_manager.{where}: {err}", flush=True)
    except Exception:
        pass


def _ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _date():
    return datetime.now().strftime("%Y-%m-%d")


def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    # WAL: يسمح بالقراءة والكتابة المتزامنة من threads مختلفة بدون تعارض
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")  # 30 ثانية انتظار بدل الخطأ الفوري
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

    # أحداث عامة
    c.execute("""CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, page TEXT,
        event_type TEXT, details TEXT,
        product_name TEXT, action_taken TEXT
    )""")

    # قرارات المستخدم (موافق/تأجيل/إزالة)
    c.execute("""CREATE TABLE IF NOT EXISTS decisions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, product_name TEXT,
        our_price REAL, comp_price REAL,
        diff REAL, competitor TEXT,
        old_status TEXT, new_status TEXT,
        reason TEXT, decided_by TEXT DEFAULT 'user'
    )""")

    # تاريخ الأسعار لكل منتج عند كل منافس
    c.execute("""CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT, product_name TEXT,
        competitor TEXT, price REAL,
        our_price REAL, diff REAL,
        match_score REAL, decision TEXT,
        product_id TEXT DEFAULT ''
    )""")

    # نقطة الاستئناف للمعالجة الخلفية
    c.execute("""CREATE TABLE IF NOT EXISTS job_progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id TEXT UNIQUE,
        started_at TEXT, updated_at TEXT,
        status TEXT DEFAULT 'running',
        total INTEGER DEFAULT 0,
        processed INTEGER DEFAULT 0,
        results_json TEXT DEFAULT '[]',
        missing_json TEXT DEFAULT '[]',
        our_file TEXT, comp_files TEXT
    )""")
    # إضافة عمود missing_json إذا لم يكن موجوداً (للتوافق مع قواعد البيانات القديمة)
    try:
        c.execute("ALTER TABLE job_progress ADD COLUMN missing_json TEXT DEFAULT '[]'")
    except sqlite3.OperationalError as e:
        if "duplicate column" not in str(e).lower():
            _log_db_err("init_db ALTER job_progress missing_json", e)

    # تاريخ التحليلات
    c.execute("""CREATE TABLE IF NOT EXISTS analysis_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, our_file TEXT,
        comp_file TEXT, total_products INTEGER,
        matched INTEGER, missing INTEGER, summary TEXT
    )""")

    # AI cache
    c.execute("""CREATE TABLE IF NOT EXISTS ai_cache (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT, prompt_hash TEXT UNIQUE,
        response TEXT, source TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS hidden_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        product_key TEXT UNIQUE,
        product_name TEXT,
        action TEXT DEFAULT 'hidden'
    )""")

    conn.commit()
    conn.close()


# ─── أحداث ────────────────────────────────
def log_event(page, event_type, details="", product_name="", action=""):
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO events (timestamp,page,event_type,details,product_name,action_taken) VALUES (?,?,?,?,?,?)",
            (_ts(), page, event_type, details, product_name, action)
        )
        conn.commit(); conn.close()
    except Exception as e:
        _log_db_err("log_event", e)


# ─── قرارات ────────────────────────────────
def log_decision(product_name, old_status, new_status, reason="",
                 our_price=0, comp_price=0, diff=0, competitor=""):
    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO decisions
               (timestamp,product_name,our_price,comp_price,diff,competitor,
                old_status,new_status,reason)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (_ts(), product_name, our_price, comp_price, diff,
             competitor, old_status, new_status, reason)
        )
        conn.commit(); conn.close()
    except Exception as e:
        _log_db_err("log_decision", e)


def get_decisions(product_name=None, status=None, limit=100):
    try:
        conn = get_db()
        if product_name:
            rows = conn.execute(
                "SELECT * FROM decisions WHERE product_name LIKE ? ORDER BY id DESC LIMIT ?",
                (f"%{product_name}%", limit)
            ).fetchall()
        elif status:
            rows = conn.execute(
                "SELECT * FROM decisions WHERE new_status=? ORDER BY id DESC LIMIT ?",
                (status, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM decisions ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        _log_db_err("get_decisions", e)
        return []


# ─── تاريخ الأسعار (الميزة الذكية) ──────────
def upsert_price_history(product_name, competitor, price,
                          our_price=0, diff=0, match_score=0,
                          decision="", product_id=""):
    """
    يحفظ السعر اليوم. إذا وُجد سعر سابق لنفس المنتج/المنافس اليوم → يحدّثه.
    إذا كان أمس → يضيف سجلاً جديداً لتتبع التغيير.
    يرجع True إذا تغير السعر عن آخر تسجيل.
    """
    conn = get_db()
    today = _date()

    # آخر سعر مسجل لهذا المنتج/المنافس
    last = conn.execute(
        """SELECT price, date FROM price_history
           WHERE product_name=? AND competitor=?
           ORDER BY id DESC LIMIT 1""",
        (product_name, competitor)
    ).fetchone()

    price_changed = False
    if last:
        last_price = last["price"]
        last_date  = last["date"]
        price_changed = abs(float(price) - float(last_price)) > 0.01

        if last_date == today:
            # نفس اليوم → حدّث فقط
            conn.execute(
                """UPDATE price_history SET price=?,our_price=?,diff=?,
                   match_score=?,decision=?,product_id=?
                   WHERE product_name=? AND competitor=? AND date=?""",
                (price, our_price, diff, match_score, decision,
                 product_id, product_name, competitor, today)
            )
        else:
            # يوم جديد → أضف سجل
            conn.execute(
                """INSERT INTO price_history
                   (date,product_name,competitor,price,our_price,diff,
                    match_score,decision,product_id)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (today, product_name, competitor, price, our_price,
                 diff, match_score, decision, product_id)
            )
    else:
        # أول مرة
        conn.execute(
            """INSERT INTO price_history
               (date,product_name,competitor,price,our_price,diff,
                match_score,decision,product_id)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (today, product_name, competitor, price, our_price,
             diff, match_score, decision, product_id)
        )

    conn.commit(); conn.close()
    return price_changed


def get_price_history(product_name, competitor="", limit=30):
    try:
        conn = get_db()
        if competitor:
            rows = conn.execute(
                """SELECT * FROM price_history
                   WHERE product_name=? AND competitor=?
                   ORDER BY date DESC LIMIT ?""",
                (product_name, competitor, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT * FROM price_history WHERE product_name=?
                   ORDER BY date DESC LIMIT ?""",
                (product_name, limit)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        _log_db_err("get_price_history", e)
        return []


def get_price_changes(days=7):
    """منتجات تغير سعرها خلال X يوم"""
    try:
        conn = get_db()
        rows = conn.execute(
            """SELECT p1.product_name, p1.competitor,
                      p1.price as new_price, p2.price as old_price,
                      p1.date as new_date, p2.date as old_date,
                      (p1.price - p2.price) as price_diff
               FROM price_history p1
               JOIN price_history p2
                 ON p1.product_name=p2.product_name
                AND p1.competitor=p2.competitor
                AND p1.id > p2.id
               WHERE p1.date >= date('now', ?)
                 AND abs(p1.price - p2.price) > 0.01
               ORDER BY abs(p1.price - p2.price) DESC
               LIMIT 100""",
            (f"-{days} days",)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        _log_db_err("get_price_changes", e)
        return []


# ─── المعالجة الخلفية ──────────────────────
def save_job_progress(job_id, total, processed, results, status="running",
                      our_file="", comp_files="", missing=None):
    missing_data = json.dumps(missing if missing else [], ensure_ascii=False, default=str)
    results_data = json.dumps(results, ensure_ascii=False, default=str)
    with sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        conn.execute(
            """INSERT OR REPLACE INTO job_progress
               (job_id,started_at,updated_at,status,total,processed,
                results_json,missing_json,our_file,comp_files)
               VALUES (?,
                   COALESCE((SELECT started_at FROM job_progress WHERE job_id=?), ?),
                   ?, ?, ?, ?, ?, ?, ?, ?)""",
            (job_id, job_id, _ts(), _ts(), status, total, processed,
             results_data, missing_data, our_file, comp_files)
        )
        conn.commit()


def get_job_progress(job_id):
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT * FROM job_progress WHERE job_id=?", (job_id,)
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try: d["results"] = json.loads(d.get("results_json", "[]"))
            except Exception:
                d["results"] = []
            try: d["missing"] = json.loads(d.get("missing_json", "[]"))
            except Exception:
                d["missing"] = []
            return d
    except Exception as e:
        _log_db_err("get_job_progress", e)
    return None


def get_last_job():
    try:
        conn = get_db()
        row = conn.execute(
            "SELECT * FROM job_progress ORDER BY id DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if row:
            d = dict(row)
            try: d["results"] = json.loads(d.get("results_json", "[]"))
            except Exception:
                d["results"] = []
            try: d["missing"] = json.loads(d.get("missing_json", "[]"))
            except Exception:
                d["missing"] = []
            return d
    except Exception as e:
        _log_db_err("get_last_job", e)
    return None


# ─── سجل التحليلات ─────────────────────────
def log_analysis(our_file, comp_file, total, matched, missing, summary=""):
    try:
        conn = get_db()
        conn.execute(
            """INSERT INTO analysis_history
               (timestamp,our_file,comp_file,total_products,matched,missing,summary)
               VALUES (?,?,?,?,?,?,?)""",
            (_ts(), our_file, comp_file, total, matched, missing, summary)
        )
        conn.commit(); conn.close()
    except Exception as e:
        _log_db_err("log_analysis", e)


def get_analysis_history(limit=20):
    try:
        conn = get_db()
        rows = conn.execute(
            "SELECT * FROM analysis_history ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        _log_db_err("get_analysis_history", e)
        return []


def get_events(page=None, limit=50):
    try:
        conn = get_db()
        if page:
            rows = conn.execute(
                "SELECT * FROM events WHERE page=? ORDER BY id DESC LIMIT ?",
                (page, limit)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        _log_db_err("get_events", e)
        return []


# ── دوال المنتجات المخفية الدائمة ──────────────────────
def save_hidden_product(product_key: str, product_name: str = "", action: str = "hidden"):
    """يحفظ منتجاً مخفياً في قاعدة البيانات بشكل دائم"""
    try:
        conn = get_db()
        conn.execute(
            """INSERT OR REPLACE INTO hidden_products
               (timestamp, product_key, product_name, action)
               VALUES (?, ?, ?, ?)""",
            (_ts(), product_key, product_name, action)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        _log_db_err("save_hidden_product", e)


def get_hidden_product_keys() -> set:
    """يُرجع مجموعة كل مفاتيح المنتجات المخفية من قاعدة البيانات"""
    try:
        conn = get_db()
        rows = conn.execute("SELECT product_key FROM hidden_products").fetchall()
        conn.close()
        return {r["product_key"] for r in rows}
    except Exception as e:
        _log_db_err("get_hidden_product_keys", e)
        return set()


init_db()


def comp_row_dedupe_key(
    competitor: str, norm_name: str, price: float, raw_product_id: str, image_url: str = ""
) -> str:
    """مفتاح فريد لكل صف منافس: رقم المنتج إن وُجد، وإلا تجزئة مستقرة (اسم+سعر+صورة+منافس)."""
    pid = str(raw_product_id or "").strip().rstrip(".0")
    if pid and pid.lower() not in ("nan", "none", "0", ""):
        return pid[:200]
    base = f"{competitor}|{norm_name}|{str(image_url or '')[:200]}|{float(price):.6f}"
    return "h:" + hashlib.sha256(base.encode("utf-8")).hexdigest()[:32]


# ═══════════════════════════════════════════════════════════════
#  v26 — Upsert Catalog + Processed Products
# ═══════════════════════════════════════════════════════════════

def init_db_v26(conn=None):
    """إضافة جداول v26 للـ upsert ومتابعة المنتجات المعالجة"""
    c_conn = conn or get_db()
    cur = c_conn.cursor()

    # كتالوج مؤقت للمنافسين (يُحدَّث يومياً) — مفتاح فريد comp_product_key يمنع دمج منتجين مختلفين بنفس الاسم
    cur.execute("""CREATE TABLE IF NOT EXISTS comp_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        competitor TEXT NOT NULL,
        product_name TEXT NOT NULL,
        norm_name TEXT,
        price REAL,
        product_id TEXT DEFAULT '',
        image_url TEXT DEFAULT '',
        comp_product_key TEXT NOT NULL,
        first_seen TEXT,
        last_seen TEXT,
        UNIQUE(competitor, comp_product_key)
    )""")

    # كتالوج متجرنا (يُحدَّث يومياً)
    cur.execute("""CREATE TABLE IF NOT EXISTS our_catalog (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id TEXT UNIQUE,
        product_name TEXT NOT NULL,
        norm_name TEXT,
        price REAL,
        first_seen TEXT,
        last_seen TEXT
    )""")

    # المنتجات المعالجة (ترحيل/تسعير/إضافة)
    cur.execute("""CREATE TABLE IF NOT EXISTS processed_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        product_key TEXT UNIQUE,
        product_name TEXT,
        competitor TEXT,
        action TEXT,
        old_price REAL,
        new_price REAL,
        product_id TEXT,
        notes TEXT
    )""")

    c_conn.commit()
    if not conn:
        c_conn.close()


def upsert_our_catalog(our_df, name_col="اسم المنتج", id_col="رقم المنتج", price_col="السعر"):
    """يُحدِّث كتالوج متجرنا عند كل رفع جديد — بدون تكرار"""
    import re
    conn = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    rows_updated = 0
    rows_inserted = 0

    for _, row in our_df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if not name:
            continue
        norm = re.sub(r'\s+', ' ', name.lower().strip())
        pid  = str(row.get(id_col, "")).strip().rstrip(".0")
        try:
            price = float(str(row.get(price_col, 0)).replace(",", ""))
        except Exception:
            price = 0.0

        existing = conn.execute(
            "SELECT id, price FROM our_catalog WHERE product_id=? OR norm_name=?",
            (pid, norm)
        ).fetchone()

        if existing:
            conn.execute(
                "UPDATE our_catalog SET price=?, last_seen=?, norm_name=? WHERE id=?",
                (price, today, norm, existing[0])
            )
            rows_updated += 1
        else:
            conn.execute(
                """INSERT INTO our_catalog (product_id, product_name, norm_name, price, first_seen, last_seen)
                   VALUES (?,?,?,?,?,?)""",
                (pid, name, norm, price, today, today)
            )
            rows_inserted += 1

    conn.commit()
    conn.close()
    return {"updated": rows_updated, "inserted": rows_inserted}


def upsert_comp_catalog(comp_dfs: dict):
    """يُحدِّث كتالوج المنافسين — بدون دمج صفين مختلفين تحت نفس norm_name فقط."""
    import re
    conn = get_db()
    today = datetime.now().strftime("%Y-%m-%d")
    total_new = 0

    for cname, cdf in comp_dfs.items():
        cols = list(cdf.columns)
        name_col = price_col = img_col = id_col = None
        for c in cols:
            cs = str(c)
            if id_col is None and any(
                k in cs for k in ("رقم المنتج", "معرف", "product_id", "SKU", "sku", "رقم_المنتج")
            ):
                id_col = c
            if img_col is None and any(
                k in cs for k in ("رابط_الصورة", "صورة", "image", "Image")
            ):
                img_col = c
            sample = str(cdf[c].dropna().iloc[0]) if not cdf[c].dropna().empty else ""
            try:
                float(sample.replace(",", ""))
                if price_col is None:
                    price_col = c
            except Exception:
                if name_col is None and len(sample) > 5:
                    name_col = c

        if name_col is None:
            name_col = cols[0]
        if price_col is None:
            price_col = cols[1] if len(cols) > 1 else cols[0]

        for _, row in cdf.iterrows():
            name = str(row.get(name_col, "")).strip()
            if not name or len(name) < 4 or name.startswith("styles_"):
                continue
            norm = re.sub(r"\s+", " ", name.lower().strip())
            try:
                price = float(str(row.get(price_col, 0)).replace(",", ""))
            except Exception:
                price = 0.0
            raw_pid = ""
            if id_col:
                raw_pid = str(row.get(id_col, "") or "").strip()
            img = ""
            if img_col:
                img = str(row.get(img_col, "") or "").strip()
            ckey = comp_row_dedupe_key(cname, norm, price, raw_pid, img)

            existing = conn.execute(
                "SELECT id FROM comp_catalog WHERE competitor=? AND comp_product_key=?",
                (cname, ckey),
            ).fetchone()

            if existing:
                conn.execute(
                    """UPDATE comp_catalog SET price=?, last_seen=?, product_name=?, norm_name=?,
                       product_id=?, image_url=? WHERE id=?""",
                    (price, today, name, norm, raw_pid[:200], img[:2000], existing[0]),
                )
            else:
                conn.execute(
                    """INSERT INTO comp_catalog (
                        competitor, product_name, norm_name, price, product_id, image_url,
                        comp_product_key, first_seen, last_seen)
                       VALUES (?,?,?,?,?,?,?,?,?)""",
                    (cname, name, norm, price, raw_pid[:200], img[:2000], ckey, today, today),
                )
                total_new += 1

    conn.commit()
    conn.close()
    return {"new_products": total_new}


def load_comp_catalog_grouped(exclude_competitors: set[str] | None = None) -> dict:
    """يجمّع صفوف comp_catalog إلى {اسم_المنافس: DataFrame} بأعمدة متوافقة مع كشط CSV (ومنها رابط_الصورة)."""
    import pandas as pd

    conn = get_db()
    try:
        cur = conn.execute("PRAGMA table_info(comp_catalog)")
        col_names = {r[1] for r in cur.fetchall()}
        has_img = "image_url" in col_names
        has_pid = "product_id" in col_names
        sel = "SELECT competitor, product_name, price"
        sel += ", image_url" if has_img else ", ''"
        sel += ", product_id" if has_pid else ", ''"
        sel += " FROM comp_catalog WHERE 1=1 "
        if exclude_competitors:
            xs = tuple(exclude_competitors)
            placeholders = ",".join("?" * len(xs))
            sel += f" AND competitor NOT IN ({placeholders})"
            rows = conn.execute(sel + " ORDER BY competitor, id", xs).fetchall()
        else:
            rows = conn.execute(sel + " ORDER BY competitor, id").fetchall()
    finally:
        conn.close()

    by: dict[str, list] = {}
    for tup in rows:
        competitor, product_name, price = tup[0], tup[1], tup[2]
        img_v = str(tup[3] or "") if len(tup) > 3 else ""
        pid_v = str(tup[4] or "") if len(tup) > 4 else ""
        try:
            pr = float(price) if price is not None else 0.0
        except (TypeError, ValueError):
            pr = 0.0
        by.setdefault(str(competitor), []).append(
            {
                "اسم المنتج": str(product_name or ""),
                "السعر": pr,
                "رقم المنتج": pid_v,
                "رابط_الصورة": img_v,
            }
        )
    return {k: pd.DataFrame(v) for k, v in by.items() if v}


def merged_comp_dfs_for_analysis(comp_key: str, fresh_df) -> dict:
    """يدمج بيانات المنافس الحالي (كشط حي أو ملف) مع المنافسين الآخرين الموجودين في comp_catalog."""
    import pandas as pd

    ck = (comp_key or "Scraped_Competitor").strip() or "Scraped_Competitor"
    others = load_comp_catalog_grouped(exclude_competitors={ck})
    out = dict(others)
    if fresh_df is not None and not getattr(fresh_df, "empty", True):
        out[ck] = fresh_df.copy()
    elif ck not in out:
        out[ck] = pd.DataFrame(columns=["اسم المنتج", "السعر", "رقم المنتج", "رابط_الصورة"])
    return out


def load_all_comp_catalog_as_comp_dfs() -> dict:
    """جميع المنافسين المخزّنين في comp_catalog — لملء session_state بعد التحليل."""
    return load_comp_catalog_grouped(exclude_competitors=None)


def save_processed(product_key: str, product_name: str, competitor: str,
                   action: str, old_price=0.0, new_price=0.0,
                   product_id="", notes=""):
    """يحفظ منتجاً في قائمة المعالجة — مع منع التكرار، آمن للثريدات"""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA busy_timeout=30000;")
            conn.execute(
                """INSERT OR REPLACE INTO processed_products
                   (timestamp, product_key, product_name, competitor, action,
                    old_price, new_price, product_id, notes)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (_ts(), product_key, product_name, competitor, action,
                 old_price, new_price, product_id, notes)
            )
            conn.commit()
    except Exception as e:
        _log_db_err("save_processed", e)


def get_processed(limit=200) -> list:
    """يُعيد قائمة المنتجات المعالجة"""
    conn = get_db()
    rows = conn.execute(
        """SELECT timestamp, product_key, product_name, competitor,
                  action, old_price, new_price, product_id, notes
           FROM processed_products ORDER BY timestamp DESC LIMIT ?""",
        (limit,)
    ).fetchall()
    conn.close()
    keys = ["timestamp","product_key","product_name","competitor",
            "action","old_price","new_price","product_id","notes"]
    return [dict(zip(keys, r)) for r in rows]


def undo_processed(product_key: str) -> bool:
    """تراجع: إزالة المنتج من قائمة المعالجة"""
    conn = get_db()
    conn.execute("DELETE FROM processed_products WHERE product_key=?", (product_key,))
    conn.execute("DELETE FROM hidden_products WHERE product_key=?", (product_key,))
    conn.commit()
    conn.close()
    return True


def get_processed_keys() -> set:
    """مفاتيح المنتجات المعالجة لاستبعادها من القوائم"""
    conn = get_db()
    rows = conn.execute("SELECT product_key FROM processed_products").fetchall()
    conn.close()
    return {r[0] for r in rows}


# ═══════════════════════════════════════════════════════════════
#  v26.0 — Migration Script + Automation Log
# ═══════════════════════════════════════════════════════════════
def _migrate_comp_catalog_if_needed(cur) -> None:
    """يحوّل comp_catalog القديم (UNIQUE competitor+norm_name) إلى نسخة تحتوي صورة ومفتاح فريد."""
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='comp_catalog'")
        if not cur.fetchone():
            return
        cur.execute("PRAGMA table_info(comp_catalog)")
        cols = [r[1] for r in cur.fetchall()]
        if "comp_product_key" in cols and "image_url" in cols:
            return
        cur.execute("DROP TABLE IF EXISTS comp_catalog__new")
        cur.execute(
            """CREATE TABLE comp_catalog__new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            competitor TEXT NOT NULL,
            product_name TEXT NOT NULL,
            norm_name TEXT,
            price REAL,
            product_id TEXT DEFAULT '',
            image_url TEXT DEFAULT '',
            comp_product_key TEXT NOT NULL,
            first_seen TEXT,
            last_seen TEXT,
            UNIQUE(competitor, comp_product_key)
        )"""
        )
        cur.execute(
            "SELECT competitor, product_name, norm_name, price, first_seen, last_seen FROM comp_catalog"
        )
        for row in cur.fetchall():
            comp, pname, norm, price, fs, ls = (
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
            )
            norm = norm or ""
            price = float(price or 0)
            ck = comp_row_dedupe_key(str(comp), str(norm), price, "", "")
            cur.execute(
                """INSERT INTO comp_catalog__new (
                    competitor, product_name, norm_name, price, product_id, image_url,
                    comp_product_key, first_seen, last_seen)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (comp, pname, norm, price, "", "", ck, fs, ls),
            )
        cur.execute("DROP TABLE comp_catalog")
        cur.execute("ALTER TABLE comp_catalog__new RENAME TO comp_catalog")
    except Exception as e:
        _log_db_err("migrate comp_catalog rebuild", e)


def migrate_db_v26():
    """
    سكريبت ترحيل v26.0 — يُنفَّذ مرة واحدة فقط.
    يضمن وجود كل الجداول المطلوبة بدون فقدان أي بيانات.
    آمن للتشغيل المتكرر (idempotent).
    """
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()

        _migrate_comp_catalog_if_needed(cur)

        # ── 1. جدول سجل الأتمتة ──
        cur.execute("""CREATE TABLE IF NOT EXISTS automation_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT DEFAULT (datetime('now','localtime')),
            product_name TEXT,
            product_id TEXT,
            rule_name TEXT,
            action TEXT,
            old_price REAL,
            new_price REAL,
            comp_price REAL,
            competitor TEXT,
            match_score REAL,
            reason TEXT,
            pushed_to_make INTEGER DEFAULT 0
        )""")

        # ── 2. جدول إعدادات الأتمتة (للحفظ بين الجلسات) ──
        cur.execute("""CREATE TABLE IF NOT EXISTS automation_settings (
            key TEXT PRIMARY KEY,
            value TEXT,
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        )""")

        # ── 3. جدول نسخة قاعدة البيانات (لتتبع الترحيلات) ──
        cur.execute("""CREATE TABLE IF NOT EXISTS db_version (
            version TEXT PRIMARY KEY,
            applied_at TEXT DEFAULT (datetime('now','localtime')),
            description TEXT
        )""")

        # ── 4. تسجيل أن الترحيل v26.0 تم تنفيذه ──
        cur.execute("""INSERT OR IGNORE INTO db_version (version, description)
                       VALUES ('v26.0', 'إضافة جداول الأتمتة الذكية وسجل القرارات')""")

        # ── 5. إضافة أعمدة جديدة للجداول الموجودة (بأمان) ──
        # إضافة عمود cost_price لجدول our_catalog إذا لم يكن موجوداً
        try:
            cur.execute("ALTER TABLE our_catalog ADD COLUMN cost_price REAL DEFAULT 0")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                _log_db_err("migrate_db_v26 ALTER our_catalog cost_price", e)

        # إضافة عمود auto_processed لجدول processed_products
        try:
            cur.execute("ALTER TABLE processed_products ADD COLUMN auto_processed INTEGER DEFAULT 0")
        except sqlite3.OperationalError as e:
            if "duplicate column" not in str(e).lower():
                _log_db_err("migrate_db_v26 ALTER processed_products", e)

        conn.commit()
        conn.close()
        conn = None
    except Exception as e:
        print(f"Migration v26 error: {e}", flush=True)
        _log_db_err("migrate_db_v26", e)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

