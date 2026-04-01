#!/usr/bin/env python3
"""
🎂 Birthday Fund Bot — фінальна версія

Логіка нагадувань:
  За 3 дні  → група: анонс + банка
              особисто всім (крім іменинниці): сума + кнопка ✅
  За 1 день → група: відсоток оплат
              особисто тільки боржницям: нагадування + кнопка ✅
  В день ДН → група: святкове повідомлення (без грошей)

Адмін:
  /eventstatus — хто оплатив / хто ні
  /remind      — ручне нагадування боржницям у будь-який момент
"""

import logging
import sqlite3
import re
import os
from datetime import datetime, date, time as dtime
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ChatMemberHandler, filters, ContextTypes,
)

# ─── Конфігурація ───────────────────────────────────────────────────────────
BOT_TOKEN            = os.getenv("BOT_TOKEN", "ВСТАВТЕ_ТОКЕН")
ADMIN_IDS            = [int(x) for x in os.getenv("ADMIN_IDS", "123456789").split(",")]
BIRTHDAY_FUND_AMOUNT = int(os.getenv("BIRTHDAY_FUND_AMOUNT", "4000"))
JAR_LINK             = os.getenv("JAR_LINK", "https://send.monobank.ua/YOUR_LINK")
GROUP_CHAT_ID        = int(os.getenv("GROUP_CHAT_ID", "0"))
BIRTHDAY_THREAD_ID   = int(os.getenv("BIRTHDAY_THREAD_ID", "0")) or None
CHECK_HOUR_UTC       = int(os.getenv("CHECK_HOUR_UTC", "18"))  # 20:00 Київ

# Гілка куди бот ПИШЕ всі повідомлення (спілкування)
GROUP_THREAD_ID  = int(os.getenv("GROUP_THREAD_ID", "0")) or None

# Гілка для привітань в день ДН (якщо відрізняється від GROUP_THREAD_ID)
CONGRATS_THREAD_ID = int(os.getenv("CONGRATS_THREAD_ID", "0")) or None

# BIRTHDAY_THREAD_ID — гілка звідки ЧИТАЄМО анкети (вже є вище)

# ─── Логування ──────────────────────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Назви місяців ──────────────────────────────────────────────────────────
MONTH_NAMES_UA = [
    "", "Січень", "Лютий", "Березень", "Квітень", "Травень", "Червень",
    "Липень", "Серпень", "Вересень", "Жовтень", "Листопад", "Грудень"
]
MONTH_GENITIVE_UA = [
    "", "січня", "лютого", "березня", "квітня", "травня", "червня",
    "липня", "серпня", "вересня", "жовтня", "листопада", "грудня"
]

# ─── Парсинг дат ────────────────────────────────────────────────────────────
_MONTH_PARSE = {
    "січня":1,"січень":1,"лютого":2,"лютий":2,"березня":3,"березень":3,
    "квітня":4,"квітень":4,"травня":5,"травень":5,"червня":6,"червень":6,
    "липня":7,"липень":7,"серпня":8,"серпень":8,"вересня":9,"вересень":9,
    "жовтня":10,"жовтень":10,"листопада":11,"листопад":11,"грудня":12,"грудень":12,
    "january":1,"jan":1,"february":2,"feb":2,"march":3,"mar":3,
    "april":4,"apr":4,"may":5,"june":6,"jun":6,"july":7,"jul":7,
    "august":8,"aug":8,"september":9,"sep":9,"october":10,"oct":10,
    "november":11,"nov":11,"december":12,"dec":12,
}

def parse_birthday(text: str) -> Optional[tuple]:
    """Повертає (day, month) або None."""
    t = text.lower().strip()
    # ДД.ММ / ДД/ММ / ДД-ММ
    m = re.search(r'\b(\d{1,2})[.\-/](\d{1,2})\b', t)
    if m:
        d, mo = int(m.group(1)), int(m.group(2))
        if 1 <= d <= 31 and 1 <= mo <= 12:
            return d, mo
    # "25 квітня" або "квітня 25"
    for name, month_num in _MONTH_PARSE.items():
        for pat in (rf'\b(\d{{1,2}})\s+{re.escape(name)}\b',
                    rf'\b{re.escape(name)}\s+(\d{{1,2}})\b'):
            m = re.search(pat, t)
            if m:
                d = int(m.group(1))
                if 1 <= d <= 31:
                    return d, month_num
    return None

def parse_birth_year(text: str) -> Optional[int]:
    """Витягує рік народження з тексту (1970–2010)."""
    m = re.search(r'\b(19[7-9]\d|200\d|201[0-9])\b', text)
    if m:
        return int(m.group(1))
    return None


def parse_extra_info(text: str) -> dict:
    """Парсить з повідомлення: Нова пошта, Instagram, улюблений колір."""
    result = {}
    lines = text.split("\n")

    for line in lines:
        line = line.strip()
        low = line.lower()

        # Нова пошта: "НП відділення 232", "Нова пошта НП відділення 232" тощо
        m = re.search(r'(?:нп|нова\sпошта)[^\d](\d+)', low)
        if m:
            result["nova_poshta"] = f"НП відділення {m.group(1)}"
            continue

        # Instagram: @нікнейм або Instagram: нікнейм
        m = re.search(r'(?:instagram|інстаграм|інста)[:\s]*@?([\w.]+)', low)
        if m:
            result["instagram"] = "@" + m.group(1)
            continue
        m = re.search(r'@([\w.]{3,30})', line)
        if m and "instagram" not in result:
            result["instagram"] = "@" + m.group(1)
            continue

        # Улюблений колір
        m = re.search(r'(?:улюблений\s*колір|колір)[:\s]+(.+)', low)
        if m:
            result["favorite_color"] = m.group(1).strip().capitalize()
            continue

    return result


# ─── БД ─────────────────────────────────────────────────────────────────────
DB_PATH = "/data/birthday_fund.db"

def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS members (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id        INTEGER UNIQUE,
            name               TEXT NOT NULL,
            birthday           TEXT,           -- MM-DD or YYYY-MM-DD
            subscription_until TEXT,           -- YYYY-MM-DD; NULL = безстрокова
            is_active          INTEGER DEFAULT 1,
            joined_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            nova_poshta        TEXT,
            instagram          TEXT,
            favorite_color     TEXT
        );

        CREATE TABLE IF NOT EXISTS birthday_events (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            birthday_person_name TEXT NOT NULL,
            birthday_person_id   INTEGER,       -- members.id іменинниці (може NULL)
            event_date           TEXT NOT NULL,  -- YYYY-MM-DD дата ДН
            amount_per_person    REAL NOT NULL,
            total_members        INTEGER NOT NULL,
            auto_created         INTEGER DEFAULT 0,
            created_at           TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS payments (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id  INTEGER NOT NULL,
            member_id INTEGER NOT NULL,
            amount    REAL NOT NULL,
            paid      INTEGER DEFAULT 0,
            paid_at   TEXT,
            UNIQUE (event_id, member_id),
            FOREIGN KEY (event_id)  REFERENCES birthday_events(id),
            FOREIGN KEY (member_id) REFERENCES members(id)
        );

        -- Журнал надісланих нагадувань — захист від дублювання
        CREATE TABLE IF NOT EXISTS reminder_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            member_id   INTEGER NOT NULL,
            days_before INTEGER NOT NULL,  -- 3, 1, 0
            year        INTEGER NOT NULL,
            log_type    TEXT NOT NULL,     -- 'group' або 'personal'
            sent_at     TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (member_id, days_before, year, log_type)
        );
    """)
    # Міграція: додаємо нові колонки якщо їх ще немає
    for col, definition in [
        ("nova_poshta",    "TEXT"),
        ("instagram",      "TEXT"),
        ("favorite_color", "TEXT"),
        ("birth_year",     "INTEGER"),
        ("username",       "TEXT"),
    ]:
        try:
            conn.execute(f"ALTER TABLE members ADD COLUMN {col} {definition}")
        except Exception:
            pass  # колонка вже існує
    conn.commit()
    conn.close()
    logger.info("✅ БД ініціалізована")


# ─── Хелпери БД ─────────────────────────────────────────────────────────────

def get_active_members() -> list:
    """Повертає активних учасниць з дійсною підпискою."""
    today = date.today().isoformat()
    conn = get_conn()
    rows = conn.execute("""
        SELECT id, telegram_id, name FROM members
        WHERE is_active = 1
          AND (subscription_until IS NULL OR subscription_until >= ?)
        ORDER BY name
    """, (today,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

async def is_member_in_group(bot, telegram_id: int) -> bool:
    """Перевіряє чи є учасниця зараз в групі."""
    if not GROUP_CHAT_ID or not telegram_id:
        return True  # якщо немає ID групи — не блокуємо
    try:
        member = await bot.get_chat_member(chat_id=GROUP_CHAT_ID, user_id=telegram_id)
        return member.status in ("member", "administrator", "creator")
    except Exception:
        return False  # якщо помилка — вважаємо що не в групі

async def get_active_group_members(bot) -> list:
    """Повертає активних учасниць. Перевіряє членство тільки якщо є GROUP_CHAT_ID."""
    return get_active_members()

async def check_birthday_person_in_group(bot, member: dict) -> bool:
    """Перевіряє тільки чи іменинниця ще в групі."""
    if not member.get("telegram_id"):
        return True
    return await is_member_in_group(bot, member["telegram_id"])

def get_latest_event() -> Optional[dict]:
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM birthday_events ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return dict(row) if row else None

def get_event_for_member_date(member_id: int, bd_date: date) -> Optional[int]:
    conn = get_conn()
    row = conn.execute("""
        SELECT id FROM birthday_events
        WHERE birthday_person_id = ? AND event_date = ?
        ORDER BY id DESC LIMIT 1
    """, (member_id, bd_date.isoformat())).fetchone()
    conn.close()
    return row["id"] if row else None

def get_unpaid(event_id: int) -> list:
    conn = get_conn()
    rows = conn.execute("""
        SELECT m.id, m.telegram_id, m.name
        FROM payments p JOIN members m ON p.member_id = m.id
        WHERE p.event_id = ? AND p.paid = 0
        ORDER BY m.name
    """, (event_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def count_paid(event_id: int) -> int:
    conn = get_conn()
    n = conn.execute(
        "SELECT COUNT(*) as n FROM payments WHERE event_id = ? AND paid = 1",
        (event_id,)
    ).fetchone()["n"]
    conn.close()
    return n

def already_reminded(member_id, days_before, year, log_type) -> bool:
    conn = get_conn()
    row = conn.execute("""
        SELECT id FROM reminder_log
        WHERE member_id=? AND days_before=? AND year=? AND log_type=?
    """, (member_id, days_before, year, log_type)).fetchone()
    conn.close()
    return row is not None

def log_reminder(member_id, days_before, year, log_type):
    conn = get_conn()
    conn.execute("""
        INSERT OR IGNORE INTO reminder_log (member_id, days_before, year, log_type)
        VALUES (?,?,?,?)
    """, (member_id, days_before, year, log_type))
    conn.commit()
    conn.close()

def create_event(member: dict, bd_date: date, amount: float,
                 count: int, active: list) -> int:
    """Створює подію ДН і записує payments для всіх учасниць."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO birthday_events
            (birthday_person_name, birthday_person_id, event_date,
             amount_per_person, total_members, auto_created)
        VALUES (?,?,?,?,?,1)
    """, (member["name"], member.get("id"), bd_date.isoformat(), amount, count))
    event_id = c.lastrowid
    for m in active:
        # Іменинниця не платить → пропускаємо її
        if m["id"] == member.get("id"):
            continue
        c.execute(
            "INSERT OR IGNORE INTO payments (event_id, member_id, amount) VALUES (?,?,?)",
            (event_id, m["id"], amount)
        )
    conn.commit()
    conn.close()
    return event_id


# ─── Тексти повідомлень ─────────────────────────────────────────────────────

def get_member_info(member_id: int) -> dict:
    """Повертає додаткову інфу про учасницю."""
    conn = get_conn()
    row = conn.execute(
        "SELECT nova_poshta, instagram, favorite_color FROM members WHERE id=?", (member_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else {}

def instagram_link(insta: str) -> str:
    """Перетворює @нік на клікабельне посилання Instagram."""
    if not insta:
        return ""
    nick = insta.lstrip("@").rstrip("/")
    return f"https://www.instagram.com/{nick}/"

def text_group_announce(name: str, bd_date: date, count: int, amount: int,
                        member_id: int = None) -> str:
    """За 3 дні — анонс у групу."""
    mo = bd_date.month
    d  = bd_date.day

    # Додаткова інфо про іменинницю
    extra_lines = ""
    if member_id:
        info = get_member_info(member_id)
        parts = []
        if info.get("nova_poshta"):
            parts.append(f"📦 {info['nova_poshta']}")
        if info.get("instagram"):
            parts.append(f"📸 {instagram_link(info['instagram'])}")
        if info.get("favorite_color"):
            parts.append(f"🎨 Улюблений колір: {info['favorite_color']}")
        if parts:
            extra_lines = "\n\n" + "\n".join(parts)

    return (
        f"🛎️ Дівчата, у нас скоро іменинниця!\n\n"
        f"🌸 {name} святкує день народження {d} {MONTH_GENITIVE_UA[mo]}!"
        f"{extra_lines}\n\n"
        f"💰 Збираємо {BIRTHDAY_FUND_AMOUNT} грн\n"
        f"👥 Учасниць: {count} — по {amount} грн з кожної\n\n"
        f"Скидаємось сюди 👇\n"
        f"💳 {JAR_LINK}"
    )

def text_personal_announce(birthday_name: str, amount: int,
                           member_id: int = None) -> str:
    """За 3 дні — особисте повідомлення кожній учасниці."""
    uname = get_member_username(member_id) if member_id else None
    mention = f" ({uname})" if uname else ""

    extra_lines = ""
    if member_id:
        info = get_member_info(member_id)
        parts = []
        if info.get("nova_poshta"):
            parts.append(f"📦 Нова пошта: {info['nova_poshta']}")
        if info.get("instagram"):
            link = instagram_link(info["instagram"])
            parts.append(f"📸 Instagram: {link}")
        if info.get("favorite_color"):
            parts.append(f"🎨 Улюблений колір: {info['favorite_color']}")
        if parts:
            extra_lines = "\n\nКорисна інфо про іменинницю:\n" + "\n".join(parts)

    return (
        f"🛎️ У нашій спільноті скоро іменинниця!\n\n"
        f"🌸 {birthday_name}{mention} святкує день народження"
        f"{extra_lines}\n\n"
        f"💰 Твоя частина: {amount} грн\n\n"
        f"💳 Переказати на банку:\n{JAR_LINK}\n\n"
        f"Після переказу натисни кнопку нижче — так ми бачимо загальну картину збору 🙏"
    )

def text_group_day_before(name: str, bd_date: date,
                          paid: int, total: int,
                          member_id: int = None) -> str:
    """Нагадування в групу з відсотком оплат."""
    from datetime import date as _date
    today = _date.today()
    days_left = (bd_date - today).days

    if days_left == 0:
        timing = "сьогодні день народження!"
        emoji = "🎂"
    elif days_left == 1:
        timing = "завтра день народження!"
        emoji = "⏰"
    elif days_left <= 3:
        timing = f"через {days_left} дні день народження!"
        emoji = "🛎️"
    else:
        timing = f"через {days_left} днів день народження!"
        emoji = "🛎️"

    percent = round(paid / total * 100) if total else 0

    uname = get_member_username(member_id) if member_id else None
    mention = f" ({uname})" if uname else ""

    return (
        f"{emoji} Нагадування — {timing}\n\n"
        f"🌸 {name}{mention}\n\n"
        f"📊 Вже здали: {percent}%\n\n"
        f"Дівчата, хто ще не встиг — перевірте особисті повідомлення 💳"
    )

def text_personal_reminder(birthday_name: str, bd_date: date, amount: int) -> str:
    """За 1 день — нагадування боржниці в особисті."""
    return (
        f"👋 Нагадую!\n\n"
        f"Твій внесок на день народження {birthday_name} "
        f"({bd_date.strftime('%d.%m')}) ще не зафіксовано.\n\n"
        f"⚠️ Завтра вже день народження — встигни сьогодні!\n\n"
        f"💰 Сума: {amount} грн\n"
        f"💳 {JAR_LINK}\n\n"
        f"Після переказу натисни кнопку ↓"
    )

def get_member_age(member_id: int, bd_date: date) -> Optional[int]:
    """Повертає вік іменинниці якщо відомий рік народження."""
    conn = get_conn()
    row = conn.execute("SELECT birth_year FROM members WHERE id=?", (member_id,)).fetchone()
    conn.close()
    if row and row["birth_year"]:
        return bd_date.year - row["birth_year"]
    return None

def get_member_username(member_id: int) -> Optional[str]:
    """Повертає Telegram username або telegram_id для mention."""
    conn = get_conn()
    row = conn.execute("SELECT telegram_id, username FROM members WHERE id=?", (member_id,)).fetchone()
    conn.close()
    if not row:
        return None
    if row["username"]:
        return row["username"]  # @username
    return None

def text_group_birthday(name: str, bd_date: date, member_id: int = None) -> str:
    """В день ДН — святкове повідомлення в групу (без грошей)."""
    mo  = bd_date.month
    d   = bd_date.day
    age = get_member_age(member_id, bd_date) if member_id else None
    age_str = f"\n🎈 Виповнюється {age} років!" if age else ""

    # Додаємо @username якщо є
    mention = ""
    if member_id:
        uname = get_member_username(member_id)
        if uname:
            mention = f" ({uname})"

    return (
        f"🎂 Сьогодні день народження нашої {name}{mention}!{age_str}\n\n"
        f"Дівчата, давайте всі разом привітаємо іменинницю! 🥳🎉\n\n"
        f"З днем народження, {name}! 💐"
    )

def text_remind_manual(birthday_name: str, bd_date: date, amount: int) -> str:
    """Ручне нагадування (команда /remind)."""
    return (
        f"👋 Нагадуємо!\n\n"
        f"Внесок на день народження {birthday_name} "
        f"({bd_date.strftime('%d.%m')}) ще не зафіксовано.\n\n"
        f"💰 Сума: {amount} грн\n"
        f"💳 {JAR_LINK}\n\n"
        f"Після переказу натисни кнопку ↓"
    )


# ─── Надсилання в групу ─────────────────────────────────────────────────────

async def send_to_group(context: ContextTypes.DEFAULT_TYPE, text: str,
                       congrats: bool = False):
    """
    Надсилає повідомлення в групу.
    - Звичайні повідомлення → GROUP_THREAD_ID (гілка спілкування)
    - congrats=True → CONGRATS_THREAD_ID (гілка привітань, якщо є) + GROUP_THREAD_ID
    """
    if not GROUP_CHAT_ID:
        return

    # Визначаємо куди писати
    write_thread = GROUP_THREAD_ID  # гілка спілкування

    kwargs = {"chat_id": GROUP_CHAT_ID, "text": text}
    if write_thread:
        kwargs["message_thread_id"] = write_thread
    logger.info(f"📤 Надсилаю в групу: chat_id={GROUP_CHAT_ID}, thread={write_thread}")
    try:
        await context.bot.send_message(**kwargs)
        logger.info("✅ Надіслано успішно")
    except Exception as e:
        logger.error(f"❌ Помилка надсилання в групу: {e}")
        # Спробуємо без Markdown
        try:
            kwargs_plain = {k: v for k, v in kwargs.items() if k != "parse_mode"}
            kwargs_plain["text"] = kwargs["text"].replace("*", "").replace("_", "").replace("`", "")
            await context.bot.send_message(**kwargs_plain)
            logger.info("✅ Надіслано без Markdown")
        except Exception as e2:
            logger.error(f"❌ Помилка навіть без Markdown: {e2}")

    # В день ДН — додатково пишемо в гілку привітань (якщо окрема)
    if congrats and CONGRATS_THREAD_ID and CONGRATS_THREAD_ID != write_thread:
        kwargs2 = {"chat_id": GROUP_CHAT_ID, "text": text,
                   "message_thread_id": CONGRATS_THREAD_ID}
        try:
            await context.bot.send_message(**kwargs2)
        except Exception as e:
            logger.error(f"Помилка надсилання в гілку привітань: {e}")


# ─── Планувальник ───────────────────────────────────────────────────────────

async def daily_birthday_check(context: ContextTypes.DEFAULT_TYPE):
    today = date.today()
    logger.info(f"⏰ Перевірка ДН: {today}")
    logger.info(f"GROUP_CHAT_ID={GROUP_CHAT_ID} GROUP_THREAD_ID={GROUP_THREAD_ID} BIRTHDAY_THREAD_ID={BIRTHDAY_THREAD_ID}")

    conn = get_conn()
    all_members = conn.execute("""
        SELECT id, telegram_id, name, birthday
        FROM members WHERE is_active = 1 AND birthday IS NOT NULL
    """).fetchall()
    conn.close()
    logger.info(f"Знайдено учасниць з ДН: {len(all_members)}")

    for member in all_members:
        parts = member["birthday"].split("-")
        month, day = int(parts[-2]), int(parts[-1])
        try:
            bd = date(today.year, month, day)
        except ValueError:
            continue
        if bd < today:
            try:
                bd = date(today.year + 1, month, day)
            except ValueError:
                continue

        days_until = (bd - today).days
        m = dict(member)
        logger.info(f"  {m['name']}: ДН {bd}, через {days_until} дн.")

        # ── За 3 дні: анонс у групу + особисті всім ────────────────────────
        if days_until == 3 and not already_reminded(m["id"], 3, today.year, "group"):
            await _do_announce(context, m, bd)
            log_reminder(m["id"], 3, today.year, "group")
            log_reminder(m["id"], 3, today.year, "personal")  # особисті йдуть разом

        # ── За 1 день: % у групу + нагадування боржницям ───────────────────
        elif days_until == 1 and not already_reminded(m["id"], 1, today.year, "group"):
            await _do_day_before(context, m, bd)
            log_reminder(m["id"], 1, today.year, "group")
            log_reminder(m["id"], 1, today.year, "personal")

        # ── В день ДН: святкове повідомлення ───────────────────────────────
        elif days_until == 0 and not already_reminded(m["id"], 0, today.year, "group"):
            await send_to_group(context, text_group_birthday(m["name"], bd, member_id=m["id"]), congrats=True)
            log_reminder(m["id"], 0, today.year, "group")


async def _do_announce(context, member: dict, bd_date: date):
    """За 3 дні: анонс у групу + особисті повідомлення."""
    # Перевіряємо тільки чи іменинниця ще в групі
    if not await check_birthday_person_in_group(context.bot, member):
        logger.info(f"⚠️ {member['name']} більше не в групі — збір скасовано")
        return

    active = get_active_members()

    count  = len(active) - 1  # іменинниця не платить
    if count <= 0:
        count = len(active)
    amount = round(BIRTHDAY_FUND_AMOUNT / count)

    # Створюємо подію
    event_id = get_event_for_member_date(member["id"], bd_date)
    if not event_id:
        event_id = create_event(member, bd_date, amount, count, active)

    # В групу
    await send_to_group(context, text_group_announce(member["name"], bd_date, count, amount, member_id=member.get("id")))

    # Особисто кожній (крім іменинниці)
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Я оплатила!", callback_data=f"paid_{event_id}")
    ]])
    sent, failed = 0, 0
    for m in active:
        if m["id"] == member["id"]:       # іменинниця — пропускаємо
            continue
        if not m["telegram_id"]:
            failed += 1
            continue
        try:
            await context.bot.send_message(
                chat_id=m["telegram_id"],
                text=text_personal_announce(member["name"], amount, member_id=member.get("id")),
                
                reply_markup=keyboard
            )
            sent += 1
        except Exception as e:
            logger.warning(f"Не надіслано {m['name']}: {e}")
            failed += 1

    logger.info(f"Анонс ДН {member['name']}: надіслано {sent}, помилок {failed}")

async def _debug_send_test(context, member, days_until, bd):
    logger.info(f"🔍 Знайдено: {member['name']} ДН {bd} — через {days_until} дн.")


async def _do_day_before(context, member: dict, bd_date: date):
    """За 1 день: % у групу + нагадування тільки боржницям."""
    event_id = get_event_for_member_date(member["id"], bd_date)
    if not event_id:
        logger.warning(f"Немає події для {member['name']} {bd_date} — нагадування пропущено")
        return

    conn = get_conn()
    ev   = conn.execute("SELECT * FROM birthday_events WHERE id=?", (event_id,)).fetchone()
    conn.close()
    if not ev:
        return

    total  = ev["total_members"]
    paid   = count_paid(event_id)
    unpaid = get_unpaid(event_id)
    amount = int(ev["amount_per_person"])

    # В групу — відсоток оплат
    await send_to_group(context, text_group_day_before(member["name"], bd_date, paid, total))

    # Особисто тільки боржницям
    if not unpaid:
        logger.info(f"ДН {member['name']}: всі здали 🎉")
        return

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Я оплатила!", callback_data=f"paid_{event_id}")
    ]])
    sent = 0
    for u in unpaid:
        if not u["telegram_id"]:
            continue
        try:
            await context.bot.send_message(
                chat_id=u["telegram_id"],
                text=text_personal_reminder(member["name"], bd_date, amount),
                
                reply_markup=keyboard
            )
            sent += 1
        except Exception as e:
            logger.warning(f"Не надіслано {u['name']}: {e}")

    # Список боржниць адміну
    names_list = "\n".join(f"  • {u['name']}" for u in unpaid)
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"📋 Боржниці — ДН {member['name']} "
                    f"({bd_date.strftime('%d.%m')})\n\n"
                    f"❌ Не оплатили ({len(unpaid)}/{total}):\n{names_list}\n\n"
                    f"Нагадати вручну: /remind"
                ),
            )
        except Exception:
            pass

    logger.info(f"Нагадувань: {sent}/{len(unpaid)} для ДН {member['name']}")


# ─── Авто-парсинг з гілки ───────────────────────────────────────────────────

async def handle_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Спрацьовує коли нова учасниця приєднується до групи."""
    result = update.chat_member
    if not result:
        return

    # Перевіряємо що це саме наша група
    if GROUP_CHAT_ID and result.chat.id != GROUP_CHAT_ID:
        return

    old_status = result.old_chat_member.status
    new_status = result.new_chat_member.status

    # Нова учасниця = була не в групі, стала member/administrator
    if old_status in ("left", "kicked", "restricted") and new_status in ("member", "administrator"):
        user = result.new_chat_member.user
        if user.is_bot:
            return

        bot_info = await context.bot.get_me()
        bot_username = bot_info.username

        # Повідомлення в групу
        uname_str = f" (@{user.username})" if user.username else ""
        welcome_text = (
            f"Вітаємо нову учасницю {user.first_name}{uname_str}! 🎉\n\n"
            f"У нас є бот для збору на дні народження — "
            f"він автоматично нагадує і відстежує хто здав.\n\n"
            f"Активуй його щоб отримувати сповіщення 👇"
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "Активувати бота",
                url=f"https://t.me/{bot_username}?start=activate"
            )
        ]])

        try:
            kwargs = {"chat_id": GROUP_CHAT_ID, "text": welcome_text, "reply_markup": keyboard}
            if GROUP_THREAD_ID:
                kwargs["message_thread_id"] = GROUP_THREAD_ID
            await context.bot.send_message(**kwargs)
            logger.info(f"Привітання надіслано для {user.full_name}")
        except Exception as e:
            logger.error(f"Помилка привітання: {e}")


async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return
    is_our_thread = (BIRTHDAY_THREAD_ID is None or
                     message.message_thread_id == BIRTHDAY_THREAD_ID)
    is_our_group  = (GROUP_CHAT_ID == 0 or message.chat_id == GROUP_CHAT_ID)
    if not (is_our_group and is_our_thread):
        return

    # Якщо повідомлення переслане — беремо оригінального автора
    user = message.forward_from or message.from_user
    if not user:
        return

    text   = message.text or message.caption or ""
    result = parse_birthday(text)
    if not result:
        return
    day, month = result
    bd = f"{month:02d}-{day:02d}"
    birth_year = parse_birth_year(text)
    conn = get_conn()
    existing = conn.execute(
        "SELECT id FROM members WHERE telegram_id=?", (user.id,)
    ).fetchone()
    if existing:
        yr_update = ", birth_year=?" if birth_year else ""
        yr_params = [birth_year] if birth_year else []
        params = [bd, user.full_name] + yr_params + [user.id]
        conn.execute(f"UPDATE members SET birthday=?, name=?{yr_update} WHERE telegramid=?", params)
    else:
        conn.execute(
            "INSERT OR IGNORE INTO members (telegram_id, name, birthday) VALUES (?,?,?)",
            (user.id, user.full_name, bd)
        )
        if birth_year:
            conn.execute("UPDATE members SET birth_year=? WHERE telegram_id=?", (birth_year, user.id))
    # Парсимо додаткові дані
    extra = parse_extra_info(text)
    updates = []
    params = []
    if extra.get("nova_poshta"):
        updates.append("nova_poshta=?")
        params.append(extra["nova_poshta"])
    if extra.get("instagram"):
        updates.append("instagram=?")
        params.append(extra["instagram"])
    if extra.get("favorite_color"):
        updates.append("favorite_color=?")
        params.append(extra["favorite_color"])

    if updates:
        params.append(user.id)
        conn.execute(
            f"UPDATE members SET {', '.join(updates)} WHERE telegram_id=?", params
        )

    conn.commit()
    conn.close()
    extras = ", ".join(f"{k}: {v}" for k, v in extra.items()) if extra else "немає"
    logger.info(f"Збережено ДН {user.full_name}: {day:02d}.{month:02d} | {extras}")


# ─── Команди ────────────────────────────────────────────────────────────────

async def _handle_onboarding(update: Update, context: ContextTypes.DEFAULT_TYPE,
                              step: str, text: str):
    """Покроковий збір анкети при першій активації."""
    user_id = update.effective_user.id

    if step == "birthday":
        # Парсимо дату — підтримуємо ДД.ММ і ДД.ММ.РРРР
        result = parse_birthday(text)
        year   = parse_birth_year(text)
        if result:
            day, month = result
            bd = f"{month:02d}-{day:02d}"
            conn = get_conn()
            conn.execute("UPDATE members SET birthday=? WHERE telegram_id=?", (bd, user_id))
            if year:
                conn.execute("UPDATE members SET birth_year=? WHERE telegram_id=?", (year, user_id))
            conn.commit()
            conn.close()
            context.user_data["onboarding_step"] = "nova_poshta"
            yr_str = f".{year}" if year else ""
            await update.message.reply_text(
                f"✅ ДН збережено: {day:02d}.{month:02d}{yr_str}\n\n"
                f"Тепер напиши своє відділення Нової пошти\n"
                f"Наприклад: НП відділення 47, Київ\n\n"
                f"_(або напиши «-» щоб пропустити)",
                parse_mode="HTML"
            )
        else:
            await update.message.reply_text(
                "❌ Не розпізнала дату. Спробуй формат: 25.04.1995 або 25.04"
            )

    elif step == "nova_poshta":
        if text != "-":
            conn = get_conn()
            conn.execute("UPDATE members SET nova_poshta=? WHERE telegram_id=?", (text, user_id))
            conn.commit()
            conn.close()
        context.user_data["onboarding_step"] = "instagram"
        await update.message.reply_text(
            "✅ Збережено!\n\n"
            "Напиши свій Instagram нікнейм\n"
            "Наприклад: @kateryna_pet\n\n"
            "_(або напиши «-» щоб пропустити)_",
            parse_mode="HTML"
        )

    elif step == "instagram":
        if text != "-":
            insta = text if text.startswith("@") else "@" + text
            conn = get_conn()
            conn.execute("UPDATE members SET instagram=? WHERE telegram_id=?", (insta, user_id))
            conn.commit()
            conn.close()
        context.user_data["onboarding_step"] = "color"
        await update.message.reply_text(
            "✅ Збережено!\n\n"
            "Останній крок — напиши свій улюблений колір 🎨\n"
            "Наприклад: лавандовий\n\n"
            "_(або напиши «-» щоб пропустити)_",
            parse_mode="HTML"
        )

    elif step == "color":
        if text != "-":
            conn = get_conn()
            conn.execute("UPDATE members SET favorite_color=? WHERE telegram_id=?", (text, user_id))
            conn.commit()
            conn.close()
        context.user_data.pop("onboarding_step", None)
        await update.message.reply_text(
            "🎉 Анкету заповнено! Дякуємо!\n\n"
            "Тепер ти будеш отримувати повідомлення про дні народження в нашій спільноті 🎂\n\n"
            "/myinfo — переглянути свою анкету\n"
            "/status — мої оплати",
        )


async def _check_urgent_birthdays(context: ContextTypes.DEFAULT_TYPE):
    """При активації нового користувача перевіряємо чи є термінові ДН (сьогодні/завтра)."""
    today = date.today()
    conn = get_conn()
    members = conn.execute(
        "SELECT id, telegram_id, name, birthday FROM members WHERE is_active=1 AND birthday IS NOT NULL"
    ).fetchall()
    conn.close()

    for member in members:
        parts = member["birthday"].split("-")
        month, day = int(parts[-2]), int(parts[-1])
        try:
            bd = date(today.year, month, day)
        except ValueError:
            continue
        if bd < today:
            continue

        days_until = (bd - today).days
        m = dict(member)

        if days_until == 0 and not already_reminded(m["id"], 0, today.year, "group"):
            # Створюємо подію якщо немає
            await _ensure_event_exists(context, m, bd)
            await send_to_group(context, text_group_birthday(m["name"], bd, m["id"]), congrats=True)
            log_reminder(m["id"], 0, today.year, "group")

        elif days_until == 1 and not already_reminded(m["id"], 1, today.year, "group"):
            # Створюємо подію якщо немає (за 3 дні не встигли)
            await _ensure_event_exists(context, m, bd)
            await _do_day_before(context, m, bd)
            log_reminder(m["id"], 1, today.year, "group")
            log_reminder(m["id"], 1, today.year, "personal")

        elif days_until == 3 and not already_reminded(m["id"], 3, today.year, "group"):
            await _do_announce(context, m, bd)
            log_reminder(m["id"], 3, today.year, "group")
            log_reminder(m["id"], 3, today.year, "personal")


async def _ensure_event_exists(context, member: dict, bd_date: date):
    """Створює подію збору якщо її ще немає — для термінових випадків."""
    event_id = get_event_for_member_date(member["id"], bd_date)
    if event_id:
        return event_id

    active = get_active_members()
    # Іменинниця не платить
    payers = [m for m in active if m["id"] != member["id"]]
    count  = len(payers) if payers else len(active)
    amount = round(BIRTHDAY_FUND_AMOUNT / count) if count else BIRTHDAY_FUND_AMOUNT

    event_id = create_event(member, bd_date, amount, count, active)
    logger.info(f"✅ Створено термінову подію для {member['name']}: event_id={eventid}")

    # Надсилаємо особисті повідомлення
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Я оплатила!", callback_data=f"paid_{event_id}")
    ]])
    sent = 0
    for m in payers:
        if not m["telegram_id"]:
            continue
        try:
            await context.bot.send_message(
                chat_id=m["telegram_id"],
                text=text_personal_announce(member["name"], amount, member_id=member.get("id")),
                
                reply_markup=keyboard
            )
            sent += 1
        except Exception as e:
            logger.warning(f"Не надіслано {m['name']}: {e}")
    logger.info(f"Особистих повідомлень надіслано: {sent}")
    return event_id


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    conn = get_conn()
    existing = conn.execute(
        "SELECT id, birthday, nova_poshta, instagram, favorite_color FROM members WHERE telegram_id=?",
        (user.id,)
    ).fetchone()
    uname = f"@{user.username}" if user.username else None
    conn.execute("INSERT OR IGNORE INTO members (telegram_id, name) VALUES (?,?)",
                 (user.id, user.full_name))
    if uname:
        conn.execute("UPDATE members SET username=? WHERE telegram_id=?", (uname, user.id))
    conn.commit()
    conn.close()

    await _check_urgent_birthdays(context)

    # Якщо нова учасниця (або не заповнила анкету) — запускаємо онбординг
    is_new = not existing
    needs_onboarding = is_new or (existing and not existing["birthday"])

    if needs_onboarding and user.id not in ADMIN_IDS:
        await update.message.reply_text(
            f"Привіт, {user.first_name}! 👋\n\n"
            "Я допомагаю збирати на дні народження в нашій спільноті 🎂\n\n"
            "Давай заповнимо твою анкету — це займе 1 хвилину!\n\n"
            "Напиши свій день народження у форматі ДД.ММ.РРРР\n"
            "Наприклад: 25.04.1995",
            parse_mode="HTML"
        )
        context.user_data["onboarding_step"] = "birthday"
        return

    text = (
        f"Привіт, {user.first_name}! 👋\n\n"
        "Я стежу за днями народження в нашій спільноті 🎂\n\n"
        "/mybirthday — оновити свій ДН\n"
        "/status     — мої оплати\n"
        "/myinfo     — моя анкета\n"
    )
    if user.id in ADMIN_IDS:
        text += (
            "\n👑 Адмін-панель:\n"
            "/newbirthday  — запустити подію ДН вручну\n"
            "/setbirthday  — Ім'я ДД.ММ (вручну додати дату)\n"
            "/birthdays    — всі ДН по місяцях ⚡\n"
            "/eventstatus  — статус поточного збору\n"
            "/remind       — нагадати боржницям зараз\n"
            "/members      — список учасниць\n"
            "/testcheck    — тест планувальника прямо зараз\n"
            "/clearlog     — очистити журнал (для повторного тесту)\n"
            "/activate      — надіслати запрошення активувати бота\n"
            "/notactivated  — хто ще не активував\n"
            "/checkactive   — перевірити хто заблокував бота\n"
        )
    await update.message.reply_text(text)


async def cmd_my_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/myinfo — показати свою анкету."""
    user_id = update.effective_user.id
    conn = get_conn()
    row = conn.execute(
        "SELECT name, birthday, birth_year, nova_poshta, instagram, favorite_color FROM members WHERE telegram_id=?",
        (user_id,)
    ).fetchone()
    conn.close()
    if not row:
        await update.message.reply_text("Тебе ще немає в системі. Напиши /start")
        return

    bd_str = "не вказано"
    if row["birthday"]:
        parts = row["birthday"].split("-")
        mo, d = int(parts[-2]), int(parts[-1])
        yr = f".{row['birth_year']}" if row["birthyear"] else ""
        bd_str = f"{d:02d}.{mo:02d}{yr}"

    lines = [
        f"📋 Твоя анкета:\n",
        f"👤 Ім'я: {row['name']}",
        f"🎂 ДН: {bd_str}",
        f"📦 Нова пошта: {row['nova_poshta'] or 'не вказано'}",
        f"📸 Instagram: {row['instagram'] or 'не вказано'}",
        f"🎨 Улюблений колір: {row['favorite_color'] or 'не вказано'}",
        f"\nЩоб оновити — напиши /editinfo",
    ]
    await update.message.reply_text("\n".join(lines))


async def cmd_edit_info(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/editinfo — оновити свою анкету."""
    await update.message.reply_text(
        "✏️ Оновлення анкети\n\n"
        "Напиши свій день народження у форматі ДД.ММ.РРРР\n"
        "Наприклад: 25.04.1995\n\n"
        "_(або ДД.ММ якщо не хочеш вказувати рік)_",
        parse_mode="HTML"
    )
    context.user_data["onboarding_step"] = "birthday"


async def cmd_my_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Введи свій день народження у форматі ДД.ММ\n"
        "Наприклад: 25.04"
    )
    context.user_data["waiting_for"] = "birthday"


async def cmd_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    conn = get_conn()
    member = conn.execute(
        "SELECT id FROM members WHERE telegram_id=?", (user_id,)
    ).fetchone()
    if not member:
        await update.message.reply_text("Тебе ще немає в системі. Напиши /start")
        conn.close()
        return
    rows = conn.execute("""
        SELECT e.birthday_person_name, e.event_date, p.amount, p.paid
        FROM payments p JOIN birthday_events e ON p.event_id = e.id
        WHERE p.member_id = ? ORDER BY e.id DESC LIMIT 10
    """, (member["id"],)).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("Поки що подій не було 🎉")
        return
    lines = ["📊 Твої оплати:\n"]
    for r in rows:
        icon = "✅" if r["paid"] else "❌"
        lines.append(
            f"{icon} ДН {r['birthday_personname']} "
            f"({r['event_date']}) — {r['amount']:.0f} грн"
        )
    await update.message.reply_text("\n".join(lines))


async def cmd_new_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручний запуск події — адмін вводить ім'я."""
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("Введи ім'я іменинниці:")
    context.user_data["waiting_for"] = "new_birthday_name"


async def _launch_manual_event(update, context, person_name: str):
    """Створює подію і одразу розсилає повідомлення."""
    active = get_active_members()
    if not active:
        await update.message.reply_text("❌ Немає активних учасниць!")
        return

    # Іменинниця не платить — шукаємо її в списку
    birthday_member = next(
        (m for m in active if m["name"].lower() == person_name.lower()), None
    )
    payers = [m for m in active if m != birthday_member]
    count  = len(payers) if payers else len(active)
    amount = round(BIRTHDAY_FUND_AMOUNT / count)
    today  = date.today()

    pseudo_member = birthday_member or {"name": person_name, "id": None, "telegram_id": None}
    event_id = create_event(pseudo_member, today, amount, count, active)

    # В групу
    await send_to_group(context, text_group_announce(person_name, today, count, amount, member_id=pseudo_member.get("id")))

    # Особисто всім (крім іменинниці)
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Я оплатила!", callback_data=f"paid_{event_id}")
    ]])
    sent, failed = 0, 0
    for m in payers:
        if not m["telegram_id"]:
            failed += 1
            continue
        try:
            await context.bot.send_message(
                chat_id=m["telegram_id"],
                text=text_personal_announce(person_name, amount),
                
                reply_markup=keyboard
            )
            sent += 1
        except Exception:
            failed += 1

    await update.message.reply_text(
        f"✅ Подію «ДН {person_name}» запущено!\n"
        f"📤 Надіслано: {sent}  |  ⚠️ Помилок: {failed}\n"
        f"💰 З кожної: {amount} грн"
    )


async def cmd_event_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    event = get_latest_event()
    if not event:
        await update.message.reply_text("Подій ще не було")
        return

    conn = get_conn()
    rows = conn.execute("""
        SELECT m.name, p.paid, p.paid_at
        FROM payments p JOIN members m ON p.member_id = m.id
        WHERE p.event_id = ? ORDER BY p.paid DESC, m.name
    """, (event["id"],)).fetchall()
    conn.close()

    paid_rows   = [r for r in rows if r["paid"]]
    unpaid_rows = [r for r in rows if not r["paid"]]
    total       = event["total_members"]
    collected   = len(paid_rows) * event["amount_per_person"]
    percent     = round(len(paid_rows) / total * 100) if total else 0

    lines = [
        f"📊 ДН {event['birthday_person_name']} | {event['event_date']}",
        f"💰 {event['amount_per_person']:.0f} грн × {total} = {BIRTHDAY_FUND_AMOUNT} грн",
        f"💵 Зібрано: {collected:.0f} / {BIRTHDAY_FUND_AMOUNT} грн ({percent}%)\n",
        f"✅ Оплатили ({len(paid_rows)}):",
    ]
    for r in paid_rows:
        lines.append(f"  • {r['name']}")
    lines.append(f"\n❌ Не здали ({len(unpaid_rows)}):")
    for r in unpaid_rows:
        lines.append(f"  • {r['name']}")

    await update.message.reply_text("\n".join(lines))


async def cmd_remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручне нагадування боржницям від адміна."""
    if update.effective_user.id not in ADMIN_IDS:
        return
    event = get_latest_event()
    if not event:
        await update.message.reply_text("Активних подій немає")
        return

    unpaid = get_unpaid(event["id"])
    if not unpaid:
        await update.message.reply_text("🎉 Всі оплатили!")
        return

    bd_date = date.fromisoformat(event["event_date"])
    amount  = int(event["amount_per_person"])
    active  = get_active_members()
    paid    = count_paid(event["id"])

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Я оплатила!", callback_data=f"paid_{event['id']}")
    ]])

    # Особисті нагадування
    sent = 0
    for u in unpaid:
        if not u["telegram_id"]:
            continue
        try:
            await context.bot.send_message(
                chat_id=u["telegram_id"],
                text=text_remind_manual(event["birthday_person_name"], bd_date, amount),
                
                reply_markup=keyboard
            )
            sent += 1
        except Exception:
            pass

    # В групу — поточний відсоток
    member_id = None
    conn_tmp = get_conn()
    row_tmp = conn_tmp.execute(
        "SELECT id FROM members WHERE name=?", (event["birthday_person_name"],)
    ).fetchone()
    conn_tmp.close()
    if row_tmp:
        member_id = row_tmp["id"]

    await send_to_group(context, text_group_day_before(
        event["birthday_person_name"], bd_date, paid, event["total_members"],
        member_id=member_id
    ))

    # Список боржниць адміну
    names = "\n".join(f"  • {u['name']}" for u in unpaid)
    await update.message.reply_text(
        f"📤 Нагадувань надіслано: {sent}\n\n"
        f"❌ Ще не здали ({len(unpaid)}/{event['total_members']}):\n{names}"
    )


async def cmd_set_birthday_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/setbirthday Ім'я ДД.ММ — додати дату вручну."""
    if update.effective_user.id not in ADMIN_IDS:
        return
    if len(context.args) < 2:
        await update.message.reply_text(
            "Формат: /setbirthday Ім'я ДД.ММ\n"
            "Наприклад: /setbirthday Катя 25.04"
        )
        return
    name     = context.args[0]
    date_str = " ".join(context.args[1:])
    result   = parse_birthday(date_str)
    if not result:
        await update.message.reply_text(f"❌ Не розпізнала дату: «{datestr}»")
        return
    day, month = result
    bd = f"{month:02d}-{day:02d}"
    conn = get_conn()
    existing = conn.execute(
        "SELECT id FROM members WHERE LOWER(name)=LOWER(?)", (name,)
    ).fetchone()
    if existing:
        conn.execute("UPDATE members SET birthday=? WHERE id=?", (bd, existing["id"]))
        msg = f"✅ ДН для «{name}» оновлено: {day:02d}.{month:02d}"
    else:
        conn.execute("INSERT INTO members (name, birthday) VALUES (?,?)", (name, bd))
        msg = f"✅ Додано «{name}» з ДН {day:02d}.{month:02d}"
    conn.commit()
    conn.close()
    await update.message.reply_text(msg)


async def cmd_birthdays(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    rows = conn.execute("""
        SELECT name, birthday FROM members
        WHERE is_active = 1 AND birthday IS NOT NULL
        ORDER BY SUBSTR(birthday,1,2), SUBSTR(birthday,4,2)
    """).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("Дат народження ще немає")
        return

    today = date.today()
    by_month: dict = {}
    for r in rows:
        mo, d = map(int, r["birthday"].split("-"))
        by_month.setdefault(mo, []).append((d, r["name"]))

    lines = ["🎂 Дні народження учасниць:\n"]
    for mo in sorted(by_month.keys()):
        lines.append(f"📅 {MONTH_NAMESUA[mo]}:")
        for d, name in sorted(by_month[mo]):
            try:
                bd = date(today.year, mo, d)
                if bd < today:
                    bd = date(today.year + 1, mo, d)
                days = (bd - today).days
                tag = f" ⚡ (через {days} дн.)" if days <= 7 else ""
            except ValueError:
                tag = ""
            lines.append(f"  {d:02d}.{mo:02d} — {name}{tag}")
    lines.append("\n⚡ — до ДН ≤ 7 днів")
    await update.message.reply_text("\n".join(lines))


async def cmd_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    active = get_active_members()
    conn = get_conn()
    inactive_n = conn.execute(
        "SELECT COUNT(*) as n FROM members WHERE is_active=0"
    ).fetchone()["n"]
    conn.close()
    lines = [f"👥 Активних: {len(active)}  |  Неактивних: {inactive_n}\n"]
    for m in active:
        lines.append(f"• {m['name']}")
    await update.message.reply_text("\n".join(lines))


async def cmd_deactivate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text(
        "Введи ім'я учасниці, яку потрібно деактивувати:"
    )
    context.user_data["waiting_for"] = "deactivate_name"


async def cmd_force_bday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/forcebday Ім'я — примусово запустити збір для конкретної людини."""
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not context.args:
        await update.message.reply_text("Формат: /forcebday Ім'я\nНаприклад: /forcebday Женя")
        return

    name = " ".join(context.args)
    conn = get_conn()
    member = conn.execute(
        "SELECT * FROM members WHERE LOWER(name) LIKE LOWER(?)", (f"%{name}%",)
    ).fetchone()
    conn.close()

    if not member:
        await update.message.reply_text(f"Не знайдено: {name}")
        return

    m = dict(member)
    parts = m["birthday"].split("-")
    month, day = int(parts[-2]), int(parts[-1])
    today = date.today()
    try:
        bd = date(today.year, month, day)
    except ValueError:
        await update.message.reply_text("Помилка дати")
        return

    if bd < today:
        bd = date(today.year + 1, month, day)

    days_until = (bd - today).days

    # Перевірка дублювання
    conn3 = get_conn()
    already = conn3.execute(
        "SELECT id FROM reminder_log WHERE member_id=? AND year=? AND log_type='force'",
        (m["id"], today.year)
    ).fetchone()
    conn3.close()

    if already:
        await update.message.reply_text(
            f"Повідомлення для {m['name']} вже надсилалось.\n"
            f"Щоб надіслати ще раз — спочатку /clearlog"
        )
        return

    await update.message.reply_text(
        f"Знайдено: {m['name']}, ДН {bd}, через {days_until} дн.\nЗапускаю..."
    )

    # Рахуємо учасниць і суму
    active = get_active_members()
    payers = [x for x in active if x["id"] != m["id"]]
    count  = len(payers) if payers else len(active)
    amount = round(BIRTHDAY_FUND_AMOUNT / count) if count else BIRTHDAY_FUND_AMOUNT

    # Надсилаємо в групу
    await update.message.reply_text(f"Надсилаю в групу...")
    if days_until == 0:
        text = text_group_birthday(m["name"], bd, m["id"])
        await send_to_group(context, text, congrats=True)
    elif days_until == 1:
        text = text_group_day_before(m["name"], bd, 0, count)
        await send_to_group(context, text)
    else:
        text = text_group_announce(m["name"], bd, count, amount, member_id=m["id"])
        await send_to_group(context, text)

    await update.message.reply_text("Готово! Перевір групу.")

    # Записуємо що надсилали — захист від дублювання
    conn_log = get_conn()
    conn_log.execute(
        "INSERT OR IGNORE INTO reminder_log (member_id, days_before, year, log_type) VALUES (?,?,?,?)",
        (m["id"], days_until, today.year, "force")
    )
    conn_log.commit()
    conn_log.close()

    # Створюємо подію і надсилаємо особисті
    event_id = get_event_for_member_date(m["id"], bd)
    if not event_id:
        event_id = create_event(m, bd, amount, count, active)
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Я оплатила!", callback_data=f"paid_{event_id}")
    ]])
    sent = 0
    for p in payers:
        if not p["telegram_id"]:
            continue
        try:
            await context.bot.send_message(
                chat_id=p["telegram_id"],
                text=text_personal_announce(m["name"], amount, member_id=m["id"]),
                reply_markup=keyboard
            )
            sent += 1
        except Exception as ex:
            logger.warning(f"Не надіслано {p['name']}: {ex}")
    await update.message.reply_text(f"Особистих надіслано: {sent}")


async def cmd_test_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text(f"Надсилаю... chat={GROUP_CHAT_ID} thread={GROUP_THREAD_ID}")
    try:
        await context.bot.send_message(
            chat_id=GROUP_CHAT_ID,
            text="Тест повідомлення від бота",
            message_thread_id=GROUP_THREAD_ID
        )
        await update.message.reply_text("Успішно!")
    except Exception as e:
        await update.message.reply_text(f"Помилка: {e}")


async def cmd_clear_log(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/clearlog — очистити журнал нагадувань (щоб testcheck спрацював знову)."""
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    conn.execute("DELETE FROM reminder_log")
    conn.commit()
    conn.close()
    await update.message.reply_text(
        "✅ Журнал нагадувань очищено!\n\n"
        "Тепер /testcheck відправить всі актуальні повідомлення."
    )


async def cmd_test_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("⏰ Запускаю перевірку ДН...")
    await daily_birthday_check(context)
    await update.message.reply_text("✅ Перевірку завершено")


# ─── Callback: ✅ Я оплатила ─────────────────────────────────────────────────

async def callback_paid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query    = update.callback_query
    await query.answer()
    event_id = int(query.data.split("_")[1])
    user_id  = query.from_user.id

    conn = get_conn()
    member = conn.execute(
        "SELECT id FROM members WHERE telegram_id=?", (user_id,)
    ).fetchone()
    if not member:
        await query.edit_message_text("❌ Тебе немає в системі. Напиши /start боту")
        conn.close()
        return

    payment = conn.execute(
        "SELECT id, paid FROM payments WHERE event_id=? AND member_id=?",
        (event_id, member["id"])
    ).fetchone()
    if not payment:
        await query.edit_message_text("❌ Запис не знайдено")
        conn.close()
        return
    if payment["paid"]:
        await query.edit_message_text("✅ Твою оплату вже зафіксовано! Дякуємо 💕")
        conn.close()
        return

    conn.execute(
        "UPDATE payments SET paid=1, paid_at=? WHERE id=?",
        (datetime.now().isoformat(), payment["id"])
    )
    conn.commit()
    ev = conn.execute(
        "SELECT birthday_person_name FROM birthday_events WHERE id=?", (event_id,)
    ).fetchone()
    conn.close()

    bd_name = ev["birthday_person_name"] if ev else ""
    await query.edit_message_text(
        f"✅ Дякуємо за внесок в комуну! Оплату на ДН {bd_name} зафіксовано 💕"
    )

    # Повідомлення адміну
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                admin_id,
                f"💰 {query.from_user.fullname} відмітила оплату\n"
                f"🎂 ДН {bd_name}",
            )
        except Exception:
            pass


# ─── Обробник тексту ────────────────────────────────────────────────────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Повідомлення з групи — парсимо дати з гілки
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        await handle_group_message(update, context)
        return

    message = update.message
    user_id = update.effective_user.id

    # Адмін пересилає повідомлення в особисті — імпорт дат
    if message.forward_from and user_id in ADMIN_IDS:
        original_user = message.forward_from
        text_fwd = message.text or message.caption or ""
        result = parse_birthday(text_fwd)
        extra  = parse_extra_info(text_fwd)

        if result:
            day, month = result
            bd = f"{month:02d}-{day:02d}"
            conn = get_conn()
            existing = conn.execute(
                "SELECT id FROM members WHERE telegram_id=?", (original_user.id,)
            ).fetchone()

            updates = ["birthday=?", "name=?"]
            params  = [bd, original_user.full_name]
            if extra.get("nova_poshta"):
                updates.append("nova_poshta=?"); params.append(extra["nova_poshta"])
            if extra.get("instagram"):
                updates.append("instagram=?"); params.append(extra["instagram"])
            if extra.get("favorite_color"):
                updates.append("favorite_color=?"); params.append(extra["favorite_color"])

            if existing:
                params.append(original_user.id)
                conn.execute(
                    f"UPDATE members SET {', '.join(updates)} WHERE telegram_id=?", params
                )
                action = "оновлено"
            else:
                conn.execute(
                    "INSERT OR IGNORE INTO members (telegram_id, name, birthday) VALUES (?,?,?)",
                    (original_user.id, original_user.full_name, bd)
                )
                if extra:
                    params2 = []
                    upd2 = []
                    if extra.get("nova_poshta"):
                        upd2.append("nova_poshta=?"); params2.append(extra["nova_poshta"])
                    if extra.get("instagram"):
                        upd2.append("instagram=?"); params2.append(extra["instagram"])
                    if extra.get("favorite_color"):
                        upd2.append("favorite_color=?"); params2.append(extra["favorite_color"])
                    if upd2:
                        params2.append(original_user.id)
                        conn.execute(
                            f"UPDATE members SET {', '.join(upd2)} WHERE telegram_id=?", params2
                        )
                action = "додано"

            conn.commit()
            conn.close()

            extras_str = ""
            if extra:
                parts = []
                if extra.get("nova_poshta"): parts.append(f"📦 {extra['novaposhta']}")
                if extra.get("instagram"):   parts.append(f"📸 {extra['instagram']}")
                if extra.get("favorite_color"): parts.append(f"🎨 {extra['favoritecolor']}")
                extras_str = "\n" + "\n".join(parts)

            await message.reply_text(
                f"✅ {action.capitalize()}: {original_user.fullname}\n"
                f"🎂 ДН: {day:02d}.{month:02d}{extras_str}",
                parse_mode="HTML"
            )
        else:
            await message.reply_text(
                f"⚠️ Повідомлення від {message.forward_from.fullname} — дату не знайдено\n"
                f"Спробуй: /setbirthday {message.forward_from.firstname} ДД.ММ",
                parse_mode="HTML"
            )
        return

    # Онбординг — покроковий збір анкети
    onboarding_step = context.user_data.get("onboarding_step")
    if onboarding_step:
        text_raw = message.text.strip() if message.text else ""
        await _handle_onboarding(update, context, onboarding_step, text_raw)
        return

    waiting = context.user_data.get("waiting_for")
    text    = message.text.strip() if message.text else ""
    user_id = update.effective_user.id

    if waiting == "birthday":
        result = parse_birthday(text)
        if result:
            day, month = result
            conn = get_conn()
            conn.execute(
                "UPDATE members SET birthday=? WHERE telegram_id=?",
                (f"{month:02d}-{day:02d}", user_id)
            )
            conn.commit()
            conn.close()
            context.user_data.pop("waiting_for", None)
            await update.message.reply_text(f"✅ ДН збережено: {day:02d}.{month:02d} 🎂")
        else:
            await update.message.reply_text("❌ Не розпізнала. Спробуй: 25.04")

    elif waiting == "new_birthday_name" and user_id in ADMIN_IDS:
        context.user_data.pop("waiting_for", None)
        await _launch_manual_event(update, context, text)

    elif waiting == "deactivate_name" and user_id in ADMIN_IDS:
        conn = get_conn()
        n = conn.execute(
            "UPDATE members SET is_active=0 WHERE name=?", (text,)
        ).rowcount
        conn.commit()
        conn.close()
        context.user_data.pop("waiting_for", None)
        if n:
            await update.message.reply_text(f"✅ «{text}» деактивовано")
        else:
            await update.message.reply_text(f"❌ Не знайдено: «{text}»")


# ─── Запуск ─────────────────────────────────────────────────────────────────

async def cmd_activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/activate — бот пише в групу повідомлення з кнопкою активації."""
    if update.effective_user.id not in ADMIN_IDS:
        return

    bot_info = await context.bot.get_me()
    bot_username = bot_info.username

    text = (
        "👋 Дівчата, у нас є бот для збору на дні народження!\n\n"
        "🎂 Він автоматично нагадує про ДН, рахує суму і відстежує хто здав.\n\n"
        "❗️ Щоб отримувати особисті повідомлення — потрібно активувати бота.\n\n"
        "Натисни кнопку нижче 👇"
    )
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton(
            "🤖 Активувати бота",
            url=f"https://t.me/{bot_username}?start=activate"
        )
    ]])

    if GROUP_CHAT_ID:
        kwargs = {"chat_id": GROUP_CHAT_ID, "text": text,
                   "reply_markup": keyboard}
        if GROUP_THREAD_ID:
            kwargs["message_thread_id"] = GROUP_THREAD_ID
        try:
            await context.bot.send_message(**kwargs)
            await update.message.reply_text("✅ Повідомлення надіслано в групу!")
        except Exception as e:
            await update.message.reply_text(f"❌ Помилка: {e}")
    else:
        await update.message.reply_text("❌ GROUP_CHAT_ID не вказано в налаштуваннях")


async def weekly_activation_reminder(context: ContextTypes.DEFAULT_TYPE):
    """Щотижневе нагадування в групу активувати бота — поки не всі підключились."""
    if not GROUP_CHAT_ID:
        return

    conn = get_conn()
    # Рахуємо скільки учасниць ще без telegram_id (не активували)
    no_tg = conn.execute(
        "SELECT COUNT(*) as n FROM members WHERE is_active=1 AND telegram_id IS NULL"
    ).fetchone()["n"]
    conn.close()

    if no_tg == 0:
        logger.info("✅ Всі активували бота — щотижневе нагадування не потрібне")
        return

    bot_info = await context.bot.get_me()
    bot_username = bot_info.username

    text = (
        f"👋 Нагадування!\n\n"
        f"{no_tg} учасниць ще не активували бота і не отримуватимуть "
        f"сповіщення про збори на дні народження.\n\n"
        f"Це займає 5 секунд 👇"
    )
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("🤖 Активувати бота", url=f"https://t.me/{bot_username}?start=activate")
    ]])

    kwargs = {"chat_id": GROUP_CHAT_ID, "text": text,
               "reply_markup": keyboard}
    if GROUP_THREAD_ID:
        kwargs["message_thread_id"] = GROUP_THREAD_ID
    try:
        await context.bot.send_message(**kwargs)
        logger.info(f"Щотижневе нагадування надіслано ({no_tg} не активували)")
    except Exception as e:
        logger.error(f"Помилка щотижневого нагадування: {e}")


async def cmd_not_activated(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/notactivated — список учасниць які ще не активували бота."""
    if update.effective_user.id not in ADMIN_IDS:
        return

    conn = get_conn()
    # Ті у кого немає telegram_id — взагалі не відомі боту
    # Ті у кого є telegram_id але вони не написали /start — їх немає в members
    # Порівнюємо members (хто написав дату в гілці) з тими хто написав /start
    rows = conn.execute("""
        SELECT name, telegram_id, birthday FROM members
        WHERE is_active = 1
        ORDER BY name
    """).fetchall()
    conn.close()

    no_tg = []       # є в базі (додані вручну) але без telegram_id
    not_started = [] # є telegram_id але ніколи не писали /start (telegram_id є бо парсили з гілки)

    for r in rows:
        if not r["telegram_id"]:
            no_tg.append(r["name"])

    # Перевіряємо хто з тих що мають telegram_id — заблокували або не писали /start
    # Спробуємо надіслати "тихе" повідомлення — якщо помилка, значить не активували
    # Замість цього просто показуємо тих у кого немає telegram_id або хто не в чаті

    lines = [f"📋 Статус активації бота\n"]

    if no_tg:
        lines.append(f"❌ Немає в Telegram ({len(no_tg)}) — додані вручну:")
        for name in no_tg:
            lines.append(f"  • {name}")
    else:
        lines.append("✅ Всі учасниці мають Telegram акаунт")

    lines.append(f"\n💡 Щоб перевірити хто не активував — натисни /checkactive")
    lines.append(f"\nЩоб нагадати всій групі: /activate")

    await update.message.reply_text("\n".join(lines))


async def cmd_check_active(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/checkactive — перевіряє кожну учасницю чи може бот їй написати."""
    if update.effective_user.id not in ADMIN_IDS:
        return

    await update.message.reply_text("⏳ Перевіряю... це може зайняти хвилину.")

    conn = get_conn()
    rows = conn.execute("""
        SELECT id, name, telegram_id FROM members
        WHERE is_active = 1 AND telegram_id IS NOT NULL
    """).fetchall()
    conn.close()

    cant_reach = []
    can_reach  = 0

    for r in rows:
        try:
            # Пробуємо надіслати "порожню" дію — якщо не заблокували, не буде помилки
            await context.bot.send_chat_action(
                chat_id=r["telegram_id"], action="typing"
            )
            can_reach += 1
        except Exception:
            cant_reach.append(r["name"])

    lines = [
        f"📊 Результати перевірки:\n",
        f"✅ Отримають повідомлення: {can_reach}",
        f"❌ Не активували бота: {len(cant_reach)}\n",
    ]
    if cant_reach:
        lines.append("Ці дівчата не отримають сповіщень:")
        for name in cant_reach:
            lines.append(f"  • {name}")
        lines.append("\nНадіслати нагадування в групу: /activate")

    await update.message.reply_text("\n".join(lines))


def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("activate",    cmd_activate))
    app.add_handler(CommandHandler("notactivated", cmd_not_activated))
    app.add_handler(CommandHandler("checkactive",  cmd_check_active))
    app.add_handler(CommandHandler("mybirthday",  cmd_my_birthday))
    app.add_handler(CommandHandler("myinfo",      cmd_my_info))
    app.add_handler(CommandHandler("editinfo",    cmd_edit_info))
    app.add_handler(CommandHandler("status",      cmd_status))
    app.add_handler(CommandHandler("birthdays",   cmd_birthdays))
    app.add_handler(CommandHandler("members",     cmd_members))
    app.add_handler(CommandHandler("deactivate",  cmd_deactivate))
    app.add_handler(CommandHandler("newbirthday", cmd_new_birthday))
    app.add_handler(CommandHandler("setbirthday", cmd_set_birthday_admin))
    app.add_handler(CommandHandler("eventstatus", cmd_event_status))
    app.add_handler(CommandHandler("remind",      cmd_remind))
    app.add_handler(CommandHandler("testcheck",   cmd_test_check))
    app.add_handler(CommandHandler("clearlog",    cmd_clear_log))
    app.add_handler(CommandHandler("testgroup",   cmd_test_group))
    app.add_handler(CommandHandler("forcebday",   cmd_force_bday))
    app.add_handler(CallbackQueryHandler(callback_paid, pattern=r"^paid_\d+$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(ChatMemberHandler(handle_new_member, ChatMemberHandler.CHAT_MEMBER))

    app.job_queue.run_daily(
        daily_birthday_check,
        time=dtime(hour=CHECK_HOUR_UTC, minute=0),
        name="daily_check"
    )

    # Щотижневе нагадування активувати бота (кожної неділі о 10:00 Київ)
    app.job_queue.run_repeating(
        weekly_activation_reminder,
        interval=7 * 24 * 3600,  # 7 днів
        first=10,                 # перший запуск через 10 секунд після старту
        name="weekly_activation"
    )

    logger.info("🤖 Birthday Fund Bot запущено!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
