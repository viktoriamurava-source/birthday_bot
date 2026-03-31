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
    MessageHandler, filters, ContextTypes,
)

# ─── Конфігурація ───────────────────────────────────────────────────────────
BOT_TOKEN            = os.getenv("BOT_TOKEN", "ВСТАВТЕ_ТОКЕН")
ADMIN_IDS            = [int(x) for x in os.getenv("ADMIN_IDS", "123456789").split(",")]
BIRTHDAY_FUND_AMOUNT = int(os.getenv("BIRTHDAY_FUND_AMOUNT", "4000"))
JAR_LINK             = os.getenv("JAR_LINK", "https://send.monobank.ua/YOUR_LINK")
GROUP_CHAT_ID        = int(os.getenv("GROUP_CHAT_ID", "0"))
BIRTHDAY_THREAD_ID   = int(os.getenv("BIRTHDAY_THREAD_ID", "0")) or None
CHECK_HOUR_UTC       = int(os.getenv("CHECK_HOUR_UTC", "7"))   # 09:00 Київ

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


# ─── БД ─────────────────────────────────────────────────────────────────────
DB_PATH = "birthday_fund.db"

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
            birthday           TEXT,           -- MM-DD
            subscription_until TEXT,           -- YYYY-MM-DD; NULL = безстрокова
            is_active          INTEGER DEFAULT 1,
            joined_at          TEXT DEFAULT CURRENT_TIMESTAMP
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

def text_group_announce(name: str, bd_date: date, count: int, amount: int) -> str:
    """За 3 дні — анонс у групу."""
    mo = bd_date.month
    d  = bd_date.day
    return (
        f"🎀 Дівчата, у нас скоро іменинниця!\n\n"
        f"🌸 *{name}* святкує день народження {d} {MONTH_GENITIVE_UA[mo]}!\n\n"
        f"💰 Збираємо *{BIRTHDAY_FUND_AMOUNT} грн* — по *{amount} грн* з кожної\n\n"
        f"Скидаємось сюди 👇\n"
        f"💳 {JAR_LINK}"
    )

def text_personal_announce(birthday_name: str, amount: int) -> str:
    """За 3 дні — особисте повідомлення кожній учасниці."""
    return (
        f"🎀 У нашій спільноті скоро іменинниця!\n\n"
        f"🌸 *{birthday_name}* святкує день народження\n\n"
        f"💰 Твоя частина: *{amount} грн*\n\n"
        f"💳 Переказати на банку:\n{JAR_LINK}\n\n"
        f"_Після переказу натисни кнопку нижче — "
        f"так ми бачимо загальну картину збору_ 🙏"
    )

def text_group_day_before(name: str, bd_date: date,
                          paid: int, total: int) -> str:
    """За 1 день — нагадування в групу з відсотком оплат."""
    percent = round(paid / total * 100) if total else 0
    remaining = total - paid
    return (
        f"⏰ Нагадування — *завтра* день народження!\n\n"
        f"🌸 *{name}* ({bd_date.strftime('%d.%m')})\n\n"
        f"📊 Вже здали: *{paid} із {total}* ({percent}%)\n"
        f"❌ Ще не здали: *{remaining}*\n\n"
        f"Дівчата, хто ще не встиг — перевірте особисті повідомлення 💳"
    )

def text_personal_reminder(birthday_name: str, bd_date: date, amount: int) -> str:
    """За 1 день — нагадування боржниці в особисті."""
    return (
        f"👋 Нагадуємо!\n\n"
        f"Твій внесок на день народження *{birthday_name}* "
        f"({bd_date.strftime('%d.%m')}) ще не зафіксовано.\n\n"
        f"⚠️ *Завтра* вже день народження — встигни сьогодні!\n\n"
        f"💰 Сума: *{amount} грн*\n"
        f"💳 {JAR_LINK}\n\n"
        f"Після переказу натисни кнопку ↓"
    )

def text_group_birthday(name: str, bd_date: date) -> str:
    """В день ДН — святкове повідомлення в групу (без грошей)."""
    mo = bd_date.month
    d  = bd_date.day
    return (
        f"🎂 Сьогодні день народження!\n\n"
        f"🌸 Наша улюблена *{name}* святкує {d} {MONTH_GENITIVE_UA[mo]}!\n\n"
        f"Дівчата, давайте всі разом привітаємо іменинницю! 🥳🎉\n\n"
        f"З днем народження, *{name}*! 💐"
    )

def text_remind_manual(birthday_name: str, bd_date: date, amount: int) -> str:
    """Ручне нагадування (команда /remind)."""
    return (
        f"👋 Нагадуємо!\n\n"
        f"Внесок на день народження *{birthday_name}* "
        f"({bd_date.strftime('%d.%m')}) ще не зафіксовано.\n\n"
        f"💰 Сума: *{amount} грн*\n"
        f"💳 {JAR_LINK}\n\n"
        f"Після переказу натисни кнопку ↓"
    )


# ─── Надсилання в групу ─────────────────────────────────────────────────────

async def send_to_group(context: ContextTypes.DEFAULT_TYPE, text: str):
    if not GROUP_CHAT_ID:
        return
    kwargs = {"chat_id": GROUP_CHAT_ID, "text": text, "parse_mode": "Markdown"}
    if BIRTHDAY_THREAD_ID:
        kwargs["message_thread_id"] = BIRTHDAY_THREAD_ID
    try:
        await context.bot.send_message(**kwargs)
    except Exception as e:
        logger.error(f"Помилка надсилання в групу: {e}")


# ─── Планувальник ───────────────────────────────────────────────────────────

async def daily_birthday_check(context: ContextTypes.DEFAULT_TYPE):
    today = date.today()
    logger.info(f"⏰ Перевірка ДН: {today}")

    conn = get_conn()
    all_members = conn.execute("""
        SELECT id, telegram_id, name, birthday
        FROM members WHERE is_active = 1 AND birthday IS NOT NULL
    """).fetchall()
    conn.close()

    for member in all_members:
        month, day = map(int, member["birthday"].split("-"))
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
            await send_to_group(context, text_group_birthday(m["name"], bd))
            log_reminder(m["id"], 0, today.year, "group")


async def _do_announce(context, member: dict, bd_date: date):
    """За 3 дні: анонс у групу + особисті повідомлення."""
    active = get_active_members()
    count  = len(active) - 1  # іменинниця не платить
    if count <= 0:
        count = len(active)   # якщо вона єдина — все одно показуємо
    amount = round(BIRTHDAY_FUND_AMOUNT / count)

    # Створюємо подію
    event_id = get_event_for_member_date(member["id"], bd_date)
    if not event_id:
        event_id = create_event(member, bd_date, amount, count, active)

    # В групу
    await send_to_group(context, text_group_announce(member["name"], bd_date, count, amount))

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
                text=text_personal_announce(member["name"], amount),
                parse_mode="Markdown",
                reply_markup=keyboard
            )
            sent += 1
        except Exception as e:
            logger.warning(f"Не надіслано {m['name']}: {e}")
            failed += 1

    logger.info(f"Анонс ДН {member['name']}: надіслано {sent}, помилок {failed}")


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
                parse_mode="Markdown",
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
                    f"📋 Боржниці — ДН *{member['name']}* "
                    f"({bd_date.strftime('%d.%m')})\n\n"
                    f"❌ Не оплатили ({len(unpaid)}/{total}):\n{names_list}\n\n"
                    f"Нагадати вручну: /remind"
                ),
                parse_mode="Markdown"
            )
        except Exception:
            pass

    logger.info(f"Нагадувань: {sent}/{len(unpaid)} для ДН {member['name']}")


# ─── Авто-парсинг з гілки ───────────────────────────────────────────────────

async def handle_group_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return
    is_our_thread = (BIRTHDAY_THREAD_ID is None or
                     message.message_thread_id == BIRTHDAY_THREAD_ID)
    is_our_group  = (GROUP_CHAT_ID == 0 or message.chat_id == GROUP_CHAT_ID)
    if not (is_our_group and is_our_thread):
        return
    user = message.from_user
    if not user:
        return
    text   = message.text or message.caption or ""
    result = parse_birthday(text)
    if not result:
        return
    day, month = result
    bd = f"{month:02d}-{day:02d}"
    conn = get_conn()
    existing = conn.execute(
        "SELECT id FROM members WHERE telegram_id=?", (user.id,)
    ).fetchone()
    if existing:
        conn.execute("UPDATE members SET birthday=?, name=? WHERE telegram_id=?",
                     (bd, user.full_name, user.id))
    else:
        conn.execute(
            "INSERT OR IGNORE INTO members (telegram_id, name, birthday) VALUES (?,?,?)",
            (user.id, user.full_name, bd)
        )
    conn.commit()
    conn.close()
    logger.info(f"Збережено ДН {user.full_name}: {day:02d}.{month:02d}")


# ─── Команди ────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO members (telegram_id, name) VALUES (?,?)",
                 (user.id, user.full_name))
    conn.commit()
    conn.close()

    text = (
        f"Привіт, {user.first_name}! 👋\n\n"
        "Я стежу за днями народження в нашій спільноті 🎂\n\n"
        "/mybirthday — вказати свій ДН\n"
        "/status     — мої оплати\n"
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
        )
    await update.message.reply_text(text)


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
            f"{icon} ДН {r['birthday_person_name']} "
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
    await send_to_group(context, text_group_announce(person_name, today, count, amount))

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
                parse_mode="Markdown",
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
        f"📊 ДН *{event['birthday_person_name']}* | {event['event_date']}",
        f"💰 {event['amount_per_person']:.0f} грн × {total} = {BIRTHDAY_FUND_AMOUNT} грн",
        f"💵 Зібрано: {collected:.0f} / {BIRTHDAY_FUND_AMOUNT} грн ({percent}%)\n",
        f"✅ Оплатили ({len(paid_rows)}):",
    ]
    for r in paid_rows:
        lines.append(f"  • {r['name']}")
    lines.append(f"\n❌ Не здали ({len(unpaid_rows)}):")
    for r in unpaid_rows:
        lines.append(f"  • {r['name']}")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


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
                parse_mode="Markdown",
                reply_markup=keyboard
            )
            sent += 1
        except Exception:
            pass

    # В групу — поточний відсоток
    await send_to_group(context, text_group_day_before(
        event["birthday_person_name"], bd_date, paid, event["total_members"]
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
        await update.message.reply_text(f"❌ Не розпізнала дату: «{date_str}»")
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
        lines.append(f"📅 {MONTH_NAMES_UA[mo]}:")
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
        f"✅ Дякуємо! Оплату зафіксовано 💕\n\n"
        f"🎂 ДН {bd_name}"
    )

    # Повідомлення адміну
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                admin_id,
                f"💰 *{query.from_user.full_name}* відмітила оплату\n"
                f"🎂 ДН {bd_name}",
                parse_mode="Markdown"
            )
        except Exception:
            pass


# ─── Обробник тексту ────────────────────────────────────────────────────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Повідомлення з групи — парсимо дати з гілки
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        await handle_group_message(update, context)
        return

    waiting = context.user_data.get("waiting_for")
    text    = update.message.text.strip()
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

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("mybirthday",  cmd_my_birthday))
    app.add_handler(CommandHandler("status",      cmd_status))
    app.add_handler(CommandHandler("birthdays",   cmd_birthdays))
    app.add_handler(CommandHandler("members",     cmd_members))
    app.add_handler(CommandHandler("deactivate",  cmd_deactivate))
    app.add_handler(CommandHandler("newbirthday", cmd_new_birthday))
    app.add_handler(CommandHandler("setbirthday", cmd_set_birthday_admin))
    app.add_handler(CommandHandler("eventstatus", cmd_event_status))
    app.add_handler(CommandHandler("remind",      cmd_remind))
    app.add_handler(CommandHandler("testcheck",   cmd_test_check))
    app.add_handler(CallbackQueryHandler(callback_paid, pattern=r"^paid_\d+$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    app.job_queue.run_daily(
        daily_birthday_check,
        time=dtime(hour=CHECK_HOUR_UTC, minute=0),
        name="daily_check"
    )

    logger.info("🤖 Birthday Fund Bot запущено!")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
