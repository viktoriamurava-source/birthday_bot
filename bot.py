#!/usr/bin/env python3
"""
Community Bot v2 — повний рефакторинг
Комуна Жіноцтва — бот підтримки спільноти
"""

import logging
import sqlite3
import re
import os
import json
from datetime import datetime, date, time as dtime, timedelta
from typing import Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ChatMemberHandler, filters, ContextTypes,
)

# ─── Конфігурація ────────────────────────────────────────────────────────────
BOT_TOKEN           = os.getenv("BOT_TOKEN", "")
ADMIN_IDS           = [int(x) for x in os.getenv("ADMIN_IDS", "123456789").split(",")]
GROUP_CHAT_ID       = int(os.getenv("GROUP_CHAT_ID", "0"))
GROUP_THREAD_ID     = int(os.getenv("GROUP_THREAD_ID", "0")) or None
BIRTHDAY_THREAD_ID  = int(os.getenv("BIRTHDAY_THREAD_ID", "0")) or None
CONGRATS_THREAD_ID  = int(os.getenv("CONGRATS_THREAD_ID", "0")) or None
CHECK_HOUR_UTC      = int(os.getenv("CHECK_HOUR_UTC", "17"))
JAR_LINK            = os.getenv("JAR_LINK", "https://send.monobank.ua/jar/YOUR")
AMOUNT_PER_PERSON   = int(os.getenv("AMOUNT_PER_PERSON", "88"))
INVITE_LINK         = os.getenv("INVITE_LINK", "https://t.me/+YOUR")
INSTAGRAM_COMMUNITY = os.getenv("INSTAGRAM_COMMUNITY", "https://www.instagram.com/your_community/")
INSTAGRAM_FOUNDER   = os.getenv("INSTAGRAM_FOUNDER", "https://www.instagram.com/vmuravska/")
FORWARD_CHANNEL_ID  = int(os.getenv("FORWARD_CHANNEL_ID", "0"))
WFP_SUB_URL         = os.getenv("WFP_SUB_URL", "https://secure.wayforpay.com/sub/womenscommune")
WFP_PRODUCT_3M      = os.getenv("WFP_PRODUCT_3M", "Легкий старт")
WFP_PRODUCT_6M      = os.getenv("WFP_PRODUCT_6M", "Впевнена стабільність")
WFP_PRODUCT_1Y      = os.getenv("WFP_PRODUCT_1Y", "Тотальна довіра")
WFP_MERCHANT        = os.getenv("WFP_MERCHANT_ACCOUNT", "")
WFP_SECRET          = os.getenv("WFP_SECRET_KEY", "")
WFP_DOMAIN          = os.getenv("WFP_DOMAIN", "your-domain.railway.app")
GEMINI_API_KEY      = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL        = os.getenv("GEMINI_MODEL", "gemini-pro")

# ─── Логування ───────────────────────────────────────────────────────────────
logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# ─── Місяці ──────────────────────────────────────────────────────────────────
MONTH_NAMES_UA = ["","Січень","Лютий","Березень","Квітень","Травень","Червень",
                  "Липень","Серпень","Вересень","Жовтень","Листопад","Грудень"]
MONTH_GENITIVE_UA = ["","січня","лютого","березня","квітня","травня","червня",
                     "липня","серпня","вересня","жовтня","листопада","грудня"]
_MONTH_PARSE = {
    "січня":1,"січень":1,"лютого":2,"лютий":2,"березня":3,"березень":3,
    "квітня":4,"квітень":4,"травня":5,"травень":5,"червня":6,"червень":6,
    "липня":7,"липень":7,"серпня":8,"серпень":8,"вересня":9,"вересень":9,
    "жовтня":10,"жовтень":10,"листопада":11,"листопад":11,"грудня":12,"грудень":12,
}

def parse_birthday(text: str) -> Optional[tuple]:
    t = text.lower().strip()
    m = re.search(r'\b(\d{1,2})[.\-/](\d{1,2})\b', t)
    if m:
        d, mo = int(m.group(1)), int(m.group(2))
        if 1 <= d <= 31 and 1 <= mo <= 12:
            return d, mo
    for name, month_num in _MONTH_PARSE.items():
        for pat in (rf'\b(\d{{1,2}})\s+{re.escape(name)}\b', rf'\b{re.escape(name)}\s+(\d{{1,2}})\b'):
            m = re.search(pat, t)
            if m:
                d = int(m.group(1))
                if 1 <= d <= 31:
                    return d, month_num
    return None

def parse_birth_year(text: str) -> Optional[int]:
    m = re.search(r'\b(19[7-9]\d|200\d|201[0-9])\b', text)
    return int(m.group(1)) if m else None

def instagram_link(insta: str) -> str:
    if not insta:
        return ""
    nick = insta.lstrip("@").rstrip("/")
    return f"https://www.instagram.com/{nick}/"

# ─── БД ──────────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "/data/community.db")

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
            name               TEXT NOT NULL DEFAULT '',
            username           TEXT,
            birthday           TEXT,
            birth_year         INTEGER,
            city               TEXT,
            nova_poshta        TEXT,
            instagram          TEXT,
            favorite_color     TEXT,
            wishlist           TEXT,
            phone              TEXT,
            subscription_until TEXT,
            subscription_plan  TEXT,
            is_active          INTEGER DEFAULT 1,
            onboarding_done    INTEGER DEFAULT 0,
            joined_at          TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS birthday_events (
            id                   INTEGER PRIMARY KEY AUTOINCREMENT,
            birthday_person_name TEXT NOT NULL,
            birthday_person_id   INTEGER,
            event_date           TEXT NOT NULL,
            amount_per_person    REAL NOT NULL,
            total_members        INTEGER NOT NULL,
            created_at           TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS payments (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id  INTEGER NOT NULL,
            member_id INTEGER NOT NULL,
            amount    REAL NOT NULL,
            paid      INTEGER DEFAULT 0,
            paid_at   TEXT,
            UNIQUE (event_id, member_id)
        );
        CREATE TABLE IF NOT EXISTS events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            title        TEXT NOT NULL,
            description  TEXT,
            location     TEXT,
            event_date   TEXT NOT NULL,
            event_time   TEXT,
            is_paid      INTEGER DEFAULT 0,
            price        INTEGER DEFAULT 0,
            wfp_link     TEXT,
            max_spots    INTEGER DEFAULT 0,
            spots_left   INTEGER DEFAULT 0,
            is_active    INTEGER DEFAULT 1,
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS event_registrations (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id  INTEGER NOT NULL,
            member_id INTEGER NOT NULL,
            paid      INTEGER DEFAULT 0,
            UNIQUE (event_id, member_id)
        );
        CREATE TABLE IF NOT EXISTS reminder_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            member_id   INTEGER NOT NULL,
            days_before INTEGER NOT NULL,
            year        INTEGER NOT NULL,
            log_type    TEXT NOT NULL,
            sent_at     TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (member_id, days_before, year, log_type)
        );
        CREATE TABLE IF NOT EXISTS sub_reminder_log (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            member_id INTEGER NOT NULL,
            log_type  TEXT NOT NULL,
            sent_date TEXT NOT NULL,
            UNIQUE (member_id, log_type, sent_date)
        );
        CREATE TABLE IF NOT EXISTS message_activity (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            member_id INTEGER,
            msg_date  TEXT,
            msg_count INTEGER DEFAULT 0,
            UNIQUE (member_id, msg_date)
        );
        CREATE TABLE IF NOT EXISTS recurring_events (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            title        TEXT NOT NULL,
            description  TEXT,
            location     TEXT,
            weekday      INTEGER NOT NULL,
            event_time   TEXT NOT NULL,
            is_active    INTEGER DEFAULT 1,
            active_until TEXT NOT NULL,
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS recurring_registrations (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id   INTEGER NOT NULL,
            member_id  INTEGER NOT NULL,
            week_start TEXT NOT NULL,
            UNIQUE (event_id, member_id, week_start)
        );
    """)
    for col, definition in [
        ("username", "TEXT"), ("birth_year", "INTEGER"), ("city", "TEXT"),
        ("wishlist", "TEXT"), ("phone", "TEXT"), ("subscription_plan", "TEXT"),
        ("onboarding_done", "INTEGER DEFAULT 0"),
    ]:
        try:
            conn.execute(f"ALTER TABLE members ADD COLUMN {col} {definition}")
        except Exception:
            pass
    conn.commit()
    conn.close()
    logger.info("БД ініціалізована")

# ─── Хелпери БД ──────────────────────────────────────────────────────────────

def get_member(telegram_id: int) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("SELECT * FROM members WHERE telegram_id=?", (telegram_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_member_by_id(mid: int) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("SELECT * FROM members WHERE id=?", (mid,)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_active_members() -> list:
    today = date.today().isoformat()
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM members WHERE is_active=1 AND (subscription_until IS NULL OR subscription_until >= ?) ORDER BY name",
        (today,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def has_active_sub(member: dict) -> bool:
    s = member.get("subscription_until")
    return bool(s and s >= date.today().isoformat())

def upsert_member(telegram_id: int, name: str, username: str = None):
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO members (telegram_id, name) VALUES (?,?)", (telegram_id, name))
    conn.execute("UPDATE members SET name=? WHERE telegram_id=?", (name, telegram_id))
    if username:
        conn.execute("UPDATE members SET username=? WHERE telegram_id=?", (f"@{username}", telegram_id))
    conn.commit()
    conn.close()

def already_reminded(member_id, days_before, year, log_type) -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT id FROM reminder_log WHERE member_id=? AND days_before=? AND year=? AND log_type=?",
        (member_id, days_before, year, log_type)
    ).fetchone()
    conn.close()
    return row is not None

def log_reminder(member_id, days_before, year, log_type):
    conn = get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO reminder_log (member_id, days_before, year, log_type) VALUES (?,?,?,?)",
        (member_id, days_before, year, log_type)
    )
    conn.commit()
    conn.close()

def track_activity(telegram_id: int):
    today = date.today().isoformat()
    conn = get_conn()
    member = conn.execute("SELECT id FROM members WHERE telegram_id=?", (telegram_id,)).fetchone()
    if member:
        conn.execute(
            "INSERT INTO message_activity (member_id, msg_date, msg_count) VALUES (?,?,1) "
            "ON CONFLICT (member_id, msg_date) DO UPDATE SET msg_count = msg_count + 1",
            (member["id"], today)
        )
        conn.commit()
    conn.close()

# ─── Gemini ──────────────────────────────────────────────────────────────────

async def gemini_call(prompt: str, max_tokens: int = 400) -> str:
    if not GEMINI_API_KEY:
        return ""
    try:
        import aiohttp
        for api_url in [
            f"https://generativelanguage.googleapis.com/v1/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
            f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}",
            f"https://generativelanguage.googleapis.com/v1/models/gemini-pro:generateContent?key={GEMINI_API_KEY}",
        ]:
            async with aiohttp.ClientSession() as session:
                async with session.post(api_url, json={
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"maxOutputTokens": max_tokens, "temperature": 0.8}
                }, timeout=aiohttp.ClientTimeout(total=15)) as resp:
                    data = await resp.json()
                    candidates = data.get("candidates", [])
                    if candidates:
                        return candidates[0]["content"]["parts"][0]["text"].strip()
                    if data.get("error", {}).get("code") == 404:
                        continue
                    logger.error(f"Gemini error: {data.get('error', data)}")
                    return ""
    except Exception as e:
        logger.error(f"Gemini exception: {e}")
    return ""

async def gemini_birthday(member: dict, bd_date: date) -> str:
    age = (bd_date.year - member["birth_year"]) if member.get("birth_year") else None
    info = f"ім'я {member['name']}"
    if age:
        info += f", {age} років"
    if member.get("city"):
        info += f", місто {member['city']}"
    if member.get("favorite_color"):
        info += f", колір {member['favorite_color']}"
    if member.get("wishlist"):
        info += f", вішліст {member['wishlist']}"
    return await gemini_call(
        f"Ти — бот жіночої спільноти. Напиши тепле привітання з ДН для {info}. "
        f"Звернись до всіх дівчат. З емодзі. До 80 слів.", 200
    )

async def gemini_weekly(events: list, new_members: list, active_members: list) -> str:
    return await gemini_call(
        f"Ти — бот Комуни Жіноцтва. Напиши теплий дайджест на понеділок.\n"
        f"Події: {', '.join(e['title'] + ' ' + e['event_date'] for e in events) or 'немає'}\n"
        f"Нові: {', '.join(m['name'] for m in new_members) or 'немає'}\n"
        f"Активні: {', '.join(m['name'] for m in active_members[:3]) or 'немає'}\n"
        f"Стиль: тепло, з емодзі. До 150 слів.", 300
    )

async def gemini_personal(member: dict, events: list, buddy: Optional[dict]) -> str:
    buddy_str = f"Учасниця з твого міста: {buddy['name']}" if buddy else ""
    return await gemini_call(
        f"Ти — бот Комуни Жіноцтва. Особистий дайджест для {member['name']}.\n"
        f"Події: {', '.join(e['title'] + ' ' + e['event_date'] for e in events) or 'немає'}\n"
        f"{buddy_str}\nСтиль: тепло, особисто. До 100 слів.", 200
    )

async def gemini_reply(question: str, topic: str = "") -> str:
    topic_context = {
        "підписка": "Дівчина питає про оплату або поновлення підписки. Поясни що треба перейти за посиланням WayForPay і обрати план. НЕ згадуй /start.",
        "бот": "Дівчина питає про бота. Поясни що треба написати /start боту в особисті (не в групі). Можна згадати що там є меню з усіма функціями.",
        "нова пошта": "Дівчина шукає адресу для відправки подарунку. Поясни що адреса є в профілі учасниці в боті через кнопку Знайти учасницю. НЕ згадуй /start.",
        "події": "Дівчата обговорюють подію або зустріч. Відповідай як учасниця спільноти — запитай коли домовились або запропонуй додати подію в бот. НЕ вставляй /start і не рекламуй бота.",
        "день народження": "Питання про подарунок або збір на ДН. Поясни що збори відбуваються автоматично через бота, вішліст іменинниці є в особистому повідомленні. НЕ згадуй /start.",
    }
    ctx = topic_context.get(topic, "Відповідай ситуативно і тепло.")
    return await gemini_call(
        f"Ти — учасниця і помічниця жіночої спільноти Комуна Жіноцтва. {ctx}\n\n"
        f"Повідомлення з чату: {question}\n\n"
        f"Відповідь: тепло, коротко, ситуативно. До 60 слів. Не починай з 'Привіт'.", 150
    )

# ─── Надсилання в групу ───────────────────────────────────────────────────────

async def send_to_group(context: ContextTypes.DEFAULT_TYPE, text: str, congrats: bool = False):
    if not GROUP_CHAT_ID:
        return
    kwargs = {"chat_id": GROUP_CHAT_ID, "text": text}
    if GROUP_THREAD_ID:
        kwargs["message_thread_id"] = GROUP_THREAD_ID
    try:
        await context.bot.send_message(**kwargs)
    except Exception as e:
        logger.error(f"Помилка в групу: {e}")
    if congrats and CONGRATS_THREAD_ID and CONGRATS_THREAD_ID != GROUP_THREAD_ID:
        try:
            await context.bot.send_message(chat_id=GROUP_CHAT_ID, text=text, message_thread_id=CONGRATS_THREAD_ID)
        except Exception as e:
            logger.error(f"Помилка в гілку привітань: {e}")

# ─── UI хелпери ───────────────────────────────────────────────────────────────

PERSISTENT_KB = ReplyKeyboardMarkup([["Головне меню"]], resize_keyboard=True, is_persistent=True)

def back_btn(label="Назад", data="menu"):
    return InlineKeyboardButton(f"← {label}", callback_data=data)

def menu_btn():
    return InlineKeyboardButton("Головне меню", callback_data="menu")

async def show_menu(target, context, member: dict):
    sub = member.get("subscription_until")
    sub_text = f"Підписка до {date.fromisoformat(sub).strftime('%d.%m.%Y')}" if sub and sub >= date.today().isoformat() else "Підписка не активна"
    text = f"Привіт, {member['name']}!\n\n{sub_text}\n\nЩо хочеш зробити?"
    # Визначаємо чи адмін
    is_admin = member.get("telegram_id") in ADMIN_IDS

    admin_rows = [
        [InlineKeyboardButton("Адмін-панель", callback_data="admin_panel")],
    ] if is_admin else []

    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Моя анкета", callback_data="profile"),
         InlineKeyboardButton("Вішліст", callback_data="wishlist")],
        [InlineKeyboardButton("Знайти учасницю", callback_data="search"),
         InlineKeyboardButton("Події", callback_data="events")],
        [InlineKeyboardButton("Моя підписка", callback_data="subscription"),
         InlineKeyboardButton("Мої оплати ДН", callback_data="bday_status")],
        [InlineKeyboardButton("Instagram комуни", url=INSTAGRAM_COMMUNITY),
         InlineKeyboardButton("Instagram засновниці", url=INSTAGRAM_FOUNDER)],
        *admin_rows,
    ])
    if hasattr(target, "edit_message_text"):
        try:
            await target.edit_message_text(text, reply_markup=kb)
            return
        except Exception:
            pass
        msg = target.message
    else:
        msg = target.message
    await msg.reply_text(text, reply_markup=kb)
    await msg.reply_text("Натисни кнопку нижче щоб відкрити меню в будь-який момент", reply_markup=PERSISTENT_KB)

# ─── Онбординг ────────────────────────────────────────────────────────────────

QUESTIONS = {
    "birthday":      "Введи свій день народження (ДД.ММ.РРРР)\nНаприклад: 25.04.1995\n_(або «-» пропустити)_",
    "city":          "Напиши своє місто\n_(або «-» пропустити)_",
    "phone":         "Номер телефону (+380991234567)\nБуде видно іншим учасницям для НП\n_(або «-» пропустити)_",
    "nova_poshta":   "Відділення Нової пошти\nНаприклад: НП відділення 47, Київ\n_(або «-» пропустити)_",
    "instagram":     "Instagram нікнейм (@kateryna)\n_(або «-» пропустити)_",
    "favorite_color":"Улюблений колір\n_(або «-» пропустити)_",
    "wishlist":      "Посилання на вішліст або що хочеш отримати в подарунок\n\nРекомендуємо: https://goodsend.it/intro/1\n_(або «-» пропустити)_",
}

async def start_onboarding(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["onboarding_step"] = "birthday"
    await update.message.reply_text("Давай заповнимо твою анкету!\n\n" + QUESTIONS["birthday"])

async def handle_onboarding(update: Update, context: ContextTypes.DEFAULT_TYPE, step: str, text: str):
    uid = update.effective_user.id
    conn = get_conn()
    next_step = None
    if step == "birthday":
        result = parse_birthday(text)
        if result and text != "-":
            day, month = result
            conn.execute("UPDATE members SET birthday=? WHERE telegram_id=?", (f"{month:02d}-{day:02d}", uid))
            year = parse_birth_year(text)
            if year:
                conn.execute("UPDATE members SET birth_year=? WHERE telegram_id=?", (year, uid))
        next_step = "city"
    elif step == "city":
        if text != "-":
            conn.execute("UPDATE members SET city=? WHERE telegram_id=?", (text, uid))
        next_step = "phone"
    elif step == "phone":
        if text != "-":
            conn.execute("UPDATE members SET phone=? WHERE telegram_id=?", (text, uid))
        next_step = "nova_poshta"
    elif step == "nova_poshta":
        if text != "-":
            conn.execute("UPDATE members SET nova_poshta=? WHERE telegram_id=?", (text, uid))
        next_step = "instagram"
    elif step == "instagram":
        if text != "-":
            insta = text if text.startswith("@") else "@" + text
            conn.execute("UPDATE members SET instagram=? WHERE telegram_id=?", (insta, uid))
        next_step = "favorite_color"
    elif step == "favorite_color":
        if text != "-":
            conn.execute("UPDATE members SET favorite_color=? WHERE telegram_id=?", (text, uid))
        next_step = "wishlist"
    elif step == "wishlist":
        if text != "-":
            conn.execute("UPDATE members SET wishlist=? WHERE telegram_id=?", (text, uid))
        conn.execute("UPDATE members SET onboarding_done=1 WHERE telegram_id=?", (uid,))
        conn.commit()
        conn.close()
        context.user_data.pop("onboarding_step", None)
        member = get_member(uid)
        await update.message.reply_text("Анкету заповнено! Дякуємо!")
        await show_menu(update, context, member)
        return
    conn.commit()
    conn.close()
    if next_step:
        context.user_data["onboarding_step"] = next_step
        await update.message.reply_text(QUESTIONS[next_step])

# ─── ДН тексти ────────────────────────────────────────────────────────────────

def _extra_info(member: dict) -> str:
    parts = []
    if member.get("nova_poshta"):
        parts.append(f"НП: {member['nova_poshta']}")
    if member.get("instagram"):
        parts.append(f"Instagram: {instagram_link(member['instagram'])}")
    if member.get("favorite_color"):
        parts.append(f"Улюблений колір: {member['favorite_color']}")
    if member.get("wishlist"):
        parts.append(f"Вішліст: {member['wishlist']}")
    return "\n" + "\n".join(parts) if parts else ""

def bday_group_text(member: dict, bd_date: date, days_until: int, amount: int) -> str:
    mo, d = bd_date.month, bd_date.day
    uname = f" ({member['username']})" if member.get("username") else ""
    if days_until == 0:
        header = "Сьогодні день народження!"
    elif days_until == 1:
        header = "Завтра день народження!"
    else:
        header = f"Через {days_until} дні день народження!"
    return (
        f"Дівчата, {header}\n\n"
        f"{member['name']}{uname} святкує {d} {MONTH_GENITIVE_UA[mo]}!"
        f"{_extra_info(member)}\n\n"
        f"Збираємо — по {amount} грн з кожної\n\nСкидаємось сюди:\n{JAR_LINK}"
    )

def bday_personal_text(member: dict, amount: int) -> str:
    uname = f" ({member['username']})" if member.get("username") else ""
    return (
        f"У нашій спільноті скоро іменинниця!\n\n"
        f"{member['name']}{uname} святкує день народження"
        f"{_extra_info(member)}\n\n"
        f"Твоя частина: {amount} грн\n\n"
        f"Переказати на банку:\n{JAR_LINK}\n\n"
        f"Після переказу натисни кнопку"
    )

async def bday_congrats_text(member: dict, bd_date: date) -> str:
    greeting = await gemini_birthday(member, bd_date)
    if greeting:
        return greeting
    mo, d = bd_date.month, bd_date.day
    uname = f" ({member['username']})" if member.get("username") else ""
    age = (bd_date.year - member["birth_year"]) if member.get("birth_year") else None
    age_str = f"\nВиповнюється {age} років!" if age else ""
    wl_str = f"\n\nВішліст іменинниці:\n{member['wishlist']}" if member.get("wishlist") else ""
    return (
        f"Сьогодні день народження!\n\n"
        f"Наша улюблена {member['name']}{uname} святкує {d} {MONTH_GENITIVE_UA[mo]}!{age_str}"
        f"{wl_str}\n\nДівчата, давайте привітаємо!\n\nЗ днем народження, {member['name']}!"
    )

# ─── Планувальник ─────────────────────────────────────────────────────────────

async def _create_bday_event(conn, member: dict, bd: date, payers: list) -> int:
    amount = AMOUNT_PER_PERSON
    conn.execute(
        "INSERT INTO birthday_events (birthday_person_name, birthday_person_id, event_date, amount_per_person, total_members) VALUES (?,?,?,?,?)",
        (member["name"], member.get("id"), bd.isoformat(), amount, len(payers))
    )
    event_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for p in payers:
        conn.execute("INSERT OR IGNORE INTO payments (event_id, member_id, amount) VALUES (?,?,?)",
                     (event_id, p["id"], amount))
    return event_id

async def daily_check(context: ContextTypes.DEFAULT_TYPE):
    today = date.today()
    logger.info(f"Щоденна перевірка: {today}")
    await _check_bdays(context, today)
    await _check_subs(context, today)
    await _check_event_reminders(context, today)

async def _check_bdays(context, today: date):
    conn = get_conn()
    members = conn.execute("SELECT * FROM members WHERE is_active=1 AND birthday IS NOT NULL").fetchall()
    conn.close()
    active = get_active_members()
    for member in members:
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
        days = (bd - today).days
        m = dict(member)
        payers = [x for x in active if x["id"] != m["id"]]
        amount = AMOUNT_PER_PERSON

        if days == 3 and not already_reminded(m["id"], 3, today.year, "group"):
            conn = get_conn()
            event_id = await _create_bday_event(conn, m, bd, payers)
            conn.commit()
            conn.close()
            await send_to_group(context, bday_group_text(m, bd, 3, amount))
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("Я оплатила!", callback_data=f"bday_paid_{event_id}")]])
            for p in payers:
                if not p.get("telegram_id"):
                    continue
                try:
                    await context.bot.send_message(p["telegram_id"], bday_personal_text(m, amount), reply_markup=kb)
                except Exception:
                    pass
            # Нагадування іменинниці про вішліст
            if m.get("telegram_id"):
                wl = m.get("wishlist") or ""
                try:
                    await context.bot.send_message(
                        m["telegram_id"],
                        f"За 3 дні твій день народження!\n\nНагадую оновити вішліст.\n"
                        f"{'Поточний: ' + wl if wl else 'Ще немає вішлісту.'}\n\n"
                        f"Рекомендуємо: https://goodsend.it/intro/1",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Оновити вішліст", callback_data="edit_wishlist")]])
                    )
                except Exception:
                    pass
            log_reminder(m["id"], 3, today.year, "group")

        elif days == 1 and not already_reminded(m["id"], 1, today.year, "group"):
            conn = get_conn()
            ev = conn.execute(
                "SELECT * FROM birthday_events WHERE birthday_person_id=? AND event_date=? ORDER BY id DESC LIMIT 1",
                (m["id"], bd.isoformat())
            ).fetchone()
            if ev:
                paid = conn.execute("SELECT COUNT(*) as n FROM payments WHERE event_id=? AND paid=1", (ev["id"],)).fetchone()["n"]
                total = ev["total_members"]
                percent = round(paid / total * 100) if total else 0
                uname = f" ({m['username']})" if m.get("username") else ""
                await send_to_group(context, (
                    f"Нагадування — завтра день народження!\n\n{m['name']}{uname}\n\n"
                    f"Вже здали: {percent}%\n\nХто не встиг — перевірте особисті повідомлення"
                ))
                unpaid = conn.execute("""
                    SELECT m.telegram_id FROM payments p JOIN members m ON p.member_id=m.id
                    WHERE p.event_id=? AND p.paid=0
                """, (ev["id"],)).fetchall()
                kb = InlineKeyboardMarkup([[InlineKeyboardButton("Я оплатила!", callback_data=f"bday_paid_{ev['id']}")]])
                for u in unpaid:
                    if not u["telegram_id"] or u["telegram_id"] == m.get("telegram_id"):
                        continue
                    try:
                        await context.bot.send_message(u["telegram_id"], bday_personal_text(m, amount), reply_markup=kb)
                    except Exception:
                        pass
                # Адміну список боржниць
                unpaid_names = conn.execute("""
                    SELECT mem.name FROM payments p JOIN members mem ON p.member_id=mem.id
                    WHERE p.event_id=? AND p.paid=0
                """, (ev["id"],)).fetchall()
                if unpaid_names:
                    names_str = "\n".join(f"• {u['name']}" for u in unpaid_names)
                    for admin_id in ADMIN_IDS:
                        try:
                            await context.bot.send_message(admin_id, f"Боржниці — ДН {m['name']}:\n{names_str}")
                        except Exception:
                            pass
            conn.close()
            log_reminder(m["id"], 1, today.year, "group")

        elif days == 0 and not already_reminded(m["id"], 0, today.year, "group"):
            text = await bday_congrats_text(m, bd)
            await send_to_group(context, text, congrats=True)
            log_reminder(m["id"], 0, today.year, "group")

async def _check_subs(context, today: date):
    conn = get_conn()
    members = conn.execute(
        "SELECT * FROM members WHERE is_active=1 AND subscription_until IS NOT NULL AND telegram_id IS NOT NULL"
    ).fetchall()
    conn.close()
    for member in members:
        m = dict(member)
        try:
            sub_until = date.fromisoformat(m["subscription_until"])
        except Exception:
            continue
        days_left = (sub_until - today).days
        for days, log_type, text in [
            (3, "3d", f"Твоя підписка закінчується {sub_until.strftime('%d.%m.%Y')} — через 3 дні!\n\nПоновити підписку:"),
            (0, "0d", "Сьогодні закінчується твоя підписка!\n\nПоновити зараз:"),
        ]:
            if days_left == days:
                conn = get_conn()
                already = conn.execute(
                    "SELECT id FROM sub_reminder_log WHERE member_id=? AND log_type=? AND sent_date=?",
                    (m["id"], log_type, today.isoformat())
                ).fetchone()
                conn.close()
                if not already:
                    try:
                        await context.bot.send_message(
                            m["telegram_id"], text,
                            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Поновити підписку", callback_data="subscribe")]])
                        )
                        conn = get_conn()
                        conn.execute("INSERT OR IGNORE INTO sub_reminder_log (member_id, log_type, sent_date) VALUES (?,?,?)",
                                     (m["id"], log_type, today.isoformat()))
                        conn.commit()
                        conn.close()
                    except Exception:
                        pass
        if days_left < 0 and days_left >= -1:
            for admin_id in ADMIN_IDS:
                try:
                    uname = m.get("username") or m["name"]
                    await context.bot.send_message(
                        admin_id,
                        f"Підписка закінчилась: {m['name']} ({uname})\n{sub_until.strftime('%d.%m.%Y')}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Видалити з чату", callback_data=f"admin_kick_{m['id']}")]])
                    )
                except Exception:
                    pass

async def _check_event_reminders(context, today: date):
    tomorrow = (today + timedelta(days=1)).isoformat()
    conn = get_conn()
    events = conn.execute("SELECT * FROM events WHERE is_active=1 AND event_date=?", (tomorrow,)).fetchall()
    conn.close()
    for ev in events:
        ev = dict(ev)
        conn = get_conn()
        regs = conn.execute("""
            SELECT m.telegram_id FROM event_registrations er JOIN members m ON er.member_id=m.id
            WHERE er.event_id=? AND (er.paid=1 OR ?=0)
        """, (ev["id"], ev["is_paid"])).fetchall()
        conn.close()
        for r in regs:
            if not r["telegram_id"]:
                continue
            try:
                await context.bot.send_message(
                    r["telegram_id"],
                    f"Нагадування! Завтра подія:\n\n{ev['title']}\n"
                    f"Час: {ev.get('event_time','')}\nМісце: {ev.get('location','не вказано')}"
                )
            except Exception:
                pass

async def _check_urgent_bdays(context: ContextTypes.DEFAULT_TYPE):
    today = date.today()
    conn = get_conn()
    members = conn.execute("SELECT * FROM members WHERE is_active=1 AND birthday IS NOT NULL").fetchall()
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
        days = (bd - today).days
        if days in [0, 1, 3] and not already_reminded(member["id"], days, today.year, "group"):
            await _check_bdays(context, today)
            break

# ─── Щотижневі задачі ─────────────────────────────────────────────────────────

async def job_monday_digest(context: ContextTypes.DEFAULT_TYPE):
    if not GROUP_CHAT_ID:
        return
    today_str = date.today().isoformat()
    next_week = (date.today() + timedelta(days=7)).isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()
    conn = get_conn()
    events = [dict(e) for e in conn.execute(
        "SELECT * FROM events WHERE is_active=1 AND event_date BETWEEN ? AND ? ORDER BY event_date",
        (today_str, next_week)
    ).fetchall()]
    new_members = [dict(r) for r in conn.execute(
        "SELECT name FROM members WHERE joined_at >= ? AND is_active=1", (week_ago,)
    ).fetchall()]
    active_members = [dict(r) for r in conn.execute("""
        SELECT m.name, SUM(a.msg_count) as msg_count FROM message_activity a
        JOIN members m ON a.member_id=m.id WHERE a.msg_date >= ?
        GROUP BY m.id ORDER BY msg_count DESC LIMIT 3
    """, (week_ago,)).fetchall()]
    conn.close()
    text = await gemini_weekly(events, new_members, active_members)
    if not text:
        lines = ["Доброго ранку, дівчата! Починаємо новий тиждень!\n"]
        if events:
            lines.append("На цьому тижні:")
            for ev in events:
                lines.append(f"  • {ev['title']} — {ev['event_date']}")
        if new_members:
            lines.append("\nНові учасниці: " + ", ".join(m["name"] for m in new_members))
        text = "\n".join(lines)
    await send_to_group(context, text)

async def job_thursday_digest(context: ContextTypes.DEFAULT_TYPE):
    today_str = date.today().isoformat()
    next_week = (date.today() + timedelta(days=7)).isoformat()
    conn = get_conn()
    events = [dict(e) for e in conn.execute(
        "SELECT * FROM events WHERE is_active=1 AND event_date BETWEEN ? AND ? ORDER BY event_date",
        (today_str, next_week)
    ).fetchall()]
    members = conn.execute(
        "SELECT * FROM members WHERE is_active=1 AND telegram_id IS NOT NULL AND (subscription_until IS NULL OR subscription_until >= ?)",
        (today_str,)
    ).fetchall()
    conn.close()
    for member in members:
        m = dict(member)
        if not m["telegram_id"]:
            continue
        buddy = None
        if m.get("city"):
            conn = get_conn()
            buddy_row = conn.execute("""
                SELECT name, username, instagram, city FROM members
                WHERE is_active=1 AND id != ? AND LOWER(city) LIKE LOWER(?) AND telegram_id IS NOT NULL
                ORDER BY RANDOM() LIMIT 1
            """, (m["id"], f"%{m['city']}%")).fetchone()
            conn.close()
            if buddy_row:
                buddy = dict(buddy_row)
        text = await gemini_personal(m, events, buddy)
        if not text:
            lines = ["Привіт! Ось що цікавого на тиждень:\n"]
            for ev in events:
                lines.append(f"  • {ev['title']} — {ev['event_date']}")
            if buddy:
                lines.append(f"\nПорекомендую познайомитись з {buddy['name']}!")
            text = "\n".join(lines) if len(lines) > 1 else "Гарного тижня!"
        try:
            await context.bot.send_message(
                m["telegram_id"], text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Переглянути події", callback_data="events")]])
            )
        except Exception:
            pass

# ─── Автовідповідь ────────────────────────────────────────────────────────────

_last_auto_reply: dict = {}

async def handle_auto_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.message
    if not msg or not msg.text or msg.chat_id != GROUP_CHAT_ID:
        return
    text = msg.text.lower()

    # Розширений список ключових слів для кожної теми
    topics = {
        "підписка": [
            "підписка", "підписку", "підписки", "підписатись", "підписатися",
            "оплатити", "оплата", "оплачу", "оплатила", "оплатити підписку",
            "скільки коштує", "скільки стоїть", "ціна", "тариф", "план",
            "поновити", "поновлення", "продовжити", "продовжити підписку",
            "як платити", "куди платити", "де платити", "wayforpay", "вейфорпей",
            "закінчилась підписка", "немає підписки", "втратила доступ",
        ],
        "бот": [
            "бот", "боту", "бота", "ботом",
            "активувати", "активація", "активувати бота",
            "як користуватись", "як користуватися", "що вміє",
            "де знайти бота", "як знайти бота", "старт", "/start",
            "команди бота", "не працює бот",
        ],
        "нова пошта": [
            "нова пошта", "нп", "відділення",
            "відправити подарунок", "надіслати подарунок",
            "адреса для подарунку", "де живе", "звідки",
        ],
        "події": [
            "подія", "події", "подію", "захід", "заходи",
            "зустріч", "зустрічі", "зустрітись", "зустрітися",
            "зареєструватись", "реєстрація", "записатись",
            "розклад", "афіша", "що планується", "що буде",
            "коли наступна", "де подивитись події",
        ],
        "день народження": [
            "день народження", "іменинниця", "іменинниці",
            "вішліст", "список бажань", "що подарувати",
            "подарунок", "збір", "скидуємось", "скинутись",
            "зібрати на подарунок",
        ],
    }

    matched = None
    for topic, keywords in topics.items():
        if any(kw in text for kw in keywords):
            matched = topic
            break

    if not matched:
        return

    logger.info(f"Автовідповідь: тема={matched}, текст={msg.text[:50]}")

    now = datetime.now()
    last = _last_auto_reply.get(matched)
    if last and (now - last).seconds < 300:
        logger.info(f"Автовідповідь пропущена — cooldown для {matched}")
        return
    _last_auto_reply[matched] = now

    reply = await gemini_reply(msg.text, topic=matched)
    logger.info(f"Gemini відповідь: {reply[:50] if reply else 'порожньо'}")
    if reply:
        try:
            await msg.reply_text(reply)
        except Exception as e:
            logger.error(f"Auto reply error: {e}")
    else:
        # Fallback якщо Gemini не відповів
        fallback = {
            "підписка": "Для оплати або поновлення підписки — напиши @vmuravska, вона допоможе!",
            "бот": "Щоб активувати бота — напиши йому /start в особисті. Якщо щось не виходить — @vmuravska допоможе!",
            "нова пошта": "Адресу для відправки знайдеш в профілі учасниці через бота. Питання — @vmuravska!",
            "події": "Всі актуальні події є в боті — натисни 'Події' в меню. Питання — @vmuravska!",
            "день народження": "Інфо про іменинницю та збір приходить особисто в бот. Питання — @vmuravska!",
        }
        text = fallback.get(matched, f"Дякуємо за питання! @vmuravska зможе допомогти 💕")
        try:
            await msg.reply_text(text)
        except Exception as e:
            logger.error(f"Fallback reply error: {e}")

# ─── AI розпізнавання подій ───────────────────────────────────────────────────

_msg_buffer: list = []
_last_ai_check: datetime = datetime.now() - timedelta(minutes=10)

# Слова що одразу тригерять — одне повідомлення достатньо
_IMMEDIATE_TRIGGERS = [
    "зустрінемось", "зустрітись", "зустрітися", "зберемось", "зібратись",
    "зібратися", "приходьте", "прийдіть", "запрошую всіх", "зустріч о ",
    "зустріч в ", "де зустрічаємось", "де збираємось", "може зустрінемось",
    "давайте зустрінемось", "пропоную зустрітись",
]

async def handle_ai_events(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global _last_ai_check
    msg = update.message
    if not msg or not msg.text or msg.chat_id != GROUP_CHAT_ID:
        return

    text_lower = msg.text.lower()
    immediate = any(kw in text_lower for kw in _IMMEDIATE_TRIGGERS)

    _msg_buffer.append(f"{msg.from_user.first_name}: {msg.text}")
    if len(_msg_buffer) > 20:
        _msg_buffer.pop(0)

    now = datetime.now()

    if not immediate:
        # Без прямого тригера — перевіряємо cooldown і буфер
        if (now - _last_ai_check).seconds < 300:
            return
        general = ["захід", "о котрій", "де зустрічаємось", "зустріч"]
        if not any(kw in " ".join(_msg_buffer).lower() for kw in general):
            return
    else:
        # Прямий тригер — ігноруємо cooldown
        logger.info(f"AI event immediate trigger: {msg.text[:50]}")

    _last_ai_check = now
    from datetime import date as _date
    today = _date.today().isoformat()
    result = await gemini_call(
        f"Сьогодні {today}. Проаналізуй повідомлення з чату жіночої спільноти.\n"
        f"Повідомлення:\n{chr(10).join(_msg_buffer)}\n\n"
        f"Якщо дівчата домовляються про конкретну зустріч або подію — поверни JSON з полями: "
        f"title (назва), date (YYYY-MM-DD, точна дата якщо відома, інакше найближча згадана), "
        f"time (ЧЧ:ХХ або null), location (місце або null), price (число або null), description.\n"
        f"Якщо це просте обговорення без конкретної дати і місця — поверни null.\n"
        f"Відповідь тільки JSON або null.", 200
    )
    if not result or result.strip().lower() == "null":
        return
    try:
        result = result.replace("```json", "").replace("```", "").strip()
        event = json.loads(result)
    except Exception:
        return
    price_str = f"{event.get('price')} грн" if event.get("price") else "безкоштовно"
    text = (
        f"Схоже дівчата домовились:\n\n"
        f"Назва: {event.get('title','')}\nДата: {event.get('date','')}\n"
        f"Час: {event.get('time','')}\nМісце: {event.get('location','')}\n"
        f"Вартість: {price_str}\n\nДодати подію?"
    )
    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                admin_id, text,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Додати", callback_data="ai_add"),
                     InlineKeyboardButton("Редагувати", callback_data="ai_edit"),
                     InlineKeyboardButton("Скасувати", callback_data="ai_skip")]
                ])
            )
            context.bot_data[f"ai_event_{admin_id}"] = event
        except Exception:
            pass
    _msg_buffer.clear()

# ─── Нова учасниця ────────────────────────────────────────────────────────────

async def handle_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if not result or (GROUP_CHAT_ID and result.chat.id != GROUP_CHAT_ID):
        return
    if result.old_chat_member.status not in ("left", "kicked", "restricted"):
        return
    if result.new_chat_member.status not in ("member", "administrator"):
        return
    user = result.new_chat_member.user
    if user.is_bot:
        return
    bot_info = await context.bot.get_me()
    uname_str = f" (@{user.username})" if user.username else ""
    try:
        kwargs = {
            "chat_id": GROUP_CHAT_ID,
            "text": (
                f"Вітаємо нову учасницю {user.first_name}{uname_str}!\n\n"
                f"Активуй бота щоб отримувати повідомлення про дні народження, події та новини."
            ),
            "reply_markup": InlineKeyboardMarkup([[
                InlineKeyboardButton("Активувати бота", url=f"https://t.me/{bot_info.username}?start=activate")
            ]])
        }
        if GROUP_THREAD_ID:
            kwargs["message_thread_id"] = GROUP_THREAD_ID
        await context.bot.send_message(**kwargs)
    except Exception as e:
        logger.error(f"Помилка привітання: {e}")

# ─── Callback handler ─────────────────────────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    member = get_member(user_id)

    if data == "menu":
        if member:
            await show_menu(query, context, member)
        return

    if data == "noop":
        return

    if data == "skip_phone":
        context.user_data.pop("waiting_for", None)
        if member:
            await show_menu(query, context, member)
        return

    if data == "subscribe":
        await query.edit_message_text(
            "Натисни кнопку щоб обрати план і оплатити.\n\nПісля оплати підписка активується автоматично.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Хочу приєднатись", url=WFP_SUB_URL)],
                [InlineKeyboardButton("Я оплатила", callback_data=f"sub_paid_{member['id']}" if member else "menu")],
                [back_btn()],
            ])
        )
        return

    if data.startswith("sub_paid_"):
        mid = int(data.split("_")[2])
        m = get_member_by_id(mid)
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"Оплата підписки:\n{m['name'] if m else mid} {m.get('username','') if m else ''}\n"
                    f"Якщо не активувалась — /renewsub {m.get('username', m['name']) if m else mid} 3"
                )
            except Exception:
                pass
        await query.edit_message_text("Дякуємо! Якщо оплата пройшла — підписку активовано автоматично.\nЯкщо ні — напиши адміну.")
        return

    if data == "subscription":
        sub = member.get("subscription_until") if member else None
        if sub and sub >= date.today().isoformat():
            days_left = (date.fromisoformat(sub) - date.today()).days
            text = f"Підписка активна до {date.fromisoformat(sub).strftime('%d.%m.%Y')}\nЗалишилось: {days_left} дн."
        else:
            text = "Підписка не активна."
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Поновити / Оформити", callback_data="subscribe")],
            [back_btn(), menu_btn()],
        ]))
        return

    if not member:
        await query.edit_message_text("Напиши /start щоб зареєструватись.")
        return

    if data == "profile":
        bd_str = "не вказано"
        if member.get("birthday"):
            parts = member["birthday"].split("-")
            mo, d = int(parts[-2]), int(parts[-1])
            yr = f".{member['birth_year']}" if member.get("birth_year") else ""
            bd_str = f"{d:02d}.{mo:02d}{yr}"
        insta_str = member.get("instagram") or "не вказано"
        if member.get("instagram"):
            insta_str = f"{member['instagram']} ({instagram_link(member['instagram'])})"
        sub_str = "не активна"
        if member.get("subscription_until"):
            sub_str = f"до {date.fromisoformat(member['subscription_until']).strftime('%d.%m.%Y')}"
        await query.edit_message_text(
            f"Твоя анкета:\n\nІм'я: {member['name']}\nДН: {bd_str}\n"
            f"Місто: {member.get('city') or 'не вказано'}\nТелефон: {member.get('phone') or 'не вказано'}\n"
            f"НП: {member.get('nova_poshta') or 'не вказано'}\nInstagram: {insta_str}\n"
            f"Колір: {member.get('favorite_color') or 'не вказано'}\nПідписка: {sub_str}",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Редагувати", callback_data="edit_profile")],
                [InlineKeyboardButton("Вішліст", callback_data="wishlist")],
                [back_btn(), menu_btn()],
            ])
        )

    elif data == "edit_profile":
        context.user_data["onboarding_step"] = "birthday"
        await query.edit_message_text(QUESTIONS["birthday"])

    elif data == "wishlist":
        wl = member.get("wishlist")
        text = f"Твій вішліст:\n\n{wl}" if wl else "Ще немає вішлісту.\n\nРекомендуємо: https://goodsend.it/intro/1"
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Оновити вішліст", callback_data="edit_wishlist")],
            [back_btn("Профіль", "profile"), menu_btn()],
        ]))

    elif data == "edit_wishlist":
        context.user_data["waiting_for"] = "wishlist"
        await query.edit_message_text("Введи посилання або опис вішлісту:")

    elif data == "search":
        context.user_data["waiting_for"] = "search"
        await query.edit_message_text(
            "Напиши @username або номер телефону:",
            reply_markup=InlineKeyboardMarkup([[back_btn(), menu_btn()]])
        )

    elif data == "bday_status":
        conn = get_conn()
        rows = conn.execute("""
            SELECT e.id as event_id, e.birthday_person_name, e.event_date, p.amount, p.paid
            FROM payments p JOIN birthday_events e ON p.event_id=e.id
            WHERE p.member_id=? ORDER BY e.id DESC LIMIT 10
        """, (member["id"],)).fetchall()
        conn.close()
        if not rows:
            await query.edit_message_text("Поки що подій не було.", reply_markup=InlineKeyboardMarkup([[menu_btn()]]))
            return
        buttons = []
        lines = ["Твої оплати на ДН:\n"]
        for r in rows:
            if r["paid"]:
                lines.append(f"✅ ДН {r['birthday_person_name']} ({r['event_date']}) — {r['amount']:.0f} грн")
            else:
                lines.append(f"❌ ДН {r['birthday_person_name']} ({r['event_date']}) — {r['amount']:.0f} грн")
                buttons.append([InlineKeyboardButton(f"Оплатила за {r['birthday_person_name']}", callback_data=f"bday_paid_{r['event_id']}")])
        buttons.append([menu_btn()])
        await query.edit_message_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(buttons))

    elif data.startswith("bday_paid_"):
        event_id = int(data.split("_")[2])
        conn = get_conn()
        conn.execute("UPDATE payments SET paid=1, paid_at=? WHERE event_id=? AND member_id=?",
                     (datetime.now().isoformat(), event_id, member["id"]))
        ev = conn.execute("SELECT birthday_person_name FROM birthday_events WHERE id=?", (event_id,)).fetchone()
        conn.commit()
        conn.close()
        bd_name = ev["birthday_person_name"] if ev else ""
        await query.edit_message_text(f"Дякуємо за внесок! Оплату на ДН {bd_name} зафіксовано.")
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(admin_id, f"💰 {query.from_user.full_name} відмітила оплату на ДН {bd_name}")
            except Exception:
                pass

    elif data == "events":
        conn = get_conn()
        events = conn.execute(
            "SELECT * FROM events WHERE is_active=1 AND event_date >= ? ORDER BY event_date LIMIT 10",
            (date.today().isoformat(),)
        ).fetchall()
        recurring = conn.execute(
            "SELECT * FROM recurring_events WHERE is_active=1 AND active_until >= ? ORDER BY weekday",
            (date.today().isoformat(),)
        ).fetchall()
        conn.close()
        if not events and not recurring:
            await query.edit_message_text("Найближчих подій немає.", reply_markup=InlineKeyboardMarkup([[menu_btn()]]))
            return
        buttons = []
        for ev in events:
            icon = "💰" if ev["is_paid"] else "🆓"
            buttons.append([InlineKeyboardButton(f"{icon} {ev['title']} — {ev['event_date']}", callback_data=f"event_{ev['id']}")])
        for rev in recurring:
            rev = dict(rev)
            day_name = WEEKDAYS_NAME[rev["weekday"]]
            buttons.append([InlineKeyboardButton(f"🔄 {rev['title']} — щo{day_name} о {rev['event_time']}", callback_data=f"rec_view_{rev['id']}")])
        buttons.append([menu_btn()])
        await query.edit_message_text("Майбутні події:", reply_markup=InlineKeyboardMarkup(buttons))

    elif data.startswith("rec_view_"):
        ev_id = int(data.split("_")[2])
        conn = get_conn()
        ev = conn.execute("SELECT * FROM recurring_events WHERE id=?", (ev_id,)).fetchone()
        conn.close()
        if not ev:
            return
        ev = dict(ev)
        day_name = WEEKDAYS_NAME[ev["weekday"]]
        next_date = get_next_weekday(ev["weekday"])
        location_str = f"\n🔗 {ev['location']}" if ev.get("location") else ""
        text = (
            f"🔄 {ev['title']}\n\n"
            f"Повторення: щo{day_name} о {ev['event_time']}\n"
            f"Наступна зустріч: {next_date.strftime('%d.%m.%Y')}"
            f"{location_str}"
        )
        buttons = [[InlineKeyboardButton("Зареєструватись", callback_data=f"rec_join_{ev_id}")]]
        buttons.append([back_btn("Події", "events"), menu_btn()])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    elif data.startswith("event_") and not any(x in data for x in ["_reg_", "_pay_"]):
        try:
            event_id = int(data.split("_")[1])
        except Exception:
            return
        conn = get_conn()
        ev = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        reg = conn.execute("SELECT * FROM event_registrations WHERE event_id=? AND member_id=?",
                           (event_id, member["id"])).fetchone()
        conn.close()
        if not ev:
            return
        ev = dict(ev)
        paid_str = f"Вартість: {ev['price']} грн" if ev["is_paid"] else "Безкоштовно"
        spots_str = f"Вільних місць: {ev['spots_left']}" if ev["max_spots"] > 0 else ""
        text = f"{ev['title']}\n\nДата: {ev['event_date']} {ev.get('event_time','')}\nМісце: {ev.get('location','')}\n{paid_str}\n{spots_str}\n\n{ev.get('description','')}"
        buttons = []
        if reg:
            if ev["is_paid"] and not reg["paid"]:
                buttons.append([InlineKeyboardButton("Оплатити", callback_data=f"event_pay_{event_id}")])
            buttons.append([InlineKeyboardButton("✅ Зареєстрована", callback_data="noop")])
        elif ev["max_spots"] == 0 or ev["spots_left"] > 0:
            buttons.append([InlineKeyboardButton("Зареєструватись", callback_data=f"event_reg_{event_id}")])
        buttons.append([back_btn("Події", "events"), menu_btn()])
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))

    elif data.startswith("event_reg_"):
        event_id = int(data.split("_")[2])
        conn = get_conn()
        ev = dict(conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone())
        conn.execute("INSERT OR IGNORE INTO event_registrations (event_id, member_id) VALUES (?,?)", (event_id, member["id"]))
        if ev["max_spots"] > 0:
            conn.execute("UPDATE events SET spots_left=MAX(0, spots_left-1) WHERE id=?", (event_id,))
        conn.commit()
        conn.close()
        if ev["is_paid"]:
            await query.edit_message_text(
                f"Зареєстрована на {ev['title']}!\nОплати участь:",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Оплатити", callback_data=f"event_pay_{event_id}")], [menu_btn()]])
            )
        else:
            await query.edit_message_text(f"Зареєстрована! Чекаємо тебе на {ev['title']}!", reply_markup=InlineKeyboardMarkup([[menu_btn()]]))

    elif data.startswith("event_pay_"):
        event_id = int(data.split("_")[2])
        conn = get_conn()
        ev = dict(conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone())
        conn.close()
        pay_url = ev.get("wfp_link") or JAR_LINK
        await query.edit_message_text(
            f"Оплата: {ev['title']}\nСума: {ev['price']} грн\n\n{pay_url}",
            reply_markup=InlineKeyboardMarkup([[back_btn("Подія", f"event_{event_id}"), menu_btn()]])
        )

    elif data == "ai_add":
        event = context.bot_data.pop(f"ai_event_{user_id}", None)
        if event:
            conn = get_conn()
            conn.execute(
                "INSERT INTO events (title, description, location, event_date, event_time, is_paid, price, max_spots, spots_left) VALUES (?,?,?,?,?,?,?,0,0)",
                (event.get("title","Зустріч"), event.get("description",""), event.get("location",""),
                 event.get("date", date.today().isoformat()), event.get("time",""),
                 1 if event.get("price") else 0, event.get("price") or 0)
            )
            conn.commit()
            conn.close()
        await query.edit_message_text("Подію додано!")

    elif data == "ai_edit":
        event = context.bot_data.get(f"ai_event_{user_id}", {})
        context.user_data["new_event"] = {
            "title": event.get("title", ""),
            "event_date": event.get("date", date.today().isoformat()),
            "event_time": event.get("time", ""),
            "location": event.get("location", ""),
            "description": event.get("description", ""),
            "price": event.get("price") or 0,
            "is_paid": 1 if event.get("price") else 0,
            "max_spots": 0, "spots_left": 0, "wfp_link": "",
        }
        context.user_data["waiting_for"] = "admin_event_title"
        title = event.get("title", "")
        await query.edit_message_text(
            f"Редагуємо подію. Поточна назва: {title}\n\nВведи нову назву (або - щоб залишити):"
        )

    elif data == "ai_skip":
        context.bot_data.pop(f"ai_event_{user_id}", None)
        await query.edit_message_text("Скасовано")


    elif data.startswith("rec_add_"):
        # rec_add_{weekday}_{time}_{title}
        parts = data.split("_", 4)
        weekday = int(parts[2])
        time_str = parts[3]
        title = parts[4] if len(parts) > 4 else "Подія"
        event_data = context.bot_data.get(f"ai_event_{user_id}", {})
        location = event_data.get("location") or ""
        from datetime import date as _d
        active_until = (_d.today() + timedelta(days=30)).isoformat()
        conn = get_conn()
        conn.execute(
            "INSERT INTO recurring_events (title, location, weekday, event_time, active_until) VALUES (?,?,?,?,?)",
            (title, location, weekday, time_str, active_until)
        )
        conn.commit()
        conn.close()
        weekday_name = WEEKDAYS_NAME[weekday]
        await query.edit_message_text(
            f"✅ Регулярна подія додана!\n\n"
            f"«{title}» — що{weekday_name} о {time_str}\n"
            f"Активна до: {date.fromisoformat(active_until).strftime('%d.%m.%Y')}"
        )

    elif data.startswith("rec_extend_"):
        ev_id = int(data.split("_")[2])
        conn = get_conn()
        ev = conn.execute("SELECT * FROM recurring_events WHERE id=?", (ev_id,)).fetchone()
        if ev:
            from datetime import date as _d
            new_until = (_d.today() + timedelta(days=30)).isoformat()
            conn.execute("UPDATE recurring_events SET active_until=?, is_active=1 WHERE id=?", (new_until, ev_id))
            conn.commit()
            await query.edit_message_text(
                f"✅ Подію «{dict(ev)['title']}» продовжено до {date.fromisoformat(new_until).strftime('%d.%m.%Y')}"
            )
        conn.close()

    elif data.startswith("rec_stop_"):
        ev_id = int(data.split("_")[2])
        conn = get_conn()
        ev = conn.execute("SELECT * FROM recurring_events WHERE id=?", (ev_id,)).fetchone()
        if ev:
            conn.execute("UPDATE recurring_events SET is_active=0 WHERE id=?", (ev_id,))
            conn.commit()
            await query.edit_message_text(f"🛑 Подію «{dict(ev)['title']}» завершено.")
        conn.close()

    elif data == "rec_skip":
        await query.edit_message_text("Скасовано.")

    elif data == "rec_edit":
        await query.edit_message_text(
            "Щоб відредагувати подію перед додаванням — напиши мені деталі:\n"
            "Назва, день тижня, час, місце/посилання"
        )

    elif data.startswith("rec_join_"):
        ev_id = int(data.split("_")[2])
        member = get_member(user_id)
        if not member:
            await query.answer("Спочатку зареєструйся через /start")
            return
        from datetime import date as _d
        week_start = get_week_start(_d.today())
        conn = get_conn()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO recurring_registrations (event_id, member_id, week_start) VALUES (?,?,?)",
                (ev_id, member["id"], week_start)
            )
            conn.commit()
            await query.answer("✅ Зареєстрована!")
            ev = conn.execute("SELECT * FROM recurring_events WHERE id=?", (ev_id,)).fetchone()
            if ev:
                ev = dict(ev)
                location_text = f"\n🔗 {ev['location']}" if ev.get("location") else ""
                await query.edit_message_text(
                    f"✅ Зареєстрована на «{ev['title']}»!\n"
                    f"Час: {ev['event_time']}{location_text}\n\n"
                    f"За годину до початку отримаєш нагадування 🌸"
                )
        except Exception as e:
            logger.error(f"rec_join error: {e}")
            await query.answer("Вже зареєстрована!")
        conn.close()

    elif data == "admin_panel":
        await query.edit_message_text(
            "Адмін-панель:\n\n"
            "/members — список учасниць\n"
            "/birthdays — дні народження\n"
            "/eventstatus — статус збору ДН\n"
            "/remind — нагадати боржницям\n"
            "/forcebday Ім'я — запустити збір\n"
            "/setbirthday Ім'я ДД.ММ\n"
            "/setusername Ім'я @нік\n"
            "/setsub @нік РРРР-ММ-ДД\n"
            "/renewsub @нік 3\n"
            "/subexpiring — закінчуються за 7 днів\n"
            "/subexpired — прострочені\n"
            "/importsubs — імпорт підписок\n"
            "/bycity Місто\n"
            "/addevent — додати подію\n"
        "/editevent — редагувати подію\n"
            "/testcheck — тест\n"
            "/clearlog — очистити журнал",
            reply_markup=InlineKeyboardMarkup([[back_btn(), menu_btn()]])
        )

    elif data.startswith("admin_edit_event_"):
        event_id = int(data.split("_")[3])
        conn = get_conn()
        ev = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        conn.close()
        if not ev:
            await query.edit_message_text("Подію не знайдено")
            return
        ev = dict(ev)
        paid_str = f"Платна — {ev['price']} грн" if ev["is_paid"] else "Безкоштовна"
        ev_title = ev['title']
        ev_date = ev['event_date']
        ev_time = ev.get('event_time', '')
        ev_loc = ev.get('location', '')
        await query.edit_message_text(
            f"Подія: {ev_title}\nДата: {ev_date} {ev_time}\nМісце: {ev_loc}\n{paid_str}\n\nЩо редагуємо?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Назва", callback_data=f"aef_title_{event_id}"),
                 InlineKeyboardButton("Дата", callback_data=f"aef_date_{event_id}")],
                [InlineKeyboardButton("Час", callback_data=f"aef_time_{event_id}"),
                 InlineKeyboardButton("Місце", callback_data=f"aef_location_{event_id}")],
                [InlineKeyboardButton("Опис", callback_data=f"aef_description_{event_id}"),
                 InlineKeyboardButton("Ціна", callback_data=f"aef_price_{event_id}")],
                [InlineKeyboardButton("Деактивувати подію", callback_data=f"aef_deactivate_{event_id}")],
                [back_btn("Назад", "menu")],
            ])
        )

    elif data.startswith("aef_"):
        parts = data.split("_")
        field = parts[1]
        event_id = int(parts[2])
        if field == "deactivate":
            conn = get_conn()
            conn.execute("UPDATE events SET is_active=0 WHERE id=?", (event_id,))
            conn.commit()
            conn.close()
            await query.edit_message_text("Подію деактивовано")
            return
        field_names = {
            "title": "назву", "date": "дату (РРРР-ММ-ДД)",
            "time": "час (ЧЧ:ХХ)", "location": "місце",
            "description": "опис", "price": "ціну (грн, або 0 для безкоштовної)",
        }
        context.user_data["waiting_for"] = f"admin_edit_field_{field}_{event_id}"
        await query.edit_message_text(
            f"Введи нову {field_names.get(field, field)}:"
        )

    elif data.startswith("admin_kick_"):
        mid = int(data.split("_")[2])
        m = get_member_by_id(mid)
        if m and m.get("telegram_id") and GROUP_CHAT_ID:
            try:
                await context.bot.ban_chat_member(GROUP_CHAT_ID, m["telegram_id"])
                await context.bot.unban_chat_member(GROUP_CHAT_ID, m["telegram_id"])
                conn = get_conn()
                conn.execute("UPDATE members SET is_active=0 WHERE id=?", (mid,))
                conn.commit()
                conn.close()
                await query.edit_message_text(f"{m['name']} видалена з чату")
            except Exception as e:
                await query.edit_message_text(f"Помилка: {e}")

# ─── handle_text ─────────────────────────────────────────────────────────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    if not message:
        return

    # Пересилання з каналу
    if update.channel_post and FORWARD_CHANNEL_ID and message.chat.id == FORWARD_CHANNEL_ID and GROUP_CHAT_ID:
        try:
            kwargs = {"chat_id": GROUP_CHAT_ID, "from_chat_id": FORWARD_CHANNEL_ID, "message_id": message.message_id}
            if GROUP_THREAD_ID:
                kwargs["message_thread_id"] = GROUP_THREAD_ID
            await context.bot.forward_message(**kwargs)
        except Exception as e:
            logger.error(f"Помилка пересилання: {e}")
        return

    # Група
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        msg = update.message
        if not msg:
            return
        logger.info(f"Група повідомлення: chat_id={msg.chat_id}, GROUP_CHAT_ID={GROUP_CHAT_ID}, text={msg.text[:30] if msg.text else None}")
        if msg.chat_id == GROUP_CHAT_ID and msg.from_user:
            track_activity(msg.from_user.id)
        # Парсинг дат з гілки анкет
        is_bday_thread = BIRTHDAY_THREAD_ID is None or msg.message_thread_id == BIRTHDAY_THREAD_ID
        if (GROUP_CHAT_ID == 0 or msg.chat_id == GROUP_CHAT_ID) and is_bday_thread and msg.from_user:
            text = msg.text or ""
            result = parse_birthday(text)
            if result:
                day, month = result
                user = msg.from_user
                conn = get_conn()
                conn.execute("INSERT OR IGNORE INTO members (telegram_id, name) VALUES (?,?)", (user.id, user.full_name))
                conn.execute("UPDATE members SET birthday=?, name=? WHERE telegram_id=?",
                             (f"{month:02d}-{day:02d}", user.full_name, user.id))
                year = parse_birth_year(text)
                if year:
                    conn.execute("UPDATE members SET birth_year=? WHERE telegram_id=?", (year, user.id))
                conn.commit()
                conn.close()
        if GEMINI_API_KEY and msg.chat_id == GROUP_CHAT_ID:
            await handle_auto_reply(update, context)
            await handle_ai_events(update, context)
        return

    # Приватні
    user_id = update.effective_user.id
    member = get_member(user_id)
    text = message.text.strip() if message.text else ""

    # Оновлення телефону
    if context.user_data.get("waiting_for") == "phone_update":
        context.user_data.pop("waiting_for", None)
        if text != "-":
            conn = get_conn()
            conn.execute("UPDATE members SET phone=? WHERE telegram_id=?", (text, user_id))
            conn.commit()
            conn.close()
            await message.reply_text("Телефон збережено!")
        member = get_member(user_id)
        await show_menu(update, context, member)
        return

    # Онбординг
    if context.user_data.get("onboarding_step"):
        await handle_onboarding(update, context, context.user_data["onboarding_step"], text)
        return

    # Постійна кнопка
    if text == "Головне меню":
        if member and has_active_sub(member):
            await show_menu(update, context, member)
        else:
            await cmd_start(update, context)
        return

    waiting = context.user_data.get("waiting_for")

    if waiting == "search":
        context.user_data.pop("waiting_for", None)
        search = text.lstrip("@")
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM members WHERE LOWER(username)=LOWER(?) OR LOWER(username)=LOWER(?) OR phone=? LIMIT 1",
            (f"@{search}", search, text)
        ).fetchone()
        conn.close()
        if not row:
            await message.reply_text("Учасницю не знайдено.", reply_markup=InlineKeyboardMarkup([[menu_btn()]]))
            return
        m = dict(row)
        insta_str = f"Instagram: {instagram_link(m['instagram'])}\n" if m.get("instagram") else ""
        wl_str = f"Вішліст: {m['wishlist']}\n" if m.get("wishlist") else ""
        await message.reply_text(
            f"Учасниця: {m['name']}\nМісто: {m.get('city') or 'не вказано'}\n"
            f"Телефон: {m.get('phone') or 'не вказано'}\nНП: {m.get('nova_poshta') or 'не вказано'}\n"
            f"{insta_str}Колір: {m.get('favorite_color') or 'не вказано'}\n{wl_str}",
            reply_markup=InlineKeyboardMarkup([[back_btn("Пошук", "search"), menu_btn()]])
        )

    elif waiting == "wishlist":
        context.user_data.pop("waiting_for", None)
        conn = get_conn()
        conn.execute("UPDATE members SET wishlist=? WHERE telegram_id=?", (text, user_id))
        conn.commit()
        conn.close()
        await message.reply_text("Вішліст оновлено!", reply_markup=InlineKeyboardMarkup([[menu_btn()]]))

    elif waiting and waiting.startswith("admin_"):
        await handle_admin_input(update, context, waiting, text)

    elif member:
        if has_active_sub(member) and member.get("onboarding_done"):
            await show_menu(update, context, member)
        elif has_active_sub(member):
            await start_onboarding(update, context)
        else:
            await show_welcome(update, context)

async def show_welcome(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Привіт! Вітаємо в боті Комуни Жіноцтва!\n\n"
        "Ми — спільнота жінок, що підтримують одна одну, діляться досвідом і разом ростуть.\n\n"
        "Щоб долучитись — оформи підписку:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Instagram комуни", url=INSTAGRAM_COMMUNITY),
             InlineKeyboardButton("Instagram засновниці", url=INSTAGRAM_FOUNDER)],
            [InlineKeyboardButton("Хочу приєднатись", callback_data="subscribe")],
        ])
    )

# ─── Адмін команди ────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    user = update.effective_user
    uname = user.username
    conn = get_conn()
    pre = None
    if uname:
        pre = conn.execute(
            "SELECT id FROM members WHERE LOWER(username)=LOWER(?) AND telegram_id IS NULL",
            (f"@{uname}",)
        ).fetchone()
    if pre:
        conn.execute("UPDATE members SET telegram_id=?, name=? WHERE id=?", (user.id, user.full_name, pre["id"]))
    else:
        conn.execute("INSERT OR IGNORE INTO members (telegram_id, name) VALUES (?,?)", (user.id, user.full_name))
        conn.execute("UPDATE members SET name=? WHERE telegram_id=?", (user.full_name, user.id))
    if uname:
        conn.execute("UPDATE members SET username=? WHERE telegram_id=?", (f"@{uname}", user.id))
    member_row = conn.execute("SELECT id FROM members WHERE telegram_id=?", (user.id,)).fetchone()
    if member_row:
        cutoff = (date.today() - timedelta(days=14)).isoformat()
        events = conn.execute("SELECT id, amount_per_person FROM birthday_events WHERE event_date >= ?", (cutoff,)).fetchall()
        for ev in events:
            conn.execute("INSERT OR IGNORE INTO payments (event_id, member_id, amount) VALUES (?,?,?)",
                         (ev["id"], member_row["id"], ev["amount_per_person"]))
    conn.commit()
    conn.close()
    member = get_member(user.id)
    if not has_active_sub(member):
        await show_welcome(update, context)
        return
    if not member.get("onboarding_done"):
        await start_onboarding(update, context)
        return
    if not member.get("phone") and not context.user_data.get("phone_asked"):
        context.user_data["phone_asked"] = True
        context.user_data["waiting_for"] = "phone_update"
        await update.message.reply_text(
            "Привіт! Ми оновили бота.\n\nВкажи свій номер телефону для автоматичної активації підписки.\n\n"
            "Наприклад: +380991234567\n_(або «-» щоб пропустити)_",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Пропустити", callback_data="skip_phone")]])
        )
        return
    await show_menu(update, context, member)
    await _check_urgent_bdays(context)

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text(
        "Адмін-панель:\n\n"
        "/members — список\n/birthdays — дні народження\n/eventstatus — статус збору\n"
        "/remind — нагадати боржницям\n/forcebday Ім'я — запустити збір\n"
        "/setbirthday Ім'я ДД.ММ\n/setusername Ім'я @нік\n"
        "/setsub @нік РРРР-ММ-ДД\n/renewsub @нік 3\n"
        "/subexpiring — закінчуються за 7 днів\n/subexpired — прострочені\n"
        "/importsubs — імпорт\n/bycity Місто\n/addevent — додати подію\n"
        "/testcheck — тест\n/clearlog — очистити журнал"
    )

async def cmd_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    active = get_active_members()
    conn = get_conn()
    inactive_n = conn.execute("SELECT COUNT(*) as n FROM members WHERE is_active=0").fetchone()["n"]
    conn.close()
    lines = [f"Активних: {len(active)} | Неактивних: {inactive_n}\n"]
    for m in active:
        uname = f" {m['username']}" if m.get("username") else ""
        lines.append(f"• {m['name']}{uname}")
    await update.message.reply_text("\n".join(lines))

async def cmd_birthdays(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    rows = conn.execute("SELECT name, birthday FROM members WHERE is_active=1 AND birthday IS NOT NULL").fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("Дат народження немає")
        return
    today = date.today()
    by_month = {}
    for r in rows:
        mo, d = map(int, r["birthday"].split("-"))
        by_month.setdefault(mo, []).append((d, r["name"]))
    lines = ["Дні народження:\n"]
    for mo in sorted(by_month.keys()):
        lines.append(f"{MONTH_NAMES_UA[mo]}:")
        for d, name in sorted(by_month[mo]):
            try:
                bd = date(today.year, mo, d)
                if bd < today:
                    bd = date(today.year + 1, mo, d)
                tag = f" (через {(bd-today).days} дн.)" if (bd-today).days <= 7 else ""
            except ValueError:
                tag = ""
            lines.append(f"  {d:02d}.{mo:02d} — {name}{tag}")
    await update.message.reply_text("\n".join(lines))

async def cmd_event_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    event = conn.execute("SELECT * FROM birthday_events ORDER BY id DESC LIMIT 1").fetchone()
    if not event:
        await update.message.reply_text("Подій не було")
        conn.close()
        return
    rows = conn.execute("""
        SELECT m.name, p.paid FROM payments p JOIN members m ON p.member_id=m.id
        WHERE p.event_id=? ORDER BY p.paid DESC, m.name
    """, (event["id"],)).fetchall()
    conn.close()
    paid = [r for r in rows if r["paid"]]
    unpaid = [r for r in rows if not r["paid"]]
    lines = [
        f"ДН {event['birthday_person_name']} | {event['event_date']}",
        f"{event['amount_per_person']:.0f} грн | {event['total_members']} учасниць\n",
        f"Оплатили ({len(paid)}):"
    ]
    for r in paid:
        lines.append(f"  ✅ {r['name']}")
    lines.append(f"\nНе здали ({len(unpaid)}):")
    for r in unpaid:
        lines.append(f"  • {r['name']}")
    await update.message.reply_text("\n".join(lines))

async def cmd_remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    event = conn.execute("SELECT * FROM birthday_events ORDER BY id DESC LIMIT 1").fetchone()
    if not event:
        await update.message.reply_text("Активних подій немає")
        conn.close()
        return
    event = dict(event)
    unpaid = conn.execute("""
        SELECT m.telegram_id, m.name FROM payments p JOIN members m ON p.member_id=m.id
        WHERE p.event_id=? AND p.paid=0
    """, (event["id"],)).fetchall()
    conn.close()
    if not unpaid:
        await update.message.reply_text("Всі оплатили!")
        return
    bd_member = get_member_by_id(event["birthday_person_id"]) if event.get("birthday_person_id") else None
    m_dict = bd_member if bd_member else {"name": event["birthday_person_name"], "id": None, "username": None, "nova_poshta": None, "instagram": None, "favorite_color": None, "wishlist": None}
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Я оплатила!", callback_data=f"bday_paid_{event['id']}")]])
    sent = 0
    for u in unpaid:
        if not u["telegram_id"]:
            continue
        try:
            await context.bot.send_message(u["telegram_id"], bday_personal_text(m_dict, int(event["amount_per_person"])), reply_markup=kb)
            sent += 1
        except Exception:
            pass
    conn = get_conn()
    paid_count = conn.execute("SELECT COUNT(*) as n FROM payments WHERE event_id=? AND paid=1", (event["id"],)).fetchone()["n"]
    conn.close()
    total = event["total_members"]
    percent = round(paid_count / total * 100) if total else 0
    bd_date = date.fromisoformat(event["event_date"])
    days_left = max((bd_date - date.today()).days, 0)
    uname = f" ({m_dict['username']})" if m_dict.get("username") else ""
    await send_to_group(context, (
        f"Нагадування — через {days_left} дн. день народження!\n\n"
        f"{event['birthday_person_name']}{uname}\n\nВже здали: {percent}%\n\n"
        f"Хто не встиг — перевірте особисті повідомлення"
    ))
    names = "\n".join(f"  • {u['name']}" for u in unpaid)
    await update.message.reply_text(f"Надіслано: {sent}\n\nНе здали ({len(unpaid)}):\n{names}")

async def cmd_force_bday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not context.args:
        await update.message.reply_text("Формат: /forcebday Ім'я")
        return
    name = " ".join(context.args)
    conn = get_conn()
    member = conn.execute("SELECT * FROM members WHERE LOWER(name) LIKE LOWER(?)", (f"%{name}%",)).fetchone()
    conn.close()
    if not member:
        await update.message.reply_text(f"Не знайдено: {name}")
        return
    m = dict(member)
    if not m.get("birthday"):
        await update.message.reply_text(f"У {m['name']} немає ДН")
        return
    parts = m["birthday"].split("-")
    month, day = int(parts[-2]), int(parts[-1])
    today = date.today()
    bd = date(today.year, month, day)
    if bd < today:
        bd = date(today.year + 1, month, day)
    days_until = (bd - today).days
    conn = get_conn()
    already = conn.execute(
        "SELECT id FROM reminder_log WHERE member_id=? AND year=? AND log_type='force'",
        (m["id"], today.year)
    ).fetchone()
    conn.close()
    if already:
        await update.message.reply_text(f"Вже надсилалось. /clearlog → /forcebday {name}")
        return
    active = get_active_members()
    payers = [x for x in active if x["id"] != m["id"]]
    amount = AMOUNT_PER_PERSON
    conn = get_conn()
    event_id = await _create_bday_event(conn, m, bd, payers)
    conn.execute("INSERT OR IGNORE INTO reminder_log (member_id, days_before, year, log_type) VALUES (?,?,?,?)",
                 (m["id"], days_until, today.year, "force"))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"Знайдено: {m['name']}, ДН {bd}, через {days_until} дн.\nНадсилаю...")
    await send_to_group(context, bday_group_text(m, bd, days_until, amount))
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("Я оплатила!", callback_data=f"bday_paid_{event_id}")]])
    sent = 0
    for p in payers:
        if not p.get("telegram_id"):
            continue
        try:
            await context.bot.send_message(p["telegram_id"], bday_personal_text(m, amount), reply_markup=kb)
            sent += 1
        except Exception:
            pass
    await update.message.reply_text(f"Готово! В групу: ok\nОсобистих: {sent}")

async def cmd_set_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Формат: /setbirthday Ім'я ДД.ММ")
        return
    name = context.args[0]
    date_str = " ".join(context.args[1:])
    result = parse_birthday(date_str)
    if not result:
        await update.message.reply_text(f"Не розпізнала дату: {date_str}")
        return
    day, month = result
    year = parse_birth_year(date_str)
    bd = f"{month:02d}-{day:02d}"
    conn = get_conn()
    existing = conn.execute("SELECT id FROM members WHERE LOWER(name) LIKE LOWER(?)", (f"%{name}%",)).fetchone()
    if existing:
        conn.execute("UPDATE members SET birthday=? WHERE id=?", (bd, existing["id"]))
        if year:
            conn.execute("UPDATE members SET birth_year=? WHERE id=?", (year, existing["id"]))
        msg = f"ДН оновлено: {name} — {day:02d}.{month:02d}"
    else:
        conn.execute("INSERT INTO members (name, birthday) VALUES (?,?)", (name, bd))
        msg = f"Додано: {name} з ДН {day:02d}.{month:02d}"
    conn.commit()
    conn.close()
    await update.message.reply_text(msg)

async def cmd_set_username(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Формат: /setusername Ім'я @нік")
        return
    name = context.args[0]
    uname = context.args[1] if context.args[1].startswith("@") else "@" + context.args[1]
    conn = get_conn()
    existing = conn.execute("SELECT id FROM members WHERE LOWER(name) LIKE LOWER(?)", (f"%{name}%",)).fetchone()
    if not existing:
        await update.message.reply_text(f"Не знайдено: {name}")
        conn.close()
        return
    conn.execute("UPDATE members SET username=? WHERE id=?", (uname, existing["id"]))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"Username {uname} встановлено для {name}")

async def cmd_set_sub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Формат: /setsub @нік РРРР-ММ-ДД")
        return
    identifier = context.args[0].lstrip("@")
    until_str = context.args[1]
    try:
        date.fromisoformat(until_str)
    except ValueError:
        await update.message.reply_text("Формат дати: РРРР-ММ-ДД")
        return
    conn = get_conn()
    result = conn.execute("""
        UPDATE members SET subscription_until=? WHERE
        LOWER(username)=LOWER(?) OR LOWER(username)=LOWER(?) OR LOWER(name) LIKE LOWER(?)
    """, (until_str, f"@{identifier}", identifier, f"%{identifier}%"))
    if result.rowcount:
        conn.commit()
        conn.close()
        await update.message.reply_text(f"Підписку до {until_str} встановлено")
    else:
        conn.execute("INSERT INTO members (name, username, subscription_until) VALUES (?,?,?)",
                     (identifier, f"@{identifier}", until_str))
        conn.commit()
        conn.close()
        await update.message.reply_text(f"Створено @{identifier} з підпискою до {until_str}")

async def cmd_renew_sub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Формат: /renewsub @нік 3")
        return
    identifier = context.args[0].lstrip("@")
    months = int(context.args[1])
    conn = get_conn()
    row = conn.execute("""
        SELECT id, subscription_until FROM members WHERE
        LOWER(username)=LOWER(?) OR LOWER(username)=LOWER(?) OR LOWER(name) LIKE LOWER(?) LIMIT 1
    """, (f"@{identifier}", identifier, f"%{identifier}%")).fetchone()
    if not row:
        await update.message.reply_text("Не знайдено")
        conn.close()
        return
    current = date.fromisoformat(row["subscription_until"]) if row["subscription_until"] else date.today()
    if current < date.today():
        current = date.today()
    new_until = (current + timedelta(days=30 * months)).isoformat()
    conn.execute("UPDATE members SET subscription_until=? WHERE id=?", (new_until, row["id"]))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"Підписку поновлено до {new_until}")

async def cmd_sub_expiring(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    in_7 = (date.today() + timedelta(days=7)).isoformat()
    today_str = date.today().isoformat()
    conn = get_conn()
    rows = conn.execute("""
        SELECT name, username, subscription_until FROM members
        WHERE is_active=1 AND subscription_until BETWEEN ? AND ? ORDER BY subscription_until
    """, (today_str, in_7)).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("Немає підписок що закінчуються за 7 днів")
        return
    lines = [f"Закінчуються за 7 днів ({len(rows)}):\n"]
    for r in rows:
        uname = f" {r['username']}" if r["username"] else ""
        lines.append(f"• {r['name']}{uname} — до {r['subscription_until']}")
    await update.message.reply_text("\n".join(lines))

async def cmd_sub_expired(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    today_str = date.today().isoformat()
    conn = get_conn()
    expired = conn.execute("""
        SELECT name, username, subscription_until FROM members
        WHERE is_active=1 AND subscription_until IS NOT NULL AND subscription_until < ? ORDER BY subscription_until
    """, (today_str,)).fetchall()
    no_sub = conn.execute("""
        SELECT name, username FROM members
        WHERE is_active=1 AND (subscription_until IS NULL OR subscription_until = '') ORDER BY name
    """).fetchall()
    conn.close()
    lines = ["Підписки що потребують уваги:\n"]
    if expired:
        lines.append(f"Прострочена ({len(expired)}):")
        for r in expired:
            uname = f" {r['username']}" if r["username"] else ""
            lines.append(f"  • {r['name']}{uname} — {r['subscription_until']}")
    if no_sub:
        lines.append(f"\nБез підписки ({len(no_sub)}):")
        for r in no_sub:
            uname = f" {r['username']}" if r["username"] else ""
            lines.append(f"  • {r['name']}{uname}")
    if not expired and not no_sub:
        lines.append("Всі мають активну підписку!")
    await update.message.reply_text("\n".join(lines))

async def cmd_import_subs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("Надішли список:\n@username — РРРР-ММ-ДД\n\nНаприклад:\n@kateryna — 2026-06-01")
    context.user_data["waiting_for"] = "admin_import_subs"

async def cmd_by_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not context.args:
        await update.message.reply_text("Формат: /bycity Київ")
        return
    city = " ".join(context.args)
    conn = get_conn()
    rows = conn.execute("""
        SELECT name, username FROM members WHERE is_active=1 AND LOWER(city) LIKE LOWER(?) ORDER BY name
    """, (f"%{city}%",)).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text(f"Ніхто не вказав місто: {city}")
        return
    lines = [f"Учасниці з {city} ({len(rows)}):\n"]
    for r in rows:
        uname = f" {r['username']}" if r["username"] else ""
        lines.append(f"• {r['name']}{uname}")
    await update.message.reply_text("\n".join(lines))

async def cmd_add_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    context.user_data["waiting_for"] = "admin_event_title"
    context.user_data["new_event"] = {}
    await update.message.reply_text("Введи назву події:")

async def cmd_edit_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/editevent — редагувати існуючу подію."""
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    events = conn.execute(
        "SELECT * FROM events WHERE is_active=1 ORDER BY event_date LIMIT 10"
    ).fetchall()
    conn.close()
    if not events:
        await update.message.reply_text("Активних подій немає")
        return
    buttons = []
    for ev in events:
        buttons.append([InlineKeyboardButton(
            f"{ev['title']} — {ev['event_date']}",
            callback_data=f"admin_edit_event_{ev['id']}"
        )])
    await update.message.reply_text(
        "Обери подію для редагування:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )


async def cmd_test_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("Запускаю...")
    await daily_check(context)
    await update.message.reply_text("Готово")

async def cmd_clear_log(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    conn.execute("DELETE FROM reminder_log")
    conn.commit()
    conn.close()
    await update.message.reply_text("Журнал очищено")

async def cmd_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    member = get_member(update.effective_user.id)
    if member and has_active_sub(member):
        await show_menu(update, context, member)
    else:
        await cmd_start(update, context)

# ─── Адмін введення ───────────────────────────────────────────────────────────

async def handle_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE, waiting: str, text: str):
    if waiting == "admin_import_subs":
        context.user_data["waiting_for"] = None
        lines = text.strip().split("\n")
        imported, created, failed = 0, 0, 0
        conn = get_conn()
        for line in lines:
            line = line.strip()
            if not line:
                continue
            m = re.match(r'(@?\S+)\s*[—\-–]\s*(\d{4}-\d{2}-\d{2})', line)
            if m:
                identifier = m.group(1).lstrip("@")
                until_str = m.group(2)
                result = conn.execute("""
                    UPDATE members SET subscription_until=? WHERE
                    LOWER(username)=LOWER(?) OR LOWER(username)=LOWER(?) OR LOWER(name) LIKE LOWER(?)
                """, (until_str, f"@{identifier}", identifier, f"%{identifier}%"))
                if result.rowcount:
                    imported += 1
                else:
                    conn.execute("INSERT INTO members (name, username, subscription_until) VALUES (?,?,?)",
                                 (identifier, f"@{identifier}", until_str))
                    created += 1
            else:
                failed += 1
        conn.commit()
        conn.close()
        await update.message.reply_text(f"Оновлено: {imported}\nСтворено: {created}\nПомилок: {failed}")
    elif waiting == "admin_event_title":
        context.user_data["new_event"]["title"] = text
        context.user_data["waiting_for"] = "admin_event_date"
        await update.message.reply_text("Дата (РРРР-ММ-ДД):")
    elif waiting == "admin_event_date":
        context.user_data["new_event"]["event_date"] = text
        context.user_data["waiting_for"] = "admin_event_time"
        await update.message.reply_text("Час (ЧЧ:ХХ) або «-»:")
    elif waiting == "admin_event_time":
        context.user_data["new_event"]["event_time"] = "" if text == "-" else text
        context.user_data["waiting_for"] = "admin_event_location"
        await update.message.reply_text("Місце або «-»:")
    elif waiting == "admin_event_location":
        context.user_data["new_event"]["location"] = "" if text == "-" else text
        context.user_data["waiting_for"] = "admin_event_desc"
        await update.message.reply_text("Опис або «-»:")
    elif waiting == "admin_event_desc":
        context.user_data["new_event"]["description"] = "" if text == "-" else text
        context.user_data["waiting_for"] = "admin_event_spots"
        await update.message.reply_text("Кількість місць (або 0):")
    elif waiting == "admin_event_spots":
        spots = int(text) if text.isdigit() else 0
        context.user_data["new_event"]["max_spots"] = spots
        context.user_data["new_event"]["spots_left"] = spots
        context.user_data["waiting_for"] = "admin_event_paid"
        await update.message.reply_text("Платна? (так/ні):")
    elif waiting == "admin_event_paid":
        is_paid = text.lower() in ("так", "yes", "+", "1")
        context.user_data["new_event"]["is_paid"] = 1 if is_paid else 0
        if is_paid:
            context.user_data["waiting_for"] = "admin_event_price"
            await update.message.reply_text("Вартість (грн):")
        else:
            context.user_data["new_event"]["price"] = 0
            context.user_data["new_event"]["wfp_link"] = ""
            await _save_event(update, context)
    elif waiting == "admin_event_price":
        context.user_data["new_event"]["price"] = int(text) if text.isdigit() else 0
        context.user_data["waiting_for"] = "admin_event_wfp"
        await update.message.reply_text("Посилання WayForPay або «-»:")
    elif waiting == "admin_event_wfp":
        context.user_data["new_event"]["wfp_link"] = "" if text == "-" else text
        await _save_event(update, context)

    elif waiting and waiting.startswith("admin_edit_field_"):
        parts = waiting.split("_")
        field = parts[3]
        event_id = int(parts[4])
        conn = get_conn()
        if field == "price":
            price = int(text) if text.isdigit() else 0
            is_paid = 1 if price > 0 else 0
            conn.execute("UPDATE events SET price=?, is_paid=? WHERE id=?", (price, is_paid, event_id))
        elif field == "date":
            conn.execute("UPDATE events SET event_date=? WHERE id=?", (text, event_id))
        elif field == "time":
            conn.execute("UPDATE events SET event_time=? WHERE id=?", ("" if text == "-" else text, event_id))
        elif field == "location":
            conn.execute("UPDATE events SET location=? WHERE id=?", ("" if text == "-" else text, event_id))
        elif field == "description":
            conn.execute("UPDATE events SET description=? WHERE id=?", ("" if text == "-" else text, event_id))
        elif field == "title":
            conn.execute("UPDATE events SET title=? WHERE id=?", (text, event_id))
        conn.commit()
        ev = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        conn.close()
        context.user_data["waiting_for"] = None
        if ev:
            ev = dict(ev)
            paid_str = f"Платна — {ev['price']} грн" if ev["is_paid"] else "Безкоштовна"
            t = ev['title']
            d = ev['event_date']
            tm = ev.get('event_time', '')
            loc = ev.get('location', '')
            await update.message.reply_text(
                f"Оновлено!\n\n{t}\n{d} {tm}\n{loc}\n{paid_str}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("Редагувати ще", callback_data=f"admin_edit_event_{event_id}"),
                    menu_btn()
                ]])
            )

async def _save_event(target, context: ContextTypes.DEFAULT_TYPE):
    ev = context.user_data.pop("new_event", {})
    context.user_data["waiting_for"] = None
    conn = get_conn()
    conn.execute("""
        INSERT INTO events (title, description, location, event_date, event_time,
                           is_paid, price, wfp_link, max_spots, spots_left)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (ev.get("title"), ev.get("description"), ev.get("location"), ev.get("event_date"),
          ev.get("event_time"), ev.get("is_paid", 0), ev.get("price", 0),
          ev.get("wfp_link"), ev.get("max_spots", 0), ev.get("spots_left", 0)))
    conn.commit()
    conn.close()
    paid_str = f"Платна — {ev.get('price', 0)} грн" if ev.get("is_paid") else "Безкоштовна"
    result_text = f"Подію створено!\n{ev.get('title')}\n{ev.get('event_date')} {ev.get('event_time', '')}\n{ev.get('location', '')}\n{paid_str}"
    if hasattr(target, "edit_message_text"):
        await target.edit_message_text(result_text)
    else:
        await target.message.reply_text(result_text)


# ─── Регулярні події ───────────────────────────────────────────────────────────

WEEKDAYS_UA = {
    "понеділок": 0, "вівторок": 1, "середа": 2, "середи": 2, "середу": 2,
    "четвер": 3, "п'ятниця": 4, "п'ятницю": 4, "субота": 5, "суботу": 5,
    "неділя": 6, "неділю": 6,
}
WEEKDAYS_NAME = ["понеділок", "вівторок", "середу", "четвер", "п'ятницю", "суботу", "неділю"]

def get_next_weekday(weekday: int) -> date:
    """Повертає дату наступного вказаного дня тижня."""
    today = date.today()
    days_ahead = weekday - today.weekday()
    if days_ahead <= 0:
        days_ahead += 7
    return today + timedelta(days=days_ahead)

def get_week_start(d: date) -> str:
    """Повертає ISO рядок початку тижня (понеділок)."""
    return (d - timedelta(days=d.weekday())).isoformat()

async def handle_bot_mention(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробляє звернення до бота в чаті групи."""
    msg = update.message
    if not msg or not msg.text or msg.chat_id != GROUP_CHAT_ID:
        return
    if GROUP_THREAD_ID and msg.message_thread_id != GROUP_THREAD_ID:
        return
    text = msg.text
    text_lower = text.lower()
    # Перевіряємо чи є звернення до бота
    bot_mentions = ["помічник комуни", "@помічникkomuni", "помічнику комуни"]
    bot_username = (context.bot.username or "").lower()
    if bot_username:
        bot_mentions.append(f"@{bot_username}")
    if not any(m in text_lower for m in bot_mentions):
        return
    logger.info(f"Звернення до бота: {text[:80]}")
    # Передаємо Gemini для аналізу
    from datetime import date as _date
    today = _date.today().isoformat()
    result = await gemini_call(
        f"Сьогодні {today}. Проаналізуй звернення до бота: \"{text}\"\n\n"
        f"Визнач чи це прохання додати подію.\n"
        f"Якщо так — поверни JSON:\n"
        f"{{\n"
        f"  \"type\": \"recurring\" або \"once\",\n"
        f"  \"title\": \"назва події\",\n"
        f"  \"weekday\": день тижня (0=пн, 1=вт, 2=ср, 3=чт, 4=пт, 5=сб, 6=нд) або null,\n"
        f"  \"date\": \"YYYY-MM-DD\" або null,\n"
        f"  \"time\": \"ЧЧ:ХХ\" або null,\n"
        f"  \"location\": \"місце або посилання\" або null\n"
        f"}}\n"
        f"Якщо це не прохання про подію — поверни null.",
        300
    )
    if not result or result.strip().lower() == "null":
        return
    try:
        cleaned = result.replace("```json", "").replace("```", "").strip()
        event_data = json.loads(cleaned)
    except Exception:
        return

    event_type = event_data.get("type", "once")
    title = event_data.get("title", "Подія")
    time_str = event_data.get("time") or "—"
    location = event_data.get("location") or "—"
    weekday = event_data.get("weekday")
    ev_date = event_data.get("date")

    if event_type == "recurring" and weekday is not None:
        weekday_name = WEEKDAYS_NAME[int(weekday)]
        text_admin = (
            f"📅 Запит на регулярну подію з чату:\n\n"
            f"Назва: {title}\n"
            f"Повторення: щo{weekday_name}\n"
            f"Час: {time_str}\n"
            f"Місце/посилання: {location}\n\n"
            f"Додати на місяць?"
        )
        buttons = [[
            InlineKeyboardButton("✅ Додати", callback_data=f"rec_add_{weekday}_{time_str}_{title[:20]}"),
            InlineKeyboardButton("✏️ Редагувати", callback_data=f"rec_edit"),
            InlineKeyboardButton("❌ Скасувати", callback_data="rec_skip"),
        ]]
    else:
        date_str = ev_date or "—"
        text_admin = (
            f"📅 Запит на подію з чату:\n\n"
            f"Назва: {title}\n"
            f"Дата: {date_str}\n"
            f"Час: {time_str}\n"
            f"Місце/посилання: {location}\n\n"
            f"Додати подію?"
        )
        buttons = [[
            InlineKeyboardButton("✅ Додати", callback_data="ai_add"),
            InlineKeyboardButton("✏️ Редагувати", callback_data="ai_edit"),
            InlineKeyboardButton("❌ Скасувати", callback_data="ai_skip"),
        ]]

    for admin_id in ADMIN_IDS:
        try:
            await context.bot.send_message(
                admin_id, text_admin,
                reply_markup=InlineKeyboardMarkup(buttons)
            )
            context.bot_data[f"ai_event_{admin_id}"] = event_data
        except Exception as e:
            logger.error(f"Помилка надсилання адміну: {e}")


async def job_recurring_reminders(context: ContextTypes.DEFAULT_TYPE):
    """Щоденна перевірка регулярних подій — нагадування в чат і особисті."""
    conn = get_conn()
    now = datetime.now()
    today = date.today()
    current_weekday = today.weekday()
    events = conn.execute(
        "SELECT * FROM recurring_events WHERE is_active=1 AND active_until >= ?",
        (today.isoformat(),)
    ).fetchall()

    for ev in events:
        ev = dict(ev)
        ev_weekday = ev["weekday"]
        ev_time_str = ev["event_time"]
        try:
            ev_hour, ev_min = map(int, ev_time_str.split(":"))
        except Exception:
            continue

        days_until = (ev_weekday - current_weekday) % 7
        if days_until == 0:
            days_until = 7

        # За 7 днів — нагадування в чат
        if days_until == 7:
            next_date = today + timedelta(days=7)
            msg = (
                f"📅 Нагадуємо — наступного {WEEKDAYS_NAME[ev_weekday]} о {ev_time_str} "
                f"у нас {ev['title']}!\n\n"
                f"Зареєструватись можна в боті 👇"
            )
            kwargs = {"chat_id": GROUP_CHAT_ID, "text": msg}
            if GROUP_THREAD_ID:
                kwargs["message_thread_id"] = GROUP_THREAD_ID
            try:
                await context.bot.send_message(**kwargs)
            except Exception as e:
                logger.error(f"Помилка нагадування в чат (7д): {e}")

        # За 1 день — нагадування в чат
        if days_until == 1:
            msg = (
                f"⏰ Завтра о {ev_time_str} — {ev['title']}!\n\n"
                f"Ще не записалась? Реєструйся в боті 👇"
            )
            kwargs = {"chat_id": GROUP_CHAT_ID, "text": msg}
            if GROUP_THREAD_ID:
                kwargs["message_thread_id"] = GROUP_THREAD_ID
            try:
                await context.bot.send_message(**kwargs)
            except Exception as e:
                logger.error(f"Помилка нагадування в чат (1д): {e}")

        # За 1 годину — особисте повідомлення зареєстрованим
        if days_until == 0 or (days_until == 7 and current_weekday == ev_weekday):
            event_dt = datetime.combine(today, __import__('datetime').time(ev_hour, ev_min))
            diff_minutes = (event_dt - now).total_seconds() / 60
            if 55 <= diff_minutes <= 65:
                week_start = get_week_start(today)
                regs = conn.execute("""
                    SELECT m.telegram_id, m.name FROM recurring_registrations rr
                    JOIN members m ON rr.member_id = m.id
                    WHERE rr.event_id=? AND rr.week_start=?
                """, (ev["id"], week_start)).fetchall()
                location_text = f"\n\n🔗 {ev['location']}" if ev.get("location") else ""
                for reg in regs:
                    try:
                        await context.bot.send_message(
                            reg["telegram_id"],
                            f"⏰ За годину починається {ev['title']}!\n"
                            f"Час: {ev_time_str}{location_text}"
                        )
                    except Exception:
                        pass

    # Щомісячне питання про продовження
    for ev in events:
        ev = dict(ev)
        active_until = date.fromisoformat(ev["active_until"])
        days_left = (active_until - today).days
        if days_left == 3:
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        admin_id,
                        f"📅 Регулярна подія «{ev['title']}» закінчується через 3 дні ({active_until.strftime('%d.%m.%Y')}).\n\nПродовжити ще на місяць?",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("✅ Продовжити", callback_data=f"rec_extend_{ev['id']}"),
                            InlineKeyboardButton("❌ Завершити", callback_data=f"rec_stop_{ev['id']}"),
                        ]])
                    )
                except Exception as e:
                    logger.error(f"Помилка нагадування про продовження: {e}")

    # Скидаємо реєстрації після завершення тижня (о 00:00 в понеділок)
    if current_weekday == 0 and now.hour == 0:
        last_week = get_week_start(today - timedelta(days=7))
        conn.execute("DELETE FROM recurring_registrations WHERE week_start=?", (last_week,))
        conn.commit()

    conn.close()

# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",       cmd_start))
    app.add_handler(CommandHandler("menu",        cmd_menu))
    app.add_handler(CommandHandler("admin",       cmd_admin))
    app.add_handler(CommandHandler("members",     cmd_members))
    app.add_handler(CommandHandler("birthdays",   cmd_birthdays))
    app.add_handler(CommandHandler("eventstatus", cmd_event_status))
    app.add_handler(CommandHandler("remind",      cmd_remind))
    app.add_handler(CommandHandler("forcebday",   cmd_force_bday))
    app.add_handler(CommandHandler("setbirthday", cmd_set_birthday))
    app.add_handler(CommandHandler("setusername", cmd_set_username))
    app.add_handler(CommandHandler("setsub",      cmd_set_sub))
    app.add_handler(CommandHandler("renewsub",    cmd_renew_sub))
    app.add_handler(CommandHandler("subexpiring", cmd_sub_expiring))
    app.add_handler(CommandHandler("subexpired",  cmd_sub_expired))
    app.add_handler(CommandHandler("importsubs",  cmd_import_subs))
    app.add_handler(CommandHandler("bycity",      cmd_by_city))
    app.add_handler(CommandHandler("addevent",    cmd_add_event))
    app.add_handler(CommandHandler("editevent",   cmd_edit_event))
    app.add_handler(CommandHandler("testcheck",   cmd_test_check))
    app.add_handler(CommandHandler("clearlog",    cmd_clear_log))

    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(MessageHandler(filters.TEXT & filters.ChatType.GROUPS & ~filters.COMMAND, handle_bot_mention))
    app.add_handler(ChatMemberHandler(handle_new_member, ChatMemberHandler.CHAT_MEMBER))

    app.job_queue.run_daily(daily_check, time=dtime(hour=CHECK_HOUR_UTC, minute=0), name="daily_check")
    app.job_queue.run_daily(job_monday_digest, time=dtime(hour=7, minute=0), days=(0,), name="monday_digest")
    app.job_queue.run_daily(job_thursday_digest, time=dtime(hour=9, minute=0), days=(3,), name="thursday_digest")
    app.job_queue.run_repeating(job_recurring_reminders, interval=3600, first=60, name="recurring_check")

    logger.info("Community Bot v2 запущено!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
