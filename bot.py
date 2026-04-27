#!/usr/bin/env python3
"""
Community Bot — повний бот для комуни жіноцтва
Функції: підписка, ДН збори, події, вішліст, пошук учасниць, пересилання з каналу
"""

import logging
import sqlite3
import re
import os
import hashlib
import hmac
import json
import asyncio
from datetime import datetime, date, time as dtime, timedelta
from typing import Optional
from aiohttp import web

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, ReplyKeyboardRemove
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, ChatMemberHandler, filters, ContextTypes,
)

# ─── Конфігурація ────────────────────────────────────────────────────────────
BOT_TOKEN         = os.getenv("BOT_TOKEN", "")
ADMIN_IDS         = [int(x) for x in os.getenv("ADMIN_IDS", "123456789").split(",")]
GROUP_CHAT_ID     = int(os.getenv("GROUP_CHAT_ID", "0"))
GROUP_THREAD_ID   = int(os.getenv("GROUP_THREAD_ID", "0")) or None
BIRTHDAY_THREAD_ID = int(os.getenv("BIRTHDAY_THREAD_ID", "0")) or None
CONGRATS_THREAD_ID = int(os.getenv("CONGRATS_THREAD_ID", "0")) or None
CHECK_HOUR_UTC    = int(os.getenv("CHECK_HOUR_UTC", "17"))
JAR_LINK          = os.getenv("JAR_LINK", "https://send.monobank.ua/jar/YOUR_LINK")
AMOUNT_PER_PERSON = int(os.getenv("AMOUNT_PER_PERSON", "88"))
INVITE_LINK       = os.getenv("INVITE_LINK", "https://t.me/+YOUR_INVITE_LINK")
INSTAGRAM_COMMUNITY = os.getenv("INSTAGRAM_COMMUNITY", "https://www.instagram.com/your_community/")
INSTAGRAM_FOUNDER   = os.getenv("INSTAGRAM_FOUNDER",   "https://www.instagram.com/your_founder/")
FORWARD_CHANNEL_ID  = int(os.getenv("FORWARD_CHANNEL_ID", "0"))

# WayForPay
WFP_MERCHANT    = os.getenv("WFP_MERCHANT_ACCOUNT", "")
WFP_SECRET      = os.getenv("WFP_SECRET_KEY", "")
WFP_DOMAIN      = os.getenv("WFP_DOMAIN", "your-domain.railway.app")

# Ціни підписки (грн)
SUB_PRICE_3M = int(os.getenv("SUB_PRICE_3M", "500"))
SUB_PRICE_6M = int(os.getenv("SUB_PRICE_6M", "1000"))
SUB_PRICE_1Y = int(os.getenv("SUB_PRICE_1Y", "1800"))

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
            name               TEXT NOT NULL,
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
            auto_created         INTEGER DEFAULT 0,
            created_at           TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS payments (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id  INTEGER NOT NULL,
            member_id INTEGER NOT NULL,
            amount    REAL NOT NULL,
            paid      INTEGER DEFAULT 0,
            confirmed INTEGER DEFAULT 0,
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
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
            is_active    INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS event_registrations (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id  INTEGER NOT NULL,
            member_id INTEGER NOT NULL,
            paid      INTEGER DEFAULT 0,
            paid_at   TEXT,
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

        CREATE TABLE IF NOT EXISTS wfp_orders (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            order_ref    TEXT UNIQUE,
            member_id    INTEGER,
            order_type   TEXT,
            amount       INTEGER,
            status       TEXT DEFAULT 'pending',
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
            event_id     INTEGER
        );
    """)
    # Міграція нових колонок
    for col, definition in [
        ("username",           "TEXT"),
        ("birth_year",         "INTEGER"),
        ("city",               "TEXT"),
        ("wishlist",           "TEXT"),
        ("phone",              "TEXT"),
        ("subscription_plan",  "TEXT"),
        ("onboarding_done",    "INTEGER DEFAULT 0"),
        ("confirmed",          "INTEGER DEFAULT 0"),
    ]:
        try:
            conn.execute(f"ALTER TABLE members ADD COLUMN {col} {definition}")
        except Exception:
            pass
        try:
            conn.execute(f"ALTER TABLE payments ADD COLUMN {col} {definition}")
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

def get_member_by_id(member_id: int) -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
    conn.close()
    return dict(row) if row else None

def get_active_members() -> list:
    today = date.today().isoformat()
    conn = get_conn()
    rows = conn.execute("""
        SELECT * FROM members WHERE is_active=1
        AND (subscription_until IS NULL OR subscription_until >= ?)
        ORDER BY name
    """, (today,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def has_active_subscription(member: dict) -> bool:
    if not member.get("subscription_until"):
        return False
    return member["subscription_until"] >= date.today().isoformat()

def upsert_member(telegram_id: int, name: str, username: str = None):
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO members (telegram_id, name) VALUES (?,?)",
                 (telegram_id, name))
    conn.execute("UPDATE members SET name=? WHERE telegram_id=?", (name, telegram_id))
    if username:
        conn.execute("UPDATE members SET username=? WHERE telegram_id=?",
                     (f"@{username}", telegram_id))
    conn.commit()
    conn.close()

def get_unpaid_bday(event_id: int) -> list:
    conn = get_conn()
    rows = conn.execute("""
        SELECT m.id, m.telegram_id, m.name
        FROM payments p JOIN members m ON p.member_id=m.id
        WHERE p.event_id=? AND p.paid=0
        ORDER BY m.name
    """, (event_id,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def count_paid_bday(event_id: int) -> int:
    conn = get_conn()
    n = conn.execute("SELECT COUNT(*) as n FROM payments WHERE event_id=? AND paid=1",
                     (event_id,)).fetchone()["n"]
    conn.close()
    return n

def get_latest_bday_event() -> Optional[dict]:
    conn = get_conn()
    row = conn.execute("SELECT * FROM birthday_events ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return dict(row) if row else None

def get_bday_event_for_member(member_id: int, bd_date: date) -> Optional[int]:
    conn = get_conn()
    row = conn.execute("""
        SELECT id FROM birthday_events
        WHERE birthday_person_id=? AND event_date=?
        ORDER BY id DESC LIMIT 1
    """, (member_id, bd_date.isoformat())).fetchone()
    conn.close()
    return row["id"] if row else None

def create_bday_event(member: dict, bd_date: date, amount: float, active: list) -> int:
    payers = [m for m in active if m["id"] != member["id"]]
    count = len(payers)
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
        INSERT INTO birthday_events
            (birthday_person_name, birthday_person_id, event_date, amount_per_person, total_members, auto_created)
        VALUES (?,?,?,?,?,1)
    """, (member["name"], member["id"], bd_date.isoformat(), amount, count))
    event_id = c.lastrowid
    for m in payers:
        c.execute("INSERT OR IGNORE INTO payments (event_id, member_id, amount) VALUES (?,?,?)",
                  (event_id, m["id"], amount))
    # Додаємо нових учасниць які можуть бути не в payers
    conn.commit()
    conn.close()
    return event_id

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
    conn.execute("INSERT OR IGNORE INTO reminder_log (member_id, days_before, year, log_type) VALUES (?,?,?,?)",
                 (member_id, days_before, year, log_type))
    conn.commit()
    conn.close()

# ─── WayForPay ───────────────────────────────────────────────────────────────

def wfp_signature(params: list) -> str:
    data = ";".join(str(p) for p in params)
    return hmac.new(WFP_SECRET.encode(), data.encode(), hashlib.md5).hexdigest()

def create_wfp_payment(order_ref: str, amount: int, description: str, return_url: str) -> str:
    """Повертає URL для оплати WayForPay."""
    params = {
        "merchantAccount": WFP_MERCHANT,
        "merchantAuthType": "SimpleSignature",
        "merchantDomainName": WFP_DOMAIN,
        "orderReference": order_ref,
        "orderDate": str(int(datetime.now().timestamp())),
        "amount": str(amount),
        "currency": "UAH",
        "orderTimeout": "49000",
        "productName[]": description,
        "productCount[]": "1",
        "productPrice[]": str(amount),
        "returnUrl": return_url,
        "serviceUrl": f"https://{WFP_DOMAIN}/wfp-webhook",
    }
    sig_params = [
        params["merchantAccount"], params["merchantDomainName"],
        params["orderReference"], params["orderDate"],
        params["amount"], params["currency"],
        description, "1", str(amount)
    ]
    params["merchantSignature"] = wfp_signature(sig_params)
    query = "&".join(f"{k}={v}" for k, v in params.items())
    return f"https://secure.wayforpay.com/pay?{query}"

# ─── Навігація (хлібні крихти) ────────────────────────────────────────────────

def back_btn(label: str = "Назад", data: str = "menu") -> InlineKeyboardButton:
    return InlineKeyboardButton(f"← {label}", callback_data=data)

def menu_btn() -> InlineKeyboardButton:
    return InlineKeyboardButton("Головне меню", callback_data="menu")

# ─── Головне меню ─────────────────────────────────────────────────────────────

async def show_main_menu(update_or_query, context, member: dict):
    sub_until = member.get("subscription_until")
    if sub_until:
        d = date.fromisoformat(sub_until)
        sub_text = f"Підписка до {d.strftime('%d.%m.%Y')}"
    else:
        sub_text = "Підписка не активна"

    text = (
        f"Привіт, {member['name']}!\n\n"
        f"{sub_text}\n\n"
        "Що хочеш зробити?"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Моя анкета", callback_data="profile"),
         InlineKeyboardButton("Вішліст", callback_data="wishlist")],
        [InlineKeyboardButton("Знайти учасницю", callback_data="search"),
         InlineKeyboardButton("Події", callback_data="events")],
        [InlineKeyboardButton("Моя підписка", callback_data="subscription"),
         InlineKeyboardButton("Мої оплати ДН", callback_data="bday_status")],
    ])

    if hasattr(update_or_query, "edit_message_text"):
        await update_or_query.edit_message_text(text, reply_markup=keyboard)
    else:
        msg = update_or_query.message or update_or_query.channel_post
        await msg.reply_text(text, reply_markup=keyboard)

# ─── /start ───────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    uname = user.username

    upsert_member(user.id, user.full_name, uname)
    member = get_member(user.id)

    # Додаємо до активних подій ДН якщо нова
    conn = get_conn()
    cutoff = (date.today() - timedelta(days=14)).isoformat()
    active_events = conn.execute(
        "SELECT id, amount_per_person FROM birthday_events WHERE event_date >= ?",
        (cutoff,)
    ).fetchall()
    for ev in active_events:
        conn.execute("INSERT OR IGNORE INTO payments (event_id, member_id, amount) VALUES (?,?,?)",
                     (ev["id"], member["id"], ev["amount_per_person"]))
    conn.commit()
    conn.close()

    # Якщо не має підписки — вітальне повідомлення для нових
    if not has_active_subscription(member):
        await show_welcome_new(update, context)
        return

    # Якщо не пройшла онбординг — запускаємо
    if not member.get("onboarding_done"):
        await start_onboarding(update, context)
        return

    await show_main_menu(update, context, member)
    await _check_urgent_birthdays(context)

async def show_welcome_new(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "Привіт! Вітаємо в боті Комуни Жіноцтва!\n\n"
        "Ми — спільнота жінок, що підтримують одна одну, діляться досвідом і разом ростуть.\n\n"
        "Ти можеш:\n"
        "· Долучитись до закритого чату\n"
        "· Отримувати запрошення на події\n"
        "· Брати участь у зборах на дні народження\n"
        "· Знаходити однодумиць\n\n"
        f"Instagram комуни: {INSTAGRAM_COMMUNITY}\n"
        f"Instagram засновниці: {INSTAGRAM_FOUNDER}\n\n"
        "Щоб долучитись — оформи підписку:"
    )
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Підписатись", callback_data="subscribe")
    ]])
    await update.message.reply_text(text, reply_markup=keyboard)

# ─── Онбординг ────────────────────────────────────────────────────────────────

ONBOARDING_STEPS = ["birthday", "city", "nova_poshta", "instagram", "favorite_color", "wishlist"]
ONBOARDING_QUESTIONS = {
    "birthday":      "Введи свій день народження у форматі ДД.ММ.РРРР\nНаприклад: 25.04.1995\n_(або «-» щоб пропустити)_",
    "city":          "Напиши своє місто\nНаприклад: Київ\n_(або «-» щоб пропустити)_",
    "nova_poshta":   "Напиши своє відділення Нової пошти\nНаприклад: НП відділення 47, Київ\n_(або «-» щоб пропустити)_",
    "instagram":     "Напиши свій Instagram нікнейм\nНаприклад: @kateryna\n_(або «-» щоб пропустити)_",
    "favorite_color":"Напиши свій улюблений колір\nНаприклад: лавандовий\n_(або «-» щоб пропустити)_",
    "wishlist":      "Додай посилання на свій вішліст!\n\nРекомендуємо створити на сайті:\nhttps://goodsend.it/intro/1\n\nВстав посилання або напиши що хочеш отримати в подарунок\n_(або «-» щоб пропустити)_",
}

async def start_onboarding(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["onboarding_step"] = "birthday"
    await update.message.reply_text(
        "Давай заповнимо твою анкету — займе 1 хвилину!\n\n" +
        ONBOARDING_QUESTIONS["birthday"]
    )

async def handle_onboarding_step(update: Update, context: ContextTypes.DEFAULT_TYPE, step: str, text: str):
    user_id = update.effective_user.id
    conn = get_conn()

    if step == "birthday":
        result = parse_birthday(text)
        year = parse_birth_year(text)
        if result and text != "-":
            day, month = result
            bd = f"{month:02d}-{day:02d}"
            conn.execute("UPDATE members SET birthday=? WHERE telegram_id=?", (bd, user_id))
            if year:
                conn.execute("UPDATE members SET birth_year=? WHERE telegram_id=?", (year, user_id))
        conn.commit()
        conn.close()
        context.user_data["onboarding_step"] = "city"
        await update.message.reply_text(ONBOARDING_QUESTIONS["city"])

    elif step == "city":
        if text != "-":
            conn.execute("UPDATE members SET city=? WHERE telegram_id=?", (text, user_id))
        conn.commit()
        conn.close()
        context.user_data["onboarding_step"] = "nova_poshta"
        await update.message.reply_text(ONBOARDING_QUESTIONS["nova_poshta"])

    elif step == "nova_poshta":
        if text != "-":
            conn.execute("UPDATE members SET nova_poshta=? WHERE telegram_id=?", (text, user_id))
        conn.commit()
        conn.close()
        context.user_data["onboarding_step"] = "instagram"
        await update.message.reply_text(ONBOARDING_QUESTIONS["instagram"])

    elif step == "instagram":
        if text != "-":
            insta = text if text.startswith("@") else "@" + text
            conn.execute("UPDATE members SET instagram=? WHERE telegram_id=?", (insta, user_id))
        conn.commit()
        conn.close()
        context.user_data["onboarding_step"] = "favorite_color"
        await update.message.reply_text(ONBOARDING_QUESTIONS["favorite_color"])

    elif step == "favorite_color":
        if text != "-":
            conn.execute("UPDATE members SET favorite_color=? WHERE telegram_id=?", (text, user_id))
        conn.commit()
        conn.close()
        context.user_data["onboarding_step"] = "wishlist"
        await update.message.reply_text(ONBOARDING_QUESTIONS["wishlist"])

    elif step == "wishlist":
        if text != "-":
            conn.execute("UPDATE members SET wishlist=? WHERE telegram_id=?", (text, user_id))
        conn.execute("UPDATE members SET onboarding_done=1 WHERE telegram_id=?", (user_id,))
        conn.commit()
        conn.close()
        context.user_data.pop("onboarding_step", None)
        member = get_member(user_id)
        await update.message.reply_text(
            "Анкету заповнено! Дякуємо!\n\n"
            "Тепер ти будеш отримувати повідомлення про дні народження, події та новини спільноти."
        )
        await show_main_menu(update, context, member)

# ─── Підписка ─────────────────────────────────────────────────────────────────

async def show_subscription_plans(query, member_id: int):
    text = (
        "Обери план підписки:\n\n"
        f"3 місяці — {SUB_PRICE_3M} грн\n"
        f"6 місяців — {SUB_PRICE_6M} грн\n"
        f"1 рік — {SUB_PRICE_1Y} грн"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"3 місяці — {SUB_PRICE_3M} грн", callback_data=f"sub_3m_{member_id}")],
        [InlineKeyboardButton(f"6 місяців — {SUB_PRICE_6M} грн", callback_data=f"sub_6m_{member_id}")],
        [InlineKeyboardButton(f"1 рік — {SUB_PRICE_1Y} грн", callback_data=f"sub_1y_{member_id}")],
        [back_btn("Назад", "menu")],
    ])
    await query.edit_message_text(text, reply_markup=keyboard)

async def handle_sub_plan(query, context, plan: str, member_id: int):
    plans = {"3m": (3, SUB_PRICE_3M, "3 місяці"), "6m": (6, SUB_PRICE_6M, "6 місяців"), "1y": (12, SUB_PRICE_1Y, "1 рік")}
    months, price, label = plans[plan]
    member = get_member_by_id(member_id)
    if not member:
        await query.answer("Помилка")
        return

    order_ref = f"sub_{member_id}_{int(datetime.now().timestamp())}"
    return_url = f"https://t.me/{(await context.bot.get_me()).username}?start=sub_success"

    conn = get_conn()
    conn.execute("INSERT INTO wfp_orders (order_ref, member_id, order_type, amount) VALUES (?,?,?,?)",
                 (order_ref, member_id, f"sub_{plan}", price))
    conn.commit()
    conn.close()

    if WFP_MERCHANT:
        pay_url = create_wfp_payment(order_ref, price, f"Підписка {label}", return_url)
    else:
        pay_url = JAR_LINK  # fallback якщо WFP не налаштований

    await query.edit_message_text(
        f"Підписка: {label} — {price} грн\n\n"
        f"Для оплати перейди за посиланням:\n{pay_url}\n\n"
        "Після успішної оплати твій доступ активується автоматично.\n"
        "Якщо оплата не підтвердилась протягом 10 хвилин — напиши адміну.",
        reply_markup=InlineKeyboardMarkup([[back_btn("Назад", "subscription")]])
    )

# ─── Профіль ──────────────────────────────────────────────────────────────────

async def show_profile(query, member: dict):
    bd_str = "не вказано"
    if member.get("birthday"):
        parts = member["birthday"].split("-")
        mo, d = int(parts[-2]), int(parts[-1])
        yr = f".{member['birth_year']}" if member.get("birth_year") else ""
        bd_str = f"{d:02d}.{mo:02d}{yr}"

    sub_str = "не активна"
    if member.get("subscription_until"):
        sub_str = f"до {date.fromisoformat(member['subscription_until']).strftime('%d.%m.%Y')}"

    insta_str = member.get("instagram") or "не вказано"
    if member.get("instagram"):
        link = instagram_link(member["instagram"])
        insta_str = f"{member['instagram']} ({link})"

    text = (
        f"Твоя анкета:\n\n"
        f"Ім'я: {member['name']}\n"
        f"ДН: {bd_str}\n"
        f"Місто: {member.get('city') or 'не вказано'}\n"
        f"НП: {member.get('nova_poshta') or 'не вказано'}\n"
        f"Instagram: {insta_str}\n"
        f"Улюблений колір: {member.get('favorite_color') or 'не вказано'}\n"
        f"Підписка: {sub_str}"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Редагувати анкету", callback_data="edit_profile")],
        [InlineKeyboardButton("Мій вішліст", callback_data="wishlist")],
        [back_btn(), menu_btn()],
    ])
    await query.edit_message_text(text, reply_markup=keyboard)

# ─── Вішліст ─────────────────────────────────────────────────────────────────

async def show_wishlist(query, member: dict):
    wl = member.get("wishlist")
    if wl:
        text = f"Твій вішліст:\n\n{wl}\n\nРекомендуємо оновлювати перед днем народження!"
    else:
        text = (
            "У тебе ще немає вішлісту.\n\n"
            "Додай посилання або опиши що хочеш отримати в подарунок.\n\n"
            "Рекомендуємо створити на сайті:\nhttps://goodsend.it/intro/1"
        )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Оновити вішліст", callback_data="edit_wishlist")],
        [back_btn("Профіль", "profile"), menu_btn()],
    ])
    await query.edit_message_text(text, reply_markup=keyboard)

# ─── Пошук учасниці ──────────────────────────────────────────────────────────

async def show_search(query):
    await query.edit_message_text(
        "Знайти учасницю:\n\nНапиши @username або номер телефону",
        reply_markup=InlineKeyboardMarkup([[back_btn(), menu_btn()]])
    )

async def do_search(update: Update, context: ContextTypes.DEFAULT_TYPE, search_text: str):
    conn = get_conn()
    search = search_text.strip().lstrip("@")

    # Пошук за username або телефоном
    row = conn.execute("""
        SELECT * FROM members WHERE
        LOWER(username) = LOWER(?) OR
        LOWER(username) = LOWER(?) OR
        phone = ?
        LIMIT 1
    """, (f"@{search}", search, search_text)).fetchone()
    conn.close()

    if not row:
        await update.message.reply_text(
            "Учасницю не знайдено.",
            reply_markup=InlineKeyboardMarkup([[menu_btn()]])
        )
        return

    m = dict(row)
    insta_str = ""
    if m.get("instagram"):
        link = instagram_link(m["instagram"])
        insta_str = f"Instagram: {link}\n"

    wl_str = f"Вішліст:\n{m['wishlist']}\n" if m.get("wishlist") else ""

    text = (
        f"Учасниця: {m['name']}\n"
        f"Місто: {m.get('city') or 'не вказано'}\n"
        f"НП: {m.get('nova_poshta') or 'не вказано'}\n"
        f"{insta_str}"
        f"Улюблений колір: {m.get('favorite_color') or 'не вказано'}\n"
        f"{wl_str}"
    )
    await update.message.reply_text(
        text,
        reply_markup=InlineKeyboardMarkup([[back_btn("Пошук", "search"), menu_btn()]])
    )
    context.user_data.pop("waiting_for", None)

# ─── Статус оплат ДН ─────────────────────────────────────────────────────────

async def show_bday_status(query, member: dict):
    conn = get_conn()
    rows = conn.execute("""
        SELECT e.id as event_id, e.birthday_person_name, e.event_date, p.amount, p.paid, p.confirmed
        FROM payments p JOIN birthday_events e ON p.event_id=e.id
        WHERE p.member_id=?
        ORDER BY e.id DESC LIMIT 10
    """, (member["id"],)).fetchall()
    conn.close()

    if not rows:
        await query.edit_message_text(
            "Поки що подій не було.",
            reply_markup=InlineKeyboardMarkup([[menu_btn()]])
        )
        return

    buttons = []
    lines = ["Твої оплати на ДН:\n"]
    for r in rows:
        if r["paid"]:
            icon = "✅" if r["confirmed"] else "⏳"
            lines.append(f"{icon} ДН {r['birthday_person_name']} ({r['event_date']}) — {r['amount']:.0f} грн")
        else:
            lines.append(f"❌ ДН {r['birthday_person_name']} ({r['event_date']}) — {r['amount']:.0f} грн")
            buttons.append([InlineKeyboardButton(
                f"Оплатила за {r['birthday_person_name']}",
                callback_data=f"bday_paid_{r['event_id']}"
            )])

    buttons.append([menu_btn()])
    await query.edit_message_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons)
    )

# ─── Підписка — статус ───────────────────────────────────────────────────────

async def show_sub_status(query, member: dict):
    if has_active_subscription(member):
        d = date.fromisoformat(member["subscription_until"])
        days_left = (d - date.today()).days
        text = (
            f"Твоя підписка активна до {d.strftime('%d.%m.%Y')}\n"
            f"Залишилось днів: {days_left}\n\n"
            "Поновити підписку заздалегідь:"
        )
    else:
        text = "Твоя підписка не активна.\n\nОформи підписку щоб мати доступ до всіх функцій:"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("Поновити / Оформити", callback_data="subscribe")],
        [back_btn(), menu_btn()],
    ])
    await query.edit_message_text(text, reply_markup=keyboard)

# ─── Події ───────────────────────────────────────────────────────────────────

async def show_events_list(query, member: dict):
    conn = get_conn()
    events = conn.execute("""
        SELECT * FROM events WHERE is_active=1 AND event_date >= ?
        ORDER BY event_date ASC LIMIT 10
    """, (date.today().isoformat(),)).fetchall()
    conn.close()

    if not events:
        await query.edit_message_text(
            "Найближчих подій немає.",
            reply_markup=InlineKeyboardMarkup([[menu_btn()]])
        )
        return

    buttons = []
    for ev in events:
        paid_icon = "💰" if ev["is_paid"] else "🆓"
        buttons.append([InlineKeyboardButton(
            f"{paid_icon} {ev['title']} — {ev['event_date']}",
            callback_data=f"event_{ev['id']}"
        )])
    buttons.append([menu_btn()])

    await query.edit_message_text(
        "Майбутні події:",
        reply_markup=InlineKeyboardMarkup(buttons)
    )

async def show_event_detail(query, event_id: int, member: dict):
    conn = get_conn()
    ev = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    reg = conn.execute("""
        SELECT * FROM event_registrations WHERE event_id=? AND member_id=?
    """, (event_id, member["id"])).fetchone()
    conn.close()

    if not ev:
        await query.edit_message_text("Подію не знайдено.")
        return

    ev = dict(ev)
    paid_str = f"Вартість: {ev['price']} грн" if ev["is_paid"] else "Безкоштовно"
    spots_str = f"Вільних місць: {ev['spots_left']}" if ev["max_spots"] > 0 else ""

    text = (
        f"{ev['title']}\n\n"
        f"Дата: {ev['event_date']} {ev.get('event_time','')}\n"
        f"Місце: {ev.get('location','не вказано')}\n"
        f"{paid_str}\n"
        f"{spots_str}\n\n"
        f"{ev.get('description','')}"
    )

    buttons = []
    if reg:
        status = "✅ Зареєстрована"
        if ev["is_paid"] and not reg["paid"]:
            status = "⏳ Зареєстрована (очікує оплати)"
            buttons.append([InlineKeyboardButton("Оплатити", callback_data=f"event_pay_{event_id}")])
        buttons.append([InlineKeyboardButton(status, callback_data="noop")])
    else:
        if ev["max_spots"] == 0 or ev["spots_left"] > 0:
            buttons.append([InlineKeyboardButton("Зареєструватись", callback_data=f"event_reg_{event_id}")])
        else:
            buttons.append([InlineKeyboardButton("Місць немає", callback_data="noop")])

    buttons.append([back_btn("Події", "events"), menu_btn()])
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))

# ─── Callback handler ─────────────────────────────────────────────────────────

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    member = get_member(user_id)

    if not member:
        await query.edit_message_text("Напиши /start щоб зареєструватись.")
        return

    # Головне меню
    if data == "menu":
        await show_main_menu(query, context, member)

    elif data == "noop":
        pass

    # Підписка
    elif data == "subscribe":
        await show_subscription_plans(query, member["id"])

    elif data.startswith("sub_") and not data.startswith("sub_reminder"):
        parts = data.split("_")
        if len(parts) == 3:
            plan = parts[1]
            mid = int(parts[2])
            await handle_sub_plan(query, context, plan, mid)

    elif data == "subscription":
        await show_sub_status(query, member)

    # Профіль
    elif data == "profile":
        await show_profile(query, member)

    elif data == "edit_profile":
        context.user_data["onboarding_step"] = "birthday"
        context.user_data["editing"] = True
        await query.edit_message_text(ONBOARDING_QUESTIONS["birthday"])

    # Вішліст
    elif data == "wishlist":
        await show_wishlist(query, member)

    elif data == "edit_wishlist":
        context.user_data["waiting_for"] = "wishlist"
        await query.edit_message_text(ONBOARDING_QUESTIONS["wishlist"])

    # Пошук
    elif data == "search":
        context.user_data["waiting_for"] = "search"
        await show_search(query)

    # Статус ДН
    elif data == "bday_status":
        await show_bday_status(query, member)

    elif data.startswith("bday_paid_"):
        event_id = int(data.split("_")[2])
        conn = get_conn()
        conn.execute("""
            UPDATE payments SET paid=1, paid_at=?
            WHERE event_id=? AND member_id=?
        """, (datetime.now().isoformat(), event_id, member["id"]))
        conn.commit()
        ev = conn.execute("SELECT * FROM birthday_events WHERE id=?", (event_id,)).fetchone()
        conn.close()

        bd_name = ev["birthday_person_name"] if ev else ""
        await query.edit_message_text(
            f"Дякуємо за внесок в комуну! Оплату на ДН {bd_name} зафіксовано.\nАдмін підтвердить найближчим часом."
        )

        # Повідомлення адміну для підтвердження
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"{query.from_user.full_name} відмітила оплату на ДН {bd_name}\n"
                    f"Подія #{event_id}",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("Підтвердити", callback_data=f"admin_confirm_pay_{event_id}_{member['id']}"),
                        InlineKeyboardButton("Відхилити", callback_data=f"admin_reject_pay_{event_id}_{member['id']}"),
                    ]])
                )
            except Exception:
                pass

    # Адмін підтверджує оплату ДН
    elif data.startswith("admin_confirm_pay_"):
        parts = data.split("_")
        event_id, mid = int(parts[3]), int(parts[4])
        conn = get_conn()
        conn.execute("UPDATE payments SET confirmed=1 WHERE event_id=? AND member_id=?",
                     (event_id, mid))
        conn.commit()
        m = conn.execute("SELECT telegram_id, name FROM members WHERE id=?", (mid,)).fetchone()
        ev = conn.execute("SELECT birthday_person_name FROM birthday_events WHERE id=?", (event_id,)).fetchone()
        conn.close()
        await query.edit_message_text(f"Оплату підтверджено для {m['name']}")
        if m and m["telegram_id"]:
            try:
                await context.bot.send_message(
                    m["telegram_id"],
                    f"Адмін підтвердив твою оплату на ДН {ev['birthday_person_name']}!"
                )
            except Exception:
                pass

    elif data.startswith("admin_reject_pay_"):
        parts = data.split("_")
        event_id, mid = int(parts[3]), int(parts[4])
        conn = get_conn()
        conn.execute("UPDATE payments SET paid=0 WHERE event_id=? AND member_id=?",
                     (event_id, mid))
        conn.commit()
        m = conn.execute("SELECT telegram_id, name FROM members WHERE id=?", (mid,)).fetchone()
        conn.close()
        await query.edit_message_text(f"Оплату відхилено для {m['name']}")
        if m and m["telegram_id"]:
            try:
                await context.bot.send_message(
                    m["telegram_id"],
                    "Адмін не підтвердив твою оплату. Будь ласка, перевір переказ і спробуй ще раз."
                )
            except Exception:
                pass

    # Події
    elif data == "events":
        await show_events_list(query, member)

    elif data.startswith("event_") and not data.startswith("event_reg") and not data.startswith("event_pay"):
        event_id = int(data.split("_")[1])
        await show_event_detail(query, event_id, member)

    elif data.startswith("event_reg_"):
        event_id = int(data.split("_")[2])
        conn = get_conn()
        ev = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        conn.execute("INSERT OR IGNORE INTO event_registrations (event_id, member_id) VALUES (?,?)",
                     (event_id, member["id"]))
        if ev and ev["max_spots"] > 0:
            conn.execute("UPDATE events SET spots_left=MAX(0, spots_left-1) WHERE id=?", (event_id,))
        conn.commit()
        conn.close()

        ev = dict(ev)
        if ev["is_paid"]:
            await query.edit_message_text(
                f"Зареєстрована на подію {ev['title']}!\n\nТепер оплати участь:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("Оплатити", callback_data=f"event_pay_{event_id}")],
                    [back_btn("Events", "events"), menu_btn()],
                ])
            )
        else:
            await query.edit_message_text(
                f"Зареєстрована на подію {ev['title']}!\n\nЧекаємо тебе!",
                reply_markup=InlineKeyboardMarkup([[back_btn("Події", "events"), menu_btn()]])
            )

    elif data.startswith("event_pay_"):
        event_id = int(data.split("_")[2])
        conn = get_conn()
        ev = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        conn.close()
        if ev:
            pay_url = ev["wfp_link"] or JAR_LINK
            await query.edit_message_text(
                f"Оплата за подію {ev['title']}\nСума: {ev['price']} грн\n\n"
                f"Посилання для оплати:\n{pay_url}",
                reply_markup=InlineKeyboardMarkup([[back_btn("Подія", f"event_{event_id}"), menu_btn()]])
            )

# ─── Текстові повідомлення ────────────────────────────────────────────────────

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Пересилання з каналу
    message = update.message or update.channel_post
    if not message:
        return

    # Якщо це повідомлення з каналу
    if update.channel_post and FORWARD_CHANNEL_ID:
        if message.chat.id == FORWARD_CHANNEL_ID and GROUP_CHAT_ID:
            kwargs = {"chat_id": GROUP_CHAT_ID, "text": message.text or ""}
            if GROUP_THREAD_ID:
                kwargs["message_thread_id"] = GROUP_THREAD_ID
            try:
                await context.bot.forward_message(
                    chat_id=GROUP_CHAT_ID,
                    from_chat_id=FORWARD_CHANNEL_ID,
                    message_id=message.message_id,
                    message_thread_id=GROUP_THREAD_ID
                )
            except Exception as e:
                logger.error(f"Помилка пересилання: {e}")
        return

    # Повідомлення в групі — парсинг дат народження з гілки анкет
    if update.effective_chat and update.effective_chat.type in ("group", "supergroup"):
        msg = update.message
        if not msg:
            return
        is_bday_thread = (BIRTHDAY_THREAD_ID is None or msg.message_thread_id == BIRTHDAY_THREAD_ID)
        is_our_group = (GROUP_CHAT_ID == 0 or msg.chat_id == GROUP_CHAT_ID)
        if is_our_group and is_bday_thread:
            user = msg.from_user
            text = msg.text or msg.caption or ""
            result = parse_birthday(text)
            if result and user:
                day, month = result
                bd = f"{month:02d}-{day:02d}"
                birth_year = parse_birth_year(text)
                conn = get_conn()
                existing = conn.execute("SELECT id FROM members WHERE telegram_id=?", (user.id,)).fetchone()
                if existing:
                    conn.execute("UPDATE members SET birthday=?, name=? WHERE telegram_id=?",
                                 (bd, user.full_name, user.id))
                else:
                    conn.execute("INSERT OR IGNORE INTO members (telegram_id, name, birthday) VALUES (?,?,?)",
                                 (user.id, user.full_name, bd))
                if birth_year:
                    conn.execute("UPDATE members SET birth_year=? WHERE telegram_id=?", (birth_year, user.id))

                # Парсимо місто, НП, Instagram, колір
                _parse_and_save_profile(conn, user.id, text)
                conn.commit()
                conn.close()
        return

    # Приватні повідомлення
    user_id = update.effective_user.id
    member = get_member(user_id)
    text = message.text.strip() if message.text else ""

    # Онбординг
    if context.user_data.get("onboarding_step"):
        await handle_onboarding_step(update, context, context.user_data["onboarding_step"], text)
        return

    # Очікування вводу
    waiting = context.user_data.get("waiting_for")

    if waiting == "search":
        context.user_data.pop("waiting_for", None)
        await do_search(update, context, text)

    elif waiting == "wishlist":
        context.user_data.pop("waiting_for", None)
        conn = get_conn()
        conn.execute("UPDATE members SET wishlist=? WHERE telegram_id=?", (text, user_id))
        conn.commit()
        conn.close()
        await update.message.reply_text(
            "Вішліст оновлено!",
            reply_markup=InlineKeyboardMarkup([[menu_btn()]])
        )

    elif waiting and waiting.startswith("admin_"):
        await handle_admin_input(update, context, waiting, text)

    elif member:
        if has_active_subscription(member) and member.get("onboarding_done"):
            await show_main_menu(update, context, member)
        elif has_active_subscription(member):
            await start_onboarding(update, context)
        else:
            await show_welcome_new(update, context)

def _parse_and_save_profile(conn, user_id: int, text: str):
    """Парсить анкетні дані з вільного тексту."""
    t = text.lower()
    m = re.search(r'(?:нп|нова\s*пошта)[^\d]*(\d+)', t)
    if m:
        conn.execute("UPDATE members SET nova_poshta=? WHERE telegram_id=?",
                     (f"НП відділення {m.group(1)}", user_id))
    m = re.search(r'(?:instagram|інстаграм|інста)[\:\s]*@?([\w.]+)', t)
    if m:
        conn.execute("UPDATE members SET instagram=? WHERE telegram_id=?",
                     (f"@{m.group(1)}", user_id))
    m = re.search(r'(?:улюблений\s*колір|колір)[\:\s]+(.+)', t)
    if m:
        conn.execute("UPDATE members SET favorite_color=? WHERE telegram_id=?",
                     (m.group(1).strip().capitalize(), user_id))

# ─── Адмін команди ────────────────────────────────────────────────────────────

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    text = (
        "Адмін-панель:\n\n"
        "/members — список учасниць\n"
        "/birthdays — дні народження\n"
        "/eventstatus — статус збору ДН\n"
        "/remind — нагадати боржницям\n"
        "/forcebday Ім'я — запустити збір вручну\n"
        "/newbirthday — новий збір ДН\n"
        "/setbirthday Ім'я ДД.ММ — встановити ДН\n"
        "/setusername Ім'я @нік — встановити нік\n"
        "/setsub @нік РРРР-ММ-ДД — підписка до дати\n"
        "/renewsub @нік 3 — поновити підписку (міс)\n"
        "/subexpiring — підписки що закінчуються\n"
        "/importsubs — імпорт підписок\n"
        "/bycity Місто — список по місту\n"
        "/addevent — додати подію\n"
        "/testcheck — тест планувальника\n"
        "/clearlog — очистити журнал\n"
    )
    await update.message.reply_text(text)

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

async def cmd_by_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if not context.args:
        await update.message.reply_text("Формат: /bycity Київ")
        return
    city = " ".join(context.args)
    conn = get_conn()
    rows = conn.execute("""
        SELECT name, username FROM members
        WHERE is_active=1 AND LOWER(city) LIKE LOWER(?)
        ORDER BY name
    """, (f"%{city}%",)).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text(f"Ніхто не вказав місто: {city}")
        return
    lines = [f"Учасниці з міста {city} ({len(rows)}):\n"]
    for r in rows:
        uname = f" {r['username']}" if r["username"] else ""
        lines.append(f"• {r['name']}{uname}")
    await update.message.reply_text("\n".join(lines))

async def cmd_set_sub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Формат: /setsub @нік РРРР-ММ-ДД\nАбо: /setsub Ім'я РРРР-ММ-ДД")
        return
    identifier = context.args[0].lstrip("@")
    until_str = context.args[1]
    try:
        date.fromisoformat(until_str)
    except ValueError:
        await update.message.reply_text("Формат дати: РРРР-ММ-ДД (наприклад: 2026-06-01)")
        return
    conn = get_conn()
    result = conn.execute("""
        UPDATE members SET subscription_until=? WHERE
        LOWER(username)=LOWER(?) OR LOWER(username)=LOWER(?) OR LOWER(name) LIKE LOWER(?)
    """, (until_str, f"@{identifier}", identifier, f"%{identifier}%"))
    conn.commit()
    conn.close()
    if result.rowcount:
        await update.message.reply_text(f"Підписку до {until_str} встановлено")
    else:
        await update.message.reply_text("Учасницю не знайдено")

async def cmd_renew_sub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Формат: /renewsub @нік 3 (місяців)")
        return
    identifier = context.args[0].lstrip("@")
    months = int(context.args[1])
    conn = get_conn()
    row = conn.execute("""
        SELECT id, subscription_until FROM members WHERE
        LOWER(username)=LOWER(?) OR LOWER(username)=LOWER(?) OR LOWER(name) LIKE LOWER(?)
        LIMIT 1
    """, (f"@{identifier}", identifier, f"%{identifier}%")).fetchone()
    if not row:
        await update.message.reply_text("Учасницю не знайдено")
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
    in_7_days = (date.today() + timedelta(days=7)).isoformat()
    today_str = date.today().isoformat()
    conn = get_conn()
    rows = conn.execute("""
        SELECT name, username, subscription_until FROM members
        WHERE is_active=1 AND subscription_until BETWEEN ? AND ?
        ORDER BY subscription_until
    """, (today_str, in_7_days)).fetchall()
    conn.close()
    if not rows:
        await update.message.reply_text("Підписок що закінчуються за 7 днів немає.")
        return
    lines = [f"Підписки що закінчуються за 7 днів ({len(rows)}):\n"]
    for r in rows:
        uname = f" {r['username']}" if r["username"] else ""
        lines.append(f"• {r['name']}{uname} — до {r['subscription_until']}")
    await update.message.reply_text("\n".join(lines))

async def cmd_import_subs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text(
        "Надішли список у форматі:\n\n"
        "@username або Ім'я — РРРР-ММ-ДД\n\n"
        "Наприклад:\n"
        "@kateryna — 2026-06-01\n"
        "Марина — 2026-09-15\n"
        "@olia — 2027-01-01"
    )
    context.user_data["waiting_for"] = "admin_import_subs"

async def cmd_birthdays(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    conn = get_conn()
    rows = conn.execute("""
        SELECT name, birthday FROM members
        WHERE is_active=1 AND birthday IS NOT NULL
        ORDER BY SUBSTR(birthday,1,2), SUBSTR(birthday,4,2)
    """).fetchall()
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
    event = get_latest_bday_event()
    if not event:
        await update.message.reply_text("Подій не було")
        return
    conn = get_conn()
    rows = conn.execute("""
        SELECT m.name, p.paid, p.confirmed FROM payments p
        JOIN members m ON p.member_id=m.id
        WHERE p.event_id=? ORDER BY p.paid DESC, m.name
    """, (event["id"],)).fetchall()
    conn.close()
    paid = [r for r in rows if r["paid"]]
    unpaid = [r for r in rows if not r["paid"]]
    collected = len([r for r in paid if r["confirmed"]]) * event["amount_per_person"]
    lines = [
        f"ДН {event['birthday_person_name']} | {event['event_date']}",
        f"{event['amount_per_person']:.0f} грн x {event['total_members']} учасниць",
        f"Підтверджено: {collected:.0f} грн\n",
        f"Оплатили ({len(paid)}):",
    ]
    for r in paid:
        icon = "✅" if r["confirmed"] else "⏳"
        lines.append(f"  {icon} {r['name']}")
    lines.append(f"\nНе здали ({len(unpaid)}):")
    for r in unpaid:
        lines.append(f"  • {r['name']}")
    await update.message.reply_text("\n".join(lines))

async def cmd_remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    event = get_latest_bday_event()
    if not event:
        await update.message.reply_text("Активних подій немає")
        return
    unpaid = get_unpaid_bday(event["id"])
    if not unpaid:
        await update.message.reply_text("Всі оплатили!")
        return
    bd_date = date.fromisoformat(event["event_date"])
    amount = int(event["amount_per_person"])
    member_row = get_member_by_id(event["birthday_person_id"]) if event.get("birthday_person_id") else None
    uname = member_row.get("username") if member_row else None

    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Я оплатила!", callback_data=f"bday_paid_{event['id']}")
    ]])
    sent = 0
    for u in unpaid:
        if not u["telegram_id"]:
            continue
        try:
            mention = f" ({uname})" if uname else ""
            await context.bot.send_message(
                chat_id=u["telegram_id"],
                text=(
                    f"Нагадую!\n\n"
                    f"Твій внесок на день народження {event['birthday_person_name']}{mention} "
                    f"({bd_date.strftime('%d.%m')}) ще не зафіксовано.\n\n"
                    f"Сума: {amount} грн\n"
                    f"Посилання: {JAR_LINK}\n\n"
                    f"Після переказу натисни кнопку"
                ),
                reply_markup=keyboard
            )
            sent += 1
        except Exception:
            pass

    today = date.today()
    days_left = max((bd_date - today).days, 0)
    paid = count_paid_bday(event["id"])
    total = event["total_members"]
    percent = round(paid / total * 100) if total else 0

    await send_to_group(context, (
        f"Нагадування!\n\n"
        f"{event['birthday_person_name']} — через {days_left} дн. ({bd_date.strftime('%d.%m')})\n\n"
        f"Вже здали: {percent}%\n\n"
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
    member = conn.execute("SELECT * FROM members WHERE LOWER(name) LIKE LOWER(?)",
                          (f"%{name}%",)).fetchone()
    conn.close()
    if not member:
        await update.message.reply_text(f"Не знайдено: {name}")
        return
    m = dict(member)
    if not m.get("birthday"):
        await update.message.reply_text(f"У {m['name']} немає дати народження")
        return

    parts = m["birthday"].split("-")
    month, day = int(parts[-2]), int(parts[-1])
    today = date.today()
    bd = date(today.year, month, day)
    if bd < today:
        bd = date(today.year + 1, month, day)
    days_until = (bd - today).days

    active = get_active_members()
    payers = [x for x in active if x["id"] != m["id"]]
    count = len(payers) if payers else len(active)
    amount = AMOUNT_PER_PERSON

    event_id = get_bday_event_for_member(m["id"], bd)
    if not event_id:
        event_id = create_bday_event(m, bd, amount, active)

    uname = m.get("username")
    mention = f" ({uname})" if uname else ""

    # Повідомлення в групу
    await send_to_group(context, _group_announce_text(m, bd, days_until, count, amount))

    # Особисті
    keyboard = InlineKeyboardMarkup([[
        InlineKeyboardButton("Я оплатила!", callback_data=f"bday_paid_{event_id}")
    ]])
    sent = 0
    for p in payers:
        if not p["telegram_id"]:
            continue
        try:
            await context.bot.send_message(
                chat_id=p["telegram_id"],
                text=_personal_announce_text(m, amount),
                reply_markup=keyboard
            )
            sent += 1
        except Exception:
            pass

    await update.message.reply_text(f"Надіслано: {sent}\nВ групу: ok")

async def cmd_new_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("Введи ім'я іменинниці:")
    context.user_data["waiting_for"] = "admin_new_bday_name"

async def cmd_set_birthday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    if len(context.args) < 2:
        await update.message.reply_text("Формат: /setbirthday Ім'я ДД.ММ")
        return
    name, date_str = context.args[0], " ".join(context.args[1:])
    result = parse_birthday(date_str)
    if not result:
        await update.message.reply_text(f"Не розпізнала дату: {date_str}")
        return
    day, month = result
    year = parse_birth_year(date_str)
    bd = f"{month:02d}-{day:02d}"
    conn = get_conn()
    existing = conn.execute("SELECT id FROM members WHERE LOWER(name) LIKE LOWER(?)",
                            (f"%{name}%",)).fetchone()
    if existing:
        conn.execute("UPDATE members SET birthday=? WHERE id=?", (bd, existing["id"]))
        if year:
            conn.execute("UPDATE members SET birth_year=? WHERE id=?", (year, existing["id"]))
        msg = f"ДН для {name} оновлено: {day:02d}.{month:02d}"
    else:
        conn.execute("INSERT INTO members (name, birthday) VALUES (?,?)", (name, bd))
        msg = f"Додано {name} з ДН {day:02d}.{month:02d}"
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
    existing = conn.execute("SELECT id FROM members WHERE LOWER(name) LIKE LOWER(?)",
                            (f"%{name}%",)).fetchone()
    if not existing:
        await update.message.reply_text(f"Не знайдено: {name}")
        conn.close()
        return
    conn.execute("UPDATE members SET username=? WHERE id=?", (uname, existing["id"]))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"Username для {name}: {uname}")

async def cmd_add_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("Введи назву події:")
    context.user_data["waiting_for"] = "admin_event_title"
    context.user_data["new_event"] = {}

async def cmd_test_check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS:
        return
    await update.message.reply_text("Запускаю перевірку...")
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

# ─── Тексти повідомлень ДН ────────────────────────────────────────────────────

def _get_member_info(member_id: int) -> dict:
    conn = get_conn()
    row = conn.execute("SELECT * FROM members WHERE id=?", (member_id,)).fetchone()
    conn.close()
    return dict(row) if row else {}

def _group_announce_text(member: dict, bd_date: date, days_until: int, count: int, amount: int) -> str:
    mo, d = bd_date.month, bd_date.day
    uname = member.get("username")
    mention = f" ({uname})" if uname else ""

    info = _get_member_info(member["id"])
    parts = []
    if info.get("nova_poshta"):
        parts.append(f"НП: {info['nova_poshta']}")
    if info.get("instagram"):
        link = instagram_link(info["instagram"])
        parts.append(f"Instagram: {link}")
    if info.get("favorite_color"):
        parts.append(f"Улюблений колір: {info['favorite_color']}")
    if info.get("wishlist"):
        parts.append(f"Вішліст: {info['wishlist']}")
    extra = "\n" + "\n".join(parts) if parts else ""

    if days_until == 0:
        header = "Сьогодні день народження!"
    elif days_until == 1:
        header = "Завтра день народження!"
    elif days_until <= 3:
        header = f"Через {days_until} дні день народження!"
    else:
        header = "Скоро день народження!"

    return (
        f"Дівчата, у нас {header}\n\n"
        f"{member['name']}{mention} святкує {d} {MONTH_GENITIVE_UA[mo]}!"
        f"{extra}\n\n"
        f"Збираємо — по {amount} грн з кожної\n\n"
        f"Скидаємось сюди:\n{JAR_LINK}"
    )

def _personal_announce_text(member: dict, amount: int) -> str:
    uname = member.get("username")
    mention = f" ({uname})" if uname else ""

    info = _get_member_info(member["id"])
    parts = []
    if info.get("nova_poshta"):
        parts.append(f"НП: {info['nova_poshta']}")
    if info.get("instagram"):
        link = instagram_link(info["instagram"])
        parts.append(f"Instagram: {link}")
    if info.get("favorite_color"):
        parts.append(f"Улюблений колір: {info['favorite_color']}")
    if info.get("wishlist"):
        parts.append(f"Вішліст: {info['wishlist']}")
    extra = "\nКорисна інфо про іменинницю:\n" + "\n".join(parts) if parts else ""

    return (
        f"У нашій спільноті скоро іменинниця!\n\n"
        f"{member['name']}{mention} святкує день народження"
        f"{extra}\n\n"
        f"Твоя частина: {amount} грн\n\n"
        f"Переказати на банку:\n{JAR_LINK}\n\n"
        f"Після переказу натисни кнопку"
    )

def _group_birthday_text(member: dict, bd_date: date) -> str:
    mo, d = bd_date.month, bd_date.day
    uname = member.get("username")
    mention = f" ({uname})" if uname else ""
    age = (bd_date.year - member["birth_year"]) if member.get("birth_year") else None
    age_str = f"\nВиповнюється {age} років!" if age else ""

    info = _get_member_info(member["id"])
    wishlist_str = f"\n\nВішліст іменинниці:\n{info['wishlist']}" if info.get("wishlist") else ""

    return (
        f"Сьогодні день народження!\n\n"
        f"Наша улюблена {member['name']}{mention} святкує {d} {MONTH_GENITIVE_UA[mo]}!{age_str}"
        f"{wishlist_str}\n\n"
        f"Дівчата, давайте привітаємо іменинницю!\n\n"
        f"З днем народження, {member['name']}!"
    )

# ─── Надсилання в групу ───────────────────────────────────────────────────────

async def send_to_group(context: ContextTypes.DEFAULT_TYPE, text: str, congrats: bool = False):
    if not GROUP_CHAT_ID:
        return
    thread = GROUP_THREAD_ID
    kwargs = {"chat_id": GROUP_CHAT_ID, "text": text}
    if thread:
        kwargs["message_thread_id"] = thread
    try:
        await context.bot.send_message(**kwargs)
    except Exception as e:
        logger.error(f"Помилка надсилання в групу: {e}")

    if congrats and CONGRATS_THREAD_ID and CONGRATS_THREAD_ID != thread:
        kwargs2 = {"chat_id": GROUP_CHAT_ID, "text": text, "message_thread_id": CONGRATS_THREAD_ID}
        try:
            await context.bot.send_message(**kwargs2)
        except Exception as e:
            logger.error(f"Помилка надсилання в гілку привітань: {e}")

# ─── Планувальник ─────────────────────────────────────────────────────────────

async def daily_check(context: ContextTypes.DEFAULT_TYPE):
    today = date.today()
    logger.info(f"Щоденна перевірка: {today}")
    await _check_birthdays(context, today)
    await _check_subscriptions(context, today)
    await _check_event_reminders(context, today)

async def _check_birthdays(context, today: date):
    conn = get_conn()
    members = conn.execute(
        "SELECT * FROM members WHERE is_active=1 AND birthday IS NOT NULL"
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
            try:
                bd = date(today.year + 1, month, day)
            except ValueError:
                continue

        days_until = (bd - today).days
        m = dict(member)

        active = get_active_members()
        payers = [x for x in active if x["id"] != m["id"]]
        count = len(payers)
        amount = AMOUNT_PER_PERSON

        if days_until == 3 and not already_reminded(m["id"], 3, today.year, "group"):
            event_id = get_bday_event_for_member(m["id"], bd)
            if not event_id:
                event_id = create_bday_event(m, bd, amount, active)
            await send_to_group(context, _group_announce_text(m, bd, 3, count, amount))
            keyboard = InlineKeyboardMarkup([[
                InlineKeyboardButton("Я оплатила!", callback_data=f"bday_paid_{event_id}")
            ]])
            for p in payers:
                if not p["telegram_id"]:
                    continue
                try:
                    await context.bot.send_message(
                        chat_id=p["telegram_id"],
                        text=_personal_announce_text(m, amount),
                        reply_markup=keyboard
                    )
                except Exception:
                    pass
            # Нагадування іменинниці оновити вішліст
            if m.get("telegram_id"):
                wl = m.get("wishlist")
                wl_text = (
                    f"Вже за 3 дні твій день народження!\n\n"
                    f"Нагадую оновити свій вішліст щоб дівчата знали що подарувати.\n\n"
                    f"{('Поточний вішліст: ' + wl) if wl else 'У тебе ще немає вішлісту.'}\n\n"
                    f"Рекомендуємо: https://goodsend.it/intro/1"
                )
                try:
                    await context.bot.send_message(
                        chat_id=m["telegram_id"],
                        text=wl_text,
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("Оновити вішліст", callback_data="edit_wishlist")
                        ]])
                    )
                except Exception:
                    pass
            log_reminder(m["id"], 3, today.year, "group")

        elif days_until == 1 and not already_reminded(m["id"], 1, today.year, "group"):
            event_id = get_bday_event_for_member(m["id"], bd)
            if event_id:
                paid = count_paid_bday(event_id)
                percent = round(paid / count * 100) if count else 0
                await send_to_group(context, (
                    f"Нагадування — завтра день народження!\n\n"
                    f"{m['name']}{' (' + m['username'] + ')' if m.get('username') else ''}\n\n"
                    f"Вже здали: {percent}%\n\n"
                    f"Хто не встиг — перевірте особисті повідомлення"
                ))
                unpaid = get_unpaid_bday(event_id)
                keyboard = InlineKeyboardMarkup([[
                    InlineKeyboardButton("Я оплатила!", callback_data=f"bday_paid_{event_id}")
                ]])
                for u in unpaid:
                    if not u["telegram_id"] or u["telegram_id"] == m.get("telegram_id"):
                        continue
                    try:
                        await context.bot.send_message(
                            chat_id=u["telegram_id"],
                            text=_personal_announce_text(m, amount),
                            reply_markup=keyboard
                        )
                    except Exception:
                        pass
                # Список боржниць адміну
                if unpaid:
                    names = "\n".join(f"  • {u['name']}" for u in unpaid)
                    for admin_id in ADMIN_IDS:
                        try:
                            await context.bot.send_message(
                                admin_id,
                                f"Боржниці — ДН {m['name']} ({bd.strftime('%d.%m')}):\n{names}"
                            )
                        except Exception:
                            pass
            log_reminder(m["id"], 1, today.year, "group")

        elif days_until == 0 and not already_reminded(m["id"], 0, today.year, "group"):
            await send_to_group(context, _group_birthday_text(m, bd), congrats=True)
            log_reminder(m["id"], 0, today.year, "group")

async def _check_subscriptions(context, today: date):
    """Нагадування про закінчення підписки."""
    conn = get_conn()
    members = conn.execute(
        "SELECT * FROM members WHERE is_active=1 AND subscription_until IS NOT NULL"
    ).fetchall()
    conn.close()

    for member in members:
        m = dict(member)
        if not m["telegram_id"]:
            continue
        try:
            sub_until = date.fromisoformat(m["subscription_until"])
        except (ValueError, TypeError):
            continue

        days_left = (sub_until - today).days

        # За 3 дні
        if days_left == 3:
            log_key = f"sub_3d_{today.isoformat()}"
            conn = get_conn()
            already = conn.execute(
                "SELECT id FROM sub_reminder_log WHERE member_id=? AND log_type=? AND sent_date=?",
                (m["id"], "3d", today.isoformat())
            ).fetchone()
            conn.close()
            if not already:
                try:
                    await context.bot.send_message(
                        chat_id=m["telegram_id"],
                        text=(
                            f"Твоя підписка на комуну закінчується {sub_until.strftime('%d.%m.%Y')} — "
                            f"через 3 дні!\n\nПоновити підписку:"
                        ),
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("Поновити підписку", callback_data="subscribe")
                        ]])
                    )
                    conn = get_conn()
                    conn.execute("INSERT OR IGNORE INTO sub_reminder_log (member_id, log_type, sent_date) VALUES (?,?,?)",
                                 (m["id"], "3d", today.isoformat()))
                    conn.commit()
                    conn.close()
                except Exception:
                    pass

        # В день закінчення
        elif days_left == 0:
            conn = get_conn()
            already = conn.execute(
                "SELECT id FROM sub_reminder_log WHERE member_id=? AND log_type=? AND sent_date=?",
                (m["id"], "0d", today.isoformat())
            ).fetchone()
            conn.close()
            if not already:
                try:
                    await context.bot.send_message(
                        chat_id=m["telegram_id"],
                        text=(
                            "Сьогодні закінчується твоя підписка на комуну!\n\n"
                            "Поновити зараз щоб не втратити доступ:"
                        ),
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("Поновити підписку", callback_data="subscribe")
                        ]])
                    )
                    conn = get_conn()
                    conn.execute("INSERT OR IGNORE INTO sub_reminder_log (member_id, log_type, sent_date) VALUES (?,?,?)",
                                 (m["id"], "0d", today.isoformat()))
                    conn.commit()
                    conn.close()
                except Exception:
                    pass

        # Після закінчення — повідомлення адміну
        elif days_left < 0 and days_left >= -1:
            for admin_id in ADMIN_IDS:
                try:
                    uname = m.get("username") or m["name"]
                    await context.bot.send_message(
                        admin_id,
                        f"Підписка закінчилась: {m['name']} ({uname})\n"
                        f"Закінчилась: {sub_until.strftime('%d.%m.%Y')}",
                        reply_markup=InlineKeyboardMarkup([[
                            InlineKeyboardButton("Видалити з чату", callback_data=f"admin_kick_{m['id']}")
                        ]])
                    )
                except Exception:
                    pass

async def _check_event_reminders(context, today: date):
    """Нагадування про події за день до."""
    tomorrow = (today + timedelta(days=1)).isoformat()
    conn = get_conn()
    events = conn.execute(
        "SELECT * FROM events WHERE is_active=1 AND event_date=?", (tomorrow,)
    ).fetchall()
    conn.close()
    for ev in events:
        ev = dict(ev)
        conn = get_conn()
        regs = conn.execute("""
            SELECT m.telegram_id, m.name FROM event_registrations er
            JOIN members m ON er.member_id=m.id
            WHERE er.event_id=? AND (er.paid=1 OR ? = 0)
        """, (ev["id"], ev["is_paid"])).fetchall()
        conn.close()
        for r in regs:
            if not r["telegram_id"]:
                continue
            try:
                await context.bot.send_message(
                    chat_id=r["telegram_id"],
                    text=(
                        f"Нагадування! Завтра подія:\n\n"
                        f"{ev['title']}\n"
                        f"Дата: {ev['event_date']} {ev.get('event_time','')}\n"
                        f"Місце: {ev.get('location','не вказано')}"
                    )
                )
            except Exception:
                pass

async def _check_urgent_birthdays(context: ContextTypes.DEFAULT_TYPE):
    """При активації — перевіряємо термінові ДН."""
    today = date.today()
    conn = get_conn()
    members = conn.execute(
        "SELECT * FROM members WHERE is_active=1 AND birthday IS NOT NULL"
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
        if days_until in [0, 1, 3]:
            if not already_reminded(m["id"], days_until, today.year, "group"):
                await _check_birthdays(context, today)
                break

# ─── Нова учасниця в групі ────────────────────────────────────────────────────

async def handle_new_member(update: Update, context: ContextTypes.DEFAULT_TYPE):
    result = update.chat_member
    if not result:
        return
    if GROUP_CHAT_ID and result.chat.id != GROUP_CHAT_ID:
        return
    old_status = result.old_chat_member.status
    new_status = result.new_chat_member.status
    if old_status in ("left", "kicked", "restricted") and new_status in ("member", "administrator"):
        user = result.new_chat_member.user
        if user.is_bot:
            return
        bot_info = await context.bot.get_me()
        uname_str = f" (@{user.username})" if user.username else ""
        text = (
            f"Вітаємо нову учасницю {user.first_name}{uname_str}!\n\n"
            f"У нас є бот для підтримки комуни — активуй його щоб отримувати повідомлення про дні народження, події та новини."
        )
        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "Активувати бота",
                url=f"https://t.me/{bot_info.username}?start=activate"
            )
        ]])
        try:
            kwargs = {"chat_id": GROUP_CHAT_ID, "text": text, "reply_markup": keyboard}
            if GROUP_THREAD_ID:
                kwargs["message_thread_id"] = GROUP_THREAD_ID
            await context.bot.send_message(**kwargs)
        except Exception as e:
            logger.error(f"Помилка привітання: {e}")

# ─── Адмін видалення з чату ───────────────────────────────────────────────────

async def handle_admin_kick(query, context, member_id: int):
    member = get_member_by_id(member_id)
    if not member or not member.get("telegram_id"):
        await query.edit_message_text("Не вдалось видалити — немає telegram_id")
        return
    try:
        await context.bot.ban_chat_member(GROUP_CHAT_ID, member["telegram_id"])
        await context.bot.unban_chat_member(GROUP_CHAT_ID, member["telegram_id"])
        conn = get_conn()
        conn.execute("UPDATE members SET is_active=0 WHERE id=?", (member_id,))
        conn.commit()
        conn.close()
        await query.edit_message_text(f"{member['name']} видалена з чату")
    except Exception as e:
        await query.edit_message_text(f"Помилка: {e}")

# ─── Адмін введення даних ────────────────────────────────────────────────────

async def handle_admin_input(update: Update, context: ContextTypes.DEFAULT_TYPE, waiting: str, text: str):
    user_id = update.effective_user.id

    if waiting == "admin_new_bday_name":
        context.user_data["pending_bday_name"] = text
        context.user_data["waiting_for"] = None
        # Запускаємо збір
        conn = get_conn()
        member = conn.execute("SELECT * FROM members WHERE LOWER(name) LIKE LOWER(?)",
                              (f"%{text}%",)).fetchone()
        conn.close()
        if member:
            m = dict(member)
        else:
            m = {"name": text, "id": None, "telegram_id": None, "username": None,
                 "birth_year": None, "wishlist": None}

        active = get_active_members()
        payers = [x for x in active if x.get("id") != m.get("id")]
        count = len(payers)
        amount = AMOUNT_PER_PERSON
        today = date.today()

        if m.get("birthday"):
            parts = m["birthday"].split("-")
            mo, d = int(parts[-2]), int(parts[-1])
            bd = date(today.year, mo, d)
            if bd < today:
                bd = date(today.year + 1, mo, d)
        else:
            bd = today

        event_id = create_bday_event(m, bd, amount, active)
        await send_to_group(context, _group_announce_text(m, bd, (bd - today).days, count, amount))

        keyboard = InlineKeyboardMarkup([[
            InlineKeyboardButton("Я оплатила!", callback_data=f"bday_paid_{event_id}")
        ]])
        sent = 0
        for p in payers:
            if not p.get("telegram_id"):
                continue
            try:
                await context.bot.send_message(
                    chat_id=p["telegram_id"],
                    text=_personal_announce_text(m, amount),
                    reply_markup=keyboard
                )
                sent += 1
            except Exception:
                pass
        await update.message.reply_text(f"Подію створено. Надіслано: {sent}")

    elif waiting == "admin_import_subs":
        context.user_data["waiting_for"] = None
        lines = text.strip().split("\n")
        imported, failed = 0, 0
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
                    failed += 1
            else:
                failed += 1
        conn.commit()
        conn.close()
        await update.message.reply_text(f"Імпортовано: {imported}\nНе знайдено: {failed}")

    elif waiting == "admin_event_title":
        context.user_data["new_event"]["title"] = text
        context.user_data["waiting_for"] = "admin_event_date"
        await update.message.reply_text("Дата події (РРРР-ММ-ДД):")

    elif waiting == "admin_event_date":
        context.user_data["new_event"]["event_date"] = text
        context.user_data["waiting_for"] = "admin_event_time"
        await update.message.reply_text("Час події (наприклад: 18:00) або «-»:")

    elif waiting == "admin_event_time":
        context.user_data["new_event"]["event_time"] = text if text != "-" else ""
        context.user_data["waiting_for"] = "admin_event_location"
        await update.message.reply_text("Місце проведення (або «-»):")

    elif waiting == "admin_event_location":
        context.user_data["new_event"]["location"] = text if text != "-" else ""
        context.user_data["waiting_for"] = "admin_event_desc"
        await update.message.reply_text("Опис події (або «-»):")

    elif waiting == "admin_event_desc":
        context.user_data["new_event"]["description"] = text if text != "-" else ""
        context.user_data["waiting_for"] = "admin_event_spots"
        await update.message.reply_text("Кількість місць (або 0 якщо необмежено):")

    elif waiting == "admin_event_spots":
        spots = int(text) if text.isdigit() else 0
        context.user_data["new_event"]["max_spots"] = spots
        context.user_data["new_event"]["spots_left"] = spots
        context.user_data["waiting_for"] = "admin_event_paid"
        await update.message.reply_text("Платна подія? (так/ні):")

    elif waiting == "admin_event_paid":
        is_paid = text.lower() in ("так", "yes", "y", "+", "1")
        context.user_data["new_event"]["is_paid"] = 1 if is_paid else 0
        if is_paid:
            context.user_data["waiting_for"] = "admin_event_price"
            await update.message.reply_text("Вартість (грн):")
        else:
            await _save_event(update, context)

    elif waiting == "admin_event_price":
        context.user_data["new_event"]["price"] = int(text) if text.isdigit() else 0
        context.user_data["waiting_for"] = "admin_event_wfp"
        await update.message.reply_text("Посилання для оплати WayForPay (або «-»):")

    elif waiting == "admin_event_wfp":
        context.user_data["new_event"]["wfp_link"] = text if text != "-" else ""
        await _save_event(update, context)

async def _save_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ev = context.user_data.pop("new_event", {})
    context.user_data["waiting_for"] = None
    conn = get_conn()
    conn.execute("""
        INSERT INTO events (title, description, location, event_date, event_time,
                           is_paid, price, wfp_link, max_spots, spots_left)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (
        ev.get("title"), ev.get("description"), ev.get("location"),
        ev.get("event_date"), ev.get("event_time"),
        ev.get("is_paid", 0), ev.get("price", 0), ev.get("wfp_link"),
        ev.get("max_spots", 0), ev.get("spots_left", 0)
    ))
    conn.commit()
    conn.close()
    paid_str = f"Платна — {ev.get('price',0)} грн" if ev.get("is_paid") else "Безкоштовна"
    await update.message.reply_text(
        f"Подію створено!\n\n"
        f"{ev.get('title')}\n"
        f"{ev.get('event_date')} {ev.get('event_time','')}\n"
        f"{ev.get('location','')}\n"
        f"{paid_str}"
    )

# ─── WFP Webhook ─────────────────────────────────────────────────────────────

async def wfp_webhook(request):
    """Обробник webhook від WayForPay."""
    try:
        data = await request.json()
        order_ref = data.get("orderReference")
        status = data.get("transactionStatus")
        if not order_ref or not status:
            return web.Response(text="ok")

        conn = get_conn()
        order = conn.execute("SELECT * FROM wfp_orders WHERE order_ref=?", (order_ref,)).fetchone()
        if not order:
            conn.close()
            return web.Response(text="ok")

        order = dict(order)
        if status == "Approved":
            conn.execute("UPDATE wfp_orders SET status='paid' WHERE order_ref=?", (order_ref,))
            # Підписка
            if order["order_type"].startswith("sub_"):
                plan = order["order_type"].replace("sub_", "")
                months = {"3m": 3, "6m": 6, "1y": 12}.get(plan, 3)
                new_until = (date.today() + timedelta(days=30 * months)).isoformat()
                conn.execute("UPDATE members SET subscription_until=?, subscription_plan=? WHERE id=?",
                             (new_until, plan, order["member_id"]))
                conn.commit()
                m = conn.execute("SELECT telegram_id FROM members WHERE id=?",
                                 (order["member_id"],)).fetchone()
                conn.close()
                if m and m["telegram_id"]:
                    try:
                        app = request.app["bot_app"]
                        await app.bot.send_message(
                            m["telegram_id"],
                            f"Підписку активовано до {new_until}!\n\nПосилання на чат:\n{INVITE_LINK}"
                        )
                    except Exception:
                        pass
        else:
            conn.execute("UPDATE wfp_orders SET status='failed' WHERE order_ref=?", (order_ref,))
            conn.commit()
            conn.close()
    except Exception as e:
        logger.error(f"WFP webhook error: {e}")
    return web.Response(text="ok")

# ─── Запуск ───────────────────────────────────────────────────────────────────

def main():
    init_db()
    app = Application.builder().token(BOT_TOKEN).build()

    # Команди
    app.add_handler(CommandHandler("start",        cmd_start))
    app.add_handler(CommandHandler("admin",        cmd_admin))
    app.add_handler(CommandHandler("members",      cmd_members))
    app.add_handler(CommandHandler("birthdays",    cmd_birthdays))
    app.add_handler(CommandHandler("eventstatus",  cmd_event_status))
    app.add_handler(CommandHandler("remind",       cmd_remind))
    app.add_handler(CommandHandler("forcebday",    cmd_force_bday))
    app.add_handler(CommandHandler("newbirthday",  cmd_new_birthday))
    app.add_handler(CommandHandler("setbirthday",  cmd_set_birthday))
    app.add_handler(CommandHandler("setusername",  cmd_set_username))
    app.add_handler(CommandHandler("setsub",       cmd_set_sub))
    app.add_handler(CommandHandler("renewsub",     cmd_renew_sub))
    app.add_handler(CommandHandler("subexpiring",  cmd_sub_expiring))
    app.add_handler(CommandHandler("importsubs",   cmd_import_subs))
    app.add_handler(CommandHandler("bycity",       cmd_by_city))
    app.add_handler(CommandHandler("addevent",     cmd_add_event))
    app.add_handler(CommandHandler("testcheck",    cmd_test_check))
    app.add_handler(CommandHandler("clearlog",     cmd_clear_log))

    # Callbacks
    app.add_handler(CallbackQueryHandler(
        lambda u, c: handle_admin_kick(u.callback_query, c, int(u.callback_query.data.split("_")[2])),
        pattern=r"^admin_kick_\d+$"
    ))
    app.add_handler(CallbackQueryHandler(handle_callback))

    # Повідомлення
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    app.add_handler(ChatMemberHandler(handle_new_member, ChatMemberHandler.CHAT_MEMBER))

    # Планувальник
    app.job_queue.run_daily(
        daily_check,
        time=dtime(hour=CHECK_HOUR_UTC, minute=0),
        name="daily_check"
    )

    logger.info("Community Bot запущено!")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
