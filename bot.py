#!/usr/bin/env python3
"""
🎂 Birthday Fund Bot — Production Ready Version

Includes:
- Stable payment button handling
- Retry for SQLite locks
- Proper logging
- Kyiv timezone
- Protection from duplicates
- Tracking inactive users
- Safer env handling
"""

import logging
import sqlite3
import os
import time
from datetime import datetime, date
import pytz

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)

# CONFIG
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROUP_CHAT_ID = int(os.getenv("GROUP_CHAT_ID", "0"))
DB_PATH = "birthday.db"

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN not set")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

kyiv = pytz.timezone("Europe/Kyiv")

# DB

def get_conn():
    return sqlite3.connect(DB_PATH, timeout=10, check_same_thread=False)


def execute_with_retry(conn, query, params=(), retries=3):
    for i in range(retries):
        try:
            return conn.execute(query, params)
        except sqlite3.OperationalError as e:
            if "locked" in str(e):
                time.sleep(0.2)
            else:
                raise


def init_db():
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE,
            name TEXT,
            is_bot_active INTEGER DEFAULT 1
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            created_at TEXT
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id INTEGER,
            member_id INTEGER,
            paid INTEGER DEFAULT 0,
            paid_at TEXT,
            UNIQUE(event_id, member_id)
        )
    """)

    conn.commit()
    conn.close()

# COMMANDS

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user

    conn = get_conn()
    execute_with_retry(
        conn,
        "INSERT OR IGNORE INTO members (telegram_id, name) VALUES (?, ?)",
        (user.id, user.full_name)
    )
    conn.commit()
    conn.close()

    await update.message.reply_text("Бот активовано ✅")


async def create_event(update: Update, context: ContextTypes.DEFAULT_TYPE):
    conn = get_conn()

    now = datetime.now(kyiv).isoformat()

    execute_with_retry(conn, "INSERT INTO events (name, created_at) VALUES (?, ?)",
                       ("Birthday", now))
    event_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    members = conn.execute("SELECT id, telegram_id FROM members").fetchall()

    for m in members:
        execute_with_retry(
            conn,
            "INSERT OR IGNORE INTO payments (event_id, member_id) VALUES (?, ?)",
            (event_id, m[0])
        )

    conn.commit()
    conn.close()

    keyboard = InlineKeyboardMarkup([[ 
        InlineKeyboardButton("✅ Я оплатила", callback_data=f"paid{event_id}")
    ]])

    for m in members:
        if m[1]:
            try:
                await context.bot.send_message(
                    chat_id=m[1],
                    text="Оплати внесок",
                    reply_markup=keyboard
                )
            except Exception:
                conn = get_conn()
                execute_with_retry(conn,
                    "UPDATE members SET is_bot_active=0 WHERE telegram_id=?",
                    (m[1],)
                )
                conn.commit()
                conn.close()

# CALLBACK

async def handle_paid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data
    if not data.startswith("paid"):
        return

    event_id = int(data.replace("paid", ""))
    user_id = query.from_user.id

    logger.info(f"[PAYMENT CLICK] user={user_id} event={event_id}")

    conn = get_conn()

    member = conn.execute(
        "SELECT id FROM members WHERE telegram_id=?",
        (user_id,)
    ).fetchone()

    if not member:
        await query.edit_message_text("❌ Тебе немає в системі")
        conn.close()
        return

    member_id = member[0]

    existing = conn.execute(
        "SELECT paid FROM payments WHERE event_id=? AND member_id=?",
        (event_id, member_id)
    ).fetchone()

    if not existing:
        await query.edit_message_text("❌ Подію не знайдено")
        conn.close()
        return

    if existing[0] == 1:
        await query.answer("Вже відмічено ✅")
        conn.close()
        return

    execute_with_retry(
        conn,
        "UPDATE payments SET paid=1, paid_at=? WHERE event_id=? AND member_id=?",
        (datetime.now(kyiv).isoformat(), event_id, member_id)
    )

    conn.commit()
    conn.close()

    await query.edit_message_text("✅ Оплату зафіксовано! 💖")

# MAIN

def main():
    init_db()

    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("create", create_event))

    app.add_handler(CallbackQueryHandler(handle_paid, pattern=r"^paid\\d+$"))

    logger.info("Bot started")
    app.run_polling()


if __name__ == "__main__":
    main()
