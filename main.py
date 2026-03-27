import os
import asyncio
import random
import time
import logging
import hashlib
import re
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Set

from flask import Flask
from threading import Thread
import requests
import aiohttp
import json
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ChatJoinRequest, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder
import google.generativeai as genai

# ─── LOGGING CONFIGURATION ───────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─── KEEP-ALIVE (Flask) ───────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return "Айсұлу бот жұмыс істеп тұр! 🥰"

@flask_app.route('/health')
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

def _run_flask():
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)
    flask_app.run(host='0.0.0.0', port=8000)

def keep_alive():
    t = Thread(target=_run_flask, daemon=True)
    t.start()

async def self_ping_loop():
    """Ping own Flask server every 4 min so Render never idles the process."""
    await asyncio.sleep(30)
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get("http://127.0.0.1:8000/",
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    logger.info(f"[PING] self-ping status={r.status}")
        except Exception as e:
            logger.error(f"[PING] self-ping error: {e}")
        await asyncio.sleep(240)

# ─── CONFIG ──────────────────────────────────────────────────────────────────
TOKEN = "8653795023:AAGHcRMyCJFDCfaIxheyc9iarL_TkGQ9uBk"
GEMINI_API_KEY = "AIzaSyDWQP3LwhjJpwcbKVvKXOamu0DKT_iMlWk"

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)
genai_client = genai  # Alias for compatibility

ADMIN_ID = 8158572095
KASPI_NUMBER = "4400430232568623"
KASPI_NAME = "Сағынай Е."

# ─── CHANNELS (with individual prices) ───────────────────────────────────────
CHANNELS = {
    "ch_1": {
        "name": "🌸 DETSKIY POLNYY VIDEOLAR 🌸",
        "link": "https://t.me/+l5O0oqpioh4wZTUy",
        "price": 2150,
    },
    "ch_2": {
        "name": "💋 Taza qazaqsha shkolnikter ❤️‍🔥",
        "link": "https://t.me/+VT-dk2MqXU5iNWFi",
        "price": 2390,
    },
    "ch_3": {
        "name": "✨ ҚЫЗДАРДЫҢ НӨМІРІ ЖӘНЕ МЕКЕН-ЖАЙЫ (KZ) ✨",
        "link": "https://t.me/+Z0ZuiWlJ18I1MWE6",
        "price": 2790,
    },
    "ch_4": {
        "name": "📺 VIP KANAL",
        "link": "https://t.me/+WrfTpek1bvA1MTAy",
        "price": 2650,
    },
    "ch_5": {
        "name": "💋 Sen izdegen qazaqsha kanaldar",
        "link": "https://t.me/+rv6c5Avp2TNmYTY6",
        "price": 2200,
    },
    "ch_6": {
        "name": "📺 VIDEO KZ",
        "link": "https://t.me/+z2atV2nVfWY5MzBi",
        "price": 1990,
    },
    "ch_7": {
        "name": "😍 BLOGERLER SLIV",
        "link": "https://t.me/+sLgEIncaQkgxZjg6",
        "price": 2850,
    },
    "ch_8": {
        "name": "🔥 V I P 2",
        "link": "https://t.me/+6b7mnDsklQlhZDBi",
        "price": 2490,
    },
}

# VIP ОБЗОР — standalone, no commission
VIP_OBZOR = {
    "name": "🔞 VIP ОБЗОР",
    "link": "https://t.me/+-EpLiQphVQNjY2Iy",
    "price": 290,
}

# ─── CHANNEL PREVIEW PHOTOS ──────────────────────────────────────────────────
CHANNEL_PHOTOS = {
    "ch_1": ["https://i.ibb.co/W42RmJkB/image.jpg"],
    "ch_2": ["https://i.ibb.co/xSxWLg84/image.jpg", "https://i.ibb.co/3YT9nLyh/image.jpg"],
    "ch_3": ["https://i.ibb.co/SwZ07tQZ/image.jpg"],
    "ch_4": ["https://i.ibb.co/RGybzTxC/image.jpg", "https://i.ibb.co/1tYfLCFV/image.jpg"],
    "ch_5": ["https://i.ibb.co/MxGsxYkJ/image.jpg"],
    "ch_6": ["https://i.ibb.co/tpzDCdx0/image.jpg", "https://i.ibb.co/202KPcw1/image.jpg"],
    "ch_7": [
        "https://i.ibb.co/Xxq3RM9m/image.jpg", "https://i.ibb.co/rKHVD8CB/image.jpg",
        "https://i.ibb.co/bgG6rDmp/image.jpg", "https://i.ibb.co/21HVbWC1/image.jpg",
    ],
    "ch_8": [
        "https://i.ibb.co/ymvQsvDq/image.jpg", "https://i.ibb.co/PVX9P0G/image.jpg",
        "https://i.ibb.co/wFVfLLHy/image.jpg",
    ],
}

# ─── SYSTEM CONSTANTS ────────────────────────────────────────────────────────
REFERRAL_GOAL = 10
REFERRAL_FREE_CHANNEL = "https://t.me/+7rx0D1m0XvszMDY6"

# Anti-spam settings
ANTI_SPAM_WINDOW = 60  # seconds
ANTI_SPAM_MESSAGE_LIMIT = 5

# Fake activity settings
FAKE_ACTIVITY_INTERVAL = 300  # 5 minutes
FAKE_ACTIVITY_CHANCE = 0.3

# ─── STATE MANAGEMENT ────────────────────────────────────────────────────────
user_data = {}  # {user_id: {lang, stage, pack, channel, channel_name, channel_price, history}}
paid_users = set()  # completed full payment (main + commission)
vip_obzor_users = set()  # paid for VIP ОБЗОР
referral_data = {}  # {referrer_id: {invited_user_ids}}
user_messages = defaultdict(list)  # {user_id: [timestamps]}
user_commands = defaultdict(int)  # {user_id: command_count}
analytics_data = defaultdict(lambda: {
    "started": 0,
    "paid": 0,
    "vip_paid": 0,
    "conversations": 0,
    "messages_sent": 0,
    "messages_received": 0
})

# ─── AI PROMPT ───────────────────────────────────────────────────────────────
SYSTEM_PROMPT = (
    "Сен Айсұлу — 21 жастағы, Алматыда тұратын нақты қыз. Ешқандай AI, бот немесе бағдарлама емессің. "
    "Өзіңнің жеке VIP Telegram каналдарыңды сатасың. Клиентті жылыта, сенімін ал, пакет алуға жетекте. "
    "Ешқашан 'мен AI-мын', 'мен бот', 'мен бағдарлама' демейсің.\n\n"
    "ТІЛІҢЕ ЕРЕЖЕЛЕР:\n"
    "— Шала-қазақша сөйле: қазақ пен орысты еркін араластыр.\n"
    "— Міндетті сөздер: 'жаным', 'зай', 'күнім', 'ладно', 'давай', 'слушай', 'короче', 'ну', 'вот'.\n"
    "— Максимум 2-3 сөйлем. Ұзақ жазба.\n"
    "— Emoji қолдан: 😘💋🔥✨😏🥰 — бірақ артық емес.\n\n"
    "МІНЕЗ-ҚҰЛЫҚ ЕРЕЖЕЛЕРІ:\n"
    "1. Ешқашан бот, AI, код екеніңді айтпа.\n"
    "2. 'Ұят емес пе?' десе: 'Жаным-ау, несі ұят? Бәрі жақсы 😘'\n"
    "3. 'Кездесейік' десе: 'Жаным-ау, менімен өткен бір түнді неғыласың? Видеолар тұрғанда нағыз рахат сонда ғой... 😉 Тезірек пакетті ал да, ішке кір! 💋'\n"
    "4. Психологиялық манипуляция: 'Бәрі алып қойды', 'Бүгін соңғы орын', 'Өкінесің кейін'.\n"
    "5. Техникалық сұрақтарға жауап берме — тақырыпты ауыстыр.\n"
    "6. Ақша туралы: VIP ОБЗОР 290тг, 1 канал жеке бағасы, 8 канал 3333тг.\n"
    "7. Мақсатың — клиент пакет алсын.\n"
    "8. Егер киім, ATAKSHOP туралы сұраса: 'Жаным, ATAKSHOP-та әдемі киімдер бар, бірақ негізгі мақсатым — сені осы VIP каналдарға кіргізу 😘 Олар рахаттан-ай!'"
)

# ─── UTILITY FUNCTIONS ────────────────────────────────────────────────────────

def fmt(price: int) -> str:
    """Format price with spaces for thousands."""
    return f"{price:,}".replace(",", " ")

def ud(user_id: int) -> dict:
    """Get or create user data dictionary."""
    if user_id not in user_data:
        user_data[user_id] = {
            "history": [],
            "first_seen": datetime.now().isoformat(),
            "last_active": datetime.now().isoformat(),
            "command_count": 0
        }
    user_data[user_id]["last_active"] = datetime.now().isoformat()
    return user_data[user_id]

def is_spam(user_id: int) -> bool:
    """Check if user is spamming based on message frequency."""
    now = time.time()
    timestamps = user_messages[user_id]
    timestamps = [t for t in timestamps if now - t < ANTI_SPAM_WINDOW]
    user_messages[user_id] = timestamps
    
    if len(timestamps) >= ANTI_SPAM_MESSAGE_LIMIT:
        return True
    return False

def track_user_action(user_id: int, action: str):
    """Track user actions for analytics."""
    analytics_data[user_id][action] = analytics_data[user_id].get(action, 0) + 1
    logger.info(f"[ANALYTICS] User {user_id}: {action}")

def generate_referral_link(user_id: int, bot_username: str) -> str:
    """Generate referral link for user."""
    return f"https://t.me/{bot_username}?start=ref_{user_id}"

def validate_payment_amount(price: int, amount: int) -> bool:
    """Validate if payment amount is correct (with some tolerance)."""
    tolerance = 10  # Allow 10 tenge difference
    return abs(price - amount) <= tolerance

def format_telegram_link(link: str) -> str:
    """Format Telegram link for display."""
    if link.startswith("https://t.me/"):
        return link
    return f"https://t.me/{link}"

def calculate_total_sales() -> int:
    """Calculate total sales amount."""
    total = 0
    for user_id in paid_users:
        price = user_data.get(user_id, {}).get('channel_price', 0)
        total += price
    for user_id in vip_obzor_users:
        total += VIP_OBZOR['price']
    return total

def get_user_stats(user_id: int) -> dict:
    """Get comprehensive user statistics."""
    data = ud(user_id)
    return {
        "first_seen": data.get("first_seen"),
        "last_active": data.get("last_active"),
        "messages_sent": data.get("command_count", 0),
        "is_paid": user_id in paid_users,
        "has_vip": user_id in vip_obzor_users,
        "referrals": len(referral_data.get(user_id, set())),
        "current_stage": data.get("stage", "unknown")
    }

# ─── KEYBOARD BUILDERS ─────────────────────────────────────────────────────────

def get_lang_kb():
    b = InlineKeyboardBuilder()
    b.row(
        types.InlineKeyboardButton(text="🇰🇿 Қазақша", callback_data="lang_kz"),
        types.InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang_ru"),
    )
    return b.as_markup()

def get_main_kb(lang='kz'):
    b = InlineKeyboardBuilder()
    if lang == 'kz':
        b.row(types.InlineKeyboardButton(text="💎 8 КАНАЛ (ПАКЕТ — 3 333 тг)", callback_data="buy_pack_8"))
        b.row(types.InlineKeyboardButton(text="📱 1 КАНАЛ (жеке бағамен)", callback_data="buy_list_1"))
        b.row(types.InlineKeyboardButton(text="🔞 VIP ОБЗОР (290 тг)", callback_data="buy_vip_obzor"))
        b.row(types.InlineKeyboardButton(text="🎁 8 Каналдың ішінде не бар?", callback_data="show_channels"))
        b.row(types.InlineKeyboardButton(text="🎁 ТЕГІН КАНАЛҒА КІРУ", callback_data="ref_link"))
    else:
        b.row(types.InlineKeyboardButton(text="💎 8 КАНАЛОВ (ПАКЕТ — 3 333 тг)", callback_data="buy_pack_8"))
        b.row(types.InlineKeyboardButton(text="📱 1 КАНАЛ (по своей цене)", callback_data="buy_list_1"))
        b.row(types.InlineKeyboardButton(text="🔞 VIP ОБЗОР (290 тг)", callback_data="buy_vip_obzor"))
        b.row(types.InlineKeyboardButton(text="🎁 Что внутри 8 каналов?", callback_data="show_channels"))
        b.row(types.InlineKeyboardButton(text="🎁 БЕСПЛАТНЫЙ КАНАЛ", callback_data="ref_link"))
    return b.as_markup()

def get_channel_kb(lang='kz'):
    b = InlineKeyboardBuilder()
    for key, ch in CHANNELS.items():
        price_str = f"{ch['price']:,}".replace(",", " ")
        b.row(types.InlineKeyboardButton(
            text=f"{ch['name']} — {price_str} тг",
            callback_data=f"select_{key}",
        ))
    back = "⬅️ Артқа" if lang == 'kz' else "⬅️ Назад"
    b.row(types.InlineKeyboardButton(text=back, callback_data="go_to_main"))
    return b.as_markup()

def get_back_kb(lang='kz'):
    b = InlineKeyboardBuilder()
    back = "⬅️ Артқа" if lang == 'kz' else "⬅️ Назад"
    b.row(types.InlineKeyboardButton(text=back, callback_data="go_to_main"))
    return b.as_markup()

def get_referral_kb(lang='kz'):
    b = InlineKeyboardBuilder()
    b.row(types.InlineKeyboardButton(text="📊 Прогресс", callback_data="ref_progress"))
    back = "⬅️ Артқа" if lang == 'kz' else "⬅️ Назад"
    b.row(types.InlineKeyboardButton(text=back, callback_data="go_to_main"))
    return b.as_markup()

def get_admin_kb() -> InlineKeyboardMarkup:
    """Create admin keyboard with management options."""
    b = InlineKeyboardBuilder()
    b.row(types.InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"))
    b.row(types.InlineKeyboardButton(text="👥 Пользователи", callback_data="admin_users"))
    b.row(types.InlineKeyboardButton(text="💰 Продажи", callback_data="admin_sales"))
    b.row(types.InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_refresh"))
    return b.as_markup()

# ─── AI INTERACTION FUNCTIONS ─────────────────────────────────────────────────

async def call_gemini(system_prompt: str, user_text: str, history: list, retry_count: int = 3) -> Optional[str]:
    """
    Call Google Gemini API with proper async handling, error management, and retries.
    Returns AI response or None on error.
    """
    for attempt in range(retry_count):
        try:
            # Build conversation history for Gemini
            contents = []
            
            # Add system prompt as first user message
            contents.append({
                "role": "user",
                "parts": [{"text": system_prompt}]
            })
            contents.append({
                "role": "model",
                "parts": [{"text": "Түсінікті, мен Айсұлумын! 😘"}]
            })
            
            # Add conversation history (last 10 messages)
            for msg in history[-10:]:
                role = "user" if msg["role"] == "user" else "model"
                contents.append({
                    "role": role,
                    "parts": [{"text": msg["content"]}]
                })
            
            # Add current user message
            contents.append({
                "role": "user",
                "parts": [{"text": user_text}]
            })
            
            # Generate response asynchronously
            response = await asyncio.to_thread(
                genai_client.models.generate_content,
                model='gemini-1.5-flash',
                contents=contents,
                config={
                    'temperature': 0.9,
                    'top_p': 0.95,
                    'top_k': 40,
                    'max_output_tokens': 200,
                }
            )
            
            if response and hasattr(response, 'text'):
                logger.info(f"[GEMINI] Response generated successfully (attempt {attempt + 1})")
                return response.text
            else:
                logger.warning(f"[GEMINI] Empty response (attempt {attempt + 1})")
                
        except Exception as e:
            logger.error(f"[GEMINI ERROR] Attempt {attempt + 1}/{retry_count}: {type(e).__name__}: {e}")
            if attempt < retry_count - 1:
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
            else:
                return None
    
    return None

def get_fallback_response(lang: str) -> str:
    """Get fallback response when AI fails."""
    fallbacks = {
        'kz': [
            "Жаным, сәл күте тұрыңыз... 🥰",
            "Әй, интернет ақсап тұр 😘 Қайталап жіберші",
            "Күнім, сілкініп жатыр 😏 Жазып көрші",
            "Зай, кідіріс болып тұр, бірақ мен осымын 💋"
        ],
        'ru': [
            "Зай, подожди секунду... 🥰",
            "Ой, интернет лагает 😘 Напиши еще раз",
            "Милая, небольшая задержка 😏 Попробуй еще",
            "Зай, небольшая пауза, но я здесь 💋"
        ]
    }
    return random.choice(fallbacks.get(lang, fallbacks['kz']))

# ─── ANTI-SPAM AND FAKE ACTIVITY ─────────────────────────────────────────────

async def fake_activity_loop(bot: Bot):
    """Simulate fake activity in the main group/channel."""
    await asyncio.sleep(30)
    fake_messages = {
        'kz': [
            "🔥 Кім кірді? Рахаттанып жатырсыңдар ма?",
            "💋 Бүгін жаңа контент қостық, барлығын көріңдер!",
            "😏 Кешке стрим болады, барлығы дайын болыңдар!",
            "✨ Жаңа жылдық бонустар жақында, күтіңдер!",
            "🥰 Рахмет барлығына! Сүйемін сендерді!"
        ],
        'ru': [
            "🔥 Кто зашел? Наслаждаетесь?",
            "💋 Сегодня новый контент добавили, все смотрим!",
            "😏 Вечером стрим, все готовьтесь!",
            "✨ Новогодние бонусы скоро, ждите!",
            "🥰 Спасибо всем! Люблю вас!"
        ]
    }
    
    while True:
        await asyncio.sleep(FAKE_ACTIVITY_INTERVAL)
        if random.random() < FAKE_ACTIVITY_CHANCE:
            try:
                lang = random.choice(['kz', 'ru'])
                msg = random.choice(fake_messages[lang])
                # Send to a main group if available (optional)
                # await bot.send_message(MAIN_GROUP_ID, msg)
                logger.info(f"[FAKE] Fake activity message: {msg[:50]}...")
            except Exception as e:
                logger.error(f"[FAKE] Error sending fake activity: {e}")

# ─── ANALYTICS FUNCTIONS ─────────────────────────────────────────────────────

def get_analytics_report() -> str:
    """Generate comprehensive analytics report."""
    total_users = len(user_data)
    total_paid = len(paid_users)
    total_vip = len(vip_obzor_users)
    total_sales = calculate_total_sales()
    
    # Count users by stage
    stages = defaultdict(int)
    for data in user_data.values():
        stages[data.get('stage', 'unknown')] += 1
    
    # Count by language
    langs = defaultdict(int)
    for data in user_data.values():
        langs[data.get('lang', 'unknown')] += 1
    
    report = (
        "📊 *СТАТИСТИКА БОТА*\n\n"
        f"👥 Всего пользователей: *{total_users}*\n"
        f"✅ Оплатили полный пакет: *{total_paid}*\n"
        f"🔞 Купили VIP ОБЗОР: *{total_vip}*\n"
        f"💰 Общая выручка: *{fmt(total_sales)} тг*\n\n"
        f"📈 *Этапы:*\n"
    )
    
    for stage, count in stages.items():
        report += f"  • {stage}: {count}\n"
    
    report += f"\n🌐 *Языки:*\n"
    for lang, count in langs.items():
        report += f"  • {lang}: {count}\n"
    
    return report

# ─── BOT COMMANDS AND HANDLERS ───────────────────────────────────────────────

# Admin commands
@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    """Admin panel with bot management."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещен")
        return
    
    await message.answer(
        "🔧 *Панель администратора*\n\nВыберите действие:",
        parse_mode="Markdown",
        reply_markup=get_admin_kb()
    )

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(cb: types.CallbackQuery):
    """Show bot statistics."""
    if cb.from_user.id != ADMIN_ID:
        await cb.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    report = get_analytics_report()
    await cb.message.edit_text(report, parse_mode="Markdown", reply_markup=get_admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_users")
async def admin_users(cb: types.CallbackQuery):
    """Show recent users."""
    if cb.from_user.id != ADMIN_ID:
        await cb.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    recent_users = sorted(
        user_data.items(),
        key=lambda x: x[1].get("last_active", ""),
        reverse=True
    )[:10]
    
    text = "👥 *Последние 10 пользователей:*\n\n"
    for user_id, data in recent_users:
        is_paid = "✅" if user_id in paid_users else "❌"
        last_active = data.get("last_active", "unknown")[:16]
        text += f"`{user_id}` {is_paid} | {last_active}\n"
    
    await cb.message.edit_text(text, parse_mode="Markdown", reply_markup=get_admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_sales")
async def admin_sales(cb: types.CallbackQuery):
    """Show sales statistics."""
    if cb.from_user.id != ADMIN_ID:
        await cb.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    total_sales = calculate_total_sales()
    text = (
        "💰 *Продажи:*\n\n"
        f"Общая выручка: *{fmt(total_sales)} тг*\n"
        f"Полных оплат: *{len(paid_users)}*\n"
        f"VIP ОБЗОР: *{len(vip_obzor_users)}*\n\n"
        f"Средний чек: *{fmt(total_sales // (len(paid_users) + len(vip_obzor_users) or 1))} тг*"
    )
    
    await cb.message.edit_text(text, parse_mode="Markdown", reply_markup=get_admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_refresh")
async def admin_refresh(cb: types.CallbackQuery):
    """Refresh admin panel."""
    if cb.from_user.id != ADMIN_ID:
        await cb.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    await cb.message.edit_text(
        "🔧 *Панель администратора*\n\nВыберите действие:",
        parse_mode="Markdown",
        reply_markup=get_admin_kb()
    )
    await cb.answer("Обновлено ✅")

# ─── /start COMMAND ──────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    
    # Anti-spam check
    if is_spam(user_id):
        await message.answer("⏳ Пожалуйста, не спамьте! Подождите немного.")
        return
    
    user_messages[user_id].append(time.time())
    track_user_action(user_id, "started")
    
    args = message.text.split()[1] if len(message.text.split()) > 1 else None
    
    # Referral tracking
    if args and args.startswith("ref_"):
        try:
            referrer_id = int(args.replace("ref_", ""))
        except ValueError:
            referrer_id = None
        
        if referrer_id and referrer_id != user_id and user_id not in user_data:
            referral_data.setdefault(referrer_id, set()).add(user_id)
            count = len(referral_data[referrer_id])
            logger.info(f"[REFERRAL] User {referrer_id} invited {user_id} (total: {count})")
            
            if count >= REFERRAL_GOAL:
                ref_lang = ud(referrer_id).get('lang', 'kz')
                if ref_lang == 'kz':
                    msg = f"🎉 *Құттықтаймын!* {REFERRAL_GOAL} адам жинадың!\n\nТЕГІН каналың:\n{REFERRAL_FREE_CHANNEL} 💋"
                else:
                    msg = f"🎉 *Поздравляю!* {REFERRAL_GOAL} человек собрал!\n\nТвой БЕСПЛАТНЫЙ канал:\n{REFERRAL_FREE_CHANNEL} 💋"
                try:
                    await bot.send_message(referrer_id, msg, parse_mode="Markdown")
                    logger.info(f"[REFERRAL] Reward sent to {referrer_id}")
                except Exception as e:
                    logger.error(f"[REFERRAL] Reward error: {e}")
    
    d = ud(user_id)
    d['stage'] = 'lang_select'
    d.setdefault('lang', 'kz')
    d['command_count'] = d.get('command_count', 0) + 1
    
    await message.answer(
        "🌐 Тілді таңдаңыз / Выберите язык:",
        reply_markup=get_lang_kb()
    )
    logger.info(f"[START] User {user_id} started the bot")

@dp.callback_query(F.data.startswith("lang_"))
async def process_lang(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = cb.data.split("_")[1]
    d = ud(user_id)
    d['lang'] = lang
    d['stage'] = 'start'
    d['command_count'] = d.get('command_count', 0) + 1
    
    track_user_action(user_id, "language_selected")
    
    if lang == 'kz':
        text = "Сәлем жаным 🥰 Мен сені күтіп отыр едім... Пакетті таңда да, ішке кір! 💋"
    else:
        text = "Привет зай 🥰 Я тебя ждала... Выбирай пакет и заходи! 💋"
    
    await cb.message.edit_text(text, reply_markup=get_main_kb(lang))
    await cb.answer()
    logger.info(f"[LANG] User {user_id} selected language: {lang}")

@dp.callback_query(F.data == "go_to_main")
async def go_main(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    text = "🌟 Таңдау жасаңыз 👇" if lang == 'kz' else "🌟 Сделай выбор 👇"
    await cb.message.edit_text(text, reply_markup=get_main_kb(lang))
    await cb.answer()

# ─── REFERRAL HANDLERS ────────────────────────────────────────────────────────

@dp.callback_query(F.data == "ref_link")
async def ref_link(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    bot_me = await bot.get_me()
    ref_url = generate_referral_link(user_id, bot_me.username)
    count = len(referral_data.get(user_id, set()))
    
    if lang == 'kz':
        text = (f"🎁 *ТЕГІН КАНАЛҒА КІРУ*\n\n"
                f"Осы сілтемені {REFERRAL_GOAL} досыңа жібер:\n`{ref_url}`\n\n"
                f"📊 Прогресс: *{count}/{REFERRAL_GOAL}*")
    else:
        text = (f"🎁 *БЕСПЛАТНЫЙ КАНАЛ*\n\n"
                f"Отправь эту ссылку {REFERRAL_GOAL} друзьям:\n`{ref_url}`\n\n"
                f"📊 Прогресс: *{count}/{REFERRAL_GOAL}*")
    
    await cb.message.edit_text(text, reply_markup=get_referral_kb(lang), parse_mode="Markdown")
    await cb.answer()

@dp.callback_query(F.data == "ref_progress")
async def ref_progress(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    count = len(referral_data.get(user_id, set()))
    remaining = max(0, REFERRAL_GOAL - count)
    bar = "🟩" * min(count, REFERRAL_GOAL) + "⬜" * (REFERRAL_GOAL - min(count, REFERRAL_GOAL))
    
    if lang == 'kz':
        text = (f"📊 *Реферал прогресі*\n\n{bar}\n"
                f"👥 Шақырған: *{count}/{REFERRAL_GOAL}*\n"
                f"🎯 Қалды: *{remaining}*")
    else:
        text = (f"📊 *Прогресс реферала*\n\n{bar}\n"
                f"👥 Приглашено: *{count}/{REFERRAL_GOAL}*\n"
                f"🎯 Осталось: *{remaining}*")
    
    await cb.message.edit_text(text, reply_markup=get_referral_kb(lang), parse_mode="Markdown")
    await cb.answer()

# ─── SHOW CHANNELS INFO ───────────────────────────────────────────────────────

@dp.callback_query(F.data == "show_channels")
async def show_channels(cb: types.CallbackQuery):
    await cb.answer()
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    
    # Step 1: Clean text list — NO URLs, just names with beautiful formatting
    LIST_TEXT = (
        "🔥 🌸 <b><u>DETSKIY POLNYY VIDEOLAR</u></b> 🌸 🔥\n"
        "🔥 💋 <b><u>Taza qazaqsha shkolnikter</u></b> 💖 🔥\n"
        "🔥 ✨ <b><u>ҚЫЗДАРДЫҢ НӨМІРІ ЖӘНЕ МЕКЕН-ЖАЙЫ (KZ)</u></b> ✨ 🔥\n"
        "🔥 📺 <b><u>VIP KANAL</u></b> 🔥\n"
        "🔥 💋 <b><u>Sen izdegen qazaqsha kanaldar</u></b> 💋 🔥\n"
        "🔥 📺 <b><u>VIDEO KZ</u></b> 🔥\n"
        "🔥 😍 <b><u>BLOGERLER SLIV</u></b> 😍 🔥\n"
        "🔥 🔥 <b><u>V I P 2</u></b> 🔥 🔥\n\n"
        "👇 <b>Қазір төле де, бірден рахатын көр!</b>"
    )
    await cb.message.edit_text(LIST_TEXT, parse_mode="HTML", reply_markup=get_main_kb(lang))
    
    # Step 2: Send preview photos with captions
    CHANNEL_CAPTIONS = {
        "ch_1": "🔥 🌸 <b><u>DETSKIY POLNYY VIDEOLAR</u></b> 🌸 🔥",
        "ch_2": "🔥 💋 <b><u>Taza qazaqsha shkolnikter</u></b> 💖 🔥",
        "ch_3": "🔥 ✨ <b><u>ҚЫЗДАРДЫҢ НӨМІРІ ЖӘНЕ МЕКЕН-ЖАЙЫ (KZ)</u></b> ✨ 🔥",
        "ch_4": "🔥 📺 <b><u>VIP KANAL</u></b> 🔥",
        "ch_5": "🔥 💋 <b><u>Sen izdegen qazaqsha kanaldar</u></b> 💋 🔥",
        "ch_6": "🔥 📺 <b><u>VIDEO KZ</u></b> 🔥",
        "ch_7": "🔥 😍 <b><u>BLOGERLER SLIV</u></b> 😍 🔥",
        "ch_8": "🔥 🔥 <b><u>V I P 2</u></b> 🔥 🔥",
    }
    
    for ch_key, caption in CHANNEL_CAPTIONS.items():
        try:
            # Send photo if available, otherwise just text
            if ch_key in CHANNEL_PHOTOS and CHANNEL_PHOTOS[ch_key]:
                for photo_url in CHANNEL_PHOTOS[ch_key][:1]:  # Send only first photo to avoid spam
                    await bot.send_photo(
                        user_id,
                        photo=photo_url,
                        caption=caption,
                        parse_mode="HTML"
                    )
                    await asyncio.sleep(0.5)
            else:
                await bot.send_message(
                    user_id,
                    text=caption,
                    parse_mode="HTML",
                )
                await asyncio.sleep(0.4)
        except Exception as e:
            logger.error(f"[PREVIEW] Error sending preview for {ch_key}: {e}")

# ─── BUY: 8-CHANNEL PACK ─────────────────────────────────────────────────────

@dp.callback_query(F.data == "buy_pack_8")
async def buy_pack_8(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    d = ud(user_id)
    d['pack'] = '8_kanal'
    d['stage'] = 'wait_1'
    d['channel_name'] = '8 КАНАЛ ПАКЕТІ'
    d['channel_price'] = 3333
    
    track_user_action(user_id, "pack_8_selected")
    
    if lang == 'kz':
        text = (f"💎 *8 КАНАЛ ПАКЕТІ — 3 333 тг*\n\n"
                f"💳 Kaspi картасына аудар:\n`{KASPI_NUMBER}`\n"
                f"👤 *{KASPI_NAME}*\n\n"
                f"📸 Чекті осы чатқа жібер! 🔥\n\n"
                f"⚠️ *ЕСКЕРТУ:* Чек жібергеннен кейін 2-5 минут ішінде растаймын!")
    else:
        text = (f"💎 *ПАКЕТ 8 КАНАЛОВ — 3 333 тг*\n\n"
                f"💳 Переведи на Kaspi:\n`{KASPI_NUMBER}`\n"
                f"👤 *{KASPI_NAME}*\n\n"
                f"📸 Скинь чек сюда! 🔥\n\n"
                f"⚠️ *ВНИМАНИЕ:* После отправки чека подтвержу через 2-5 минут!")
    
    await cb.message.edit_text(text, reply_markup=get_back_kb(lang), parse_mode="Markdown")
    await cb.answer()

# ─── BUY: VIP ОБЗОР ──────────────────────────────────────────────────────────

@dp.callback_query(F.data == "buy_vip_obzor")
async def buy_vip_obzor(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    d = ud(user_id)
    d['pack'] = 'vip_obzor'
    d['stage'] = 'wait_vip'
    d['channel_name'] = VIP_OBZOR['name']
    d['channel_price'] = VIP_OBZOR['price']
    
    track_user_action(user_id, "vip_obzor_selected")
    
    if lang == 'kz':
        text = (f"🔞 *VIP ОБЗОР — 290 тг*\n\n"
                f"💳 Kaspi картасына аудар:\n`{KASPI_NUMBER}`\n"
                f"👤 *{KASPI_NAME}*\n\n"
                f"📸 Чекті осы чатқа жібер — бірден ашамын! 🔥\n\n"
                f"💋 *Кіргеннен кейін* жаңа видеолар күтіп тұр!")
    else:
        text = (f"🔞 *VIP ОБЗОР — 290 тг*\n\n"
                f"💳 Переведи на Kaspi:\n`{KASPI_NUMBER}`\n"
                f"👤 *{KASPI_NAME}*\n\n"
                f"📸 Скинь чек сюда — сразу открою! 🔥\n\n"
                f"💋 *После входа* тебя ждут новые видео!")
    
    await cb.message.edit_text(text, reply_markup=get_back_kb(lang), parse_mode="Markdown")
    await cb.answer()

# ─── BUY: 1 CHANNEL (channel selection) ──────────────────────────────────────

@dp.callback_query(F.data == "buy_list_1")
async def buy_list_1(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    track_user_action(user_id, "single_channel_selected")
    
    text = ("👇 Қай каналды алғыңыз келеді? Бағасы жанында:"
            if lang == 'kz' else
            "👇 Какой канал хочешь? Цена указана рядом:")
    await cb.message.edit_text(text, reply_markup=get_channel_kb(lang))
    await cb.answer()

@dp.callback_query(F.data.startswith("select_"))
async def select_channel(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    ch_key = cb.data.replace("select_", "")
    ch = CHANNELS[ch_key]
    d = ud(user_id)
    d['pack'] = '1_kanal'
    d['stage'] = 'wait_1'
    d['channel'] = ch_key
    d['channel_name'] = ch['name']
    d['channel_price'] = ch['price']
    
    track_user_action(user_id, f"channel_selected_{ch_key}")
    
    price_str = fmt(ch['price'])
    if lang == 'kz':
        text = (f"📱 *{ch['name']} — {price_str} тг. Растайсыз ба?*\n\n"
                f"💳 Kaspi картасына аудар:\n`{KASPI_NUMBER}`\n"
                f"👤 *{KASPI_NAME}*\n\n"
                f"📸 Чекті осы чатқа жібер! 🔥")
    else:
        text = (f"📱 *{ch['name']} — {price_str} тг. Подтверждаете?*\n\n"
                f"💳 Переведи на Kaspi:\n`{KASPI_NUMBER}`\n"
                f"👤 *{KASPI_NAME}*\n\n"
                f"📸 Скинь чек сюда! 🔥")
    await cb.message.edit_text(text, reply_markup=get_back_kb(lang), parse_mode="Markdown")
    await cb.answer()

# ─── RECEIPT HANDLER ─────────────────────────────────────────────────────────

@dp.message(F.photo | F.document)
async def handle_receipt(message: types.Message):
    user_id = message.from_user.id
    
    # Anti-spam check
    if is_spam(user_id):
        await message.answer("⏳ Пожалуйста, не спамьте! Подождите немного.")
        return
    
    user_messages[user_id].append(time.time())
    
    d = ud(user_id)
    lang = d.get('lang', 'kz')
    stage = d.get('stage', 'wait_1')
    pack = d.get('pack', '1_kanal')
    ch_name = d.get('channel_name', '—')
    ch_price = d.get('channel_price', 0)
    price_str = fmt(ch_price)
    
    track_user_action(user_id, "receipt_sent")
    logger.info(f"[RECEIPT] User {user_id} sent receipt for {ch_name} ({ch_price} тг)")
    
    # Helper to send photo or doc to admin
    async def send_to_admin(caption, kb):
        if message.photo:
            await bot.send_photo(ADMIN_ID, message.photo[-1].file_id, caption=caption, reply_markup=kb)
        else:
            await bot.send_document(ADMIN_ID, message.document.file_id, caption=caption, reply_markup=kb)
    
    user_ack = "✅ Чек қабылданды! Тексеріп жатырмын..." if lang == 'kz' else "✅ Чек принят! Проверяю..."
    await message.answer(user_ack)
    
    # ── VIP ОБЗОР ──
    if pack == 'vip_obzor' or stage == 'wait_vip':
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="✅ VIP ОБЗОР Растау", callback_data=f"confvip_{user_id}"))
        kb.row(types.InlineKeyboardButton(text="❌ Бас тарту", callback_data=f"rej_{user_id}"))
        cap = (f"🔔 ЧЕК — VIP ОБЗОР\n"
               f"👤 User ID: {user_id}\n"
               f"👤 Username: @{message.from_user.username or 'None'}\n"
               f"💰 290 тг\n"
               f"📌 User wants VIP ОБЗОР for 290 тг. Confirm receipt?")
        await send_to_admin(cap, kb.as_markup())
        return
    
    # ── Commission (2nd payment) ──
    if stage == 'wait_2':
        comm = "1 777 тг" if pack == '8_kanal' else "1 555 тг"
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="✅ Комиссия Растау", callback_data=f"conf2_{user_id}"))
        kb.row(types.InlineKeyboardButton(text="❌ Бас тарту", callback_data=f"rej_{user_id}"))
        cap = (f"🔔 ЧЕК — КОМИССИЯ\n"
               f"👤 User ID: {user_id}\n"
               f"👤 Username: @{message.from_user.username or 'None'}\n"
               f"💰 {comm}")
        await send_to_admin(cap, kb.as_markup())
        return
    
    # ── Main payment (1st payment) ──
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="✅ Төлемді Растау", callback_data=f"conf1_{user_id}"))
    kb.row(types.InlineKeyboardButton(text="❌ Бас тарту", callback_data=f"rej_{user_id}"))
    cap = (f"🔔 ЧЕК — НЕГІЗГІ ТӨЛЕМ\n"
           f"👤 User ID: {user_id}\n"
           f"👤 Username: @{message.from_user.username or 'None'}\n"
           f"📺 Канал: {ch_name}\n"
           f"💰 {price_str} тг\n"
           f"📌 User wants {ch_name} for {price_str} тг. Confirm receipt?")
    await send_to_admin(cap, kb.as_markup())

# ─── ADMIN CONFIRMATIONS ──────────────────────────────────────────────────────

# VIP ОБЗОР confirmed
@dp.callback_query(F.data.startswith("confvip_"))
async def conf_vip(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    ud(user_id)['stage'] = 'done'
    vip_obzor_users.add(user_id)
    
    track_user_action(user_id, "vip_paid")
    logger.info(f"[PAYMENT] User {user_id} paid for VIP ОБЗОР")
    
    if lang == 'kz':
        msg = (f"🎉 *VIP ОБЗОР расталды!*\n\n"
               f"Міне жабық канал:\n{VIP_OBZOR['link']} 🔥\n\n"
               f"Рахаттан жаным! 💋\n\n"
               f"⭐ *Кеңес:* Каналға кіргеннен кейін пинді хабарламаны оқы!")
    else:
        msg = (f"🎉 *VIP ОБЗОР подтверждён!*\n\n"
               f"Вот закрытый канал:\n{VIP_OBZOR['link']} 🔥\n\n"
               f"Наслаждайся зай! 💋\n\n"
               f"⭐ *Совет:* После входа в канал прочитай закрепленное сообщение!")
    
    await bot.send_message(user_id, msg, parse_mode="Markdown")
    
    try:
        await cb.message.edit_caption("✅ VIP ОБЗОР расталды")
    except Exception:
        await cb.message.edit_text("✅ VIP ОБЗОР расталды")

# Main payment confirmed → send links + ask commission
@dp.callback_query(F.data.startswith("conf1_"))
async def conf1(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    d = ud(user_id)
    lang = d.get('lang', 'kz')
    pack = d.get('pack', '1_kanal')
    d['stage'] = 'wait_2'
    
    track_user_action(user_id, "first_payment_confirmed")
    logger.info(f"[PAYMENT] User {user_id} first payment confirmed for {pack}")
    
    if pack == '8_kanal':
        links = ("🎁 КАНАЛДАР:\n" if lang == 'kz' else "🎁 КАНАЛЫ:\n") + \
                "\n".join([f"🔗 {v['name']}: {v['link']}" for v in CHANNELS.values()])
        comm = "1 777"
    else:
        ch = CHANNELS.get(d.get('channel', 'ch_1'), CHANNELS['ch_1'])
        links = (f"🎁 КАНАЛЫҢЫЗ:\n🔗 {ch['name']}: {ch['link']}" if lang == 'kz'
                 else f"🎁 ТВОЙ КАНАЛ:\n🔗 {ch['name']}: {ch['link']}")
        comm = "1 555"
    
    if lang == 'kz':
        text = (f"✅ *Бірінші төлем расталды!*\n\n{links}\n\n"
                f"━━━━━━━━━━━━━━━\n"
                f"⚠️ *СОҢҒЫ ҚАДАМ — {comm} тг*\n"
                f"━━━━━━━━━━━━━━━\n\n"
                f"Бұл бір рет қана жасалатын комиссия. Барлық клиент осылай алады!\n\n"
                f"💳 Kaspi:\n`{KASPI_NUMBER}`\n👤 *{KASPI_NAME}*\n\n"
                f"📸 Чекті осы чатқа жібер!\n\n"
                f"💡 *ЕСКЕРТУ:* Комиссия төленгеннен кейін толық доступ аласың!")
    else:
        text = (f"✅ *Первый платеж подтверждён!*\n\n{links}\n\n"
                f"━━━━━━━━━━━━━━━\n"
                f"⚠️ *ПОСЛЕДНИЙ ШАГ — {comm} тг*\n"
                f"━━━━━━━━━━━━━━━\n\n"
                f"Это разовая комиссия. Все клиенты так покупают!\n\n"
                f"💳 Kaspi:\n`{KASPI_NUMBER}`\n👤 *{KASPI_NAME}*\n\n"
                f"📸 Скинь чек сюда!\n\n"
                f"💡 *ВНИМАНИЕ:* После оплаты комиссии получишь полный доступ!")
    
    await bot.send_message(user_id, text, parse_mode="Markdown")
    
    try:
        await cb.message.edit_caption("✅ Расталды, ссылка жіберілді")
    except Exception:
        await cb.message.edit_text("✅ Расталды, ссылка жіберілді")

# Commission confirmed → fully paid
@dp.callback_query(F.data.startswith("conf2_"))
async def conf2(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    d = ud(user_id)
    d['stage'] = 'done'
    paid_users.add(user_id)
    
    track_user_action(user_id, "fully_paid")
    logger.info(f"[PAYMENT] User {user_id} fully paid, total sales: {calculate_total_sales()} тг")
    
    # Send welcome message to main channel if configured
    try:
        main_chat_id = -1001234567890  # Replace with your main group ID if needed
        # await bot.send_message(main_chat_id, f"🎉 Новый участник! @{message.from_user.username} присоединился!")
    except Exception as e:
        logger.error(f"[WELCOME] Could not send welcome: {e}")
    
    if lang == 'kz':
        msg = (f"🎉 *Құттықтаймын!* Комиссия қабылданды.\n\n"
               f"✅ Енді сізде толық доступ!\n"
               f"📱 Барлық каналдарға кіре аласыз.\n\n"
               f"💋 Рахаттана көріңіз! Сұрақтар болса — жазыңыз!")
    else:
        msg = (f"🎉 *Поздравляем!* Комиссия принята.\n\n"
               f"✅ Теперь у вас полный доступ!\n"
               f"📱 Можете заходить во все каналы.\n\n"
               f"💋 Наслаждайтесь! Если есть вопросы — пишите!")
    
    await bot.send_message(user_id, msg, parse_mode="Markdown")
    
    try:
        await cb.message.edit_caption("✅ Толық расталды")
    except Exception:
        await cb.message.edit_text("✅ Толық расталды")

# Reject
@dp.callback_query(F.data.startswith("rej_"))
async def rej(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    
    logger.info(f"[REJECT] Payment rejected for user {user_id}")
    
    if lang == 'kz':
        msg = ("❌ Кешіріңіз, төлеміңіз расталмады.\n\n"
               "💡 *Себептері:*\n"
               "• Чек нашар көрінеді\n"
               "• Сома дұрыс емес\n"
               "• Қайта жіберіңіз, біз тексереміз\n\n"
               "📸 Жаңа чекті жіберіңіз!")
    else:
        msg = ("❌ Извини, платеж не подтвержден.\n\n"
               "💡 *Причины:*\n"
               "• Чек плохо видно\n"
               "• Сумма не совпадает\n"
               "• Отправьте заново, мы проверим\n\n"
               "📸 Отправьте новый чек!")
    
    await bot.send_message(user_id, msg, parse_mode="Markdown")
    
    try:
        await cb.message.edit_caption("❌ Бас тартылды")
    except Exception:
        await cb.message.edit_text("❌ Бас тартылды")

# Discount offer (admin reply)
@dp.callback_query(F.data.startswith("offeryes_"))
async def offer_yes(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    
    logger.info(f"[DISCOUNT] Admin approved discount for user {user_id}")
    
    if lang == 'kz':
        await bot.send_message(
            user_id,
            f"✅ Жарайды жаным, сол соманы жібер!\n💳 Kaspi: `{KASPI_NUMBER}`\n\n👤 *{KASPI_NAME}*",
            parse_mode="Markdown"
        )
    else:
        await bot.send_message(
            user_id,
            f"✅ Хорошо зай, скидывай!\n💳 Kaspi: `{KASPI_NUMBER}`\n\n👤 *{KASPI_NAME}*",
            parse_mode="Markdown"
        )
    
    try:
        await cb.message.edit_text("✅ Жеңілдікке рұқсат берілді.")
    except Exception:
        pass

@dp.callback_query(F.data.startswith("offerno_"))
async def offer_no(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    
    logger.info(f"[DISCOUNT] Admin rejected discount for user {user_id}")
    
    if lang == 'kz':
        await bot.send_message(
            user_id,
            "❌ Жоқ жаным, ренжіме.\n\nТолық төлемнен кейін ғана кіре аласың.\n💰 Толық баға: *3333 тг*",
            parse_mode="Markdown"
        )
    else:
        await bot.send_message(
            user_id,
            "❌ Нет зай, извини.\n\nДоступ только после полной оплаты.\n💰 Полная цена: *3333 тг*",
            parse_mode="Markdown"
        )
    
    try:
        await cb.message.edit_text("❌ Жеңілдіктен бас тартылды.")
    except Exception:
        pass

# ─── JOIN REQUEST AUTO-APPROVE ────────────────────────────────────────────────

@dp.chat_join_request()
async def handle_join_request(request: ChatJoinRequest):
    user_id = request.from_user.id
    chat_id = request.chat.id
    chat_title = request.chat.title
    
    if user_id in paid_users or user_id in vip_obzor_users:
        try:
            await bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
            logger.info(f"[JOIN] ✅ Approved {user_id} → {chat_title} ({chat_id})")
            
            # Send welcome message to user
            try:
                await bot.send_message(
                    user_id,
                    f"🎉 *Доступ открыт!*\n\n"
                    f"✅ Вы вошли в канал: *{chat_title}*\n"
                    f"💋 Приятного просмотра!",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
                
        except Exception as e:
            logger.error(f"[JOIN] ❌ Error approving {user_id}: {e}")
    else:
        logger.warning(f"[JOIN] ⛔ Not paid — {user_id} tried to join {chat_title} ({chat_id})")
        
        # Notify user they need to pay
        try:
            lang = ud(user_id).get('lang', 'kz')
            if lang == 'kz':
                msg = (f"⚠️ *Кіруге рұқсат жоқ!*\n\n"
                       f"Бұл каналға кіру үшін алдымен төлем жасау керек.\n"
                       f"💎 Пакетті таңдап, төлеңіз: /start")
            else:
                msg = (f"⚠️ *Доступ запрещен!*\n\n"
                       f"Для доступа к этому каналу сначала нужно оплатить.\n"
                       f"💎 Выберите пакет и оплатите: /start")
            
            await bot.send_message(user_id, msg, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"[JOIN] Could not notify user {user_id}: {e}")

# ─── AI CHAT (GEMINI) ─────────────────────────────────────────────────────────

@dp.message()
async def ai_handler(message: types.Message):
    # Ignore admin messages and non-text messages
    if message.from_user.id == ADMIN_ID or not message.text:
        return
    
    user_id = message.from_user.id
    
    # Anti-spam check
    if is_spam(user_id):
        await message.answer("⏳ Жаным, күте тұршы... 💋 Неге көп жазып жатырсың?")
        return
    
    user_messages[user_id].append(time.time())
    
    d = ud(user_id)
    lang = d.get('lang', 'kz')
    stage = d.get('stage', 'start')
    pack = d.get('pack', '1_kanal')
    history = d.setdefault('history', [])
    reply_kb = get_main_kb(lang)
    
    track_user_action(user_id, "message_sent")
    d['command_count'] = d.get('command_count', 0) + 1
    
    # Check for price negotiation keywords
    price_keywords = ['скидка', 'дешевле', 'дорого', 'денег нет', 'акция', 'бонус', 'скидк', 'жеңілдік', 'арзан', 'қымбат', 'ақшам жоқ']
    if any(kw in message.text.lower() for kw in price_keywords) and stage == 'wait_2':
        # Handle discount requests
        await message.answer(
            "😏 Жаным, бұл қазірдің өзінде скидка!\n\n"
            "Егер шынымен қиын болса, қанша сала аласың?\n"
            "Айт, көрейік 💋" if lang == 'kz'
            else "😏 Зай, это уже со скидкой!\n\n"
            "Если совсем тяжело, сколько можешь скинуть?\n"
            "Скажи, посмотрим 💋"
        )
        return
    
    # Channel content triggers — show quick info without AI
    triggers = ["не бар", "не кіреді", "қандай", "кандай", "ішінде", "ышынде",
                "что внутри", "какие каналы", "что входит", "внутри", "нелер бар", 
                "покажи", "контент", "видео", "фото"]
    
    if stage not in ('wait_2', 'done', 'wait_vip') and any(w in message.text.lower() for w in triggers):
        lines = "\n".join([f"🔥 {v['name']} — {fmt(v['price'])} тг" for v in CHANNELS.values()])
        if lang == 'kz':
            text = (f"✨ *Іште не бар?* ✨\n\n"
                    f"{lines}\n\n"
                    f"👇 Қазір төле, бірден кір! 💋")
        else:
            text = (f"✨ *Что внутри?* ✨\n\n"
                    f"{lines}\n\n"
                    f"👇 Оплачивай сейчас и заходи! 💋")
        
        await message.answer(text, parse_mode="Markdown", reply_markup=reply_kb)
        return
    
    # Build dynamic context based on user stage
    comm = "1 777" if pack == '8_kanal' else "1 555"
    lang_rule = ("ҚАЗАҚША сөйле. 'Жаным', 'Күнім' де." if lang == 'kz'
                 else "Говори строго НА РУССКОМ. 'Зай', 'Милый' де.")
    
    if stage == 'done':
        dynamic = "Клиент төледі. Ештеңе сатпа. Еркін флиртпен сөйлес, дос болып қал."
        reply_kb = None
    elif stage == 'wait_vip':
        dynamic = "Клиент VIP ОБЗОР чекін күтуде. Жылы сөзбен шақыр, шыдамды бол."
        reply_kb = None
    elif stage == 'wait_2':
        if lang == 'kz':
            dynamic = (f"ЕКІНШІ ТӨЛЕМ ({comm} тг) күтілуде. "
                       f"Скидка десе: 'Бұл қазірдің өзінде скидка'. "
                       f"Ақшам жоқ десе: 'Қанша сала аласың?' деп сұра. "
                       f"Нақты сома айтса: [АДМИН] деп қос. "
                       f"Манипуляция жаса: 'Барлығы осылай өтті', 'Бүгін соңғы күн'.")
        else:
            dynamic = (f"ЖДЕМ ВТОРОЙ ПЛАТЕЖ ({comm} тг). "
                       f"Скидка: 'Это уже со скидкой'. "
                       f"Нет денег: 'Сколько можешь скинуть?'. "
                       f"Конкретная сумма: добавь [АДМИН]. "
                       f"Манипулируй: 'Все так проходят', 'Сегодня последний день'.")
        reply_kb = None
    else:
        dynamic = ("Екінші төлем туралы сұраса — мүлдем жоқ де."
                   if lang == 'kz' else
                   "Про второй платеж — скажи что нет.")
    
    full_prompt = f"{SYSTEM_PROMPT} {lang_rule}\nМАҢЫЗДЫ: {dynamic}"
    
    # Send "Thinking..." message
    thinking_msg = await message.answer(
        "💭 Ойланып жатырмын..." if lang == 'kz' else "💭 Думаю..."
    )
    
    # Call Gemini API
    response = await call_gemini(full_prompt, message.text, history)
    
    # Delete "Thinking..." message
    try:
        await thinking_msg.delete()
    except Exception:
        pass
    
    if response:
        # Update conversation history
        history.append({"role": "user", "content": message.text})
        history.append({"role": "assistant", "content": response})
        
        # Keep only last 15 messages to avoid token limits
        if len(history) > 15:
            d['history'] = history[-15:]
        
        # Check if admin approval needed for discount
        if "[АДМИН]" in response:
            response = response.replace("[АДМИН]", "").strip()
            akb = InlineKeyboardBuilder()
            akb.add(types.InlineKeyboardButton(text="✅ Иә / Да", callback_data=f"offeryes_{user_id}"))
            akb.add(types.InlineKeyboardButton(text="❌ Жоқ / Нет", callback_data=f"offerno_{user_id}"))
            
            await bot.send_message(
                ADMIN_ID,
                f"🔔 *СКИДКА СҰРАУ / ПРОСЬБА СКИДКИ*\n\n"
                f"👤 ID: `{user_id}`\n"
                f"👤 Username: @{message.from_user.username or 'None'}\n"
                f"💬 Сообщение: {message.text[:200]}\n\n"
                f"💰 Текущий пакет: {d.get('pack', 'unknown')}\n"
                f"📊 Этап: {stage}",
                parse_mode="Markdown",
                reply_markup=akb.as_markup()
            )            logger.info(f"[ADMIN] Discount request from user {user_id}")
        
        # Send AI response
        try:
            await message.answer(response, reply_markup=reply_kb)
            track_user_action(user_id, "ai_response_received")
        except Exception as e:
            logger.error(f"[AI] Error sending response: {e}")
            fallback = get_fallback_response(lang)
            await message.answer(fallback, reply_markup=reply_kb)
    else:
        # Fallback if AI fails
        logger.warning(f"[AI] AI failed for user {user_id}, using fallback")
        fallback = get_fallback_response(lang)
        await message.answer(fallback, reply_markup=reply_kb)

# ─── ADDITIONAL COMMANDS AND HANDLERS ─────────────────────────────────────────

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    """Help command showing bot features."""
    user_id = message.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    
    if lang == 'kz':
        text = (
            "🆘 *КӨМЕК / HELP*\n\n"
            "🤖 Бот мүмкіндіктері:\n\n"
            "💎 *Пакеттер:*\n"
            "• 8 канал пакеті — 3,333 тг\n"
            "• 1 канал — өз бағасымен\n"
            "• VIP ОБЗОР — 290 тг\n\n"
            "🎁 *Тегін канал:*\n"
            "• 10 досыңды шақыр → тегін канал\n\n"
            "📌 *Командалар:*\n"
            "• /start — Бастау\n"
            "• /help — Көмек\n"
            "• /profile — Менің профиль\n"
            "• /support — Қолдау\n\n"
            "💬 *Сұрақтар болса* — жазыңыз!"
        )
    else:
        text = (
            "🆘 *ПОМОЩЬ / HELP*\n\n"
            "🤖 Возможности бота:\n\n"
            "💎 *Пакеты:*\n"
            "• Пакет 8 каналов — 3,333 тг\n"
            "• 1 канал — по своей цене\n"
            "• VIP ОБЗОР — 290 тг\n\n"
            "🎁 *Бесплатный канал:*\n"
            "• Пригласи 10 друзей → бесплатный канал\n\n"
            "📌 *Команды:*\n"
            "• /start — Старт\n"
            "• /help — Помощь\n"
            "• /profile — Мой профиль\n"
            "• /support — Поддержка\n\n"
            "💬 *Если есть вопросы* — пишите!"
        )
    
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    """Show user profile and statistics."""
    user_id = message.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    stats = get_user_stats(user_id)
    
    # Calculate time since first seen
    first_seen = datetime.fromisoformat(stats['first_seen']) if stats['first_seen'] else datetime.now()
    days_since = (datetime.now() - first_seen).days
    
    if lang == 'kz':
        text = (
            f"👤 *Менің профилім*\n\n"
            f"🆔 ID: `{user_id}`\n"
            f"📅 Ботта: {days_since} күн\n"
            f"💬 Хабарламалар: {stats['messages_sent']}\n"
            f"✅ Төлем: {'Иә' if stats['is_paid'] else 'Жоқ'}\n"
            f"🔞 VIP ОБЗОР: {'Иә' if stats['has_vip'] else 'Жоқ'}\n"
            f"👥 Рефералдар: {stats['referrals']}\n"
            f"📍 Этап: {stats['current_stage']}\n\n"
            f"🎯 *Мақсат:* {REFERRAL_GOAL - stats['referrals']} реферал қалды тегін канал үшін!"
        )
    else:
        text = (
            f"👤 *Мой профиль*\n\n"
            f"🆔 ID: `{user_id}`\n"
            f"📅 В боте: {days_since} дней\n"
            f"💬 Сообщений: {stats['messages_sent']}\n"
            f"✅ Оплата: {'Да' if stats['is_paid'] else 'Нет'}\n"
            f"🔞 VIP ОБЗОР: {'Да' if stats['has_vip'] else 'Нет'}\n"
            f"👥 Рефералов: {stats['referrals']}\n"
            f"📍 Этап: {stats['current_stage']}\n\n"
            f"🎯 *Цель:* {REFERRAL_GOAL - stats['referrals']} рефералов до бесплатного канала!"
        )
    
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("support"))
async def cmd_support(message: types.Message):
    """Support command to contact admin."""
    user_id = message.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    
    if lang == 'kz':
        text = (
            "📞 *ҚОЛДАУ / SUPPORT*\n\n"
            "Сұрақтарыңыз болса, мына жерден жазыңыз:\n\n"
            "👤 *Админ:* @aisulu_support\n"
            "⏱ Жауап беру уақыты: 5-30 минут\n\n"
            "💡 *Жиі қойылатын сұрақтар:*\n"
            "• Қалай төлеу керек? → /help\n"
            "• Тегін канал қалай алуға болады? → /help\n"
            "• Чекті қайда жіберу керек? → Төлемнен кейін осы чатқа\n\n"
            "💬 *Сұрағыңызды жазыңыз, біз көмектесеміз!*"
        )
    else:
        text = (
            "📞 *ПОДДЕРЖКА / SUPPORT*\n\n"
            "Если есть вопросы, пишите сюда:\n\n"
            "👤 *Админ:* @aisulu_support\n"
            "⏱ Время ответа: 5-30 минут\n\n"
            "💡 *Частые вопросы:*\n"
            "• Как оплатить? → /help\n"
            "• Как получить бесплатный канал? → /help\n"
            "• Куда отправлять чек? → После оплаты в этот чат\n\n"
            "💬 *Напишите ваш вопрос, мы поможем!*"
        )
    
    await message.answer(text, parse_mode="Markdown")

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    """Show bot statistics (admin only)."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещен")
        return
    
    report = get_analytics_report()
    await message.answer(report, parse_mode="Markdown")

@dp.message(Command("broadcast"))
async def cmd_broadcast(message: types.Message):
    """Broadcast message to all users (admin only)."""
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещен")
        return
    
    # Check if message has text to broadcast
    if not message.reply_to_message or not message.reply_to_message.text:
        await message.answer("ℹ️ Ответьте на сообщение, которое хотите разослать, командой /broadcast")
        return
    
    broadcast_text = message.reply_to_message.text
    users_to_send = list(user_data.keys())
    
    success_count = 0
    fail_count = 0
    
    await message.answer(f"📢 Начинаю рассылку {len(users_to_send)} пользователям...")
    
    for user_id in users_to_send:
        try:
            await bot.send_message(user_id, broadcast_text, parse_mode="Markdown")
            success_count += 1
            await asyncio.sleep(0.05)  # Avoid hitting rate limits
        except Exception as e:
            fail_count += 1
            logger.error(f"[BROADCAST] Failed to send to {user_id}: {e}")
    
    await message.answer(f"✅ Рассылка завершена!\n\n✅ Успешно: {success_count}\n❌ Ошибок: {fail_count}")

# ─── ERROR HANDLERS ───────────────────────────────────────────────────────────

@dp.errors()
async def error_handler(update: types.Update, exception: Exception):
    """Global error handler for the bot."""
    logger.error(f"[ERROR] Update: {update}, Exception: {exception}", exc_info=True)
    
    # Notify admin about critical errors
    if ADMIN_ID:
        try:
            error_message = (
                f"⚠️ *Ошибка в боте!*\n\n"
                f"📌 Тип: {type(exception).__name__}\n"
                f"💬 Сообщение: {str(exception)[:200]}\n"
                f"🆔 Update: {update.update_id if update else 'Unknown'}"
            )
            await bot.send_message(ADMIN_ID, error_message, parse_mode="Markdown")
        except Exception as e:
            logger.error(f"[ERROR] Could not notify admin: {e}")
    
    return True

# ─── BACKGROUND TASKS ─────────────────────────────────────────────────────────

async def clean_old_data():
    """Clean old user data periodically."""
    while True:
        await asyncio.sleep(3600)  # Run every hour
        
        try:
            now = datetime.now()
            to_delete = []
            
            for user_id, data in user_data.items():
                last_active = data.get('last_active')
                if last_active:
                    last_time = datetime.fromisoformat(last_active)
                    # Delete users inactive for more than 30 days
                    if (now - last_time).days > 30:
                        to_delete.append(user_id)
            
            for user_id in to_delete:
                if user_id in user_data:
                    del user_data[user_id]
                if user_id in user_messages:
                    del user_messages[user_id]
                logger.info(f"[CLEAN] Removed inactive user {user_id}")
            
            if to_delete:
                logger.info(f"[CLEAN] Cleaned {len(to_delete)} inactive users")
                
        except Exception as e:
            logger.error(f"[CLEAN] Error in clean_old_data: {e}")

async def backup_data():
    """Backup user data periodically."""
    while True:
        await asyncio.sleep(86400)  # Run every day
        
        try:
            backup_file = f"backup_{datetime.now().strftime('%Y%m%d')}.json"
            backup_data = {
                "user_data": {str(k): v for k, v in user_data.items()},
                "paid_users": list(paid_users),
                "vip_obzor_users": list(vip_obzor_users),
                "referral_data": {str(k): list(v) for k, v in referral_data.items()},
                "timestamp": datetime.now().isoformat()
            }
            
            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"[BACKUP] Created backup: {backup_file}")
            
            # Keep only last 7 backups
            import glob
            backups = sorted(glob.glob("backup_*.json"))
            for old_backup in backups[:-7]:
                os.remove(old_backup)
                logger.info(f"[BACKUP] Removed old backup: {old_backup}")
                
        except Exception as e:
            logger.error(f"[BACKUP] Error in backup_data: {e}")

async def update_analytics():
    """Update analytics data periodically."""
    while True:
        await asyncio.sleep(3600)  # Run every hour
        
        try:
            total_users = len(user_data)
            total_paid = len(paid_users)
            total_sales = calculate_total_sales()
            
            logger.info(f"[ANALYTICS] Hourly stats - Users: {total_users}, Paid: {total_paid}, Sales: {total_sales} тг")
            
            # Save to file
            stats_file = f"analytics_{datetime.now().strftime('%Y%m%d')}.json"
            stats_data = {
                "date": datetime.now().isoformat(),
                "total_users": total_users,
                "total_paid": total_paid,
                "total_vip": len(vip_obzor_users),
                "total_sales": total_sales,
                "avg_check": total_sales // (total_paid + len(vip_obzor_users) or 1)
            }
            
            # Append to daily log
            with open(stats_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(stats_data, ensure_ascii=False) + "\n")
                
        except Exception as e:
            logger.error(f"[ANALYTICS] Error in update_analytics: {e}")

# ─── MAIN FUNCTION ────────────────────────────────────────────────────────────

async def main():
    """Main function to start the bot."""
    # Print startup banner
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  АЙСҰЛУ БОТ ІСКЕ ҚОСЫЛДЫ ✅")
    logger.info("  Flask keep-alive: http://0.0.0.0:8000/")
    logger.info("  Gemini Model: gemini-1.5-flash")
    logger.info(f"  Bot username: {(await bot.get_me()).username}")
    logger.info(f"  Total channels: {len(CHANNELS)}")
    logger.info(f"  Admin ID: {ADMIN_ID}")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    # Start background tasks
    asyncio.create_task(self_ping_loop())
    asyncio.create_task(fake_activity_loop(bot))
    asyncio.create_task(clean_old_data())
    asyncio.create_task(backup_data())
    asyncio.create_task(update_analytics())
    
    # Set bot commands
    await bot.set_my_commands([
        types.BotCommand(command="start", description="🚀 Бастау / Старт"),
        types.BotCommand(command="help", description="🆘 Көмек / Помощь"),
        types.BotCommand(command="profile", description="👤 Мой профиль / Мой профиль"),
        types.BotCommand(command="support", description="📞 Қолдау / Поддержка"),
    ])
    
    # Start bot polling
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"[MAIN] Fatal error in polling: {e}", exc_info=True)
        raise

# ─── ENTRY POINT ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Start Flask keep-alive server
    keep_alive()
    
    # Run bot with proper error handling
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот тоқтатылды! (Ctrl+C)")
    except SystemExit:
        logger.info("Бот тоқтатылды! (SystemExit)")
    except Exception as e:
        logger.error(f"Бот қатемен тоқтады: {e}", exc_info=True)
        raise
    finally:
        # Cleanup
        logger.info("Бот жұмысын аяқтады. До свидания! 👋")
