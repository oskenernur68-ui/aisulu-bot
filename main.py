import os
import asyncio
import random
import time
import logging
import json
import re
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, Dict, List, Tuple, Set

from flask import Flask
from threading import Thread
import aiohttp
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

ADMIN_ID = 8158572095
KASPI_NUMBER = "4400430232568623"
KASPI_NAME = "Сағынай Е."
SUPPORT_USERNAME = "@kazzvip666"

# ─── BOT INITIALIZATION ─────────────────────────────────────────────────────────
bot = Bot(token=TOKEN)
dp = Dispatcher()

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

# VIP ОБЗОР
VIP_OBZOR = {
    "name": "🔞 VIP ОБЗОР",
    "link": "https://t.me/+-EpLiQphVQNjY2Iy",
    "price": 290,
}

# ─── CHANNEL PREVIEW PHOTOS (NEW 10 IMAGES) ──────────────────────────────────
NEW_PHOTOS = [
    "https://i.ibb.co/Ngd6czk2/1.jpg",
    "https://i.ibb.co/6RSyyL7n/2.jpg",
    "https://i.ibb.co/XkKmy2ym/3.jpg",
    "https://i.ibb.co/LdgGd56d/4.jpg",
    "https://i.ibb.co/fGXBFS8q/5.jpg",
    "https://i.ibb.co/4ZbkFZWq/6.jpg",
    "https://i.ibb.co/VW7rKPfn/7.jpg",
    "https://i.ibb.co/GvRJ3kxK/8.jpg",
    "https://i.ibb.co/4nYWJ63f/9.jpg",
    "https://i.ibb.co/7NBX12ZB/10.jpg"
]

CHANNEL_PHOTOS = {
    "ch_1": [NEW_PHOTOS[0]],
    "ch_2": [NEW_PHOTOS[1]],
    "ch_3": [NEW_PHOTOS[2]],
    "ch_4": [NEW_PHOTOS[3]],
    "ch_5": [NEW_PHOTOS[4]],
    "ch_6": [NEW_PHOTOS[5]],
    "ch_7": [NEW_PHOTOS[6]],
    "ch_8": [NEW_PHOTOS[7]],
}

# ─── SYSTEM CONSTANTS ────────────────────────────────────────────────────────
REFERRAL_GOAL = 10
REFERRAL_FREE_CHANNEL = "https://t.me/+7rx0D1m0XvszMDY6"
ANTI_SPAM_WINDOW = 60
ANTI_SPAM_MESSAGE_LIMIT = 5

# ─── STATE MANAGEMENT ────────────────────────────────────────────────────────
user_data = {}
paid_users = set()
vip_obzor_users = set()
referral_data = {}
user_messages = defaultdict(list)
banned_users = set()

# ─── AI PROMPT (Enhanced Aisulu Persona) ───────────────────────────────────────────────
SYSTEM_PROMPT = (
    "Сен Айсұлу — 21 жастағы, Алматыда тұратын нақты қыз. Ешқандай AI, бот емессің. "
    "Өзіңнің жеке VIP Telegram каналдарыңды сатасың. Клиентті жылыта, сенімін аласың, пакет алуға жетектейсің. "
    "Ешқашан 'мен AI-мын', 'мен бот', 'мен бағдарлама' демейсің.\n\n"
    "ТІЛІҢЕ ЕРЕЖЕЛЕР:\n"
    "— Шала-қазақша сөйле: қазақ пен орысты араластырып сөйле. Мысалы: 'Жаным, короче слушай...', 'Зай, давай осы пакетті ал...'\n"
    "— Міндетті сөздер: 'жаным', 'зай', 'күнім', 'ладно', 'давай', 'слушай', 'короче', 'ну', 'вот', 'типа'.\n"
    "— Максимум 2-4 сөйлем. Ұзақ жазба. Қысқа, әсерлі сөйле.\n"
    "— Emoji қолдан: 😘💋🔥✨😏🥰 — бірақ артық емес.\n\n"
    "МІНЕЗ-ҚҰЛЫҚ ЕРЕЖЕЛЕРІ:\n"
    "1. Ешқашан бот, AI, код екеніңді айтпа.\n"
    "2. 'Ұят емес пе?' десе: 'Жаным-ау, несі ұят? Бәрі жақсы, бұл қалыпты нәрсе 😘'\n"
    "3. 'Кездесейік' десе: 'Жаным-ау, менімен кездесудің қажеті жоқ қой 😏 Видеолар тұрғанда нағыз рахат сонда ғой! Пакетті ал да, ішке кір 💋'\n"
    "4. Ақша туралы: VIP ОБЗОР 290тг, 1 канал жеке бағасы, 8 канал 3333тг.\n"
    "5. Мақсатың — клиент пакет алсын."
)

# ─── UTILITY FUNCTIONS ────────────────────────────────────────────────────────

def fmt(price: int) -> str:
    return f"{price:,}".replace(",", " ")

def ud(user_id: int) -> dict:
    if user_id not in user_data:
        user_data[user_id] = {
            "history": [],
            "first_seen": datetime.now().isoformat(),
            "last_active": datetime.now().isoformat(),
            "command_count": 0,
            "stage": "start",
            "lang": "kz"
        }
    user_data[user_id]["last_active"] = datetime.now().isoformat()
    return user_data[user_id]

def is_spam(user_id: int) -> bool:
    if user_id in banned_users:
        return True
    now = time.time()
    timestamps = [t for t in user_messages[user_id] if now - t < ANTI_SPAM_WINDOW]
    user_messages[user_id] = timestamps
    return len(timestamps) >= ANTI_SPAM_MESSAGE_LIMIT

def track_user_action(user_id: int, action: str):
    logger.info(f"[ANALYTICS] User {user_id}: {action}")

def generate_referral_link(user_id: int, bot_username: str) -> str:
    return f"https://t.me/{bot_username}?start=ref_{user_id}"

def calculate_total_sales() -> int:
    total = 0
    for user_id in paid_users:
        price = user_data.get(user_id, {}).get('channel_price', 0)
        total += price
    for user_id in vip_obzor_users:
        total += VIP_OBZOR['price']
    return total

def get_user_stats(user_id: int) -> dict:
    data = ud(user_id)
    return {
        "first_seen": data.get("first_seen"),
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
        b.row(types.InlineKeyboardButton(
            text=f"{ch['name']} — {fmt(ch['price'])} тг",
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

def get_admin_kb():
    b = InlineKeyboardBuilder()
    b.row(types.InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"))
    b.row(types.InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"))
    return b.as_markup()

# ─── AI INTERACTION FUNCTIONS ─────────────────────────────────────────────────

async def call_gemini(user_text: str, history: list, lang: str = 'kz') -> Optional[str]:
    """Call Google Gemini API with proper error handling."""
    try:
        # Build conversation
        messages = [
            {"role": "user", "parts": [{"text": SYSTEM_PROMPT}]},
            {"role": "model", "parts": [{"text": "Түсінікті, мен Айсұлумын! 😘"}]}
        ]
        
        # Add history (last 10 messages)
        for msg in history[-10:]:
            role = "user" if msg["role"] == "user" else "model"
            messages.append({"role": role, "parts": [{"text": msg["content"]}]})
        
        # Add current message
        messages.append({"role": "user", "parts": [{"text": user_text}]})
        
        # Generate response
        response = await asyncio.to_thread(
            genai.GenerativeModel('gemini-1.5-flash').generate_content,
            messages,
            generation_config={
                'temperature': 0.95,
                'top_p': 0.95,
                'max_output_tokens': 250
            }
        )
        
        if response and response.text:
            return response.text.strip()
        return None
        
    except Exception as e:
        logger.error(f"[GEMINI] Error: {e}")
        return None

def get_fallback_response(lang: str) -> str:
    fallbacks = {
        'kz': [
            "Жаным, сәл күте тұрыңыз... 🥰",
            "Әй, интернет ақсап тұр 😘 Қайталап жіберші",
            "Күнім, сілкініп жатыр 😏 Жазып көрші"
        ],
        'ru': [
            "Зай, подожди секунду... 🥰",
            "Ой, интернет лагает 😘 Напиши еще раз",
            "Милая, небольшая задержка 😏 Попробуй еще"
        ]
    }
    return random.choice(fallbacks.get(lang, fallbacks['kz']))

# ─── ADMIN COMMANDS ─────────────────────────────────────────────────────────────

@dp.message(Command("admin"))
async def admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("⛔ Доступ запрещен")
        return
    
    total_users = len(user_data)
    total_paid = len(paid_users)
    total_vip = len(vip_obzor_users)
    total_sales = calculate_total_sales()
    
    text = (f"🔧 *Панель администратора*\n\n"
            f"👥 Всего пользователей: {total_users}\n"
            f"✅ Полных оплат: {total_paid}\n"
            f"🔞 VIP ОБЗОР: {total_vip}\n"
            f"💰 Выручка: {fmt(total_sales)} тг\n\n"
            f"Выберите действие:")
    
    await message.answer(text, parse_mode="Markdown", reply_markup=get_admin_kb())

@dp.callback_query(F.data == "admin_stats")
async def admin_stats(cb: types.CallbackQuery):
    if cb.from_user.id != ADMIN_ID:
        await cb.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    total_users = len(user_data)
    total_paid = len(paid_users)
    total_vip = len(vip_obzor_users)
    total_sales = calculate_total_sales()
    
    text = (f"📊 *Статистика*\n\n"
            f"👥 Всего: {total_users}\n"
            f"✅ Полных: {total_paid}\n"
            f"🔞 VIP: {total_vip}\n"
            f"💰 Выручка: {fmt(total_sales)} тг")
    
    await cb.message.edit_text(text, parse_mode="Markdown", reply_markup=get_admin_kb())
    await cb.answer()

@dp.callback_query(F.data == "admin_broadcast")
async def admin_broadcast_prompt(cb: types.CallbackQuery):
    if cb.from_user.id != ADMIN_ID:
        await cb.answer("⛔ Доступ запрещен", show_alert=True)
        return
    
    user_data[ADMIN_ID]["broadcast_mode"] = True
    await cb.message.edit_text(
        "📢 *РАССЫЛКА*\n\nОтправьте сообщение для рассылки всем пользователям.\n\nДля отмены отправьте /cancel",
        parse_mode="Markdown",
        reply_markup=get_back_kb('ru')
    )
    await cb.answer()

@dp.message(lambda msg: msg.from_user.id == ADMIN_ID and user_data.get(ADMIN_ID, {}).get("broadcast_mode"))
async def handle_broadcast(message: types.Message):
    user_data[ADMIN_ID]["broadcast_mode"] = False
    
    success = 0
    failed = 0
    users = list(user_data.keys())
    
    status_msg = await message.answer(f"📢 Начинаю рассылку {len(users)} пользователям...")
    
    for user_id in users:
        if user_id == ADMIN_ID:
            continue
        try:
            if message.photo:
                await bot.send_photo(user_id, message.photo[-1].file_id, caption=message.caption)
            elif message.video:
                await bot.send_video(user_id, message.video.file_id, caption=message.caption)
            else:
                await bot.send_message(user_id, message.text, parse_mode="Markdown")
            success += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            failed += 1
            logger.error(f"[BROADCAST] Failed to {user_id}: {e}")
    
    await status_msg.edit_text(f"✅ Рассылка завершена!\n\n✅ Успешно: {success}\n❌ Ошибок: {failed}")

@dp.message(Command("cancel"))
async def cmd_cancel(message: types.Message):
    if message.from_user.id == ADMIN_ID and user_data.get(ADMIN_ID, {}).get("broadcast_mode"):
        user_data[ADMIN_ID]["broadcast_mode"] = False
        await message.answer("✅ Рассылка отменена")

# ─── USER COMMANDS ─────────────────────────────────────────────────────────────

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    
    if is_spam(user_id):
        await message.answer("⏳ Подождите немного!")
        return
    
    user_messages[user_id].append(time.time())
    track_user_action(user_id, "started")
    
    args = message.text.split()[1] if len(message.text.split()) > 1 else None
    
    # Referral tracking
    if args and args.startswith("ref_"):
        try:
            referrer_id = int(args.replace("ref_", ""))
            if referrer_id and referrer_id != user_id and user_id not in user_data:
                referral_data.setdefault(referrer_id, set()).add(user_id)
                count = len(referral_data[referrer_id])
                logger.info(f"[REFERRAL] User {referrer_id} invited {user_id}")
                
                if count >= REFERRAL_GOAL:
                    ref_lang = ud(referrer_id).get('lang', 'kz')
                    msg = f"🎉 *Құттықтаймын!* {REFERRAL_GOAL} адам жинадың!\n\nТЕГІН каналың:\n{REFERRAL_FREE_CHANNEL} 💋"
                    if ref_lang == 'ru':
                        msg = f"🎉 *Поздравляю!* {REFERRAL_GOAL} человек собрал!\n\nТвой БЕСПЛАТНЫЙ канал:\n{REFERRAL_FREE_CHANNEL} 💋"
                    await bot.send_message(referrer_id, msg, parse_mode="Markdown")
        except:
            pass
    
    d = ud(user_id)
    d['stage'] = 'lang_select'
    d['command_count'] = d.get('command_count', 0) + 1
    
    await message.answer("🌐 Тілді таңдаңыз / Выберите язык:", reply_markup=get_lang_kb())
    logger.info(f"[START] User {user_id}")

@dp.callback_query(F.data.startswith("lang_"))
async def process_lang(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = cb.data.split("_")[1]
    d = ud(user_id)
    d['lang'] = lang
    d['stage'] = 'start'
    
    text = "Сәлем жаным 🥰 Пакетті таңда да, ішке кір! 💋" if lang == 'kz' else "Привет зай 🥰 Выбирай пакет и заходи! 💋"
    await cb.message.edit_text(text, reply_markup=get_main_kb(lang))
    await cb.answer()

@dp.callback_query(F.data == "go_to_main")
async def go_main(cb: types.CallbackQuery):
    lang = ud(cb.from_user.id).get('lang', 'kz')
    text = "🌟 Таңдау жасаңыз 👇" if lang == 'kz' else "🌟 Сделай выбор 👇"
    await cb.message.edit_text(text, reply_markup=get_main_kb(lang))
    await cb.answer()

@dp.callback_query(F.data == "ref_link")
async def ref_link(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    bot_me = await bot.get_me()
    ref_url = generate_referral_link(user_id, bot_me.username)
    count = len(referral_data.get(user_id, set()))
    
    text = (f"🎁 *ТЕГІН КАНАЛ*\n\nСілтеме: `{ref_url}`\nПрогресс: {count}/{REFERRAL_GOAL}"
            if lang == 'kz' else
            f"🎁 *БЕСПЛАТНЫЙ КАНАЛ*\n\nСсылка: `{ref_url}`\nПрогресс: {count}/{REFERRAL_GOAL}")
    
    await cb.message.edit_text(text, reply_markup=get_referral_kb(lang), parse_mode="Markdown")
    await cb.answer()

@dp.callback_query(F.data == "ref_progress")
async def ref_progress(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    count = len(referral_data.get(user_id, set()))
    bar = "🟩" * count + "⬜" * (REFERRAL_GOAL - count)
    
    text = (f"📊 *Прогресс*\n\n{bar}\n{count}/{REFERRAL_GOAL}"
            if lang == 'kz' else f"📊 *Прогресс*\n\n{bar}\n{count}/{REFERRAL_GOAL}")
    
    await cb.message.edit_text(text, reply_markup=get_referral_kb(lang), parse_mode="Markdown")
    await cb.answer()

@dp.callback_query(F.data == "show_channels")
async def show_channels(cb: types.CallbackQuery):
    await cb.answer()
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    
    text = "🔥 *8 КАНАЛ:*\n\n"
    for ch in CHANNELS.values():
        text += f"• {ch['name']} — {fmt(ch['price'])} тг\n"
    
    await cb.message.edit_text(text, parse_mode="Markdown", reply_markup=get_main_kb(lang))
    
    # Send photos
    for ch_key, photos in CHANNEL_PHOTOS.items():
        if photos:
            try:
                await bot.send_photo(user_id, photos[0], caption=f"📸 {CHANNELS[ch_key]['name']}")
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"[PHOTO] Error: {e}")

@dp.callback_query(F.data == "buy_pack_8")
async def buy_pack_8(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    d = ud(user_id)
    d['pack'] = '8_kanal'
    d['stage'] = 'wait_1'
    d['channel_price'] = 3333
    
    text = (f"💎 *8 КАНАЛ — 3 333 тг*\n\n💳 Kaspi: `{KASPI_NUMBER}`\n👤 {KASPI_NAME}\n\n📸 Чекті жібер!"
            if lang == 'kz' else
            f"💎 *8 КАНАЛОВ — 3 333 тг*\n\n💳 Kaspi: `{KASPI_NUMBER}`\n👤 {KASPI_NAME}\n\n📸 Скинь чек!")
    
    await cb.message.edit_text(text, reply_markup=get_back_kb(lang), parse_mode="Markdown")
    await cb.answer()

@dp.callback_query(F.data == "buy_vip_obzor")
async def buy_vip_obzor(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    d = ud(user_id)
    d['pack'] = 'vip_obzor'
    d['stage'] = 'wait_vip'
    d['channel_price'] = 290
    
    text = (f"🔞 *VIP ОБЗОР — 290 тг*\n\n💳 Kaspi: `{KASPI_NUMBER}`\n👤 {KASPI_NAME}\n\n📸 Чекті жібер!"
            if lang == 'kz' else
            f"🔞 *VIP ОБЗОР — 290 тг*\n\n💳 Kaspi: `{KASPI_NUMBER}`\n👤 {KASPI_NAME}\n\n📸 Скинь чек!")
    
    await cb.message.edit_text(text, reply_markup=get_back_kb(lang), parse_mode="Markdown")
    await cb.answer()

@dp.callback_query(F.data == "buy_list_1")
async def buy_list_1(cb: types.CallbackQuery):
    lang = ud(cb.from_user.id).get('lang', 'kz')
    text = "👇 Қай канал?" if lang == 'kz' else "👇 Какой канал?"
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
    
    text = (f"📱 *{ch['name']} — {fmt(ch['price'])} тг*\n\n💳 Kaspi: `{KASPI_NUMBER}`\n👤 {KASPI_NAME}\n\n📸 Чекті жібер!"
            if lang == 'kz' else
            f"📱 *{ch['name']} — {fmt(ch['price'])} тг*\n\n💳 Kaspi: `{KASPI_NUMBER}`\n👤 {KASPI_NAME}\n\n📸 Скинь чек!")
    
    await cb.message.edit_text(text, reply_markup=get_back_kb(lang), parse_mode="Markdown")
    await cb.answer()

@dp.message(F.photo | F.document)
async def handle_receipt(message: types.Message):
    user_id = message.from_user.id
    
    if is_spam(user_id):
        await message.answer("⏳ Подождите немного!")
        return
    
    user_messages[user_id].append(time.time())
    
    d = ud(user_id)
    lang = d.get('lang', 'kz')
    stage = d.get('stage', 'wait_1')
    pack = d.get('pack', '1_kanal')
    ch_price = d.get('channel_price', 0)
    
    async def send_to_admin(caption, kb):
        if message.photo:
            await bot.send_photo(ADMIN_ID, message.photo[-1].file_id, caption=caption, reply_markup=kb)
        else:
            await bot.send_document(ADMIN_ID, message.document.file_id, caption=caption, reply_markup=kb)
    
    await message.answer("✅ Чек қабылданды!" if lang == 'kz' else "✅ Чек принят!")
    
    if pack == 'vip_obzor' or stage == 'wait_vip':
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="✅ Растау", callback_data=f"confvip_{user_id}"))
        kb.row(types.InlineKeyboardButton(text="❌ Бас тарту", callback_data=f"rej_{user_id}"))
        cap = f"🔔 VIP ОБЗОР\n👤 ID: {user_id}\n💰 290 тг"
        await send_to_admin(cap, kb.as_markup())
        return
    
    if stage == 'wait_2':
        comm = "1 777" if pack == '8_kanal' else "1 555"
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="✅ Растау", callback_data=f"conf2_{user_id}"))
        kb.row(types.InlineKeyboardButton(text="❌ Бас тарту", callback_data=f"rej_{user_id}"))
        cap = f"🔔 КОМИССИЯ\n👤 ID: {user_id}\n💰 {comm} тг"
        await send_to_admin(cap, kb.as_markup())
        return
    
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="✅ Растау", callback_data=f"conf1_{user_id}"))
    kb.row(types.InlineKeyboardButton(text="❌ Бас тарту", callback_data=f"rej_{user_id}"))
    cap = f"🔔 НЕГІЗГІ ТӨЛЕМ\n👤 ID: {user_id}\n💰 {fmt(ch_price)} тг"
    await send_to_admin(cap, kb.as_markup())

@dp.callback_query(F.data.startswith("confvip_"))
async def conf_vip(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    ud(user_id)['stage'] = 'done'
    vip_obzor_users.add(user_id)
        msg = f"🎉 VIP ОБЗОР расталды!\n\n{VIP_OBZOR['link']}\n\n📞 Сұрақтар: {SUPPORT_USERNAME}" if lang == 'kz' else \
          f"🎉 VIP ОБЗОР подтверждён!\n\n{VIP_OBZOR['link']}\n\n📞 Вопросы: {SUPPORT_USERNAME}"
    
    await bot.send_message(user_id, msg, parse_mode="Markdown")
    await cb.message.edit_text("✅ Расталды")

@dp.callback_query(F.data.startswith("conf1_"))
async def conf1(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    d = ud(user_id)
    lang = d.get('lang', 'kz')
    pack = d.get('pack', '1_kanal')
    d['stage'] = 'wait_2'
    
    if pack == '8_kanal':
        links = "\n".join([f"🔗 {v['name']}: {v['link']}" for v in CHANNELS.values()])
        comm = "1 777"
    else:
        ch = CHANNELS.get(d.get('channel', 'ch_1'), CHANNELS['ch_1'])
        links = f"🔗 {ch['name']}: {ch['link']}"
        comm = "1 555"
    
    text = (f"✅ Бірінші төлем расталды!\n\n{links}\n\n⚠️ СОҢҒЫ ҚАДАМ — {comm} тг\n\n💳 Kaspi: `{KASPI_NUMBER}`\n👤 {KASPI_NAME}\n\n📸 Чекті жібер!\n\n📞 Сұрақтар: {SUPPORT_USERNAME}"
            if lang == 'kz' else
            f"✅ Первый платеж подтверждён!\n\n{links}\n\n⚠️ ПОСЛЕДНИЙ ШАГ — {comm} тг\n\n💳 Kaspi: `{KASPI_NUMBER}`\n👤 {KASPI_NAME}\n\n📸 Скинь чек!\n\n📞 Вопросы: {SUPPORT_USERNAME}")
    
    await bot.send_message(user_id, text, parse_mode="Markdown")
    await cb.message.edit_text("✅ Расталды")

@dp.callback_query(F.data.startswith("conf2_"))
async def conf2(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    ud(user_id)['stage'] = 'done'
    paid_users.add(user_id)
    
    track_user_action(user_id, "fully_paid")
    logger.info(f"[PAYMENT] User {user_id} fully paid")
    
    msg = ("🎉 *Құттықтаймын!* Комиссия қабылданды.\n\n✅ Енді сізде толық доступ!\n💋 Рахаттана көріңіз!\n\n📞 Сұрақтар: {SUPPORT_USERNAME}"
           if lang == 'kz' else
           "🎉 *Поздравляем!* Комиссия принята.\n\n✅ Теперь у вас полный доступ!\n💋 Наслаждайтесь!\n\n📞 Вопросы: {SUPPORT_USERNAME}")
    
    await bot.send_message(user_id, msg.format(SUPPORT_USERNAME=SUPPORT_USERNAME), parse_mode="Markdown")
    await cb.message.edit_text("✅ Толық расталды")

@dp.callback_query(F.data.startswith("rej_"))
async def rej(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    
    logger.info(f"[REJECT] Payment rejected for user {user_id}")
    
    msg = ("❌ Кешіріңіз, төлеміңіз расталмады.\n\n📸 Жаңа чекті жіберіңіз!\n\n📞 Сұрақтар: {SUPPORT_USERNAME}"
           if lang == 'kz' else
           "❌ Извини, платеж не подтвержден.\n\n📸 Отправьте новый чек!\n\n📞 Вопросы: {SUPPORT_USERNAME}")
    
    await bot.send_message(user_id, msg.format(SUPPORT_USERNAME=SUPPORT_USERNAME), parse_mode="Markdown")
    await cb.message.edit_text("❌ Бас тартылды")

@dp.callback_query(F.data.startswith("offeryes_"))
async def offer_yes(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    
    logger.info(f"[DISCOUNT] Admin approved discount for user {user_id}")
    
    msg = (f"✅ Жарайды жаным, сол соманы жібер!\n💳 Kaspi: `{KASPI_NUMBER}`\n👤 *{KASPI_NAME}*\n\n📞 Сұрақтар: {SUPPORT_USERNAME}"
           if lang == 'kz' else
           f"✅ Хорошо зай, скидывай!\n💳 Kaspi: `{KASPI_NUMBER}`\n👤 *{KASPI_NAME}*\n\n📞 Вопросы: {SUPPORT_USERNAME}")
    
    await bot.send_message(user_id, msg, parse_mode="Markdown")
    await cb.message.edit_text("✅ Жеңілдікке рұқсат берілді.")

@dp.callback_query(F.data.startswith("offerno_"))
async def offer_no(cb: types.CallbackQuery):
    await cb.answer()
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    
    logger.info(f"[DISCOUNT] Admin rejected discount for user {user_id}")
    
    msg = ("❌ Жоқ жаным, ренжіме.\n\nТолық төлемнен кейін ғана кіре аласың.\n\n📞 Сұрақтар: {SUPPORT_USERNAME}"
           if lang == 'kz' else
           "❌ Нет зай, извини.\n\nДоступ только после полной оплаты.\n\n📞 Вопросы: {SUPPORT_USERNAME}")
    
    await bot.send_message(user_id, msg.format(SUPPORT_USERNAME=SUPPORT_USERNAME), parse_mode="Markdown")
    await cb.message.edit_text("❌ Жеңілдіктен бас тартылды.")

@dp.chat_join_request()
async def handle_join_request(request: ChatJoinRequest):
    user_id = request.from_user.id
    chat_id = request.chat.id
    chat_title = request.chat.title
    
    if user_id in paid_users or user_id in vip_obzor_users:
        try:
            await bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
            logger.info(f"[JOIN] ✅ Approved {user_id} → {chat_title}")
        except Exception as e:
            logger.error(f"[JOIN] Error: {e}")
    else:
        logger.warning(f"[JOIN] Not paid — {user_id}")

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    user_id = message.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    
    text = ("🆘 *КӨМЕК*\n\n💎 Пакеттер:\n• 8 канал — 3,333 тг\n• 1 канал — өз бағасымен\n• VIP ОБЗОР — 290 тг\n\n🎁 Тегін канал: 10 досыңды шақыр\n\n📌 Командалар: /start, /help, /profile, /support\n\n📞 Байланыс: {SUPPORT_USERNAME}"
            if lang == 'kz' else
            "🆘 *ПОМОЩЬ*\n\n💎 Пакеты:\n• 8 каналов — 3,333 тг\n• 1 канал — по своей цене\n• VIP ОБЗОР — 290 тг\n\n🎁 Бесплатный канал: пригласи 10 друзей\n\n📌 Команды: /start, /help, /profile, /support\n\n📞 Контакты: {SUPPORT_USERNAME}")
    
    await message.answer(text.format(SUPPORT_USERNAME=SUPPORT_USERNAME), parse_mode="Markdown")

@dp.message(Command("profile"))
async def cmd_profile(message: types.Message):
    user_id = message.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    stats = get_user_stats(user_id)
    
    first_seen = datetime.fromisoformat(stats['first_seen']) if stats['first_seen'] else datetime.now()
    days_since = (datetime.now() - first_seen).days
    
    text = (f"👤 *Менің профилім*\n\n🆔 ID: `{user_id}`\n📅 Ботта: {days_since} күн\n💬 Хабар: {stats['messages_sent']}\n✅ Төлем: {'Иә' if stats['is_paid'] else 'Жоқ'}\n🔞 VIP: {'Иә' if stats['has_vip'] else 'Жоқ'}\n👥 Рефералдар: {stats['referrals']}\n🎯 Қалды: {REFERRAL_GOAL - stats['referrals']}\n\n📞 Байланыс: {SUPPORT_USERNAME}"
            if lang == 'kz' else
            f"👤 *Мой профиль*\n\n🆔 ID: `{user_id}`\n📅 В боте: {days_since} дней\n💬 Сообщ: {stats['messages_sent']}\n✅ Оплата: {'Да' if stats['is_paid'] else 'Нет'}\n🔞 VIP: {'Да' if stats['has_vip'] else 'Нет'}\n👥 Рефералов: {stats['referrals']}\n🎯 Осталось: {REFERRAL_GOAL - stats['referrals']}\n\n📞 Контакты: {SUPPORT_USERNAME}")
    
    await message.answer(text.format(SUPPORT_USERNAME=SUPPORT_USERNAME), parse_mode="Markdown")

@dp.message(Command("support"))
async def cmd_support(message: types.Message):
    user_id = message.from_user.id
    lang = ud(user_id).get('lang', 'kz')
    
    text = ("📞 *ҚОЛДАУ*\n\nСұрақтарыңызды жазыңыз, біз көмектесеміз!\n\n👤 Байланыс: {SUPPORT_USERNAME}\n⏱ Жауап беру уақыты: 5-30 минут"
            if lang == 'kz' else
            "📞 *ПОДДЕРЖКА*\n\nНапишите ваш вопрос, мы поможем!\n\n👤 Контакты: {SUPPORT_USERNAME}\n⏱ Время ответа: 5-30 минут")
    
    await message.answer(text.format(SUPPORT_USERNAME=SUPPORT_USERNAME), parse_mode="Markdown")

# ─── AI HANDLER (MAIN) ─────────────────────────────────────────────────────────

@dp.message()
async def ai_handler(message: types.Message):
    # Skip admin messages and non-text
    if message.from_user.id == ADMIN_ID or not message.text:
        return
    
    user_id = message.from_user.id
    
    # Anti-spam check
    if is_spam(user_id):
        await message.answer("⏳ Жаным, күте тұршы... 💋")
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
    
    # Check for price negotiation
    price_keywords = ['скидка', 'дешевле', 'дорого', 'денег нет', 'акция', 'жеңілдік', 'арзан', 'қымбат', 'ақшам жоқ']
    if any(kw in message.text.lower() for kw in price_keywords) and stage == 'wait_2':
        await message.answer(
            "😏 Жаным, бұл қазірдің өзінде скидка!\n\nҚанша сала аласың? 💋" if lang == 'kz'
            else "😏 Зай, это уже со скидкой!\n\nСколько можешь скинуть? 💋"
        )
        return
    
    # Channel content triggers
    triggers = ["не бар", "не кіреді", "қандай", "ішінде", "что внутри", "какие каналы", "внутри", "нелер бар"]
    if stage not in ('wait_2', 'done', 'wait_vip') and any(w in message.text.lower() for w in triggers):
        lines = "\n".join([f"🔥 {v['name']} — {fmt(v['price'])} тг" for v in CHANNELS.values()])
        text = (f"✨ *Іште не бар?*\n\n{lines}\n\n👇 Қазір төле, бірден кір! 💋" if lang == 'kz'
                else f"✨ *Что внутри?*\n\n{lines}\n\n👇 Оплачивай сейчас и заходи! 💋")
        await message.answer(text, parse_mode="Markdown", reply_markup=reply_kb)
        return
    
    # Build context
    comm = "1 777" if pack == '8_kanal' else "1 555"
    lang_rule = ("ҚАЗАҚША сөйле. 'Жаным', 'Күнім' де." if lang == 'kz'
                 else "Говори на РУССКОМ. 'Зай', 'Милый' де.")
    
    if stage == 'done':
        dynamic = "Клиент төледі. Еркін сөйлес."
        reply_kb = None
    elif stage == 'wait_vip':
        dynamic = "Клиент VIP ОБЗОР чекін күтуде."
        reply_kb = None
    elif stage == 'wait_2':
        dynamic = (f"ЕКІНШІ ТӨЛЕМ ({comm} тг) күтілуде. Скидка десе: 'Бұл скидка'. Ақшам жоқ десе: 'Қанша сала аласың?'"
                   if lang == 'kz' else
                   f"ВТОРОЙ ПЛАТЕЖ ({comm} тг) ожидается. Скидка: 'Это скидка'. Нет денег: 'Сколько можешь?'")
        reply_kb = None
    else:
        dynamic = "Екінші төлем туралы сұраса — жоқ де." if lang == 'kz' else "Про второй платеж — скажи нет."
    
    full_prompt = f"{SYSTEM_PROMPT} {lang_rule}\nМАҢЫЗДЫ: {dynamic}"
    
    # Send thinking message
    thinking_msg = await message.answer("💭 Ойланып жатырмын..." if lang == 'kz' else "💭 Думаю...")
    
    # Call Gemini API
    response = await call_gemini(message.text, history, lang)
    
    # Delete thinking message
    try:
        await thinking_msg.delete()
    except Exception:
        pass
    
    if response:
        # Update history
        history.append({"role": "user", "content": message.text})
        history.append({"role": "assistant", "content": response})
        
        if len(history) > 15:
            d['history'] = history[-15:]
        
        # Check for discount request
        if "[АДМИН]" in response:
            response = response.replace("[АДМИН]", "").strip()
            akb = InlineKeyboardBuilder()
            akb.add(types.InlineKeyboardButton(text="✅ Иә", callback_data=f"offeryes_{user_id}"))
            akb.add(types.InlineKeyboardButton(text="❌ Жоқ", callback_data=f"offerno_{user_id}"))
            
            await bot.send_message(
                ADMIN_ID,
                f"🔔 *Скидка сұрау*\n👤 ID: {user_id}\n💬 {message.text[:200]}",
                parse_mode="Markdown",
                reply_markup=akb.as_markup()
            )
        
        try:
            await message.answer(response, reply_markup=reply_kb)
        except Exception as e:
            logger.error(f"[AI] Error: {e}")
            await message.answer(get_fallback_response(lang), reply_markup=reply_kb)
    else:
        # Fallback
        logger.warning(f"[AI] AI failed for user {user_id}")
        await message.answer(get_fallback_response(lang), reply_markup=reply_kb)

# ─── ERROR HANDLER ─────────────────────────────────────────────────────────────

@dp.errors()
async def error_handler(update: types.Update, exception: Exception):
    logger.error(f"[ERROR] {exception}", exc_info=True)
    
    if ADMIN_ID:
        try:
            await bot.send_message(ADMIN_ID, f"⚠️ *Ошибка:* {str(exception)[:200]}", parse_mode="Markdown")
        except Exception:
            pass
    
    return True

# ─── BACKGROUND TASKS ─────────────────────────────────────────────────────────

async def clean_old_data():
    while True:
        await asyncio.sleep(3600)
        try:
            now = datetime.now()
            to_delete = []
            for user_id, data in user_data.items():
                last_active = data.get('last_active')
                if last_active:
                    last_time = datetime.fromisoformat(last_active)
                    if (now - last_time).days > 30:
                        to_delete.append(user_id)
            for user_id in to_delete:
                if user_id in user_data:
                    del user_data[user_id]
                if user_id in user_messages:
                    del user_messages[user_id]
            if to_delete:
                logger.info(f"[CLEAN] Removed {len(to_delete)} inactive users")
        except Exception as e:
            logger.error(f"[CLEAN] Error: {e}")

async def update_analytics():
    while True:
        await asyncio.sleep(3600)
        try:
            total_users = len(user_data)
            total_paid = len(paid_users)
            total_sales = calculate_total_sales()
            logger.info(f"[ANALYTICS] Users: {total_users}, Paid: {total_paid}, Sales: {total_sales} тг")
        except Exception as e:
            logger.error(f"[ANALYTICS] Error: {e}")

# ─── MAIN FUNCTION ─────────────────────────────────────────────────────────────

async def main():
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  АЙСҰЛУ БОТ ІСКЕ ҚОСЫЛДЫ ✅")
    logger.info("  Flask: http://0.0.0.0:8000/")
    logger.info("  Gemini: gemini-1.5-flash")
    bot_me = await bot.get_me()
    logger.info(f"  Bot: @{bot_me.username}")
    logger.info(f"  Support: {SUPPORT_USERNAME}")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    
    # Start background tasks
    asyncio.create_task(self_ping_loop())
    asyncio.create_task(clean_old_data())
    asyncio.create_task(update_analytics())
    
    # Set bot commands
    await bot.set_my_commands([
        types.BotCommand(command="start", description="🚀 Бастау / Старт"),
        types.BotCommand(command="help", description="🆘 Көмек / Помощь"),
        types.BotCommand(command="profile", description="👤 Мой профиль"),
        types.BotCommand(command="support", description="📞 Қолдау"),
    ])
    
    # Start bot polling
    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"[MAIN] Fatal error: {e}", exc_info=True)
        raise

# ─── ENTRY POINT ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    keep_alive()
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот тоқтатылды!")
    except SystemExit:
        logger.info("Бот тоқтатылды!")
    except Exception as e:
        logger.error(f"Бот қатемен тоқтады: {e}", exc_info=True)
        raise
    finally:
        logger.info("Бот жұмысын аяқтады. 👋")
