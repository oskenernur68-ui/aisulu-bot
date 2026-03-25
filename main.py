import os
import asyncio
import random
from flask import Flask
from threading import Thread
import requests

# ─── KEEP-ALIVE (Flask) ───────────────────────────────────────────────────────
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return "Айсұлу бот жұмыс істеп тұр! 🥰"

def _run_flask():
    import logging
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)          # silence Flask request noise
    flask_app.run(host='0.0.0.0', port=8000)

def keep_alive():
    t = Thread(target=_run_flask, daemon=True)
    t.start()

async def self_ping_loop():
    """Ping own Flask server every 4 min so Replit never idles the process."""
    await asyncio.sleep(30)             # wait for Flask to be fully up
    while True:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get("http://127.0.0.1:8000/",
                                  timeout=aiohttp.ClientTimeout(total=10)) as r:
                    print(f"[PING] self-ping status={r.status}")
        except Exception as e:
            print(f"[PING] self-ping error: {e}")
        await asyncio.sleep(240)         # every 4 minutes

# ─── IMPORTS ─────────────────────────────────────────────────────────────────
import aiohttp
import json
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ChatJoinRequest
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ─── CONFIG ──────────────────────────────────────────────────────────────────
TOKEN        = os.environ.get("TOKEN", "")
AI_API_KEY   = os.environ.get("AI_API_KEY", "")
OPENROUTER_URL   = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = "google/gemini-2.0-flash-001"

ADMIN_ID     = 8158572095
KASPI_NUMBER = "4400430232568623"
KASPI_NAME   = "Сағынай Е."

# ─── CHANNELS (with individual prices) ───────────────────────────────────────
CHANNELS = {
    "ch_1": {
        "name":  "🌸 DETSKIY POLNYY VIDEOLAR 🌸",
        "link":  "https://t.me/+l5O0oqpioh4wZTUy",
        "price": 2150,
    },
    "ch_2": {
        "name":  "💋 Taza qazaqsha shkolnikter ❤️‍🔥",
        "link":  "https://t.me/+VT-dk2MqXU5iNWFi",
        "price": 2390,
    },
    "ch_3": {
        "name":  "✨ ҚЫЗДАРДЫҢ НӨМІРІ ЖӘНЕ МЕКЕН-ЖАЙЫ (KZ) ✨",
        "link":  "https://t.me/+Z0ZuiWlJ18I1MWE6",
        "price": 2790,
    },
    "ch_4": {
        "name":  "📺 VIP KANAL",
        "link":  "https://t.me/+WrfTpek1bvA1MTAy",
        "price": 2650,
    },
    "ch_5": {
        "name":  "💋 Sen izdegen qazaqsha kanaldar",
        "link":  "https://t.me/+rv6c5Avp2TNmYTY6",
        "price": 2200,
    },
    "ch_6": {
        "name":  "📺 VIDEO KZ",
        "link":  "https://t.me/+z2atV2nVfWY5MzBi",
        "price": 1990,
    },
    "ch_7": {
        "name":  "😍 BLOGERLER SLIV",
        "link":  "https://t.me/+sLgEIncaQkgxZjg6",
        "price": 2850,
    },
    "ch_8": {
        "name":  "🔥 V I P 2",
        "link":  "https://t.me/+6b7mnDsklQlhZDBi",
        "price": 2490,
    },
}

# VIP ОБЗОР — standalone, no commission
VIP_OBZOR = {
    "name":  "🔞 VIP ОБЗОР",
    "link":  "https://t.me/+-EpLiQphVQNjY2Iy",
    "price": 290,
}

# ─── CHANNEL PREVIEW PHOTOS ──────────────────────────────────────────────────
# Keys match CHANNELS keys. Each list = possible preview images (one picked randomly).
# NOTE: These must be direct image URLs (ending in .jpg/.png).
# ibb.co viewer links won't work — replace with i.ibb.co direct links when available.
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
    "7. Мақсатың — клиент пакет алсын."
)

# ─── STATE ───────────────────────────────────────────────────────────────────
bot            = Bot(token=TOKEN)
dp             = Dispatcher()
user_data      = {}        # {user_id: {lang, stage, pack, channel, channel_name, channel_price, history}}
paid_users     = set()    # completed full payment (main + commission)
vip_obzor_users = set()   # paid for VIP ОБЗОР
referral_data  = {}        # {referrer_id: {invited_user_ids}}

REFERRAL_GOAL         = 10
REFERRAL_FREE_CHANNEL = CHANNELS["ch_1"]["link"]


# ─── KEYBOARDS ───────────────────────────────────────────────────────────────
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
        b.row(types.InlineKeyboardButton(text="💎 8 КАНАЛ (ПАКЕТ — 3 333 тг)",  callback_data="buy_pack_8"))
        b.row(types.InlineKeyboardButton(text="📱 1 КАНАЛ (жеке бағамен)",      callback_data="buy_list_1"))
        b.row(types.InlineKeyboardButton(text="🔞 VIP ОБЗОР (290 тг)",          callback_data="buy_vip_obzor"))
        b.row(types.InlineKeyboardButton(text="🎁 8 Каналдың ішінде не бар?",   callback_data="show_channels"))
        b.row(types.InlineKeyboardButton(text="🎁 ТЕГІН КАНАЛҒА КІРУ",          callback_data="ref_link"))
    else:
        b.row(types.InlineKeyboardButton(text="💎 8 КАНАЛОВ (ПАКЕТ — 3 333 тг)", callback_data="buy_pack_8"))
        b.row(types.InlineKeyboardButton(text="📱 1 КАНАЛ (по своей цене)",      callback_data="buy_list_1"))
        b.row(types.InlineKeyboardButton(text="🔞 VIP ОБЗОР (290 тг)",           callback_data="buy_vip_obzor"))
        b.row(types.InlineKeyboardButton(text="🎁 Что внутри 8 каналов?",        callback_data="show_channels"))
        b.row(types.InlineKeyboardButton(text="🎁 БЕСПЛАТНЫЙ КАНАЛ",             callback_data="ref_link"))
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


# ─── HELPERS ─────────────────────────────────────────────────────────────────
def fmt(price: int) -> str:
    return f"{price:,}".replace(",", " ")


def ud(user_id: int) -> dict:
    if user_id not in user_data:
        user_data[user_id] = {}
    return user_data[user_id]


# ─── /start ──────────────────────────────────────────────────────────────────
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    args    = message.text.split()[1] if len(message.text.split()) > 1 else None

    # referral tracking
    if args and args.startswith("ref_"):
        try:
            referrer_id = int(args.replace("ref_", ""))
        except ValueError:
            referrer_id = None
        if referrer_id and referrer_id != user_id and user_id not in user_data:
            referral_data.setdefault(referrer_id, set()).add(user_id)
            count = len(referral_data[referrer_id])
            if count >= REFERRAL_GOAL:
                ref_lang = ud(referrer_id).get('lang', 'kz')
                if ref_lang == 'kz':
                    msg = f"🎉 *Құттықтаймын!* {REFERRAL_GOAL} адам жинадың!\n\nТЕГІН каналың:\n{REFERRAL_FREE_CHANNEL} 💋"
                else:
                    msg = f"🎉 *Поздравляю!* {REFERRAL_GOAL} человек собрал!\n\nТвой БЕСПЛАТНЫЙ канал:\n{REFERRAL_FREE_CHANNEL} 💋"
                try:
                    await bot.send_message(referrer_id, msg, parse_mode="Markdown")
                except Exception as e:
                    print(f"[REF] reward error: {e}")

    d = ud(user_id)
    d['stage'] = 'lang_select'
    d.setdefault('lang', 'kz')
    await message.answer("🌐 Тілді таңдаңыз / Выберите язык:", reply_markup=get_lang_kb())


@dp.callback_query(F.data.startswith("lang_"))
async def process_lang(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang    = cb.data.split("_")[1]
    d       = ud(user_id)
    d['lang']  = lang
    d['stage'] = 'start'
    if lang == 'kz':
        text = "Сәлем жаным 🥰 Мен сені күтіп отыр едім... Пакетті таңда да, ішке кір! 💋"
    else:
        text = "Привет зай 🥰 Я тебя ждала... Выбирай пакет и заходи! 💋"
    await cb.message.edit_text(text, reply_markup=get_main_kb(lang))
    await cb.answer()


@dp.callback_query(F.data == "go_to_main")
async def go_main(cb: types.CallbackQuery):
    lang = ud(cb.from_user.id).get('lang', 'kz')
    text = "🌟 Таңдау жасаңыз 👇" if lang == 'kz' else "🌟 Сделай выбор 👇"
    await cb.message.edit_text(text, reply_markup=get_main_kb(lang))
    await cb.answer()


# ─── REFERRAL ────────────────────────────────────────────────────────────────
@dp.callback_query(F.data == "ref_link")
async def ref_link(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang    = ud(user_id).get('lang', 'kz')
    bot_me  = await bot.get_me()
    ref_url = f"https://t.me/{bot_me.username}?start=ref_{user_id}"
    count   = len(referral_data.get(user_id, set()))
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
    lang    = ud(user_id).get('lang', 'kz')
    count   = len(referral_data.get(user_id, set()))
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
    lang    = ud(user_id).get('lang', 'kz')

    # Step 1: Clean text list — NO URLs, just names with beautiful formatting
    LIST_TEXT = (
        "🔥 🌸 <b><u>DETSKIY POLNYY VIDEOLAR</u></b> 🌸 🔥\n"
        "🔥 💋 <b><u>Taza qazaqshalpha shkolniktep</u></b> 💖 🔥\n"
        "🔥 ✨ <b><u>ҚЫЗДАРДЫҢ НӨМІРІ ЖӘНЕ МЕКЕН-ЖАЙЫ (KZ)</u></b> ✨ 🔥\n"
        "🔥 📺 <b><u>VIP KANAL</u></b> 🔥\n"
        "🔥 💋 <b><u>Sен izdegeң qazaqsha кaнaldar</u></b> 💋 🔥\n"
        "🔥 📺 <b><u>VIDEO KZ</u></b> 🔥\n"
        "🔥 😍 <b><u>BLOGERLER SLIV</u></b> 😍 🔥\n"
        "🔥 🔥 <b><u>V I P 2</u></b> 🔥 🔥\n\n"
        "👇 <b>Қазір төле де, бірден рахатын көр!</b>"
    )
    await cb.message.edit_text(LIST_TEXT, parse_mode="HTML", reply_markup=get_main_kb(lang))

    # Step 2: 8 separate photos — each with its name as caption, no URLs in caption
    CHANNEL_CAPTIONS = {
        "ch_1": "🔥 🌸 <b><u>DETSKIY POLNYY VIDEOLAR</u></b> 🌸 🔥",
        "ch_2": "🔥 💋 <b><u>Taza qazaqshalpha shkolniktep</u></b> 💖 🔥",
        "ch_3": "🔥 ✨ <b><u>ҚЫЗДАРДЫҢ НӨМІРІ ЖӘНЕ МЕКЕН-ЖАЙЫ (KZ)</u></b> ✨ 🔥",
        "ch_4": "🔥 📺 <b><u>VIP KANAL</u></b> 🔥",
        "ch_5": "🔥 💋 <b><u>Sен izdegeң qazaqsha кaнaldar</u></b> 💋 🔥",
        "ch_6": "🔥 📺 <b><u>VIDEO KZ</u></b> 🔥",
        "ch_7": "🔥 😍 <b><u>BLOGERLER SLIV</u></b> 😍 🔥",
        "ch_8": "🔥 🔥 <b><u>V I P 2</u></b> 🔥 🔥",
    }

    for ch_key, caption in CHANNEL_CAPTIONS.items():
        
        try:
            await bot.send_message(
                user_id,
                
                text=caption,
                parse_mode="HTML",
            )
            await asyncio.sleep(0.4)   # small pause — avoids Telegram rate-limit
        except Exception as e:
         print(f"Error: {e}")
            
            
# ─── BUY: 8-CHANNEL PACK ─────────────────────────────────────────────────────
@dp.callback_query(F.data == "buy_pack_8")
async def buy_pack_8(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang    = ud(user_id).get('lang', 'kz')
    d = ud(user_id)
    d['pack']          = '8_kanal'
    d['stage']         = 'wait_1'
    d['channel_name']  = '8 КАНАЛ ПАКЕТІ'
    d['channel_price'] = 3333

    if lang == 'kz':
        text = (f"💎 *8 КАНАЛ ПАКЕТІ — 3 333 тг*\n\n"
                f"💳 Kaspi картасына аудар:\n`{KASPI_NUMBER}`\n"
                f"👤 *{KASPI_NAME}*\n\n"
                f"📸 Чекті осы чатқа жібер! 🔥")
    else:
        text = (f"💎 *ПАКЕТ 8 КАНАЛОВ — 3 333 тг*\n\n"
                f"💳 Переведи на Kaspi:\n`{KASPI_NUMBER}`\n"
                f"👤 *{KASPI_NAME}*\n\n"
                f"📸 Скинь чек сюда! 🔥")
    await cb.message.edit_text(text, reply_markup=get_back_kb(lang), parse_mode="Markdown")
    await cb.answer()


# ─── BUY: VIP ОБЗОР ──────────────────────────────────────────────────────────
@dp.callback_query(F.data == "buy_vip_obzor")
async def buy_vip_obzor(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang    = ud(user_id).get('lang', 'kz')
    d = ud(user_id)
    d['pack']          = 'vip_obzor'
    d['stage']         = 'wait_vip'
    d['channel_name']  = VIP_OBZOR['name']
    d['channel_price'] = VIP_OBZOR['price']

    if lang == 'kz':
        text = (f"🔞 *VIP ОБЗОР — 290 тг*\n\n"
                f"💳 Kaspi картасына аудар:\n`{KASPI_NUMBER}`\n"
                f"👤 *{KASPI_NAME}*\n\n"
                f"📸 Чекті осы чатқа жібер — бірден ашамын! 🔥")
    else:
        text = (f"🔞 *VIP ОБЗОР — 290 тг*\n\n"
                f"💳 Переведи на Kaspi:\n`{KASPI_NUMBER}`\n"
                f"👤 *{KASPI_NAME}*\n\n"
                f"📸 Скинь чек сюда — сразу открою! 🔥")
    await cb.message.edit_text(text, reply_markup=get_back_kb(lang), parse_mode="Markdown")
    await cb.answer()


# ─── BUY: 1 CHANNEL (channel selection) ──────────────────────────────────────
@dp.callback_query(F.data == "buy_list_1")
async def buy_list_1(cb: types.CallbackQuery):
    lang = ud(cb.from_user.id).get('lang', 'kz')
    text = ("👇 Қай каналды алғыңыз келеді? Бағасы жанында:"
            if lang == 'kz' else
            "👇 Какой канал хочешь? Цена указана рядом:")
    await cb.message.edit_text(text, reply_markup=get_channel_kb(lang))
    await cb.answer()


@dp.callback_query(F.data.startswith("select_"))
async def select_channel(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    lang    = ud(user_id).get('lang', 'kz')
    ch_key  = cb.data.replace("select_", "")
    ch      = CHANNELS[ch_key]
    d = ud(user_id)
    d['pack']          = '1_kanal'
    d['stage']         = 'wait_1'
    d['channel']       = ch_key
    d['channel_name']  = ch['name']
    d['channel_price'] = ch['price']

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
    d       = ud(user_id)
    lang    = d.get('lang', 'kz')
    stage   = d.get('stage', 'wait_1')
    pack    = d.get('pack', '1_kanal')
    ch_name = d.get('channel_name', '—')
    ch_price = d.get('channel_price', 0)
    price_str = fmt(ch_price)

    # Helper to send photo or doc to admin
    async def send_to_admin(caption, kb):
        if message.photo:
            await bot.send_photo(ADMIN_ID, message.photo[-1].file_id, caption=caption, reply_markup=kb)
        else:
            await bot.send_document(ADMIN_ID, message.document.file_id, caption=caption, reply_markup=kb)

    user_ack = "✅ Чек қабылданды! Тексеріп жатырмын..." if lang == 'kz' else "✅ Чек принят! Проверяю..."

    # ── VIP ОБЗОР ──
    if pack == 'vip_obzor' or stage == 'wait_vip':
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="✅ VIP ОБЗОР Растау", callback_data=f"confvip_{user_id}"))
        kb.row(types.InlineKeyboardButton(text="❌ Бас тарту", callback_data=f"rej_{user_id}"))
        cap = (f"🔔 ЧЕК — VIP ОБЗОР\n"
               f"👤 User ID: {user_id}\n"
               f"💰 290 тг\n"
               f"📌 User wants VIP ОБЗОР for 290 тг. Confirm receipt?")
        await send_to_admin(cap, kb.as_markup())
        await message.answer(user_ack)
        return

    # ── Commission (2nd payment) ──
    if stage == 'wait_2':
        comm = "1 777 тг" if pack == '8_kanal' else "1 555 тг"
        kb = InlineKeyboardBuilder()
        kb.row(types.InlineKeyboardButton(text="✅ Комиссия Растау", callback_data=f"conf2_{user_id}"))
        kb.row(types.InlineKeyboardButton(text="❌ Бас тарту", callback_data=f"rej_{user_id}"))
        cap = (f"🔔 ЧЕК — КОМИССИЯ\n"
               f"👤 User ID: {user_id}\n"
               f"💰 {comm}")
        await send_to_admin(cap, kb.as_markup())
        await message.answer(user_ack)
        return

    # ── Main payment (1st payment) ──
    kb = InlineKeyboardBuilder()
    kb.row(types.InlineKeyboardButton(text="✅ Төлемді Растау", callback_data=f"conf1_{user_id}"))
    kb.row(types.InlineKeyboardButton(text="❌ Бас тарту", callback_data=f"rej_{user_id}"))
    cap = (f"🔔 ЧЕК — НЕГІЗГІ ТӨЛЕМ\n"
           f"👤 User ID: {user_id}\n"
           f"📺 Канал: {ch_name}\n"
           f"💰 {price_str} тг\n"
           f"📌 User wants {ch_name} for {price_str} тг. Confirm receipt?")
    await send_to_admin(cap, kb.as_markup())
    await message.answer(user_ack)


# ─── ADMIN CONFIRMATIONS ──────────────────────────────────────────────────────

# VIP ОБЗОР confirmed
@dp.callback_query(F.data.startswith("confvip_"))
async def conf_vip(cb: types.CallbackQuery):
    await cb.answer()                    # ← instant: removes spinner immediately
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    ud(user_id)['stage'] = 'done'
    vip_obzor_users.add(user_id)
    if lang == 'kz':
        msg = f"🎉 *VIP ОБЗОР расталды!*\n\nМіне жабық канал:\n{VIP_OBZOR['link']} 🔥\n\nРахаттан жаным! 💋"
    else:
        msg = f"🎉 *VIP ОБЗОР подтверждён!*\n\nВот закрытый канал:\n{VIP_OBZOR['link']} 🔥\n\nНаслаждайся зай! 💋"
    await bot.send_message(user_id, msg, parse_mode="Markdown")
    try:
        await cb.message.edit_caption("✅ VIP ОБЗОР расталды")
    except Exception:
        await cb.message.edit_text("✅ VIP ОБЗОР расталды")


# Main payment confirmed → send links + ask commission
@dp.callback_query(F.data.startswith("conf1_"))
async def conf1(cb: types.CallbackQuery):
    await cb.answer()                    # ← instant
    user_id = int(cb.data.split("_")[1])
    d    = ud(user_id)
    lang = d.get('lang', 'kz')
    pack = d.get('pack', '1_kanal')
    d['stage'] = 'wait_2'

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
                f"📸 Чекті осы чатқа жібер!")
    else:
        text = (f"✅ *Первый платеж подтверждён!*\n\n{links}\n\n"
                f"━━━━━━━━━━━━━━━\n"
                f"⚠️ *ПОСЛЕДНИЙ ШАГ — {comm} тг*\n"
                f"━━━━━━━━━━━━━━━\n\n"
                f"Это разовая комиссия. Все клиенты так покупают!\n\n"
                f"💳 Kaspi:\n`{KASPI_NUMBER}`\n👤 *{KASPI_NAME}*\n\n"
                f"📸 Скинь чек сюда!")
    await bot.send_message(user_id, text, parse_mode="Markdown")
    try:
        await cb.message.edit_caption("✅ Расталды, ссылка жіберілді")
    except Exception:
        await cb.message.edit_text("✅ Расталды, ссылка жіберілді")


# Commission confirmed → fully paid
@dp.callback_query(F.data.startswith("conf2_"))
async def conf2(cb: types.CallbackQuery):
    await cb.answer()                    # ← instant
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    ud(user_id)['stage'] = 'done'
    paid_users.add(user_id)
    msg = ("🎉 Құттықтаймыз! Комиссия қабылданды. Каналдарды еш кедергісіз тамашалай аласыз!"
           if lang == 'kz' else
           "🎉 Поздравляем! Комиссия принята. Наслаждайся каналами!")
    await bot.send_message(user_id, msg)
    try:
        await cb.message.edit_caption("✅ Толық расталды")
    except Exception:
        await cb.message.edit_text("✅ Толық расталды")


# Reject
@dp.callback_query(F.data.startswith("rej_"))
async def rej(cb: types.CallbackQuery):
    await cb.answer()                    # ← instant
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    msg = ("❌ Кешіріңіз, төлеміңіз расталмады. Қайта жіберіңіз."
           if lang == 'kz' else
           "❌ Извини, платеж не подтвержден. Отправь заново.")
    await bot.send_message(user_id, msg)
    try:
        await cb.message.edit_caption("❌ Бас тартылды")
    except Exception:
        await cb.message.edit_text("❌ Бас тартылды")


# Discount offer (admin reply)
@dp.callback_query(F.data.startswith("offeryes_"))
async def offer_yes(cb: types.CallbackQuery):
    await cb.answer()                    # ← instant
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    if lang == 'kz':
        await bot.send_message(user_id, f"✅ Жарайды жаным, сол соманы жібер!\n💳 Kaspi: `{KASPI_NUMBER}`", parse_mode="Markdown")
    else:
        await bot.send_message(user_id, f"✅ Хорошо зай, скидывай!\n💳 Kaspi: `{KASPI_NUMBER}`", parse_mode="Markdown")
    await cb.message.edit_text("✅ Жеңілдікке рұқсат берілді.")


@dp.callback_query(F.data.startswith("offerno_"))
async def offer_no(cb: types.CallbackQuery):
    await cb.answer()                    # ← instant
    user_id = int(cb.data.split("_")[1])
    lang = ud(user_id).get('lang', 'kz')
    if lang == 'kz':
        await bot.send_message(user_id, "❌ Жоқ жаным, ренжіме. Толық төлемнен кейін ғана кіре аласың.")
    else:
        await bot.send_message(user_id, "❌ Нет зай, извини. Доступ только после полной оплаты.")
    await cb.message.edit_text("❌ Жеңілдіктен бас тартылды.")


# ─── JOIN REQUEST AUTO-APPROVE ────────────────────────────────────────────────
@dp.chat_join_request()
async def handle_join_request(request: ChatJoinRequest):
    user_id = request.from_user.id
    chat_id = request.chat.id
    if user_id in paid_users or user_id in vip_obzor_users:
        try:
            await bot.approve_chat_join_request(chat_id=chat_id, user_id=user_id)
            print(f"[JOIN] ✅ Approved {user_id} → chat {chat_id}")
        except Exception as e:
            print(f"[JOIN] ❌ Error approving {user_id}: {e}")
    else:
        print(f"[JOIN] ⛔ Not paid — {user_id} in chat {chat_id}")


# ─── AI CHAT ──────────────────────────────────────────────────────────────────
async def call_ai(system_prompt: str, user_text: str, history: list) -> str | None:
    messages = [{"role": "system", "content": system_prompt}]
    messages.extend(history)
    messages.append({"role": "user", "content": user_text})
    headers = {
        "Authorization": f"Bearer {AI_API_KEY}",
        "Content-Type":  "application/json",
        "HTTP-Referer":  "https://aisulu-bot.replit.app",
        "X-Title":       "Aisulu Bot",
    }
    payload = {
        "model":      OPENROUTER_MODEL,
        "messages":   messages,
        "safe_prompt": False,
        "transforms":  [],
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(OPENROUTER_URL, headers=headers, json=payload,
                                    timeout=aiohttp.ClientTimeout(total=30)) as resp:
                raw = await resp.json()
                if resp.status == 200:
                    return raw["choices"][0]["message"]["content"]
                print(f"[AI ERROR] {resp.status}: {raw}")
    except Exception as e:
        print(f"[AI EXCEPTION] {e}")
    return None


@dp.message()
async def ai_handler(message: types.Message):
    if message.from_user.id == ADMIN_ID or not message.text:
        return
    user_id  = message.from_user.id
    d        = ud(user_id)
    lang     = d.get('lang', 'kz')
    stage    = d.get('stage', 'start')
    pack     = d.get('pack', '1_kanal')
    history  = d.setdefault('history', [])
    reply_kb = get_main_kb(lang)

    # Channel content triggers
    triggers = ["не бар", "не кіреді", "қандай", "кандай", "ішінде", "ышынде",
                "что внутри", "какие каналы", "что входит", "внутри", "нелер бар"]
    if stage not in ('wait_2', 'done', 'wait_vip') and any(w in message.text.lower() for w in triggers):
        lines = "\n".join([f"🔥 {v['name']} — {fmt(v['price'])} тг" for v in CHANNELS.values()])
        text = (f"Іште не бар:\n\n{lines}\n\n👇 Қазір төле!" if lang == 'kz'
                else f"Что внутри:\n\n{lines}\n\n👇 Оплачивай сейчас!")
        await message.answer(text, reply_markup=reply_kb)
        return

    comm = "1 777" if pack == '8_kanal' else "1 555"
    lang_rule = ("ҚАЗАҚША сөйле. 'Жаным', 'Күнім' де." if lang == 'kz'
                 else "Говори строго НА РУССКОМ. 'Зай', 'Милый' де.")

    if stage == 'done':
        dynamic = "Клиент төледі. Ештеңе сатпа. Еркін флиртпен сөйлес."
        reply_kb = None
    elif stage == 'wait_vip':
        dynamic = "Клиент VIP ОБЗОР чекін күтуде. Жылы сөзбен шақыр."
        reply_kb = None
    elif stage == 'wait_2':
        if lang == 'kz':
            dynamic = (f"ЕКІНШІ ТӨЛЕМ ({comm} тг) күтілуде. "
                       f"Скидка десе: 'Бұл қазірдің өзінде скидка'. "
                       f"Ақшам жоқ десе: 'Қанша сала аласың?' деп сұра. "
                       f"Нақты сома айтса: [АДМИН] деп қос.")
        else:
            dynamic = (f"ЖДЕМ ВТОРОЙ ПЛАТЕЖ ({comm} тг). "
                       f"Скидка: 'Это уже со скидкой'. "
                       f"Нет денег: 'Сколько можешь скинуть?'. "
                       f"Конкретная сумма: добавь [АДМИН].")
        reply_kb = None
    else:
        dynamic = ("Екінші төлем туралы сұраса — мүлдем жоқ де."
                   if lang == 'kz' else
                   "Про второй платеж — скажи что нет.")

    full_prompt = f"{SYSTEM_PROMPT} {lang_rule}\nМАЎЫЗДЫ: {dynamic}"
    response = await call_ai(full_prompt, message.text, history)

    if response:
        history.append({"role": "user",      "content": message.text})
        history.append({"role": "assistant", "content": response})
        if len(history) > 10:
            d['history'] = history[-10:]

        if "[АДМИН]" in response:
            response = response.replace("[АДМИН]", "").strip()
            akb = InlineKeyboardBuilder()
            akb.add(types.InlineKeyboardButton(text="✅ Иә", callback_data=f"offeryes_{user_id}"))
            akb.add(types.InlineKeyboardButton(text="❌ Жоқ", callback_data=f"offerno_{user_id}"))
            await bot.send_message(ADMIN_ID,
                                   f"🔔 СКИДКА СҰРАУ\n👤 ID: {user_id}\n💬 {message.text}",
                                   reply_markup=akb.as_markup())
        await message.answer(response, reply_markup=reply_kb)
    else:
        fallback = ("Жаным, сәл күте тұрыңыз... 🥰" if lang == 'kz' else "Зай, секунду... 🥰")
        await message.answer(fallback, reply_markup=reply_kb)


# ─── MAIN ────────────────────────────────────────────────────────────────────
async def main():
    webview = os.environ.get("REPLIT_DOMAINS", "localhost:8000").split(",")[0].strip()
    uptime_url = f"https://{webview}/"
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    print("  АЙСҰЛУ БОТ ІСКЕ ҚОСЫЛДЫ ✅")
    print(f"  Flask keep-alive: http://0.0.0.0:8000/")
    print(f"  UptimeRobot URL : {uptime_url}")
    print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    asyncio.create_task(self_ping_loop())   # keep-alive heartbeat (never sleeps)
    await dp.start_polling(bot)

if __name__ == "__main__":
    keep_alive()
    try:
        asyncio.run(main())
    except (KeyboardInterrupt,          SystemExit):
        print("Бот тоқтатылды!")
    except Exception as e:
        print(f"Қате шықты: {e}")

