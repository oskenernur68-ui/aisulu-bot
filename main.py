import asyncio
import logging
import os
import random
import aiohttp

from aiogram import Bot, Dispatcher, types, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import CommandStart

import google.generativeai as genai
import openai

# ================= CONFIG =================

TOKEN = "СЕНІҢ_BOT_TOKEN"
GEMINI_API_KEY = "СЕНІҢ_GEMINI_API"
OPENAI_API_KEY = "СЕНІҢ_OPENAI_API"

KASPI_NUMBER = "4400430232568623"
KASPI_NAME = "Сағынай Е."

PHOTO_START_1 = "https://i.ibb.co/Ngd6czk2/image.jpg"
PHOTO_START_2 = "https://i.ibb.co/XkKmy2ym/image.jpg"
PHOTO_CHANNEL = "https://i.ibb.co/6RSyyL7n/image.jpg"
PHOTO_PAYMENT = "https://i.ibb.co/VW7rKPfn/image.jpg"
PHOTO_FOLLOW = "https://i.ibb.co/4nYWJ63f/image.jpg"

# ================= INIT =================

bot = Bot(token=TOKEN)
dp = Dispatcher()

genai.configure(api_key=GEMINI_API_KEY)
openai.api_key = OPENAI_API_KEY
# ================= USER DATA =================

users = {}

def ud(user_id):
    if user_id not in users:
        users[user_id] = {
            "lang": "kz",
            "history": [],
            "last_action": 0,
            "paid": False
        }
    return users[user_id]


# ================= INTERNET CHECK =================

async def is_internet():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://google.com", timeout=5):
                return True
    except:
        return False


# ================= OFFLINE SCRIPT =================

def offline_reply(text):
    text = text.lower()

    if "баға" in text or "цена" in text:
        return "Жаным, пакет всего 3333 тг 😘 бүгін соңғы баға..."

    if "не бар" in text or "что внутри" in text:
        return "Ішінде 🔥 топ каналдар, блогерлер 😏"

    if "ақша жоқ" in text or "денег нет" in text:
        return "Қанша сала аласың? 😏"

    if "скидка" in text:
        return "Бұл уже скидка 😘"

    if "кейін" in text or "потом" in text:
        return "Кейін кеш болады 😏"

    return "Жаным 😘 кірсең бәрін көресің 🔥"
    # ================= AI SYSTEM =================

# -------- GEMINI --------
async def call_gemini(system_prompt: str, user_text: str, history: list):
    try:
        model = genai.GenerativeModel("gemini-1.5-flash")

        full_text = system_prompt + "\n\n"

        for msg in history:
            role = "Клиент" if msg["role"] == "user" else "Айсұлу"
            full_text += f"{role}: {msg['content']}\n"

        full_text += f"Клиент: {user_text}\nАйсұлу:"

        response = await asyncio.to_thread(
            model.generate_content,
            full_text,
            generation_config={
                "temperature": 0.9,
                "top_p": 0.95,
                "top_k": 40,
                "max_output_tokens": 200,
            }
        )

        if response and hasattr(response, "text"):
            return response.text

        return None

    except Exception as e:
        print("[GEMINI ERROR]", e)
        return None


# -------- GPT (BACKUP) --------
async def call_gpt(system_prompt, user_text, history):
    try:
        messages = [{"role": "system", "content": system_prompt}]

        for msg in history:
            messages.append({
                "role": msg["role"],
                "content": msg["content"]
            })

        messages.append({"role": "user", "content": user_text})

        response = await asyncio.to_thread(
            openai.ChatCompletion.create,
            model="gpt-4o-mini",
            messages=messages,
            temperature=0.9,
            max_tokens=200
        )

        return response.choices[0].message.content

    except Exception as e:
        print("[GPT ERROR]", e)
        return None


# -------- SMART AI (MAIN) --------
async def smart_ai(system_prompt, user_text, history):

    # 1️⃣ Gemini
    try:
        res = await call_gemini(system_prompt, user_text, history)
        if res:
            print("✅ Gemini жауап берді")
            return res
    except Exception as e:
        print("❌ Gemini құлады:", e)

    # 2️⃣ GPT fallback
    try:
        res = await call_gpt(system_prompt, user_text, history)
        if res:
            print("⚡ GPT fallback жұмыс істеді")
            return res
    except Exception as e:
        print("❌ GPT құлады:", e)

    # 3️⃣ FINAL fallback
    text = user_text.lower()

    if "баға" in text or "цена" in text:
        return "Жаным, пакет 3333 тг ғана 😘"

    if "не бар" in text or "что внутри" in text:
        return "Ішінде 🔥 топ каналдар... бір кірсең шықпайсың 😏"

    return "Жаным, сәл күте тұршы... 😘"


# -------- MAIN AI HANDLER --------
SYSTEM_PROMPT = """
Сен Айсұлу деген қызсың.
Мақсатың — клиентпен флирт жасап, оны Telegram канал сатып алуға жеткізу.

Ережелер:
- қысқа жауап бер
- флирт қос 😏
- баға: 3333 тг
- клиентті дожим жаса
- соңында сатып алуға итер
"""

@dp.message()
async def ai_chat(message: types.Message):
    user_id = message.from_user.id
    data = ud(user_id)

    history = data["history"]

    # сақтау
    history.append({"role": "user", "content": message.text})

    # интернет тексеру
    if not await is_internet():
        reply = offline_reply(message.text)
    else:
        reply = await smart_ai(SYSTEM_PROMPT, message.text, history)

    # сақтау
    history.append({"role": "assistant", "content": reply})

    # лимит
    if len(history) > 10:
        history.pop(0)

    await message.answer(reply)
    # ================= UI / BUTTONS =================

def get_main_kb():
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Каналдарды көру", callback_data="show_channels")],
        [InlineKeyboardButton(text="💬 Қызбен сөйлесу", callback_data="chat")]
    ])
    return kb


# ================= START =================

@dp.message(CommandStart())
async def start(message: types.Message):
    user_id = message.from_user.id

    ud(user_id)  # init user

    await bot.send_photo(
        user_id,
        photo=PHOTO_START_1,
        caption="Сәлем жаным 😘 Мен сені күтіп отыр едім..."
    )

    await bot.send_photo(
        user_id,
        photo=PHOTO_START_2,
        caption="Ішіндегі видеоларды көрсең... 😏🔥"
    )

    await message.answer(
        "Дайынсың ба? 😏👇",
        reply_markup=get_main_kb()
    )


# ================= CHANNELS =================

CHANNELS = [
    {"name": "🔥 VIP KANAL 1", "price": 3333},
    {"name": "💋 QAZAQSHA PRIVATE", "price": 3333},
    {"name": "😏 BLOGER SLIV", "price": 3333},
    {"name": "🔥 VIP 2", "price": 3333},
]


@dp.callback_query(F.data == "show_channels")
async def show_channels(cb: types.CallbackQuery):
    await cb.answer()

    text = "🔥 <b>VIP КАНАЛДАР:</b>\n\n"

    for ch in CHANNELS:
        text += f"• {ch['name']} — {ch['price']} тг\n"

    text += "\n👇 Таңда да кір 😏"

    await cb.message.delete()

    await bot.send_photo(
        cb.from_user.id,
        photo=PHOTO_CHANNEL,
        caption=text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=ch["name"], callback_data=f"buy_{i}")]
                for i, ch in enumerate(CHANNELS)
            ]
        )
    )


# ================= BUY =================

@dp.callback_query(F.data.startswith("buy_"))
async def buy_channel(cb: types.CallbackQuery):
    await cb.answer()

    index = int(cb.data.split("_")[1])
    ch = CHANNELS[index]

    text = f"""
📱 {ch['name']}

💰 Баға: {ch['price']} тг

💳 Kaspi:
{KASPI_NUMBER}
👤 {KASPI_NAME}

📸 Чек жіберші 😘
"""

    await bot.send_photo(
        cb.from_user.id,
        photo=PHOTO_PAYMENT,
        caption=text
    ) 
# ================= ADMIN =================

ADMIN_ID = 123456789  # өз ID қой!


# ================= SEND CHECK =================

@dp.message(F.photo)
async def handle_check(message: types.Message):
    user_id = message.from_user.id

    caption = f"""
💸 Жаңа чек!

👤 User: {user_id}
"""

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Қабылдау", callback_data=f"accept_{user_id}"),
            InlineKeyboardButton(text="❌ Бас тарту", callback_data=f"reject_{user_id}")
        ]
    ])

    await bot.send_photo(
        ADMIN_ID,
        photo=message.photo[-1].file_id,
        caption=caption,
        reply_markup=kb
    )

    await message.answer("Чек жіберілді, күте тұр 😘")


# ================= ACCEPT =================

@dp.callback_query(F.data.startswith("accept_"))
async def accept_payment(cb: types.CallbackQuery):
    await cb.answer()

    user_id = int(cb.data.split("_")[1])

    ud(user_id)["paid"] = True

    await bot.send_message(
        user_id,
        "🔥 Төлем қабылданды!\n\nСілтеме:\nhttps://t.me/your_channel"
    )

    await cb.message.edit_text("✅ Қабылданды")


# ================= REJECT =================

@dp.callback_query(F.data.startswith("reject_"))
async def reject_payment(cb: types.CallbackQuery):
    await cb.answer()

    user_id = int(cb.data.split("_")[1])

    await bot.send_message(
        user_id,
        "❌ Чек дұрыс емес, қайта жібер 😘"
    )

    await cb.message.edit_text("❌ Бас тартылды")


# ================= FOLLOW-UP =================

async def follow_up(user_id):
    await asyncio.sleep(300)

    if not ud(user_id)["paid"]:
        await bot.send_photo(
            user_id,
            photo=PHOTO_FOLLOW,
            caption="Жаным, жоғалып кеттің ғой 😔"
        )

    await asyncio.sleep(600)

    if not ud(user_id)["paid"]:
        await bot.send_message(
            user_id,
            "Соңғы шанс 😏 бүгін соңғы баға"
        )


# ================= TRIGGER FOLLOW =================

@dp.message()
async def trigger_follow(message: types.Message):
    asyncio.create_task(follow_up(message.from_user.id))


# ================= FAKE ACTIVITY =================

async def fake_activity():
    while True:
        await asyncio.sleep(random.randint(60, 120))

        for user_id in users:
            try:
                await bot.send_message(
                    user_id,
                    random.choice([
                        "🔥 Біреу сатып алды",
                        "💸 Тағы 1 адам кірді",
                        "😏 Қазір бәрі алып жатыр"
                    ])
                )
            except:
                pass


# ================= START BACKGROUND =================

async def on_startup():
    asyncio.create_task(fake_activity())
    # ================= EXTRA SYSTEM =================

# -------- USER STATS --------
stats = {
    "users": 0,
    "paid": 0
}

def update_stats(user_id):
    if user_id not in users:
        stats["users"] += 1


# -------- REFERRAL SYSTEM --------
def set_ref(user_id, ref_id=None):
    data = ud(user_id)
    if "ref" not in data:
        data["ref"] = ref_id


def add_bonus(user_id):
    data = ud(user_id)
    data["bonus"] = data.get("bonus", 0) + 1


# -------- ADMIN STATS --------
@dp.message(F.text == "/admin")
async def admin_panel(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        return

    text = f"""
📊 Статистика:

👥 Users: {len(users)}
💸 Paid: {sum(1 for u in users.values() if u.get("paid"))}
"""

    await message.answer(text)


# -------- LOGGING --------
def log(msg):
    print(f"[LOG] {msg}")


# -------- ERROR HANDLER --------
@dp.errors()
async def error_handler(update, exception):
    print("ERROR:", exception)
    return True


# ================= KEEP ALIVE (RENDER) =================

from flask import Flask
import threading

app = Flask('')


@app.route('/')
def home():
    return "Bot is alive!"


def run_flask():
    port = int(os.environ.get("PORT", 8000))
    app.run(host='0.0.0.0', port=port)


def keep_alive():
    t = threading.Thread(target=run_flask)
    t.start()


# ================= MAIN RUN =================

async def main():
    keep_alive()

    print("🚀 BOT STARTED")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
