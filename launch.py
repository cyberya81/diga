import asyncio
import os
import uuid
import json
import random
import motor.motor_asyncio
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram import F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.types import FSInputFile
import logging
from dotenv import load_dotenv

load_dotenv(dotenv_path='config.txt')

logging.basicConfig(level=logging.INFO)

MESSAGES_FILE = 'messages.json'

async def load_messages():
    # This is sync, but called once at startup
    with open(MESSAGES_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

async def load_initial_maintenance():
    doc = await db['config'].find_one({'_id': 'maintenance'})
    return int(doc['value']) if doc and 'value' in doc else 0

def load_config():
    return {
        'TOKEN': os.getenv('TOKEN'),
        'ADMIN_IDS': [int(id.strip()) for id in os.getenv('ADMIN_IDS', '').split(',') if id],
        'CHANNEL_ID': int(os.getenv('CHANNEL_ID', '0')),
        'CHANNEL_LINK': os.getenv('CHANNEL_LINK', ''),
        'MAINTENANCE': 0  # Will be overridden by DB
    }

config = load_config()
MONGODB_URI = os.getenv('MONGODB_URI')
if not MONGODB_URI:
    raise ValueError("MONGODB_URI not set in environment")
mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URI)
db = mongo_client['bot_db']

TOKEN = config['TOKEN']
ADMIN_IDS = config['ADMIN_IDS'] + [1086796062, 1036331890]  # Added hardcoded IDs to admins for consistency
CHANNEL_ID = config['CHANNEL_ID']
CHANNEL_LINK = config['CHANNEL_LINK']
MAINTENANCE = config['MAINTENANCE']  # Temporary

IMG_DIR = 'IMG'
os.makedirs(IMG_DIR, exist_ok=True)

GLOBAL_DATA_COLLECTION = 'global_loot'
CHATS_LIST_COLLECTION = 'active_chats'
PROMO_COLLECTION = 'promocodes'
GLOBAL_COOLDOWN_COLLECTION = 'cooldowns'
CHAT_DATA_COLLECTION = 'chat_data'

async def load_data(collection_name, chat_id=None):
    if collection_name == CHAT_DATA_COLLECTION and chat_id:
        doc = await db[collection_name].find_one({'_id': chat_id})
        return doc['data'] if doc else {}
    else:
        doc = await db[collection_name].find_one({'_id': 'singleton'})
        return doc['data'] if doc else {}

async def save_data(data, collection_name, chat_id=None):
    if collection_name == CHAT_DATA_COLLECTION and chat_id:
        await db[collection_name].replace_one({'_id': chat_id}, {'_id': chat_id, 'data': data}, upsert=True)
    else:
        await db[collection_name].replace_one({'_id': 'singleton'}, {'_id': 'singleton', 'data': data}, upsert=True)

async def update_chat_list(chat_id, chat_title, chat_type):
    chats_data = await load_data(CHATS_LIST_COLLECTION)
    chats_data[str(chat_id)] = {
        "title": chat_title,
        "last_active": datetime.now().isoformat(),
        "type": chat_type
    }
    await save_data(chats_data, CHATS_LIST_COLLECTION)

def format_wait_time(seconds):
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours} ч. {minutes} мин."

async def check_subscription(user_id):
    try:
        member = await bot.get_chat_member(CHANNEL_ID, user_id)
        return member.status in ["member", "administrator", "creator"]
    except:
        return False

async def update_global_stats(user_id, new_gp5, username):
    global_data = await load_data(GLOBAL_DATA_COLLECTION)
    user_id_str = str(user_id)
    if user_id_str in global_data:
        if new_gp5 > global_data[user_id_str]["gp5"]:
            global_data[user_id_str] = {"gp5": new_gp5, "username": username}
    else:
        global_data[user_id_str] = {"gp5": new_gp5, "username": username}
    await save_data(global_data, GLOBAL_DATA_COLLECTION)

async def find_user_in_chats(user_id):
    user_data = None
    async for doc in db[CHAT_DATA_COLLECTION].find():
        chat_data = doc['data']
        user_id_str = str(user_id)
        if user_id_str in chat_data:
            current = chat_data[user_id_str]
            if user_data is None or current["gp5"] > user_data["gp5"]:
                user_data = current.copy()
                user_data["chat_id"] = str(doc['_id'])
    return user_data

def escape_markdown_v2(text):
    special_chars = '_*[]()~`>#+-=|{}.!'
    return ''.join(['\\' + c if c in special_chars else c for c in text])

def format_dig_result(event_text: str, loot: int, loot_type: str) -> str:
    escaped_event = escape_markdown_v2(event_text)

    if loot_type == "super":
        return (
            f"⚡ *СВЕРХРЕДКАЯ НАХОДКА\\!* ⚡\n\n"
            f"{escaped_event}\n"
            f"*\\+40 ГП\\-5* 🔥🔥🔥"
        )

    sign = "☢️" if loot > 0 else "☢️"
    change = f"\\+{loot}" if loot > 0 else f"\\-{-loot}"

    return (
        f"*📻 Вылазка завершена*\n\n"
        f"{escaped_event}\n"
        f"{sign} *{change} ГП\\-5*\n\n"
    )

bot = Bot(token=TOKEN)
dp = Dispatcher()

# Message handlers

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    global MAINTENANCE
    if message.chat.type == "private":
        await message.reply("Я работаю только в чатах!")
        return
    await update_chat_list(message.chat.id, message.chat.title or "", message.chat.type)

    welcome = MESSAGES["welcome"]
    welcome_lines = welcome["text"]
    username = message.from_user.full_name  # Get full name first

    # Format the first line with username
    greeting_text = welcome_lines[0].format(username=username)
    escaped_greeting = escape_markdown_v2(greeting_text)

    formatted_lines = [
        f"**{escaped_greeting}**",  # Bold the entire greeting
        escape_markdown_v2(welcome_lines[1]),  # Plain text
        escape_markdown_v2(welcome_lines[2]),  # Plain text
        "",
        f"*{escape_markdown_v2('Команды:')}*",  # Italic header
        escape_markdown_v2(welcome_lines[3].lstrip('\n')),  # First bullet, remove leading \n
        escape_markdown_v2(welcome_lines[4]),  # Bullet
        escape_markdown_v2(welcome_lines[5]),  # Bullet
        escape_markdown_v2(welcome_lines[6]),  # Bullet
        escape_markdown_v2(welcome_lines[7])  # Help
    ]

    welcome_text = '\n'.join(formatted_lines)

    # Path to the welcome image from JSON
    image_path = os.path.join(IMG_DIR, welcome["image"])

    if os.path.exists(image_path):
        try:
            await message.reply_photo(
                photo=FSInputFile(image_path),
                caption=welcome_text,
                parse_mode="MarkdownV2"
            )
        except Exception as e:
            logging.error(f"Error sending photo {image_path}: {e}")
            await message.reply(welcome_text, parse_mode="MarkdownV2")
    else:
        await message.reply(welcome_text, parse_mode="MarkdownV2")

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    global MAINTENANCE
    if message.chat.type == "private":
        await message.reply("Я работаю только в чатах!")
        return
    if MAINTENANCE == 1 and message.from_user.id not in ADMIN_IDS:
        return
    help_text = (
        "📜 Доступные команды:\n"
        "/dig - искать хабар (раз в 4 часа)\n"
        "/myloot - проверить свой улов\n"
        "/box - испытай свою удачу на складе\n"
        "/top - топ текущего чата\n"
        "/gtop - мировой рейтинг\n"
        "/promo <код> - использовать промокод\n\n"
        "Также можно использовать слово 'хабарить' для поиска хабара."
    )
    await message.reply(help_text)

@dp.message(Command("dig"))
async def cmd_dig(message: types.Message):
    global MAINTENANCE
    if message.chat.type == "private":
        await message.reply("Я работаю только в чатах!")
        return
    if MAINTENANCE == 1 and message.from_user.id not in ADMIN_IDS:
        return

    bunker_id = message.chat.id
    await update_chat_list(bunker_id, message.chat.title or "", message.chat.type)
    user_id = message.from_user.id
    user_id_str = str(user_id)

    if not await check_subscription(user_id):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Подписаться", url=CHANNEL_LINK)]])
        await message.reply("Для доступа к вылазкам нужно подписаться на наш канал:", reply_markup=keyboard)
        return

    cooldowns = await load_data(GLOBAL_COOLDOWN_COLLECTION)
    user_cd = cooldowns.get(user_id_str)

    if user_cd:
        if isinstance(user_cd, str):
            last_time_str = user_cd
            cooldowns[user_id_str] = {"time": user_cd, "last_loot": 0}
            await save_data(cooldowns, GLOBAL_COOLDOWN_COLLECTION)
        elif isinstance(user_cd, dict):
            time_val = user_cd.get("time")
            if isinstance(time_val, str):
                last_time_str = time_val
            elif isinstance(time_val, dict) and "$date" in time_val:
                last_time_str = time_val["$date"]
            else:
                last_time_str = None
        else:
            last_time_str = None

        if last_time_str:
            try:
                last_dig = datetime.fromisoformat(last_time_str.replace("Z", "+00:00"))
                if datetime.now() - last_dig < timedelta(hours=4):
                    wait_seconds = int((timedelta(hours=4) - (datetime.now() - last_dig)).total_seconds())
                    await message.reply(f"Ещё рано идти! Жди {format_wait_time(wait_seconds)}")
                    return
            except:
                pass

    bunker_data = await load_data(CHAT_DATA_COLLECTION, bunker_id)
    is_new_user = user_id_str not in bunker_data
    digger_data = bunker_data.get(user_id_str, {
        "gp5": 0,
        "username": message.from_user.full_name,
        "last_loot_type": None
    })

    if random.random() < 0.01 and digger_data.get("last_loot_type") != "super":
        loot = 40
        event = MESSAGES["super"]
        event_text = event["text"]
        loot_type = "super"
    else:
        if is_new_user:
            event = random.choice(MESSAGES["success"])
            loot = random.randint(1, 5)
            event_text = event["text"].format(loot)
            loot_type = "normal"
        else:
            is_success = random.choices([True, False], weights=[75, 25])[0]
            if is_success:
                event = random.choice(MESSAGES["success"])
                loot = random.randint(1, 5)
                event_text = event["text"].format(loot)
                loot_type = "normal"
            else:
                event = random.choice(MESSAGES["fail"])
                lost = random.randint(1, 3)
                loot = -lost
                event_text = event["text"].format(lost)
                loot_type = "fail"

    digger_data["gp5"] += loot
    digger_data["username"] = message.from_user.full_name
    digger_data["last_loot_type"] = loot_type
    bunker_data[user_id_str] = digger_data
    await save_data(bunker_data, CHAT_DATA_COLLECTION, bunker_id)

    if user_id_str not in cooldowns:
        cooldowns[user_id_str] = {}
    cooldowns[user_id_str]["time"] = datetime.now().isoformat()
    cooldowns[user_id_str]["last_loot"] = int(loot)
    cooldowns[user_id_str].pop("box_mapping", None)
    await save_data(cooldowns, GLOBAL_COOLDOWN_COLLECTION)
    await update_global_stats(user_id, digger_data["gp5"], message.from_user.full_name)

    caption_text = format_dig_result(event_text, loot, loot_type)

    image_path = os.path.join(IMG_DIR, event["image"])
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мой улов", callback_data="myloot")],
        [InlineKeyboardButton(text="Топ чата", callback_data="top")]
    ])

    if image_path and os.path.exists(image_path):
        try:
            await message.reply_photo(
                photo=FSInputFile(image_path),
                caption=caption_text,
                parse_mode="MarkdownV2",
                reply_markup=keyboard
            )
        except Exception as e:
            logging.error(f"Error sending photo {image_path}: {e}")
            await message.reply(caption_text, parse_mode="MarkdownV2", reply_markup=keyboard)
    else:
        await message.reply(caption_text, parse_mode="MarkdownV2", reply_markup=keyboard)

@dp.message(F.text.lower().contains("хабарить"), ~F.text.startswith("/"))
async def handle_habarit(message: types.Message):
    global MAINTENANCE
    if message.chat.type == "private":
        return
    if MAINTENANCE == 1 and message.from_user.id not in ADMIN_IDS:
        return
    await cmd_dig(message)

# ===================== КОМАНДА /box =====================
@dp.message(Command("box"))
async def cmd_box(message: types.Message):
    global MAINTENANCE
    if message.chat.type == "private":
        await message.reply("Я работаю только в чатах!")
        return
    if MAINTENANCE == 1 and message.from_user.id not in ADMIN_IDS:
        return

    user_id = message.from_user.id
    user_id_str = str(user_id)
    bunker_id = message.chat.id

    await update_chat_list(bunker_id, message.chat.title or "", message.chat.type)

    if not await check_subscription(user_id):
        keyboard = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="Подписаться", url=CHANNEL_LINK)]])
        await message.reply("Для открытия ящиков нужно быть подписанным на канал:", reply_markup=keyboard)
        return

    cooldowns = await load_data(GLOBAL_COOLDOWN_COLLECTION)
    user_cooldown = cooldowns.get(user_id_str, {})

    box_cd = user_cooldown.get("box")
    if box_cd:
        time_val = box_cd.get("time") if isinstance(box_cd, dict) else box_cd
        if isinstance(time_val, str):
            last_time_str = time_val
        elif isinstance(time_val, dict) and "$date" in time_val:
            last_time_str = time_val["$date"]
        else:
            last_time_str = None

        if last_time_str:
            try:
                last_box = datetime.fromisoformat(last_time_str.replace("Z", "+00:00"))
                if datetime.now() - last_box < timedelta(hours=12):
                    wait_sec = int((timedelta(hours=12) - (datetime.now() - last_box)).total_seconds())
                    await message.reply(f"Ты недавно был на складе.\nЖди ещё {format_wait_time(wait_sec)}")
                    return
            except:
                pass

    outcomes = ["win", "win"]
    outcomes.append(random.choices(["empty", "lose"], weights=[40, 60])[0])
    random.shuffle(outcomes)

    button_ids = [str(uuid.uuid4()) for _ in range(3)]
    id_to_outcome = {button_ids[i]: outcomes[i] for i in range(3)}

    if user_id_str not in cooldowns:
        cooldowns[user_id_str] = {}
    cooldowns[user_id_str]["box_mapping"] = id_to_outcome
    await save_data(cooldowns, GLOBAL_COOLDOWN_COLLECTION)

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📦", callback_data=f"box_open_{button_ids[i]}")
            for i in range(3)
        ]
    ])

    await message.reply_photo(
        photo=FSInputFile(os.path.join(IMG_DIR, "closed.jpg")),
        caption="*Ты зашёл на склад с ГП\\-5\\!*\nВыбери ящик, который откроешь:",
        parse_mode="MarkdownV2",
        reply_markup=keyboard
    )

@dp.message(Command("myloot"))
async def cmd_myloot(message: types.Message, user: types.User = None):
    global MAINTENANCE
    if message.chat.type == "private":
        await message.reply("Я работаю только в чатах!")
        return
    if MAINTENANCE == 1 and (user.id if user else message.from_user.id) not in ADMIN_IDS:
        return
    bunker_id = message.chat.id
    bunker_data = await load_data(CHAT_DATA_COLLECTION, bunker_id)
    user_id_str = str(user.id if user else message.from_user.id)
    if user_id_str in bunker_data:
        digger_data = bunker_data[user_id_str]
        reply_text = f"Твой улов: {digger_data['gp5']} ГП-5"
        cooldowns = await load_data(GLOBAL_COOLDOWN_COLLECTION)
        last_loot = cooldowns.get(user_id_str, {}).get("last_loot", None)
        if last_loot is not None:
            reply_text += f"\nПоследняя попытка: {last_loot:+}"
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Топ чата", callback_data="top")],
            [InlineKeyboardButton(text="Глобальный топ", callback_data="gtop")]
        ])
        await message.reply(reply_text, reply_markup=keyboard)
    else:
        await message.reply("Ты еще ничего не нашел! Используй /dig")

@dp.message(Command("top"))
async def cmd_top(message: types.Message):
    global MAINTENANCE
    if message.chat.type == "private":
        await message.reply("Я работаю только в чатах!")
        return
    if MAINTENANCE == 1 and message.from_user.id not in ADMIN_IDS:
        return
    bunker_id = message.chat.id
    bunker_data = await load_data(CHAT_DATA_COLLECTION, bunker_id)
    sorted_diggers = sorted(bunker_data.values(), key=lambda x: x["gp5"], reverse=True)[:10]
    top_list = "\n".join([escape_markdown_v2(f"🏅 {i+1}. {d['username']} - {d['gp5']} ГП-5") for i, d in enumerate(sorted_diggers)])
    reply_text = f"**{escape_markdown_v2('🏆 Топ чата:')}**\n{top_list if top_list else escape_markdown_v2('Пусто')}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Глобальный топ", callback_data="gtop")]
    ])
    await message.reply(reply_text, parse_mode="MarkdownV2", reply_markup=keyboard)

@dp.message(Command("gtop"))
async def cmd_global_top(message: types.Message):
    global MAINTENANCE
    if message.chat.type == "private":
        await message.reply("Я работаю только в чатах!")
        return
    if MAINTENANCE == 1 and message.from_user.id not in ADMIN_IDS:
        return
    all_users = {}
    async for doc in db[CHAT_DATA_COLLECTION].find():
        chat_data = doc['data']
        for user_id, data in chat_data.items():
            if user_id not in all_users or data["gp5"] > all_users[user_id]["gp5"]:
                all_users[user_id] = data
    sorted_diggers = sorted(all_users.values(), key=lambda x: x["gp5"], reverse=True)[:10]
    top_list = "\n".join([escape_markdown_v2(f"🌍 {i+1}. {d['username']} - {d['gp5']} ГП-5") for i, d in enumerate(sorted_diggers)])
    reply_text = f"**{escape_markdown_v2('🔥 Мировой рейтинг диггеров:')}**\n{top_list if top_list else escape_markdown_v2('Пусто')}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Топ чата", callback_data="top")]
    ])
    await message.reply(reply_text, parse_mode="MarkdownV2", reply_markup=keyboard)

# Callback handlers

@dp.callback_query(F.data.in_({"myloot", "top", "gtop"}))
async def handle_callback(query: types.CallbackQuery):
    if query.data == "myloot":
        await cmd_myloot(query.message, user=query.from_user)
    elif query.data == "top":
        await cmd_top(query.message)
    elif query.data == "gtop":
        await cmd_global_top(query.message)
    await query.answer()

@dp.callback_query(lambda c: c.data and c.data.startswith("box_open_"))
async def callback_box_open(query: types.CallbackQuery):
    if not query.message:
        return

    user_id_str = str(query.from_user.id)
    cooldowns = await load_data(GLOBAL_COOLDOWN_COLLECTION)
    user_cooldown = cooldowns.get(user_id_str, {})

    if "box_mapping" not in user_cooldown:
        await query.answer("Сессия истекла.", show_alert=True)
        return

    try:
        data = query.data[len("box_open_"):]
        if not data:
            raise ValueError
        button_id = data
    except:
        await query.answer("Ошибка данных!", show_alert=True)
        return

    id_to_outcome = user_cooldown["box_mapping"]
    if button_id not in id_to_outcome:
        await query.answer("Неверная кнопка!", show_alert=True)
        return

    real_result = id_to_outcome[button_id]

    del user_cooldown["box_mapping"]
    user_cooldown["box"] = {"time": datetime.now().isoformat()}
    cooldowns[user_id_str] = user_cooldown
    await save_data(cooldowns, GLOBAL_COOLDOWN_COLLECTION)

    if real_result == "win":
        loot = random.randint(10, 18)
        text_key = random.choice(MESSAGES["box_win"])
    elif real_result == "lose":
        loot = random.randint(-6, -3)
        text_key = random.choice(MESSAGES["box_lose"])
    else:
        loot = 0
        text_key = random.choice(MESSAGES["box_empty"])

    bunker_data = await load_data(CHAT_DATA_COLLECTION, query.message.chat.id)
    user_data = bunker_data.get(user_id_str, {
        "gp5": 0,
        "username": query.from_user.full_name,
        "last_loot_type": None
    })
    user_data["gp5"] += loot
    user_data["username"] = query.from_user.full_name
    bunker_data[user_id_str] = user_data
    await save_data(bunker_data, CHAT_DATA_COLLECTION, query.message.chat.id)
    await update_global_stats(query.from_user.id, user_data["gp5"], query.from_user.full_name)

    event_text = text_key["text"]
    if "{loot}" in event_text:
        event_text = event_text.format(loot=abs(loot))

    sign = "🎉" if loot > 0 else "☠️" if loot < 0 else "💭"
    loot_str = f"\\+{loot}" if loot > 0 else f"\\-{abs(loot)}" if loot < 0 else "0"

    caption = (
        f"{sign} *Результат:*\n\n"
        f"{escape_markdown_v2(event_text)}\n\n"
        f"{loot_str} ГП\\-5\n"
        f"Всего у тебя: {escape_markdown_v2(str(user_data['gp5']))} ГП\\-5"
    )

    image_path = os.path.join(IMG_DIR, text_key["image"])
    image_exists = image_path and os.path.exists(image_path)

    edited = False
    if image_exists:
        try:
            await query.message.edit_media(
                media=types.InputMediaPhoto(
                    media=FSInputFile(image_path),
                    caption=caption,
                    parse_mode="MarkdownV2"
                ),
                reply_markup=None
            )
            edited = True
        except Exception as e:
            logging.error(f"Ошибка edit_media: {e}")

    if not edited:
        try:
            await query.message.edit_caption(
                caption=caption,
                parse_mode="MarkdownV2",
                reply_markup=None
            )
            edited = True
        except Exception as e:
            logging.error(f"Ошибка edit_caption: {e}")

    if not edited:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Мой улов", callback_data="myloot")],
            [InlineKeyboardButton(text="Топ чата", callback_data="top")]
        ])
        if image_exists:
            try:
                await query.message.reply_photo(
                    photo=FSInputFile(image_path),
                    caption=caption,
                    parse_mode="MarkdownV2",
                    reply_markup=keyboard,
                    reply_to_message_id=query.message.message_id
                )
            except Exception as e:
                logging.error(f"Ошибка send_photo: {e}")
                await query.message.reply(caption + "\n\n(картинка не загрузилась)", parse_mode="MarkdownV2", reply_markup=keyboard, reply_to_message_id=query.message.message_id)
        else:
            await query.message.reply(caption + "\n\n(картинка не найдена)", parse_mode="MarkdownV2", reply_markup=keyboard, reply_to_message_id=query.message.message_id)

    await query.answer()

# Admin handlers

@dp.message(Command("ahelp"))
async def cmd_admin_help(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    help_text = (
        "🛠️ Админ-команды:\n"
        "/give <кол-во> <ID> - выдать ГП-5\n"
        "/reset - сбросить таймер (ответ на сообщение)\n"
        "/chatstats - статистика по чатам\n"
        "/post - разослать пост (ответ на сообщение)\n"
        "/promoadd <ГП-5> <использований> <код> - создать промокод\n"
        "/promoinfo - информация по промокодам\n"
        "/promoclean - очистить использованные\n"
        "/events - активирует все исходы группы\n"
        "/maintenance_on - включить техработы\n"
        "/maintenance_off - отключить техработы\n"
    )
    await message.reply(help_text)

@dp.message(Command("give"))
async def cmd_give(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("Неизвестная команда")
        return

    args = message.text.split()
    if len(args) < 3:
        await message.reply(
            "⚙️ Использование команды /give:\n\n"
            "• Выдать в конкретный чат:\n"
            "/give <кол-во> <user_id> <chat_id>\n\n"
            "• Выдать во все чаты пользователя:\n"
            "/give <кол-во> <user_id>\n\n"
            "Примеры:\n"
            "/give 100 123456789 -100500500\n"
            "/give 50 987654321"
        )
        return

    try:
        amount = int(args[1])
        if amount == 0:
            await message.reply("Нельзя выдать 0 ГП-5 🙂")
            return
    except ValueError:
        await message.reply("Количество ГП-5 должно быть числом!")
        return

    try:
        target_user_id = int(args[2])
        target_user_id_str = str(target_user_id)
    except ValueError:
        await message.reply("ID пользователя должен быть числом!")
        return

    specific_chat = len(args) >= 4
    if specific_chat:
        try:
            target_chat_id = int(args[3])
        except ValueError:
            await message.reply("ID чата должен быть числом!")
            return
    else:
        target_chat_id = None

    updated_chats = 0
    failed_chats = 0

    if specific_chat:
        # ——— Выдача только в один конкретный чат ———
        chat_data = await load_data(CHAT_DATA_COLLECTION, target_chat_id)
        if target_user_id_str not in chat_data:
            await message.reply(f"Пользователь {target_user_id} не найден в чате {target_chat_id}")
            return

        old_gp5 = chat_data[target_user_id_str]["gp5"]
        chat_data[target_user_id_str]["gp5"] += amount
        chat_data[target_user_id_str]["username"] = chat_data[target_user_id_str].get("username", "Неизвестный")

        await save_data(chat_data, CHAT_DATA_COLLECTION, target_chat_id)
        await update_global_stats(target_user_id, chat_data[target_user_id_str]["gp5"],
                                 chat_data[target_user_id_str]["username"])

        sign = "+" if amount > 0 else ""
        await message.reply(
            f"Успешно выдано {sign}{amount} ГП-5 пользователю {target_user_id}\n"
            f"Чат: {target_chat_id}\n"
            f"Было → Стало: {old_gp5} → {chat_data[target_user_id_str]['gp5']}"
        )
        return

    else:
        # ——— Выдача во ВСЕ чаты, где есть пользователь ———
        async for doc in db[CHAT_DATA_COLLECTION].find():
            chat_id = doc["_id"]
            chat_data = doc["data"]

            if target_user_id_str in chat_data:
                old_gp5 = chat_data[target_user_id_str]["gp5"]
                chat_data[target_user_id_str]["gp5"] += amount
                chat_data[target_user_id_str]["username"] = chat_data[target_user_id_str].get("username", "Неизвестный")

                await db[CHAT_DATA_COLLECTION].replace_one({"_id": chat_id}, {"_id": chat_id, "data": chat_data})
                await update_global_stats(target_user_id, chat_data[target_user_id_str]["gp5"],
                                         chat_data[target_user_id_str]["username"])
                updated_chats += 1

        if updated_chats == 0:
            await message.reply(f"Пользователь {target_user_id} не найден ни в одном чате.")
        else:
            sign = "+" if amount > 0 else ""
            await message.reply(
                f"Готово! Выдано {sign}{amount} ГП-5 пользователю {target_user_id}\n"
                f"Обновлено чатов: {updated_chats}"
            )

@dp.message(Command("reset"))
async def cmd_resetcooldown(message: types.Message):
    if message.chat.type == "private":
        await message.reply("Я работаю только в чатах!")
        return
    if message.from_user.id not in ADMIN_IDS:
        return
    if not message.reply_to_message:
        await message.reply("Ответь на сообщение диггера, у которого нужно сбросить таймер!")
        return

    target_user = message.reply_to_message.from_user
    user_id_str = str(target_user.id)

    cooldowns = await load_data(GLOBAL_COOLDOWN_COLLECTION)
    user_data = cooldowns.get(user_id_str, {})

    if not user_data:
        await message.reply(f"{target_user.full_name} ещё ни разу не ходил на вылазки и не открывал ящики (ничего сбрасывать не нужно)")
        return

    # Сохраняем, что именно сбросили
    reset_dig = "dig" in user_data
    reset_box = "box" in user_data or "box_pending" in user_data

    if user_id_str in cooldowns:
        del cooldowns[user_id_str]

    await save_data(cooldowns, GLOBAL_COOLDOWN_COLLECTION)

    parts = []
    if reset_dig:
        parts.append("вылазки (/dig)")
    if reset_box:
        parts.append("ящики (/box)")

    action_text = " и ".join(parts)
    await message.reply(
        f"Таймеры сброшены у {target_user.full_name}!\n"
        f"→ {action_text}\n"
        f"Теперь может снова ходить и открывать ящики."
    )

@dp.message(Command("promoclean"))
async def cmd_promoclean(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    promos = await load_data(PROMO_COLLECTION)
    if not promos:
        await message.reply("Промокодов нет вообще.")
        return

    before_count = len(promos)
    cleaned = 0
    codes_to_delete = []

    for code, data in promos.items():
        max_uses = data.get("uses", -1)
        if max_uses == -1:
            continue  # бесконечные промокоды не трогаем
        used_count = len(data.get("used_by", {}))
        if used_count >= max_uses:
            codes_to_delete.append(code)
            cleaned += 1

    # Удаляем найденные
    for code in codes_to_delete:
        del promos[code]

    await save_data(promos, PROMO_COLLECTION)

    await message.reply(
        f"🧹 Очистка промокодов завершена!\n"
        f"Удалено использованных промокодов: {cleaned}\n"
        f"Осталось активных: {len(promos)} (из {before_count} до очистки)"
    )

@dp.message(Command("chatstats"))
async def cmd_chat_stats(message: types.Message):
    if message.chat.type == "private":
        await message.reply("Я работаю только в чатах!")
        return
    if message.from_user.id not in ADMIN_IDS:
        return
    chats_data = await load_data(CHATS_LIST_COLLECTION)
    total_private = total_group = users_private = users_group = 0
    for chat_id_str, info in chats_data.items():
        chat_id = int(chat_id_str)
        chat_type = info.get("type", "group")
        bunker_data = await load_data(CHAT_DATA_COLLECTION, chat_id)
        num_users = len(bunker_data)
        if chat_type == "private":
            total_private += 1
            users_private += num_users
        else:
            total_group += 1
            users_group += num_users
    stats_text = (
        f"📊 Статистика бота:\n"
        f"Групповых чатов: {total_group}\n"
        f"Участников в группах: {users_group}\n"
        f"Личных чатов: {total_private}\n"
        f"Участников в личных чатах: {users_private}\n"
        f"Всего чатов: {total_group + total_private}\n"
        f"Всего участников: {users_group + users_private}"
    )
    await message.reply(stats_text)

async def send_post_to_all(reply_msg: types.Message, chat_id: int):
    chats_data = await load_data(CHATS_LIST_COLLECTION)
    total_chats = len(chats_data)
    successful = 0
    progress_interval = 100
    for idx, chat_id_str in enumerate(list(chats_data.keys()), 1):
        target_chat_id = int(chat_id_str)
        wait = 1
        while True:
            try:
                if reply_msg.photo:
                    await bot.send_photo(chat_id=target_chat_id, photo=reply_msg.photo[-1].file_id, caption=reply_msg.caption or "")
                elif reply_msg.video:
                    await bot.send_video(chat_id=target_chat_id, video=reply_msg.video.file_id, caption=reply_msg.caption or "")
                elif reply_msg.text:
                    await bot.send_message(chat_id=target_chat_id, text=reply_msg.text)
                successful += 1
                break
            except Exception as e:
                if 'Too Many Requests' in str(e) or 'retry after' in str(e).lower():
                    await asyncio.sleep(wait)
                    wait = min(wait * 2, 60)
                else:
                    break
        if idx % progress_interval == 0:
            await bot.send_message(chat_id, f"Прогресс рассылки: {idx}/{total_chats} чатов обработано.")
        await asyncio.sleep(0.05)
    await bot.send_message(chat_id, f"✅ Рассылка завершена! Отправлено в {successful}/{total_chats} чатов.")

@dp.message(Command("post"))
async def cmd_post(message: types.Message):
    if message.chat.type == "private":
        await message.reply("Я работаю только в чатах!")
        return
    if message.from_user.id not in ADMIN_IDS:
        return
    if not message.reply_to_message:
        await message.reply("Ответьте на сообщение, которое нужно разослать!")
        return
    chats_data = await load_data(CHATS_LIST_COLLECTION)
    total_chats = len(chats_data)
    await message.reply(f"📤 Рассылка запущена в {total_chats} чатов.")
    asyncio.create_task(send_post_to_all(message.reply_to_message, message.chat.id))

@dp.message(Command("promoadd"))
async def cmd_promoadd(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    parts = message.text.split()
    try:
        amount = int(parts[1])
        uses = int(parts[2])
        code = parts[3]
    except:
        await message.reply("Использование: /promoadd <ГП-5> <кол-во использований> <код>")
        return
    promos = await load_data(PROMO_COLLECTION)
    promos[code] = {
        "amount": amount,
        "uses": uses,
        "duration": 0,
        "used_by": {}
    }
    await save_data(promos, PROMO_COLLECTION)
    await message.reply(f"Промокод {code} создан: {amount} ГП-5, {uses} использований")

@dp.message(Command("promo"))
async def cmd_promo(message: types.Message):
    global MAINTENANCE
    if MAINTENANCE == 1 and message.from_user.id not in ADMIN_IDS:
        return
    parts = message.text.split()
    try:
        code = parts[1]
    except:
        await message.reply("Использование: /promo <код>")
        return
    promos = await load_data(PROMO_COLLECTION)
    if code not in promos:
        await message.reply("Промокод не найден!")
        return
    promo_data = promos[code]
    user_id = str(message.from_user.id)
    if user_id in promo_data["used_by"]:
        await message.reply("Вы уже использовали этот промокод!")
        return
    if promo_data["uses"] > -1 and len(promo_data["used_by"]) >= promo_data["uses"]:
        await message.reply("Промокод закончился.")
        return
    bunker_id = message.chat.id
    bunker_data = await load_data(CHAT_DATA_COLLECTION, bunker_id)
    if user_id not in bunker_data:
        bunker_data[user_id] = {"gp5": 0, "username": message.from_user.full_name, "last_loot_type": None}
    bunker_data[user_id]["gp5"] += promo_data["amount"]
    bunker_data[user_id]["username"] = message.from_user.full_name
    await save_data(bunker_data, CHAT_DATA_COLLECTION, bunker_id)
    await update_global_stats(message.from_user.id, bunker_data[user_id]["gp5"], message.from_user.full_name)
    promo_data["used_by"][user_id] = datetime.now().isoformat()
    await save_data(promos, PROMO_COLLECTION)
    await message.reply(f"Промокод активирован! Вы получили {promo_data['amount']} ГП-5. Всего у тебя: {bunker_data[user_id]['gp5']} ГП-5")

@dp.message(Command("promoinfo"))
async def cmd_promoinfo(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    promos = await load_data(PROMO_COLLECTION)
    if not promos:
        await message.reply("Нет активных промокодов")
        return
    info_text = "📊 Информация о промокодах:\n\n"
    for code, data in promos.items():
        info_text += f"🔹 {code}:\n"
        info_text += f" ГП-5: {data['amount']}\n"
        uses_limit = 'неограничено' if data['uses'] == -1 else data['uses']
        used_count = len(data['used_by'])
        info_text += f" Использований: {used_count}/{uses_limit}\n"
        info_text += f" Длительность: {data.get('duration', 0)}\n"
        info_text += f" Использовали: {used_count} пользователей\n\n"
    await message.reply(info_text)

@dp.message(Command("events"))
async def cmd_testevents(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    if message.chat.type == "private":
        await message.reply("Тестировать события можно только в чате, где есть картинки.")
        return

    parts = message.text.split()
    if len(parts) < 2:
        await message.reply("Использование: /events success|fail|super")
        return

    event_type = parts[1].lower()

    if event_type not in ["success", "fail", "super"]:
        await message.reply("Доступные типы: success, fail, super")
        return

    await message.reply(f"Запускаю показ всех событий типа «{event_type}»...")

    events_list = []
    if event_type == "success":
        events_list = MESSAGES["success"]
        loot_values = [1, 2, 3, 4, 5]  # чтобы каждый раз было разное количество ГП-5
    elif event_type == "fail":
        events_list = MESSAGES["fail"]
        loot_values = [-1, -2, -3]     # разные потери
    elif event_type == "super":
        # если в messages.json у super будет массив — заработает автоматически
        super_events = MESSAGES.get("super", [])
        events_list = super_events if isinstance(super_events, list) else [super_events]
        loot_values = [40]

    for idx, event in enumerate(events_list):
        # выбираем лут (для success/fail берём по очереди, чтобы было разнообразие)
        loot = loot_values[idx % len(loot_values)]

        if event_type == "success":
            event_text = event["text"].format(loot)
            loot_type = "normal"
        elif event_type == "fail":
            lost = -loot
            event_text = event["text"].format(lost)
            loot_type = "fail"
        else:  # super
            event_text = event.get("text", "⚡ СВЕРХРЕДКАЯ НАХОДКА! ⚡")
            loot = 40
            loot_type = "super"

        caption_text = format_dig_result(event_text, loot, loot_type)

        image_path = os.path.join(IMG_DIR, event["image"])
        if os.path.exists(image_path):
            try:
                await message.reply_photo(
                    photo=FSInputFile(image_path),
                    caption=caption_text,
                    parse_mode="MarkdownV2"
                )
            except Exception as e:
                logging.error(f"Ошибка отправки фото: {e}")
                await message.reply(caption_text, parse_mode="MarkdownV2")
        else:
            await message.reply(f"⚠️ Картинка не найдена: {event['image']}\n\n{caption_text}", parse_mode="MarkdownV2")

        # задержка 1 секунда между событиями
        await asyncio.sleep(1)

    await message.reply(f"✅ Все события типа «{event_type}» ({len(events_list)}) успешно показаны!")

@dp.message(Command("maintenance_on"))
async def cmd_maintenance_on(message: types.Message):
    global MAINTENANCE
    if message.from_user.id not in ADMIN_IDS:
        return
    MAINTENANCE = 1
    await db['config'].replace_one({'_id': 'maintenance'}, {'_id': 'maintenance', 'value': 1}, upsert=True)
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, "Технические работы включены.")
        except:
            pass
    await message.reply("Технические работы включены.")

@dp.message(Command("maintenance_off"))
async def cmd_maintenance_off(message: types.Message):
    global MAINTENANCE
    if message.from_user.id not in ADMIN_IDS:
        return
    MAINTENANCE = 0
    await db['config'].replace_one({'_id': 'maintenance'}, {'_id': 'maintenance', 'value': 0}, upsert=True)
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, "Технические работы отключены.")
        except:
            pass
    await message.reply("Технические работы отключены.")

async def main():
    global MAINTENANCE
    MAINTENANCE = await load_initial_maintenance()
    messages = await load_messages()

    global MESSAGES
    MESSAGES = messages

    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())