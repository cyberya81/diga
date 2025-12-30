import asyncio
import uuid
import random
import logging

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, FSInputFile

from utils import (
    load_config, load_messages, BotState,
    escape_markdown_v2, format_wait_time, format_dig_result,
    check_subscription, send_response, is_admin, safe_image_path,
    get_user_rank, format_progress_bar, logger,
    RateLimitMiddleware, MaintenanceMiddleware, StateMiddleware,
    CHAT_DATA_COLLECTION, CHATS_LIST_COLLECTION, PROMO_COLLECTION,
    DIG_COOLDOWN_HOURS, BOX_COOLDOWN_HOURS, escape_number, send_temporary_message,
    format_balance_change
)

from database import (
    db, ensure_singleton_documents, ensure_indexes, migrate_database,
    load_data, save_data, load_initial_maintenance,
    try_claim_dig_cooldown, finish_dig_cooldown, unlock_dig_cooldown,
    try_claim_box_cooldown, atomic_add_gp5, save_box_mapping, claim_box_mapping,
    get_user_cooldown, delete_user_cooldowns, atomic_set_user_data,
    update_chat_list, update_global_stats, get_global_top,
    atomic_use_promo, get_user_profile_data, get_bot_statistics,
    get_admin_user_info
)

config = load_config()
bot = Bot(token=config.token)
dp = Dispatcher()

bot_state = BotState(config=config)

dp.message.middleware(StateMiddleware(bot_state))
dp.callback_query.middleware(StateMiddleware(bot_state))
dp.message.middleware(MaintenanceMiddleware(bot_state))
dp.callback_query.middleware(MaintenanceMiddleware(bot_state))
dp.message.middleware(RateLimitMiddleware(rate_limit=0.5))
dp.callback_query.middleware(RateLimitMiddleware(rate_limit=0.3))


@dp.message(Command("start"))
async def cmd_start(message: types.Message, bot_state: BotState):
    if message.chat.type == "private":
        await message.reply("Я работаю только в групповых чатах!")
        return
    await update_chat_list(message.chat.id, message.chat.title or "", message.chat.type)
    welcome = bot_state.messages.get("welcome", {})
    welcome_lines = welcome.get("text", [])
    if not welcome_lines:
        await message.reply("Добро пожаловать! Используйте /help для списка команд.")
        return
    username = message.from_user.full_name
    greeting_text = welcome_lines[0].format(username=username) if welcome_lines else ""
    escaped_greeting = escape_markdown_v2(greeting_text)
    formatted_lines = [
        f"**{escaped_greeting}**",
        escape_markdown_v2(welcome_lines[1]),
        escape_markdown_v2(welcome_lines[2]),
        "",
        f"*{escape_markdown_v2('Команды:')}*",
        escape_markdown_v2(welcome_lines[3].lstrip('\n')),
        escape_markdown_v2(welcome_lines[4]),
        escape_markdown_v2(welcome_lines[5]),
        escape_markdown_v2(welcome_lines[6]),
        escape_markdown_v2(welcome_lines[7])
    ]
    welcome_text = '\n'.join(formatted_lines)
    await send_response(
        message,
        welcome_text,
        image=welcome.get("image"),
        parse_mode="MarkdownV2"
    )


@dp.message(Command("help"))
async def cmd_help(message: types.Message, bot_state: BotState):
    if message.chat.type == "private":
        await message.reply("Я работаю только в групповых чатах!")
        return
    help_text = (
        "📜 *Доступные команды:*\n\n"
        "• /dig — искать хабар (раз в 4 часа в каждом чате)\n"
        "• /profile — твой профиль\n"
        "• /box — испытай удачу (раз в 12 часов)\n"
        "• /top — топ текущего чата\n"
        "• /gtop — мировой рейтинг\n"
        "• /promo <код> — использовать промокод\n\n"
        "💡 Также можно использовать слово «хабарить» для поиска хабара."
    )
    await message.reply(help_text, parse_mode="Markdown")


@dp.message(Command("dig"))
async def cmd_dig(message: types.Message, bot_state: BotState):
    if message.chat.type == "private":
        await message.reply("Я работаю только в групповых чатах!")
        return

    bunker_id = message.chat.id
    user_id = message.from_user.id
    user_id_str = str(user_id)
    username = message.from_user.full_name

    subscription_task = check_subscription(bot, bot_state.config.channel_id, user_id)
    cooldown_task = try_claim_dig_cooldown(user_id_str, bunker_id, cooldown_hours=DIG_COOLDOWN_HOURS)
    data_task = load_data(CHAT_DATA_COLLECTION, bunker_id)

    is_subscribed, (can_dig, wait_seconds), bunker_data = await asyncio.gather(
        subscription_task, cooldown_task, data_task
    )

    if not is_subscribed:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подписаться", url=bot_state.config.channel_link)]
        ])
        await message.reply(
            "Для доступа к вылазкам нужно подписаться на наш канал:",
            reply_markup=keyboard
        )
        return

    if not can_dig:
        if wait_seconds:
            await message.answer(
                f"Ещё рано выходить\\!\nЖди ещё *{escape_markdown_v2(format_wait_time(wait_seconds))}*",
                parse_mode="MarkdownV2"
            )

    asyncio.create_task(
        update_chat_list(bunker_id, message.chat.title or "", message.chat.type)
    )

    try:
        is_new_user = user_id_str not in bunker_data
        digger_data = bunker_data.get(user_id_str, {
            "gp5": 0,
            "username": username,
            "last_loot_type": None
        })

        # Сохраняем старый баланс
        old_balance = digger_data.get("gp5", 0)

        messages = bot_state.messages
        if random.random() < 0.01 and digger_data.get("last_loot_type") != "super":
            loot = 40
            event = messages.get("super", {"text": "Невероятная находка!", "image": "super.jpg"})
            event_text = event["text"]
            loot_type = "super"
        else:
            if is_new_user:
                event = random.choice(messages.get("success", [{"text": "Нашёл {} ГП-5!", "image": "success.jpg"}]))
                loot = random.randint(1, 5)
                event_text = event["text"].format(loot)
                loot_type = "normal"
            else:
                is_success = random.choices([True, False], weights=[75, 25])[0]
                if is_success:
                    event = random.choice(messages.get("success", [{"text": "Нашёл {} ГП-5!", "image": "success.jpg"}]))
                    loot = random.randint(1, 5)
                    event_text = event["text"].format(loot)
                    loot_type = "normal"
                else:
                    event = random.choice(messages.get("fail", [{"text": "Потерял {} ГП-5!", "image": "fail.jpg"}]))
                    lost = random.randint(1, 3)
                    loot = -lost
                    event_text = event["text"].format(lost)
                    loot_type = "fail"

        digger_data["gp5"] += loot
        digger_data["username"] = username
        digger_data["last_loot_type"] = loot_type

        # Новый баланс
        new_balance = digger_data["gp5"]

        caption_text = format_dig_result(
            event_text, loot, loot_type,
            old_balance=old_balance,
            new_balance=new_balance
        )

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Профиль", callback_data=f"profile_{user_id}")],
            [InlineKeyboardButton(text="Топ чата", callback_data="top")]
        ])

        send_task = send_response(
            message,
            caption_text,
            image=event.get("image"),
            keyboard=keyboard,
            parse_mode="MarkdownV2"
        )

        save_tasks = [
            atomic_set_user_data(bunker_id, user_id_str, digger_data),
            finish_dig_cooldown(user_id_str, bunker_id, loot),
            update_global_stats(user_id, loot, username)
        ]

        await asyncio.gather(send_task, *save_tasks)

        logger.info(
            f"DIG | {username} (@{message.from_user.username}) | "
            f"Chat: {message.chat.title or message.chat.id} | "
            f"Loot: {'+' if loot >= 0 else ''}{loot} | "
            f"Total: {new_balance} GP-5"
        )
    except Exception as e:
        await unlock_dig_cooldown(user_id_str, bunker_id)
        logging.error(f"Error in cmd_dig: {e}")
        raise


@dp.message(F.text.lower().contains("хабарить"), ~F.text.startswith("/"))
async def handle_habarit(message: types.Message, bot_state: BotState):
    if message.chat.type == "private":
        return
    await cmd_dig(message, bot_state)


@dp.message(Command("box"))
async def cmd_box(message: types.Message, bot_state: BotState):
    if message.chat.type == "private":
        await message.reply("Я работаю только в групповых чатах!")
        return

    user_id = message.from_user.id
    user_id_str = str(user_id)
    bunker_id = message.chat.id

    is_subscribed, (can_open, wait_seconds) = await asyncio.gather(
        check_subscription(bot, bot_state.config.channel_id, user_id),
        try_claim_box_cooldown(user_id_str, cooldown_hours=BOX_COOLDOWN_HOURS)
    )

    if not is_subscribed:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Подписаться", url=bot_state.config.channel_link)]
        ])
        await message.reply(
            "Для открытия ящиков нужно быть подписанным на канал:",
            reply_markup=keyboard
        )
        return

    if not can_open:
        if wait_seconds:
            await message.answer(
                f"Ещё рано идти\\! Жди *{escape_markdown_v2(format_wait_time(wait_seconds))}*",
                parse_mode="MarkdownV2"
            )
            return
    asyncio.create_task(
        update_chat_list(bunker_id, message.chat.title or "", message.chat.type)
    )
    outcomes = ["win", "win"]
    outcomes.append(random.choices(["empty", "lose"], weights=[40, 60])[0])
    random.shuffle(outcomes)
    button_ids = [str(uuid.uuid4()) for _ in range(3)]
    id_to_outcome = {button_ids[i]: outcomes[i] for i in range(3)}
    await save_box_mapping(user_id_str, id_to_outcome)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📦", callback_data=f"box_{user_id}_{button_ids[i]}")
            for i in range(3)
        ]
    ])
    image_path = safe_image_path("closed.jpg")
    caption = "*🏭 Ты нашёл схрон с ГП\\-5\\!*\n\nВыбери ящик, который откроешь:"
    if image_path:
        await message.reply_photo(
            photo=FSInputFile(image_path),
            caption=caption,
            parse_mode="MarkdownV2",
            reply_markup=keyboard
        )
    else:
        await message.reply(
            caption,
            parse_mode="MarkdownV2",
            reply_markup=keyboard
        )


@dp.message(Command("myloot"))
async def cmd_myloot(message: types.Message, bot_state: BotState):
    await cmd_profile(message, bot_state)


@dp.message(Command("profile"))
async def cmd_profile(message: types.Message, bot_state: BotState, target_user: types.User = None):
    if message.chat.type == "private":
        await message.reply("Я работаю только в групповых чатах!")
        return

    chat_id = message.chat.id
    user = target_user or message.from_user
    user_id_str = str(user.id)

    profile = await get_user_profile_data(chat_id, user_id_str)

    if not profile["exists_in_chat"] and not profile["exists_globally"]:
        await message.reply(
            "❌ Ты ещё не начал игру\\!\n"
            "Используй /dig чтобы отправиться на вылазку",
            parse_mode="MarkdownV2"
        )
        return

    global_gp5 = profile["global_gp5"]
    chat_gp5 = profile["chat_gp5"]
    rank = get_user_rank(global_gp5, bot_state.messages)
    username = escape_markdown_v2(profile["username"])

    if profile["chat_position"]:
        position_text = f"*{profile['chat_position']}* из {profile['chat_total']}"
        if profile["chat_position"] == 1:
            position_emoji = "🥇"
        elif profile["chat_position"] == 2:
            position_emoji = "🥈"
        elif profile["chat_position"] == 3:
            position_emoji = "🥉"
        else:
            position_emoji = "📍"
    else:
        position_text = "—"
        position_emoji = "📍"

    if rank["next_rank"]:
        next_rank_name = escape_markdown_v2(rank["next_rank"]["name"])
        next_rank_min = rank["next_rank"]["min_gp5"]
        progress_bar = escape_markdown_v2(format_progress_bar(rank["progress"]))
        gp5_needed = next_rank_min - global_gp5
        progress_text = (
            f"\n\n📈 *До следующего ранга:*\n"
            f"└ {progress_bar} {rank['progress']}%\n"
            f"└ Осталось: *{escape_number(gp5_needed)}* ГП\\-5"
        )
    else:
        progress_text = "\n\n⭐ *Максимальный ранг достигнут\\!*"

    rank_name = escape_markdown_v2(rank["name"])
    rank_emoji = rank["emoji"]

    last_loot = profile.get("last_loot")
    if last_loot is not None:
        if last_loot > 0:
            last_loot_text = f"\n🎯 *Последняя вылазка:* \\+{last_loot} ГП\\-5"
        elif last_loot < 0:
            last_loot_text = f"\n🎯 *Последняя вылазка:* \\-{abs(last_loot)} ГП\\-5"
        else:
            last_loot_text = f"\n🎯 *Последняя вылазка:* 0 ГП\\-5"
    else:
        last_loot_text = ""

    profile_text = (
        f"{rank_emoji} *Профиль: {username}*\n\n"
        f"🎖️ *Ранг:* {rank_name}\n\n"
        f"☢️ *ГП\\-5 в этом чате:* {escape_number(chat_gp5)}\n"
        f"🌍 *ГП\\-5 всего:* {escape_number(global_gp5)}"
        f"{last_loot_text}\n\n"
        f"{position_emoji} *Место в чате:* {position_text}"
        f"{progress_text}"
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="Топ чата", callback_data="top"),
            InlineKeyboardButton(text="Глобальный", callback_data="gtop")
        ],
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"profile_{user.id}")
        ]
    ])

    await send_response(
        message,
        profile_text,
        image=rank.get("image"),
        keyboard=keyboard,
        parse_mode="MarkdownV2"
    )


@dp.callback_query(F.data.startswith("profile_"))
async def callback_profile(query: types.CallbackQuery, bot_state: BotState):
    try:
        callback_user_id = int(query.data.split("_")[1])
    except (IndexError, ValueError):
        await query.answer("❌ Ошибка данных", show_alert=True)
        return
    if query.from_user.id != callback_user_id:
        await query.answer("❌ Это не твой профиль!", show_alert=True)
        return
    await query.answer("🔄 Обновляю...")
    await cmd_profile(query.message, bot_state, target_user=query.from_user)


@dp.message(Command("top"))
async def cmd_top(message: types.Message, bot_state: BotState):
    if message.chat.type == "private":
        await message.reply("Я работаю только в групповых чатах!")
        return
    bunker_id = message.chat.id
    bunker_data = await load_data(CHAT_DATA_COLLECTION, bunker_id)
    sorted_diggers = sorted(
        bunker_data.values(),
        key=lambda x: x.get("gp5", 0),
        reverse=True
    )[:10]

    def escape_gp5(n: int) -> str:
        if n < 0:
            return f"\\-{abs(n)}"
        return str(n)

    if sorted_diggers:
        top_lines = []
        for i, d in enumerate(sorted_diggers):
            medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "🏅"
            username = d.get('username', 'Unknown')
            gp5 = d.get('gp5', 0)
            top_lines.append(f"{medal} {i + 1}\\. {escape_markdown_v2(username)} — *{escape_gp5(gp5)}* ГП\\-5")
        top_list = "\n".join(top_lines)
    else:
        top_list = escape_markdown_v2("Пока пусто...")
    reply_text = f"*Топ чата:*\n\n{top_list}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Глобальный топ", callback_data="gtop")]
    ])
    await message.reply(reply_text, parse_mode="MarkdownV2", reply_markup=keyboard)


@dp.message(Command("gtop"))
async def cmd_global_top(message: types.Message, bot_state: BotState):
    if message.chat.type == "private":
        await message.reply("Я работаю только в групповых чатах!")
        return
    top_users = await get_global_top(10)

    def escape_gp5(n: int) -> str:
        if n < 0:
            return f"\\-{abs(n)}"
        return str(n)

    if top_users:
        top_lines = []
        for i, d in enumerate(top_users):
            medal = "🥇" if i == 0 else "🥈" if i == 1 else "🥉" if i == 2 else "🌍"
            username = d.get('username', 'Unknown')
            gp5 = d.get('gp5', 0)
            top_lines.append(f"{medal} {i + 1}\\. {escape_markdown_v2(username)} — *{escape_gp5(gp5)}* ГП\\-5")
        top_list = "\n".join(top_lines)
    else:
        top_list = escape_markdown_v2("Пока пусто...")
    reply_text = f"*🔥 Мировой рейтинг диггеров:*\n\n{top_list}"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Топ чата", callback_data="top")]
    ])
    await message.reply(reply_text, parse_mode="MarkdownV2", reply_markup=keyboard)


@dp.callback_query(F.data == "top")
async def callback_top(query: types.CallbackQuery, bot_state: BotState):
    await cmd_top(query.message, bot_state)
    await query.answer()


@dp.callback_query(F.data == "gtop")
async def callback_gtop(query: types.CallbackQuery, bot_state: BotState):
    await cmd_global_top(query.message, bot_state)
    await query.answer()


@dp.callback_query(F.data.startswith("myloot_"))
async def callback_myloot(query: types.CallbackQuery, bot_state: BotState):
    try:
        callback_user_id = int(query.data.split("_")[1])
    except (IndexError, ValueError):
        await query.answer("❌ Ошибка данных", show_alert=True)
        return
    if query.from_user.id != callback_user_id:
        await query.answer("❌ Это не твоя кнопка!", show_alert=True)
        return
    await cmd_profile(query.message, bot_state, target_user=query.from_user)
    await query.answer()


@dp.callback_query(F.data.startswith("box_"))
async def callback_box_open(query: types.CallbackQuery, bot_state: BotState):
    if not query.message:
        return

    try:
        parts = query.data.split("_")
        if len(parts) != 3:
            raise ValueError("Invalid format")
        owner_user_id = int(parts[1])
        button_id = parts[2]
    except (IndexError, ValueError):
        await query.answer("Ошибка данных!", show_alert=True)
        return

    if query.from_user.id != owner_user_id:
        await query.answer("Это не твой ящик!", show_alert=True)
        return

    user_id_str = str(query.from_user.id)
    chat_id = query.message.chat.id
    username = query.from_user.full_name

    outcome = await claim_box_mapping(user_id_str, button_id)
    if outcome is None:
        await query.answer("Ты уже открыл ящик!", show_alert=True)
        return

    await query.answer()

    messages = bot_state.messages
    if outcome == "win":
        loot = random.randint(10, 18)
        text_key = random.choice(messages.get("box_win", [{"text": "Ты нашёл {loot} ГП-5!", "image": "box_win.jpg"}]))
    elif outcome == "lose":
        loot = random.randint(-6, -3)
        text_key = random.choice(messages.get("box_lose", [{"text": "Потерял {loot} ГП-5!", "image": "box_lose.jpg"}]))
    else:
        loot = 0
        text_key = random.choice(messages.get("box_empty", [{"text": "Пусто...", "image": "box_empty.jpg"}]))

    new_gp5 = await atomic_add_gp5(chat_id, user_id_str, loot, username)
    asyncio.create_task(update_global_stats(query.from_user.id, loot, username))

    # Вычисляем старый баланс
    old_gp5 = new_gp5 - loot

    event_text = text_key["text"]
    if "{loot}" in event_text:
        event_text = event_text.format(loot=abs(loot))

    if loot > 0:
        sign = ""
        loot_str = f"\\+{loot}"
    elif loot < 0:
        sign = ""
        loot_str = f"\\-{abs(loot)}"
    else:
        sign = ""
        loot_str = "0"

    caption = (
        f"*📻 Результат:*\n\n"
        f"{escape_markdown_v2(event_text)}\n\n"
        f"*☢️{loot_str} ГП\\-5*\n"
        f"{format_balance_change(old_gp5, new_gp5)} ГП\\-5"
    )
    image_path = safe_image_path(text_key.get("image"))
    edited = False
    if image_path:
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
            logging.error(f"Error edit_media: {e}")
    if not edited:
        try:
            await query.message.edit_caption(
                caption=caption,
                parse_mode="MarkdownV2",
                reply_markup=None
            )
            edited = True
        except Exception as e:
            logging.error(f"Error edit_caption: {e}")
    if not edited:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Профиль", callback_data=f"profile_{query.from_user.id}")],
            [InlineKeyboardButton(text="Топ чата", callback_data="top")]
        ])
        await query.message.reply(
            caption,
            parse_mode="MarkdownV2",
            reply_markup=keyboard
        )
    logger.info(
        f"BOX | {username} (@{query.from_user.username}) | "
        f"Chat: {query.message.chat.title or chat_id} | "
        f"Result: {outcome} | Loot: {'+' if loot >= 0 else ''}{loot} | "
        f"Total: {new_gp5} GP-5"
    )


@dp.message(Command("ahelp"))
async def cmd_admin_help(message: types.Message, bot_state: BotState):
    if not is_admin(message.from_user.id, bot_state):
        return
    help_text = (
        "🛠 *Админ\\-команды:*\n\n"
        "📌 /give \\<кол\\-во\\> \\<ID\\> \\[chat\\_id\\] — выдать ГП\\-5\n"
        "📌 /reset — сбросить таймеры \\(ответ на сообщение\\)\n"
        "📌 /info — информация об игроке\n"
        "📌 /chatstats — статистика по чатам\n"
        "📌 /post — разослать пост \\(ответ на сообщение\\)\n\n"
        "🎟 /promoadd \\<ГП\\-5\\> \\<использований\\> \\<код\\> — создать промокод\n"
        "🎟 /promoinfo — информация по промокодам\n"
        "🎟 /promoclean — очистить использованные\n\n"
        "🔧 /events \\<success\\|fail\\|super\\> — показать события\n"
        "🔧 /maintenance\\_on — включить техработы\n"
        "🔧 /maintenance\\_off — отключить техработы"
    )
    await message.reply(help_text, parse_mode="MarkdownV2")


@dp.message(Command("info"))
async def cmd_info(message: types.Message, bot_state: BotState):
    if not is_admin(message.from_user.id, bot_state):
        return
    args = message.text.split()
    if len(args) < 2:
        await message.reply(
            "💡 *Использование:*\n"
            "`/info <user_id>`\n\n"
            "*Пример:*\n"
            "`/info 123456789`",
            parse_mode="Markdown"
        )
        return
    try:
        target_user_id = int(args[1])
        target_user_id_str = str(target_user_id)
    except ValueError:
        await message.reply("❌ ID пользователя должен быть числом!")
        return
    info = await get_admin_user_info(target_user_id_str)
    if not info["exists_globally"] and info["chats_count"] == 0:
        await message.reply(
            f"❌ Пользователь `{target_user_id}` не найден в базе данных",
            parse_mode="Markdown"
        )
        return

    def escape_num(n: int) -> str:
        if n < 0:
            return f"\\-{abs(n)}"
        return str(n)

    global_gp5 = info["global_gp5"]
    rank = get_user_rank(global_gp5, bot_state.messages)
    username = escape_markdown_v2(info["username"])
    rank_name = escape_markdown_v2(rank["name"])
    rank_emoji = rank["emoji"]
    cooldown_data = info.get("cooldown_data", {})
    dig_data = cooldown_data.get("dig", {})
    last_loots = []
    for chat_id_str, dig_info in dig_data.items():
        last_loot = dig_info.get("last_loot")
        if last_loot is not None:
            last_loots.append((chat_id_str, last_loot))
    if last_loots:
        last_loots_text = "\n\n📊 *Последние вылазки по чатам:*\n"
        for chat_id_str, loot in last_loots[-5:]:
            if loot > 0:
                loot_str = f"\\+{loot}"
            elif loot < 0:
                loot_str = f"\\-{abs(loot)}"
            else:
                loot_str = "0"
            escaped_chat_id = escape_markdown_v2(str(chat_id_str))
            last_loots_text += f"└ `{escaped_chat_id}`: *{loot_str}* ГП\\-5\n"
    else:
        last_loots_text = ""
    if rank["next_rank"]:
        next_rank_name = escape_markdown_v2(rank["next_rank"]["name"])
        next_rank_min = rank["next_rank"]["min_gp5"]
        progress_bar = escape_markdown_v2(format_progress_bar(rank["progress"]))
        gp5_needed = next_rank_min - global_gp5
        progress_text = (
            f"\n\n📈 *До ранга {next_rank_name}:*\n"
            f"└ {progress_bar} {rank['progress']}%\n"
            f"└ Осталось: *{escape_num(gp5_needed)}* ГП\\-5"
        )
    else:
        progress_text = "\n\n⭐ *Максимальный ранг достигнут\\!*"
    info_text = (
        f"{rank_emoji} *Профиль \\(админ\\): {username}*\n"
        f"🆔 ID: `{target_user_id}`\n\n"
        f"🎖️ *Ранг:* {rank_name}\n\n"
        f"🌍 *ГП\\-5 \\(лучший результат\\):* {escape_num(global_gp5)}\n"
        f"💬 *Активных чатов:* {info['chats_count']}\n"
        f"📦 *Сумма ГП\\-5 по всем чатам:* {escape_num(info['total_gp5_sum'])}"
        f"{last_loots_text}"
        f"{progress_text}"
    )
    await send_response(
        message,
        info_text,
        image=rank.get("image"),
        parse_mode="MarkdownV2"
    )

@dp.message(Command("give"))
async def cmd_give(message: types.Message, bot_state: BotState):
    if not is_admin(message.from_user.id, bot_state):
        await message.reply("❌ Неизвестная команда")
        return

    args = message.text.split()
    if len(args) < 3:
        await message.reply(
            "⚙️ *Использование команды /give:*\n\n"
            "• Выдать в конкретный чат:\n"
            "`/give <кол-во> <user_id> <chat_id>`\n\n"
            "• Выдать во все чаты пользователя:\n"
            "`/give <кол-во> <user_id>`\n\n"
            "*Примеры:*\n"
            "`/give 100 123456789 -100500500`\n"
            "`/give 50 987654321`",
            parse_mode="Markdown"
        )
        return

    try:
        amount = int(args[1])
        if amount == 0:
            await message.reply("❌ Нельзя выдать 0 ГП-5")
            return
    except ValueError:
        await message.reply("❌ Количество ГП-5 должно быть числом!")
        return

    try:
        target_user_id = int(args[2])
        target_user_id_str = str(target_user_id)
    except ValueError:
        await message.reply("❌ ID пользователя должен быть числом!")
        return

    specific_chat = len(args) >= 4
    target_chat_id = None

    if specific_chat:
        try:
            target_chat_id = int(args[3])
        except ValueError:
            await message.reply("❌ ID чата должен быть числом!")
            return

    if specific_chat and target_chat_id:
        # Выдача в конкретный чат
        chat_data = await load_data(CHAT_DATA_COLLECTION, target_chat_id)
        if target_user_id_str not in chat_data:
            await message.reply(
                f"❌ Пользователь `{target_user_id}` не найден в чате `{target_chat_id}`",
                parse_mode="Markdown"
            )
            return

        old_gp5 = chat_data[target_user_id_str].get("gp5", 0)
        chat_data[target_user_id_str]["gp5"] = old_gp5 + amount
        username = chat_data[target_user_id_str].get("username", "Неизвестный")

        await save_data(chat_data, CHAT_DATA_COLLECTION, target_chat_id)
        await update_global_stats(target_user_id, amount, username)  # Передаём дельту

        sign = "+" if amount > 0 else ""
        await message.reply(
            f"✅ Успешно выдано *{sign}{amount}* ГП-5\n"
            f"👤 Пользователь: `{target_user_id}`\n"
            f"💬 Чат: `{target_chat_id}`\n"
            f"📊 Было → Стало: *{old_gp5}* → *{chat_data[target_user_id_str]['gp5']}*",
            parse_mode="Markdown"
        )
    else:
        # Выдача во все чаты пользователя
        updated_chats = 0
        username = "Неизвестный"

        async for doc in db[CHAT_DATA_COLLECTION].find():
            chat_id = doc["_id"]
            chat_data = doc.get("data", {})

            if target_user_id_str in chat_data:
                chat_data[target_user_id_str]["gp5"] = chat_data[target_user_id_str].get("gp5", 0) + amount
                username = chat_data[target_user_id_str].get("username", username)

                await db[CHAT_DATA_COLLECTION].replace_one(
                    {"_id": chat_id},
                    {"_id": chat_id, "data": chat_data}
                )
                updated_chats += 1

        if updated_chats == 0:
            await message.reply(
                f"❌ Пользователь `{target_user_id}` не найден ни в одном чате",
                parse_mode="Markdown"
            )
        else:
            # Общая дельта = amount * количество чатов
            total_delta = amount * updated_chats
            await update_global_stats(target_user_id, total_delta, username)

            sign = "+" if amount > 0 else ""
            await message.reply(
                f"✅ Готово! Выдано *{sign}{amount}* ГП-5 в каждый чат\n"
                f"👤 Пользователь: `{target_user_id}`\n"
                f"📊 Обновлено чатов: *{updated_chats}*\n"
                f"💰 Общая дельта: *{sign}{total_delta}* ГП-5",
                parse_mode="Markdown"
            )


@dp.message(Command("recalc"))
async def cmd_recalc(message: types.Message, bot_state: BotState):
    """Принудительный пересчёт глобальной статистики"""
    if not is_admin(message.from_user.id, bot_state):
        return

    status_msg = await message.reply("🔄 Пересчитываю глобальную статистику...")

    try:
        from database import recalculate_global_stats
        count = await recalculate_global_stats()
        await status_msg.edit_text(
            f"✅ *Пересчёт завершён\\!*\n\n"
            f"👥 Обновлено пользователей: *{count}*",
            parse_mode="MarkdownV2"
        )
    except Exception as e:
        logging.error(f"Recalc error: {e}")
        await status_msg.edit_text(f"❌ Ошибка: {e}")


@dp.message(Command("reset"))
async def cmd_resetcooldown(message: types.Message, bot_state: BotState):
    if message.chat.type == "private":
        await message.reply("Я работаю только в групповых чатах!")
        return
    if not is_admin(message.from_user.id, bot_state):
        return
    if not message.reply_to_message:
        await message.reply("💡 Ответь на сообщение диггера, у которого нужно сбросить таймеры!")
        return
    target_user = message.reply_to_message.from_user
    user_id_str = str(target_user.id)
    user_data = await get_user_cooldown(user_id_str)
    if not user_data:
        await message.reply(
            f"ℹ️ *{escape_markdown_v2(target_user.full_name)}* ещё ни разу не ходил на вылазки "
            "и не открывал ящики \\(нечего сбрасывать\\)",
            parse_mode="MarkdownV2"
        )
        return
    reset_dig = "dig" in user_data
    reset_box = "box" in user_data
    await delete_user_cooldowns(user_id_str)
    parts = []
    if reset_dig:
        parts.append("вылазки (/dig)")
    if reset_box:
        parts.append("ящики (/box)")
    action_text = " и ".join(parts) if parts else "все таймеры"
    await message.reply(
        f"✅ Таймеры сброшены у *{escape_markdown_v2(target_user.full_name)}*\\!\n\n"
        f"🔄 Сброшено: {escape_markdown_v2(action_text)}\n"
        f"Теперь может снова ходить и открывать ящики\\.",
        parse_mode="MarkdownV2"
    )


@dp.message(Command("promoclean"))
async def cmd_promoclean(message: types.Message, bot_state: BotState):
    if not is_admin(message.from_user.id, bot_state):
        return
    promos = await load_data(PROMO_COLLECTION)
    if not promos:
        await message.reply("ℹ️ Промокодов нет вообще.")
        return
    before_count = len(promos)
    codes_to_delete = []
    for code, data in promos.items():
        max_uses = data.get("uses", -1)
        if max_uses == -1:
            continue
        used_count = len(data.get("used_by", {}))
        if used_count >= max_uses:
            codes_to_delete.append(code)
    for code in codes_to_delete:
        del promos[code]
    await save_data(promos, PROMO_COLLECTION)
    await message.reply(
        f"🧹 *Очистка промокодов завершена\\!*\n\n"
        f"🗑 Удалено: *{len(codes_to_delete)}*\n"
        f"📋 Осталось: *{len(promos)}* \\(было {before_count}\\)",
        parse_mode="MarkdownV2"
    )


@dp.message(Command("chatstats"))
async def cmd_chat_stats(message: types.Message, bot_state: BotState):
    if not is_admin(message.from_user.id, bot_state):
        return
    loading_msg = await message.reply("📊 Собираю статистику...")
    try:
        stats = await get_bot_statistics()

        def fmt(n):
            formatted = f"{n:,}".replace(",", " ")
            return escape_markdown_v2(formatted)

        def fmt_float(n):
            return escape_markdown_v2(str(n))

        unique = stats["unique_players"]
        chats = stats["active_chats"]
        records = stats["total_player_records"]
        max_in_chat = stats["max_players_in_chat"]
        avg_per_chat = stats["avg_players_per_chat"]
        top = stats["top_player"]
        avg_chats_per_player = round(records / unique, 1) if unique > 0 else 0
        stats_text = (
            f"📊 *Статистика бота*\n\n"
            f"👥 *Игроки:*\n"
            f"├ Уникальных игроков: *{fmt(unique)}*\n"
            f"├ Записей игрок\\-чат: *{fmt(records)}*\n"
            f"└ Среднее чатов на игрока: *{fmt_float(avg_chats_per_player)}*\n\n"
            f"💬 *Чаты:*\n"
            f"├ Активных чатов: *{fmt(chats)}*\n"
            f"├ Макс\\. игроков в чате: *{fmt(max_in_chat)}*\n"
            f"└ Среднее игроков в чате: *{fmt_float(avg_per_chat)}*"
        )
        if top:
            top_name = escape_markdown_v2(top.get("username", "Unknown"))
            top_gp5 = fmt(top.get("gp5", 0))
            stats_text += (
                f"\n\n🏆 *Лидер:*\n"
                f"└ {top_name} — *{top_gp5}* ГП\\-5"
            )
        await loading_msg.edit_text(stats_text, parse_mode="MarkdownV2")
    except Exception as e:
        logging.error(f"Error in chatstats: {e}")
        await loading_msg.edit_text("❌ Ошибка при сборе статистики")


async def send_post_to_all(reply_msg: types.Message, admin_chat_id: int):
    chats_data = await load_data(CHATS_LIST_COLLECTION)
    total_chats = len(chats_data)
    successful = 0
    failed = 0
    progress_interval = 100
    for idx, chat_id_str in enumerate(list(chats_data.keys()), 1):
        try:
            target_chat_id = int(chat_id_str)
        except ValueError:
            failed += 1
            continue
        wait = 1
        max_retries = 3
        retries = 0
        while retries < max_retries:
            try:
                if reply_msg.photo:
                    await bot.send_photo(
                        chat_id=target_chat_id,
                        photo=reply_msg.photo[-1].file_id,
                        caption=reply_msg.caption or ""
                    )
                elif reply_msg.video:
                    await bot.send_video(
                        chat_id=target_chat_id,
                        video=reply_msg.video.file_id,
                        caption=reply_msg.caption or ""
                    )
                elif reply_msg.text:
                    await bot.send_message(chat_id=target_chat_id, text=reply_msg.text)
                successful += 1
                break
            except Exception as e:
                error_str = str(e).lower()
                if 'too many requests' in error_str or 'retry after' in error_str:
                    await asyncio.sleep(wait)
                    wait = min(wait * 2, 60)
                    retries += 1
                else:
                    failed += 1
                    break
        if idx % progress_interval == 0:
            await bot.send_message(
                admin_chat_id,
                f"📤 Прогресс: {idx}/{total_chats} чатов..."
            )
        await asyncio.sleep(0.05)
    await bot.send_message(
        admin_chat_id,
        f"✅ *Рассылка завершена\\!*\n\n"
        f"📨 Успешно: *{successful}*\n"
        f"❌ Ошибок: *{failed}*\n"
        f"📊 Всего: *{total_chats}*",
        parse_mode="MarkdownV2"
    )


@dp.message(Command("post"))
async def cmd_post(message: types.Message, bot_state: BotState):
    if message.chat.type == "private":
        await message.reply("Я работаю только в групповых чатах!")
        return
    if not is_admin(message.from_user.id, bot_state):
        return
    if not message.reply_to_message:
        await message.reply("💡 Ответьте на сообщение, которое нужно разослать!")
        return
    chats_data = await load_data(CHATS_LIST_COLLECTION)
    total_chats = len(chats_data)
    await message.reply(f"📤 Рассылка запущена в *{total_chats}* чатов...", parse_mode="Markdown")
    asyncio.create_task(send_post_to_all(message.reply_to_message, message.chat.id))


@dp.message(Command("promoadd"))
async def cmd_promoadd(message: types.Message, bot_state: BotState):
    if not is_admin(message.from_user.id, bot_state):
        return
    parts = message.text.split()
    try:
        amount = int(parts[1])
        uses = int(parts[2])
        code = parts[3].upper().strip()
        if len(code) < 3 or len(code) > 20:
            await message.reply("❌ Код должен быть от 3 до 20 символов")
            return
        if not code.replace('_', '').isalnum():
            await message.reply("❌ Код может содержать только буквы, цифры и _")
            return
    except (IndexError, ValueError):
        await message.reply(
            "💡 *Использование:*\n"
            "`/promoadd <ГП-5> <кол-во использований> <код>`\n\n"
            "*Пример:*\n"
            "`/promoadd 50 100 NEWYEAR2024`",
            parse_mode="Markdown"
        )
        return
    from datetime import datetime
    promos = await load_data(PROMO_COLLECTION)
    promos[code] = {
        "amount": amount,
        "uses": uses,
        "duration": 0,
        "used_by": {},
        "created_at": datetime.now().isoformat()
    }
    await save_data(promos, PROMO_COLLECTION)
    uses_text = "неограничено" if uses == -1 else str(uses)
    await message.reply(
        f"✅ *Промокод создан\\!*\n\n"
        f"🎟 Код: `{escape_markdown_v2(code)}`\n"
        f"💰 Награда: *{amount}* ГП\\-5\n"
        f"🔢 Использований: *{escape_markdown_v2(uses_text)}*",
        parse_mode="MarkdownV2"
    )


@dp.message(Command("promo"))
async def cmd_promo(message: types.Message, bot_state: BotState):
    parts = message.text.split()
    try:
        code = parts[1].upper().strip()
    except IndexError:
        await send_temporary_message(
            message,
            "Использование: `/promo <код>`",
            delete_after=10,
            parse_mode="Markdown"
        )
        return

    user_id = str(message.from_user.id)
    bunker_id = message.chat.id

    promos = await load_data(PROMO_COLLECTION)
    if code not in promos:
        await send_temporary_message(
            message,
            "Промокод не найден!",
            delete_after=10
        )
        return

    promo_data = promos[code]
    amount = promo_data["amount"]

    success, reason = await atomic_use_promo(code, user_id, amount)
    if not success:
        error_messages = {
            "not_found": "Промокод не найден!",
            "already_used": "Вы уже использовали этот промокод!",
            "exhausted": "Промокод закончился."
        }
        await send_temporary_message(
            message,
            error_messages.get(reason, "Ошибка!"),
            delete_after=8
        )
        return

    new_gp5 = await atomic_add_gp5(bunker_id, user_id, amount, message.from_user.full_name)
    await update_global_stats(message.from_user.id, amount, message.from_user.full_name)

    # Вычисляем старый баланс
    old_gp5 = new_gp5 - amount

    await message.reply(
        f"*🎟Промокод активирован\\!*\n\n"
        f"*\\+{amount} ГП\\-5*\n"
        f"{format_balance_change(old_gp5, new_gp5)} ГП\\-5",
        parse_mode="MarkdownV2"
    )


@dp.message(Command("promoinfo"))
async def cmd_promoinfo(message: types.Message, bot_state: BotState):
    if not is_admin(message.from_user.id, bot_state):
        return
    promos = await load_data(PROMO_COLLECTION)
    if not promos:
        await message.reply("ℹ️ Нет активных промокодов")
        return
    info_lines = ["📊 *Информация о промокодах:*\n"]
    for code, data in promos.items():
        uses_limit = 'безлимит' if data['uses'] == -1 else str(data['uses'])
        used_count = len(data.get('used_by', {}))
        info_lines.append(
            f"🎟 `{escape_markdown_v2(code)}`\n"
            f"   💰 Награда: *{data['amount']}* ГП\\-5\n"
            f"   📊 Использовано: *{used_count}*/*{escape_markdown_v2(uses_limit)}*\n"
        )
    await message.reply("\n".join(info_lines), parse_mode="MarkdownV2")


@dp.message(Command("events"))
async def cmd_testevents(message: types.Message, bot_state: BotState):
    if not is_admin(message.from_user.id, bot_state):
        return
    parts = message.text.split()
    if len(parts) < 2:
        await message.reply(
            "💡 *Использование:*\n"
            "`/events success` — успешные события\n"
            "`/events fail` — неудачные события\n"
            "`/events super` — супер события",
            parse_mode="Markdown"
        )
        return
    event_type = parts[1].lower()
    if event_type not in ["success", "fail", "super"]:
        await message.reply("❌ Доступные типы: `success`, `fail`, `super`", parse_mode="Markdown")
        return
    await message.reply(f"🔄 Запускаю показ всех событий типа «{event_type}»...")
    messages = bot_state.messages
    if event_type == "success":
        events_list = messages.get("success", [])
        loot_values = [1, 2, 3, 4, 5]
    elif event_type == "fail":
        events_list = messages.get("fail", [])
        loot_values = [-1, -2, -3]
    else:
        super_events = messages.get("super", {})
        events_list = super_events if isinstance(super_events, list) else [super_events]
        loot_values = [40]
    if not events_list:
        await message.reply(f"❌ Нет событий типа «{event_type}» в messages.json")
        return
    for idx, event in enumerate(events_list):
        loot = loot_values[idx % len(loot_values)]
        if event_type == "success":
            event_text = event["text"].format(loot)
            loot_type = "normal"
        elif event_type == "fail":
            event_text = event["text"].format(-loot)
            loot_type = "fail"
        else:
            event_text = event.get("text", "⚡ СВЕРХРЕДКАЯ НАХОДКА! ⚡")
            loot = 40
            loot_type = "super"
        caption_text = format_dig_result(event_text, loot, loot_type)
        await send_response(
            message,
            caption_text,
            image=event.get("image"),
            parse_mode="MarkdownV2"
        )
        await asyncio.sleep(1)
    await message.reply(f"✅ Все события типа «{event_type}» ({len(events_list)}) показаны!")


@dp.message(Command("maintenance_on"))
async def cmd_maintenance_on(message: types.Message, bot_state: BotState):
    if not is_admin(message.from_user.id, bot_state):
        return
    bot_state.maintenance = True
    from database import db
    await db['config'].update_one(
        {'_id': 'maintenance'},
        {'$set': {'value': 1}},
        upsert=True
    )
    for admin_id in bot_state.config.admin_ids:
        try:
            await bot.send_message(admin_id, "⚙️ Технические работы *включены*.", parse_mode="Markdown")
        except Exception:
            pass
    await message.reply("⚙️ Технические работы *включены*.\n\nБот игнорирует все запросы от пользователей.",
                        parse_mode="Markdown")


@dp.message(Command("maintenance_off"))
async def cmd_maintenance_off(message: types.Message, bot_state: BotState):
    if not is_admin(message.from_user.id, bot_state):
        return
    bot_state.maintenance = False
    from database import db
    await db['config'].update_one(
        {'_id': 'maintenance'},
        {'$set': {'value': 0}},
        upsert=True
    )
    for admin_id in bot_state.config.admin_ids:
        try:
            await bot.send_message(admin_id, "✅ Технические работы *отключены*.", parse_mode="Markdown")
        except Exception:
            pass
    await message.reply("✅ Технические работы *отключены*.\n\nБот работает в обычном режиме.", parse_mode="Markdown")


async def main():
    await ensure_singleton_documents()
    await migrate_database()
    await ensure_indexes()
    bot_state.maintenance = await load_initial_maintenance()
    bot_state.messages = await load_messages()
    logger.info("=" * 50)
    logger.info("BOT STARTED")
    logger.info(f"Maintenance mode: {bot_state.maintenance}")
    logger.info(f"Admins: {bot_state.config.admin_ids}")
    logger.info("=" * 50)
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())