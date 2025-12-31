import os
import time
import json
import logging
import aiofiles
import asyncio
from dataclasses import dataclass, field
from typing import Optional, Dict, Tuple, Callable, Any, Awaitable
from aiogram import Bot, types, BaseMiddleware
from aiogram.types import InlineKeyboardMarkup
from aiogram.types import FSInputFile
from dotenv import load_dotenv
from asyncio import Lock

load_dotenv(dotenv_path='config.txt')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logging.getLogger('aiogram').setLevel(logging.WARNING)
logging.getLogger('aiogram.event').setLevel(logging.WARNING)

logger = logging.getLogger('Digger')
logger.setLevel(logging.INFO)

MESSAGES_FILE = 'messages.json'
IMG_DIR = 'IMG'
os.makedirs(IMG_DIR, exist_ok=True)

GLOBAL_DATA_COLLECTION = 'global_loot'
CHATS_LIST_COLLECTION = 'active_chats'
PROMO_COLLECTION = 'promocodes'
GLOBAL_COOLDOWN_COLLECTION = 'cooldowns'
CHAT_DATA_COLLECTION = 'chat_data'
MEDIA_CACHE_COLLECTION = 'media_cache'

DIG_COOLDOWN_HOURS = 4
BOX_COOLDOWN_HOURS = 12
MIGRATION_VERSION = 3
SUBSCRIPTION_CACHE_TTL = 300

_dig_locks: Dict[str, Lock] = {}
_box_locks: Dict[str, Lock] = {}
_lock_cleanup_time = 0.0

_subscription_cache: Dict[int, Tuple[bool, float]] = {}

# Кэш file_id в памяти
_file_id_cache: Dict[str, str] = {}


@dataclass
class BotConfig:
    token: str
    admin_ids: list[int]
    channel_id: int
    channel_link: str
    media_channel_id: Optional[int] = None


@dataclass
class BotState:
    maintenance: bool = False
    messages: dict = field(default_factory=dict)
    config: Optional[BotConfig] = None


def load_config() -> BotConfig:
    media_channel = os.getenv('MEDIA_CHANNEL_ID', '')
    return BotConfig(
        token=os.getenv('TOKEN', ''),
        admin_ids=[int(id.strip()) for id in os.getenv('ADMIN_IDS', '').split(',') if id.strip()],
        channel_id=int(os.getenv('CHANNEL_ID', '0')),
        channel_link=os.getenv('CHANNEL_LINK', ''),
        media_channel_id=int(media_channel) if media_channel else None
    )


def safe_image_path(filename: str) -> Optional[str]:
    if not filename:
        return None
    filename = filename.replace('..', '').replace('/', '').replace('\\', '')
    base = os.path.realpath(IMG_DIR)
    full_path = os.path.realpath(os.path.join(IMG_DIR, filename))
    if not full_path.startswith(base):
        logging.warning(f"Path traversal attempt blocked: {filename}")
        return None
    return full_path if os.path.exists(full_path) else None


def escape_markdown_v2(text: str) -> str:
    special_chars = '_*[]()~`>#+-=|{}.!'
    return ''.join(['\\' + c if c in special_chars else c for c in text])


def escape_number(n: int) -> str:
    if n < 0:
        return f"\\-{abs(n)}"
    return str(n)


def format_balance_change(old_balance: int, new_balance: int) -> str:
    return f"{escape_number(old_balance)} → *{escape_number(new_balance)}*"


def format_wait_time(seconds: int) -> str:
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    if hours > 0:
        return f"{hours} ч. {minutes} мин."
    return f"{minutes} мин."


def _cleanup_locks():
    """Очистка неиспользуемых локов"""
    global _dig_locks, _box_locks, _lock_cleanup_time
    now = time.time()
    if now - _lock_cleanup_time > 300:
        _dig_locks = {k: v for k, v in _dig_locks.items() if v.locked()}
        _box_locks = {k: v for k, v in _box_locks.items() if v.locked()}
        _lock_cleanup_time = now


def get_dig_lock(user_id: str, chat_id: int) -> Lock:
    """Получить lock для операции dig"""
    _cleanup_locks()
    key = f"dig:{user_id}:{chat_id}"
    if key not in _dig_locks:
        _dig_locks[key] = Lock()
    return _dig_locks[key]


def get_box_lock(user_id: str) -> Lock:
    """Получить lock для операции box"""
    _cleanup_locks()
    key = f"box:{user_id}"
    if key not in _box_locks:
        _box_locks[key] = Lock()
    return _box_locks[key]


def format_dig_result(
        event_text: str,
        loot: int,
        loot_type: str,
        old_balance: int = None,
        new_balance: int = None
) -> str:
    escaped_event = escape_markdown_v2(event_text)

    balance_line = ""
    if old_balance is not None and new_balance is not None:
        balance_line = f"\n{format_balance_change(old_balance, new_balance)} ГП\\-5"

    if loot_type == "super":
        return (
            f"⚡ *СВЕРХРЕДКАЯ НАХОДКА\\!* ⚡\n\n"
            f"{escaped_event}\n"
            f"*☢️\\+40 ГП\\-5*"
            f"{balance_line}"
        )

    change = f"\\+{loot}" if loot > 0 else f"\\-{-loot}"
    return (
        f"*📻 Вылазка завершена*\n\n"
        f"{escaped_event}\n"
        f"*☢️{change} ГП\\-5*"
        f"{balance_line}\n"
    )


async def send_temporary_message(
        message: types.Message,
        text: str,
        delete_after: int = 7,
        parse_mode: str = None,
        reply_markup: InlineKeyboardMarkup = None
) -> types.Message:
    sent = await message.reply(
        text,
        parse_mode=parse_mode,
        reply_markup=reply_markup
    )

    async def delete_later():
        await asyncio.sleep(delete_after)
        try:
            await sent.delete()
        except Exception:
            pass

    asyncio.create_task(delete_later())
    return sent


async def check_subscription(bot: Bot, channel_id: int, user_id: int) -> bool:
    now = time.time()
    cached = _subscription_cache.get(user_id)
    if cached:
        is_subscribed, cached_time = cached
        if now - cached_time < SUBSCRIPTION_CACHE_TTL:
            return is_subscribed
    try:
        member = await bot.get_chat_member(channel_id, user_id)
        is_subscribed = member.status in ["member", "administrator", "creator"]
    except Exception as e:
        logging.warning(f"Error checking subscription for {user_id}: {e}")
        if cached:
            return cached[0]
        is_subscribed = False
    _subscription_cache[user_id] = (is_subscribed, now)
    if len(_subscription_cache) > 1000:
        cutoff = now - SUBSCRIPTION_CACHE_TTL
        keys_to_remove = [k for k, v in _subscription_cache.items() if v[1] < cutoff]
        for k in keys_to_remove[:500]:
            _subscription_cache.pop(k, None)
    return is_subscribed


def invalidate_subscription_cache(user_id: int):
    _subscription_cache.pop(user_id, None)


async def load_messages() -> dict:
    try:
        async with aiofiles.open(MESSAGES_FILE, 'r', encoding='utf-8') as f:
            content = await f.read()
            return json.loads(content)
    except FileNotFoundError:
        logging.error(f"Messages file not found: {MESSAGES_FILE}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in messages file: {e}")
        return {}


def get_user_rank(gp5: int, messages: dict) -> dict:
    ranks = messages.get("ranks", [])
    if not ranks:
        return {
            "name": "Неизвестный",
            "emoji": "❓",
            "image": None,
            "min_gp5": 0,
            "next_rank": None,
            "progress": 0
        }
    sorted_ranks = sorted(ranks, key=lambda x: x.get("min_gp5", 0), reverse=True)
    current_rank = sorted_ranks[-1]
    next_rank = None
    for i, rank in enumerate(sorted_ranks):
        if gp5 >= rank.get("min_gp5", 0):
            current_rank = rank
            if i > 0:
                next_rank = sorted_ranks[i - 1]
            break
        next_rank = rank
    progress = 100
    if next_rank:
        current_min = current_rank.get("min_gp5", 0)
        next_min = next_rank.get("min_gp5", 0)
        if next_min > current_min:
            progress = int(((gp5 - current_min) / (next_min - current_min)) * 100)
            progress = max(0, min(progress, 99))
    return {
        "name": current_rank.get("name", "Неизвестный"),
        "emoji": current_rank.get("emoji", "❓"),
        "image": current_rank.get("image"),
        "min_gp5": current_rank.get("min_gp5", 0),
        "next_rank": next_rank,
        "progress": progress
    }


def format_progress_bar(progress: int, length: int = 10) -> str:
    filled = int(progress / 100 * length)
    empty = length - filled
    return "▓" * filled + "░" * empty


async def get_cached_file_id(filename: str) -> Optional[str]:
    """Получить file_id из кэша (память + БД)"""
    if not filename:
        return None

    # Сначала проверяем память
    if filename in _file_id_cache:
        return _file_id_cache[filename]

    # Затем проверяем БД
    from database import db
    doc = await db[MEDIA_CACHE_COLLECTION].find_one({'_id': filename})
    if doc and 'file_id' in doc:
        _file_id_cache[filename] = doc['file_id']
        return doc['file_id']

    return None


async def save_file_id(filename: str, file_id: str):
    """Сохранить file_id в кэш (память + БД)"""
    _file_id_cache[filename] = file_id

    from database import db
    await db[MEDIA_CACHE_COLLECTION].update_one(
        {'_id': filename},
        {'$set': {'file_id': file_id, 'updated_at': time.time()}},
        upsert=True
    )


async def send_photo_cached(
        bot: Bot,
        chat_id: int,
        filename: str,
        caption: str = None,
        parse_mode: str = None,
        reply_markup: InlineKeyboardMarkup = None,
        reply_to_message_id: int = None
) -> Optional[types.Message]:
    """
    Отправляет фото с использованием кэша file_id.
    Если file_id нет — загружает файл и кэширует.
    """
    if not filename:
        return None

    # Пробуем получить file_id из кэша
    file_id = await get_cached_file_id(filename)

    if file_id:
        # Отправляем по file_id (мгновенно)
        try:
            msg = await bot.send_photo(
                chat_id=chat_id,
                photo=file_id,
                caption=caption,
                parse_mode=parse_mode,
                reply_markup=reply_markup,
                reply_to_message_id=reply_to_message_id
            )
            return msg
        except Exception as e:
            # file_id мог устареть, пробуем загрузить заново
            logging.warning(f"Cached file_id failed for {filename}: {e}")
            _file_id_cache.pop(filename, None)

    # Загружаем файл локально
    image_path = safe_image_path(filename)
    if not image_path:
        logging.warning(f"Image not found: {filename}")
        return None

    try:
        msg = await bot.send_photo(
            chat_id=chat_id,
            photo=FSInputFile(image_path),
            caption=caption,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
            reply_to_message_id=reply_to_message_id
        )

        # Сохраняем file_id для будущих отправок
        if msg.photo:
            new_file_id = msg.photo[-1].file_id
            await save_file_id(filename, new_file_id)
            logger.info(f"Cached file_id for {filename}")

        return msg
    except Exception as e:
        logging.error(f"Failed to send photo {filename}: {e}")
        return None


async def send_response(
        message: types.Message,
        text: str,
        image: Optional[str] = None,
        keyboard: Optional[InlineKeyboardMarkup] = None,
        parse_mode: str = "MarkdownV2"
) -> types.Message:
    """
    Отправляет ответ с изображением (если есть) или просто текст.
    Использует кэширование file_id для быстрой отправки.
    """
    if image:
        # Пробуем отправить с фото
        msg = await send_photo_cached(
            bot=message.bot,
            chat_id=message.chat.id,
            filename=image,
            caption=text,
            parse_mode=parse_mode,
            reply_markup=keyboard,
            reply_to_message_id=message.message_id
        )
        if msg:
            return msg
        # Если не получилось — отправляем без фото
        logging.warning(f"Falling back to text-only for image: {image}")

    return await message.reply(text, parse_mode=parse_mode, reply_markup=keyboard)


def is_admin(user_id: int, bot_state: BotState) -> bool:
    return user_id in (bot_state.config.admin_ids if bot_state.config else [])


class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, rate_limit: float = 0.5):
        self.rate_limit = rate_limit
        self.user_last_request: Dict[int, float] = {}
        self.user_pending: Dict[int, bool] = {}
        self._last_cleanup = time.time()
        self._cleanup_interval = 300
        super().__init__()

    async def __call__(
            self,
            handler: Callable[[types.Message, Dict[str, Any]], Awaitable[Any]],
            event: types.Message | types.CallbackQuery,
            data: Dict[str, Any]
    ) -> Any:
        # Админы пропускают rate limit
        if data.get('bypass_cooldown'):
            return await handler(event, data)

        user_id = event.from_user.id if event.from_user else None
        if user_id:
            now = time.time()

            if now - self._last_cleanup > self._cleanup_interval:
                cutoff = now - 300
                self.user_last_request = {
                    uid: t for uid, t in self.user_last_request.items()
                    if t > cutoff
                }
                self.user_pending = {}
                self._last_cleanup = now

            last_request = self.user_last_request.get(user_id)
            if last_request:
                elapsed = now - last_request
                if elapsed < self.rate_limit:
                    return

            self.user_last_request[user_id] = now

        return await handler(event, data)


class MaintenanceMiddleware(BaseMiddleware):
    def __init__(self, bot_state: BotState):
        self.bot_state = bot_state
        super().__init__()

    async def __call__(
            self,
            handler: Callable[[types.Message, Dict[str, Any]], Awaitable[Any]],
            event: types.Message | types.CallbackQuery,
            data: Dict[str, Any]
    ) -> Any:
        user_id = event.from_user.id if event.from_user else None
        admin_ids = self.bot_state.config.admin_ids if self.bot_state.config else []
        if self.bot_state.maintenance and user_id not in admin_ids:
            return
        return await handler(event, data)


class StateMiddleware(BaseMiddleware):
    def __init__(self, bot_state: BotState):
        self.bot_state = bot_state
        super().__init__()

    async def __call__(
            self,
            handler: Callable[[types.Message, Dict[str, Any]], Awaitable[Any]],
            event: types.Message | types.CallbackQuery,
            data: Dict[str, Any]
    ) -> Any:
        data['bot_state'] = self.bot_state

        # Добавляем флаги для админов
        user_id = event.from_user.id if event.from_user else None
        admin_ids = self.bot_state.config.admin_ids if self.bot_state.config else []
        is_admin = user_id in admin_ids

        data['is_admin'] = is_admin
        data['bypass_cooldown'] = is_admin  # Админы пропускают cooldown

        return await handler(event, data)