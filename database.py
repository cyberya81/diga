import os
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple
import motor.motor_asyncio
from pymongo.errors import DuplicateKeyError

from utils import (
    GLOBAL_COOLDOWN_COLLECTION, CHATS_LIST_COLLECTION, PROMO_COLLECTION,
    CHAT_DATA_COLLECTION, DIG_COOLDOWN_HOURS, BOX_COOLDOWN_HOURS, MIGRATION_VERSION
)

MONGODB_URI = os.getenv('MONGODB_URI')
if not MONGODB_URI:
    raise ValueError("MONGODB_URI not set in environment")

mongo_client = motor.motor_asyncio.AsyncIOMotorClient(MONGODB_URI)
db = mongo_client['dbtest']


async def ensure_singleton_documents():
    singletons = [
        (GLOBAL_COOLDOWN_COLLECTION, 'singleton'),
        (CHATS_LIST_COLLECTION, 'singleton'),
        (PROMO_COLLECTION, 'singleton'),
        ('config', 'maintenance'),
        ('migrations', 'version'),
    ]
    for collection_name, doc_id in singletons:
        try:
            await db[collection_name].update_one(
                {'_id': doc_id},
                {'$setOnInsert': {'data': {}}},
                upsert=True
            )
        except DuplicateKeyError:
            pass
    logging.info("Singleton documents ensured")


async def ensure_indexes():
    await db['global_stats'].create_index([('total_gp5', -1)])  # Изменено с 'gp5' на 'total_gp5'
    await db[GLOBAL_COOLDOWN_COLLECTION].create_index([('_id', 1)])
    await db[CHAT_DATA_COLLECTION].create_index([('_id', 1)])
    await db['promo_usage'].create_index([('user_id', 1), ('code', 1)], unique=True)
    logging.info("Database indexes created")


async def migrate_database():
    lock_result = await db['migrations'].find_one_and_update(
        {
            '_id': 'lock',
            '$or': [
                {'locked': False},
                {'locked': {'$exists': False}},
                {'locked_at': {'$lt': datetime.now() - timedelta(minutes=5)}}
            ]
        },
        {
            '$set': {
                'locked': True,
                'locked_at': datetime.now()
            }
        },
        upsert=True,
        return_document=True
    )
    if not lock_result or (lock_result.get('locked') and
                           lock_result.get('locked_at') and
                           lock_result['locked_at'] > datetime.now() - timedelta(minutes=5) and
                           lock_result.get('_id') != 'lock'):
        logging.info("Migration already running by another process")
        return
    try:
        version_doc = await db['migrations'].find_one({'_id': 'version'})
        current_version = version_doc.get('version', 0) if version_doc else 0

        if current_version < 1:
            await _migrate_v1_global_stats()
            await db['migrations'].update_one(
                {'_id': 'version'},
                {'$set': {'version': 1, 'migrated_at': datetime.now()}},
                upsert=True
            )
            logging.info("Migration v1 completed")

        if current_version < 2:
            await _migrate_v2_total_gp5()
            await db['migrations'].update_one(
                {'_id': 'version'},
                {'$set': {'version': 2, 'migrated_at': datetime.now()}},
                upsert=True
            )
            logging.info("Migration v2 completed")

        logging.info(f"Database at version {max(current_version, MIGRATION_VERSION)}")
    except Exception as e:
        logging.error(f"Migration failed: {e}")
        raise
    finally:
        await db['migrations'].update_one(
            {'_id': 'lock'},
            {'$set': {'locked': False}}
        )


async def recalculate_global_stats():
    """Принудительный пересчёт total_gp5 для всех пользователей"""
    logging.info("Starting full recalculation of global_stats...")

    # Очищаем старые данные
    await db['global_stats'].delete_many({})

    pipeline = [
        {"$project": {"data": {"$objectToArray": "$data"}}},
        {"$unwind": "$data"},
        {"$group": {
            "_id": "$data.k",  # user_id
            "total_gp5": {"$sum": "$data.v.gp5"},
            "username": {"$last": "$data.v.username"}
        }}
    ]

    count = 0
    batch = []

    async for doc in db[CHAT_DATA_COLLECTION].aggregate(pipeline):
        user_id = doc["_id"]
        total_gp5 = doc.get("total_gp5", 0)
        username = doc.get("username", "Unknown")

        batch.append({
            '_id': user_id,
            'total_gp5': total_gp5,
            'username': username
        })

        if len(batch) >= 100:
            await db['global_stats'].insert_many(batch)
            count += len(batch)
            batch = []

    if batch:
        await db['global_stats'].insert_many(batch)
        count += len(batch)

    logging.info(f"Recalculation completed: {count} users updated")
    return count

async def _migrate_v2_total_gp5():
    """Пересчёт total_gp5 (сумма по всем чатам) для всех пользователей"""
    logging.info("Migrating to total_gp5 (sum across all chats)...")

    pipeline = [
        {"$project": {"data": {"$objectToArray": "$data"}}},
        {"$unwind": "$data"},
        {"$group": {
            "_id": "$data.k",
            "total_gp5": {"$sum": "$data.v.gp5"},
            "username": {"$last": "$data.v.username"}
        }}
    ]

    batch = []
    async for doc in db[CHAT_DATA_COLLECTION].aggregate(pipeline):
        user_id = doc["_id"]
        total_gp5 = doc.get("total_gp5", 0)
        username = doc.get("username", "Unknown")

        batch.append({
            'filter': {'_id': user_id},
            'update': {'$set': {'total_gp5': total_gp5, 'username': username}},
            'upsert': True
        })

        if len(batch) >= 100:
            await _execute_batch_updates(batch)
            batch = []

    if batch:
        await _execute_batch_updates(batch)

    logging.info("Migration to total_gp5 completed")


async def _execute_batch_updates(batch: list):
    """Выполняет пакетное обновление"""
    from pymongo import UpdateOne
    operations = [
        UpdateOne(item['filter'], item['update'], upsert=item['upsert'])
        for item in batch
    ]
    if operations:
        await db['global_stats'].bulk_write(operations, ordered=False)

async def _migrate_v1_global_stats():
    stats_count = await db['global_stats'].count_documents({})
    if stats_count > 0:
        return
    logging.info("Migrating global stats from chat data...")
    all_users = {}
    async for doc in db[CHAT_DATA_COLLECTION].find():
        chat_data = doc.get('data', {})
        for user_id, data in chat_data.items():
            gp5 = data.get('gp5', 0)
            if user_id not in all_users or gp5 > all_users[user_id]['gp5']:
                all_users[user_id] = {
                    '_id': user_id,
                    'gp5': gp5,
                    'username': data.get('username', 'Unknown')
                }
    if all_users:
        try:
            await db['global_stats'].insert_many(list(all_users.values()), ordered=False)
            logging.info(f"Migrated {len(all_users)} users to global_stats")
        except Exception as e:
            logging.warning(f"Some users already existed in global_stats: {e}")


async def load_data(collection_name: str, chat_id: int = None) -> dict:
    if collection_name == CHAT_DATA_COLLECTION and chat_id:
        doc = await db[collection_name].find_one({'_id': chat_id})
        return doc.get('data', {}) if doc else {}
    else:
        doc = await db[collection_name].find_one({'_id': 'singleton'})
        return doc.get('data', {}) if doc else {}


async def save_data(data: dict, collection_name: str, chat_id: int = None):
    if collection_name == CHAT_DATA_COLLECTION and chat_id:
        await db[collection_name].replace_one(
            {'_id': chat_id},
            {'_id': chat_id, 'data': data},
            upsert=True
        )
    else:
        await db[collection_name].update_one(
            {'_id': 'singleton'},
            {'$set': {'data': data}},
            upsert=True
        )


async def _retry_on_duplicate(operation, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            return await operation()
        except DuplicateKeyError:
            if attempt < max_retries - 1:
                await asyncio.sleep(0.01 * (attempt + 1))
                continue
            raise


async def try_claim_dig_cooldown(
        user_id: str,
        chat_id: int,
        cooldown_hours: int = DIG_COOLDOWN_HOURS
) -> Tuple[bool, Optional[int]]:
    now = datetime.now()
    cutoff = now - timedelta(hours=cooldown_hours)
    chat_id_str = str(chat_id)

    async def do_update():
        return await db[GLOBAL_COOLDOWN_COLLECTION].find_one_and_update(
            {
                '_id': 'singleton',
                '$or': [
                    {f'data.{user_id}.dig.{chat_id_str}.time': {'$exists': False}},
                    {f'data.{user_id}.dig.{chat_id_str}.time': {'$lt': cutoff.isoformat()}}
                ]
            },
            {
                '$set': {
                    f'data.{user_id}.dig.{chat_id_str}.time': now.isoformat(),
                    f'data.{user_id}.dig.{chat_id_str}.locked': True
                }
            },
            return_document=False
        )

    try:
        result = await _retry_on_duplicate(do_update)
    except DuplicateKeyError:
        logging.error(f"DuplicateKeyError in try_claim_dig_cooldown after retries")
        return False, None
    except Exception as e:
        logging.error(f"Error in try_claim_dig_cooldown: {e}")
        return False, None
    if result is not None:
        return True, None
    doc = await db[GLOBAL_COOLDOWN_COLLECTION].find_one({'_id': 'singleton'})
    if doc and 'data' in doc:
        user_data = doc['data'].get(user_id, {})
        dig_data = user_data.get('dig', {}).get(chat_id_str, {})
        if dig_data.get('locked'):
            time_str = dig_data.get('time')
            if time_str:
                try:
                    dig_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                    if abs((now - dig_time).total_seconds()) < 2:
                        return True, None
                except ValueError:
                    pass
        time_str = dig_data.get('time')
        if time_str:
            try:
                last_dig = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                remaining = timedelta(hours=cooldown_hours) - (datetime.now() - last_dig)
                if remaining.total_seconds() > 0:
                    return False, int(remaining.total_seconds())
            except ValueError:
                pass
    return False, None


async def finish_dig_cooldown(user_id: str, chat_id: int, last_loot: int):
    chat_id_str = str(chat_id)
    await db[GLOBAL_COOLDOWN_COLLECTION].update_one(
        {'_id': 'singleton'},
        {
            '$set': {f'data.{user_id}.dig.{chat_id_str}.last_loot': last_loot},
            '$unset': {f'data.{user_id}.dig.{chat_id_str}.locked': 1}
        }
    )


async def unlock_dig_cooldown(user_id: str, chat_id: int):
    chat_id_str = str(chat_id)
    await db[GLOBAL_COOLDOWN_COLLECTION].update_one(
        {'_id': 'singleton'},
        {'$unset': {f'data.{user_id}.dig.{chat_id_str}.locked': 1}}
    )


async def try_claim_box_cooldown(
        user_id: str,
        cooldown_hours: int = BOX_COOLDOWN_HOURS
) -> Tuple[bool, Optional[int]]:
    now = datetime.now()
    cutoff = now - timedelta(hours=cooldown_hours)

    async def do_update():
        return await db[GLOBAL_COOLDOWN_COLLECTION].find_one_and_update(
            {
                '_id': 'singleton',
                '$or': [
                    {f'data.{user_id}.box.time': {'$exists': False}},
                    {f'data.{user_id}.box.time': {'$lt': cutoff.isoformat()}}
                ]
            },
            {
                '$set': {f'data.{user_id}.box.pending': True}
            },
            return_document=False
        )

    try:
        result = await _retry_on_duplicate(do_update)
    except DuplicateKeyError:
        logging.error(f"DuplicateKeyError in try_claim_box_cooldown after retries")
        return False, None
    except Exception as e:
        logging.error(f"Error in try_claim_box_cooldown: {e}")
        return False, None
    if result is not None:
        return True, None
    doc = await db[GLOBAL_COOLDOWN_COLLECTION].find_one({'_id': 'singleton'})
    if doc and 'data' in doc:
        user_data = doc['data'].get(user_id, {})
        box_data = user_data.get('box', {})
        if box_data.get('pending') and not box_data.get('time'):
            return True, None
        time_str = box_data.get('time')
        if time_str:
            try:
                last_box = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                remaining = timedelta(hours=cooldown_hours) - (datetime.now() - last_box)
                if remaining.total_seconds() > 0:
                    return False, int(remaining.total_seconds())
            except ValueError:
                pass
    return False, None


async def atomic_add_gp5(chat_id: int, user_id: str, amount: int, username: str) -> int:
    result = await db[CHAT_DATA_COLLECTION].find_one_and_update(
        {'_id': chat_id},
        {
            '$inc': {f'data.{user_id}.gp5': amount},
            '$set': {f'data.{user_id}.username': username}
        },
        upsert=True,
        return_document=True
    )
    if result and 'data' in result and user_id in result['data']:
        return result['data'][user_id].get('gp5', amount)
    return amount


async def save_box_mapping(user_id: str, mapping: dict):
    await db[GLOBAL_COOLDOWN_COLLECTION].update_one(
        {'_id': 'singleton'},
        {'$set': {f'data.{user_id}.box_mapping': mapping}}
    )


async def claim_box_mapping(user_id: str, button_id: str) -> Optional[str]:
    result = await db[GLOBAL_COOLDOWN_COLLECTION].find_one_and_update(
        {
            '_id': 'singleton',
            f'data.{user_id}.box_mapping': {'$exists': True, '$ne': {}},
            f'data.{user_id}.box_mapping.{button_id}': {'$exists': True}
        },
        {
            '$unset': {
                f'data.{user_id}.box_mapping': 1,
                f'data.{user_id}.box.pending': 1
            },
            '$set': {
                f'data.{user_id}.box.time': datetime.now().isoformat(),
                f'data.{user_id}.box.claimed': True
            }
        },
        return_document=False
    )
    if not result:
        return None
    user_data = result.get('data', {}).get(user_id, {})
    box_mapping = user_data.get('box_mapping', {})
    return box_mapping.get(button_id)


async def get_user_cooldown(user_id: str) -> Optional[dict]:
    doc = await db[GLOBAL_COOLDOWN_COLLECTION].find_one({'_id': 'singleton'})
    if doc and 'data' in doc:
        return doc['data'].get(user_id)
    return None


async def get_user_dig_cooldown(user_id: str, chat_id: int) -> Optional[dict]:
    doc = await db[GLOBAL_COOLDOWN_COLLECTION].find_one({'_id': 'singleton'})
    if doc and 'data' in doc:
        user_data = doc['data'].get(user_id, {})
        return user_data.get('dig', {}).get(str(chat_id))
    return None


async def delete_user_cooldowns(user_id: str):
    await db[GLOBAL_COOLDOWN_COLLECTION].update_one(
        {'_id': 'singleton'},
        {'$unset': {f'data.{user_id}': 1}}
    )


async def atomic_set_user_data(chat_id: int, user_id: str, data: dict):
    await db[CHAT_DATA_COLLECTION].update_one(
        {'_id': chat_id},
        {'$set': {f'data.{user_id}': data}},
        upsert=True
    )


async def update_chat_list(chat_id: int, chat_title: str, chat_type: str):
    await db[CHATS_LIST_COLLECTION].update_one(
        {'_id': 'singleton'},
        {'$set': {
            f'data.{chat_id}': {
                "title": chat_title,
                "last_active": datetime.now().isoformat(),
                "type": chat_type
            }
        }}
    )


async def update_global_stats(user_id: int, delta_gp5: int, username: str):
    await db['global_stats'].update_one(
        {'_id': str(user_id)},
        {
            '$inc': {'total_gp5': delta_gp5},
            '$set': {'username': username}
        },
        upsert=True
    )


async def get_global_top(limit: int = 10) -> list:
    cursor = db['global_stats'].find().sort('total_gp5', -1).limit(limit)
    result = await cursor.to_list(length=limit)

    # Нормализуем для обратной совместимости
    for doc in result:
        doc['gp5'] = doc.get('total_gp5', doc.get('gp5', 0))

    if result:
        return result

    # Fallback — вычисляем на лету если global_stats пуста
    all_users = {}
    async for doc in db[CHAT_DATA_COLLECTION].find():
        chat_data = doc.get('data', {})
        for user_id, data in chat_data.items():
            gp5 = data.get('gp5', 0)
            if user_id not in all_users:
                all_users[user_id] = {'gp5': 0, 'username': data.get('username', 'Unknown')}
            all_users[user_id]['gp5'] += gp5

    sorted_users = sorted(all_users.values(), key=lambda x: x.get('gp5', 0), reverse=True)
    return sorted_users[:limit]


async def find_user_in_chats(user_id: int) -> Optional[dict]:
    user_data = None
    async for doc in db[CHAT_DATA_COLLECTION].find():
        chat_data = doc.get('data', {})
        user_id_str = str(user_id)
        if user_id_str in chat_data:
            current = chat_data[user_id_str]
            if user_data is None or current.get("gp5", 0) > user_data.get("gp5", 0):
                user_data = current.copy()
                user_data["chat_id"] = str(doc['_id'])
    return user_data


async def atomic_use_promo(code: str, user_id: str, amount: int) -> Tuple[bool, str]:
    result = await db[PROMO_COLLECTION].find_one_and_update(
        {
            '_id': 'singleton',
            f'data.{code}': {'$exists': True},
            f'data.{code}.used_by.{user_id}': {'$exists': False},
            '$or': [
                {f'data.{code}.uses': -1},
                {f'data.{code}.uses': {'$gt': {'$size': f'$data.{code}.used_by'}}}
            ]
        },
        {
            '$set': {f'data.{code}.used_by.{user_id}': datetime.now().isoformat()}
        },
        return_document=False
    )
    if result is not None:
        return True, "success"
    promos = await load_data(PROMO_COLLECTION)
    if code not in promos:
        return False, "not_found"
    promo_data = promos[code]
    if user_id in promo_data.get("used_by", {}):
        return False, "already_used"
    if promo_data["uses"] > -1 and len(promo_data.get("used_by", {})) >= promo_data["uses"]:
        return False, "exhausted"
    promos[code]["used_by"][user_id] = datetime.now().isoformat()
    await save_data(promos, PROMO_COLLECTION)
    return True, "success"


async def get_user_profile_data(chat_id: int, user_id: str) -> dict:
    chat_data_task = db[CHAT_DATA_COLLECTION].find_one({'_id': chat_id})
    global_stats_task = db['global_stats'].find_one({'_id': user_id})
    cooldown_task = db[GLOBAL_COOLDOWN_COLLECTION].find_one(
        {'_id': 'singleton'},
        {f'data.{user_id}.dig.{chat_id}': 1}
    )

    chat_doc, global_doc, cooldown_doc = await asyncio.gather(
        chat_data_task, global_stats_task, cooldown_task
    )

    chat_data = chat_doc.get('data', {}) if chat_doc else {}
    user_chat_data = chat_data.get(user_id, {})

    last_loot = None
    if cooldown_doc and 'data' in cooldown_doc:
        user_cooldown = cooldown_doc['data'].get(user_id, {})
        dig_data = user_cooldown.get('dig', {}).get(str(chat_id), {})
        last_loot = dig_data.get('last_loot')

    if chat_data:
        sorted_users = sorted(
            chat_data.items(),
            key=lambda x: x[1].get('gp5', 0),
            reverse=True
        )
        position = next(
            (i + 1 for i, (uid, _) in enumerate(sorted_users) if uid == user_id),
            None
        )
        total_in_chat = len(sorted_users)
    else:
        position = None
        total_in_chat = 0

    return {
        "chat_gp5": user_chat_data.get("gp5", 0),
        "global_gp5": global_doc.get("total_gp5", 0) if global_doc else 0,  # Изменено
        "chat_position": position,
        "chat_total": total_in_chat,
        "username": user_chat_data.get("username") or (
            global_doc.get("username") if global_doc else "Unknown"
        ),
        "exists_in_chat": user_id in chat_data,
        "exists_globally": global_doc is not None,
        "last_loot": last_loot
    }

async def get_admin_user_info(user_id: str) -> dict:
    global_stats_task = db['global_stats'].find_one({'_id': user_id})
    cooldown_task = db[GLOBAL_COOLDOWN_COLLECTION].find_one(
        {'_id': 'singleton'},
        {f'data.{user_id}': 1}
    )
    chats_pipeline = [
        {"$project": {"data": {"$objectToArray": "$data"}}},
        {"$unwind": "$data"},
        {"$match": {"data.k": user_id}},
        {"$group": {
            "_id": None,
            "chats_count": {"$sum": 1},
            "total_gp5": {"$sum": "$data.v.gp5"}
        }}
    ]

    global_doc, cooldown_doc, chats_agg = await asyncio.gather(
        global_stats_task,
        cooldown_task,
        db[CHAT_DATA_COLLECTION].aggregate(chats_pipeline).to_list(1)
    )

    cooldown_data = {}
    if cooldown_doc and 'data' in cooldown_doc:
        cooldown_data = cooldown_doc['data'].get(user_id, {})

    chats_info = chats_agg[0] if chats_agg else {"chats_count": 0, "total_gp5": 0}

    return {
        "global_gp5": global_doc.get("total_gp5", 0) if global_doc else 0,  # Изменено
        "username": global_doc.get("username", "Unknown") if global_doc else "Unknown",
        "exists_globally": global_doc is not None,
        "chats_count": chats_info.get("chats_count", 0),
        "total_gp5_sum": chats_info.get("total_gp5", 0),
        "cooldown_data": cooldown_data
    }

async def get_bot_statistics() -> dict:
    unique_players_task = db['global_stats'].count_documents({})
    active_chats_task = db[CHAT_DATA_COLLECTION].count_documents({})

    pipeline = [
        {"$project": {"players": {"$objectToArray": "$data"}}},
        {"$project": {"player_count": {"$size": "$players"}}},
        {"$group": {
            "_id": None,
            "total_player_records": {"$sum": "$player_count"},
            "max_players_in_chat": {"$max": "$player_count"},
            "avg_players_per_chat": {"$avg": "$player_count"}
        }}
    ]
    aggregation_task = db[CHAT_DATA_COLLECTION].aggregate(pipeline).to_list(1)
    top_player_task = db['global_stats'].find().sort('total_gp5', -1).limit(1).to_list(1)  # Изменено

    unique_players, active_chats, agg_result, top_player = await asyncio.gather(
        unique_players_task,
        active_chats_task,
        aggregation_task,
        top_player_task
    )

    agg_data = agg_result[0] if agg_result else {}

    # Нормализуем top_player
    if top_player:
        top_player[0]['gp5'] = top_player[0].get('total_gp5', top_player[0].get('gp5', 0))

    return {
        "unique_players": unique_players,
        "active_chats": active_chats,
        "total_player_records": agg_data.get("total_player_records", 0),
        "max_players_in_chat": agg_data.get("max_players_in_chat", 0),
        "avg_players_per_chat": round(agg_data.get("avg_players_per_chat", 0), 1),
        "top_player": top_player[0] if top_player else None
    }


async def load_initial_maintenance() -> bool:
    doc = await db['config'].find_one({'_id': 'maintenance'})
    return bool(doc.get('value')) if doc else False