import asyncio
import logging
import os
import time
import json
import base64
from typing import List, Dict, Optional
from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
import aiohttp
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv("BOT_TOKEN")
if not TOKEN:
    raise ValueError("BOT_TOKEN not found in .env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)

SOURCES = [
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/barry-far/V2ray-Config/main/All_Configs_base64_Sub.txt",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/All_Configs_Sub.txt",
    "https://raw.githubusercontent.com/Epodonios/v2ray-configs/main/All_Configs_base64_Sub.txt",
    "https://raw.githubusercontent.com/MatinGhanbari/v2ray-configs/main/subscriptions/v2ray/all_sub.txt",
    "https://raw.githubusercontent.com/MatinGhanbari/v2ray-configs/main/subscriptions/v2ray/super-sub.txt",
    "https://raw.githubusercontent.com/sevcator/5ubscrpt10n/main/sub/vless+vmess+trojan+ss.txt",
    "https://raw.githubusercontent.com/SoliSpirit/v2ray-configs/main/all_configs.txt",
    "https://raw.githubusercontent.com/ebrasha/free-v2ray-public-list/main/all_extracted_configs.txt",
    "https://raw.githubusercontent.com/ninjastrikers/v2ray-configs/main/combined/all.txt",
    "https://raw.githubusercontent.com/zipvpn/FreeVPNNodes/main/free_v2ray_xray_nodes.txt",
]

ITEMS_PER_PAGE = 8
UPDATE_INTERVAL_MIN = 30
FASTEST_CACHE_TTL = 900
PING_CACHE_TTL = 600
MAX_CONCURRENT_PINGS = 15

user_configs: Dict[int, List[str]] = {}
user_ping_cache: Dict[int, Dict[str, tuple[float, float | None]]] = {}
sorted_by_ping_cache: Dict[int, tuple[list[str], float]] = {}
cancel_tasks: Dict[int, asyncio.Task] = {}

def parse_server_address(config: str) -> tuple[str, int] | None:
    if not config.startswith(("vmess://", "vless://", "trojan://", "ss://")):
        return None
    try:
        encoded_part = config.split("://", 1)[1].split("#")[0].split("?")[0].strip()
        
        if "@" in encoded_part:
            encoded_part = encoded_part.split("@")[-1]
        
        if ":" in encoded_part and encoded_part.count(":") >= 1:
            host_port = encoded_part.rsplit(":", 1)
            if host_port[1].isdigit():
                return host_port[0].strip(), int(host_port[1])
        
        decoded = base64.urlsafe_b64decode(encoded_part + "==" * 2).decode("utf-8", errors="ignore")
        data = json.loads(decoded)
        
        add = data.get("add") or data.get("address") or data.get("host")
        port = data.get("port")
        
        if add and port and isinstance(port, (int, str)) and str(port).isdigit():
            return str(add).strip(), int(port)
            
    except Exception:
        pass
    
    try:
        if "://" in config:
            after = config.split("://", 1)[1]
            if ":" in after:
                parts = after.rsplit(":", 1)
                if len(parts) == 2 and parts[1].split("#")[0].strip().isdigit():
                    return parts[0].strip(), int(parts[1].split("#")[0].strip())
    except:
        pass
        
    return None

async def measure_tcp_ping(host: str, port: int, timeout: float = 3.0) -> float | None:
    try:
        start = time.time()
        _, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)
        writer.close()
        await writer.wait_closed()
        return round((time.time() - start) * 1000, 1)
    except:
        return None

async def get_ping(user_id: int, config: str) -> str:
    key = config[:100]
    now = time.time()
    cache = user_ping_cache.get(user_id, {})
    if key in cache:
        ts, val = cache[key]
        if now - ts < PING_CACHE_TTL:
            return f"{val:.1f}ms" if val is not None else "❌"
    addr = parse_server_address(config)
    if not addr:
        ping_val = None
    else:
        ping_val = await measure_tcp_ping(*addr)
    if user_id not in user_ping_cache:
        user_ping_cache[user_id] = {}
    user_ping_cache[user_id][key] = (now, ping_val)
    return f"{ping_val:.1f}ms" if ping_val is not None else "❌"

def split_configs(text: str) -> list[str]:
    result = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith(('#', '备注', '备注:', '说明', '备注：', 'سرور', 'Channel', 'Group', '必进', '--------------------------------')):
            continue
        if any(line.startswith(p) for p in ['vmess://', 'vless://', 'trojan://', 'ss://', 'ssr://']):
            result.append(line)
    return result

async def fetch_url(session: aiohttp.ClientSession, url: str) -> str | None:
    try:
        async with session.get(url, timeout=15) as resp:
            if resp.status == 200:
                return await resp.text()
    except:
        pass
    return None

def escape_md_v2(text: str) -> str:
    special_chars = r'_[]()~`>#+-=|{}.!'
    for char in special_chars:
        text = text.replace(char, '\\' + char)
    return text

async def safe_edit(message: Message, text: str, reply_markup=None, parse_mode="MarkdownV2"):
    try:
        await message.edit_text(
            escape_md_v2(text) if parse_mode == "MarkdownV2" else text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.warning(f"Не удалось отредактировать сообщение: {e}")
        try:
            await message.answer("⚠ Произошла ошибка при обновлении. Попробуйте /start")
        except:
            pass

async def safe_answer(message: Message, text: str, reply_markup=None, parse_mode="MarkdownV2"):
    try:
        await message.answer(
            escape_md_v2(text) if parse_mode == "MarkdownV2" else text,
            reply_markup=reply_markup,
            parse_mode=parse_mode,
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}", exc_info=True)

async def build_config_list_keyboard(page: int, total: int, user_id: int, use_sorted: bool = False) -> InlineKeyboardMarkup:
    if use_sorted and user_id in sorted_by_ping_cache:
        configs, ts = sorted_by_ping_cache[user_id]
        if time.time() - ts > FASTEST_CACHE_TTL:
            configs = user_configs.get(user_id, [])
    else:
        configs = user_configs.get(user_id, [])
    builder = InlineKeyboardBuilder()
    start = page * ITEMS_PER_PAGE
    end = min(start + ITEMS_PER_PAGE, len(configs))
    sem = asyncio.Semaphore(MAX_CONCURRENT_PINGS)
    async def limited_ping(i: int):
        async with sem:
            return await get_ping(user_id, configs[i])
    if end > start:
        ping_tasks = [limited_ping(i) for i in range(start, end)]
        pings = await asyncio.gather(*ping_tasks)
    else:
        pings = []
    for i, ping in enumerate(pings, start=start):
        cfg = configs[i]
        short = cfg[:38] + "…" if len(cfg) > 38 else cfg
        short_esc = escape_md_v2(short)
        builder.button(text=f"[{ping}] {short_esc}", callback_data=f"cfg:{i}:{page}")
    builder.adjust(1)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"page:{page-1}"))
    pages_total = (len(configs) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    nav.append(InlineKeyboardButton(text=f"{page+1}/{pages_total}", callback_data="ignore"))
    if end < len(configs):
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"page:{page+1}"))
    builder.row(*nav)
    builder.row(
        InlineKeyboardButton(text="⚡ Лучшие (пинг)", callback_data="sort:fastest"),
        InlineKeyboardButton(text="Скачать этот список", callback_data="dl_menu:current")
    )
    builder.row(InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main"))
    return builder.as_markup()

def build_download_menu_keyboard(current_mode: str = "all") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for n in (5, 10, 15, 20, 30, 50):
        builder.button(text=f"{n} шт", callback_data=f"dl:{current_mode}:{n}")
    builder.button(text="Все", callback_data=f"dl:{current_mode}:all")
    builder.adjust(3)
    builder.row(InlineKeyboardButton(text="← Назад", callback_data="back_to_list"))
    return builder.as_markup()

def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📥 Все конфиги", callback_data="get:all"),
        InlineKeyboardButton(text="⚡ Самые быстрые", callback_data="get:fastest"),
    )
    builder.row(
        InlineKeyboardButton(text="🇷🇺 RU", callback_data="get:ru"),
        InlineKeyboardButton(text="🇩🇪 DE", callback_data="get:de"),
        InlineKeyboardButton(text="🇺🇸 US", callback_data="get:us"),
    )
    builder.row(
        InlineKeyboardButton(text="🇵🇱 PL", callback_data="get:pl"),
        InlineKeyboardButton(text="🇫🇷 FR", callback_data="get:fr"),
        InlineKeyboardButton(text="🇳🇱 NL", callback_data="get:nl"),
    )
    builder.row(
        InlineKeyboardButton(text="🔍 Только VLESS", callback_data="get:vless"),
        InlineKeyboardButton(text="👥 Клиенты", callback_data="clients"),
    )
    return builder.as_markup()

def get_fastest_count_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    counts = [20, 50, 100, 200]
    row = [InlineKeyboardButton(text=f"{c}", callback_data=f"fastest:{c}") for c in counts]
    builder.row(*row)
    builder.row(InlineKeyboardButton(text="Все (медленно)", callback_data="fastest:all"))
    builder.row(InlineKeyboardButton(text="← Назад", callback_data="back_to_main"))
    return builder.as_markup()

async def show_main_list(obj, user_id: int):
    if user_id not in user_configs or not user_configs[user_id]:
        text = "Конфиги ещё не загружены.\nВыберите действие ниже"
        kb = get_main_menu_keyboard()
    else:
        total = len(user_configs[user_id])
        text = (
            f"Найдено конфигов: {total}\n"
            f"Страница 1/{((total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)}\n"
            "Пинг — время TCP-подключения"
        )
        kb = await build_config_list_keyboard(0, total, user_id)
    if isinstance(obj, Message):
        await safe_answer(obj, text, kb)
    else:
        await safe_edit(obj, text, kb)

@router.message(Command("start", "help"))
async def cmd_start(message: Message):
    user_id = message.from_user.id
    if user_id in user_configs and user_configs[user_id]:
        total = len(user_configs[user_id])
        text = f"Конфиги уже загружены ({total} шт)\nВыберите действие:"
        kb = await build_config_list_keyboard(0, total, user_id, use_sorted=user_id in sorted_by_ping_cache)
    else:
        text = (
            "Бот раздаёт бесплатные VLESS / VMess / Trojan конфиги\n\n"
            "Выбери действие:"
        )
        kb = get_main_menu_keyboard()
    await safe_answer(message, text, kb)

async def load_and_show_configs(
    obj,
    user_id: int,
    is_fastest: bool = False,
    country: Optional[str] = None,
    ping_count: Optional[int] = None
):
    if isinstance(obj, Message):
        sent = await obj.answer("Собираю конфиги...")
    else:
        sent = await obj.edit_text("Собираю конфиги с серверов...")
    configs = []
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_url(session, url) for url in SOURCES]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, str):
                configs.extend(split_configs(res))
    if not configs:
        await safe_edit(sent, "Не удалось загрузить конфиги 😔")
        return
    if country:
        country = country.lower()
        filtered = [c for c in configs if country in (c.split("#")[-1].lower() if "#" in c else "")]
        configs = filtered
    if not configs:
        msg = f"Конфигов с '{country.upper()}' не найдено" if country else "Конфигов не найдено"
        await safe_edit(sent, msg)
        return
    user_configs[user_id] = configs
    user_ping_cache.pop(user_id, None)
    sorted_by_ping_cache.pop(user_id, None)
    if is_fastest:
        if ping_count is None:
            await safe_edit(
                sent,
                "Сколько серверов проверить на скорость?",
                get_fastest_count_keyboard()
            )
            return
        else:
            await safe_edit(
                sent,
                f"Пингую {ping_count if ping_count != 'all' else 'все'} серверов...\nЭто может занять время",
                None
            )
            await sort_by_ping(user_id, sent, limit=ping_count if ping_count != 'all' else None)
    else:
        await show_main_list(sent, user_id)

async def sort_by_ping(user_id: int, message_to_edit: Message, limit: Optional[int] = None):
    configs = user_configs.get(user_id, [])
    if not configs:
        await safe_edit(message_to_edit, "Нет конфигов для сортировки")
        return
    if limit is not None and isinstance(limit, int):
        configs = configs[:limit]
        text_limit = f" (первые {limit} из {len(user_configs[user_id])})"
    else:
        text_limit = ""
    total = len(configs)
    processed = 0
    sem = asyncio.Semaphore(MAX_CONCURRENT_PINGS)
    async def limited_ping(cfg: str):
        nonlocal processed
        async with sem:
            p = await get_ping(user_id, cfg)
            processed += 1
            if processed % 10 == 0 or processed == total:
                perc = round(processed / total * 100)
                try:
                    await safe_edit(
                        message_to_edit,
                        f"Пингую {processed}/{total} ({perc}%){text_limit}",
                        message_to_edit.reply_markup
                    )
                except:
                    pass
            return p
    ping_tasks = [limited_ping(cfg) for cfg in configs]
    pings_str = await asyncio.gather(*ping_tasks, return_exceptions=True)
    ping_values = []
    for p in pings_str:
        if isinstance(p, Exception) or p == "❌":
            ping_values.append(99999)
        else:
            try:
                ping_values.append(float(str(p).rstrip("ms")))
            except:
                ping_values.append(99999)
    sorted_indices = sorted(range(len(configs)), key=lambda i: ping_values[i])
    sorted_configs = [configs[i] for i in sorted_indices]
    sorted_by_ping_cache[user_id] = (sorted_configs, time.time())
    text = (
        f"Отсортировано по пингу (лучшие первые){text_limit}\n"
        f"Показано: {len(sorted_configs)} конфигов\n"
        f"Страница 1/{((len(sorted_configs) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)}"
    )
    kb = await build_config_list_keyboard(0, len(sorted_configs), user_id, use_sorted=True)
    await safe_edit(message_to_edit, text, kb)

@router.callback_query(F.data.startswith("get:"))
async def handle_get_action(callback: CallbackQuery):
    action = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id
    is_fastest = action == "fastest"
    country = None
    is_vless = action == "vless"
    if action not in ("all", "fastest", "vless"):
        country = action
    await load_and_show_configs(callback.message, user_id, is_fastest=is_fastest, country=country)
    await callback.answer()

@router.callback_query(F.data.startswith("fastest:"))
async def handle_fastest_count(callback: CallbackQuery):
    user_id = callback.from_user.id
    arg = callback.data.split(":", 1)[1]
    count = None if arg == "all" else int(arg)
    await load_and_show_configs(callback.message, user_id, is_fastest=True, ping_count=count)
    await callback.answer()

@router.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery):
    await safe_edit(
        callback.message,
        "Выбери действие:",
        get_main_menu_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data == "clients")
async def handle_clients(callback: CallbackQuery):
    text = (
        "Популярные клиенты для V2Ray / Xray / VLESS / VMess / Trojan:\n\n"
        "• v2rayNG (Android) — https://github.com/2dust/v2rayNG\n"
        "• Nekobox (Android) — https://github.com/MatsuriDayo/NekoBoxForAndroid\n"
        "• Shadowrocket (iOS) — App Store\n"
        "• Streisand / FoXray (iOS) — https://apps.apple.com/app/id...\n"
        "• V2RayN (Windows) — https://github.com/2dust/v2rayN\n"
        "• Qv2ray / v2rayA (Linux / Windows / macOS)\n"
        "• Hiddify Next (кроссплатформенный) — https://github.com/hiddify/hiddify-next\n\n"
        "Рекомендация: начните с v2rayNG (Android) или Hiddify Next."
    )
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main"))
    await safe_edit(callback.message, text, builder.as_markup())
    await callback.answer()

@router.callback_query(F.data == "cancel")
async def handle_cancel_inline(callback: CallbackQuery):
    uid = callback.from_user.id
    if uid in cancel_tasks and not cancel_tasks[uid].done():
        cancel_tasks[uid].cancel()
    user_configs.pop(uid, None)
    user_ping_cache.pop(uid, None)
    sorted_by_ping_cache.pop(uid, None)
    await safe_edit(
        callback.message,
        "Сессия очищена.\nЧто дальше?",
        get_main_menu_keyboard()
    )
    await callback.answer()

@router.callback_query(F.data.startswith("dl_menu:"))
async def show_download_menu_filtered(callback: CallbackQuery):
    user_id = callback.from_user.id
    mode = callback.data.split(":", 1)[1]
    if user_id not in user_configs:
        await callback.answer("Сначала загрузите конфиги", show_alert=True)
        return
    text = f"Сколько конфигов скачать ({mode.upper()}):"
    if mode == "fastest" and user_id not in sorted_by_ping_cache:
        text += "\n(список fastest ещё не отсортирован — будет отсортирован сейчас)"
    await safe_edit(callback.message, text, build_download_menu_keyboard(current_mode=mode))
    await callback.answer()

@router.callback_query(F.data.startswith("dl:"))
async def handle_download(callback: CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in user_configs:
        await callback.answer("Нет конфигов в сессии", show_alert=True)
        return
    try:
        _, mode, arg = callback.data.split(":")
    except:
        await callback.answer("Ошибка формата", show_alert=True)
        return
    if mode == "current":
        configs = user_configs[user_id]
        if user_id in sorted_by_ping_cache:
            configs, _ = sorted_by_ping_cache[user_id]
    elif mode == "fastest":
        if user_id not in sorted_by_ping_cache:
            await sort_by_ping(user_id, callback.message)
        configs, _ = sorted_by_ping_cache[user_id]
    elif mode in ("ru", "de", "us", "pl", "fr", "nl"):
        country = mode.lower()
        configs = [c for c in user_configs[user_id] if country in (c.split("#")[-1].lower() if "#" in c else "")]
    else:
        configs = user_configs[user_id]
    if arg == "all":
        selected = configs
    else:
        try:
            selected = configs[:int(arg)]
        except:
            selected = configs
    if not selected:
        await callback.answer("Нечего скачивать", show_alert=True)
        return
    path = f"configs_{user_id}_{mode}.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(selected))
    caption = f"Скачано {len(selected)} конфигов ({mode.upper()})"
    try:
        await callback.message.answer_document(
            FSInputFile(path),
            caption=caption
        )
    except Exception as e:
        logger.error(f"Ошибка отправки файла: {e}")
        await callback.message.answer("Не удалось отправить файл 😔")
    try:
        os.remove(path)
    except:
        pass
    await callback.answer()

@router.callback_query(F.data.startswith("page:"))
async def handle_page(callback: CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_configs:
        await callback.answer("Сессия устарела. Нажмите /start", show_alert=True)
        return
    try:
        page = int(callback.data.split(":")[1])
    except:
        return
    total = len(user_configs[uid])
    max_page = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    if page < 0 or page >= max_page:
        return
    use_sorted = uid in sorted_by_ping_cache
    kb = await build_config_list_keyboard(page, total, uid, use_sorted=use_sorted)
    text = (
        f"Найдено {total} конфигов\n"
        f"Страница {page+1}/{max_page}\n"
        "Пинг — время TCP-подключения"
    )
    await safe_edit(callback.message, text, kb)
    await callback.answer()

@router.callback_query(F.data.startswith("cfg:"))
async def show_one_config(callback: CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in user_configs:
        await callback.answer("Сессия устарела", show_alert=True)
        return
    try:
        _, idx_str, page_str = callback.data.split(":")
        idx = int(idx_str)
        page = int(page_str)
    except:
        await callback.answer()
        return
    configs = user_configs[user_id]
    if idx >= len(configs):
        await callback.answer()
        return
    cfg = configs[idx]
    ping = await get_ping(user_id, cfg)
    builder = InlineKeyboardBuilder()
    builder.button(text="← Назад к списку", callback_data=f"page:{page}")
    builder.row(InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main"))
    try:
        await callback.message.answer(
            f"Конфиг #{idx+1} Пинг: {ping}\n\n"
            f"Скопируй весь текст ниже:\n\n"
            f"{cfg}",
            reply_markup=builder.as_markup(),
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Ошибка при отправке конфига: {e}")
        await callback.message.answer("Не удалось отправить конфиг. Попробуйте позже.")
    await callback.answer("Конфиг отправлен ↓")

@router.callback_query(F.data == "back_to_list")
async def back_to_list(callback: CallbackQuery):
    user_id = callback.from_user.id
    if user_id not in user_configs:
        await callback.answer("Сессия устарела", show_alert=True)
        return
    total = len(user_configs[user_id])
    use_sorted = user_id in sorted_by_ping_cache
    kb = await build_config_list_keyboard(0, total, user_id, use_sorted=use_sorted)
    text = (
        f"Найдено {total} конфигов\n"
        f"Страница 1/{((total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)}\n"
        "Пинг — время TCP-подключения"
    )
    await safe_edit(callback.message, text, kb)
    await callback.answer()

@router.callback_query(F.data == "sort:fastest")
async def handle_sort_fastest(callback: CallbackQuery):
    uid = callback.from_user.id
    if uid not in user_configs:
        await callback.answer("Сессия устарела. Используйте /start", show_alert=True)
        return
    await load_and_show_configs(callback.message, uid, is_fastest=True)
    await callback.answer()

async def auto_update_configs():
    while True:
        await asyncio.sleep(UPDATE_INTERVAL_MIN * 60)
        new_configs = []
        async with aiohttp.ClientSession() as session:
            results = await asyncio.gather(*[fetch_url(session, u) for u in SOURCES], return_exceptions=True)
            for r in results:
                if isinstance(r, str):
                    new_configs.extend(split_configs(r))
        if new_configs:
            logger.info(f"Обновлены конфиги: {len(new_configs)} строк")
            for uid in list(user_configs.keys()):
                user_configs[uid] = new_configs[:]
                user_ping_cache.pop(uid, None)
                sorted_by_ping_cache.pop(uid, None)

async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    asyncio.create_task(auto_update_configs())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())