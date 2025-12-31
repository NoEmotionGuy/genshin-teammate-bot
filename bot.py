import os
import logging
from datetime import datetime
from typing import Optional, Set

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    ParseMode, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand,
    ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
)
from dotenv import load_dotenv
import requests
import db

# load .env
load_dotenv()

TOKEN = os.getenv("TELEGRAM_TOKEN")
if not TOKEN:
    raise SystemExit("TELEGRAM_TOKEN не задан. Поместите его в .env или в переменные окружения.")

DEVELOPER_ID = os.getenv("DEVELOPER_ID")
try:
    DEVELOPER_ID_INT = int(DEVELOPER_ID) if DEVELOPER_ID else None
except Exception:
    DEVELOPER_ID_INT = None

# logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# optional quick token check (won't stop startup if fails, but warns)
try:
    r = requests.get(f"https://api.telegram.org/bot{TOKEN}/getMe", timeout=5)
    if r.status_code != 200 or not r.json().get("ok"):
        logger.warning("Telegram token check failed (getMe returned not ok).")
except Exception:
    logger.warning("Не удалось проверить токен у Telegram (продолжаем).")

bot = Bot(token=TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

# Servers list
SERVERS = [
    ("Asia", "Asia"),
    ("Europe", "Europe"),
    ("North America", "NA"),
    ("TW/HK/MO", "TW"),
    ("China", "CN"),
]

# Languages shown as buttons (code -> emoji)
LANG_BUTTONS = [
    ("RU", "🇷🇺"), ("EN", "🇬🇧"), ("UA", "🇺🇦"), ("BY", "🇧🇾"),
    ("KZ", "🇰🇿"), ("RS", "🇷🇸"), ("EE", "🇪🇪"), ("BG", "🇧🇬"),
    ("LT", "🇱🇹"), ("LV", "🇱🇻"), ("GE", "🇬🇪"), ("MD", "🇲🇩"),
]
LANG_EMOJI = {code.upper(): emoji for code, emoji in LANG_BUTTONS}

# In-memory viewing contexts per viewer (resets on bot restart)
view_contexts = {}
# In-memory convenience to prevent duplicates in runtime if needed (not primary store)
# but actual persistence of likes is in DB (db.likes table).
# liked_pairs kept optionally – not strictly necessary, but we can omit to rely on DB.

class Form(StatesGroup):
    choosing_server = State()
    nickname = State()
    uid = State()
    adventure_rank = State()
    languages = State()
    playtime = State()
    bio = State()
    confirm = State()
    sending_message = State()

# ---------------- helper UI functions ----------------

def servers_keyboard(prefix: str = "server"):
    kb = InlineKeyboardMarkup(row_width=2)
    for label, key in SERVERS:
        kb.insert(InlineKeyboardButton(label, callback_data=f"{prefix}:{key}"))
    return kb

def languages_keyboard(selected: Set[str]):
    def label(code, emoji, sel_set):
        return f"{emoji} {code}" + (" ✅" if code in sel_set else "")
    kb = InlineKeyboardMarkup(row_width=3)
    for code, emoji in LANG_BUTTONS:
        kb.insert(InlineKeyboardButton(label(code, emoji, selected), callback_data=f"lang:{code}"))
    kb.row(InlineKeyboardButton("Готово", callback_data="lang:DONE"))
    return kb

def reply_action_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    kb.row(
        KeyboardButton("👍 Лайк"),
        KeyboardButton("✉️ Письмо"),
        KeyboardButton("👎 Дизлайк"),
        KeyboardButton("⏹️ Стоп"),
    )
    return kb

def main_menu_keyboard():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=False)
    kb.row(KeyboardButton("Смотреть анкеты"), KeyboardButton("Моя анкета"))
    return kb

def get_owner_id(profile: dict) -> Optional[int]:
    for key in ("tg_id", "owner_id", "user_id", "id"):
        v = profile.get(key)
        if v:
            try:
                return int(v)
            except Exception:
                continue
    return None

def format_language_flags(langs_raw: str) -> str:
    if not langs_raw:
        return ""
    parts = [p.strip().upper() for p in langs_raw.split(",") if p.strip()]
    emojis = []
    for p in parts:
        em = LANG_EMOJI.get(p)
        emojis.append(em if em else p)
    return " ".join(emojis)

# ---------------- core flow ----------------

async def send_profile_with_actions(viewer_id: int, server: str, offset: int):
    total = await db.count_profiles(server)
    if total == 0:
        await bot.send_message(viewer_id, f"Анкет на сервере {server} ещё нет.")
        await bot.send_message(viewer_id, "Меню:", reply_markup=main_menu_keyboard())
        return

    # clamp offset
    if offset < 0:
        offset = 0
    if offset >= total:
        offset = total - 1

    profiles = await db.list_profiles(server, limit=1, offset=offset)
    if not profiles:
        await bot.send_message(viewer_id, "Ошибка загрузки анкеты.")
        return

    prof = profiles[0]
    owner_id = get_owner_id(prof)
    like_num = await db.get_likes_count(owner_id) if owner_id else 0
    langs_flags = format_language_flags(prof.get("languages", "") or "")

    text = (
        f"Ник: {prof.get('nickname')}\n"
        f"UID: {prof.get('uid')}\n"
        f"AR: {prof.get('adventure_rank')}\n"
        f"Языки: {langs_flags}\n"
        f"Часовой пояс (от MSK): {prof.get('playtime')}\n"
        f"О себе: {prof.get('bio')}\n"
        f"Лайков: {like_num}\n"
    )

    profile_id = prof.get("id") or owner_id or ""
    inline_kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("⚠️ Пожаловаться", callback_data=f"complain:{owner_id}:{profile_id}")
    )

    profile_msg = await bot.send_message(viewer_id, text, reply_markup=inline_kb)

    prev_ctx = view_contexts.get(viewer_id)
    prev_kb_msg_id = prev_ctx.get("keyboard_message_id") if prev_ctx else None

    kb_msg = await bot.send_message(viewer_id, "Действия (используйте кнопки ниже):", reply_markup=reply_action_keyboard())

    if prev_kb_msg_id:
        try:
            await bot.delete_message(viewer_id, prev_kb_msg_id)
        except Exception:
            pass

    view_contexts[viewer_id] = {
        "server": server,
        "offset": offset,
        "total": total,
        "owner_id": owner_id,
        "profile_id": profile_id,
        "keyboard_message_id": kb_msg.message_id,
        "profile_message_id": profile_msg.message_id,
    }
    logger.info("Stored context for %s: server=%s offset=%s owner=%s", viewer_id, server, offset, owner_id)

# ---------------- handlers ----------------

@dp.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    prof = await db.get_profile_by_tg(message.from_user.id)
    if prof:
        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton("Показать мою анкету", callback_data="profile:view"),
            InlineKeyboardButton("Редактировать анкету", callback_data="profile:edit"),
        )
        kb.add(InlineKeyboardButton("Удалить анкету", callback_data="profile:delete"))
        kb.add(InlineKeyboardButton("Отмена", callback_data="profile:cancel"))
        await message.answer("У вас уже есть сохранённая анкета. Что вы хотите сделать?", reply_markup=kb)
        return

    await message.answer("Привет! Я бот для поиска тиммейтов по Genshin.\nСначала выберите сервер:", reply_markup=servers_keyboard(prefix="server"))
    await Form.choosing_server.set()

@dp.message_handler(commands=["edit"])
async def cmd_edit(message: types.Message, state: FSMContext):
    prof = await db.get_profile_by_tg(message.from_user.id)
    if not prof:
        await message.answer("Анкета не найдена. Создать: /start")
        return
    await state.update_data(**prof, editing=True)
    current_nick = prof.get("nickname") or "(пусто)"
    await message.answer(f"Редактирование анкеты. Текущий ник: {current_nick}\nВведите новый ник (или отправьте '-' чтобы оставить текущий):")
    await Form.nickname.set()

@dp.callback_query_handler(lambda c: c.data == "profile:edit")
async def profile_edit_callback(callback_query: types.CallbackQuery, state: FSMContext):
    await bot.answer_callback_query(callback_query.id)
    prof = await db.get_profile_by_tg(callback_query.from_user.id)
    if not prof:
        await bot.send_message(callback_query.from_user.id, "Анкета не найдена. Создать: /start")
        return
    try:
        await state.update_data(**prof, editing=True)
    except Exception as e:
        logger.exception("Failed to set FSM data for edit: %s", e)
        await bot.send_message(callback_query.from_user.id, "Ошибка при подготовке редактирования. Попробуйте /edit")
        return
    current_nick = prof.get("nickname") or "(пусто)"
    await bot.send_message(callback_query.from_user.id, f"Редактирование анкеты. Текущий ник: {current_nick}\nВведите новый ник (или отправьте '-' чтобы оставить текущий):")
    await Form.nickname.set()

@dp.callback_query_handler(lambda c: c.data == "profile:cancel")
async def profile_cancel(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id, text="Отменено.")

@dp.callback_query_handler(lambda c: c.data == "profile:view")
async def profile_view(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    prof = await db.get_profile_by_tg(callback_query.from_user.id)
    if not prof:
        await bot.send_message(callback_query.from_user.id, "Анкета не найдена.")
        return
    owner_id = get_owner_id(prof)
    like_num = await db.get_likes_count(owner_id) if owner_id else 0
    langs_flags = format_language_flags(prof.get("languages", "") or "")
    text = (
        f"Ваша анкета:\n\n"
        f"Сервер: {prof.get('server')}\n"
        f"Ник: {prof.get('nickname')}\n"
        f"UID: {prof.get('uid')}\n"
        f"AR: {prof.get('adventure_rank')}\n"
        f"Языки: {langs_flags}\n"
        f"Часовой пояс (от MSK): {prof.get('playtime')}\n"
        f"О себе: {prof.get('bio')}\n"
        f"Лайков: {like_num}\n"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Удалить анкету", callback_data="profile:delete"))
    kb.add(InlineKeyboardButton("Редактировать", callback_data="profile:edit"))
    await bot.send_message(callback_query.from_user.id, text, reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("complain:"))
async def handle_complain(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id, text="Жалоба зарегистрирована. Спасибо.")
    parts = callback_query.data.split(":", 2)
    if len(parts) < 3:
        logger.warning("Invalid complain callback data: %s", callback_query.data)
        return
    owner_part = parts[1]
    profile_part = parts[2]
    reporter = callback_query.from_user
    reporter_info = f"{reporter.full_name} (id={reporter.id})"

    profile_info = ""
    try:
        owner_id = int(owner_part) if owner_part and str(owner_part).isdigit() else None
    except Exception:
        owner_id = None

    if owner_id:
        prof = await db.get_profile_by_tg(owner_id)
        if prof:
            langs_flags = format_language_flags(prof.get("languages", "") or "")
            profile_info = (
                f"Ник: {prof.get('nickname')}\nUID: {prof.get('uid')}\nAR: {prof.get('adventure_rank')}\n"
                f"Языки: {langs_flags}\nЧасовой пояс: {prof.get('playtime')}\nО себе: {prof.get('bio')}\n"
            )
        else:
            profile_info = f"Анкета с owner_id={owner_id} не найдена в БД."
    else:
        profile_info = f"Неполные данные анкеты: profile_id={profile_part}, owner={owner_part}"

    dev_msg = (
        f"⚠️ Поступила жалоба на анкету\n\n"
        f"Отправитель: {reporter_info}\n"
        f"Анкета (owner_id={owner_part}, profile_id={profile_part}):\n\n"
        f"{profile_info}\n"
    )

    # Add inline delete button for developer convenience
    kb = InlineKeyboardMarkup()
    if owner_part and str(owner_part).isdigit():
        kb.add(InlineKeyboardButton("Удалить анкету (DEV)", callback_data=f"dev:delete:{owner_part}"))

    if DEVELOPER_ID_INT:
        try:
            await bot.send_message(DEVELOPER_ID_INT, dev_msg, reply_markup=kb if kb.inline_keyboard else None)
            logger.info("Complaint forwarded to developer %s by %s", DEVELOPER_ID_INT, reporter_info)
        except Exception:
            logger.exception("Failed to forward complaint to developer.")
    else:
        logger.warning("Developer ID not configured; complaint: %s", dev_msg)

@dp.callback_query_handler(lambda c: c.data == "profile:delete")
async def profile_delete_request(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✅ Подтвердить удаление", callback_data="profile:delete_confirm"),
        InlineKeyboardButton("❌ Отмена", callback_data="profile:delete_cancel"),
    )
    await bot.send_message(callback_query.from_user.id, "Вы уверены, что хотите удалить вашу анкету? Это действие нельзя отменить.", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data == "profile:delete_confirm")
async def profile_delete_confirm(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await db.delete_profile(callback_query.from_user.id)
    await bot.send_message(callback_query.from_user.id, "Ваша анкета удалена.")
    await bot.send_message(callback_query.from_user.id, "Если хотите создать новую анкету — используйте /start")

@dp.callback_query_handler(lambda c: c.data == "profile:delete_cancel")
async def profile_delete_cancel(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id, text="Удаление отменено.")
    await bot.send_message(callback_query.from_user.id, "Удаление отменено. Ваша анкета сохранена.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("server:"), state=Form.choosing_server)
async def process_server_with_state(callback_query: types.CallbackQuery, state: FSMContext):
    server = callback_query.data.split(":",1)[1]
    await state.update_data(server=server)
    await bot.answer_callback_query(callback_query.id, text=f"Сервер {server} выбран")
    try:
        await bot.send_message(callback_query.from_user.id, f"Вы выбрали сервер: <b>{server}</b>\nТеперь введите ваш никнейм (в Genshin):", parse_mode=ParseMode.HTML, reply_markup=ReplyKeyboardRemove())
    except Exception:
        await bot.send_message(callback_query.from_user.id, f"Вы выбрали сервер: <b>{server}</b>\nТеперь введите ваш никнейм (в Genshin):", parse_mode=ParseMode.HTML)
    await Form.nickname.set()

@dp.message_handler(state=Form.nickname)
async def process_nickname(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    editing = data.get("editing", False)
    if not (txt == "-" and editing):
        await state.update_data(nickname=txt[:64])
    await message.answer("UID (можно пропустить) или отправьте '-' для пропуска/сохранения (при редактировании):")
    await Form.uid.set()

@dp.message_handler(state=Form.uid)
async def process_uid(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    editing = data.get("editing", False)
    if not (txt == "-" and editing):
        await state.update_data(uid=(txt if txt != "-" else ""))
    await message.answer("Adventure Rank (AR) — введите число от 1 до 60 или '-' для пропуска/сохранения (при редактировании):")
    await Form.adventure_rank.set()

@dp.message_handler(state=Form.adventure_rank)
async def process_ar(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    editing = data.get("editing", False)
    if not (txt == "-" and editing):
        if txt == "-":
            await state.update_data(adventure_rank="")
        else:
            try:
                ar = int(txt)
            except ValueError:
                await message.answer("Неверный формат AR. Введите число от 1 до 60 или '-' для пропуска/сохранения:")
                return
            if not (1 <= ar <= 60):
                await message.answer("AR вне допустимого диапазона. Введите число от 1 до 60 или '-' для пропуска/сохранения:")
                return
            await state.update_data(adventure_rank=str(ar))
    selected = set([p.strip().upper() for p in (data.get("languages","") or "").split(",") if p.strip()])
    kb = languages_keyboard(selected)
    prompt = "Выберите языки (нажмите кнопки, чтобы отметить/снять):"
    if editing:
        prompt += " Нажмите Готово, чтобы продолжить (или оставьте выбор без изменений)."
    await message.answer(prompt, reply_markup=kb)
    await Form.languages.set()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("lang:"), state=Form.languages)
async def process_lang_toggle(callback_query: types.CallbackQuery, state: FSMContext):
    action = callback_query.data.split(":",1)[1]
    data = await state.get_data()
    langs_raw = data.get("languages", "") or ""
    selected = set([p.strip().upper() for p in langs_raw.split(",") if p.strip()])
    codes = {code for code, _ in LANG_BUTTONS}
    if action in codes:
        if action in selected:
            selected.remove(action)
        else:
            selected.add(action)
        await state.update_data(languages=",".join(sorted(selected)))
        kb = languages_keyboard(selected)
        try:
            await bot.edit_message_text(chat_id=callback_query.from_user.id, message_id=callback_query.message.message_id, text="Выберите языки (нажмите кнопки, чтобы отметить/снять):", reply_markup=kb)
        except Exception:
            await bot.answer_callback_query(callback_query.id, text="Обновлено")
            await bot.send_message(callback_query.from_user.id, "Выберите языки (нажмите кнопки, чтобы отметить/снять):", reply_markup=kb)
        await bot.answer_callback_query(callback_query.id)
        return
    if action == "DONE":
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id, "Сколько часов относительно MSK? Введите целое число (например: 0, +3, -2). Отправьте '-' чтобы пропустить/сохранить (при редактировании).")
        await Form.playtime.set()
        return

@dp.message_handler(state=Form.languages)
async def process_languages_text_blocked(message: types.Message, state: FSMContext):
    await message.answer("Пожалуйста, используйте кнопки для выбора языков. Ввод текста для языков отключён.")

@dp.message_handler(state=Form.playtime)
async def process_playtime(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    editing = data.get("editing", False)
    if not (txt == "-" and editing):
        if txt == "-":
            await state.update_data(playtime="")
        else:
            val = txt.upper().replace(" ", "")
            parsed = None
            if val.startswith("MSK"):
                rest = val[3:]
                if rest in ("", "+", "+0"):
                    parsed = 0
                else:
                    try:
                        parsed = int(rest.replace("+", ""))
                    except Exception:
                        parsed = None
            else:
                try:
                    parsed = int(val)
                except Exception:
                    parsed = None
            if parsed is None:
                if 1 <= len(txt) <= 64:
                    await state.update_data(playtime=txt[:64])
                else:
                    await message.answer("Непонятный формат. Введите целое число (например 0, +2, -3) относительно MSK, или '-' для пропуска/сохранения:")
                    return
            else:
                if not (-12 <= parsed <= 14):
                    await message.answer("Сдвиг от MSK должен быть в диапазоне от -12 до +14. Введите другое значение или '-' для пропуска/сохранения:")
                    return
                sign = f"+{parsed}" if parsed >= 0 else str(parsed)
                await state.update_data(playtime=f"MSK{sign}")
    await message.answer("Коротко о себе / что ищете (до 500 символов). Отправьте '-' чтобы оставить текущее (при редактировании).")
    await Form.bio.set()

@dp.message_handler(state=Form.bio)
async def process_bio(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    editing = data.get("editing", False)
    if not (txt == "-" and editing):
        await state.update_data(bio=txt[:500])
    data = await state.get_data()
    preview_playtime = data.get('playtime', '')
    preview = (
        f"Анкета (предпросмотр):\n\n"
        f"Сервер: {data.get('server')}\n"
        f"Ник: {data.get('nickname')}\n"
        f"UID: {data.get('uid')}\n"
        f"AR: {data.get('adventure_rank')}\n"
        f"Языки: {format_language_flags(data.get('languages','') or '')}\n"
        f"Часовой пояс (от MSK): {preview_playtime}\n"
        f"О себе: {data.get('bio')}\n"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Подтвердить и сохранить", callback_data="confirm:yes"))
    kb.add(InlineKeyboardButton("Отмена", callback_data="confirm:no"))
    await message.answer(preview, reply_markup=kb)
    await Form.confirm.set()

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("confirm:"), state=Form.confirm)
async def process_confirm(callback_query: types.CallbackQuery, state: FSMContext):
    choice = callback_query.data.split(":",1)[1]
    await bot.answer_callback_query(callback_query.id)
    if choice == "yes":
        data = await state.get_data()
        data_to_save = {k: v for k, v in data.items() if k != "editing"}
        if "platforms" not in data_to_save:
            data_to_save["platforms"] = ""
        if "playstyle" not in data_to_save:
            data_to_save["playstyle"] = ""
        await db.save_profile(callback_query.from_user.id, data_to_save)
        await bot.send_message(callback_query.from_user.id, "Анкета сохранена! Используйте /search чтобы просматривать анкеты.")
        await state.finish()
    else:
        await bot.send_message(callback_query.from_user.id, "Анкета не сохранена. Начните заново с /start.")
        await state.finish()

# Search flow
@dp.message_handler(commands=["search"])
async def cmd_search(message: types.Message):
    await message.answer("Выберите сервер для просмотра анкет:", reply_markup=servers_keyboard(prefix="browse_server"))

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("browse_server:"))
async def process_browse_server(callback_query: types.CallbackQuery):
    server = callback_query.data.split(":",1)[1]
    await bot.answer_callback_query(callback_query.id)
    await send_profile_with_actions(callback_query.from_user.id, server, 0)

# Actions: Like / Message / Dislike / Stop
@dp.message_handler(lambda m: m.text in ("👍 Лайк", "✉️ Письмо", "👎 Дизлайк", "⏹️ Стоп"))
async def handle_action_message(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    cmd = message.text
    ctx = view_contexts.get(user_id)
    if not ctx:
        await message.answer("Нет текущей просматриваемой анкеты. Сначала используйте /search и выберите сервер.")
        return

    server = ctx["server"]
    offset = ctx["offset"]
    total = ctx["total"]
    owner_id = ctx["owner_id"]

    if cmd == "👍 Лайк":
        if owner_id is None or owner_id == 0:
            await message.answer("Невозможно поставить лайк — не найден владелец анкеты.")
            return
        if owner_id == user_id:
            await message.answer("Нельзя лайкать свою анкету.")
            return
        already = await db.has_liked(user_id, owner_id)
        if already:
            await message.answer("Вы уже ставили лайк этой анкете ранее.")
            return
        inserted = await db.add_like(user_id, owner_id)
        if inserted:
            liker = message.from_user
            liker_name = liker.username and f"@{liker.username}" or liker.full_name
            try:
                await bot.send_message(owner_id, f"Ваша анкета получила лайк от {liker_name}.")
                await message.answer("Лайк отправлен и владелец уведомлён. Переходим к следующей анкете.", reply_markup=ReplyKeyboardRemove())
            except Exception:
                await message.answer("Лайк учтён, но не удалось уведомить владельца (возможно, он заблокировал бота). Переходим к следующей анкете.", reply_markup=ReplyKeyboardRemove())
        else:
            await message.answer("Не удалось поставить лайк (возможно, вы уже ставили).", reply_markup=ReplyKeyboardRemove())
        next_offset = offset + 1
        if next_offset >= total:
            kb_msg_id = ctx.get("keyboard_message_id")
            if kb_msg_id:
                try:
                    await bot.delete_message(user_id, kb_msg_id)
                except Exception:
                    pass
            view_contexts.pop(user_id, None)
            await bot.send_message(user_id, "Больше анкет нет. Просмотр остановлен.", reply_markup=main_menu_keyboard())
            return
        await send_profile_with_actions(user_id, server, next_offset)
        return

    if cmd == "👎 Дизлайк":
        next_offset = offset + 1
        if next_offset >= total:
            kb_msg_id = ctx.get("keyboard_message_id")
            if kb_msg_id:
                try:
                    await bot.delete_message(user_id, kb_msg_id)
                except Exception:
                    pass
            view_contexts.pop(user_id, None)
            await bot.send_message(user_id, "Это была последняя анкета. Просмотр остановлен.", reply_markup=main_menu_keyboard())
            return
        await send_profile_with_actions(user_id, server, next_offset)
        return

    if cmd == "✉️ Письмо":
        if owner_id is None:
            await message.answer("Невозможно отправить сообщение — не найден владелец анкеты.")
            return
        if owner_id == user_id:
            await message.answer("Нельзя отправлять сообщение самому себе.")
            return
        await state.update_data(message_target=owner_id)
        await message.answer("Введите сообщение, которое хотите отправить владельцу анкеты. Отправьте '-' чтобы отменить.", reply_markup=ReplyKeyboardRemove())
        await Form.sending_message.set()
        return

    if cmd == "⏹️ Стоп":
        kb_msg_id = ctx.get("keyboard_message_id")
        if kb_msg_id:
            try:
                await bot.delete_message(user_id, kb_msg_id)
            except Exception:
                pass
        view_contexts.pop(user_id, None)
        await bot.send_message(user_id, "Просмотр остановлен.", reply_markup=main_menu_keyboard())
        return

@dp.message_handler(state=Form.sending_message)
async def handle_sending_message(message: types.Message, state: FSMContext):
    txt = message.text.strip()
    data = await state.get_data()
    target_id = data.get("message_target")
    if txt == "-":
        await message.answer("Отправка отменена.")
        await state.finish()
        ctx = view_contexts.get(message.from_user.id)
        if ctx:
            kb_msg = await bot.send_message(message.from_user.id, "Действия (используйте кнопки ниже):", reply_markup=reply_action_keyboard())
            prev_kb = ctx.get("keyboard_message_id")
            if prev_kb:
                try:
                    await bot.delete_message(message.from_user.id, prev_kb)
                except Exception:
                    pass
            ctx["keyboard_message_id"] = kb_msg.message_id
        else:
            await bot.send_message(message.from_user.id, "Меню:", reply_markup=main_menu_keyboard())
        return
    if not target_id:
        await message.answer("Не удалось найти получателя. Отмена.")
        await state.finish()
        return
    sender = message.from_user
    sender_name = sender.username and f"@{sender.username}" or sender.full_name
    forward_text = f"Сообщение от {sender_name} через бот:\n\n{txt}"
    try:
        await bot.send_message(target_id, forward_text)
        await message.answer("Сообщение отправлено владельцу анкеты.")
    except Exception as e:
        logger.exception("Failed to forward message to owner: %s", e)
        await message.answer("Не удалось отправить сообщение владельцу (возможно, он заблокировал бота).")
    ctx = view_contexts.get(message.from_user.id)
    if ctx:
        kb_msg = await bot.send_message(message.from_user.id, "Действия (используйте кнопки ниже):", reply_markup=reply_action_keyboard())
        prev_kb = ctx.get("keyboard_message_id")
        if prev_kb:
            try:
                await bot.delete_message(message.from_user.id, prev_kb)
            except Exception:
                pass
        ctx["keyboard_message_id"] = kb_msg.message_id
    else:
        await bot.send_message(message.from_user.id, "Меню:", reply_markup=main_menu_keyboard())
    await state.finish()

@dp.message_handler(lambda m: m.text == "Смотреть анкеты")
async def menu_watch_profiles(message: types.Message):
    await message.answer("Выберите сервер для просмотра анкет:", reply_markup=servers_keyboard(prefix="browse_server"))

@dp.message_handler(lambda m: m.text == "Моя анкета")
async def menu_my_profile(message: types.Message):
    prof = await db.get_profile_by_tg(message.from_user.id)
    if not prof:
        await message.answer("Анкета не найдена. Создать: /start", reply_markup=main_menu_keyboard())
        return
    owner_id = get_owner_id(prof)
    like_num = await db.get_likes_count(owner_id) if owner_id else 0
    langs_flags = format_language_flags(prof.get("languages", "") or "")
    text = (
        f"Ваша анкета:\n\n"
        f"Сервер: {prof.get('server')}\n"
        f"Ник: {prof.get('nickname')}\n"
        f"UID: {prof.get('uid')}\n"
        f"AR: {prof.get('adventure_rank')}\n"
        f"Языки: {langs_flags}\n"
        f"Часовой пояс (от MSK): {prof.get('playtime')}\n"
        f"О себе: {prof.get('bio')}\n"
        f"Лайков: {like_num}\n"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Удалить анкету", callback_data="profile:delete"))
    kb.add(InlineKeyboardButton("Редактировать", callback_data="profile:edit"))
    await message.answer(text, reply_markup=kb)

@dp.message_handler(commands=["myprofile"])
async def cmd_myprofile(message: types.Message):
    prof = await db.get_profile_by_tg(message.from_user.id)
    if not prof:
        await message.answer("Анкета не найдена. Создать: /start")
        return
    owner_id = get_owner_id(prof)
    like_num = await db.get_likes_count(owner_id) if owner_id else 0
    langs_flags = format_language_flags(prof.get("languages", "") or "")
    text = (
        f"Ваша анкета:\n\n"
        f"Сервер: {prof.get('server')}\n"
        f"Ник: {prof.get('nickname')}\n"
        f"UID: {prof.get('uid')}\n"
        f"AR: {prof.get('adventure_rank')}\n"
        f"Языки: {langs_flags}\n"
        f"Часовой пояс (от MSK): {prof.get('playtime')}\n"
        f"О себе: {prof.get('bio')}\n"
        f"Лайков: {like_num}\n"
    )
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Удалить анкету", callback_data="profile:delete"))
    kb.add(InlineKeyboardButton("Редактировать", callback_data="profile:edit"))
    await message.answer(text, reply_markup=kb)

@dp.message_handler(commands=["delete_profile"])
async def cmd_delete_profile(message: types.Message):
    # developer-only command to delete profile
    if DEVELOPER_ID_INT is None or message.from_user.id != DEVELOPER_ID_INT:
        await message.reply("Команда доступна только разработчику.")
        return
    parts = message.text.strip().split()
    if len(parts) < 2:
        await message.reply("Использование: /delete_profile <tg_id>")
        return
    try:
        target_id = int(parts[1])
    except Exception:
        await message.reply("Неверный tg_id.")
        return
    await db.delete_profile(target_id)
    await message.reply(f"Анкета {target_id} удалена.")
    try:
        await bot.send_message(target_id, "Ваша анкета была удалена администратором.")
    except Exception:
        pass

@dp.message_handler(commands=["cancel"])
async def cmd_cancel(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer("Операция отменена.", reply_markup=ReplyKeyboardRemove())

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("dev:delete:"))
async def dev_delete_profile_callback(callback_query: types.CallbackQuery):
    # защита: только разработчик
    if DEVELOPER_ID_INT is None or callback_query.from_user.id != DEVELOPER_ID_INT:
        await bot.answer_callback_query(callback_query.id, text="Нет доступа")
        return

    parts = callback_query.data.split(":")
    if len(parts) != 3:
        await bot.answer_callback_query(callback_query.id, text="Ошибка данных")
        return

    try:
        target_id = int(parts[2])
    except ValueError:
        await bot.answer_callback_query(callback_query.id, text="Неверный tg_id")
        return

    await db.delete_profile(target_id)

    await bot.answer_callback_query(callback_query.id, text="Анкета удалена")

    await bot.send_message(
        callback_query.from_user.id,
        f"Анкета пользователя {target_id} удалена."
    )

    try:
        await bot.send_message(
            target_id,
            "Ваша анкета была удалена администратором."
        )
    except Exception:
        pass

# ---------------- startup/shutdown ----------------

async def on_startup(_):
    # Initialize DB (creates profiles and likes tables if not exist)
    await db.init_db()
    logger.info("DB initialized and bot started")
    try:
        commands = [
            BotCommand(command="start", description="Начать / создать анкету"),
            BotCommand(command="search", description="Поиск/просмотр анкет"),
            BotCommand(command="edit", description="Редактировать вашу анкету"),
        ]
        await bot.set_my_commands(commands)
    except Exception as e:
        logger.warning("Не удалось установить команды бота: %s", e)

if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)