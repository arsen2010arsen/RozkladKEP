import asyncio
import requests
import re
import time
import os
import html
from datetime import datetime, timedelta
from typing import Callable, Dict, Any, Awaitable
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.filters import StateFilter
from aiohttp import web
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo
from bson.objectid import ObjectId
from pytz import timezone

class NoteStates(StatesGroup):
    waiting_for_note_text = State()
    waiting_for_subject_selection = State()
    waiting_for_edit_text = State()

load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
URL = "https://kep.nung.edu.ua/pages/education/schedule"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# База даних
MONGO_URL = os.getenv("MONGO_URL")
cluster = AsyncIOMotorClient(MONGO_URL) if MONGO_URL else None
db = cluster["rozklad_db"] if cluster else None
users_collection = db["users"] if db is not None else None
notes_collection = db["notes"] if db is not None else None
state_collection = db["bot_state"] if db is not None else None

ADMIN_ID = int(os.getenv("ADMIN_ID", 123456789))

class AntiSpamMiddleware(BaseMiddleware):
    def __init__(self, limit: float = 1.0):
        self.limit = limit
        self.users = {}

    async def __call__(self, handler: Callable[[Message, Dict[str, Any]], Awaitable[Any]], event: Message | CallbackQuery, data: Dict[str, Any]) -> Any:
        uid = event.from_user.id
        now = time.time()
        if uid in self.users and now - self.users[uid] < self.limit:
            return 
        self.users[uid] = now
        return await handler(event, data)

dp.message.middleware(AntiSpamMiddleware(0.8))
dp.callback_query.middleware(AntiSpamMiddleware(0.8))

# --- ЛОГІКА ЧАСУ ---
async def get_current_week():
    tz = ZoneInfo("Europe/Kiev")
    start = datetime(2026, 3, 9, tzinfo=tz).date()
    if state_collection is not None:
        state = await state_collection.find_one({"_id": "global_state"})
        if state and "SEMESTER_START_DATE" in state:
            start = datetime.strptime(state["SEMESTER_START_DATE"], "%Y-%m-%d").replace(tzinfo=tz).date()
    now = datetime.now(tz).date()
    diff = (now - start).days
    if diff < 0: return 1
    return ((diff // 7) % 4) + 1

async def get_week_dates(w):
    tz = ZoneInfo("Europe/Kiev")
    start_date = datetime(2026, 3, 9, tzinfo=tz).date()
    if state_collection is not None:
        state = await state_collection.find_one({"_id": "global_state"})
        if state and "SEMESTER_START_DATE" in state:
            start_date = datetime.strptime(state["SEMESTER_START_DATE"], "%Y-%m-%d").replace(tzinfo=tz).date()
    now = datetime.now(tz).date()
    days_since_start = (now - start_date).days
    if days_since_start < 0: days_since_start = 0
    cycles_passed = days_since_start // 28
    current_cycle_start = start_date + timedelta(days=cycles_passed * 28)
    
    w_start = current_cycle_start + timedelta(days=(w - 1) * 7)
    w_end = w_start + timedelta(days=4) # Пн-Пт
    return f"{w_start.strftime('%d.%m')}-{w_end.strftime('%d.%m')}"

def is_holiday(html):
    if not html: return True
    if "Розкладу немає" in html: return True
    groups = get_all_groups(html)
    if not groups: return True
    return False

def is_lesson_this_week(l_w, t_w):
    if not l_w: return True
    if '-' in l_w:
        s, e = map(int, l_w.split('-'))
        return s <= t_w <= e
    elif ',' in l_w:
        return t_w in list(map(int, l_w.split(',')))
    return t_w == int(l_w)

# --- ПАРСИНГ ---
def fetch_html():
    h = {"User-Agent": "Mozilla/5.0"}
    try:
        r = requests.get(URL, headers=h, timeout=10)
        r.encoding = 'utf-8'
        return r.text if r.status_code == 200 else None
    except: return None

def get_all_groups(html):
    if not html: return []
    return sorted(list(set(re.findall(r'"([А-ЯІЄЇA-Z0-9\-\(\)\| ]+)":\{', html))))

def parse_group_schedule(html, g_n):
    times = {}
    time_match = re.search(r'lessonTimes=\{([^}]+)\}', html)
    if time_match:
        time_data = time_match.group(1)
        for match in re.finditer(r'(\d+):`(.*?)`', time_data):
            times[match.group(1)] = match.group(2)

    m = f'"{g_n}":{{'
    s_i = html.find(m)
    if s_i == -1: return {}
    d = html[s_i + len(m):]
    e_m = re.search(r',"[А-ЯІЄЇA-Z0-9\-\(\)\| ]+":\{', d)
    g_d = d[:e_m.start()] if e_m else d
    res = {}
    p = r'"?(понеділок|вівторок|середа|четвер|п\'ятниця|субота)"?:\[(.*?)\]'
    for d_n, l_r in re.findall(p, g_d, re.I):
        d_n = d_n.lower().replace('"', '')
        res[d_n] = []
        for lr in re.findall(r'\{(.*?)\}', l_r):
            cb = re.search(r'cabinet:`(.*?)`', lr); nm = re.search(r'number:`(.*?)`', lr)
            sj = re.search(r'subject:`(.*?)`', lr); tc = re.search(r'teacher:`(.*?)`', lr)
            wk = re.search(r'week:`(.*?)`', lr)
            subj_val = sj.group(1).strip() if sj else ""
            if subj_val:
                n_v = nm.group(1).strip() if nm else "0"
                res[d_n].append({
                    'number': n_v, 'time': times.get(n_v, ""),
                    'subject': subj_val, 'teacher': tc.group(1).strip() if tc else "",
                    'room': cb.group(1).strip() if cb else "", 'week': wk.group(1).strip() if wk else ""
                })
    return res

# --- КЛАВІАТУРИ ---
def kb_main_menu():
    b = ReplyKeyboardBuilder()
    b.button(text="📅 Розклад")
    b.button(text="📓 Мої нотатки")
    b.adjust(2)
    return b.as_markup(resize_keyboard=True)

def kb_groups(grps):
    b = ReplyKeyboardBuilder()
    for g in grps: b.button(text=g)
    b.adjust(3)
    return b.as_markup(resize_keyboard=True)

async def kb_sch(s_d="none", t_w=1):
    b = InlineKeyboardBuilder()
    days = ["Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця"]
    for d in days:
        m = "✅ " if d.lower() == s_d.lower() else ""
        b.button(text=f"{m}{d}", callback_data=f"day_{d.lower()}_{t_w}")
        
    b.button(text="🗓️ ОБЕРІТЬ ТИЖДЕНЬ 🗓️", callback_data="ignore")
    
    # Кнопки тижнів з датами
    for w in range(1, 5):
        m = "✅ " if w == t_w else ""
        dates = await get_week_dates(w)
        short_date = dates.split('-')[0]
        b.button(text=f"{m}{w} ({short_date})", callback_data=f"week_{s_d.lower()}_{w}")
    b.button(text="🔙 Змінити групу", callback_data="change_group")
    b.adjust(3, 2, 1, 4, 1)
    return b.as_markup()

# --- ОБРОБНИКИ (ВАЖЛИВИЙ ПОРЯДОК!) ---

import hashlib

async def show_folders(uid: int, message_to_edit: Message = None, answer_func: Callable = None):
    if notes_collection is None:
        if answer_func: await answer_func("База даних недоступна.")
        return
        
    gn = "Не обрано"
    if users_collection is not None:
        u = await users_collection.find_one({"user_id": uid})
        if u and u.get("group"): gn = u.get("group")
        
    cursor = notes_collection.find({"user_id": uid}).sort("date", 1)
    notes = await cursor.to_list(length=None)
    
    if not notes:
        res_text = f"📓 <b>Твій записник (група {html.escape(gn)}):</b>\n\nУ тебе ще немає записів."
        b = InlineKeyboardBuilder()
        b.button(text="➕ Додати запис", callback_data="add_note_prompt")
        b.button(text="🔙 Закрити", callback_data="back_to_main")
        b.adjust(1)
        if message_to_edit: await message_to_edit.edit_text(res_text, reply_markup=b.as_markup(), parse_mode="HTML")
        elif answer_func: await answer_func(res_text, reply_markup=b.as_markup(), parse_mode="HTML")
        return
        
    subjects = {}
    for n in notes:
        text = n.get("text", "")
        subj = "Загальні"
        if text.startswith("[") and "]" in text:
            subj = text[1:text.find("]")]
        subjects[subj] = subjects.get(subj, 0) + 1
        
    res_text = f"📓 <b>Твій записник (група {html.escape(gn)}):</b>\n\nОбери папку з нотатками:"
    
    b = InlineKeyboardBuilder()
    for subj, count in subjects.items():
        h = hashlib.md5(subj.encode()).hexdigest()[:8]
        b.button(text=f"📁 {subj} ({count})", callback_data=f"v_subj_{h}")
        
    b.button(text="📄 Усі нотатки", callback_data="v_subj_all")
    b.button(text="➕ Додати запис", callback_data="add_note_prompt")
    b.button(text="🔙 Закрити", callback_data="back_to_main")
    b.adjust(1)
    
    if message_to_edit:
        await message_to_edit.edit_text(res_text, reply_markup=b.as_markup(), parse_mode="HTML")
    elif answer_func:
        await answer_func(res_text, reply_markup=b.as_markup(), parse_mode="HTML")

async def show_notes(uid: int, message_to_edit: Message = None, answer_func: Callable = None, filter_subj: str = None):
    if notes_collection is None:
        if answer_func: await answer_func("База даних недоступна.")
        return
        
    gn = "Не обрано"
    if users_collection is not None:
        u = await users_collection.find_one({"user_id": uid})
        if u and u.get("group"): gn = u.get("group")
        
    cursor = notes_collection.find({"user_id": uid}).sort("date", 1)
    notes = await cursor.to_list(length=None)
    
    filtered_notes = []
    for n in notes:
        text = n.get("text", "")
        subj = "Загальні"
        if text.startswith("[") and "]" in text:
            subj = text[1:text.find("]")]
        
        if filter_subj == "all" or filter_subj is None:
            filtered_notes.append(n)
        elif filter_subj == subj:
            filtered_notes.append(n)
            
    if not filtered_notes and filter_subj and filter_subj != "all":
        return await show_folders(uid, message_to_edit, answer_func)

    folder_name = "Усі нотатки" if (filter_subj == "all" or filter_subj is None) else filter_subj
    res_text = f"📓 <b>{html.escape(folder_name)} (група {html.escape(gn)}):</b>\n\n"
    
    if not filtered_notes:
        res_text += "У тебе ще немає записів."
    else:
        for i, n in enumerate(filtered_notes, 1):
            text = html.escape(n['text'])
            n_group = n.get("group", gn)
            
            prefix = ""
            if n_group and n_group != gn:
                prefix = f"<b>[{html.escape(n_group)}]</b>"
                
            if text.startswith("[") and "]" in text:
                idx = text.find("]")
                subj_part = text[:idx+1]
                rest_part = text[idx+1:]
                text = f"<b>{subj_part}</b>{rest_part}"
            res_text += f"<b>№{i}.</b> {prefix}{text}\n\n"
            
    b = InlineKeyboardBuilder()
    for i, n in enumerate(filtered_notes, 1):
        h = "all"
        if filter_subj and filter_subj != "all":
            h = hashlib.md5(filter_subj.encode()).hexdigest()[:8]
        b.button(text=f"✏️ Редагувати №{i}", callback_data=f"edit_note_{n['_id']}_{h}")
        b.button(text=f"❌ Видалити №{i}", callback_data=f"del_note_{n['_id']}_{h}")
        
    b.button(text="➕ Додати запис", callback_data="add_note_prompt")
    b.button(text="🔙 Назад", callback_data="back_to_folders")
    
    sizes = [2] * len(filtered_notes) + [1, 1]
    b.adjust(*sizes)
    
    if message_to_edit:
        await message_to_edit.edit_text(res_text, reply_markup=b.as_markup(), parse_mode="HTML")
    elif answer_func:
        await answer_func(res_text, reply_markup=b.as_markup(), parse_mode="HTML")

@dp.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    h = fetch_html(); gr = get_all_groups(h)
    if not gr: return await m.answer("Помилка сайту")
    
    if users_collection is not None:
        u = await users_collection.find_one({"user_id": m.from_user.id})
        if u and u.get("group"):
            return await m.answer("Головне меню:", reply_markup=kb_main_menu())
            
    await m.answer("Обери групу:", reply_markup=kb_groups(gr))

@dp.message(Command("users"))
async def get_users_stat(m: Message):
    if m.from_user.id != ADMIN_ID: return
    if users_collection is None: return await m.answer("База даних не підключена.")
    
    pipeline = [{"$group": {"_id": "$group", "count": {"$sum": 1}}}, {"$sort": {"count": -1}}]
    cursor = users_collection.aggregate(pipeline)
    stats = await cursor.to_list(length=100)
    total_users = await users_collection.count_documents({})
    
    res = f"📊 Всього користувачів: {total_users}\n\n"
    for stat in stats:
        res += f"🔹 {stat['_id'] or 'Не обрано'}: {stat['count']}\n"
    await m.answer(res)

@dp.message(Command("set_start"))
async def set_start_cmd(m: Message):
    if m.from_user.id != ADMIN_ID: return
    parts = m.text.split()
    if len(parts) < 2:
        return await m.answer("Формат: /set_start DD.MM.YYYY")
    try:
        new_start = datetime.strptime(parts[1], "%d.%m.%Y")
        if state_collection is not None:
            await state_collection.update_one(
                {"_id": "global_state"}, 
                {"$set": {"SEMESTER_START_DATE": new_start.strftime("%Y-%m-%d"), "consecutive_empty_days": 0}},
                upsert=True
            )
        await m.answer(f"✅ Дата початку семестру встановлена: {new_start.strftime('%d.%m.%Y')}")
    except ValueError:
        await m.answer("❌ Неправильний формат дати. Використовуйте DD.MM.YYYY")

@dp.message(StateFilter(NoteStates.waiting_for_note_text), F.text)
async def save_note_text(m: Message, state: FSMContext):
    uid = m.from_user.id
    gn = None
    if users_collection is not None:
        u = await users_collection.find_one({"user_id": uid})
        if u: gn = u.get("group")
        
    await state.update_data(note_text=m.text.strip(), note_group=gn)
    
    if not gn or notes_collection is None:
        if notes_collection is not None:
            await notes_collection.insert_one({
                "user_id": uid,
                "text": m.text.strip(),
                "date": datetime.now(ZoneInfo("Europe/Kiev")),
                "group": gn
            })
            await m.answer("✅ Запис збережено!")
            await show_notes(uid, answer_func=m.answer)
        await state.clear()
        return

    await state.set_state(NoteStates.waiting_for_subject_selection)
    h = fetch_html()
    sc = parse_group_schedule(h, gn)
    
    tz = ZoneInfo("Europe/Kiev")
    now = datetime.now(tz)
    days_map = {0: "понеділок", 1: "вівторок", 2: "середа", 3: "четвер", 4: "п'ятниця", 5: "субота", 6: "неділя"}
    target_day_name = days_map[now.weekday()]
    
    today_subjects = []
    if target_day_name in sc:
        for i in sc[target_day_name]:
            s = i['subject']
            if s and s not in today_subjects:
                today_subjects.append(s)
                
    all_subjects = []
    for d, lessons in sc.items():
        for i in lessons:
            s = i['subject']
            if s and s not in all_subjects and s not in today_subjects:
                all_subjects.append(s)
                
    subjects_dict = {}
    for idx, s in enumerate(today_subjects):
        subjects_dict[f"subj_t_{idx}"] = s
    for idx, s in enumerate(all_subjects):
        subjects_dict[f"subj_a_{idx}"] = s
        
    b = InlineKeyboardBuilder()
    b.button(text="📌 Без предмета", callback_data="subj_none")
    b.button(text="📅 Сьогодні", callback_data="subj_show_today")
    b.button(text="❌ Скасувати", callback_data="cancel_note")
    b.adjust(1)
    
    await state.update_data(subjects_dict=subjects_dict, today_subjects=today_subjects)
    await m.answer("Обери предмет для нотатки:\n\nАбо просто напиши назву чи абревіатуру предмета (наприклад: ОПЗ, Укр літ), і я його знайду!", reply_markup=b.as_markup())

@dp.callback_query(StateFilter(NoteStates.waiting_for_subject_selection), F.data == "subj_show_today")
async def show_today_subjects_cb(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    today_subjects = data.get("today_subjects", [])
    subjects_dict = data.get("subjects_dict", {})
    
    b = InlineKeyboardBuilder()
    if not today_subjects:
        b.button(text="Сьогодні немає пар 😎", callback_data="ignore")
    else:
        for s in today_subjects:
            cb = [k for k,v in subjects_dict.items() if v == s][0]
            b.button(text=f"📅 {s}", callback_data=cb)
            
    b.button(text="🔙 Назад", callback_data="subj_back")
    b.adjust(1)
    await c.message.edit_text("Ось пари на сьогодні:", reply_markup=b.as_markup())
    await c.answer()

@dp.callback_query(StateFilter(NoteStates.waiting_for_subject_selection), F.data == "subj_back")
async def back_to_subject_selection_cb(c: CallbackQuery, state: FSMContext):
    b = InlineKeyboardBuilder()
    b.button(text="📌 Без предмета", callback_data="subj_none")
    b.button(text="📅 Сьогодні", callback_data="subj_show_today")
    b.button(text="❌ Скасувати", callback_data="cancel_note")
    b.adjust(1)
    await c.message.edit_text("Обери предмет для нотатки:\n\nАбо просто напиши назву чи абревіатуру предмета (наприклад: ОПЗ, Укр літ), і я його знайду!", reply_markup=b.as_markup())
    await c.answer()

@dp.callback_query(StateFilter(NoteStates.waiting_for_subject_selection), F.data.startswith("subj_") & (F.data != "subj_show_today") & (F.data != "subj_back"))
async def subject_selection_cb(c: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    note_text = data.get("note_text", "")
    subjects_dict = data.get("subjects_dict", {})
    gn = data.get("note_group", "")
    
    if c.data == "subj_none":
        final_text = note_text
    else:
        subject_name = subjects_dict.get(c.data, "Невідомий предмет")
        final_text = f"[{subject_name}] {note_text}"
        
    if notes_collection is not None:
        await notes_collection.insert_one({
            "user_id": c.from_user.id,
            "text": final_text,
            "date": datetime.now(ZoneInfo("Europe/Kiev")),
            "group": gn
        })
        await c.message.delete()
        await c.message.answer("✅ Запис додано!")
        await show_notes(c.from_user.id, answer_func=c.message.answer)
        
    await state.clear()
    await c.answer()

@dp.message(StateFilter(NoteStates.waiting_for_subject_selection), F.text)
async def smart_search_subject(m: Message, state: FSMContext):
    data = await state.get_data()
    subjects_dict = data.get("subjects_dict", {})
    all_subjects = list(set(subjects_dict.values()))
    
    query = m.text.strip().lower()
    
    matched = []
    for s in all_subjects:
        q = query
        subj_lower = s.lower()
        if q in subj_lower:
            matched.append(s)
            continue
            
        words = re.findall(r"[а-яієїґa-z0-9']+", subj_lower)
        acronym = "".join(w[0] for w in words if w)
        if q == acronym:
            matched.append(s)
            continue
            
        q_words = re.findall(r"[а-яієїґa-z0-9']+", q)
        if q_words:
            s_idx = 0
            match = True
            for qw in q_words:
                found = False
                while s_idx < len(words):
                    if words[s_idx].startswith(qw):
                        found = True
                        s_idx += 1
                        break
                    s_idx += 1
                if not found:
                    match = False
                    break
            if match:
                matched.append(s)
    
    if len(matched) == 1:
        note_text = data.get("note_text", "")
        gn = data.get("note_group", "")
        final_text = f"[{matched[0]}] {note_text}"
        
        if notes_collection is not None:
            await notes_collection.insert_one({
                "user_id": m.from_user.id,
                "text": final_text,
                "date": datetime.now(ZoneInfo("Europe/Kiev")),
                "group": gn
            })
            await m.answer(f"✅ Знайдено: {matched[0]}\nЗапис додано!")
            await show_notes(m.from_user.id, answer_func=m.answer)
        await state.clear()
        
    elif len(matched) > 1:
        b = InlineKeyboardBuilder()
        for s in matched:
            cb = [k for k,v in subjects_dict.items() if v == s][0]
            b.button(text=s, callback_data=cb)
        b.button(text="❌ Скасувати", callback_data="cancel_note")
        b.adjust(1)
        await m.answer("Знайдено декілька предметів. Обери потрібний:", reply_markup=b.as_markup())
        
    else:
        b = InlineKeyboardBuilder()
        b.button(text="📌 Зберегти без предмета", callback_data="subj_none")
        b.button(text="❌ Скасувати", callback_data="cancel_note")
        b.adjust(1)
        await m.answer("Нічого не знайдено! Спробуй ще раз або збережи як загальну нотатку:", reply_markup=b.as_markup())

@dp.message(StateFilter(None), F.text)
async def handle_text(m: Message):
    text = m.text.strip()
    
    if text == "📅 Розклад":
        uid = m.from_user.id; gn = None
        if users_collection is not None:
            u = await users_collection.find_one({"user_id": uid})
            if u: gn = u.get("group")
            
        if not gn:
            h = fetch_html(); gr = get_all_groups(h)
            return await m.answer("Спочатку обери групу:", reply_markup=kb_groups(gr))
            
        cw = await get_current_week()
        markup = await kb_sch("none", cw)
        return await m.answer(f"✅ Група: {gn}\n🔥 Зараз: {cw}-й тиждень", reply_markup=markup)
        
    elif text == "📓 Мої нотатки":
        await show_folders(m.from_user.id, answer_func=m.answer)
        
    else:
        h = fetch_html(); grps = get_all_groups(h)
        if text in grps:
            if users_collection is not None:
                await users_collection.update_one({"user_id": m.from_user.id}, {"$set": {"group": text}}, upsert=True)
            cw = await get_current_week()
            markup = await kb_sch("none", cw)
            await m.answer("✅ Групу збережено!", reply_markup=kb_main_menu())
            return await m.answer(f"✅ Група: {text}\n🔥 Зараз: {cw}-й тиждень", reply_markup=markup)
            
        await m.answer("Використовуй меню нижче для навігації", reply_markup=kb_main_menu())

@dp.callback_query(F.data == "change_group")
async def change(c: CallbackQuery):
    h = fetch_html(); gr = get_all_groups(h)
    await c.message.delete()
    await c.message.answer("Обери групу:", reply_markup=kb_groups(gr))
    await c.answer()

@dp.callback_query(F.data.startswith("day_") | F.data.startswith("week_"))
async def handle_sch(c: CallbackQuery):
    uid = c.from_user.id; gn = None
    if users_collection is not None:
        u = await users_collection.find_one({"user_id": uid})
        if u: gn = u.get("group")
    
    if not gn: return await c.answer("Натисни /start", show_alert=True)
    _, sd, tw = c.data.split("_"); tw = int(tw); cw = await get_current_week()
    
    if sd == "none":
        markup = await kb_sch("none", tw)
        return await c.message.edit_text(f"🎓 Група: {gn}\n🔥 Зараз: {cw}-й тиждень\n📅 Обрано: {tw}-й тиждень", reply_markup=markup)
    
    h = fetch_html(); sc = parse_group_schedule(h, gn)
    res_t = f"🎓 {gn}\n📅 {sd.capitalize()} ({tw}-й тиждень)\n---\n"
    found = False
    if sd in sc:
        for i in sorted(sc[sd], key=lambda x: int(x['number'])):
            if is_lesson_this_week(i['week'], tw):
                found = True
                res_t += f"⏰ {i['time']} (№{i['number']})\n📘 {i['subject']}\n👨‍🏫 {i['teacher']}\n🚪 Ауд. {i['room']}\n---\n"
    if not found: res_t += "Пар немає 😎"
    markup = await kb_sch(sd, tw)
    await c.message.edit_text(res_t, reply_markup=markup, parse_mode="Markdown")
    await c.answer()

@dp.callback_query(F.data == "ignore")
async def ignore_cb(c: CallbackQuery):
    await c.answer()

@dp.callback_query(F.data == "back_to_main")
async def back_to_main_cb(c: CallbackQuery):
    await c.message.delete()
    await c.answer()

@dp.callback_query(F.data.startswith("v_subj_"))
async def view_subject_cb(c: CallbackQuery):
    h = c.data[7:]
    if h == "all":
        await show_notes(c.from_user.id, message_to_edit=c.message, filter_subj="all")
    else:
        if notes_collection is not None:
            cursor = notes_collection.find({"user_id": c.from_user.id})
            notes = await cursor.to_list(length=None)
            target_subj = None
            import hashlib
            for n in notes:
                text = n.get("text", "")
                subj = "Загальні"
                if text.startswith("[") and "]" in text:
                    subj = text[1:text.find("]")]
                if hashlib.md5(subj.encode()).hexdigest()[:8] == h:
                    target_subj = subj
                    break
            if target_subj:
                await show_notes(c.from_user.id, message_to_edit=c.message, filter_subj=target_subj)
            else:
                await show_folders(c.from_user.id, message_to_edit=c.message)
    await c.answer()

@dp.callback_query(F.data == "back_to_folders")
async def back_to_folders_cb(c: CallbackQuery):
    await show_folders(c.from_user.id, message_to_edit=c.message)
    await c.answer()

@dp.callback_query(F.data.startswith("del_note_"))
async def del_note_cb(c: CallbackQuery):
    parts = c.data.split("_")
    note_id = parts[2]
    h = parts[3] if len(parts) > 3 else "all"
    
    if notes_collection is not None:
        await notes_collection.delete_one({"_id": ObjectId(note_id), "user_id": c.from_user.id})
        
        if h == "all":
            await show_notes(c.from_user.id, message_to_edit=c.message, filter_subj="all")
        else:
            cursor = notes_collection.find({"user_id": c.from_user.id})
            notes = await cursor.to_list(length=None)
            target_subj = "all"
            import hashlib
            for n in notes:
                text = n.get("text", "")
                subj = "Загальні"
                if text.startswith("[") and "]" in text:
                    subj = text[1:text.find("]")]
                if hashlib.md5(subj.encode()).hexdigest()[:8] == h:
                    target_subj = subj
                    break
            await show_notes(c.from_user.id, message_to_edit=c.message, filter_subj=target_subj)
            
    await c.answer("Видалено!")

@dp.callback_query(F.data == "add_note_prompt")
async def add_note_prompt(c: CallbackQuery, state: FSMContext):
    await state.set_state(NoteStates.waiting_for_note_text)
    b = InlineKeyboardBuilder()
    b.button(text="❌ Скасувати", callback_data="cancel_note")
    await c.message.answer("Введіть текст вашої нотатки:", reply_markup=b.as_markup())
    await c.answer()

@dp.callback_query(F.data == "cancel_note")
async def cancel_note_cb(c: CallbackQuery, state: FSMContext):
    await state.clear()
    await c.message.delete()
    await c.message.answer("Дію скасовано.")
    await c.answer()

@dp.callback_query(F.data.startswith("edit_note_"))
async def edit_note_cb(c: CallbackQuery, state: FSMContext):
    parts = c.data.split("_")
    note_id = parts[2]
    h = parts[3] if len(parts) > 3 else "all"
    
    if notes_collection is not None:
        note = await notes_collection.find_one({"_id": ObjectId(note_id), "user_id": c.from_user.id})
        if note:
            await state.set_state(NoteStates.waiting_for_edit_text)
            await state.update_data(edit_note_id=note_id, edit_note_hash=h)
            
            b = InlineKeyboardBuilder()
            b.button(text="❌ Скасувати", callback_data="cancel_note")
            await c.message.answer("Надішліть новий текст нотатки. Щоб не писати з нуля, натисніть на старий текст нижче, щоб скопіювати його в буфер обміну:", reply_markup=b.as_markup())
            await c.message.answer(f"<code>{html.escape(note['text'])}</code>", parse_mode="HTML")
    await c.answer()

@dp.message(StateFilter(NoteStates.waiting_for_edit_text), F.text)
async def save_edited_note(m: Message, state: FSMContext):
    data = await state.get_data()
    note_id = data.get("edit_note_id")
    h = data.get("edit_note_hash", "all")
    
    if note_id and notes_collection is not None:
        await notes_collection.update_one(
            {"_id": ObjectId(note_id), "user_id": m.from_user.id},
            {"$set": {"text": m.text.strip()}}
        )
        await m.answer("✅ Нотатку успішно оновлено")
        
        if h == "all":
            await show_notes(m.from_user.id, answer_func=m.answer, filter_subj="all")
        else:
            cursor = notes_collection.find({"user_id": m.from_user.id})
            notes = await cursor.to_list(length=None)
            target_subj = "all"
            import hashlib
            for n in notes:
                text = n.get("text", "")
                subj = "Загальні"
                if text.startswith("[") and "]" in text:
                    subj = text[1:text.find("]")]
                if hashlib.md5(subj.encode()).hexdigest()[:8] == h:
                    target_subj = subj
                    break
            await show_notes(m.from_user.id, answer_func=m.answer, filter_subj=target_subj)
            
    await state.clear()

# --- АВТОРОЗСИЛКА ---
async def send_daily_schedule():
    print("🕒 [SCHEDULER] Розсилка запущена!")
    if users_collection is None: return
    tz = ZoneInfo("Europe/Kiev"); now = datetime.now(tz)
    
    h = fetch_html()
    if state_collection is not None:
        state = await state_collection.find_one({"_id": "global_state"}) or {"consecutive_empty_days": 0}
        empty_days = state.get("consecutive_empty_days", 0)
        
        if is_holiday(h):
            empty_days += 1
            await state_collection.update_one({"_id": "global_state"}, {"$set": {"consecutive_empty_days": empty_days}}, upsert=True)
            print(f"🏖️ [HOLIDAY MODE] Розкладу немає вже {empty_days} дн. Розсилка скасована.")
            return
            
        if empty_days > 14:
            await state_collection.update_one({"_id": "global_state"}, {
                "$set": {
                    "consecutive_empty_days": 0,
                    "SEMESTER_START_DATE": now.strftime("%Y-%m-%d")
                }
            }, upsert=True)
            print("🚀 [SEMESTER START] Виявлено новий розклад! Початок 1-го тижня.")
        elif empty_days > 0:
            await state_collection.update_one({"_id": "global_state"}, {"$set": {"consecutive_empty_days": 0}}, upsert=True)
    
    target_date = now # Розсилка о 00:00 на поточний день
    days_map = {0: "понеділок", 1: "вівторок", 2: "середа", 3: "четвер", 4: "п'ятниця"}
    if target_date.weekday() > 4: return # Пропуск вихідних
    
    target_day_name = days_map[target_date.weekday()]; target_week = await get_current_week()
    cursor = users_collection.find({})
    users = await cursor.to_list(length=None)
    
    for u in users:
        uid = u["user_id"]; gn = u.get("group")
        if not gn: continue
        sc = parse_group_schedule(h, gn)
        found = False; res_t = f"🔔 Авторозсилка\n📅 {target_day_name.capitalize()} ({target_week}-й тиждень)\n\n"
        if target_day_name in sc:
            for i in sorted(sc[target_day_name], key=lambda x: int(x['number'])):
                if is_lesson_this_week(i['week'], target_week):
                    found = True
                    res_t += f"⏰ {i['time']} (№{i['number']})\n📘 {i['subject']}\n👨‍🏫 {i['teacher']}\n🚪 Ауд. {i['room']}\n---\n"
        if found:
            try: await bot.send_message(uid, res_t, parse_mode="Markdown")
            except: pass
        await asyncio.sleep(0.05)

async def handle_web(request): return web.Response(text="Bot is running")

async def main():
    scheduler = AsyncIOScheduler(timezone=ZoneInfo("Europe/Kiev"))
    scheduler.add_job(send_daily_schedule, trigger=CronTrigger(hour=0, minute=0, timezone=timezone('Europe/Kyiv')))
    scheduler.start()
    app = web.Application(); app.router.add_get("/", handle_web)
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", 8080)))
    await site.start(); await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())