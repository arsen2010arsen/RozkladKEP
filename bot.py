import asyncio
import requests
import re
import time
import os
from datetime import datetime, timedelta
from typing import Callable, Dict, Any, Awaitable
from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.types import Message, CallbackQuery, ReplyKeyboardRemove
from aiogram.filters import CommandStart, Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiohttp import web
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from zoneinfo import ZoneInfo
from bson.objectid import ObjectId

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

ADMIN_ID = int(os.getenv("ADMIN_ID", 123456789))

LESSON_TIMES = {
    "1": "8:00 - 9:00", "2": "9:10 - 10:10", "3": "10:30 - 11:30",
    "4": "11:40 - 12:40", "5": "12:50 - 13:50", "6": "14:00 - 15:00",
    "7": "15:10 - 16:10", "8": "16:20 - 17:20", "9": "17:30 - 18:30"
}

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
def get_current_week():
    start = datetime(2026, 3, 9)
    now = datetime.now()
    diff = (now - start).days
    return ((diff // 7) % 4) + 1

def get_week_dates(w):
    start_date = datetime(2026, 3, 9)
    now = datetime.now()
    # Знаходимо початок поточного 4-тижневого циклу (28 днів)
    days_since_start = (now - start_date).days
    cycles_passed = days_since_start // 28
    current_cycle_start = start_date + timedelta(days=cycles_passed * 28)
    
    w_start = current_cycle_start + timedelta(days=(w - 1) * 7)
    w_end = w_start + timedelta(days=4) # Пн-Пт
    return f"{w_start.strftime('%d.%m')}-{w_end.strftime('%d.%m')}"

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
                    'number': n_v, 'time': LESSON_TIMES.get(n_v, ""),
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

def kb_notes_list(notes):
    b = InlineKeyboardBuilder()
    for i, n in enumerate(notes, 1):
        b.button(text=f"❌ Видалити №{i}", callback_data=f"del_note_{n['_id']}")
    b.button(text="➕ Додати запис", callback_data="add_note_prompt")
    b.button(text="🔙 Назад", callback_data="back_to_main")
    sizes = [2] * (len(notes) // 2) + ([1] if len(notes) % 2 != 0 else []) + [1, 1]
    b.adjust(*sizes)
    return b.as_markup()

def kb_groups(grps):
    b = ReplyKeyboardBuilder()
    for g in grps: b.button(text=g)
    b.adjust(3)
    return b.as_markup(resize_keyboard=True)

def kb_sch(s_d="none", t_w=1):
    b = InlineKeyboardBuilder()
    days = ["Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця"]
    for d in days:
        m = "✅ " if d.lower() == s_d.lower() else ""
        b.button(text=f"{m}{d}", callback_data=f"day_{d.lower()}_{t_w}")
        
    b.button(text="─── Тижні ───", callback_data="ignore")
    
    # Кнопки тижнів з датами
    for w in range(1, 5):
        m = "✅ " if w == t_w else ""
        dates = get_week_dates(w)
        b.button(text=f"{m}{w}-й ({dates})", callback_data=f"week_{s_d.lower()}_{w}")
    b.button(text="🔙 Змінити групу", callback_data="change_group")
    b.adjust(2, 2, 1, 1, 2, 2, 1)
    return b.as_markup()

# --- ОБРОБНИКИ (ВАЖЛИВИЙ ПОРЯДОК!) ---

async def show_notes(uid: int, message_to_edit: Message = None, answer_func: Callable = None):
    if notes_collection is None:
        if answer_func: await answer_func("База даних недоступна.")
        return
        
    gn = "Не обрано"
    if users_collection is not None:
        u = await users_collection.find_one({"user_id": uid})
        if u and u.get("group"): gn = u.get("group")
        
    cursor = notes_collection.find({"user_id": uid}).sort("date", 1)
    notes = await cursor.to_list(length=None)
    
    res_text = f"📓 Твій записник (група {gn}):\n\n"
    if not notes:
        res_text += "У тебе ще немає записів."
    else:
        for i, n in enumerate(notes, 1):
            res_text += f"№{i}. {n['text']}\n\n"
            
    if message_to_edit:
        await message_to_edit.edit_text(res_text, reply_markup=kb_notes_list(notes))
    elif answer_func:
        await answer_func(res_text, reply_markup=kb_notes_list(notes))

@dp.message(CommandStart())
async def start(m: Message):
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

@dp.message(F.text)
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
            
        cw = get_current_week()
        return await m.answer(f"✅ Група: {gn}\n🔥 Зараз: {cw}-й тиждень", reply_markup=kb_sch("none", cw))
        
    elif text == "📓 Мої нотатки":
        await show_notes(m.from_user.id, answer_func=m.answer)
        
    else:
        h = fetch_html(); grps = get_all_groups(h)
        if text in grps:
            if users_collection is not None:
                await users_collection.update_one({"user_id": m.from_user.id}, {"$set": {"group": text}}, upsert=True)
            cw = get_current_week()
            await m.answer("✅ Групу збережено!", reply_markup=kb_main_menu())
            return await m.answer(f"✅ Група: {text}\n🔥 Зараз: {cw}-й тиждень", reply_markup=kb_sch("none", cw))
            
        # Збереження нотатки
        if notes_collection is not None:
            await notes_collection.insert_one({
                "user_id": m.from_user.id,
                "text": text,
                "date": datetime.now(ZoneInfo("Europe/Kiev"))
            })
            await m.answer("✅ Запис додано!")
            await show_notes(m.from_user.id, answer_func=m.answer)

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
    _, sd, tw = c.data.split("_"); tw = int(tw); cw = get_current_week()
    
    if sd == "none":
        return await c.message.edit_text(f"🎓 Група: {gn}\n🔥 Зараз: {cw}-й тиждень\n📅 Обрано: {tw}-й тиждень", reply_markup=kb_sch("none", tw))
    
    h = fetch_html(); sc = parse_group_schedule(h, gn)
    res_t = f"🎓 {gn}\n📅 {sd.capitalize()} ({tw}-й тиждень)\n---\n"
    found = False
    if sd in sc:
        for i in sorted(sc[sd], key=lambda x: int(x['number'])):
            if is_lesson_this_week(i['week'], tw):
                found = True
                res_t += f"⏰ {i['time']} (№{i['number']})\n📘 {i['subject']}\n👨‍🏫 {i['teacher']}\n🚪 Ауд. {i['room']}\n---\n"
    if not found: res_t += "Пар немає 😎"
    await c.message.edit_text(res_t, reply_markup=kb_sch(sd, tw), parse_mode="Markdown")
    await c.answer()

@dp.callback_query(F.data == "ignore")
async def ignore_cb(c: CallbackQuery):
    await c.answer()

@dp.callback_query(F.data == "back_to_main")
async def back_to_main_cb(c: CallbackQuery):
    await c.message.delete()
    await c.answer()

@dp.callback_query(F.data.startswith("del_note_"))
async def del_note_cb(c: CallbackQuery):
    note_id = c.data[9:]
    if notes_collection is not None:
        await notes_collection.delete_one({"_id": ObjectId(note_id), "user_id": c.from_user.id})
        await show_notes(c.from_user.id, message_to_edit=c.message)
    await c.answer("Видалено!")

@dp.callback_query(F.data == "add_note_prompt")
async def add_note_prompt(c: CallbackQuery):
    await c.message.answer("Просто надішліть текст у чат, і я збережу його!")
    await c.answer()

# --- АВТОРОЗСИЛКА ---
async def send_daily_schedule():
    if users_collection is None: return
    tz = ZoneInfo("Europe/Kiev"); now = datetime.now(tz)
    target_date = now # Розсилка о 00:00 на поточний день
    days_map = {0: "понеділок", 1: "вівторок", 2: "середа", 3: "четвер", 4: "п'ятниця"}
    if target_date.weekday() > 4: return # Пропуск вихідних
    
    target_day_name = days_map[target_date.weekday()]; target_week = get_current_week()
    h = fetch_html(); cursor = users_collection.find({})
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
    scheduler.add_job(send_daily_schedule, CronTrigger(day_of_week='mon-fri', hour=0, minute=0))
    scheduler.start()
    app = web.Application(); app.router.add_get("/", handle_web)
    runner = web.AppRunner(app); await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", 8080)))
    await site.start(); await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())