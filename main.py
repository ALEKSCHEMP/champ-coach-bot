import asyncio
import logging
import os
from datetime import datetime, timedelta, date





from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, types
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest




from sqlalchemy import select, text, or_, and_, func
from sqlalchemy.orm import joinedload
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from database.models import Base, Location, Slot, Booking, User, SlotTemplate
from services.booking_service import create_booking, cancel_booking, get_slots_by_date, get_bookings_for_day, fix_legacy_booking_user_ids
from services.template_service import get_templates, create_template, delete_template, toggle_template, generate_week_slots

load_dotenv()

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
logging.info(f"ADMIN_ID={ADMIN_ID}")
DATABASE_URL = "sqlite+aiosqlite:////Users/admin/alekschamp_bot/champ.db"                                    
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not found. Put it into .env file")

engine = create_async_engine(DATABASE_URL, echo=False)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

async def ensure_columns():
    async with engine.begin() as conn:
        # Check if columns exist in 'slots' table
        columns = await conn.run_sync(
            lambda sync_conn: sync_conn.execute(text("PRAGMA table_info(slots)")).fetchall()
        )
        col_names = [c[1] for c in columns]

        if "capacity" not in col_names:
            await conn.execute(text("ALTER TABLE slots ADD COLUMN capacity INTEGER DEFAULT 1"))
        
        if "booked_count" not in col_names:
            await conn.execute(text("ALTER TABLE slots ADD COLUMN booked_count INTEGER DEFAULT 0"))

        duplicates = await conn.run_sync(
            lambda sync_conn: sync_conn.execute(
                text("SELECT location_code, start_time, COUNT(*) FROM slots GROUP BY location_code, start_time HAVING COUNT(*) > 1")
            ).fetchall()
        )
        if duplicates:
            raise RuntimeError(f"Cannot create unique index 'uq_slots_location_start' because duplicate slots exist: {duplicates}")
        
        await conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_slots_location_start ON slots(location_code, start_time)"))

        # Check if columns exist in 'bookings' table
        booking_columns = await conn.run_sync(
            lambda sync_conn: sync_conn.execute(text("PRAGMA table_info(bookings)")).fetchall()
        )
        booking_col_names = [c[1] for c in booking_columns]

        if "reminder_morning_sent" not in booking_col_names:
            await conn.execute(
                text("ALTER TABLE bookings ADD COLUMN reminder_morning_sent BOOLEAN DEFAULT 0")
            )

        if "reminder_day_sent" not in booking_col_names:
            await conn.execute(
                text("ALTER TABLE bookings ADD COLUMN reminder_day_sent BOOLEAN DEFAULT 0")
            )
        
        if "reminder_24h_sent" not in booking_col_names:
            await conn.execute(
                text("ALTER TABLE bookings ADD COLUMN reminder_24h_sent BOOLEAN DEFAULT 0")
            )

        if "client_confirmed" not in booking_col_names:
            await conn.execute(
                text("ALTER TABLE bookings ADD COLUMN client_confirmed BOOLEAN DEFAULT 0")
            )

        if "confirmation_status" not in booking_col_names:
            await conn.execute(
                text("ALTER TABLE bookings ADD COLUMN confirmation_status VARCHAR DEFAULT 'pending'")
            )
            
            
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    
    await ensure_columns()

    # Створюємо 2 локації один раз
    async with SessionLocal() as session:
        existing = (await session.execute(select(Location))).scalars().all()
        if not existing:
            session.add_all([
                Location(name="Локація Океан", address=None),
                Location(name="Локація Центр", address=None),
            ])
            await session.commit()

bot: Bot | None = None
dp = Dispatcher()

class BookingStates(StatesGroup):
    choosing_day = State()
    choosing_location = State()
    choosing_slot = State()
    confirming = State()

class AdminAddSlotStates(StatesGroup):
    choosing_location = State()
    choosing_day = State()
    choosing_time = State()
    choosing_capacity = State()
    confirming = State()

class AdminAddTemplateStates(StatesGroup):
    choosing_location = State()
    choosing_weekday = State()
    choosing_start = State()
    choosing_end = State()
    choosing_step = State()
    choosing_duration = State()
    choosing_capacity = State()
    confirming = State()

class AdminGenerateWeekStates(StatesGroup):
    choosing_week = State()

class AdminImportWeekStates(StatesGroup):
    choosing_week = State()
    confirming = State()


async def send_locations(target: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🏋️ Ocean",
            url="https://maps.app.goo.gl/tZCzeyj1gc8VsQJfA"
        )],
        [InlineKeyboardButton(
            text="🏋️ Center",
            url="https://maps.app.goo.gl/ubDhumBctmuUveA48"
        )]
    ])

    await target.answer(
        "📍 Локації тренувань\n\n"
        "🏋️ Ocean\n"
        "🏋️ Center\n\n"
        "Оберіть зал 👇",
        reply_markup=kb
    )


from aiogram.types import ErrorEvent

@dp.errors()
async def on_errors(event: ErrorEvent):
    logging.exception(f"Unhandled error: {event.exception}")
    return True




@dp.message(F.text == "🗓 Шаблони розкладу")
async def admin_templates_menu(message: Message):
    if not is_admin(message):
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Створити шаблон", callback_data="adm_tmpl_start")],
        [InlineKeyboardButton(text="📋 Список шаблонів", callback_data="adm_tmpl_list")],
        [InlineKeyboardButton(text="📥 Зберегти тиждень як шаблон", callback_data="adm_tmpl_imp_start")],
    ])
    await message.answer("🗓 Керування шаблонами розкладу:", reply_markup=kb)

@dp.message(F.text == "⚡ Згенерувати тиждень")
async def admin_generate_week_menu(message: Message):
    if not is_admin(message):
        return
    await message.answer("Обери тиждень для генерації слотів з активних шаблонів:", reply_markup=build_generate_week_kb())
    
# --- Template Management Logic ---
@dp.callback_query(F.data == "adm_tmpl_start")
async def adm_tmpl_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    await state.set_state(AdminAddTemplateStates.choosing_location)
    await callback.message.answer("Крок 1/7: Обери локацію", reply_markup=build_admin_locations_kb())
    await callback.answer()

@dp.callback_query(AdminAddTemplateStates.choosing_location, F.data.startswith("admin_add_loc:"))
async def adm_tmpl_loc(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    loc_key = callback.data.split(":", 1)[1]
    loc = LOCATIONS.get(loc_key, loc_key)
    await state.update_data(tmpl_loc=loc)
    await state.set_state(AdminAddTemplateStates.choosing_weekday)
    await callback.message.answer("Крок 2/7: Обери день тижня", reply_markup=build_admin_weekdays_kb())
    await callback.answer()

@dp.callback_query(AdminAddTemplateStates.choosing_weekday, F.data.startswith("adm_tmpl_wd:"))
async def adm_tmpl_wd(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    wd = int(callback.data.split(":")[1]) # 0-6
    await state.update_data(tmpl_wd=wd)
    await state.set_state(AdminAddTemplateStates.choosing_start)
    await callback.message.answer("Крок 3/7: Обери час ПОЧАТКУ вікна", reply_markup=build_admin_times_kb())
    await callback.answer()

@dp.callback_query(AdminAddTemplateStates.choosing_start, F.data.startswith("admin_add_time:"))
async def adm_tmpl_start_time(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    time_str = callback.data.split(":")[1] + ":" + callback.data.split(":")[2]
    await state.update_data(tmpl_start=time_str)
    await state.set_state(AdminAddTemplateStates.choosing_end)
    await callback.message.answer("Крок 4/7: Обери час КІНЦЯ вікна", reply_markup=build_admin_times_kb())
    await callback.answer()

@dp.callback_query(AdminAddTemplateStates.choosing_end, F.data.startswith("admin_add_time:"))
async def adm_tmpl_end_time(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    time_str = callback.data.split(":")[1] + ":" + callback.data.split(":")[2]
    data = await state.get_data()
    # Simple validation
    if time_str <= data.get("tmpl_start"):
        await callback.message.answer("Час кінця не може бути раніше або рівним початку! Спробуй ще раз.", reply_markup=build_admin_times_kb())
        await callback.answer()
        return
    await state.update_data(tmpl_end=time_str)
    await state.set_state(AdminAddTemplateStates.choosing_step)
    await callback.message.answer("Крок 5/7: Крок початку слотів (інтервал)", reply_markup=build_admin_tmpl_step_kb())
    await callback.answer()

@dp.callback_query(AdminAddTemplateStates.choosing_step, F.data.startswith("adm_tmpl_step:"))
async def adm_tmpl_step(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    step = int(callback.data.split(":")[1])
    await state.update_data(tmpl_step=step)
    await state.set_state(AdminAddTemplateStates.choosing_duration)
    await callback.message.answer("Крок 6/7: Тривалість одного тренування", reply_markup=build_admin_tmpl_duration_kb())
    await callback.answer()

@dp.callback_query(AdminAddTemplateStates.choosing_duration, F.data.startswith("adm_tmpl_dur:"))
async def adm_tmpl_dur(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    dur = int(callback.data.split(":")[1])
    await state.update_data(tmpl_dur=dur)
    await state.set_state(AdminAddTemplateStates.choosing_capacity)
    await callback.message.answer("Крок 7/7: Місткість слота (скільки людей)", reply_markup=build_admin_capacity_kb())
    await callback.answer()

@dp.callback_query(AdminAddTemplateStates.choosing_capacity, F.data.startswith("admin_add_cap:"))
async def adm_tmpl_cap(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    cap = int(callback.data.split(":")[1])
    await state.update_data(tmpl_cap=cap)
    
    data = await state.get_data()
    wd_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
    wd_str = wd_names[data.get("tmpl_wd")]
    
    text = (f"Підтвердьте створення шаблону:\n\n"
            f"📍 Локація: {data.get('tmpl_loc')}\n"
            f"📅 День: {wd_str}\n"
            f"⏰ Вікно: {data.get('tmpl_start')} - {data.get('tmpl_end')}\n"
            f"⏱ Інтервал: {data.get('tmpl_step')} хв\n"
            f"⏳ Тривалість: {data.get('tmpl_dur')} хв\n"
            f"👥 Місткість: {cap} осіб")
            
    await state.set_state(AdminAddTemplateStates.confirming)
    await callback.message.answer(text, reply_markup=build_admin_tmpl_confirm_kb())
    await callback.answer()

@dp.callback_query(AdminAddTemplateStates.confirming, F.data == "adm_tmpl_confirm")
async def adm_tmpl_commit(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    data = await state.get_data()
    try:
        async with SessionLocal() as session:
            await create_template(
                session,
                location_code=data.get('tmpl_loc'),
                weekday=data.get('tmpl_wd'),
                window_start=data.get('tmpl_start'),
                window_end=data.get('tmpl_end'),
                step_minutes=data.get('tmpl_step'),
                duration_minutes=data.get('tmpl_dur'),
                capacity=data.get('tmpl_cap')
            )
        await callback.message.answer("✅ Шаблон успішно створено!")
    except Exception as e:
        await callback.message.answer(f"❌ Помилка БД: {e}")
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "adm_tmpl_list")
async def adm_tmpl_list(callback: types.CallbackQuery):
    if not is_admin_user(callback.from_user): return
    wd_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
    async with SessionLocal() as session:
        templates = await get_templates(session)
        
    if not templates:
        await callback.message.answer("Шаблонів поки немає.")
        await callback.answer()
        return
        
    for t in templates:
        status_icon = "🟢" if t.is_active else "🔴"
        text = (f"ID: {t.id} | {t.location_code} | {wd_names[t.weekday]}\n"
                f"⏰ {t.window_start}-{t.window_end} | Крок {t.step_minutes} | Трив {t.duration_minutes} | Міст {t.capacity} | {status_icon}")
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Вимк/Увімк", callback_data=f"adm_tmpl_tg:{t.id}"),
             InlineKeyboardButton(text="🗑 Видалити", callback_data=f"adm_tmpl_del:{t.id}")]
        ])
        await callback.message.answer(text, reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.startswith("adm_tmpl_tg:"))
async def adm_tmpl_toggle(callback: types.CallbackQuery):
    if not is_admin_user(callback.from_user): return
    tid = int(callback.data.split(":")[1])
    async with SessionLocal() as session:
        success = await toggle_template(session, tid)
    if success:
        await callback.message.answer(f"✅ Статус шаблону {tid} змінено")
    else:
        await callback.message.answer("❌ Шаблон не знайдено")
    await callback.answer()
    
@dp.callback_query(F.data.startswith("adm_tmpl_del:"))
async def adm_tmpl_del(callback: types.CallbackQuery):
    if not is_admin_user(callback.from_user): return
    tid = int(callback.data.split(":")[1])
    async with SessionLocal() as session:
        success = await delete_template(session, tid)
    if success:
        await callback.message.answer(f"🗑 Шаблон {tid} видалено")
    else:
        await callback.message.answer("❌ Шаблон не знайдено")
    await callback.answer()

@dp.callback_query(F.data.startswith("adm_gen_week:"))
async def adm_gen_week_post(callback: types.CallbackQuery):
    if not is_admin_user(callback.from_user): return
    offset_weeks = int(callback.data.split(":")[1])
    target_date = date.today() + timedelta(weeks=offset_weeks)
    
    await callback.message.answer(f"⏳ Генерую тиждень для дати {target_date.isoformat()}...")
    try:
        async with SessionLocal() as session:
            created, skipped = await generate_week_slots(session, target_date)
        await callback.message.answer(f"✅ Тиждень успішно згенеровано!\n\nСтворено слотів: {created}\nПропущено (вже існують): {skipped}")
    except Exception as e:
        await callback.message.answer(f"❌ Помилка під час генерації: {e}")
        logging.exception(e)
    await callback.answer()

@dp.callback_query(F.data == "adm_tmpl_imp_start")
async def adm_tmpl_imp_start(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    await state.set_state(AdminImportWeekStates.choosing_week)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Цей тиждень", callback_data="adm_tmpl_imp_week:0")],
        [InlineKeyboardButton(text="Наступний тиждень", callback_data="adm_tmpl_imp_week:1")],
        [InlineKeyboardButton(text="Через 2 тижні", callback_data="adm_tmpl_imp_week:2")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")]
    ])
    await callback.message.answer("Обери тиждень, слоти якого потрібно перетворити на шаблони:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(AdminImportWeekStates.choosing_week, F.data.startswith("adm_tmpl_imp_week:"))
async def adm_tmpl_imp_calc(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    offset_weeks = int(callback.data.split(":")[1])
    target_date = date.today() + timedelta(weeks=offset_weeks)
    
    await callback.message.answer("⏳ Аналізую слоти...")
    
    from services.template_service import calculate_templates_from_week
    
    async with SessionLocal() as session:
        templates = await calculate_templates_from_week(session, target_date)
        
    if not templates:
        await callback.message.answer("❌ У вибраному тижні немає слотів. Шаблони не створено.")
        await state.clear()
        await callback.answer()
        return
        
    wd_names = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
    lines = ["<b>Знайдені шаблони:</b>"]
    
    # We need to serialize them to state to avoid refetching or recalculating if needed, but FSM state has limits
    # A list of dicts is safe enough
    serialized = []
    for t in templates:
        lines.append(f"• {t.location_code} | {wd_names[t.weekday]} | {t.window_start}-{t.window_end} | Крок {t.step_minutes} | Трив {t.duration_minutes} | Міст {t.capacity}")
        serialized.append({
            "loc": t.location_code, "wd": t.weekday, "w_start": t.window_start, "w_end": t.window_end, 
            "step": t.step_minutes, "dur": t.duration_minutes, "cap": t.capacity
        })
        
    lines.append("\nЩо робити зі знайденими шаблонами?")
    await state.update_data(import_tmpls=serialized)
    await state.set_state(AdminImportWeekStates.confirming)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Додати до існуючих", callback_data="adm_tmpl_imp_commit:add")],
        [InlineKeyboardButton(text="♻️ Замінити (перекрити)", callback_data="adm_tmpl_imp_commit:replace")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")]
    ])
    
    await callback.message.answer("\n".join(lines), reply_markup=kb, parse_mode="HTML")
    await callback.answer()

@dp.callback_query(AdminImportWeekStates.confirming, F.data.startswith("adm_tmpl_imp_commit:"))
async def adm_tmpl_imp_commit(callback: types.CallbackQuery, state: FSMContext):
    if not is_admin_user(callback.from_user): return
    mode = callback.data.split(":")[1]
    replace = (mode == "replace")
    
    data = await state.get_data()
    raw = data.get("import_tmpls", [])
    
    from database.models import SlotTemplate
    from services.template_service import save_imported_templates
    
    tmpls = [SlotTemplate(
        location_code=r["loc"], weekday=r["wd"], window_start=r["w_start"], window_end=r["w_end"],
        step_minutes=r["step"], duration_minutes=r["dur"], capacity=r["cap"], is_active=True
    ) for r in raw]
    
    try:
        async with SessionLocal() as session:
            saved_count = await save_imported_templates(session, tmpls, replace_mode=replace)
        await callback.message.answer(f"✅ Успішно збережено шаблонів: {saved_count}")
    except Exception as e:
        await callback.message.answer(f"❌ Помилка БД: {e}")
        logging.exception(e)
        
    await state.clear()
    await callback.answer()

def is_admin(message: Message) -> bool:
    return (
        ADMIN_ID != 0
        and message.from_user is not None
        and message.from_user.id == ADMIN_ID
    )
def is_admin_user(user: types.User | None) -> bool:
    return ADMIN_ID != 0 and user is not None and user.id == ADMIN_ID

def fmt_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M")

# Доступні локації (ключ — для команд, значення — для відображення)
LOCATIONS = {
    "ОКЕАН": "Океан",
    "ЦЕНТР": "Центр"
}
    
async def safe_edit_text(message: Message, text: str, reply_markup=None) -> bool:
    try:
        await message.edit_text(text, reply_markup=reply_markup)
        return True
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return False
        raise
    

async def reminder_worker(bot: Bot):
    while True:
        try:
            now = datetime.now()

            async with SessionLocal() as session:
                q = (
                    select(Booking)
                    .join(Slot, Booking.slot_id == Slot.id)
                    .options(joinedload(Booking.slot), joinedload(Booking.user))
                    .where(
                        Booking.status == "active",
                        Slot.start_time > now
                    )
                )

                bookings = (await session.execute(q)).scalars().unique().all()

                changed = False

                for b in bookings:
                    if not b.user or not b.slot:
                        continue

                    slot_time = b.slot.start_time
                    time_to_training = slot_time - now
                    
                     # 0. Напоминание за 24 часа
                    if (
                        not b.reminder_24h_sent
                        and timedelta(hours=23) <= time_to_training <= timedelta(hours=24)
                    ):
                        try:
                            confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
                                [
                                    InlineKeyboardButton(text="✅ Буду", callback_data=f"confirm_yes:{b.id}"),
                                    InlineKeyboardButton(text="❌ Не прийду", callback_data=f"confirm_no:{b.id}")
                                ],
                                [
                                    InlineKeyboardButton(text="🔁 Перенести", callback_data=f"reschedule_start:{b.id}")
                                ]
                            ])

                            await bot.send_message(
                                b.user.telegram_id,
                                f"⏰ Нагадування: тренування завтра\n"
                                f"📍 {b.location}\n"
                                f"🕒 {fmt_dt(slot_time)}\n\n"
                                f"Підтверди, будь ласка, чи будеш 👇",
                                reply_markup=confirm_kb
                            )

                            b.reminder_24h_sent = True
                            changed = True
                            logging.info(f"24h reminder sent for booking_id={b.id}")

                        except Exception as e:
                            logging.exception(
                                f"Failed to send 24h reminder for booking_id={b.id}: {e}"
                            )

                    # 1. Утреннее напоминание:
                    # если тренировка сегодня, время уже после 08:00,
                    # напоминание ещё не отправлялось, и тренировка ещё впереди
                    if (
                        not b.reminder_morning_sent
                        and slot_time.date() == now.date()
                        and 8 <= now.hour < 12
                    ):
                        try:
                            await bot.send_message(
                                b.user.telegram_id,
                                f"☀️ Нагадування про тренування сьогодні\n"
                                f"📍 {b.location}\n"
                                f"🕒 {fmt_dt(slot_time)}"
                            )
                            b.reminder_morning_sent = True
                            changed = True
                            logging.info(f"Morning reminder sent for booking_id={b.id}")
                        except Exception as e:
                            logging.exception(
                                f"Failed to send morning reminder for booking_id={b.id}: {e}"
                            )

                    # 2. Дневное напоминание за 3 часа
                    if (
                        not b.reminder_day_sent
                        and timedelta(hours=0) < time_to_training <= timedelta(hours=3)
                    ):
                        try:
                            await bot.send_message(
                                b.user.telegram_id,
                                f"🔔 Нагадування: тренування вже скоро\n"
                                f"📍 {b.location}\n"
                                f"🕒 {fmt_dt(slot_time)}\n"
                                f"Побачимось 💪"
                            )
                            b.reminder_day_sent = True
                            changed = True
                            logging.info(f"Day reminder sent for booking_id={b.id}")
                        except Exception as e:
                            logging.exception(
                                f"Failed to send day reminder for booking_id={b.id}: {e}"
                            )

                if changed:
                    await session.commit()

        except Exception as e:
            logging.exception(f"Reminder worker error: {e}")

        await asyncio.sleep(60)
   


async def build_free_slots_kb(target_day: date, location_filter: str | None) -> InlineKeyboardMarkup | None:
    start = datetime.combine(target_day, datetime.min.time())
    end = start + timedelta(days=1)
    now = datetime.now()

    async with SessionLocal() as session:
        q = (
            select(Slot)
            .where(Slot.booked_count < Slot.capacity) # ✅ Capacity check
            .where(Slot.start_time >= start, Slot.start_time < end)
            .where(Slot.start_time >= now)
            .order_by(Slot.location_code, Slot.start_time)
        )

        if location_filter and location_filter != "ALL":
            q = q.where(Slot.location_code == location_filter)

        slots = (await session.execute(q)).scalars().all()

    if not slots:
        return None

    rows = []
    row = []
    for s in slots:
        # Format: HH:MM (booked/cap)
        # If capacity > 1, show counter. else just time.
        t_str = s.start_time.strftime('%H:%M')
        if s.capacity > 1:
            btn_text = f"{t_str} ({s.booked_count}/{s.capacity})"
        else:
            btn_text = t_str

        # якщо показуємо ALL — додамо маленьку мітку локації
        if location_filter in (None, "ALL"):
            btn_text = f"{btn_text} • {s.location_code}"

        row.append(InlineKeyboardButton(text=btn_text, callback_data=f"slot:{s.id}"))

        # 2 кнопки в ряд
        if len(row) == 2:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data="back_to_locations")])
    rows.append([InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_booking")])
    return InlineKeyboardMarkup(inline_keyboard=rows)




def build_day_choice_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 Сьогодні", callback_data="day:today")],
        [InlineKeyboardButton(text="➡️ Завтра", callback_data="day:tomorrow")],
        [InlineKeyboardButton(text="📍 Субота", callback_data="day:sat")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_booking")],
    ])

def build_my_bookings_mode_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Активні записи", callback_data="my_mode:active")],
        [InlineKeyboardButton(text="📜 Історія записів", callback_data="my_mode:history")],
        [InlineKeyboardButton(text="❌ Закрити", callback_data="my_close")],
    ])



def build_admin_locations_kb() -> InlineKeyboardMarkup:
    rows = []
    for key, label in LOCATIONS.items():
        rows.append([InlineKeyboardButton(text=label, callback_data=f"admin_add_loc:{key}")])

    rows.append([InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)



def build_admin_days_kb(page: int = 0) -> InlineKeyboardMarkup:
    today = date.today()

    start_index = page * ADMIN_DAYS_PAGE_SIZE
    end_index = min(start_index + ADMIN_DAYS_PAGE_SIZE, ADMIN_DAYS_TOTAL)

    rows = []

    for i in range(start_index, end_index):
        d = today + timedelta(days=i)

        if i == 0:
            label = f"📅 Сьогодні • {d.strftime('%d.%m')}"
        elif i == 1:
            label = f"➡️ Завтра • {d.strftime('%d.%m')}"
        else:
            label = f"{d.strftime('%a')} • {d.strftime('%d.%m')}"

        rows.append([
            InlineKeyboardButton(
                text=label,
                callback_data=f"admin_add_day:{d.isoformat()}"
            )
        ])

    # --- навигация ---
    nav = []

    if page > 0:
        nav.append(
            InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"admin_daypage:{page-1}"
            )
        )

    if end_index < ADMIN_DAYS_TOTAL:
        nav.append(
            InlineKeyboardButton(
                text="➡️ Далі",
                callback_data=f"admin_daypage:{page+1}"
            )
        )

    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data="admin_add_back_loc")])
    rows.append([InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")])

    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_admin_slots_days_kb() -> InlineKeyboardMarkup:
    today = date.today()
    rows = []
    for i in range(7):
        d = today + timedelta(days=i)
        label = d.strftime("%a %d.%m")
        rows.append([InlineKeyboardButton(text=label, callback_data=f"admin_slots_day:{d.isoformat()}")])

    rows.append([InlineKeyboardButton(text="❌ Закрити", callback_data="admin_slots_close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_admin_times_kb() -> InlineKeyboardMarkup:
    rows = []
    times = []
    for hour in range(8, 22):  # 08:00..21:30
        times.append(f"{hour:02d}:00")
        times.append(f"{hour:02d}:30")

    row = []
    for t in times:
        row.append(InlineKeyboardButton(text=t, callback_data=f"admin_add_time:{t}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data="admin_add_back_day")])
    rows.append([InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_admin_capacity_kb() -> InlineKeyboardMarkup:
    # 1..5
    row1 = [InlineKeyboardButton(text=str(i), callback_data=f"admin_add_cap:{i}") for i in range(1, 6)]
    # 6..10
    row2 = [InlineKeyboardButton(text=str(i), callback_data=f"admin_add_cap:{i}") for i in range(6, 11)]
    
    rows = [row1, row2]
    rows.append([
        InlineKeyboardButton(text="↩️ Назад", callback_data="admin_add_back_time"),
        InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_admin_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Додати слот", callback_data="admin_add_confirm")],
        [InlineKeyboardButton(text="↩️ Назад", callback_data="admin_add_back_time")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")],
    ])

# --- Template Admin Keyboards ---

def build_admin_weekdays_kb() -> InlineKeyboardMarkup:
    days = ["Понеділок", "Вівторок", "Середа", "Четвер", "П'ятниця", "Субота", "Неділя"]
    rows = []
    for i, d in enumerate(days):
        rows.append([InlineKeyboardButton(text=d, callback_data=f"adm_tmpl_wd:{i}")])
    rows.append([InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_admin_tmpl_step_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="30 хв", callback_data="adm_tmpl_step:30"),
         InlineKeyboardButton(text="60 хв", callback_data="adm_tmpl_step:60")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")]
    ])

def build_admin_tmpl_duration_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="60 хв", callback_data="adm_tmpl_dur:60"),
         InlineKeyboardButton(text="90 хв", callback_data="adm_tmpl_dur:90")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")]
    ])

def build_admin_tmpl_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Створити шаблон", callback_data="adm_tmpl_confirm")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")]
    ])

def build_generate_week_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Цей тиждень", callback_data="adm_gen_week:0")],
        [InlineKeyboardButton(text="Наступний тиждень", callback_data="adm_gen_week:1")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")]
    ])




def build_main_kb(is_admin_user: bool) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text="📅 Записатися на тренування")],
        [KeyboardButton(text="👤 Профіль")],
        [KeyboardButton(text="ℹ️ Контакти тренера")],
    ]

    if is_admin_user:
        rows.append([KeyboardButton(text="🛠 Адмін-панель")])

    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def build_admin_slots_actions_kb(
    target_day_iso: str,
    slots: list,
) -> InlineKeyboardMarkup:
    rows = []

    for s in slots:
        t = s.start_time.strftime("%H:%M")
        
        # Row with 2 buttons: [Delete/Unbook] [Edit Cap]
        # But maybe just one row per slot is cleaner or 2 rows
        # Let's do:
        # [ 🟢 10:00 (1/5) 🗑 ]
        # [ ✏️ Capacity ]
        
        status_icon = "🟢" if s.booked_count < s.capacity else "🔴"
        cap_info = f"({s.booked_count}/{s.capacity})"
        
        rows.append([
            InlineKeyboardButton(
                text=f"{status_icon} {s.location_code} {t} {cap_info} 🗑",
                callback_data=f"admin_slot_del:{s.id}:{target_day_iso}"
            )
        ])
        rows.append([
            InlineKeyboardButton(
                text=f"✏️ Змінити місткість id:{s.id}",
                callback_data=f"admin_edit_cap_start:{s.id}:{target_day_iso}"
            )
        ])

    rows.append([InlineKeyboardButton(
        text="↩️ Назад до днів",
        callback_data="admin_slots_back_days"
    )])
    rows.append([InlineKeyboardButton(
        text="❌ Закрити",
        callback_data="admin_slots_close"
    )])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_admin_slots_filter_kb(target_day_iso: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="⚪ All",
                callback_data=f"admin_slots_day:{target_day_iso}:all"
            ),
            InlineKeyboardButton(
                text="🟢 Free",
                callback_data=f"admin_slots_day:{target_day_iso}:free"
            ),
            InlineKeyboardButton(
                text="🔴 Booked",
                callback_data=f"admin_slots_day:{target_day_iso}:booked"
            ),
        ]
    ])

def build_my_bookings_kb(bookings: list[Booking], mode: str = "active") -> InlineKeyboardMarkup:
    rows = []

    # у режимі active — даємо кнопки скасування
    if mode == "active":
        for b in bookings:
            rows.append([
                InlineKeyboardButton(
                    text=f"❌ Скасувати • {b.location} • {b.booking_date.strftime('%d.%m %H:%M')}",
                    callback_data=f"my_cancel:{b.id}"
                )
            ])

    # навігація між режимами
    rows.append([
        InlineKeyboardButton(text="🟢 Активні", callback_data="my_mode:active"),
        InlineKeyboardButton(text="📜 Історія", callback_data="my_mode:history"),
    ])
    rows.append([InlineKeyboardButton(text="❌ Закрити", callback_data="my_close")])

    return InlineKeyboardMarkup(inline_keyboard=rows)



CLIENT_DAYS_TOTAL = 14          # скільки днів показувати всього
CLIENT_DAYS_PAGE_SIZE = 7       # скільки кнопок на сторінці

ADMIN_DAYS_TOTAL = 60        # сколько дней вперед доступно админу
ADMIN_DAYS_PAGE_SIZE = 7     # сколько кнопок на странице

def build_client_days_kb(page: int = 0) -> InlineKeyboardMarkup:
    today = date.today()
    start_index = page * CLIENT_DAYS_PAGE_SIZE
    end_index = min(start_index + CLIENT_DAYS_PAGE_SIZE, CLIENT_DAYS_TOTAL)

    rows = []
    for i in range(start_index, end_index):
        d = today + timedelta(days=i)

        # підпис кнопки
        if i == 0:
            label = f"📅 Сьогодні • {d.strftime('%d.%m')}"
        elif i == 1:
            label = f"➡️ Завтра • {d.strftime('%d.%m')}"
        else:
            label = f"{d.strftime('%a')} • {d.strftime('%d.%m')}"

        rows.append([InlineKeyboardButton(text=label, callback_data=f"dayiso:{d.isoformat()}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"daypage:{page-1}"))
    if end_index < CLIENT_DAYS_TOTAL:
        nav.append(InlineKeyboardButton(text="➡️ Далі", callback_data=f"daypage:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_booking")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def build_client_locations_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🌊 Океан", callback_data="cloc:Океан")],
        [InlineKeyboardButton(text="🏙 Центр", callback_data="cloc:Центр")],
        [InlineKeyboardButton(text="⚪ Усі локації", callback_data="cloc:ALL")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_booking")],
    ])

def build_admin_bookings_days_kb() -> InlineKeyboardMarkup:
    today = date.today()
    rows = []
    for i in range(14):
        d = today + timedelta(days=i)
        label = d.strftime("%a %d.%m")
        rows.append([InlineKeyboardButton(text=label, callback_data=f"admin_bookings_day:{d.isoformat()}")])

    rows.append([InlineKeyboardButton(text="❌ Закрити", callback_data="admin_bookings_close")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.message(F.text == "📌 Мій запис")
async def my_bookings(message: Message):
    await message.answer("Обери що показати 👇", reply_markup=build_my_bookings_mode_kb())

@dp.message(F.text == "👤 Профіль")
async def profile_handler(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Мої записи", callback_data="profile_my_bookings")],
        [InlineKeyboardButton(text="📍 Локації тренувань", callback_data="profile_locations")]
    ])

    await message.answer(
        "👤 Ваш профіль\n\n"
        "Оберіть розділ 👇",
        reply_markup=kb
    )


@dp.callback_query(F.data == "profile_my_bookings")
async def profile_my_bookings_handler(callback: CallbackQuery):
    await show_my_bookings(
        callback.message,
        tg_user=callback.from_user
    )
    await callback.answer()

async def show_my_bookings(
    message: Message,
    *,
    tg_user,
    edit: bool = False,
    mode: str = "active"
):
    user = tg_user
    now = datetime.now()

    async with SessionLocal() as session:
        db_user = await session.scalar(
            select(User).where(User.telegram_id == user.id)
        )

        q = (
            select(Booking)
            .join(Slot, Booking.slot_id == Slot.id)
            .options(joinedload(Booking.slot), joinedload(Booking.user))
        )

        if db_user:
            q = q.where(
                or_(
                    Booking.user_id == db_user.id,
                    Booking.user_id == user.id  # legacy fallback
                )
            )
        else:
            q = q.where(Booking.user_id == user.id)

        if mode == "active":
            q = (
                q.where(and_(Booking.status == "active", Slot.start_time >= now))
                 .order_by(Slot.start_time.asc())
            )
        else:
            q = (
                q.where(or_(Booking.status != "active", Slot.start_time < now))
                 .order_by(Slot.start_time.desc())
            )

        bookings = (await session.execute(q)).scalars().all()

        logging.info(
            "MY_BOOKINGS mode=%s tg_id=%s db_user_id=%s count=%s now=%s",
            mode,
            user.id,
            getattr(db_user, "id", None),
            len(bookings),
            now
        )

    if not bookings:
        text = "Немає записів 🙂" if mode == "history" else "У тебе поки немає активних майбутніх записів 🙂"
        kb = build_my_bookings_mode_kb()

        if edit:
            changed = await safe_edit_text(message, text, reply_markup=kb)
            return changed, text, kb
        else:
            await message.answer(text, reply_markup=kb)
            return True, text, kb

    if mode == "active":
        lines = ["📌 Твої активні записи (майбутні):\n"]
        for i, b in enumerate(bookings, start=1):
            dt = b.slot.start_time
            lines.append(f"{i}) 📍 {b.location} • 🕒 {fmt_dt(dt)}")
    else:
        lines = ["📜 Історія записів:\n"]
        for i, b in enumerate(bookings, start=1):
            dt = b.slot.start_time
            status = "❌ скасовано" if b.status != "active" else "✅ було"
            lines.append(f"{i}) 📍 {b.location} • 🕒 {fmt_dt(dt)} • {status}")

    text = "\n".join(lines)
    kb = build_my_bookings_kb(bookings, mode=mode)

    if edit:
        changed = await safe_edit_text(message, text, reply_markup=kb)
        return changed, text, kb
    else:
        await message.answer(text, reply_markup=kb)
        return True, text, kb

@dp.callback_query(F.data == "profile_locations")
async def profile_locations_handler(callback: types.CallbackQuery):
    await send_locations(callback.message)
    await callback.answer()

@dp.message(F.text == "ℹ️ Контакти тренера")
async def coach_contacts_handler(message: Message):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💬 Telegram", url="https://t.me/@alekschemp")],
        [InlineKeyboardButton(text="📸 Instagram", url="https://www.instagram.com/alekschemp/")],
        [InlineKeyboardButton(text="📱 Показати номер", callback_data="show_coach_phone")],
    ])

    text = (
        "ℹ️ Контакти тренера\n\n"
        "Оберіть зручний спосіб зв’язку 👇"
    )

    await message.answer(text, reply_markup=kb)


@dp.callback_query(F.data == "show_coach_phone")
async def show_coach_phone(callback: CallbackQuery):
    await callback.message.answer(
        "📱 Номер тренера: +380635003137"
    )
    await callback.answer()








@dp.callback_query(F.data.startswith("admin_slots_day:"))
async def admin_slots_show_day(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    parts = callback.data.split(":")
    target_day_iso = parts[1]
    filter_mode = parts[2] if len(parts) >= 3 else "all"  # all/free/booked

    target_day = date.fromisoformat(target_day_iso)
    start = datetime.combine(target_day, datetime.min.time())
    end = start + timedelta(days=1)

    async with SessionLocal() as session:
        q = (
            select(Slot)
            .where(Slot.start_time >= start, Slot.start_time < end)
            .order_by(Slot.location_code, Slot.start_time)
        )
        slots_all = (await session.execute(q)).scalars().all()

    # totals by seats (capacity)
    total_capacity = sum(s.capacity for s in slots_all)
    total_booked = sum(s.booked_count for s in slots_all)
    total_free = max(0, total_capacity - total_booked)

    # apply filter for display
    if filter_mode == "all":
        slots = slots_all
    elif filter_mode == "free":  # has space
        slots = [s for s in slots_all if s.booked_count < s.capacity]
    elif filter_mode == "booked":  # has bookings (or change to "full" if you want)
        slots = [s for s in slots_all if s.booked_count > 0]
    else:
        slots = slots_all

    if not slots:
        await callback.message.answer(
            f"📅 {target_day.strftime('%d.%m.%Y')}  |  {filter_mode.upper()}\n"
            f"🧾 Cap: {total_capacity} | Booked: {total_booked} | Free: {total_free}\n\n"
            f"Немає слотів за фільтром.",
            reply_markup=build_admin_slots_filter_kb(target_day_iso)
        )
        await callback.answer()
        return

    # build text
    lines = [
        f"📅 {target_day.strftime('%d.%m.%Y')}  |  {filter_mode.upper()}",
        f"🧾 Cap: {total_capacity} | Booked: {total_booked} | Free: {total_free}",
        ""
    ]

    current_loc = None
    for s in slots:
        if current_loc != s.location_code:
            current_loc = s.location_code
            lines.append(f"\n📍 Локація {current_loc}")

        status_icon = "🟢" if s.booked_count < s.capacity else "🔴"
        lines.append(
            f"{status_icon} {s.start_time.strftime('%H:%M')} "
            f"({s.booked_count}/{s.capacity}) | id:{s.id}"
        )

    await callback.message.answer(
        "\n".join(lines),
        reply_markup=build_admin_slots_filter_kb(target_day_iso)
    )

    await callback.message.answer(
        "Дії зі слотами:",
        reply_markup=build_admin_slots_actions_kb(target_day_iso, slots)
    )

    await callback.answer()



@dp.callback_query(F.data.startswith("admin_daypage:"))
async def admin_days_page(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    page = int(callback.data.split(":")[1])

    await state.set_state(AdminAddSlotStates.choosing_day)

    await callback.message.answer(
        "Обери день:",
        reply_markup=build_admin_days_kb(page=page)
    )

    await callback.answer()
    
    
    
@dp.callback_query(F.data.startswith("admin_edit_cap_start:"))
async def admin_edit_cap_start(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    # admin_edit_cap_start:{slot_id}:{day_iso}
    parts = callback.data.split(":")
    slot_id = int(parts[1])
    day_iso = parts[2]

    # Show capacity keyboard 1..10
    rows = []
    # 1..5
    row1 = []
    for i in range(1, 6):
        row1.append(InlineKeyboardButton(
            text=str(i),
            callback_data=f"admin_edit_cap_save:{slot_id}:{day_iso}:{i}"
        ))
    rows.append(row1)
    
    # 6..10
    row2 = []
    for i in range(6, 11):
        row2.append(InlineKeyboardButton(
            text=str(i),
            callback_data=f"admin_edit_cap_save:{slot_id}:{day_iso}:{i}"
        ))
    rows.append(row2)

    rows.append([InlineKeyboardButton(
        text="❌ Скасувати",
        callback_data="admin_slots_close"
    )])

    await callback.message.answer(
        f"Обери нову місткість для слота id:{slot_id}:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_edit_cap_save:"))
async def admin_edit_cap_save(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    # admin_edit_cap_save:{slot_id}:{day_iso}:{new_cap}
    parts = callback.data.split(":")
    slot_id = int(parts[1])
    day_iso = parts[2]
    new_cap = int(parts[3])

    async with SessionLocal() as session:
        slot = await session.get(Slot, slot_id)
        if not slot:
            await callback.answer("Слот не знайдено", show_alert=True)
            return
        
        if new_cap < slot.booked_count:
            await callback.answer(
                f"❌ Не можна зменшити до {new_cap}. Вже зайнято: {slot.booked_count} місць.",
                show_alert=True
            )
            return

        slot.capacity = new_cap
        await session.commit()

    await callback.answer(f"✅ Місткість змінено на {new_cap}!")
    
    # Refresh day view
    # We'll just edit the message to say done and provide a back button
    await callback.message.edit_text(
        f"✅ Слот id:{slot_id} оновлено.\nНова місткість: {new_cap}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="↩️ До списку слотів", callback_data=f"admin_slots_day:{day_iso}:all")]
        ])
    )


@dp.callback_query(F.data == "admin_slots_back_days")
async def admin_slots_back_days(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.message.answer("Обери день:", reply_markup=build_admin_slots_days_kb())
    await callback.answer()


@dp.callback_query(F.data == "admin_slots_close")
async def admin_slots_close(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.message.answer("Ок ✅", reply_markup=admin_kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("my_cancel:"))
async def my_cancel(callback: types.CallbackQuery):
    booking_id = int(callback.data.split(":")[1])

    async with SessionLocal() as session:
        success, msg = await cancel_booking(
            session,
            booking_id,
            user_telegram_id=callback.from_user.id
        )

    if not success:
        await callback.answer(f"❌ {msg}", show_alert=True)
        return

    await callback.answer("✅ Скасовано", show_alert=True)

    # ✅ Оновлюємо ТО САМЕ повідомлення зі списком, без delete і без нових message_id
    await show_my_bookings(
    callback.message,
    tg_user=callback.from_user,
    edit=True,
    mode="active"
)

    # ✅ Адмін-нотифікація (працюватиме після КРОКУ 3)
    if ADMIN_ID:
        await bot.send_message(
            ADMIN_ID,
            f"❌ КЛІЄНТ СКАСУВАВ ЗАПИС\n"
            f"👤 {callback.from_user.full_name}\n"
            f"Booking ID: {booking_id}"
        )




@dp.callback_query(F.data == "my_back_menu")
async def my_back_menu(callback: types.CallbackQuery):
    await callback.message.answer("Обери дію 👇",reply_markup=build_main_kb(is_admin_user(callback.from_user))
)
    await callback.answer()




@dp.message(F.text == "➕ Додати слот")
async def admin_addslot_buttons_start(message: Message, state: FSMContext):
    if not is_admin(message):
        return

    await state.clear()
    await state.set_state(AdminAddSlotStates.choosing_location)
    await message.answer("Обери локацію:", reply_markup=build_admin_locations_kb())



@dp.message(F.text == "/start")
async def cmd_start(message: Message):
    await message.answer(
    "Вітаю! Обери дію 👇",
    reply_markup=build_main_kb(is_admin(message))
)


@dp.message(F.text == "/admin")
async def admin_panel(message: Message):
    if not is_admin(message):
        await message.answer("Немає доступу.")
        return

    await message.answer("Адмін-доступ ✅\nДалі додамо команди для слотів.")
@dp.message(F.text.startswith("/addslot"))
async def add_slot(message: Message):
    if not is_admin(message):
        await message.answer("Немає доступу.")
        return

    await message.answer("DEBUG: addslot зайшов у хендлер ✅")

    parts = message.text.strip().split()
    await message.answer(f"DEBUG parts={parts}")

    if len(parts) != 4:
        await message.answer("Формат: /addslot океан 2026-01-10 11:00")
        return

    loc_key = parts[1].upper()

    if loc_key not in LOCATIONS:
        await message.answer(
            "Локація має бути: " + " або ".join(LOCATIONS.values())
        )
        return

    loc = LOCATIONS[loc_key]
    await message.answer(f"DEBUG loc={loc}")

    dt_str = f"{parts[2]} {parts[3]}"
    try:
        start_time = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
    except ValueError:
        await message.answer("Дата/час: 2026-01-10 11:00")
        return

    await message.answer(f"DEBUG start={fmt_dt(start_time)}")

    end_time = start_time + timedelta(hours=1)

    try:
        async with SessionLocal() as session:
            q = select(Slot).where(
                Slot.location_code == loc,
                Slot.start_time == start_time
            )
            exists = (await session.execute(q)).scalar_one_or_none()
            if exists:
                await message.answer("Такий слот вже існує.")
                return

            session.add(Slot(
                location_code=loc,
                start_time=start_time,
                end_time=end_time,
                status="free",
                capacity=1,
                booked_count=0
            ))
            await session.commit()

    except Exception as e:
        logging.exception("DB ERROR in addslot")
        await message.answer(f"DB ERROR ❌ {type(e).__name__}: {e}")
        return

    await message.answer("DEBUG commit ✅ слот записано в БД")
    await message.answer(f"✅ Додано слот: {loc} • {fmt_dt(start_time)} (Cap: 1)")


@dp.message(F.text.startswith("/slots"))
async def list_slots(message: Message):
    if not is_admin(message):
        await message.answer("Немає доступу.")
        return

    # Формат: /slots 2026-01-08
    parts = message.text.strip().split()
    if len(parts) != 2:
        await message.answer("Формат: /slots 2026-01-08")
        return

    try:
        d = datetime.strptime(parts[1], "%Y-%m-%d").date()
    except ValueError:
        await message.answer("Дата: 2026-01-08")
        return

    start = datetime.combine(d, datetime.min.time())
    end = start + timedelta(days=1)

    async with SessionLocal() as session:
        q = (
            select(Slot)
            .where(Slot.start_time >= start, Slot.start_time < end)
            .order_by(Slot.location_code, Slot.start_time)
        )
        slots = (await session.execute(q)).scalars().all()

    if not slots:
        await message.answer("Слотів на цей день немає.")
        return

    # 🔢 ЛІЧИЛЬНИКИ
    total_capacity = sum(s.capacity for s in slots)
    total_booked = sum(s.booked_count for s in slots)
    total_free = total_capacity - total_booked

    # 📝 Формування тексту
    lines = [
        f"📅 Слоти на {d}:",
        f"🧾 Cap: {total_capacity} | Booked: {total_booked} | Free: {total_free}",
        ""
    ]

    current_loc = None
    for s in slots:
        if current_loc != s.location_code:
            current_loc = s.location_code
            lines.append(f"\n📍 Локація {current_loc}")

        # mark = "🟢" if s.status == "free" else "🔴"
        status_icon = "🟢" if s.booked_count < s.capacity else "🔴"
        lines.append(f"{status_icon} {fmt_dt(s.start_time)} ({s.booked_count}/{s.capacity}) (id:{s.id})")

    await message.answer("\n".join(lines))







@dp.message(F.text.startswith("/cancel"))
async def admin_cancel(message: Message):
    if not is_admin(message):
        return
    await message.answer("⚠️ Ця команда застаріла. Використовуйте меню '📅 Записи на день' в адмін-панелі для скасування конкретних бронювань.")
    



@dp.message(F.text == "/myid")
async def myid(message: Message):
    await message.answer(f"Твій Telegram ID: {message.from_user.id}")




@dp.message(F.text == "📅 Записатися на тренування")
async def start_booking(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(BookingStates.choosing_day)
    await state.update_data(day_page=0)

    await message.answer(
        "Обери день для запису:",
        reply_markup=build_client_days_kb(page=0)
    )





@dp.callback_query(F.data.startswith("confirm_yes:"))
async def confirm_yes(callback: types.CallbackQuery):
    booking_id = int(callback.data.split(":")[1])

    async with SessionLocal() as session:
        booking = await session.get(Booking, booking_id)

        if not booking:
            await callback.answer("Запис не знайдено", show_alert=True)
            return

        if booking.status != "active":
            await callback.answer("Цей запис вже не активний", show_alert=True)
            return

        booking.client_confirmed = True
        booking.confirmation_status = "confirmed"

        await session.commit()

    await callback.message.edit_text(
        "✅ Супер, запис підтверджено.\n"
        "Чекаю тебе на тренуванні 💪"
    )

    await callback.answer()



@dp.callback_query(F.data.startswith("confirm_no:"))
async def confirm_no(callback: types.CallbackQuery):
    booking_id = int(callback.data.split(":")[1])

    async with SessionLocal() as session:
        success, msg = await cancel_booking(
            session,
            booking_id,
            user_telegram_id=callback.from_user.id
        )

        if success:
            booking = await session.get(Booking, booking_id)
            if booking:
                booking.confirmation_status = "declined"
                await session.commit()

    if not success:
        await callback.answer(f"❌ {msg}", show_alert=True)
        return

    await callback.message.edit_text(
        "❌ Запис скасовано.\n"
        "Якщо захочеш — запишешся знову."
    )

    await callback.answer("Запис скасовано", show_alert=True)
    

# Адмін-меню (кнопки)
admin_kb = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="➕ Додати слот"), KeyboardButton(text="📅 Слоти")],
        [KeyboardButton(text="🗓 Шаблони розкладу"), KeyboardButton(text="⚡ Згенерувати тиждень")],
        [KeyboardButton(text="📅 Записи на день")], 
        [KeyboardButton(text="🔙 Головне меню")],
    ],
    resize_keyboard=True
)

@dp.message(F.text == "🛠 Адмін-панель")
async def open_admin_panel(message: Message):
    if not is_admin(message):
        return
    await message.answer("Адмін-панель 👇", reply_markup=admin_kb)

@dp.message(F.text == "🔙 Головне меню")
async def back_to_main_menu(message: Message):
    await message.answer("Головне меню 👇", reply_markup=build_main_kb(is_admin(message)))


@dp.message(F.text == "📅 Слоти")
async def admin_slots_menu(message: Message):
    if not is_admin(message):
        return
    await message.answer("Обери день:", reply_markup=build_admin_slots_days_kb())

@dp.message(F.text == "📅 Записи на день")
async def admin_bookings_menu(message: Message):
    if not is_admin(message):
        return
    await message.answer("Обери день:", reply_markup=build_admin_bookings_days_kb())


@dp.message(F.text == "📍 Локації тренувань")
async def locations_handler(message: Message):
    await send_locations(message)
    
    

@dp.callback_query(F.data == "confirm_booking")
async def confirm_booking(callback: types.CallbackQuery, state: FSMContext):
    if await state.get_state() != BookingStates.confirming.state:
        await callback.answer()
        return

    data = await state.get_data()
    slot_id = data.get("slot_id")
    if slot_id is None:
        await callback.message.answer("Помилка: не обрано слот.")
        await state.clear()
        await callback.answer()
        return

    # Use service to create booking
    async with SessionLocal() as session:
        booking, msg = await create_booking(
            session, 
            telegram_id=callback.from_user.id,
            username=callback.from_user.username,
            full_name=callback.from_user.full_name,
            slot_id=int(slot_id)
        )

    if not booking:
        await callback.message.answer(f"❌ {msg}")
        await state.clear()
        await callback.answer()
        return

    user = callback.from_user
    # booking object has .slot loaded? create_booking says it re-queries with joinedload
    slot_loc = booking.location
    slot_time = booking.booking_date
    
    await callback.message.answer(
        f"✅ Готово\n📍 {slot_loc}\n🕒 {fmt_dt(slot_time)}",
        reply_markup=build_main_kb(is_admin_user(callback.from_user))
    )

    if ADMIN_ID and ADMIN_ID != 0:
        await bot.send_message(
            ADMIN_ID,
            "🔥 НОВИЙ ЗАПИС\n"
            f"👤 {user.full_name} (@{user.username or '—'})\n"
            f"🆔 {user.id}\n"
            f"📍 {slot_loc}\n"
            f"🕒 {fmt_dt(slot_time)}"
        )

    await state.clear()
    await callback.answer()


@dp.callback_query(F.data.startswith("daypage:"))
async def client_days_page(callback: types.CallbackQuery, state: FSMContext):
    page = int(callback.data.split(":")[1])
    await state.update_data(day_page=page)
    await state.set_state(BookingStates.choosing_day)

    await callback.message.answer(
        "Обери день для запису:",
        reply_markup=build_client_days_kb(page=page)
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("dayiso:"))
async def client_pick_day(callback: types.CallbackQuery, state: FSMContext):
    day_iso = callback.data.split(":", 1)[1]

    await state.update_data(target_day=day_iso)
    await state.set_state(BookingStates.choosing_location)

    d = date.fromisoformat(day_iso)
    await callback.message.answer(
        f"Обери локацію на {d.strftime('%d.%m.%Y')}:",
        reply_markup=build_client_locations_kb()
    )
    await callback.answer()

@dp.callback_query(F.data.startswith("cloc:"))
async def client_pick_location(callback: types.CallbackQuery, state: FSMContext):
    loc = callback.data.split(":", 1)[1]
    data = await state.get_data()
    day_iso = data.get("target_day")

    if not day_iso:
        await callback.message.answer("❌ Помилка стану. Почни знову: 📅 Записатися")
        await state.clear()
        await callback.answer()
        return

    target_day = date.fromisoformat(day_iso)
    await state.update_data(location_filter=loc)
    await state.set_state(BookingStates.choosing_slot)

    kb = await build_free_slots_kb(target_day, loc)
    if kb is None:
        await callback.message.answer("На цей день вільних слотів немає.")
        await callback.answer()
        return

    title_loc = "Усі локації" if loc == "ALL" else loc
    await callback.message.answer(
        f"📅 {target_day.strftime('%d.%m.%Y')} • 📍 {title_loc}",
        reply_markup=kb
    )
    await callback.answer()

@dp.callback_query(F.data == "back_to_locations")
async def back_to_locations(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    day_iso = data.get("target_day")
    if not day_iso:
        await state.clear()
        await callback.message.answer("Почни знову: 📅 Записатися")
        await callback.answer()
        return

    await state.set_state(BookingStates.choosing_location)
    d = date.fromisoformat(day_iso)
    await callback.message.answer(
        f"Обери локацію на {d.strftime('%d.%m.%Y')}:",
        reply_markup=build_client_locations_kb()
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("slot:"))
async def choose_slot(callback: types.CallbackQuery, state: FSMContext):
    # Маємо бути в стані вибору слота
    if await state.get_state() != BookingStates.choosing_slot.state:
        await callback.answer()
        return

    slot_id = int(callback.data.split(":")[1])

    async with SessionLocal() as session:
        slot = (await session.execute(
            select(Slot).where(Slot.id == slot_id)
        )).scalar_one_or_none()

    if slot is None:
        await callback.message.answer("Слот не знайдено.")
        await callback.answer()
        return

    if slot.booked_count >= slot.capacity:
        await callback.message.answer("Цей слот вже повністю зайнятий. Обери інший.")
        await callback.answer()
        return

    # Переходимо на підтвердження
    await state.set_state(BookingStates.confirming)
    await state.update_data(slot_id=slot_id)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Підтвердити", callback_data="confirm_booking")],
        [InlineKeyboardButton(text="↩️ Назад", callback_data="back_to_slots")],
        [InlineKeyboardButton(text="❌ Скасувати", callback_data="cancel_booking")],
    ])

    await callback.message.answer(
        "Підтвердити запис?\n\n"
        f"📍 {slot.location_code}\n"
        f"🕒 {fmt_dt(slot.start_time)}",
        reply_markup=kb
    )
    await callback.answer()



@dp.callback_query(F.data == "back_to_slots")
async def back_to_slots(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    target_day = date.fromisoformat(data.get("target_day", date.today().isoformat()))
    loc = data.get("location_filter", "ALL")
    kb = await build_free_slots_kb(target_day, loc)


    if kb is None:
        await callback.message.answer("На цей день вільних слотів немає.")
        await state.clear()
        await callback.answer()
        return

    await state.set_state(BookingStates.choosing_slot)
    await callback.message.answer("Обери вільний час:", reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data == "cancel_booking")
async def cancel_booking_process(callback: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("Запис скасовано.")
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_add_loc:"))
async def admin_add_pick_location(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    loc_key = callback.data.split(":", 1)[1]      # ОКЕАН / ЦЕНТР
    loc = LOCATIONS[loc_key]                      # Океан / Центр

    await state.update_data(add_loc=loc)
    await state.set_state(AdminAddSlotStates.choosing_day)

    await callback.message.answer("Обери день (7 днів наперед):", reply_markup=build_admin_days_kb())
    await callback.answer()



@dp.callback_query(F.data.startswith("admin_add_day:"))
async def admin_add_pick_day(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    day_iso = callback.data.split(":", 1)[1]
    await state.update_data(add_day=day_iso)
    await state.set_state(AdminAddSlotStates.choosing_time)

    await callback.message.answer("Обери час:", reply_markup=build_admin_times_kb())
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_add_time:"))
async def admin_add_pick_time(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    time_str = callback.data.split(":", 1)[1]
    data = await state.get_data()

    loc = data.get("add_loc")
    day_iso = data.get("add_day")
    if not loc or not day_iso:
        await callback.message.answer("❌ Помилка стану. Почни спочатку: ➕ Додати слот")
        await state.clear()
        await callback.answer()
        return

    start_dt = datetime.fromisoformat(f"{day_iso} {time_str}")
    end_dt = start_dt + timedelta(hours=1)

    await state.update_data(add_time=time_str)
    await state.update_data(add_start=start_dt.isoformat())
    await state.update_data(add_end=end_dt.isoformat())
    
    # NEW: Go to Capacity step
    await state.set_state(AdminAddSlotStates.choosing_capacity)
    await callback.message.answer(
        f"3/4. Час: {time_str}\n\nОбери місткість (кількість людей):", reply_markup=build_admin_capacity_kb()
    )
    await callback.answer()



@dp.callback_query(F.data.startswith("admin_add_cap:"))
async def admin_add_pick_capacity(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    cap = int(callback.data.split(":")[1])
    await state.update_data(add_capacity=cap)
    
    data = await state.get_data()
    loc = data.get("add_loc")
    # time_str = data.get("add_time") 
    start_iso = data.get("add_start")
    if isinstance(start_iso, datetime):
        start_dt = start_iso
    elif isinstance(start_iso, str):
        start_dt = datetime.fromisoformat(start_iso)
    else:
        raise TypeError(f"start_iso has invalid type: {type(start_iso).__name__}, value={start_iso!r}")

    await state.set_state(AdminAddSlotStates.confirming)

    await callback.message.answer(
        "Підтвердь додавання слота:\n\n"
        f"📍 {loc}\n"
        f"🕒 {start_dt.strftime('%d.%m.%Y %H:%M')}\n"
        f"👥 Місткість: {cap}",
        reply_markup=build_admin_confirm_kb()
    )
    await callback.answer()

@dp.callback_query(F.data == "admin_add_confirm")
async def admin_add_confirm(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    data = await state.get_data()
    loc = data.get("add_loc")
    start_iso = data.get("add_start")
    end_iso = data.get("add_end")
    capacity = int(data.get("add_capacity", 1))

    if not loc or not start_iso or not end_iso:
        await callback.message.answer("❌ Помилка стану. Почни заново.")
        await state.clear()
        await callback.answer()
        return

    start_time = datetime.fromisoformat(start_iso)
    end_time = datetime.fromisoformat(end_iso)

    try:
        async with SessionLocal() as session:
            q = select(Slot).where(Slot.location_code == loc, Slot.start_time == start_time)
            exists = (await session.execute(q)).scalar_one_or_none()
            
            if exists:
                # Conflict resolution
                await callback.message.answer(
                     f"⚠️ Слот вже існує!\n"
                     f"📍 {loc} • {start_time.strftime('%H:%M')}\n"
                     f"Поточна: {exists.capacity} | Зайнято: {exists.booked_count}\n\n"
                     f"Змінити місткість на {capacity}?",
                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                         [InlineKeyboardButton(text=f"✏️ Так, змінити на {capacity}", callback_data=f"admin_force_cap:{exists.id}:{capacity}")],
                         [InlineKeyboardButton(text="❌ Скасувати", callback_data="admin_add_cancel")]
                     ])
                )
                await state.clear()
                await callback.answer()
                return

            # Create new
            session.add(Slot(
                location_code=loc,
                start_time=start_time,
                end_time=end_time,
                status="free",
                capacity=capacity,
                booked_count=0
            ))
            await session.commit()
            
    except Exception as e:
        await callback.message.answer(f"❌ Помилка БД: {e}")
        await callback.answer()
        return

    await callback.message.answer(
        f"✅ Додано слот: {loc} • {start_time.strftime('%d.%m.%Y %H:%M')}\n👥 Capacity: {capacity}"
    )
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data.startswith("admin_force_cap:"))
async def admin_force_capacity_update(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    # admin_force_cap:{id}:{new_cap}
    parts = callback.data.split(":")
    slot_id = int(parts[1])
    new_cap = int(parts[2])

    async with SessionLocal() as session:
        slot = await session.get(Slot, slot_id)
        if not slot:
            await callback.answer("Слот не знайдено", show_alert=True)
            return
        
        if new_cap < slot.booked_count:
             await callback.message.answer(
                 f"❌ Не можна зменшити місткість до {new_cap}, бо вже є {slot.booked_count} записів."
             )
             await callback.answer()
             return

        slot.capacity = new_cap
        await session.commit()

    await callback.message.answer(f"✅ Місткість оновлено до {new_cap}.")
    await callback.answer()


@dp.callback_query(F.data == "admin_add_back_loc")
async def admin_add_back_loc(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await state.set_state(AdminAddSlotStates.choosing_location)
    await callback.message.answer("Обери локацію:", reply_markup=build_admin_locations_kb())
    await callback.answer()


@dp.callback_query(F.data == "admin_add_back_day")
async def admin_add_back_day(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await state.set_state(AdminAddSlotStates.choosing_day)
    await callback.message.answer("Обери день:", reply_markup=build_admin_days_kb())
    await callback.answer()


@dp.callback_query(F.data == "admin_add_back_time")
async def admin_add_back_time(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await state.set_state(AdminAddSlotStates.choosing_time)
    await callback.message.answer("Обери час:", reply_markup=build_admin_times_kb())
    await callback.answer()


@dp.callback_query(F.data == "admin_add_cancel")
async def admin_add_cancel(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await state.clear()
    await callback.message.answer("Ок, скасовано ✅")
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_slot_del:"))
async def admin_slot_delete(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    _, slot_id_str, target_day_iso = callback.data.split(":", 2)
    slot_id = int(slot_id_str)

    async with SessionLocal() as session:
        slot = (await session.execute(
            select(Slot).where(Slot.id == slot_id)
        )).scalar_one_or_none()

        if slot is None:
            await callback.message.answer("Слот не знайдено.")
            await callback.answer()
            return

        # Проверяем любые бронирования по этому слоту: и active, и canceled
        bookings_count = await session.scalar(
            select(func.count()).select_from(Booking).where(Booking.slot_id == slot_id)
        )

        if bookings_count and bookings_count > 0:
            await callback.message.answer(
                f"❌ Слот не можна видалити, бо з ним пов'язано {bookings_count} бронювань "
                f"(включно з історією/скасованими)."
            )
            await callback.answer()
            return

        await session.delete(slot)
        await session.commit()

    await callback.message.answer(f"🗑 Видалено слот id:{slot_id}")
    await callback.answer()



@dp.callback_query(F.data.startswith("admin_client:"))
async def admin_client_profile(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    # Data: admin_client:{booking_id}:{day_iso}
    parts = callback.data.split(":")
    booking_id = int(parts[1])
    day_iso = parts[2]

    async with SessionLocal() as session:
        # Fetch booking with user/slot to show details
        q = (
            select(Booking)
            .options(joinedload(Booking.user), joinedload(Booking.slot))
            .where(Booking.id == booking_id)
        )
        booking = (await session.execute(q)).scalar_one_or_none()

    if not booking:
        await callback.answer("Бронювання не знайдено", show_alert=True)
        return

    user = booking.user
    if not user:
        # Fallback for legacy data where migration hasn't run or failed
        await callback.answer(
            f"⚠️ Legacy Data! User ID: {booking.user_id}\nЗапустіть міграцію (перезапуск бота).",
            show_alert=True
        )
        return

    text = (
        f"👤 Клієнт: {user.full_name or '—'}\n"
        f"TG Username: @{user.username or '—'}\n"
        f"TG ID: {user.telegram_id}\n"
        f"Internal ID: {user.id}\n\n"
        f"📅 Поточний запис: {fmt_dt(booking.slot.start_time)} ({booking.slot.location_code})"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📌 Записи клієнта", callback_data=f"admin_u_bookings:{user.id}:{day_iso}")],
        [InlineKeyboardButton(text="↩️ Назад до списку", callback_data=f"admin_bookings_day:{day_iso}")]
    ])
    
    await callback.message.edit_text(text, reply_markup=kb)
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_u_bookings:"))
async def admin_client_bookings(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    # admin_u_bookings:{user_id}:{day_iso} (day_iso for back button)
    parts = callback.data.split(":")
    user_id = int(parts[1])
    day_iso = parts[2]

    from services.booking_service import get_user_bookings_admin

    async with SessionLocal() as session:
        bookings = await get_user_bookings_admin(session, user_id)

    if not bookings:
        await callback.answer("У клієнта немає записів", show_alert=True)
        return

    lines = [f"📌 Записи клієнта (ID:{user_id}):"]
    rows = []
    now = datetime.now()

    future_active = [
        b for b in bookings
        if b.status == "active" and b.slot and b.slot.start_time >= now
    ]

    past_active = [
        b for b in bookings
        if b.status == "active" and b.slot and b.slot.start_time < now
    ]

    canceled = [
        b for b in bookings
        if b.status != "active"
    ]

    if future_active:
        lines.append("\n🟢 Майбутні активні записи:")
        for b in future_active:
            dt_str = b.slot.start_time.strftime("%d.%m %H:%M")
            loc = b.slot.location_code
            lines.append(f"🟢 {dt_str} • {loc}")
            rows.append([
                InlineKeyboardButton(
                    text=f"❌ Скасувати {dt_str}",
                    callback_data=f"admin_cancel_b:{b.id}:{user_id}:{day_iso}"
                )
            ])

    if past_active:
        lines.append("\n🕓 Минулі записи:")
        for b in past_active:
            dt_str = b.slot.start_time.strftime("%d.%m %H:%M")
            loc = b.slot.location_code
            lines.append(f"✅ {dt_str} • {loc} (було)")

    if canceled:
        lines.append("\n⚪️ Скасовані записи:")
        for b in canceled:
            dt_str = b.slot.start_time.strftime("%d.%m %H:%M")
            loc = b.slot.location_code
            lines.append(f"⚪️ {dt_str} • {loc} (скасовано)")

    rows.append([
        InlineKeyboardButton(
            text="↩️ Назад до клієнта",
            callback_data=f"admin_client:{bookings[0].id}:{day_iso}"
        )
    ])

    await callback.message.edit_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows)
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("admin_cancel_b:"))
async def admin_cancel_booking_handler(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    # admin_cancel_b:{booking_id}:{user_id}:{day_iso}
    parts = callback.data.split(":")
    booking_id = int(parts[1])
    user_id = int(parts[2])
    day_iso = parts[3]

    async with SessionLocal() as session:
        success, msg = await cancel_booking(session, booking_id, is_admin=True)

    if success:
        await callback.answer("✅ Запис скасовано", show_alert=True)
        # Refresh client bookings view
        # We can construct a fake callback or just call the function if we refactor,
        # but easier to just recursively call the handler logic or redirect.
        # Let's emit a new callback event or just call the handler manually?
        # Manually constructing data is easiest.
        
        callback.data = f"admin_u_bookings:{user_id}:{day_iso}"
        await admin_client_bookings(callback)
    else:
        await callback.answer(f"❌ Помилка: {msg}", show_alert=True)


@dp.callback_query(F.data.startswith("admin_bookings_day:"))
async def admin_bookings_show_day(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return

    day_iso = callback.data.split(":", 1)[1]
    target_day = date.fromisoformat(day_iso)
    
    # Use service or direct query
    async with SessionLocal() as session:
        # Get active bookings for the day
        bookings = await get_bookings_for_day(session, target_day)

    if not bookings:
        await callback.message.edit_text(
            f"📅 Записи на {target_day.strftime('%d.%m.%Y')}\n\nНемає записів.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="↩️ Назад", callback_data="admin_bookings_back")],
                [InlineKeyboardButton(text="❌ Закрити", callback_data="admin_bookings_close")],
            ])
        )
        await callback.answer()
        return

    lines = [f"📅 Записи на {target_day.strftime('%d.%m.%Y')}"]
    current_loc = None

    rows = []
    for b in bookings:
        # b is Booking, with b.slot and b.user loaded
        loc = b.slot.location_code
        if current_loc != loc:
            current_loc = loc
            lines.append(f"\n📍 {current_loc}")

        # Get user info from User relation, fallbacks to booked_name (legacy) if needed
        if b.user:
            name = b.user.full_name or b.user.username or f"ID:{b.user.telegram_id}"
        else:
            name = "—"

        t = b.slot.start_time.strftime("%H:%M")
        lines.append(f"🕒 {t} • 👤 {name}")

        # кнопка: відкрити анкету/профіль клієнта
        # Callback: admin_client:{booking_id}:{day_iso}
        rows.append([InlineKeyboardButton(
            text=f"{t} • {name}",
            callback_data=f"admin_client:{b.id}:{day_iso}" 
        )])

    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data="admin_bookings_back")])
    rows.append([InlineKeyboardButton(text="❌ Закрити", callback_data="admin_bookings_close")])

    await callback.message.edit_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await callback.answer()

@dp.callback_query(F.data == "admin_bookings_back")
async def admin_bookings_back(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.message.answer("Обери день:", reply_markup=build_admin_bookings_days_kb())
    await callback.answer()

@dp.callback_query(F.data == "admin_bookings_close")
async def admin_bookings_close(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer()
        return
    await callback.message.answer("Ок ✅", reply_markup=admin_kb)
    await callback.answer()


@dp.callback_query(F.data == "my_close")
async def my_close(callback: types.CallbackQuery):
    await callback.message.delete()
    await callback.answer()

@dp.callback_query(F.data.startswith("my_mode:"))
async def my_mode(callback: types.CallbackQuery):
    mode = callback.data.split(":")[1]  # active/history

    changed, text, kb = await show_my_bookings(
        callback.message,
        tg_user=callback.from_user,
        edit=True,
        mode=mode
    )

    if not changed:
        await callback.message.answer(text, reply_markup=kb)

        try:
            await callback.message.delete()
        except:
            pass

    await callback.answer()

    
    







@dp.message()
async def fallback(message: Message):
    await message.answer(
        "Будь ласка, скористайся меню нижче 👇\n"
        "або напиши /start"
    )

async def main():
    global bot  # ✅ важливо
    await init_db()

    bot = Bot(token=BOT_TOKEN)

    async with SessionLocal() as session_db:
        fixed_count = await fix_legacy_booking_user_ids(session_db)
        if fixed_count > 0:
            logging.info(f"Fixed {fixed_count} legacy bookings with incorrect user_id linkage.")

    asyncio.create_task(reminder_worker(bot))
    
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())

