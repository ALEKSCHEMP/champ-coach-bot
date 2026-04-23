import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from database.models import Booking, BookingReschedule, Slot, User

logger = logging.getLogger(__name__)
KYIV_TZ = ZoneInfo("Europe/Kyiv")


def now_kyiv_naive() -> datetime:
    return datetime.now(KYIV_TZ).replace(tzinfo=None)


async def _load_booking(session: AsyncSession, booking_id: int) -> Booking | None:
    result = await session.execute(
        select(Booking)
        .options(joinedload(Booking.slot), joinedload(Booking.user))
        .where(Booking.id == booking_id)
    )
    return result.scalar_one_or_none()


async def _has_time_conflict(
    session: AsyncSession,
    *,
    user_id: int,
    booking_id: int,
    new_slot: Slot,
) -> bool:
    result = await session.execute(
        select(Booking.id)
        .join(Slot, Booking.slot_id == Slot.id)
        .where(
            Booking.user_id == user_id,
            Booking.id != booking_id,
            Booking.status == "active",
            and_(
                Slot.start_time < new_slot.end_time,
                Slot.end_time > new_slot.start_time,
            ),
        )
        .limit(1)
    )
    return result.scalar_one_or_none() is not None


async def validate_reschedule(
    session: AsyncSession,
    *,
    booking: Booking,
    new_slot: Slot,
    actor_role: str,
    actor_user_id: int | None,
) -> tuple[bool, str]:
    actor_role = actor_role.lower()
    now = now_kyiv_naive()
    old_slot = booking.slot
    people_count = getattr(booking, "people_count", 1) or 1

    if actor_role not in {"user", "admin"}:
        return False, "Некоректна роль користувача"

    if booking.status != "active":
        return False, "Переносити можна тільки активний запис"

    if not old_slot:
        return False, "Старий слот запису не знайдено"

    if actor_role == "user":
        owner_tg_id = booking.user.telegram_id if booking.user else None
        if actor_user_id is not None and owner_tg_id != actor_user_id:
            return False, "Це не ваше бронювання"

        if old_slot.start_time <= now:
            return False, "Цей запис уже почався"

        if old_slot.start_time < now + timedelta(hours=4):
            return False, "Перенести тренування можна не пізніше ніж за 4 години до початку"

    if new_slot.start_time <= now:
        return False, "Не можна перенести запис у минулий слот"

    if new_slot.id == old_slot.id:
        return False, "Новий слот збігається зі старим"

    if (new_slot.capacity or 1) - (new_slot.booked_count or 0) < people_count:
        return False, "У новому слоті вже немає достатньо місць"

    if actor_role == "user" and await _has_time_conflict(
        session,
        user_id=booking.user_id,
        booking_id=booking.id,
        new_slot=new_slot,
    ):
        return False, "У вас вже є інший активний запис, який перетинається з цим часом"

    return True, "OK"


async def get_available_reschedule_dates(
    session: AsyncSession,
    booking_id: int,
    *,
    actor_role: str,
    actor_user_id: int | None = None,
    days: int = 14,
) -> list[date]:
    booking = await _load_booking(session, booking_id)
    if not booking or not booking.slot:
        return []

    today = now_kyiv_naive().date()
    available_dates: list[date] = []

    for i in range(days):
        target_date = today + timedelta(days=i)
        slots = await get_available_reschedule_slots(
            session,
            booking_id,
            target_date,
            actor_role=actor_role,
            actor_user_id=actor_user_id,
        )
        if slots:
            available_dates.append(target_date)

    return available_dates


async def get_available_reschedule_slots(
    session: AsyncSession,
    booking_id: int,
    target_date: date,
    *,
    actor_role: str,
    actor_user_id: int | None = None,
) -> list[Slot]:
    booking = await _load_booking(session, booking_id)
    if not booking or not booking.slot:
        return []

    day_start = datetime.combine(target_date, datetime.min.time())
    day_end = day_start + timedelta(days=1)
    now = now_kyiv_naive()
    people_count = getattr(booking, "people_count", 1) or 1

    result = await session.execute(
        select(Slot)
        .where(
            Slot.start_time >= day_start,
            Slot.start_time < day_end,
            Slot.start_time > now,
            Slot.id != booking.slot_id,
            Slot.capacity - Slot.booked_count >= people_count,
        )
        .order_by(Slot.location_code, Slot.start_time)
    )
    slots = result.scalars().all()

    if actor_role.lower() != "user":
        return slots

    filtered = []
    for slot in slots:
        ok, _ = await validate_reschedule(
            session,
            booking=booking,
            new_slot=slot,
            actor_role=actor_role,
            actor_user_id=actor_user_id,
        )
        if ok:
            filtered.append(slot)

    return filtered


async def reschedule_booking(
    session: AsyncSession,
    booking_id: int,
    new_slot_id: int,
    actor_role: str,
    actor_user_id: int | None = None,
) -> tuple[Booking | None, str, Slot | None, Slot | None]:
    logger.info(
        "reschedule_started",
        extra={
            "user_id": actor_user_id,
            "booking_id": booking_id,
            "new_slot_id": new_slot_id,
            "actor_role": actor_role,
        },
    )

    try:
        booking = await _load_booking(session, booking_id)
        if not booking:
            logger.warning(
                "reschedule_validation_failed",
                extra={"user_id": actor_user_id, "booking_id": booking_id, "new_slot_id": new_slot_id, "actor_role": actor_role},
            )
            return None, "Бронювання не знайдено", None, None

        old_slot = booking.slot
        new_slot = await session.get(Slot, new_slot_id)
        if not new_slot:
            logger.warning(
                "reschedule_validation_failed",
                extra={"user_id": actor_user_id, "booking_id": booking_id, "old_slot_id": booking.slot_id, "new_slot_id": new_slot_id, "actor_role": actor_role},
            )
            return None, "Новий слот не знайдено", old_slot, None

        ok, msg = await validate_reschedule(
            session,
            booking=booking,
            new_slot=new_slot,
            actor_role=actor_role,
            actor_user_id=actor_user_id,
        )
        if not ok:
            logger.warning(
                "reschedule_validation_failed",
                extra={"user_id": actor_user_id, "booking_id": booking_id, "old_slot_id": booking.slot_id, "new_slot_id": new_slot_id, "actor_role": actor_role},
            )
            return None, msg, old_slot, new_slot

        people_count = getattr(booking, "people_count", 1) or 1
        old_slot_id = booking.slot_id

        dec_result = await session.execute(
            update(Slot)
            .where(Slot.id == old_slot_id)
            .where(Slot.booked_count >= people_count)
            .values(booked_count=Slot.booked_count - people_count)
            .execution_options(synchronize_session="fetch")
        )
        if dec_result.rowcount == 0:
            await session.rollback()
            logger.warning(
                "reschedule_validation_failed",
                extra={"user_id": actor_user_id, "booking_id": booking_id, "old_slot_id": old_slot_id, "new_slot_id": new_slot_id, "actor_role": actor_role},
            )
            return None, "Некоректна кількість зайнятих місць у старому слоті", old_slot, new_slot

        inc_result = await session.execute(
            update(Slot)
            .where(Slot.id == new_slot_id)
            .where(Slot.capacity - Slot.booked_count >= people_count)
            .values(booked_count=Slot.booked_count + people_count)
            .execution_options(synchronize_session="fetch")
        )
        if inc_result.rowcount == 0:
            await session.rollback()
            logger.warning(
                "reschedule_validation_failed",
                extra={"user_id": actor_user_id, "booking_id": booking_id, "old_slot_id": old_slot_id, "new_slot_id": new_slot_id, "actor_role": actor_role},
            )
            return None, "Новий слот вже зайнятий або місць недостатньо", old_slot, new_slot

        booking.slot_id = new_slot_id
        booking.booking_date = new_slot.start_time
        booking.location = new_slot.location_code
        booking.client_confirmed = False
        booking.confirmation_status = "pending"
        booking.reminder_24h_sent = False
        booking.reminder_morning_sent = False
        booking.reminder_day_sent = False

        changed_by_user_id = None
        if actor_user_id is not None:
            changed_by_user_id = await session.scalar(
                select(User.id).where(User.telegram_id == actor_user_id)
            )

        session.add(
            BookingReschedule(
                booking_id=booking.id,
                old_slot_id=old_slot_id,
                new_slot_id=new_slot_id,
                changed_by_user_id=changed_by_user_id,
                changed_by_role=actor_role.lower(),
            )
        )

        await session.commit()

        reloaded = await session.execute(
            select(Booking)
            .options(joinedload(Booking.slot), joinedload(Booking.user))
            .where(Booking.id == booking_id)
        )
        updated_booking = reloaded.scalar_one()
        logger.info(
            "reschedule_completed",
            extra={"user_id": actor_user_id, "booking_id": booking_id, "old_slot_id": old_slot_id, "new_slot_id": new_slot_id, "actor_role": actor_role},
        )
        return updated_booking, "OK", old_slot, new_slot

    except Exception as exc:
        await session.rollback()
        logger.exception(
            "reschedule_failed",
            extra={"user_id": actor_user_id, "booking_id": booking_id, "new_slot_id": new_slot_id, "actor_role": actor_role},
        )
        return None, f"Помилка перенесення: {exc}", None, None
