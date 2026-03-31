import logging
from datetime import date, datetime, timedelta
from sqlalchemy import select, update, and_, desc, asc, func
from sqlalchemy.orm import joinedload
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import Slot, Booking, User
import asyncio

logger = logging.getLogger(__name__)
from services.google_calendar import safe_delete_calendar_event

async def get_slots_by_date(
    session: AsyncSession, 
    target_date: date, 
    filter_status: str | None = None
) -> list[Slot]:
    start = datetime.combine(target_date, datetime.min.time())
    end = start + timedelta(days=1)
    
    q = select(Slot).where(
        Slot.start_time >= start,
        Slot.start_time < end
    ).order_by(Slot.location_code, Slot.start_time)
    
    if filter_status:
        q = q.where(Slot.status == filter_status)
        
    result = await session.execute(q)
    return result.scalars().all()

async def get_or_create_user(
    session: AsyncSession,
    telegram_id: int,
    username: str | None = None,
    full_name: str | None = None
) -> User:
    """
    Finds a User by telegram_id or creates a new one.
    Updates username/full_name if changed.
    USES FLUSH ONLY (No Commit) to allow atomic transactions in callers.
    """
    stmt = select(User).where(User.telegram_id == telegram_id)
    user = (await session.execute(stmt)).scalar_one_or_none()

    if not user:
        user = User(
            telegram_id=telegram_id,
            username=username,
            full_name=full_name,
            role="user"
        )
        session.add(user)
        await session.flush() # Generate ID without committing
    else:
        # Update info if changed
        if user.username != username or user.full_name != full_name:
            user.username = username
            user.full_name = full_name
            await session.flush()
    
    return user

async def create_booking(
    session: AsyncSession,
    user_id: int | None = None,
    slot_id: int = 0,
    telegram_id: int | None = None,
    username: str | None = None,
    full_name: str | None = None,
    people_count: int = 1
) -> tuple[Booking | None, str]:
    logger.info("booking_creation_started", extra={"user_id": user_id, "telegram_id": telegram_id, "slot_id": slot_id, "people_count": people_count})
    try:
        # 1. Resolve User
        if telegram_id:
            user = await get_or_create_user(session, telegram_id, username, full_name)
            user_id = user.id

        if not user_id:
            return None, "Не вказано користувача"

        # 2. Fetch slot first
        slot = await session.get(Slot, slot_id)
        if not slot:
            return None, "Слот не знайдено"

        # 3. Prevent duplicate booking for the same slot
        existing_booking = await session.execute(
            select(Booking).where(
                Booking.user_id == user_id,
                Booking.slot_id == slot_id,
                Booking.status == "active"
            )
        )
        if existing_booking.scalar_one_or_none():
            logger.warning("booking_creation_blocked_duplicate", extra={"user_id": user_id, "slot_id": slot_id})
            return None, "Ви вже записані на цей слот"

        # 4. Prevent booking another slot at the same time
        same_time_booking = await session.execute(
            select(Booking)
            .join(Slot, Booking.slot_id == Slot.id)
            .where(
                Booking.user_id == user_id,
                Booking.status == "active",
                and_(
                    Slot.start_time < slot.end_time,
                    Slot.end_time > slot.start_time
                )
            )
        )
        if same_time_booking.scalar_one_or_none():
            logger.warning("booking_creation_blocked_overlap", extra={"user_id": user_id, "slot_id": slot_id})
            return None, "У вас вже є інший запис, який перетинається з цим часом"

        # 5. Atomic capacity update
        update_stmt = (
            update(Slot)
            .where(Slot.id == slot_id)
            .where(Slot.capacity - Slot.booked_count >= people_count)
            .values(booked_count=Slot.booked_count + people_count)
            .execution_options(synchronize_session="fetch")
        )

        result = await session.execute(update_stmt)

        if result.rowcount == 0:
            logger.warning("booking_creation_blocked_no_capacity", extra={"user_id": user_id, "slot_id": slot_id, "people_count": people_count})
            return None, "Недостатньо вільних місць"

        # 6. Create booking
        new_booking = Booking(
            user_id=user_id,
            slot_id=slot.id,
            booking_date=slot.start_time,
            location=slot.location_code,
            status="active",
            people_count=people_count
        )
        session.add(new_booking)

        # 7. Commit
        await session.commit()

        # 8. Reload
        reloaded_booking = await session.execute(
            select(Booking)
            .options(joinedload(Booking.slot), joinedload(Booking.user))
            .where(Booking.id == new_booking.id)
        )
        logger.info("booking_created", extra={"booking_id": new_booking.id, "user_id": user_id, "slot_id": slot_id, "location": slot.location_code})
        return reloaded_booking.scalar_one(), "OK"

    except Exception as e:
        logger.exception("booking_creation_failed", extra={"user_id": user_id, "slot_id": slot_id})
        await session.rollback()
        return None, f"Помилка створення запису: {e}"

async def fix_legacy_booking_user_ids(session: AsyncSession) -> int:
    """
    Migrates legacy bookings where user_id might be a telegram_id
    to use correct User.id internal PK.
    Creates User if missing (for legacy data consistency).
    """
    # 1. Fetch all bookings that might need fix
    q = select(Booking).options(joinedload(Booking.user)).execution_options(populate_existing=True)
    result = await session.execute(q)
    bookings = result.scalars().all()
    
    fixed_count = 0
    
    for b in bookings:
        # If relation is None, but we have a user_id, it might be a broken link (legacy telegram_id)
        if b.user is None and b.user_id:
            # Assume b.user_id holds the Telegram ID in legacy data
            legacy_tg_id = b.user_id
            
            # Try to find a user with this telegram_id
            stmt = select(User).where(User.telegram_id == legacy_tg_id)
            user = (await session.execute(stmt)).scalar_one_or_none()
            
            if not user:
                # Create missing user if not found
                user = User(
                    telegram_id=legacy_tg_id,
                    username=None,
                    full_name=None,
                    role="user"
                )
                session.add(user)
                await session.flush() # Generate ID
            
            # Update booking to point to internal ID
            b.user_id = user.id
            fixed_count += 1
    
    if fixed_count > 0:
        await session.commit()
        
    return fixed_count

async def cancel_booking(
    session: AsyncSession,
    booking_id: int,
    user_telegram_id: int | None = None,
    telegram_id: int | None = None,
    is_admin: bool = False
) -> tuple[bool, str]:
    # Normalize telegram_id
    if telegram_id is None:
        telegram_id = user_telegram_id

    logger.info("booking_cancel_started", extra={"booking_id": booking_id, "telegram_id": telegram_id, "is_admin": is_admin})

    try:
        q = (
            select(Booking)
            .options(
                joinedload(Booking.slot),
                joinedload(Booking.user)
            )
            .where(Booking.id == booking_id)
        )

        result = await session.execute(q)
        booking = result.scalar_one_or_none()

        if not booking:
            return False, "Бронювання не знайдено"

        # Check ownership
        if telegram_id is not None and not is_admin:
            owner_tg_id = booking.user.telegram_id if booking.user else None

            # legacy fallback
            if owner_tg_id is None and booking.user_id == telegram_id:
                owner_tg_id = telegram_id

            if owner_tg_id != telegram_id:
                logger.warning("booking_cancel_blocked_not_owner", extra={"booking_id": booking_id, "telegram_id": telegram_id})
                return False, "Це не ваше бронювання"

        # Check status
        if booking.status == "canceled":
            return True, "Вже скасовано"

        if booking.status != "active":
            return False, f"Неможливо скасувати запис зі статусом: {booking.status}"

        # Mark as canceled
        booking.status = "canceled"

        if booking.calendar_event_id:
            await safe_delete_calendar_event(booking.calendar_event_id)

        # Atomic decrement
        if booking.slot:
            stmt = (
                update(Slot)
                .where(Slot.id == booking.slot_id)
                .where(Slot.booked_count > 0)
                .values(booked_count=func.max(0, Slot.booked_count - getattr(booking, "people_count", 1)))
                .execution_options(synchronize_session="fetch")
            )
            await session.execute(stmt)

        await session.commit()
        logger.info("booking_canceled", extra={"booking_id": booking_id})
        return True, "Скасовано успішно"

    except Exception as e:
        logger.exception("booking_cancel_failed", extra={"booking_id": booking_id})
        await session.rollback()
        return False, f"Помилка скасування: {e}"

async def get_user_bookings(session: AsyncSession, user_id: int) -> list[Booking]:
    """
    Returns ALL user bookings (active + canceled), joined with Slot, sorted by start_time.
    """
    q = select(Booking).join(Slot).options(joinedload(Booking.slot)).where(
        Booking.user_id == user_id
    ).order_by(desc(Slot.start_time)) # DESC: Latest/Future first usually preferred if mixed with history
    
    result = await session.execute(q)
    return result.scalars().all()

async def get_bookings_for_day(
    session: AsyncSession,
    target_date: date
) -> list[Booking]:
    """
    Returns ACTIVE bookings for a specific day, joined with Slot and User.
    Sorted by Slot.location_code, Slot.start_time.
    For Admin usage.
    """
    start = datetime.combine(target_date, datetime.min.time())
    end = start + timedelta(days=1)
    
    q = (
        select(Booking)
        .join(Slot)
        .options(
            joinedload(Booking.slot),
            joinedload(Booking.user)
        )
        .where(
            Slot.start_time >= start,
            Slot.start_time < end,
            Booking.status == "active"
        )
        .order_by(Slot.location_code, Slot.start_time)
    )
    
    result = await session.execute(q)
    return result.scalars().unique().all()


async def get_user_bookings_admin(session: AsyncSession, user_id: int) -> list[Booking]:
    """
    Returns ALL user bookings (active + canceled) for Admin.
    Eager loads Slot AND User (to avoid MissingGreenlet in admin views).
    sorted by start_time DESC.
    """
    q = (
        select(Booking)
        .join(Slot)
        .options(
            joinedload(Booking.slot),
            joinedload(Booking.user)
        )
        .where(Booking.user_id == user_id)
        .order_by(desc(Slot.start_time))
    )
    
    result = await session.execute(q)
    return result.scalars().unique().all()
