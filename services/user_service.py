from sqlalchemy import select, func, desc, or_
from sqlalchemy.orm import joinedload
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import User, Booking, Slot
from datetime import datetime

async def get_or_create_user(
    session: AsyncSession, 
    telegram_id: int, 
    username: str | None = None, 
    full_name: str | None = None
) -> User:
    q = select(User).where(User.telegram_id == telegram_id)
    result = await session.execute(q)
    user = result.scalar_one_or_none()

    if not user:
        user = User(
            telegram_id=telegram_id,
            username=username,
            full_name=full_name,
            role="user"
        )
        session.add(user)
        # We don't commit here usually to allow chaining, but for this simple helper it acts as an atomic get_or_create
        await session.commit()
        await session.refresh(user)
    
    return user

async def get_user(session: AsyncSession, telegram_id: int) -> User | None:
    q = select(User).where(User.telegram_id == telegram_id)
    result = await session.execute(q)
    return result.scalar_one_or_none()

async def get_users_page(session: AsyncSession, page: int = 0, per_page: int = 10) -> tuple[list[User], int]:
    count_q = select(func.count(User.id))
    total = (await session.execute(count_q)).scalar()

    subq = select(Booking.user_id, func.max(Booking.booking_date).label('last_booking')).group_by(Booking.user_id).subquery()
    
    q = select(User).outerjoin(subq, User.id == subq.c.user_id).order_by(
        desc(subq.c.last_booking).nulls_last(),
        desc(User.id)
    ).limit(per_page).offset(page * per_page)
    
    result = await session.execute(q)
    return result.scalars().all(), total

async def search_users(session: AsyncSession, query_str: str, limit: int = 50) -> list[User]:
    q_str = f"%{query_str}%"
    conditions = [
        User.full_name.ilike(q_str),
        User.username.ilike(q_str)
    ]
    if query_str.isdigit():
        conditions.append(User.telegram_id == int(query_str))
        
    q = select(User).where(or_(*conditions)).limit(limit)
    result = await session.execute(q)
    return result.scalars().all()

async def get_user_stats(session: AsyncSession, user_id: int) -> dict:
    q = select(Booking).options(joinedload(Booking.slot)).where(Booking.user_id == user_id)
    result = await session.execute(q)
    bookings = result.scalars().all()
    
    total = len(bookings)
    active = [b for b in bookings if b.status == "active"]
    canceled = [b for b in bookings if b.status == "canceled"]
    
    visited_count = sum(1 for b in bookings if getattr(b, "attendance", None) == "visited")
    no_show_count = sum(1 for b in bookings if getattr(b, "attendance", None) == "no_show")
    rescheduled_count = sum(1 for b in bookings if getattr(b, "attendance", None) == "rescheduled")
    
    loc_stats = {}
    for b in active:
        loc_stats[b.location] = loc_stats.get(b.location, 0) + 1
        
    now = datetime.now()
    future = [b for b in active if b.slot and b.slot.start_time and b.slot.start_time >= now]
    past = [b for b in active if b.slot and b.slot.start_time and b.slot.start_time < now]
    
    future.sort(key=lambda b: b.slot.start_time)
    past.sort(key=lambda b: b.slot.start_time, reverse=True)
    
    last_booking_date = max((b.booking_date for b in bookings if b.booking_date), default=None)
    
    return {
        "total": total,
        "active": len(active),
        "canceled": len(canceled),
        "visited_count": visited_count,
        "no_show_count": no_show_count,
        "rescheduled_count": rescheduled_count,
        "last_booking_date": last_booking_date,
        "loc_stats": loc_stats,
        "nearest": future[0] if future else None,
        "last_past": past[0] if past else None
    }

async def get_clients_overall_stats(session: AsyncSession) -> dict:
    from sqlalchemy import select, func
    from datetime import timedelta
    
    total_users = (await session.execute(select(func.count(User.id)))).scalar() or 0
    
    q = select(Booking).options(joinedload(Booking.slot))
    bookings = (await session.execute(q)).scalars().all()
    
    total_bookings = len(bookings)
    active_bookings = sum(1 for b in bookings if b.status == "active")
    canceled_bookings = sum(1 for b in bookings if b.status == "canceled")
    
    visited_count = sum(1 for b in bookings if getattr(b, "attendance", None) == "visited")
    no_show_count = sum(1 for b in bookings if getattr(b, "attendance", None) == "no_show")
    rescheduled_count = sum(1 for b in bookings if getattr(b, "attendance", None) == "rescheduled")
    
    now = datetime.now()
    today_date = now.date()
    tomorrow_date = today_date + timedelta(days=1)
    
    no_attendance_count = 0
    upcoming_today = 0
    upcoming_tomorrow = 0
    loc_stats = {}
    
    for b in bookings:
        if b.status == "active":
            b_time = b.slot.start_time if getattr(b, "slot", None) else b.booking_date
            
            # Count past records that haven't been resolved with attendance
            if b_time < now and getattr(b, "attendance", None) is None:
                no_attendance_count += 1
                
            # Count upcoming records that are active
            if b_time.date() == today_date and b_time >= now:
                upcoming_today += 1
            elif b_time.date() == tomorrow_date:
                upcoming_tomorrow += 1
                
            loc_stats[b.location] = loc_stats.get(b.location, 0) + 1
                
    return {
        "total_users": total_users,
        "total_bookings": total_bookings,
        "active_bookings": active_bookings,
        "canceled_bookings": canceled_bookings,
        "visited_count": visited_count,
        "no_show_count": no_show_count,
        "rescheduled_count": rescheduled_count,
        "no_attendance_count": no_attendance_count,
        "bookings_by_location": loc_stats,
        "upcoming_today": upcoming_today,
        "upcoming_tomorrow": upcoming_tomorrow,
    }

