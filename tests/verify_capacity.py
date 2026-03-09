import asyncio
import os
import sys
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.getcwd())

from database.models import Base, Slot, User, Booking
from services.booking_service import create_booking, cancel_booking
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import select

# Use a test DB
TEST_DB_URL = "sqlite+aiosqlite:///./test_capacity.db"

async def test_atomic_capacity():
    engine = create_async_engine(TEST_DB_URL, echo=False)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    print("--- Setting up Test DB ---")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    # 1. Create a slot with capacity 2
    async with async_session() as session:
        slot = Slot(
            location_code="TEST",
            start_time=datetime.now() + timedelta(hours=1),
            end_time=datetime.now() + timedelta(hours=2),
            capacity=2,
            booked_count=0,
            status="free"
        )
        session.add(slot)
        await session.commit()
        slot_id = slot.id
        print(f"Created Slot {slot_id} with capacity 2")

    # 2. Create 3 users
    async with async_session() as session:
        for i in range(1, 4):
            u = User(telegram_id=100+i, full_name=f"User {i}", username=f"user{i}")
            session.add(u)
        await session.commit()

    print("--- Testing Concurrent Bookings ---")

    async def try_book(user_id):
        async with async_session() as session:
            booking, msg = await create_booking(session, user_id, slot_id)
            return booking, msg

    # Run 3 bookings concurrently
    results = await asyncio.gather(
        try_book(1),
        try_book(2),
        try_book(3)
    )

    success_count = 0
    for i, (b, msg) in enumerate(results):
        status = "✅ Success" if b else f"❌ Failed ({msg})"
        print(f"User {i+1} booking attempt: {status}")
        if b:
            success_count += 1

    print(f"Total successful bookings: {success_count}/3")
    assert success_count == 2, f"Expected 2 bookings, got {success_count}"

    # Verify DB state
    async with async_session() as session:
        slot = (await session.execute(select(Slot).where(Slot.id == slot_id))).scalar_one()
        print(f"Slot state: booked_count={slot.booked_count}/{slot.capacity}")
        assert slot.booked_count == 2

    print("--- Testing Cancellation ---")
    # Cancel one booking
    async with async_session() as session:
        bookings = (await session.execute(select(Booking).where(Booking.slot_id == slot_id))).scalars().all()
        b_to_cancel = bookings[0]
        print(f"Cancelling booking {b_to_cancel.id} for User {b_to_cancel.user_id}")

        # user_id=1 has telegram_id=101, etc.
        success, msg = await cancel_booking(
            session,
            b_to_cancel.id,
            user_telegram_id=100 + b_to_cancel.user_id
        )
        print(f"Cancel result: {success} - {msg}")
        assert success

    # Verify slot count decreased
    async with async_session() as session:
        slot = (await session.execute(select(Slot).where(Slot.id == slot_id))).scalar_one()
        print(f"Slot state after cancel: booked_count={slot.booked_count}/{slot.capacity}")
        assert slot.booked_count == 1

    print("--- Testing Re-booking ---")
    # Create new user 4
    async with async_session() as session:
        u4 = User(telegram_id=104, full_name="User 4", username="user4")
        session.add(u4)
        await session.commit()
        
    b, msg = await try_book(4)
    print(f"User 4 retry: {'✅ Success' if b else '❌ Failed'} - {msg}")
    assert b is not None

    async with async_session() as session:
        slot = (await session.execute(select(Slot).where(Slot.id == slot_id))).scalar_one()
        print(f"Final Slot state: booked_count={slot.booked_count}/{slot.capacity}")
        assert slot.booked_count == 2

    print("ALL TESTS PASSED ✅")

if __name__ == "__main__":
    asyncio.run(test_atomic_capacity())
