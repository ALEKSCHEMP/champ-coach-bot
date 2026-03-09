import asyncio
import os
from datetime import datetime, timedelta, date

from database.engine import init_db, async_session_maker
from database.models import Slot
from services.user_service import get_or_create_user
from services.booking_service import create_booking, get_user_bookings, cancel_booking

async def main():
    print("--- 1. Init DB ---")
    await init_db()
    print("✅ DB Initialized")

    async with async_session_maker() as session:
        print("\n--- 2. Create User ---")
        tg_id = 999888777
        user = await get_or_create_user(session, tg_id, "verify_user", "Verify User")
        print(f"✅ User: {user.id} | {user.telegram_id}")

        print("\n--- 3. Create Slots (2 slots) ---")
        start_1 = datetime.now() + timedelta(days=1, hours=10) # Tomorrow 10:00
        start_2 = datetime.now() + timedelta(days=1, hours=11) # Tomorrow 11:00
        
        slot1 = Slot(location_code="TEST_LOC", start_time=start_1, end_time=start_1+timedelta(hours=1), status="free")
        slot2 = Slot(location_code="TEST_LOC", start_time=start_2, end_time=start_2+timedelta(hours=1), status="free")
        
        session.add_all([slot1, slot2])
        await session.commit()
        await session.refresh(slot1)
        await session.refresh(slot2)
        print(f"✅ Slots created: IDs {slot1.id}, {slot2.id}")

        print("\n--- 4. Book 2 Slots ---")
        print(f"DEBUG: Slot 1 status before book: {slot1.status} (ID: {slot1.id})")
        b1, msg1 = await create_booking(session, user.id, slot1.id)
        if b1: print(f"✅ Booking 1: {b1.id} for Slot {slot1.id}")
        else: print(f"❌ Booking 1 Failed: {msg1}")

        b2, msg2 = await create_booking(session, user.id, slot2.id)
        if b2: print(f"✅ Booking 2: {b2.id} for Slot {slot2.id}")
        else: print(f"❌ Booking 2 Failed: {msg2}")

        print("\n--- 5. Attempt Double Book Slot 1 (Atomicity Check) ---")
        b3, msg3 = await create_booking(session, user.id, slot1.id)
        if b3: print(f"❌ FAILURE: Double booking succeeded! {b3.id}")
        else: print(f"✅ Atomicity worked: {msg3}")

        print("\n--- 6. List All Bookings ---")
        all_bookings = await get_user_bookings(session, user.id)
        for b in all_bookings:
            print(f"📌 {b.id} | Status: {b.status} | Time: {b.slot.start_time}")
        
        if len(all_bookings) < 2:
            print("❌ FAILURE: Expected 2 bookings")

        print("\n--- 7. Cancel Booking 1 ---")
        await cancel_booking(session, b1.id, user_telegram_id=tg_id)
        
        # Verify slot release
        await session.refresh(slot1)
        print(f"🔍 Slot 1 status: {slot1.status} (Expected 'free')")
        
        print("\n--- 8. List Again (Verify Status) ---")
        all_bookings_final = await get_user_bookings(session, user.id)
        for b in all_bookings_final:
            cancel_marker = "❌" if b.status == 'canceled' else "✅"
            print(f"{cancel_marker} {b.id} | Status: {b.status}")

        if all_bookings_final[0].status == 'active' and all_bookings_final[1].status == 'canceled':
            # Note: sort is DESC time. Slot 2 is 11:00, Slot 1 is 10:00.
            # Slot 2 is active, Slot 1 is cancelled.
            # Order DESC: Slot 2 (Active), Slot 1 (Canceled).
            print("✅ Order & Status Verified")
        else:
             print(f"⚠️ Check output carefully (Order: {[b.status for b in all_bookings_final]})")

if __name__ == "__main__":
    asyncio.run(main())
