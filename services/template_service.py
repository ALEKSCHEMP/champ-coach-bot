import logging
from datetime import date, datetime, timedelta
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import SlotTemplate, Slot

logger = logging.getLogger(__name__)

def _get_monday_date(target_date: date) -> date:
    return target_date - timedelta(days=target_date.weekday())

async def get_templates(session: AsyncSession) -> list[SlotTemplate]:
    q = select(SlotTemplate).order_by(SlotTemplate.location_code, SlotTemplate.weekday, SlotTemplate.window_start)
    result = await session.execute(q)
    return result.scalars().all()

async def create_template(
    session: AsyncSession,
    location_code: str,
    weekday: int,
    window_start: str,
    window_end: str,
    step_minutes: int,
    duration_minutes: int,
    capacity: int
) -> SlotTemplate:
    tmpl = SlotTemplate(
        location_code=location_code,
        weekday=weekday,
        window_start=window_start,
        window_end=window_end,
        step_minutes=step_minutes,
        duration_minutes=duration_minutes,
        capacity=capacity,
        is_active=True
    )
    session.add(tmpl)
    await session.commit()
    logger.info("template_created", extra={"template_id": tmpl.id, "location": location_code, "weekday": weekday})
    return tmpl

async def delete_template(session: AsyncSession, template_id: int) -> bool:
    tmpl = await session.get(SlotTemplate, template_id)
    if not tmpl:
        return False
    await session.delete(tmpl)
    await session.commit()
    logger.info("template_deleted", extra={"template_id": template_id})
    return True

async def toggle_template(session: AsyncSession, template_id: int) -> bool:
    tmpl = await session.get(SlotTemplate, template_id)
    if not tmpl:
        return False
    tmpl.is_active = not tmpl.is_active
    await session.commit()
    return True

async def generate_week_slots(session: AsyncSession, target_date: date) -> tuple[int, int]:
    """Generates slots for the Monday-Sunday week containing target_date."""
    monday = _get_monday_date(target_date)
    
    # Pre-fetch all active templates
    q = select(SlotTemplate).where(SlotTemplate.is_active == True)
    active_templates = (await session.execute(q)).scalars().all()
    
    # Pre-fetch existing slots for the whole week to check for duplicates quickly in memory
    week_start_dt = datetime.combine(monday, datetime.min.time())
    week_end_dt = week_start_dt + timedelta(days=7)
    
    slots_q = select(Slot.location_code, Slot.start_time).where(
        and_(Slot.start_time >= week_start_dt, Slot.start_time < week_end_dt)
    )
    existing_slots_raw = (await session.execute(slots_q)).all()
    existing_set = set((loc, st) for loc, st in existing_slots_raw)
    
    created_count = 0
    skipped_count = 0
    new_slots = []
    
    for i in range(7):
        current_date = monday + timedelta(days=i)
        current_weekday = current_date.weekday()
        
        day_templates = [t for t in active_templates if t.weekday == current_weekday]
        
        for tmpl in day_templates:
            # Parse times
            start_hour, start_min = map(int, tmpl.window_start.split(':'))
            end_hour, end_min = map(int, tmpl.window_end.split(':'))
            
            cursor_dt = datetime.combine(current_date, datetime.min.time()) + timedelta(hours=start_hour, minutes=start_min)
            end_dt = datetime.combine(current_date, datetime.min.time()) + timedelta(hours=end_hour, minutes=end_min)
            
            while cursor_dt + timedelta(minutes=tmpl.duration_minutes) <= end_dt:
                if (tmpl.location_code, cursor_dt) in existing_set:
                    skipped_count += 1
                else:
                    new_slots.append(
                        Slot(
                            location_code=tmpl.location_code,
                            start_time=cursor_dt,
                            end_time=cursor_dt + timedelta(minutes=tmpl.duration_minutes),
                            status="free",
                            capacity=tmpl.capacity,
                            booked_count=0
                        )
                    )
                    existing_set.add((tmpl.location_code, cursor_dt))
                    created_count += 1
                
                cursor_dt += timedelta(minutes=tmpl.step_minutes)
                
    if new_slots:
        session.add_all(new_slots)
        await session.commit()
    
    logger.info("week_slots_generated", extra={"created_count": created_count, "skipped_count": skipped_count, "target_date": target_date.isoformat()})
    return created_count, skipped_count

async def calculate_templates_from_week(session: AsyncSession, target_date: date) -> list[SlotTemplate]:
    monday = _get_monday_date(target_date)
    week_start_dt = datetime.combine(monday, datetime.min.time())
    week_end_dt = week_start_dt + timedelta(days=7)
    
    q = select(Slot).where(
        and_(Slot.start_time >= week_start_dt, Slot.start_time < week_end_dt)
    ).order_by(Slot.location_code, Slot.start_time)
    
    slots = (await session.execute(q)).scalars().all()
    
    if not slots:
        return []
        
    templates = []
    current_group = []
    
    def finalize_group(grp: list[Slot]):
        if not grp: return
        first = grp[0]
        last = grp[-1]
        
        duration = int((first.end_time - first.start_time).total_seconds() / 60)
        
        if len(grp) > 1:
            step = int((grp[1].start_time - first.start_time).total_seconds() / 60)
        else:
            step = 30 # Default safe step
            
        weekday = first.start_time.weekday()
        w_start = first.start_time.strftime("%H:%M")
        w_end = last.end_time.strftime("%H:%M")
        
        templates.append(SlotTemplate(
            location_code=first.location_code,
            weekday=weekday,
            window_start=w_start,
            window_end=w_end,
            step_minutes=step,
            duration_minutes=duration,
            capacity=first.capacity,
            is_active=True
        ))

    for slot in slots:
        if not current_group:
            current_group.append(slot)
            continue
            
        prev = current_group[-1]
        duration = int((slot.end_time - slot.start_time).total_seconds() / 60)
        prev_duration = int((prev.end_time - prev.start_time).total_seconds() / 60)
        
        # Check if it belongs to the same group
        same_loc = slot.location_code == prev.location_code
        same_day = slot.start_time.weekday() == prev.start_time.weekday()
        same_cap = slot.capacity == prev.capacity
        same_dur = duration == prev_duration
        
        # Consistent step check
        if len(current_group) == 1:
            # We can tentatively accept any step as long as it's > 0
            # But let's verify there is no massive gap (e.g. morning to evening)
            # A completely safe logic is to just take it as the series step.
            valid_next = True
        else:
            inferred_step = int((current_group[1].start_time - current_group[0].start_time).total_seconds() / 60)
            actual_diff = int((slot.start_time - prev.start_time).total_seconds() / 60)
            valid_next = (actual_diff == inferred_step)
            
        if same_loc and same_day and same_cap and same_dur and valid_next:
            current_group.append(slot)
        else:
            finalize_group(current_group)
            current_group = [slot]
            
    finalize_group(current_group)
    return templates

async def save_imported_templates(session: AsyncSession, templates: list[SlotTemplate], replace_mode: bool) -> int:
    saved = 0
    if replace_mode and templates:
        # Instead of deleting all, delete only the overlapping templates (same location + weekday)
        affected_locs_days = set((t.location_code, t.weekday) for t in templates)
        
        all_existing = (await session.execute(select(SlotTemplate))).scalars().all()
        for ext in all_existing:
            if (ext.location_code, ext.weekday) in affected_locs_days:
                await session.delete(ext)
                
        await session.flush()
        
    else:
        # In Add mode, pre-fetch existing to avoid duplicates
        all_existing = (await session.execute(select(SlotTemplate))).scalars().all()
        existing_signatures = set(
            (t.location_code, t.weekday, t.window_start, t.window_end, t.step_minutes, t.duration_minutes, t.capacity)
            for t in all_existing
        )
        
        # Filter duplicates
        unique_templates = []
        for t in templates:
            sig = (t.location_code, t.weekday, t.window_start, t.window_end, t.step_minutes, t.duration_minutes, t.capacity)
            if sig not in existing_signatures:
                unique_templates.append(t)
                existing_signatures.add(sig)
        
        templates = unique_templates

    if templates:
        session.add_all(templates)
        saved = len(templates)
        await session.commit()
        logger.info("templates_imported", extra={"saved_count": saved, "replace_mode": replace_mode})
        
    return saved
