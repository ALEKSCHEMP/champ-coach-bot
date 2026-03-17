from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, DateTime, ForeignKey, Boolean, UniqueConstraint
from datetime import datetime

class Base(DeclarativeBase):
    pass

class Location(Base):
    __tablename__ = "locations"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)
    address: Mapped[str | None] = mapped_column(String(200), nullable=True)

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    telegram_id: Mapped[int] = mapped_column(Integer, unique=True, index=True)
    username: Mapped[str | None] = mapped_column(String, nullable=True)
    full_name: Mapped[str | None] = mapped_column(String, nullable=True)
    role: Mapped[str] = mapped_column(String, default="user") # user, admin
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    bookings: Mapped[list["Booking"]] = relationship("Booking", back_populates="user")

class Slot(Base):
    __tablename__ = "slots"
    __table_args__ = (
        UniqueConstraint("location_code", "start_time", name="uq_slots_location_start"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_code: Mapped[str] = mapped_column(String(20))
    start_time: Mapped[datetime] = mapped_column(DateTime) # Stores date + time
    end_time: Mapped[datetime] = mapped_column(DateTime)
    status: Mapped[str] = mapped_column(String, default="free") # free, booked
    
    capacity: Mapped[int] = mapped_column(Integer, default=1)
    booked_count: Mapped[int] = mapped_column(Integer, default=0)

    bookings: Mapped[list["Booking"]] = relationship("Booking", back_populates="slot")

class Booking(Base):
    __tablename__ = "bookings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), index=True)
    slot_id: Mapped[int] = mapped_column(ForeignKey("slots.id"), index=True) # Changed: Remove unique=True
    
    # Snapshot fields (optional but requested/good practice)
    booking_date: Mapped[datetime] = mapped_column(DateTime) 
    location: Mapped[str] = mapped_column(String)
    
    status: Mapped[str] = mapped_column(String, default="active") # active, canceled
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    reminder_24h_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    reminder_morning_sent: Mapped[bool] = mapped_column(Boolean, default=False)
    reminder_day_sent: Mapped[bool] = mapped_column(Boolean, default=False)

    client_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    confirmation_status: Mapped[str] = mapped_column(String, default="pending")
   
    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="bookings")
    slot: Mapped["Slot"] = relationship("Slot", back_populates="bookings")

class SlotTemplate(Base):
    __tablename__ = "slot_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location_code: Mapped[str] = mapped_column(String(20))
    weekday: Mapped[int] = mapped_column(Integer) # 0=Mon, 6=Sun
    window_start: Mapped[str] = mapped_column(String(5)) # HH:MM
    window_end: Mapped[str] = mapped_column(String(5)) # HH:MM
    step_minutes: Mapped[int] = mapped_column(Integer, default=30)
    duration_minutes: Mapped[int] = mapped_column(Integer, default=60)
    capacity: Mapped[int] = mapped_column(Integer, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
