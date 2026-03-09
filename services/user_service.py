from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import User

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
