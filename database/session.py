from sqlalchemy.ext.asyncio import AsyncSession
from .engine import async_session_maker

# Dependency injection style helper or context manager usage
# In this bot, we are largely using context managers manually:
# async with SessionLocal() as session: ...

SessionLocal = async_session_maker
