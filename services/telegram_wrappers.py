import asyncio
import logging
import aiohttp
from typing import Any
from aiogram import Bot
from aiogram.types import Message, CallbackQuery
from aiogram.exceptions import TelegramNetworkError, TelegramBadRequest

logger = logging.getLogger(__name__)

async def _retry_telegram_call(action_name: str, func, *args, **kwargs) -> Any:
    delays = [0, 1, 2]
    logger.info(f"telegram_{action_name}_started")
    
    last_exception = None
    for attempt, delay in enumerate(delays, start=1):
        if attempt > 1:
            logger.warning(f"telegram_{action_name}_retry", extra={"attempt": attempt, "error": str(last_exception)})
            await asyncio.sleep(delay)
            
        try:
            res = await func(*args, **kwargs)
            logger.info(f"telegram_{action_name}_succeeded")
            return res
        except TelegramBadRequest as e:
            if "message is not modified" in str(e).lower():
                logger.info("telegram_edit_skipped_not_modified")
                return None
            logger.exception(f"telegram_{action_name}_failed", exc_info=e)
            return None
        except (TelegramNetworkError, aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_exception = e
        except Exception as e:
            logger.exception(f"telegram_{action_name}_failed", exc_info=e)
            return None
            
    if last_exception:
        logger.exception(f"telegram_{action_name}_failed", exc_info=last_exception)
    return None

async def safe_send_message(bot: Bot, chat_id: int | str, text: str, **kwargs) -> Message | None:
    return await _retry_telegram_call("send", bot.send_message, chat_id=chat_id, text=text, **kwargs)

async def safe_answer_message(message: Message, text: str, **kwargs) -> Message | None:
    return await _retry_telegram_call("send", message.answer, text=text, **kwargs)

async def safe_edit_text(message: Message, text: str, **kwargs) -> Message | bool | None:
    return await _retry_telegram_call("edit", message.edit_text, text=text, **kwargs)

async def safe_edit_reply_markup(message: Message, **kwargs) -> Message | bool | None:
    return await _retry_telegram_call("edit", message.edit_reply_markup, **kwargs)

async def safe_callback_answer(callback: CallbackQuery, text: str | None = None, **kwargs) -> bool | None:
    return await _retry_telegram_call("callback_answer", callback.answer, text=text, **kwargs)
