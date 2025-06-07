import sys
import os
import asyncio
import aiohttp
from loguru import logger
from settings import TG_CHAT_ID, TG_API

logger.remove()
logger.add(sys.stderr, format="<green>{time:MM-DD HH:mm:ss}</green> | <level>{message}</level>")

LOG_FILE = './database/logs.log'
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logger.add(
    LOG_FILE,
    rotation="100 MB",
    retention="30 days",
    level="DEBUG",
    format="{time:DD-MM HH:mm:ss} | {level: <8} | {message}"
)

if not TG_API or not TG_CHAT_ID:
    logger.warning('Logger | Telegram data is not filled, you will not get any notifications')


async def send_telegram(message: str):
    if TG_API and TG_CHAT_ID:
        async with aiohttp.ClientSession() as session:
            for i in range(3):
                try:
                    texts = []
                    while len(message) > 0:
                        texts.append(message[:1900])
                        message = message[1900:]

                    for text in texts:
                        async with session.post(
                            f'https://api.telegram.org/bot{TG_API}/sendMessage',
                            json={
                                'chat_id': TG_CHAT_ID,
                                'text': text,
                                'disable_web_page_preview': True
                            }
                        ) as response:
                            if not response.ok:
                                error_text = await response.text()
                                raise Exception(f'Telegram API error: {error_text}')
                    break
                except Exception as e:
                    logger.error(f"Failed to send Telegram message: {e}")
                    await asyncio.sleep(60)


async def info(message: str, telegram: bool = True):
    logger.info(message)
    if telegram:
        telegram_message = message.lstrip('\n')
        await send_telegram(f"ℹ️ {telegram_message}")


async def success(message: str, telegram: bool = True):
    logger.success(message)
    if telegram:
        telegram_message = message.lstrip('\n')
        await send_telegram(f"✅ {telegram_message}")


async def warning(message: str, telegram: bool = True):
    logger.warning(message)
    if telegram:
        telegram_message = message.lstrip('\n')
        await send_telegram(f"⚠️ {telegram_message}")


async def error(message: str, telegram: bool = True):
    logger.error(message)
    if telegram:
        telegram_message = message.lstrip('\n')
        await send_telegram(f"❌ {telegram_message}")


async def debug(message: str, telegram: bool = True):
    logger.debug(message)
    if telegram:
        telegram_message = message.lstrip('\n')
        await send_telegram(f"⚙️ {telegram_message}")
