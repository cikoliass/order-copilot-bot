import os
import asyncio
import logging
from aiogram import Bot, Dispatcher
from dotenv import load_dotenv

from app.bot.handlers.order import router as order_router
from app.core.database import init_db
from app.services.bitrix_sync import sync_catalog_from_bitrix
from app.services.matcher import catalog_matcher

load_dotenv(override=True)


async def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    bot_token = os.getenv("BOT_TOKEN")
    if not bot_token:
        raise ValueError("Не задан BOT_TOKEN в файле .env")

    # Инициализируем SQLite базу заказов
    await init_db()

    # Автоматически загружаем каталог товаров из Битрикс24 при запуске
    logging.info("🔄 Синхронизация каталога товаров с Битрикс24...")
    try:
        items = await sync_catalog_from_bitrix()
        if items:
            catalog_matcher.reload(items)
            logging.info("✅ Загружено %d товаров из Битрикс24!", len(items))
        else:
            logging.warning("⚠️ Товары из Битрикс24 не получены, используем локальный кэш (если есть).")
    except Exception as exc:
        logging.warning("⚠️ Не удалось синхронизировать Битрикс24 на старте: %s", exc)

    bot = Bot(token=bot_token)
    dp = Dispatcher()
    dp.include_router(order_router)

    logging.info("🚀 Бот запущен и готов к работе!")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())