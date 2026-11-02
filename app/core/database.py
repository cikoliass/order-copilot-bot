import json
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import DateTime, Integer, String, Text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from app.core.config import settings

engine = create_async_engine(settings.DATABASE_URL, echo=False)
async_session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


class Base(DeclarativeBase):
    pass


class OrderRecord(Base):
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_user_id: Mapped[str] = mapped_column(String(64), index=True)
    raw_text: Mapped[str] = mapped_column(Text)
    delivery_date: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    items_json: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), default="DRAFT")
    document_number_1c: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


async def init_db() -> None:
    """Создает таблицы в БД при старте бота."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def save_confirmed_order(
    telegram_user_id: str,
    raw_text: str,
    delivery_date: Optional[str],
    items_data: list[dict],
    document_number_1c: str,
    state_1c: str,
) -> OrderRecord:
    """Сохраняет выгруженный в 1С заказ в локальную БД."""
    async with async_session_factory() as session:
        record = OrderRecord(
            telegram_user_id=str(telegram_user_id),
            raw_text=raw_text,
            delivery_date=delivery_date,
            items_json=json.dumps(items_data, ensure_ascii=False),
            status=state_1c,
            document_number_1c=document_number_1c,
        )
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record