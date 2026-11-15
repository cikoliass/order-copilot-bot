import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from app.core.config import settings

logger = logging.getLogger(__name__)

connect_args = {"timeout": 30} if "sqlite" in settings.DATABASE_URL else {}
engine = create_async_engine(settings.DATABASE_URL, echo=False, connect_args=connect_args)
async_session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


class Base(DeclarativeBase):
    pass


class ClientBinding(Base):
    """Справочник авторизованных контрагентов HoReCa (привязка мессенджера к ИНН и карточке 1С)."""
    __tablename__ = "client_bindings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform: Mapped[str] = mapped_column(String(32), index=True)
    platform_user_id: Mapped[str] = mapped_column(String(128), index=True)
    counterparty_name: Mapped[str] = mapped_column(String(255), default="")
    inn: Mapped[str] = mapped_column(String(12), index=True, default="")
    counterparty_1c_id: Mapped[str] = mapped_column(String(64), default="")
    b24_company_id: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class OrderRecord(Base):
    """Журнал оформленных заказов клиентов во всех каналах."""
    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    session_id: Mapped[str] = mapped_column(String(128), index=True, default="")
    platform: Mapped[str] = mapped_column(String(32), index=True, default="telegram")
    platform_user_id: Mapped[str] = mapped_column(String(128), index=True, default="")
    telegram_user_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, default=None)
    raw_text: Mapped[str] = mapped_column(Text)
    delivery_date: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    items_json: Mapped[str] = mapped_column(Text)
    total_amount: Mapped[float] = mapped_column(Float, default=0.0)
    status: Mapped[str] = mapped_column(String(255), default="CONFIRMED")
    document_number_1c: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )


class OrderOutbox(Base):
    """Транзакционный Outbox для гарантированной доставки заказов в 1С и Битрикс24 при сбоях сети."""
    __tablename__ = "order_outbox"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    session_id: Mapped[str] = mapped_column(String(128), index=True)
    destination: Mapped[str] = mapped_column(String(32), default="ALL")  # 1C, BITRIX24, ALL
    payload_json: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(32), default="PENDING", index=True)  # PENDING, PROCESSING, SENT, FAILED
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, default=10)
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )
    next_retry_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        index=True,
    )


async def init_db() -> None:
    """Инициализация таблиц базы данных и безопасная миграция колонок."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

        # Автоматическая миграция для SQLite при наличии старой структуры таблицы orders
        def _migrate_sqlite_schema(sync_conn):
            if sync_conn.dialect.name == "sqlite":
                result = sync_conn.execute(text("PRAGMA table_info(orders)"))
                existing_columns = {row[1] for row in result.fetchall()}
                if existing_columns:
                    if "session_id" not in existing_columns:
                        sync_conn.execute(text("ALTER TABLE orders ADD COLUMN session_id VARCHAR(128) DEFAULT ''"))
                    if "platform" not in existing_columns:
                        sync_conn.execute(text("ALTER TABLE orders ADD COLUMN platform VARCHAR(32) DEFAULT 'telegram'"))
                    if "platform_user_id" not in existing_columns:
                        sync_conn.execute(text("ALTER TABLE orders ADD COLUMN platform_user_id VARCHAR(128) DEFAULT ''"))
                    if "telegram_user_id" not in existing_columns:
                        sync_conn.execute(text("ALTER TABLE orders ADD COLUMN telegram_user_id VARCHAR(64) DEFAULT NULL"))
                    if "total_amount" not in existing_columns:
                        sync_conn.execute(text("ALTER TABLE orders ADD COLUMN total_amount FLOAT DEFAULT 0.0"))

        await conn.run_sync(_migrate_sqlite_schema)


async def get_client_binding(platform: str, platform_user_id: str) -> Optional[ClientBinding]:
    """Поиск привязки B2B-контрагента по платформе и ID пользователя."""
    async with async_session_factory() as session:
        stmt = select(ClientBinding).where(
            ClientBinding.platform == platform,
            ClientBinding.platform_user_id == str(platform_user_id),
            ClientBinding.is_active == True,
        )
        result = await session.execute(stmt)
        return result.scalars().first()


async def save_order(
    session_id: str,
    platform: str,
    platform_user_id: str,
    raw_text: str,
    delivery_date: Optional[str],
    items_data: list[dict[str, Any]],
    total_amount: float,
    status: str,
    document_number_1c: Optional[str] = None,
) -> OrderRecord:
    """Сохранение зарегистрированного заказа."""
    async with async_session_factory() as session:
        record = OrderRecord(
            session_id=session_id,
            platform=platform,
            platform_user_id=platform_user_id,
            # Сохраняем telegram_user_id СТРОГО для платформы telegram, избегая загрязнения схемы чужими ID
            telegram_user_id=platform_user_id if platform == "telegram" else None,
            raw_text=raw_text or "",
            delivery_date=delivery_date,
            items_json=json.dumps(items_data, ensure_ascii=False),
            total_amount=total_amount,
            status=status,
            document_number_1c=document_number_1c,
        )
        session.add(record)
        await session.commit()
        await session.refresh(record)
        return record


async def create_outbox_entry(
    session_id: str,
    destination: str,
    payload: dict[str, Any],
    order_id: Optional[int] = None,
    last_error: Optional[str] = None,
) -> OrderOutbox:
    """Добавление заказа в транзакционную очередь Outbox при недоступности учетных систем."""
    async with async_session_factory() as session:
        entry = OrderOutbox(
            order_id=order_id,
            session_id=session_id,
            destination=destination,
            payload_json=json.dumps(payload, ensure_ascii=False),
            status="PENDING",
            retry_count=0,
            last_error=last_error,
            next_retry_at=datetime.now(timezone.utc),
        )
        session.add(entry)
        await session.commit()
        await session.refresh(entry)
        return entry


async def get_pending_outbox_entries(limit: int = 50) -> list[OrderOutbox]:
    """Получение ожидающих отправки задач Outbox, готовых к повтору."""
    now = datetime.now(timezone.utc)
    async with async_session_factory() as session:
        stmt = (
            select(OrderOutbox)
            .where(
                (
                    OrderOutbox.status.in_(["PENDING", "FAILED"])
                    | (OrderOutbox.status == "PROCESSING")
                ),
                OrderOutbox.retry_count < OrderOutbox.max_retries,
                OrderOutbox.next_retry_at <= now,
            )
            .order_by(OrderOutbox.id.asc())
            .limit(limit)
        )
        result = await session.execute(stmt)
        return list(result.scalars().all())


async def claim_outbox_entries(limit: int = 20) -> list[OrderOutbox]:
    """
    Атомарный захват задач Outbox с переводом в статус PROCESSING для исключения дублирования между воркерами.
    Устанавливает таймаут аренды (lease timeout) на 5 минут в next_retry_at на случай сбоя или перезапуска воркера.
    """
    now = datetime.now(timezone.utc)
    lease_until = datetime.fromtimestamp(now.timestamp() + 300, tz=timezone.utc)
    async with async_session_factory() as session:
        stmt = (
            select(OrderOutbox)
            .where(
                (
                    OrderOutbox.status.in_(["PENDING", "FAILED"])
                    | (OrderOutbox.status == "PROCESSING")
                ),
                OrderOutbox.retry_count < OrderOutbox.max_retries,
                OrderOutbox.next_retry_at <= now,
            )
            .order_by(OrderOutbox.id.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await session.execute(stmt)
        entries = list(result.scalars().all())
        if entries:
            for entry in entries:
                entry.status = "PROCESSING"
                entry.next_retry_at = lease_until
                entry.updated_at = now
            await session.commit()
            for entry in entries:
                await session.refresh(entry)
        return entries


async def mark_outbox_success(entry_id: int) -> None:
    """Пометка задачи Outbox как успешно выполненной."""
    async with async_session_factory() as session:
        entry = await session.get(OrderOutbox, entry_id)
        if entry:
            entry.status = "SENT"
            entry.updated_at = datetime.now(timezone.utc)
            await session.commit()


async def update_order_doc_number(
    order_id: int,
    document_number_1c: str,
    status: str = "Заказ успешно создан (1С + Битрикс24)",
) -> None:
    """Обновление номера документа и статуса после успешной отправки из Outbox."""
    async with async_session_factory() as session:
        order = await session.get(OrderRecord, order_id)
        if order:
            order.document_number_1c = document_number_1c
            order.status = status
            await session.commit()


async def mark_outbox_failure(
    entry_id: int,
    error_message: str,
    backoff_seconds: int = 60,
    updated_payload: Optional[dict[str, Any]] = None,
) -> None:
    """Пометка задачи Outbox как требующей повтора с увеличением задержки и сохранением прогресса."""
    async with async_session_factory() as session:
        entry = await session.get(OrderOutbox, entry_id)
        if entry:
            entry.retry_count += 1
            entry.last_error = error_message
            if updated_payload is not None:
                entry.payload_json = json.dumps(updated_payload, ensure_ascii=False)
            if entry.retry_count >= entry.max_retries:
                entry.status = "FAILED"
            else:
                entry.status = "PENDING"
            next_ts = datetime.now(timezone.utc).timestamp() + (backoff_seconds * (2 ** min(entry.retry_count, 5)))
            entry.next_retry_at = datetime.fromtimestamp(next_ts, tz=timezone.utc)
            entry.updated_at = datetime.now(timezone.utc)
            await session.commit()


async def get_user_order_history(session_id: str, limit: int = 50) -> list[list[dict[str, Any]]]:
    """Извлечение товарных составов прошлых заказов пользователя для предиктивного модуля."""
    try:
        async with async_session_factory() as session:
            stmt = (
                select(OrderRecord.items_json)
                .where(OrderRecord.session_id == session_id)
                .order_by(OrderRecord.id.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            history: list[list[dict[str, Any]]] = []
            for raw in rows:
                if not raw:
                    continue
                try:
                    parsed = json.loads(raw)
                    if isinstance(parsed, list) and parsed:
                        history.append(parsed)
                except Exception:
                    continue
            return history
    except Exception as exc:
        logger.warning("Не удалось прочитать историю заказов: %s", exc)
        return []
