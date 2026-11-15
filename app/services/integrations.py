import asyncio
import hashlib
import html
import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
import httpx
from pydantic import BaseModel
from app.core.config import settings
from app.core.database import (
    claim_outbox_entries,
    create_outbox_entry,
    get_pending_outbox_entries,
    mark_outbox_failure,
    mark_outbox_success,
    update_order_doc_number,
)
from app.services.dispatcher import dispatcher
from app.services.schemas import (
    MatchedItem,
    OutboundMessage,
    extract_chat_id_from_session,
    extract_platform_from_session,
)

logger = logging.getLogger(__name__)


def _sanitize_error_msg(err: Exception | str) -> str:
    """Удаляет секретные URL вебхуков Битрикс24 и учетные данные из текста ошибок."""
    msg = str(err)
    if settings.BITRIX_WEBHOOK_URL:
        msg = msg.replace(settings.BITRIX_WEBHOOK_URL.rstrip("/"), "https://***BITRIX_REDACTED***")
    if settings.ONEC_PASSWORD:
        msg = msg.replace(settings.ONEC_PASSWORD, "***")
    msg = re.sub(r"https?://[^\s/]+/rest/\d+/[^/\s]+", "https://***/rest/***/***", msg)
    return msg[:500]


def resolve_delivery_date(raw_date: Optional[str]) -> str:
    """Преобразует относительные даты («на завтра», «сегодня») в фиксированную дату с днем недели."""
    if not raw_date:
        return "Как можно скорее"

    import re
    if re.search(r"\(\d{2}\.\d{2}\.\d{4}\)", raw_date):
        return raw_date

    lower = raw_date.lower().strip()
    now = datetime.now()

    if "завтра" in lower:
        target = now + timedelta(days=1)
        return f"{raw_date} ({target.strftime('%d.%m.%Y')})"
    if "сегодня" in lower:
        return f"{raw_date} ({now.strftime('%d.%m.%Y')})"

    weekdays = {
        "понедельник": 0, "вторник": 1, "сред": 2, "четверг": 3,
        "пятниц": 4, "суббот": 5, "воскресень": 6,
    }
    for w_name, w_idx in weekdays.items():
        if w_name in lower:
            days_ahead = (w_idx - now.weekday()) % 7
            if days_ahead == 0:
                days_ahead = 7
            target = now + timedelta(days=days_ahead)
            return f"{raw_date} ({target.strftime('%d.%m.%Y')})"

    return raw_date


class IntegrationResult(BaseModel):
    status: str
    document_number: str
    state: str
    is_outbox: bool = False
    details: str | None = None


class IntegrationsService:
    """Сервис отправки заказов в 1С и CRM Битрикс24 с транзакционным Outbox-фоллбэком."""

    async def _send_to_1c(
        self,
        client_id: str,
        items: list[dict[str, Any]],
        delivery_date: str | None,
        idempotency_key: str | None = None,
    ) -> str:
        """Отправка заказа в HTTP-сервис 1С с поддержкой ключа идемпотентности."""
        payload = {
            "client_id": client_id,
            "delivery_date": delivery_date or "Как можно скорее",
            "items": items,
            "idempotency_key": idempotency_key,
        }
        target_url = settings.MOCK_1C_URL if settings.USE_MOCK_1C else settings.ONEC_URL
        auth = (settings.ONEC_USER, settings.ONEC_PASSWORD) if settings.ONEC_USER and settings.ONEC_PASSWORD else None
        headers = {"X-Idempotency-Key": idempotency_key} if idempotency_key else {}

        async with httpx.AsyncClient(timeout=settings.ONEC_TIMEOUT_SEC) as client:
            resp = await asyncio.wait_for(
                client.post(target_url, json=payload, auth=auth, headers=headers),
                timeout=settings.ONEC_TIMEOUT_SEC,
            )
            if resp.status_code not in (200, 201):
                raise RuntimeError(f"1C вернула HTTP {resp.status_code}: {resp.text}")

            data = resp.json()
            if not (doc_num := data.get("document_number")):
                raise RuntimeError("1С не вернула номер созданного документа")
            return str(doc_num)

    async def _send_to_bitrix24(
        self,
        client_id: str,
        items: list[dict[str, Any]],
        delivery_date: str | None,
        total_amount: float,
    ) -> str:
        """Отправка заказа в CRM Битрикс24 (Создание сделки crm.deal.add + привязка товаров)."""
        webhook_url = settings.BITRIX_WEBHOOK_URL
        if not webhook_url:
            return "Б24-DISABLED"

        lines_text = []
        product_rows = []
        for it in items:
            sku, name, unit = it.get("sku_id", "SKU-000"), it.get("name", "Товар"), it.get("unit", "шт")
            qty, price = float(it.get("qty", 1.0)), float(it.get("price", 0.0))
            lines_text.append(f"• [{html.escape(str(sku))}] <b>{html.escape(str(name))}</b>: {qty:g} {html.escape(str(unit))} × {price:g} ₽ = {qty * price:,.2f} ₽")

            row_data: dict[str, Any] = {"PRODUCT_NAME": f"[{sku}] {name}", "PRICE": price, "QUANTITY": qty}
            if str(sku).startswith("B24-") and str(sku).split("-")[-1].isdigit():
                row_data["PRODUCT_ID"] = int(str(sku).split("-")[-1])
            product_rows.append(row_data)

        comments = (
            f"<b>Клиент (ID/Канал):</b> {html.escape(str(client_id))}<br>"
            f"<b>Дата доставки:</b> {html.escape(str(delivery_date or 'Как можно скорее'))}<br><br>"
            f"<b>Состав заказа:</b><br>" + "<br>".join(lines_text)
        )
        deal_payload = {
            "fields": {
                "TITLE": f"Заказ Co-Pilot от {client_id}",
                "OPPORTUNITY": round(total_amount, 2),
                "CURRENCY_ID": "RUB",
                "COMMENTS": comments,
            }
        }

        async with httpx.AsyncClient(timeout=settings.BITRIX_TIMEOUT_SEC) as client:
            add_url = f"{webhook_url.rstrip('/')}/crm.deal.add.json"
            deal_resp = await asyncio.wait_for(
                client.post(add_url, json=deal_payload),
                timeout=settings.BITRIX_TIMEOUT_SEC,
            )
            deal_data = deal_resp.json()
            deal_id = deal_data.get("result")
            if not deal_id:
                raise RuntimeError(f"Ошибка создания сделки Битрикс24: {deal_data.get('error_description') or deal_data}")

            if product_rows:
                rows_url = f"{webhook_url.rstrip('/')}/crm.deal.productrows.set.json"
                await asyncio.wait_for(
                    client.post(rows_url, json={"id": deal_id, "rows": product_rows}),
                    timeout=settings.BITRIX_TIMEOUT_SEC,
                )

            return f"Б24-{deal_id}"

    async def _dispatch_destinations(
        self,
        client_id: str,
        items: list[dict[str, Any]],
        delivery_date: str | None,
        total_amount: float,
        completed_docs: list[str] | None = None,
        idempotency_key: str | None = None,
    ) -> tuple[list[str], list[str]]:
        docs = list(completed_docs or [])
        errors = []

        if not any(not str(d).startswith("Б24-") for d in docs):
            try:
                doc_1c = await self._send_to_1c(client_id, items, delivery_date, idempotency_key=idempotency_key)
                docs.append(doc_1c)
            except Exception as exc:
                sanitized_err = _sanitize_error_msg(exc)
                logger.warning("Сбой отправки заказа в 1С: %s", sanitized_err)
                errors.append(f"1C: {sanitized_err}")

        if not any("Б24-" in str(d) for d in docs):
            try:
                doc_b24 = await self._send_to_bitrix24(client_id, items, delivery_date, total_amount)
                if doc_b24 != "Б24-DISABLED":
                    docs.append(doc_b24)
            except Exception as exc:
                sanitized_err = _sanitize_error_msg(exc)
                logger.warning("Сбой отправки заказа в Битрикс24: %s", sanitized_err)
                errors.append(f"Битрикс24: {sanitized_err}")

        return docs, errors

    async def send_order(
        self,
        session_id: str,
        valid_items: list[MatchedItem],
        delivery_date: str | None = None,
        raw_text: str = "",
        order_id: Optional[int] = None,
        platform: str = "telegram",
        chat_id: str = "",
        counterparty_1c_id: Optional[str] = None,
    ) -> IntegrationResult:
        """Единая точка выгрузки подтвержденного заказа с Outbox-фоллбэком."""
        resolved_date = resolve_delivery_date(delivery_date)
        clean_date = (resolved_date or "Как можно скорее")[:100]
        clean_client_id = re.sub(r"[\r\n\t]+", " ", str(session_id))[:128].strip()
        items_1c = [
            {
                "sku_id": it.sku_id or "SKU-000",
                "name": it.catalog_name or it.raw_name,
                "qty": it.final_quantity,
                "unit": it.base_unit,
                "price": it.price,
            }
            for it in valid_items
        ]
        total_amount = sum(it.total_sum for it in valid_items)
        client_id = str(counterparty_1c_id).strip() if counterparty_1c_id else clean_client_id

        # Формируем уникальный ключ идемпотентности: включаем order_id, чтобы два разных заказа
        # одного клиента с одинаковым составом на один день не слипались в 1С
        unique_token = f"order-{order_id}" if order_id is not None else f"sess-{clean_client_id}"
        items_fingerprint = f"{unique_token}:{clean_client_id}:{clean_date}:{[(i['sku_id'], i['qty']) for i in items_1c]}"
        idempotency_key = f"order-{hashlib.sha256(items_fingerprint.encode()).hexdigest()[:16]}"

        doc_numbers, errors = await self._dispatch_destinations(
            client_id, items_1c, clean_date, total_amount, idempotency_key=idempotency_key
        )
        if doc_numbers and not errors:
            return IntegrationResult(
                status="success",
                document_number=" / ".join(doc_numbers),
                state="Заказ успешно создан (1С + Битрикс24)",
            )

        err_summary = "; ".join(errors) if errors else "Неизвестный сбой интеграции"
        outbox_payload = {
            "session_id": session_id,
            "client_id": client_id,
            "items": items_1c,
            "delivery_date": resolved_date,
            "total_amount": total_amount,
            "raw_text": raw_text,
            "completed_docs": doc_numbers,
            "idempotency_key": idempotency_key,
            "order_id": order_id,
            "platform": platform,
            "chat_id": chat_id,
        }
        dest = "ALL" if not doc_numbers else ("1C" if "1C" in err_summary else "BITRIX24")
        outbox_entry = await create_outbox_entry(
            session_id=session_id,
            destination=dest,
            payload=outbox_payload,
            order_id=order_id,
            last_error=err_summary,
        )

        logger.warning(
            "Заказ сохранен в локальную очередь Outbox (id=%d) из-за сбоя внешних систем: %s",
            outbox_entry.id,
            err_summary,
        )
        display_num = f"{' / '.join(doc_numbers)} (остальное в OUTBOX-{outbox_entry.id})" if doc_numbers else f"OUTBOX-{outbox_entry.id}"

        return IntegrationResult(
            status="accepted",
            document_number=display_num,
            state="Сохранено в очереди Outbox",
            is_outbox=True,
            details=err_summary,
        )


integrations_service = IntegrationsService()


async def run_outbox_worker() -> None:
    """Фоновый воркер Outbox Pattern с сохранением completed_docs, защитой от split-brain и уведомлением клиента."""
    logger.info("Outbox воркер успешно запущен с интервалом %d сек.", settings.OUTBOX_POLL_INTERVAL_SEC)
    from app.core.redis_client import redis_client

    while True:
        try:
            await asyncio.sleep(settings.OUTBOX_POLL_INTERVAL_SEC)
            # Защита от одновременной обработки несколькими инстансами через Redis Distributed Lock
            lock_token = str(uuid.uuid4())
            acquired = await redis_client.acquire_lock("outbox_worker_leader", timeout=max(10, settings.OUTBOX_POLL_INTERVAL_SEC - 5), lock_token=lock_token)
            if not acquired:
                continue

            try:
                entries = await claim_outbox_entries(limit=20)
                if not entries:
                    continue

                logger.info("Outbox воркер обрабатывает %d зависших заказов...", len(entries))
                for entry in entries:
                    try:
                        payload = json.loads(entry.payload_json)
                        client_id = payload.get("client_id", entry.session_id)
                        items = payload.get("items", [])
                        delivery_date = payload.get("delivery_date")
                        total_amount = float(payload.get("total_amount", 0.0))
                        completed_docs = payload.get("completed_docs", [])
                        idempotency_key = payload.get("idempotency_key")

                        docs, errors = await integrations_service._dispatch_destinations(
                            client_id, items, delivery_date, total_amount, completed_docs, idempotency_key=idempotency_key
                        )
                        payload["completed_docs"] = docs

                        if not errors:
                            await mark_outbox_success(entry.id)
                            final_doc_num = " / ".join(docs)
                            logger.info("Outbox entry %d успешно отправлен: %s", entry.id, final_doc_num)

                            # Обновляем OrderRecord в БД
                            order_id = payload.get("order_id") or entry.order_id
                            if order_id:
                                try:
                                    await update_order_doc_number(order_id, final_doc_num, "Заказ успешно создан (1С + Битрикс24)")
                                except Exception as upd_err:
                                    logger.warning("Не удалось обновить OrderRecord #%s: %s", order_id, upd_err)

                            # Уведомляем клиента об успешной доставке
                            platform = payload.get("platform") or extract_platform_from_session(entry.session_id)
                            chat_id = payload.get("chat_id") or extract_chat_id_from_session(entry.session_id)
                            if platform and chat_id:
                                try:
                                    text_notify = (
                                        f"📦 <b>Заказ из очереди успешно зарегистрирован в 1С!</b>\n\n"
                                        f"📄 <b>Номер документа:</b> <code>{html.escape(final_doc_num)}</code>\n"
                                        f"💰 <b>Сумма:</b> {total_amount:,.2f} ₽\n"
                                        f"📅 <b>Дата доставки:</b> {html.escape(delivery_date or 'По регламенту')}"
                                    )
                                    await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=text_notify))
                                except Exception as notify_err:
                                    logger.warning("Не удалось отправить клиенту уведомление об Outbox: %s", notify_err)
                        else:
                            # Важно: сохраняем обновленный payload с completed_docs, чтобы не дублировать 1С при следующей попытке!
                            await mark_outbox_failure(entry.id, "; ".join(errors), updated_payload=payload)
                    except Exception as entry_err:
                        safe_entry_err = _sanitize_error_msg(entry_err)
                        logger.error("Сбой обработки Outbox entry #%d: %s", entry.id, safe_entry_err)
                        try:
                            await mark_outbox_failure(entry.id, f"Ошибка обработки записи: {safe_entry_err}")
                        except Exception as fatal_err:
                            logger.critical("Не удалось зафиксировать сбой Outbox #%d: %s", entry.id, _sanitize_error_msg(fatal_err))
            finally:
                await redis_client.release_lock("outbox_worker_leader", lock_token=lock_token)
        except asyncio.CancelledError:
            logger.info("Outbox воркер остановлен.")
            break
        except Exception as exc:
            logger.error("Непредвиденная ошибка в Outbox воркере: %s", exc)
