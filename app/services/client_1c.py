import os
import logging
from typing import Optional, Any
import httpx
from pydantic import BaseModel
from dotenv import load_dotenv

from app.core.config import settings

load_dotenv(override=True)
logger = logging.getLogger(__name__)


class Order1CResponse(BaseModel):
    status: str
    document_number: str
    state: str


class Client1C:
    """Отправляет подтвержденный заказ одновременно в 1С (FastAPI) и в CRM Битрикс24."""

    async def send_order(
        self,
        client_id: str,
        matched_items: list[Any],
        delivery_date: Optional[str] = None,
    ) -> Order1CResponse:
        load_dotenv(override=True)
        onec_url = settings.MOCK_1C_URL if settings.USE_MOCK_1C else settings.ONEC_URL
        bitrix_url = os.getenv("BITRIX_WEBHOOK_URL")
        if not bitrix_url:
            raise RuntimeError("BITRIX_WEBHOOK_URL не задан")

        items_1c = []
        lines_b24 = []
        product_rows_b24 = []
        total_sum = 0.0

        for it in matched_items:
            if isinstance(it, dict):
                sku = it.get("sku_id") or it.get("sku") or "SKU-001"
                name = it.get("catalog_name") or it.get("name") or it.get("raw_name") or "Товар"
                qty = float(it.get("final_quantity") or it.get("qty") or 1.0)
                price = float(it.get("price") or 100.0)
                unit = it.get("base_unit") or it.get("unit") or "шт"
            else:
                sku = getattr(it, "sku_id", None) or getattr(it, "sku", None) or "SKU-001"
                name = getattr(it, "catalog_name", None) or getattr(it, "name", None) or getattr(it, "raw_name", "Товар")
                qty = float(getattr(it, "final_quantity", None) or getattr(it, "qty", 1.0))
                price = float(getattr(it, "price", None) or 100.0)
                unit = getattr(it, "base_unit", None) or getattr(it, "unit", "шт")

            sum_line = qty * price
            total_sum += sum_line

            items_1c.append({
                "sku_id": str(sku),
                "name": str(name),
                "qty": qty,
                "unit": str(unit),
                "price": price,
            })

            lines_b24.append(f"• [{sku}] <b>{name}</b>: {qty:g} {unit} × {price:g} ₽ = {sum_line:,.2f} ₽")

            row_b24 = {
                "PRODUCT_NAME": f"[{sku}] {name}",
                "PRICE": price,
                "QUANTITY": qty,
            }
            # Если SKU синхронизирован из Битрикс24 (вида B24-12), привязываем реальный ID товара
            if str(sku).startswith("B24-") and str(sku).split("-")[-1].isdigit():
                row_b24["PRODUCT_ID"] = int(str(sku).split("-")[-1])

            product_rows_b24.append(row_b24)

        if not items_1c:
            raise ValueError("Нельзя отправить заказ без товарных позиций")

        doc_numbers = []
        errors = []

        async with httpx.AsyncClient(timeout=20.0) as client:
            # 1. Выгрузка в 1С (FastAPI сервер)
            payload_1c = {
                "client_id": str(client_id),
                "delivery_date": str(delivery_date or "Завтра"),
                "items": items_1c,
            }
            try:
                resp_1c = await client.post(onec_url, json=payload_1c)
                if resp_1c.status_code in (200, 201):
                    data_1c = resp_1c.json()
                    document_number = data_1c.get("document_number")
                    if document_number:
                        doc_numbers.append(document_number)
                    else:
                        errors.append("1С: сервер не вернул номер документа")
                else:
                    errors.append(f"1C HTTP {resp_1c.status_code}")
            except Exception as exc:
                logger.warning("Не удалось отправить в 1С (%s): %s", onec_url, exc)
                errors.append(f"1С: {exc}")

            # 2. Выгрузка в Битрикс24 (Сделка + Товары)
            try:
                comments = (
                    f"<b>Клиент:</b> {client_id}<br>"
                    f"<b>Дата доставки:</b> {delivery_date or 'Завтра'}<br><br>"
                    f"<b>Состав заказа:</b><br>" + "<br>".join(lines_b24)
                )
                payload_b24 = {
                    "fields": {
                        "TITLE": f"Заказ HoReCa от {client_id}",
                        "OPPORTUNITY": round(total_sum, 2),
                        "CURRENCY_ID": "RUB",
                        "COMMENTS": comments,
                    }
                }
                resp_b24 = await client.post(f"{bitrix_url.rstrip('/')}/crm.deal.add.json", json=payload_b24)
                if resp_b24.status_code != 200:
                    errors.append(f"Битрикс24 HTTP {resp_b24.status_code} при создании сделки")
                else:
                    data_b24 = resp_b24.json()
                    deal_id = data_b24.get("result")
                    if data_b24.get("error") or not deal_id:
                        errors.append("Битрикс24: не удалось создать сделку")
                    else:
                        doc_numbers.append(f"Б24-{deal_id}")
                        if product_rows_b24:
                            rows_resp = await client.post(
                                f"{bitrix_url.rstrip('/')}/crm.deal.productrows.set.json",
                                json={"id": deal_id, "rows": product_rows_b24},
                            )
                            if rows_resp.status_code != 200:
                                errors.append(
                                    f"Битрикс24 HTTP {rows_resp.status_code} при добавлении товаров"
                                )
                            elif rows_resp.json().get("error"):
                                errors.append("Битрикс24: не удалось добавить товары в сделку")
            except Exception as exc:
                logger.warning("Ошибка отправки в Битрикс24: %s", exc)
                errors.append(f"Битрикс24: {exc}")

        if errors:
            err_text = "; ".join(errors)
            if doc_numbers:
                err_text += f" (частично создано: {', '.join(doc_numbers)})"
            raise RuntimeError(err_text)
        if not doc_numbers:
            raise RuntimeError("Ни одна система не подтвердила создание заказа")

        return Order1CResponse(
            status="success",
            document_number=" / ".join(doc_numbers),
            state="Черновик создан (1С + Битрикс24)",
        )


client_1c = Client1C()
bitrix_client = client_1c