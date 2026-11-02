import itertools
import logging
from typing import Optional

from fastapi import FastAPI, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s | 1C-MOCK | %(message)s")
logger = logging.getLogger("mock_1c")

app = FastAPI(
    title="Mock 1C:Enterprise HoReCa Gateway",
    version="1.0.0",
)

_doc_counter = itertools.count(start=1094)
SAVED_ORDERS: list[dict] = []


class Mock1COrderItem(BaseModel):
    sku_id: str = Field(..., description="Идентификатор номенклатуры в 1С")
    qty: float = Field(..., gt=0, description="Количество в базовых единицах")
    unit: str = Field(default="шт", description="Базовая единица измерения")
    price: float = Field(..., ge=0, description="Цена за единицу")
    name: Optional[str] = Field(default=None, description="Наименование товара")


class Mock1COrderRequest(BaseModel):
    client_id: str = Field(..., description="ID контрагента / клиента")
    delivery_date: Optional[str] = Field(default=None, description="Дата отгрузки")
    items: list[Mock1COrderItem] = Field(..., min_length=1, description="Товары заказа")


class Mock1COrderResponse(BaseModel):
    status: str
    document_number: str
    state: str


@app.post(
    "/api/v1/mock-1c/orders",
    response_model=Mock1COrderResponse,
    status_code=status.HTTP_201_CREATED,
)
@app.post(
    "/orders",
    response_model=Mock1COrderResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_order_in_1c(order: Mock1COrderRequest) -> Mock1COrderResponse:
    doc_num = f"ЗК-{next(_doc_counter)}"
    total_amount = sum(item.qty * item.price for item in order.items)

    order_record = {
        "doc_num": doc_num,
        "client_id": order.client_id,
        "delivery_date": order.delivery_date or "Не указана",
        "total_amount": total_amount,
        "state": "Черновик",
        "items": [item.model_dump() for item in order.items],
    }
    SAVED_ORDERS.insert(0, order_record)

    logger.info("Создан документ %s на сумму %.2f руб.", doc_num, total_amount)
    return Mock1COrderResponse(status="success", document_number=doc_num, state="Черновик")


@app.get("/")
async def root_redirect():
    return RedirectResponse(url="/orders")


@app.get("/api/v1/mock-1c/orders")
async def list_orders_json():
    return SAVED_ORDERS


@app.get("/orders", response_class=HTMLResponse)
async def view_orders_journal():
    rows_html = ""
    for ord_data in SAVED_ORDERS:
        items_detail = "<br>".join(
            f"• <b>[{it['sku_id']}] {it.get('name') or ''}</b>: {it['qty']} {it['unit']} × {it['price']} ₽"
            for it in ord_data["items"]
        )
        rows_html += f"""
        <tr style="border-bottom: 1px solid #ccc;">
            <td style="padding: 10px; font-weight: bold; color: #b22222;">{ord_data['doc_num']}</td>
            <td style="padding: 10px;">{ord_data['client_id']}</td>
            <td style="padding: 10px;">{ord_data['delivery_date']}</td>
            <td style="padding: 10px; font-size: 13px;">{items_detail}</td>
            <td style="padding: 10px; font-weight: bold;">{ord_data['total_amount']:,.2f} ₽</td>
            <td style="padding: 10px;"><span style="background: #fff3cd; color: #856404; padding: 3px 8px; border-radius: 4px;">{ord_data['state']}</span></td>
        </tr>
        """

    if not rows_html:
        rows_html = "<tr><td colspan='6' style='text-align: center; padding: 20px; color: #888;'>Заказов пока нет. Отправьте заказ через Telegram-бота!</td></tr>"

    return f"""
    <!DOCTYPE html>
    <html lang="ru">
    <head>
        <meta charset="UTF-8">
        <title>1С:Предприятие — Журнал заказов покупателей</title>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif; margin: 0; background-color: #f5f5f5; }}
            .header {{ background-color: #f7a400; color: #333; padding: 12px 20px; font-size: 18px; font-weight: bold; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            .container {{ padding: 20px; max-width: 1100px; margin: 0 auto; }}
            .card {{ background: white; border-radius: 8px; padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            table {{ width: 100%; border-collapse: collapse; text-align: left; }}
            th {{ background: #eaeaea; padding: 10px; font-size: 14px; border-bottom: 2px solid #bbb; }}
        </style>
    </head>
    <body>
        <div class="header">🟡 1С:Управление торговлей (Эмулятор) — Журнал: Заказы клиентов</div>
        <div class="container">
            <div class="card">
                <h3>Поступившие электронные заявки (HoReCa B2B)</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Номер документа</th>
                            <th>Контрагент</th>
                            <th>Дата поставки</th>
                            <th>Состав заказа (номенклатура 1С)</th>
                            <th>Сумма</th>
                            <th>Состояние</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>
        </div>
    </body>
    </html>
    """