import html
import itertools
import logging
from datetime import datetime, timezone
from typing import Any, Optional
from fastapi import FastAPI, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO, format="%(asctime)s | 1C-MOCK | %(message)s")
logger = logging.getLogger("mock_1c")

app = FastAPI(
    title="Mock 1C:Enterprise HoReCa Gateway",
    description="Эмулятор входящего HTTP-сервиса 1С:Управление торговлей / ERP для приема оптовых заказов HoReCa",
    version="2.0.0",
)

_doc_counter = itertools.count(start=1094)
SAVED_ORDERS: list[dict[str, Any]] = []
IDEMPOTENCY_STORE: dict[str, dict[str, Any]] = {}


class Mock1COrderItem(BaseModel):
    sku_id: str = Field(..., description="Идентификатор номенклатуры в 1С")
    qty: float = Field(..., gt=0, description="Количество в базовых единицах")
    unit: str = Field(default="шт", description="Базовая единица измерения")
    price: float = Field(..., ge=0, description="Цена за единицу в рублях")
    name: Optional[str] = Field(default=None, description="Наименование номенклатуры")


class Mock1COrderRequest(BaseModel):
    client_id: str = Field(..., description="ID контрагента / сессии")
    delivery_date: Optional[str] = Field(default=None, description="Дата отгрузки / доставки")
    items: list[Mock1COrderItem] = Field(..., min_length=1, description="Спецификация товаров")
    idempotency_key: Optional[str] = Field(default=None, description="Ключ идемпотентности")


class Mock1COrderResponse(BaseModel):
    status: str
    document_number: str
    state: str


@app.post(
    "/api/v1/mock-1c/orders",
    response_model=Mock1COrderResponse,
    status_code=status.HTTP_201_CREATED,
    tags=["1C API"],
)
@app.post(
    "/orders",
    response_model=Mock1COrderResponse,
    status_code=status.HTTP_201_CREATED,
    include_in_schema=False,
)
async def create_order_in_1c(order: Mock1COrderRequest) -> Mock1COrderResponse:
    if order.idempotency_key and order.idempotency_key in IDEMPOTENCY_STORE:
        existing = IDEMPOTENCY_STORE[order.idempotency_key]
        logger.info("1С: Повторный запрос с ключом идемпотентности %s, возврат существующего %s", order.idempotency_key, existing["doc_num"])
        return Mock1COrderResponse(
            status="success",
            document_number=existing["doc_num"],
            state="Черновик (Идемпотентный ответ)",
        )

    doc_num = f"ЗК-{next(_doc_counter)}"
    total_amount = sum(item.qty * item.price for item in order.items)
    now_str = datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M:%S UTC")

    order_record = {
        "doc_num": doc_num,
        "client_id": order.client_id,
        "delivery_date": order.delivery_date or "Как можно скорее",
        "total_amount": total_amount,
        "state": "Черновик (Проведен)",
        "created_at": now_str,
        "items": [item.model_dump() for item in order.items],
    }
    SAVED_ORDERS.insert(0, order_record)
    if order.idempotency_key:
        IDEMPOTENCY_STORE[order.idempotency_key] = order_record

    logger.info("1С: Создан документ %s на сумму %.2f руб. от клиента %s", doc_num, total_amount, order.client_id)
    return Mock1COrderResponse(
        status="success",
        document_number=doc_num,
        state="Черновик",
    )


@app.get("/", include_in_schema=False)
async def root_redirect():
    return RedirectResponse(url="/orders")


@app.get("/api/v1/mock-1c/orders", tags=["1C API"])
async def list_orders_json():
    return SAVED_ORDERS


@app.get("/orders", response_class=HTMLResponse, tags=["Dashboard"])
async def view_orders_journal():
    total_orders_count = len(SAVED_ORDERS)
    total_revenue = sum(o["total_amount"] for o in SAVED_ORDERS)
    avg_check = (total_revenue / total_orders_count) if total_orders_count > 0 else 0.0

    rows_html = ""
    for ord_data in SAVED_ORDERS:
        items_detail = "<br>".join(
            f"• <b>[{html.escape(str(it['sku_id']))}] {html.escape(str(it.get('name') or 'Товар'))}</b>: "
            f"<code>{it['qty']:g} {html.escape(str(it['unit']))}</code> × {it['price']:g} ₽ = <b>{it['qty'] * it['price']:,.2f} ₽</b>"
            for it in ord_data["items"]
        )
        safe_doc_num = html.escape(str(ord_data['doc_num']))
        safe_created_at = html.escape(str(ord_data.get('created_at', '-')))
        safe_client_id = html.escape(str(ord_data['client_id']))
        safe_delivery_date = html.escape(str(ord_data['delivery_date']))
        safe_state = html.escape(str(ord_data['state']))
        rows_html += f"""
        <tr style="border-bottom: 1px solid #334155; transition: background 0.2s;" onmouseover="this.style.background='#1e293b'" onmouseout="this.style.background='transparent'">
            <td style="padding: 14px; font-weight: 700; color: #38bdf8; font-family: monospace; font-size: 15px;">{safe_doc_num}</td>
            <td style="padding: 14px; color: #94a3b8; font-size: 13px;">{safe_created_at}</td>
            <td style="padding: 14px; font-weight: 600; color: #f1f5f9;"><span style="background: #334155; padding: 4px 8px; border-radius: 6px;">{safe_client_id}</span></td>
            <td style="padding: 14px; color: #cbd5e1;">{safe_delivery_date}</td>
            <td style="padding: 14px; font-size: 13px; line-height: 1.6; color: #e2e8f0;">{items_detail}</td>
            <td style="padding: 14px; font-weight: 700; color: #4ade80; font-size: 16px; white-space: nowrap;">{ord_data['total_amount']:,.2f} ₽</td>
            <td style="padding: 14px;"><span style="background: #064e3b; color: #6ee7b7; border: 1px solid #059669; padding: 4px 10px; border-radius: 6px; font-size: 12px; font-weight: 600;">{safe_state}</span></td>
        </tr>
        """

    if not rows_html:
        rows_html = """
        <tr>
            <td colspan="7" style="text-align: center; padding: 40px; color: #64748b; font-size: 15px;">
                📥 Заказов пока не поступало.<br>Отправьте тестовый заказ текстом или голосом через Telegram / VK / WhatsApp / Viber / MAX!
            </td>
        </tr>
        """

    html_content = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="refresh" content="5">
    <title>1С:Предприятие 8.3 — Журнал заказов покупателей (HoReCa)</title>
    <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            background-color: #0f172a;
            color: #f8fafc;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            padding: 30px 40px;
        }}
        .header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 25px;
            padding-bottom: 20px;
            border-bottom: 1px solid #1e293b;
        }}
        .brand {{
            display: flex;
            align-items: center;
            gap: 15px;
        }}
        .logo-badge {{
            background: linear-gradient(135deg, #e11d48, #be123c);
            color: #fff;
            padding: 8px 16px;
            border-radius: 8px;
            font-weight: 900;
            font-size: 18px;
            letter-spacing: 1px;
            box-shadow: 0 4px 12px rgba(225, 29, 72, 0.4);
        }}
        h1 {{
            font-size: 22px;
            font-weight: 700;
            color: #f8fafc;
        }}
        .subtitle {{
            color: #64748b;
            font-size: 13px;
            margin-top: 2px;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .card {{
            background: #1e293b;
            border: 1px solid #334155;
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
        }}
        .card-label {{
            color: #94a3b8;
            font-size: 13px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 600;
            margin-bottom: 8px;
        }}
        .card-value {{
            font-size: 26px;
            font-weight: 800;
            color: #f8fafc;
        }}
        .table-container {{
            background: #1e293b;
            border: 1px solid #334155;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.3);
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            text-align: left;
        }}
        th {{
            background: #0f172a;
            color: #94a3b8;
            padding: 14px;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            font-weight: 700;
            border-bottom: 1px solid #334155;
        }}
        .pulse-dot {{
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #22c55e;
            box-shadow: 0 0 8px #22c55e;
            margin-right: 6px;
            animation: pulse 2s infinite;
        }}
        @keyframes pulse {{
            0% {{ opacity: 1; transform: scale(1); }}
            50% {{ opacity: 0.4; transform: scale(1.2); }}
            100% {{ opacity: 1; transform: scale(1); }}
        }}
    </style>
</head>
<body>
    <div class="header">
        <div class="brand">
            <div class="logo-badge">1С:ERP</div>
            <div>
                <h1>Журнал заказов клиентов (HoReCa Gateway)</h1>
                <div class="subtitle">Интеграционный шлюз приёма заказов B2B Order Co-Pilot • Автообновление каждые 5 сек</div>
            </div>
        </div>
        <div style="font-size: 13px; color: #94a3b8; display: flex; align-items: center;">
            <span class="pulse-dot"></span> Сервис активен и принимает накладные
        </div>
    </div>

    <div class="stats-grid">
        <div class="card">
            <div class="card-label">Всего заказов</div>
            <div class="card-value" style="color: #38bdf8;">{total_orders_count}</div>
        </div>
        <div class="card">
            <div class="card-label">Общий товарооборот</div>
            <div class="card-value" style="color: #4ade80;">{total_revenue:,.2f} ₽</div>
        </div>
        <div class="card">
            <div class="card-label">Средний чек накладной</div>
            <div class="card-value" style="color: #fbbf24;">{avg_check:,.2f} ₽</div>
        </div>
    </div>

    <div class="table-container">
        <table>
            <thead>
                <tr>
                    <th>№ Документа</th>
                    <th>Время фиксации</th>
                    <th>Контрагент / Канал</th>
                    <th>Срок поставки</th>
                    <th>Номенклатура и спецификация</th>
                    <th>Сумма (НДС)</th>
                    <th>Статус 1С</th>
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
</body>
</html>
"""
    return HTMLResponse(content=html_content)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("mock_server.main:app", host="0.0.0.0", port=8080, reload=False)
