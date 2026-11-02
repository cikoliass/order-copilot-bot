import asyncio
from app.services.client_1c import client_1c

async def test():
    print("Отправка тестового запроса в 1С...")
    res = await client_1c.send_order(
        client_id="TG-TEST",
        matched_items=[{"sku": "SKU-001", "name": "Тест", "qty": 1, "price": 100}],
        delivery_date="16.10.2026"
    )
    print("Результат от клиента 1С:", res)

if __name__ == "__main__":
    asyncio.run(test())