import asyncio
import pytest
from app.services.schemas import InboundMessage
from app.core.redis_client import redis_client
from app.services.matcher import catalog_matcher
from app.api.gateway import process_inbound_core

@pytest.mark.asyncio
async def test_chef_conversational_flow():
    user_id = "test_chef_777"

    # Инициализация тестового каталога
    catalog_matcher.reload([
        {"sku_id": "SKU-APPLE", "name": "Яблоки сезонные", "base_unit": "кг", "price": 100.0, "stock_quantity": 500.0, "aliases": ["яблоки", "яблок", "яблоко"]},
        {"sku_id": "SKU-LEMON", "name": "Лимоны свежие", "base_unit": "шт", "price": 40.0, "stock_quantity": 200.0, "aliases": ["лимоны", "лимона", "лимон"]},
    ])
    await redis_client.clear_order_draft(f"telegram:{user_id}")
    
    # 1. Заказ яблок
    print("\n--- 1. Создаем заказ: 3 кг яблок ---")
    await process_inbound_core(InboundMessage(platform="telegram", platform_user_id=user_id, chat_id="100", text="3 кг яблок"))
    d1 = await redis_client.get_order_draft(f"telegram:{user_id}")
    assert d1 is not None
    assert len(d1["matched_items"]) == 1
    print("Draft 1:", [it["catalog_name"] for it in d1["matched_items"]])

    # 2. Добавление лимонов: «давай 2 лимона сверху еще»
    print("\n--- 2. Докидываем: давай 2 лимона сверху еще ---")
    await process_inbound_core(InboundMessage(platform="telegram", platform_user_id=user_id, chat_id="100", text="давай 2 лимона сверху еще"))
    d2 = await redis_client.get_order_draft(f"telegram:{user_id}")
    assert d2 is not None
    assert len(d2["matched_items"]) == 2
    print("Draft 2:", [it["catalog_name"] for it in d2["matched_items"]])

    # 3. Удаление с матом: «не, нахуй лимоны»
    print("\n--- 3. Шеф передумал: не, нахуй лимоны ---")
    await process_inbound_core(InboundMessage(platform="telegram", platform_user_id=user_id, chat_id="100", text="не, нахуй лимоны"))
    d3 = await redis_client.get_order_draft(f"telegram:{user_id}")
    assert d3 is not None
    assert len(d3["matched_items"]) == 1
    assert "Яблоки" in d3["matched_items"][0]["catalog_name"]
    print("Draft 3:", [it["catalog_name"] for it in d3["matched_items"]])

    # 4. Изменение количества: «яблок только 2 кг оставь»
    print("\n--- 4. Правка количества: яблок только 2 кг оставь ---")
    await process_inbound_core(InboundMessage(platform="telegram", platform_user_id=user_id, chat_id="100", text="яблок только 2 кг оставь"))
    d4 = await redis_client.get_order_draft(f"telegram:{user_id}")
    assert d4 is not None
    assert len(d4["matched_items"]) == 1
    assert d4["matched_items"][0]["final_quantity"] == 2.0
    print("Draft 4:", [it["catalog_name"] + f" ({it['final_quantity']})" for it in d4["matched_items"]])

    # 5. Подтверждение сленгом: «все ништяк везите»
    print("\n--- 5. Подтверждение шефа: все ништяк везите ---")
    await process_inbound_core(InboundMessage(platform="telegram", platform_user_id=user_id, chat_id="100", text="все ништяк везите"))
    d5 = await redis_client.get_order_draft(f"telegram:{user_id}")
    assert d5 is None
    print("Draft 5 (успешно отправлен в 1С, корзина очищена):", d5)

if __name__ == "__main__":
    asyncio.run(test_chef_conversational_flow())
