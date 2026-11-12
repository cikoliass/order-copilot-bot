import asyncio
import pytest
from app.api.gateway import process_inbound_core
from app.core.config import settings
from app.core.redis_client import redis_client
from app.services.matcher import catalog_matcher
from app.services.schemas import CatalogSKU, InboundMessage, MatchStatus

@pytest.mark.asyncio
async def test_pack_multiples_and_analogs():
    user_id = "chef_pro_99"

    # 1. Добавляем в каталог 2 тестовых товара для проверки аналогов и кратности
    test_sku_1 = CatalogSKU(
        sku_id="TEST-OOS-01",
        name="Сливки Чудское озеро 33% 1л",
        base_unit="л",
        price=320.0,
        stock_quantity=0.0,  # 0 на складе
        aliases=["сливки чудское озеро", "чудское озеро", "сливки 33%"],
    )
    test_sku_2 = CatalogSKU(
        sku_id="TEST-ANALOG-02",
        name="Сливки Петмол 33% 1л ультрапастеризованные",
        base_unit="л",
        price=340.0,
        stock_quantity=50.0,  # в наличии!
        aliases=["сливки петмол", "петмол", "сливки 33% петмол"],
    )
    test_sku_3 = CatalogSKU(
        sku_id="TEST-PACK-03",
        name="Соус Табаско Оригинальный 60мл (упаковка 12 шт)",
        base_unit="шт",
        price=250.0,
        stock_quantity=100.0,
        pack_multiple=12.0,  # Кратность 12 шт
        aliases=["табаско", "соус табаско"],
    )

    items_raw = [test_sku_1.model_dump(), test_sku_2.model_dump(), test_sku_3.model_dump()]
    catalog_matcher.reload(items_raw)
    catalog_matcher.mark_out_of_stock("TEST-OOS-01")

    # --- ТЕСТ 1: Товар не в наличии -> бейдж 🔴 и автоподбор аналога ---
    print("\n--- ТЕСТ 1: Заказ товара со стоп-листа ---")
    await process_inbound_core(
        InboundMessage(platform="telegram", platform_user_id=user_id, chat_id="200", text="5 л сливки чудское озеро")
    )
    draft1 = await redis_client.get_order_draft(f"telegram:{user_id}")
    assert draft1 is not None
    item1 = draft1["matched_items"][0]
    assert item1["is_out_of_stock"] is True
    assert item1["status"] == MatchStatus.OUT_OF_STOCK
    assert item1["suggested_analog_name"] == "Сливки Петмол 33% 1л ультрапастеризованные"
    print(f"✓ Товар распознан как OUT_OF_STOCK: {item1['catalog_name']}")
    print(f"✓ Автоматически подобран аналог: {item1['suggested_analog_name']} ({item1['suggested_analog_price']} ₽)")

    # --- ТЕСТ 2: Замена на аналог по команде «замени на аналог» ---
    print("\n--- ТЕСТ 2: Замена на аналог по команде ---")
    await process_inbound_core(
        InboundMessage(platform="telegram", platform_user_id=user_id, chat_id="200", text="давай аналог")
    )
    draft2 = await redis_client.get_order_draft(f"telegram:{user_id}")
    assert draft2 is not None
    item2 = draft2["matched_items"][0]
    assert item2["is_out_of_stock"] is False
    assert "Петмол" in item2["catalog_name"]
    print(f"✓ Позиция успешно заменена на аналог: {item2['catalog_name']}")

    # --- ТЕСТ 3: Кратность упаковки и предупреждение ---
    print("\n--- ТЕСТ 3: Заказ некратного количества упаковки ---")
    await process_inbound_core(
        InboundMessage(platform="telegram", platform_user_id=user_id, chat_id="200", text="плюс 5 шт табаско")
    )
    draft3 = await redis_client.get_order_draft(f"telegram:{user_id}")
    assert draft3 is not None
    assert len(draft3["matched_items"]) == 2
    tabasco_item = draft3["matched_items"][1]
    assert tabasco_item["pack_multiple"] == 12.0
    assert tabasco_item["suggested_pack_quantity"] == 12.0
    assert tabasco_item["pack_warning"] is not None
    print(f"✓ Сработал контроль кратности упаковки: {tabasco_item['pack_warning']}")

    # --- ТЕСТ 4: Округление до упаковки по команде «округли» ---
    print("\n--- ТЕСТ 4: Округление до упаковки ---")
    await process_inbound_core(
        InboundMessage(platform="telegram", platform_user_id=user_id, chat_id="200", text="округли табаско")
    )
    draft4 = await redis_client.get_order_draft(f"telegram:{user_id}")
    tabasco_rounded = draft4["matched_items"][1]
    assert tabasco_rounded["final_quantity"] == 12.0
    print(f"✓ Количество успешно округлено до упаковки: {tabasco_rounded['final_quantity']} шт")

    print("\n🎉 ВСЕ ТЕСТЫ КРАТНОСТИ И АНАЛОГОВ ПРОЙДЕНЫ УСПЕШНО!")

if __name__ == "__main__":
    asyncio.run(test_pack_multiples_and_analogs())
