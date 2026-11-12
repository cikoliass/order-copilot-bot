import asyncio
import logging
import os
import sys

# Добавляем корень проекта в sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from app.core.config import settings
from app.core.database import init_db, save_order, create_outbox_entry, get_pending_outbox_entries, mark_outbox_success
from app.core.redis_client import redis_client
from app.services.schemas import RawItem, ExtractedOrder, MatchedItem, MatchStatus, InboundMessage
from app.services.matcher import catalog_matcher
from app.services.predictor import order_predictor
from app.services.parser_yandex import _fallback_rule_based_parser
from app.services.integrations import integrations_service

import pytest

logging.basicConfig(level=logging.INFO)


@pytest.mark.asyncio
async def test_verification_workflow():
    print("=== 1. ТЕСТ БАЗЫ ДАННЫХ И ИНИЦИАЛИЗАЦИИ ===")
    await init_db()
    print("✓ init_db() выполнен успешно")

    print("\n=== 2. ТЕСТ REDIS И DISTRIBUTED LOCK ===")
    await redis_client.connect()
    lock_ok = await redis_client.acquire_lock("test:user1", timeout=5)
    assert lock_ok is True, "Первый захват лока должен быть True"
    lock_fail = await redis_client.acquire_lock("test:user1", timeout=5)
    assert lock_fail is False, "Повторный захват лока должен быть False (защита от повторных нажатий)"
    await redis_client.release_lock("test:user1")
    lock_ok_after = await redis_client.acquire_lock("test:user1", timeout=5)
    assert lock_ok_after is True, "После release_lock лок должен браться снова"
    await redis_client.release_lock("test:user1")
    print("✓ Distributed lock работает корректно!")

    # Черновик заказа в Redis
    test_session = "telegram:999888"
    draft_data = {"raw_text": "2 кор черри", "matched_items": []}
    await redis_client.set_order_draft(test_session, draft_data)
    loaded_draft = await redis_client.get_order_draft(test_session)
    assert loaded_draft == draft_data, "Черновик должен совпадать с сохраненным"
    await redis_client.clear_order_draft(test_session)
    cleared_draft = await redis_client.get_order_draft(test_session)
    assert cleared_draft is None, "Черновик должен быть удален"
    print("✓ Redis draft storage работает корректно!")

    print("\n=== 3. ТЕСТ MATCHER & RAPIDFUZZ (asyncio.to_thread) ===")
    print(f"Каталог содержит позиций: {len(catalog_matcher.catalog)}")
    raw_items = [
        RawItem(raw_name="яблоки гренни смит", quantity=2.0, unit="кор"),
        RawItem(raw_name="чеснок очищенный", quantity=3.0, unit="кг"),
        RawItem(raw_name="неведомый марсианский фрукт", quantity=1.0, unit="шт"),
    ]
    matched = await catalog_matcher.match_order_async(raw_items)
    for m in matched:
        print(f"[{m.status.value}] {m.raw_name} -> {m.catalog_name} ({m.final_quantity} {m.base_unit}) | Conf: {m.confidence} | Recalc: {m.recalculation_hint}")
    assert len(matched) == 3
    print("✓ RapidFuzz матчинг и пересчет тары отработали!")

    print("\n=== 4. ТЕСТ FALLBACK ПАРСЕРА ===")
    text_sample = "На завтра: 2 коробки яблок и 5 кг чеснока"
    parsed_sample = _fallback_rule_based_parser(text_sample)
    print(f"Parsed items: {parsed_sample.items}, Date: {parsed_sample.delivery_date}")
    assert len(parsed_sample.items) >= 2
    print("✓ Парсер заказов работает!")

    print("\n=== 5. ТЕСТ OUTBOX PATTERN ===")
    outbox_entry = await create_outbox_entry(
        session_id="vk:12345",
        destination="1C",
        payload={"client_id": "vk:12345", "items": []},
        last_error="ConnectError to 1C",
    )
    assert outbox_entry.id is not None
    pending = await get_pending_outbox_entries()
    assert any(e.id == outbox_entry.id for e in pending)
    await mark_outbox_success(outbox_entry.id)
    print("✓ Outbox Pattern создание, выборка и закрытие задач работают корректно!")

    print("\n=== 6. ТЕСТ PREDICTOR ===")
    # Сохраняем тестовый заказ в БД
    await save_order(
        session_id="telegram:777",
        platform="telegram",
        platform_user_id="777",
        raw_text="2 коробки яблок",
        delivery_date="Завтра",
        items_data=[matched[0].model_dump()],
        total_amount=matched[0].total_sum,
        status="CONFIRMED",
        document_number_1c="ЗК-TEST",
    )
    ideal = await order_predictor.predict_ideal_order("telegram:777")
    print(f"Predicted ideal items: {len(ideal)}")
    print("✓ Predictor отработал!")

    await redis_client.close()
    print("\n🎉 ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!")


if __name__ == "__main__":
    asyncio.run(run_tests())
