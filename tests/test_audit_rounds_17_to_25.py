import asyncio
import math
import re
import time
from unittest.mock import AsyncMock, MagicMock, patch
import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from app.core.config import settings
from app.core.redis_client import RedisClient, redis_client
from app.api.gateway import (
    OrderStatusUpdateRequest,
    _download_tg_file,
    bitrix_catalog_webhook,
    execute_order_confirmation,
    telegram_webhook,
    update_order_status,
)
from app.services.integrations import (
    BitrixProductRowsError,
    IntegrationsService,
)
from app.services.matcher import CatalogMatcher, catalog_matcher
from app.services.parser_yandex import (
    _fallback_cart_intent,
    _fallback_rule_based_parser,
)
from app.services.predictor import OrderPredictor
from app.services.schemas import CatalogSKU, InboundMessage, MatchStatus, MatchedItem


# =========================================================================
# РАУНД 17: Pre-Auth / Pre-RateLimit DoS при скачивании файлов в Telegram Webhook
# =========================================================================
@pytest.mark.asyncio
async def test_round17_telegram_webhook_passes_file_id_without_download():
    """Вебхук Telegram мгновенно возвращает {'ok': True} и передает voice_file_id без блокирующего скачивания."""
    payload = {
        "update_id": 999999,
        "message": {
            "from": {"id": 12345},
            "chat": {"id": 12345},
            "voice": {"file_id": "test_voice_file_id_abc123"},
        },
    }
    mock_req = MagicMock()
    mock_req.json = AsyncMock(return_value=payload)
    mock_req.headers = {}

    with patch("app.api.gateway.verify_webhook_auth"), \
         patch("app.api.gateway._download_tg_file") as mock_dl, \
         patch("app.api.gateway.process_inbound_core") as mock_core:
        resp = await telegram_webhook(mock_req)
        assert resp == {"ok": True}
        # Скачивание файла в вебхуке НЕ должно вызываться!
        mock_dl.assert_not_called()
        # process_inbound_core получает voice_file_id
        mock_core.assert_called_once()
        inbound = mock_core.call_args[0][0]
        assert inbound.voice_file_id == "test_voice_file_id_abc123"
        assert inbound.voice_bytes is None


@pytest.mark.asyncio
async def test_round17_download_tg_file_security_checks():
    """_download_tg_file отклоняет некорректные file_id и файлы более 20 МБ."""
    # 1. Слишком длинный или с недопустимыми символами file_id
    assert await _download_tg_file("a" * 300) is None
    assert await _download_tg_file("id; rm -rf /") is None
    assert await _download_tg_file("../etc/passwd") is None

    # 2. Файл более 20 МБ
    with patch.object(settings, "TELEGRAM_BOT_TOKEN", "123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11"), \
         patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_resp_meta = MagicMock(status_code=200)
        mock_resp_meta.json.return_value = {"result": {"file_path": "voice/file_1.oga"}}
        mock_client.get = AsyncMock(return_value=mock_resp_meta)

        # Симулируем стриминг более 20 МБ
        mock_dl_resp = MagicMock(status_code=200)
        async def mock_aiter(chunk_size=65536):
            for _ in range(350):  # 350 * 65536 = 22.9 MB
                yield b"x" * chunk_size
        mock_dl_resp.aiter_bytes = mock_aiter

        mock_stream_cm = MagicMock()
        mock_stream_cm.__aenter__ = AsyncMock(return_value=mock_dl_resp)
        mock_stream_cm.__aexit__ = AsyncMock(return_value=None)
        mock_client.stream = MagicMock(return_value=mock_stream_cm)
        mock_client_cls.return_value.__aenter__.return_value = mock_client

        result = await _download_tg_file("valid_file_id_123")
        assert result is None


# =========================================================================
# РАУНД 18: Вытеснение стоп-листа 1С и счетчиков Rate Limit при сбое Redis
# =========================================================================
@pytest.mark.asyncio
async def test_round18_redis_fallback_eviction_protects_system_keys():
    """При переполнении fallback-памяти вытесняются только черновики (order:draft:), не затрагивая стоп-лист и лимиты."""
    client = RedisClient("redis://localhost:6379/0")
    client._use_fallback = True

    # 1. Инициализируем системные ключи
    await client.add_stop_list_item("SKU-CRITICAL-STOP")
    await client.set_stock_override("SKU-100", 42.0)
    await client.check_rate_limit("user:important", max_requests=10)

    # 2. Генерируем 5005 черновиков от разных пользователей
    for i in range(5005):
        client._in_memory_fallback[f"order:draft:sess_{i}"] = (
            '{"matched_items": []}',
            time.time() + 3600,
        )

    # 3. Вызываем сохранение очередного черновика, инициирующее вытеснение
    await client.set_order_draft("sess_trigger", {"matched_items": []})

    # 4. Проверяем, что системные ключи НЕ вытеснены
    stop_list = await client.get_stop_list()
    assert "SKU-CRITICAL-STOP" in stop_list
    assert "catalog:stop_list" in client._in_memory_fallback

    stocks = await client.get_stock_overrides()
    assert stocks.get("SKU-100") == 42.0
    assert "catalog:stocks" in client._in_memory_fallback

    assert "rate_limit:user:important" in client._in_memory_fallback


# =========================================================================
# РАУНД 19: Размножение сделок-дублей в Битрикс24 при сбое crm.deal.productrows.set
# =========================================================================
@pytest.mark.asyncio
async def test_round19_bitrix_product_rows_failure_saves_pending_and_avoids_duplicate():
    """При падении crm.deal.productrows.set сохраняется маркер Б24-PENDING-{deal_id} и повторно используется existing_deal_id."""
    svc = IntegrationsService()

    # Симулируем: первый вызов _send_to_bitrix24 падает с BitrixProductRowsError(deal_id=777)
    with patch.object(svc, "_send_to_1c", AsyncMock(return_value="1C-100")), \
         patch.object(svc, "_send_to_bitrix24") as mock_b24:

        mock_b24.side_effect = BitrixProductRowsError(777, "Timeout while binding rows")
        docs, errors = await svc._dispatch_destinations(
            client_id="client_1",
            items=[{"sku_id": "SKU-1", "name": "Товар", "qty": 1, "price": 100}],
            delivery_date=None,
            total_amount=100.0,
            completed_docs=[],
        )

        assert "1C-100" in docs
        assert "Б24-PENDING-777" in docs
        assert any("Битрикс24" in e for e in errors)

        # При повторной попытке из Outbox _dispatch_destinations передает existing_deal_id=777
        mock_b24.side_effect = None
        mock_b24.return_value = "Б24-777"

        docs_retry, errors_retry = await svc._dispatch_destinations(
            client_id="client_1",
            items=[{"sku_id": "SKU-1", "name": "Товар", "qty": 1, "price": 100}],
            delivery_date=None,
            total_amount=100.0,
            completed_docs=docs,
        )

        assert "Б24-777" in docs_retry
        assert not any("PENDING" in str(d) for d in docs_retry)
        mock_b24.assert_called_with("client_1", [{"sku_id": "SKU-1", "name": "Товар", "qty": 1, "price": 100}], None, 100.0, existing_deal_id=777)


# =========================================================================
# РАУНД 20: Квадратичный ReDoS в парсере и добавление мусора при отрицании
# =========================================================================
def test_round20_fallback_parser_redos_protection():
    """Строка из 3000 цифр не приводит к зависанию Event Loop (ReDoS)."""
    evil_input = "1" * 3000
    t0 = time.time()
    result = _fallback_rule_based_parser(evil_input)
    elapsed = time.time() - t0
    assert elapsed < 0.2  # Должно выполниться практически мгновенно
    assert len(result.items) == 0


def test_round20_fallback_cart_intent_negation_and_word_boundaries():
    """Фразы с отрицанием или словами-частями не превращаются ложно в action='add'."""
    # 1. Отрицание не должно добавлять товар
    res1 = _fallback_cart_intent("не надо ничего больше, не добавляй", ["помидоры 2 кг"])
    assert res1.action != "add"

    res2 = _fallback_cart_intent("больше ничего не давай", ["помидоры 2 кг"])
    assert res2.action != "add"

    # 2. Слова, содержащие 'давай' как подстроку (например 'вещевой' или 'передавай')
    res3 = _fallback_cart_intent("вещевой склад закрыт", ["помидоры 2 кг"])
    assert res3.action != "add"

    # 3. Валидное добавление по-прежнему работает
    res4 = _fallback_cart_intent("добавь 5 кг картофеля", ["помидоры 2 кг"])
    assert res4.action == "add"
    assert len(res4.items_to_add) == 1
    assert "картофел" in res4.items_to_add[0].raw_name.lower()


# =========================================================================
# РАУНД 21: Ошибка DataError в PostgreSQL и валидация формата update_order_status
# =========================================================================
def test_round21_order_status_update_request_validation():
    """OrderStatusUpdateRequest ограничивает длину status (<=255) и document_number (<=128)."""
    # 1. Валидный запрос
    req = OrderStatusUpdateRequest(document_number="ZK-1234", status="В пути", comment="Тест")
    assert req.document_number == "ZK-1234"

    # 2. Превышение длины status (защита String(255) в БД)
    with pytest.raises(ValidationError):
        OrderStatusUpdateRequest(document_number="ZK-1234", status="A" * 256)

    # 3. Превышение длины document_number
    with pytest.raises(ValidationError):
        OrderStatusUpdateRequest(document_number="Z" * 129, status="Готов")


@pytest.mark.asyncio
async def test_round21_update_order_status_rejects_malformed_doc_number():
    """update_order_status отклоняет номера документов со спецсимволами и пробелами."""
    mock_req = MagicMock()
    mock_payload = OrderStatusUpdateRequest(document_number="ZK-1234", status="В пути")

    with patch("app.api.gateway.verify_webhook_auth"):
        # Попытка инъекции пробела или шаблона
        with pytest.raises(HTTPException) as exc1:
            await update_order_status(mock_payload, mock_req, doc_num="ZK-1234 (остальное в OUTBOX)")
        assert exc1.value.status_code == 400

        with pytest.raises(HTTPException) as exc2:
            await update_order_status(mock_payload, mock_req, doc_num="ZK-1234; DROP TABLE orders;--")
        assert exc2.value.status_code == 400


# =========================================================================
# РАУНД 22: Подмена контрагента в групповом чате при подтверждении чужого заказа
# =========================================================================
@pytest.mark.asyncio
async def test_round22_group_chat_cross_counterparty_confirmation_blocked():
    """В групповом чате пользователь не может подтвердить заказ, созданный сотрудником другого контрагента."""
    session_id = "telegram:group:-100123456789"
    creator_uid = "chef_user_101"
    intruder_uid = "chef_user_202"

    draft = {
        "creator_user_id": creator_uid,
        "matched_items": [
            MatchedItem(
                raw_name="Томаты 5 кг",
                raw_quantity=5.0,
                raw_unit="кг",
                sku_id="SKU-TOMATO",
                catalog_name="Томаты свежие",
                final_quantity=5.0,
                base_unit="кг",
                price=200.0,
                total_sum=1000.0,
                status=MatchStatus.RECOGNIZED,
            ).model_dump()
        ],
    }

    mock_creator_binding = MagicMock(counterparty_1c_id="1C-REST-ALPHA")
    mock_intruder_binding = MagicMock(counterparty_1c_id="1C-REST-BETA")

    async def mock_get_binding(platform, user_id):
        if str(user_id) == creator_uid:
            return mock_creator_binding
        elif str(user_id) == intruder_uid:
            return mock_intruder_binding
        return None

    with patch.object(settings, "REQUIRE_CLIENT_AUTH", True), \
         patch("app.core.redis_client.redis_client.acquire_lock", AsyncMock(return_value=True)), \
         patch("app.core.redis_client.redis_client.get_order_draft", AsyncMock(return_value=draft)), \
         patch("app.core.database.get_client_binding", side_effect=mock_get_binding), \
         patch("app.services.dispatcher.dispatcher.send", AsyncMock()) as mock_send, \
         patch("app.services.integrations.integrations_service.send_order", AsyncMock()) as mock_int_send:

        await execute_order_confirmation(session_id, "telegram", "-100123456789", intruder_uid)

        # Отправка в 1С НЕ должна произойти!
        mock_int_send.assert_not_called()
        # Пользователь должен получить сообщение об ошибке доступа
        mock_send.assert_called_once()
        sent_msg = mock_send.call_args[0][0]
        assert "Ошибка доступа" in sent_msg.text
        assert "сотрудником другого контрагента" in sent_msg.text


# =========================================================================
# РАУНД 23: Утечка WEBHOOK_SECRET в логах при setWebhook и дедупликация
# =========================================================================
@pytest.mark.asyncio
async def test_round23_webhook_secret_not_leaked_in_logs():
    """При ошибке setWebhook секреты TELEGRAM_BOT_TOKEN и WEBHOOK_SECRET маскируются в логах."""
    from app.main import lifespan

    bot_token = "123456789:ABCDEF_secret_token_bot"
    webhook_secret = "super_classified_webhook_secret_999"

    with patch.object(settings, "TELEGRAM_BOT_TOKEN", bot_token), \
         patch.object(settings, "TELEGRAM_WEBHOOK_URL", "https://bot.example.com/api/tg"), \
         patch.object(settings, "WEBHOOK_SECRET", webhook_secret), \
         patch.object(settings, "ENABLE_POLLING_BRIDGES", False), \
         patch("httpx.AsyncClient.post", side_effect=RuntimeError(f"Connection failed to {bot_token} with secret {webhook_secret}")), \
         patch("app.main.logger.warning") as mock_warn:

        # Инициализируем lifespan
        app_mock = MagicMock()
        async with lifespan(app_mock):
            pass

        mock_warn.assert_called_once()
        log_msg = mock_warn.call_args[0][1]
        assert bot_token not in log_msg
        assert webhook_secret not in log_msg
        assert "***BOT_TOKEN***" in log_msg
        assert "***WEBHOOK_SECRET***" in log_msg


# =========================================================================
# РАУНД 24: Отравление «Идеального заказа» удаленными и нулевыми товарами
# =========================================================================
@pytest.mark.asyncio
async def test_round24_predict_ideal_order_excludes_deleted_and_zero_price_items():
    """predict_ideal_order исключает удаленные товары и SKU с нулевой ценой, а также ограничивает остатком."""
    predictor = OrderPredictor()

    # Задаем историю заказов с тремя товарами
    history = [
        [
            {"sku_id": "SKU-ACTIVE", "name": "Лук репчатый", "quantity": 10.0, "price": 50.0, "unit": "кг"},
            {"sku_id": "SKU-DELETED", "name": "Выведенный товар", "quantity": 5.0, "price": 100.0, "unit": "шт"},
            {"sku_id": "SKU-ZERO-PRICE", "name": "Товар без цены", "quantity": 2.0, "price": 0.0, "unit": "кг"},
        ]
    ]

    # В активном каталоге есть только SKU-ACTIVE (остаток 8 кг) и SKU-ZERO-PRICE (цена 0)
    sku_active = CatalogSKU(sku_id="SKU-ACTIVE", name="Лук репчатый", price=50.0, stock_quantity=8.0, base_unit="кг")
    sku_zero = CatalogSKU(sku_id="SKU-ZERO-PRICE", name="Товар без цены", price=0.0, stock_quantity=100.0, base_unit="кг")

    with patch("app.services.predictor.get_user_order_history", AsyncMock(return_value=history)), \
         patch.object(catalog_matcher, "catalog", [sku_active, sku_zero]), \
         patch.object(catalog_matcher, "get_stock", return_value=8.0):

        ideal = await predictor.predict_ideal_order("test_session", top_k=5)

        # В идеальный заказ должен попасть ТОЛЬКО SKU-ACTIVE
        skus = [it.sku_id for it in ideal]
        assert "SKU-ACTIVE" in skus
        assert "SKU-DELETED" not in skus
        assert "SKU-ZERO-PRICE" not in skus

        # Количество должно быть ограничено доступным остатком (8.0 вместо 10.0)
        active_item = next(it for it in ideal if it.sku_id == "SKU-ACTIVE")
        assert active_item.final_quantity == 8.0


# =========================================================================
# РАУНД 25: DoS синхронизации Битрикс24 и падение CatalogSKU.from_raw
# =========================================================================
@pytest.mark.asyncio
async def test_round25_bitrix_catalog_sync_cooldown_prevents_dos_storm():
    """Вебхук /bitrix/product-sync защищен неблокирующим Distributed Lock, отклоняя параллельный шторм."""
    mock_req = MagicMock()

    with patch("app.api.gateway.verify_webhook_auth"), \
         patch("app.core.redis_client.redis_client.acquire_lock", AsyncMock(return_value=False)):
        res = await bitrix_catalog_webhook(mock_req)
        assert res["status"] == "skipped"
        assert "уже выполняется" in res["message"]


def test_round25_catalog_sku_from_raw_handles_malformed_values():
    """CatalogSKU.from_raw безопасно обрабатывает битые числа, NaN, inf и мусорные единицы."""
    malformed_data = {
        "sku_id": "SKU-TEST-99",
        "name": "Тестовый товар",
        "price": float("nan"),
        "stock_quantity": float("inf"),
        "pack_multiple": "invalid_num",
        "conversion_rates": {
            "ящик": float("nan"),
            "коробка": "10.5",
            "мусор": -5.0,
        },
    }

    sku = CatalogSKU.from_raw(malformed_data)
    assert sku.price == 0.0
    assert sku.stock_quantity == 0.0
    assert sku.pack_multiple is None
    # Корректные коэффициенты пересчета сохраняются, некорректные отбрасываются
    assert sku.conversion_rates == {"коробка": 10.5}


def test_round25_catalog_matcher_reload_isolates_broken_items():
    """CatalogMatcher.reload не падает при наличии битых записей в списке товаров Битрикс24."""
    matcher = CatalogMatcher()
    raw_list = [
        {"sku_id": "SKU-OK", "name": "Нормальный товар", "price": 150.0},
        "not a dict item at all",  # Не словарь
        {"sku_id": "SKU-OK2", "name": "Второй товар", "price": "invalid"},
    ]

    matcher.reload(raw_list)
    assert len(matcher.catalog) == 2
    assert {s.sku_id for s in matcher.catalog} == {"SKU-OK", "SKU-OK2"}
