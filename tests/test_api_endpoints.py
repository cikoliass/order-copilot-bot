import asyncio
import html
import json
import logging
import os
import sys
import pytest

# Добавляем корень проекта в sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

import httpx
from app.main import app as main_app
from mock_server.main import app as mock_1c_app
from app.core.config import settings
from app.core.database import init_db, save_order
from app.core.redis_client import redis_client
from app.services.matcher import catalog_matcher

logging.basicConfig(level=logging.INFO)


@pytest.mark.asyncio
async def test_apis():
    # Устанавливаем тестовое окружение
    settings.APP_ENV = "test"
    await init_db()
    await redis_client.connect()

    # Загружаем тестовую номенклатуру
    catalog_matcher.reload([
        {"sku_id": "B24-1", "name": "Яблоки Гренни Смит", "base_unit": "кг", "price": 160.0, "stock_quantity": 500.0, "aliases": ["яблоки", "яблок", "яблок еще немного", "яблочек"]},
        {"sku_id": "B24-2", "name": "Чеснок очищенный", "base_unit": "кг", "price": 250.0, "stock_quantity": 200.0, "aliases": ["чеснок", "чеснока"]},
        {"sku_id": "B24-3", "name": "Мультифрукт концентрированный", "base_unit": "кг", "price": 300.0, "stock_quantity": 100.0, "aliases": ["мультифрукт", "мультифрукта"]},
        {"sku_id": "B24-4", "name": "Картофель мытый", "base_unit": "кг", "price": 45.0, "stock_quantity": 1000.0, "aliases": ["картофель", "картофеля", "картошка"]},
        {"sku_id": "B24-5", "name": "Помидоры черри", "base_unit": "кг", "price": 280.0, "stock_quantity": 150.0, "aliases": ["помидоры черри", "помидоры", "томаты"]},
    ])

    print("=== ТЕСТИРОВАНИЕ MOCK 1C СЕРВЕРА ===")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=mock_1c_app), base_url="http://test") as client:
        # Тест создания заказа в 1С с поддержкой идемпотентности
        order_payload = {
            "client_id": "telegram:12345",
            "delivery_date": "Завтра к 10:00",
            "idempotency_key": "test-key-001",
            "items": [
                {"sku_id": "B24-5", "qty": 10.0, "unit": "кг", "price": 160.0, "name": "Яблоки Гренни Смит"}
            ]
        }
        res_create = await client.post("/api/v1/mock-1c/orders", json=order_payload)
        assert res_create.status_code == 201, f"Expected 201, got {res_create.status_code}"
        data_create = res_create.json()
        assert "ЗК-" in data_create["document_number"]
        print(f"✓ 1C заказ создан: {data_create}")

        # Проверка идемпотентности: повторный запрос возвращает тот же документ
        res_repeat = await client.post("/api/v1/mock-1c/orders", json=order_payload)
        assert res_repeat.status_code == 201
        assert res_repeat.json()["document_number"] == data_create["document_number"]
        print("✓ Идемпотентность в Mock 1C подтверждена!")

        # Тест HTML-витрины для жюри
        res_html = await client.get("/orders")
        assert res_html.status_code == 200
        assert "Журнал заказов покупателей" in res_html.text
        assert "ЗК-" in res_html.text
        print("✓ HTML витрина 1C сгенерирована успешно!")

    print("\n=== ТЕСТИРОВАНИЕ FASTAPI GATEWAY И WEBHOOKS ===")
    auth_headers = {
        "X-Webhook-Secret": settings.WEBHOOK_SECRET,
        "X-Telegram-Bot-Api-Secret-Token": settings.WEBHOOK_SECRET,
    }

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=main_app), base_url="http://test") as client:
        # 1. Тест /health
        res_health = await client.get("/health")
        assert res_health.status_code == 200
        print(f"✓ /health: {res_health.json()}")

        # 2. Тест /webhook/telegram (текстовый заказ с несколькими позициями)
        tg_payload = {
            "update_id": 1001,
            "message": {
                "message_id": 42,
                "from": {"id": 999111, "first_name": "Шеф Иван"},
                "chat": {"id": 999111, "type": "private"},
                "text": "На завтра: 2 кг яблок и 3 кг чеснока"
            }
        }
        res_tg = await client.post("/webhook/telegram", json=tg_payload, headers=auth_headers)
        assert res_tg.status_code == 200
        print("✓ /webhook/telegram обработал текстовый заказ!")

        # Проверяем, что черновик сохранился в Redis
        draft = await redis_client.get_order_draft("telegram:999111")
        assert draft is not None
        assert len(draft.get("matched_items", [])) == 2
        print(f"✓ Черновик сохранен в Redis: {len(draft['matched_items'])} позиций")

        # 2.1 Тест сценария пользователя: «56 кг мультифрукта», затем «ну и давай яблочек еще немного»
        tg_fruit_user = "telegram:888777"
        await client.post("/webhook/telegram", json={
            "update_id": 2001,
            "message": {
                "message_id": 1,
                "from": {"id": 888777},
                "chat": {"id": 888777, "type": "private"},
                "text": "56 кг мультифрукта"
            }
        }, headers=auth_headers)
        draft_fruit = await redis_client.get_order_draft(tg_fruit_user)
        assert len(draft_fruit["matched_items"]) == 1
        assert "мультифрукт" in draft_fruit["matched_items"][0]["catalog_name"].lower()

        # Второе сообщение: разговорное добавление
        await client.post("/webhook/telegram", json={
            "update_id": 2002,
            "message": {
                "message_id": 2,
                "from": {"id": 888777},
                "chat": {"id": 888777, "type": "private"},
                "text": "ну и давай яблочек еще немного"
            }
        }, headers=auth_headers)
        draft_fruit_2 = await redis_client.get_order_draft(tg_fruit_user)
        assert len(draft_fruit_2["matched_items"]) == 2
        names = [it["catalog_name"].lower() for it in draft_fruit_2["matched_items"]]
        assert any("мультифрукт" in n for n in names)
        assert any("яблок" in n for n in names)
        print("✓ Разговорное добавление «ну и давай яблочек еще немного» сохранило мультифрукт и объединило позиции!")

        # 3. Интерактивное редактирование: Добавление позиции текстом
        tg_add_payload = {
            "update_id": 1002,
            "message": {
                "message_id": 43,
                "from": {"id": 999111},
                "chat": {"id": 999111, "type": "private"},
                "text": "добавь 5 кг картофеля"
            }
        }
        res_add = await client.post("/webhook/telegram", json=tg_add_payload, headers=auth_headers)
        assert res_add.status_code == 200
        draft_after_add = await redis_client.get_order_draft("telegram:999111")
        assert len(draft_after_add["matched_items"]) == 3
        print("✓ Команда «добавь ...» успешно добавила позицию в корзину!")

        # 4. Интерактивное редактирование: Изменение количества позиции
        tg_change_payload = {
            "update_id": 1003,
            "message": {
                "message_id": 44,
                "from": {"id": 999111},
                "chat": {"id": 999111, "type": "private"},
                "text": "измени 1 на 10 кг"
            }
        }
        res_change = await client.post("/webhook/telegram", json=tg_change_payload, headers=auth_headers)
        assert res_change.status_code == 200
        draft_after_change = await redis_client.get_order_draft("telegram:999111")
        assert draft_after_change["matched_items"][0]["final_quantity"] == 10.0
        print("✓ Команда «измени 1 на 10 кг» успешно обновила количество!")

        # 5. Интерактивное редактирование: Удаление позиции по наименованию («удали чеснок»)
        tg_del_name_payload = {
            "update_id": 1004,
            "message": {
                "message_id": 45,
                "from": {"id": 999111},
                "chat": {"id": 999111, "type": "private"},
                "text": "удали чеснок"
            }
        }
        res_del_name = await client.post("/webhook/telegram", json=tg_del_name_payload, headers=auth_headers)
        assert res_del_name.status_code == 200
        draft_after_del_name = await redis_client.get_order_draft("telegram:999111")
        assert len(draft_after_del_name["matched_items"]) == 2
        assert not any("чеснок" in it["catalog_name"].lower() for it in draft_after_del_name["matched_items"])
        print("✓ Команда «удали чеснок» (по названию) успешно удалила чеснок из корзины!")

        # 5.1 Добавляем томаты и удаляем их командой «удали помидоры» (проверка кулинарных синонимов)
        tg_add_tomatoes = {
            "update_id": 1005,
            "message": {
                "message_id": 46,
                "from": {"id": 999111},
                "chat": {"id": 999111, "type": "private"},
                "text": "добавь 2 кг помидоров черри"
            }
        }
        await client.post("/webhook/telegram", json=tg_add_tomatoes, headers=auth_headers)
        draft_tomatoes = await redis_client.get_order_draft("telegram:999111")
        assert len(draft_tomatoes["matched_items"]) == 3

        tg_del_tomatoes = {
            "update_id": 1006,
            "message": {
                "message_id": 47,
                "from": {"id": 999111},
                "chat": {"id": 999111, "type": "private"},
                "text": "удали помидоры"
            }
        }
        res_del_tomatoes = await client.post("/webhook/telegram", json=tg_del_tomatoes, headers=auth_headers)
        assert res_del_tomatoes.status_code == 200
        draft_after_tomatoes_del = await redis_client.get_order_draft("telegram:999111")
        assert len(draft_after_tomatoes_del["matched_items"]) == 2
        assert not any("томат" in it["catalog_name"].lower() or "помидор" in it["catalog_name"].lower() for it in draft_after_tomatoes_del["matched_items"])
        print("✓ Команда «удали помидоры» (с синонимом томаты) успешно удалила томаты из корзины!")

        # 5.2. Интерактивное редактирование: Удаление позиции по номеру
        tg_del_payload = {
            "update_id": 1007,
            "message": {
                "message_id": 48,
                "from": {"id": 999111},
                "chat": {"id": 999111, "type": "private"},
                "text": "удали 2"
            }
        }
        res_del = await client.post("/webhook/telegram", json=tg_del_payload, headers=auth_headers)
        assert res_del.status_code == 200
        draft_after_del = await redis_client.get_order_draft("telegram:999111")
        assert len(draft_after_del["matched_items"]) == 1
        print("✓ Команда «удали 2» успешно удалила позицию из корзины!")

        # 6. Интерактивное редактирование: Удаление через callback-кнопку copilot:del:1
        tg_cb_del = {
            "update_id": 1008,
            "callback_query": {
                "id": "cb_del_01",
                "from": {"id": 999111},
                "data": "copilot:del:1"
            }
        }
        res_cb_del = await client.post("/webhook/telegram", json=tg_cb_del, headers=auth_headers)
        assert res_cb_del.status_code == 200
        draft_after_btn_del = await redis_client.get_order_draft("telegram:999111")
        assert draft_after_btn_del is None
        print("✓ Inline-кнопка «copilot:del:1» успешно очистила последнюю позицию!")

        # Создаем новый заказ для теста подтверждения
        tg_new_order = {
            "update_id": 1009,
            "message": {
                "message_id": 49,
                "from": {"id": 999111},
                "chat": {"id": 999111, "type": "private"},
                "text": "На завтра: 2 кг яблок"
            }
        }
        await client.post("/webhook/telegram", json=tg_new_order, headers=auth_headers)
        draft_new = await redis_client.get_order_draft("telegram:999111")
        assert len(draft_new["matched_items"]) == 1

        # 7. Подтверждение заказа (copilot:confirm)
        cb_confirm_payload = {
            "update_id": 1010,
            "callback_query": {
                "id": "cb_confirm_01",
                "from": {"id": 999111},
                "data": "copilot:confirm"
            }
        }
        res_confirm = await client.post("/webhook/telegram", json=cb_confirm_payload, headers=auth_headers)
        assert res_confirm.status_code == 200
        print("✓ Заказ подтвержден и отправлен в 1С!")

        # 8. Тест вебхука обратной связи от 1С (статус заказа)
        test_order = await save_order(
            session_id="telegram:999111",
            platform="telegram",
            platform_user_id="999111",
            raw_text="2 кг яблок",
            delivery_date="Завтра",
            items_data=[{"sku_id": "B24-1", "name": "Яблоки", "qty": 10.0, "unit": "кг", "price": 100.0, "total_sum": 1000.0}],
            total_amount=1000.0,
            status="CONFIRMED",
            document_number_1c="ЗК-8888",
        )
        status_payload = {
            "document_number": "ЗК-8888",
            "status": "Отгружен курьеру",
            "comment": "Водитель Алексей +79991234567, доставка к 12:00"
        }
        res_status = await client.post("/webhook/1c/status", json=status_payload, headers=auth_headers)
        assert res_status.status_code == 200
        assert res_status.json()["new_status"] == "Отгружен курьеру"
        print("✓ Вебхук статусов 1С (/webhook/1c/status) успешно обновил статус заказа и отправил пуш!")

        # 9. Тесты других каналов (VK, WhatsApp, MAX) с авторизацией
        # VK confirmation
        res_vk_conf = await client.post("/webhook/vk", json={"type": "confirmation", "group_id": 123})
        assert res_vk_conf.status_code == 200
        print(f"✓ /webhook/vk confirmation вернул: {res_vk_conf.text}")

        # VK message_new
        vk_payload = {
            "type": "message_new",
            "secret": settings.VK_SECRET_KEY,
            "object": {
                "message": {
                    "id": 88001,
                    "from_id": 555444,
                    "peer_id": 555444,
                    "text": "3 кг чеснока"
                }
            }
        }
        res_vk_msg = await client.post("/webhook/vk", json=vk_payload)
        assert res_vk_msg.status_code == 200
        print("✓ /webhook/vk message_new отработал!")

        # WhatsApp (Green-API)
        wa_payload = {
            "typeWebhook": "incomingMessageReceived",
            "idMessage": "WA-MSG-001",
            "senderData": {
                "sender": "79991234567@c.us",
                "chatId": "79991234567@c.us"
            },
            "messageData": {
                "typeMessage": "textMessage",
                "textMessageData": {
                    "textMessage": "2 кг яблок"
                }
            }
        }
        res_wa = await client.post("/webhook/whatsapp", json=wa_payload, headers=auth_headers)
        assert res_wa.status_code == 200
        print("✓ /webhook/whatsapp отработал!")

        # MAX corporate messenger
        max_payload = {
            "user_id": "max_user_001",
            "chat_id": "max_chat_001",
            "text": "2 кг яблок"
        }
        res_max = await client.post("/webhook/max", json=max_payload, headers=auth_headers)
        assert res_max.status_code == 200
        print("✓ /webhook/max отработал!")

    print("\n🎉 ВСЕ ЭНДПОИНТЫ, ИНТЕРАКТИВНОЕ РЕДАКТИРОВАНИЕ И СТАТУСЫ 1С РАБОТАЮТ БЕЗУПРЕЧНО!")


if __name__ == "__main__":
    asyncio.run(test_apis())
