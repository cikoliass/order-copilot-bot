import html
import pytest
import httpx
from app.api.gateway import (
    _escape_like_wildcards,
    _find_cart_item_index,
    _is_safe_url_for_download,
    classify_action_text,
    format_copilot_card,
)
from app.main import app as main_app
from mock_server.main import SAVED_ORDERS, view_orders_journal
from app.services.bitrix_sync import _parse_description_meta
from app.services.matcher import CatalogMatcher
from app.services.schemas import CatalogSKU, MatchStatus, MatchedItem, RawItem


def test_classify_action_text_negations_and_safety():
    # Отрицание никогда не должно подтверждать заказ!
    assert classify_action_text("не отправляй") is None
    assert classify_action_text("не подтверждаю") is None
    assert classify_action_text("не надо отправлять") is None
    assert classify_action_text("ни в коем случае не заказывай") is None

    # «верни лимоны» — это операция с позицией, а не подтверждение заказа!
    assert classify_action_text("верни лимоны") is None

    # «отмени лимоны» — это удаление конкретного товара, а не сброс всей корзины!
    assert classify_action_text("отмени лимоны") is None
    assert classify_action_text("удали чеснок") is None
    assert classify_action_text("вычеркни сыр") is None

    # Явные команды подтверждения
    assert classify_action_text("подтверждаю") == "copilot:confirm"
    assert classify_action_text("подтвердить") == "copilot:confirm"
    assert classify_action_text("отправляй") == "copilot:confirm"
    assert classify_action_text("все верно") == "copilot:confirm"
    assert classify_action_text("1") == "copilot:confirm"
    assert classify_action_text("да") == "copilot:confirm"
    assert classify_action_text("все ништяк везите") == "copilot:confirm"

    # Явные команды отмены всего заказа
    assert classify_action_text("отмени заказ") == "copilot:cancel"
    assert classify_action_text("отмена заказа") == "copilot:cancel"
    assert classify_action_text("сбрось все") == "copilot:cancel"
    assert classify_action_text("2") == "copilot:cancel"
    assert classify_action_text("отмена") == "copilot:cancel"

    # Идеальный заказ
    assert classify_action_text("3") == "copilot:predict"
    assert classify_action_text("идеальный заказ") == "copilot:predict"
    assert classify_action_text("повтори") == "copilot:predict"


def test_unit_conversions_metric():
    matcher = CatalogMatcher()
    sku = CatalogSKU(
        sku_id="SKU-KG-01",
        name="Томаты черри",
        base_unit="кг",
        price=300.0,
        stock_quantity=100.0,
        aliases=["томаты черри", "черри"],
        conversion_rates={"уп": 0.25},
    )
    matcher.reload([sku.model_dump()])

    # 1. Запрос 500 г при базовой единице кг должен пересчитаться в 0.5 кг!
    res_g = matcher.match_order_sync([RawItem(raw_name="томаты черри", quantity=500.0, unit="г")])
    assert len(res_g) == 1
    item = res_g[0]
    assert item.status == MatchStatus.RECOGNIZED
    assert item.final_quantity == 0.5
    assert item.base_unit == "кг"
    assert item.total_sum == 150.0
    assert "пересчитано" in (item.recalculation_hint or "")

    # 2. Пересчет тары из conversion_rates: 2 уп по 0.25 кг = 0.5 кг
    res_pack = matcher.match_order_sync([RawItem(raw_name="томаты черри", quantity=2.0, unit="уп")])
    assert res_pack[0].final_quantity == 0.5
    assert res_pack[0].status == MatchStatus.RECOGNIZED

    # 3. Неизвестная тара «бочка», которой нет в карточке:
    res_unknown = matcher.match_order_sync([RawItem(raw_name="томаты черри", quantity=1.0, unit="бочка")])
    assert res_unknown[0].status == MatchStatus.NEEDS_REVIEW
    assert "неизвестная тара" in (res_unknown[0].recalculation_hint or "")


def test_matcher_no_false_positives_for_single_word():
    matcher = CatalogMatcher()
    skus = [
        CatalogSKU(
            sku_id="SKU-CREAM-33",
            name="Сливки Петмол 33% 1л ультрапастеризованные",
            base_unit="л",
            price=340.0,
            stock_quantity=50.0,
            aliases=["сливки петмол", "петмол 33%"],
        ),
        CatalogSKU(
            sku_id="SKU-COCONUT",
            name="Сливки кокосовые Aroy-D 1л",
            base_unit="л",
            price=420.0,
            stock_quantity=30.0,
            aliases=["сливки кокосовые", "кокосовые сливки", "aroy-d"],
        ),
    ]
    matcher.reload([s.model_dump() for s in skus])

    # Запрос «сливки кокосовые» должен сопоставиться СТРОГО с кокосовыми сливками,
    # а не с первыми попавшимися «Сливки Петмол 33%»!
    res = matcher.match_order_sync([RawItem(raw_name="сливки кокосовые", quantity=2.0, unit="л")])
    assert len(res) == 1
    assert res[0].sku_id == "SKU-COCONUT"
    assert "кокосовые" in res[0].catalog_name.lower()


def test_bitrix_sync_no_invented_coefficients_and_no_first_word_alias():
    aliases, conversions = _parse_description_meta("Сливки Петмол 33% 1л", "<p>Описание</p>")

    # Проверяем, что нет выдуманных коэффициентов «кор: 10, ящ: 10, меш: 25»
    assert "кор" not in conversions or conversions.get("кор") == 1.0
    assert "ящ" not in conversions or conversions.get("ящ") == 1.0
    assert "меш" not in conversions or conversions.get("меш") == 1.0

    # Проверяем, что первое слово «сливки» НЕ добавлено в алиасы само по себе
    assert "сливки" not in aliases
    assert "сливки петмол 33% 1л" in aliases


def test_ssrf_safety_checker():
    # Запрещены локальные хосты и внутренние приватные подсети
    assert not _is_safe_url_for_download("http://localhost/audio.ogg")
    assert not _is_safe_url_for_download("http://127.0.0.1/audio.ogg")
    assert not _is_safe_url_for_download("http://192.168.1.1/secret.wav")
    assert not _is_safe_url_for_download("http://10.0.0.5/voice.ogg")
    assert not _is_safe_url_for_download("ftp://example.com/voice.ogg")
    assert not _is_safe_url_for_download("file:///etc/passwd")

    # Защита от DNS Rebinding (домены, резолвящиеся в loopback/private)
    assert not _is_safe_url_for_download("http://127.0.0.1.nip.io/audio.ogg")

    # Защита от Userinfo в URL (обход парсера через @)
    assert not _is_safe_url_for_download("http://admin:secret@storage.yandexcloud.net/audio.ogg")

    # Защита от сканирования внутренних портов (SSH, Redis, DB)
    assert not _is_safe_url_for_download("http://storage.yandexcloud.net:22/audio.ogg")
    assert not _is_safe_url_for_download("http://storage.yandexcloud.net:6379/audio.ogg")

    # Разрешены публичные HTTP/HTTPS ссылки
    assert _is_safe_url_for_download("https://storage.yandexcloud.net/bucket/audio.ogg")
    assert _is_safe_url_for_download("https://platform-api2.max.ru/files/audio.ogg")


def test_escape_like_wildcards():
    # Защита от wildcard injection в SQL LIKE
    raw = "ЗК-1094%_\\test"
    escaped = _escape_like_wildcards(raw)
    assert escaped == "ЗК-1094\\%\\_\\\\test"


@pytest.mark.asyncio
async def test_mock_server_xss_protection():
    # Проверка защиты от Stored XSS в HTML-дашборде 1С
    malicious_order = {
        "doc_num": "<script>alert('doc')</script>",
        "client_id": "<img src=x onerror=alert('xss')>",
        "delivery_date": "<b onmouseover=alert(1)>срок</b>",
        "total_amount": 100.0,
        "state": "<script>evil()</script>",
        "created_at": "01.01.2026",
        "items": [
            {
                "sku_id": "<script>xss()</script>",
                "name": "<svg onload=alert(1)>",
                "qty": 1.0,
                "unit": "<script>",
                "price": 100.0,
            }
        ],
    }
    SAVED_ORDERS.insert(0, malicious_order)
    response = await view_orders_journal()
    html_text = response.body.decode("utf-8")

    # Убеждаемся, что сырые теги скриптов и вредоносных обработчиков отсутствуют в HTML
    assert "<script>alert('doc')</script>" not in html_text
    assert "<img src=x onerror=alert('xss')>" not in html_text
    assert "<script>evil()</script>" not in html_text
    assert "<svg onload=alert(1)>" not in html_text

    # Убеждаемся, что спецсимволы корректно экранированы
    assert "&lt;script&gt;alert(&#x27;doc&#x27;)&lt;/script&gt;" in html_text or "&lt;script&gt;" in html_text
    assert "&lt;img src=x" in html_text


@pytest.mark.asyncio
async def test_security_headers_and_body_size_limit():
    transport = httpx.ASGITransport(app=main_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        # Проверяем заголовки безопасности
        resp = await client.get("/")
        assert resp.status_code == 200
        assert resp.headers["X-Content-Type-Options"] == "nosniff"
        assert resp.headers["X-Frame-Options"] == "DENY"
        assert resp.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
        assert "1; mode=block" in resp.headers["X-XSS-Protection"]

        # Проверяем лимит размера тела запроса (отправка огромного Content-Length)
        huge_headers = {"Content-Length": str(30 * 1024 * 1024), "Content-Type": "application/json"}
        huge_resp = await client.post("/webhook/telegram", headers=huge_headers, content=b"")
        assert huge_resp.status_code == 413
        assert "Payload Too Large" in huge_resp.text


def test_cart_item_search_by_name():
    items = [
        {"raw_name": "Томаты черри", "catalog_name": "Томаты черри свежие 250г", "final_quantity": 2.0},
        {"raw_name": "Лимоны", "catalog_name": "Лимоны узбекские", "final_quantity": 5.0},
    ]

    # Поиск по синониму: «помидоры» находит «Томаты черри»
    idx_tomatoes = _find_cart_item_index("помидоры", items)
    assert idx_tomatoes == 0

    # Поиск по точному названию
    idx_lemons = _find_cart_item_index("лимоны", items)
    assert idx_lemons == 1

    # Несуществующий товар
    assert _find_cart_item_index("авокадо", items) == -1
