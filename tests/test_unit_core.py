import pytest
from app.api.gateway import (
    _find_cart_item_index,
    _is_safe_url_for_download,
    classify_action_text,
    format_copilot_card,
)
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

    # Разрешены публичные HTTP/HTTPS ссылки
    assert _is_safe_url_for_download("https://storage.yandexcloud.net/bucket/audio.ogg")
    assert _is_safe_url_for_download("https://platform-api2.max.ru/files/audio.ogg")


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
