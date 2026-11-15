import json
import logging
import math
import re
from pathlib import Path
from typing import Any
import httpx
from app.core.config import settings

logger = logging.getLogger(__name__)

UNIT_MAP = {
    "штука": "шт", "шт": "шт", "упаковка": "уп", "уп": "уп",
    "килограмм": "кг", "кг": "кг", "литр": "л", "л": "л",
    "бутылка": "бут", "бут": "бут", "коробка": "кор", "кор": "кор",
    "ящик": "ящ", "ящ": "ящ", "мешок": "меш", "меш": "меш",
    "ведро": "вед", "вед": "вед", "пачка": "пач", "пач": "пач",
    "лоток": "лот", "ячейка": "яч", "блок": "бл", "пласт": "пл", "сетка": "сет",
    "грамм": "г", "г": "г", "гр": "г", "миллилитр": "мл", "мл": "мл",
}

BITRIX_MEASURE_MAP = {
    "1": "м",
    "3": "л",
    "5": "г",
    "7": "кг",
    "9": "шт",
    "6": "м",
    "112": "л",
    "163": "г",
    "166": "кг",
    "796": "шт",
}

HORECA_SYNONYMS = {
    "томаты": ["помидоры", "помидор", "томат"],
    "помидоры": ["томаты", "помидор", "томат"],
    "баклажаны": ["синенькие", "баклажан"],
    "картофель": ["картошка", "картоха"],
    "картошка": ["картофель"],
    "лук репчатый": ["лук", "лучок"],
    "шампиньоны": ["грибы"],
    "курица": ["цыпленок", "куриное", "кур"],
    "филе куриное": ["грудка куриная", "филе цыпленка"],
    "масло сливочное": ["сливочное масло"],
    "масло подсолнечное": ["подсолнечное масло", "масло растительное", "растительное масло"],
}


def _parse_description_meta(name: str, desc_html: str) -> tuple[list[str], dict[str, float]]:
    """Извлекает синонимы и реальные коэффициенты фасовки из описания товара."""
    text = re.sub(r"<[^>]+>", "\n", desc_html or "")
    aliases: list[str] = []
    # Базовые коэффициенты только для стандартных единиц измерения 1:1
    conversions: dict[str, float] = {
        "шт": 1.0, "кг": 1.0, "л": 1.0, "уп": 1.0,
        "пач": 1.0, "бут": 1.0,
    }

    for line in text.splitlines():
        line_clean = line.strip()
        if line_clean.lower().startswith("синонимы"):
            parts = line_clean.split(":", 1)[-1].split(",")
            aliases.extend([p.strip().lower() for p in parts if p.strip()])
        elif line_clean.lower().startswith("фасовка"):
            matches = re.findall(r"1\s*([а-яА-Яa-zA-Z/]+)\s*=\s*(\d+(?:[\.,]\d+)?)", line_clean)
            for unit_raw, val_str in matches:
                try:
                    val = float(val_str.replace(",", "."))
                    if val <= 0 or math.isnan(val) or math.isinf(val):
                        continue
                except (ValueError, TypeError):
                    continue
                for sub_u in unit_raw.lower().split("/"):
                    short_u = UNIT_MAP.get(sub_u.strip(), sub_u.strip()[:3])
                    conversions[short_u] = val

    name_lower = name.lower().strip()
    if name_lower not in aliases:
        aliases.append(name_lower)

    # Обогащаем только полными кулинарными синонимами из словаря,
    # НО никогда не добавляем первое слово названия («сливки», «сыр», «масло») в качестве алиаса!
    for term, syns in HORECA_SYNONYMS.items():
        if term in name_lower:
            for s in syns:
                if s not in aliases:
                    aliases.append(s)

    return aliases, conversions


async def sync_catalog_from_bitrix() -> list[dict[str, Any]]:
    """
    Скачивает активные товары из CRM Битрикс24 (crm.product.list),
    нормализует номенклатуру, сохраняет кэш на диск и возвращает список.
    """
    if settings.APP_ENV == "test":
        return []

    bitrix_url = settings.BITRIX_WEBHOOK_URL
    if not bitrix_url or "bitrix24" not in bitrix_url:
        logger.warning("BITRIX_WEBHOOK_URL не настроен в .env! Пропуск live-синхронизации.")
        return []

    url = f"{bitrix_url.rstrip('/')}/crm.product.list.json"
    payload = {
        "select": ["ID", "NAME", "PRICE", "CURRENCY_ID", "MEASURE", "DESCRIPTION"],
        "filter": {"ACTIVE": "Y"},
    }

    products_b24 = []
    async with httpx.AsyncClient(timeout=settings.BITRIX_TIMEOUT_SEC) as client:
        start = 0
        pages = 0
        max_pages = 100  # До 5000 товаров, защита от бесконечного цикла
        while pages < max_pages:
            pages += 1
            payload["start"] = start
            try:
                resp = await client.post(url, json=payload)
                if resp.status_code != 200:
                    logger.error("Ошибка Битрикс24 при выгрузке каталога: HTTP %d", resp.status_code)
                    break
                data = resp.json()
                products_b24.extend(data.get("result", []))
                next_start = data.get("next")
                if next_start is not None and str(next_start).isdigit() and int(next_start) > start:
                    start = int(next_start)
                else:
                    break
            except Exception as exc:
                from app.services.integrations import _sanitize_error_msg
                logger.error("Сетевая ошибка при обращении к Битрикс24: %s", _sanitize_error_msg(exc))
                break

    if not products_b24:
        logger.warning("В CRM Битрикс24 не найдено активных товаров.")
        return []

    catalog_items = []
    for p in products_b24:
        b24_id = p.get("ID")
        name = (p.get("NAME") or "Товар").strip()
        raw_price = p.get("PRICE")
        try:
            price = max(0.0, float(raw_price)) if raw_price is not None and str(raw_price).strip() != "" else 0.0
            if math.isnan(price) or math.isinf(price):
                price = 0.0
        except (ValueError, TypeError):
            price = 0.0
        sku_code = f"B24-{b24_id}"

        aliases, conversions = _parse_description_meta(name, p.get("DESCRIPTION", ""))

        measure_raw = str(p.get("MEASURE") or "").strip()
        base_unit = BITRIX_MEASURE_MAP.get(measure_raw)
        if not base_unit:
            name_lower = name.lower()
            if re.search(r"\b(?:кг|килограмм|кило)\b", name_lower):
                base_unit = "кг"
            elif re.search(r"\b(?:л|литр)\b", name_lower):
                base_unit = "л"
            else:
                base_unit = "шт"

        catalog_items.append({
            "sku_id": sku_code,
            "sku": sku_code,
            "bitrix_product_id": int(b24_id) if str(b24_id).isdigit() else 0,
            "name": name,
            "catalog_name": name,
            "price": price,
            "base_unit": base_unit,
            "aliases": aliases,
            "conversion_rates": conversions,
            "unit_conversions": conversions,
        })

    return catalog_items
