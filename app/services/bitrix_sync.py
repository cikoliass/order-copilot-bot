import os
import re
import json
import logging
from pathlib import Path
import httpx
from dotenv import load_dotenv

load_dotenv(override=True)
logger = logging.getLogger(__name__)

CATALOG_PATH = Path(__file__).resolve().parent.parent / "data" / "catalog.json"

UNIT_MAP = {
    "штука": "шт", "шт": "шт", "упаковка": "уп", "уп": "уп",
    "килограмм": "кг", "кг": "кг", "литр": "л", "л": "л",
    "бутылка": "бут", "бут": "бут", "коробка": "кор", "кор": "кор",
    "ящик": "ящ", "ящ": "ящ", "мешок": "меш", "меш": "меш",
    "ведро": "вед", "вед": "вед", "пачка": "пач", "пач": "пач",
    "лоток": "лот", "ячейка": "яч", "блок": "бл", "пласт": "пл", "сетка": "сет"
}


def _parse_description_meta(name: str, desc_html: str) -> tuple[list[str], dict[str, float]]:
    """Извлекает 'Синонимы для заказа:' и 'Фасовка:' из описания товара в Битрикс24."""
    text = re.sub(r"<[^>]+>", "\n", desc_html or "")
    aliases = []
    conversions = {"шт": 1.0, "кг": 1.0, "л": 1.0, "уп": 1.0, "пач": 1.0, "бут": 1.0, "кор": 10.0, "ящ": 10.0}

    for line in text.splitlines():
        line_clean = line.strip()
        if line_clean.lower().startswith("синонимы"):
            parts = line_clean.split(":", 1)[-1].split(",")
            aliases = [p.strip().lower() for p in parts if p.strip()]
        elif line_clean.lower().startswith("фасовка"):
            matches = re.findall(r"1\s*([а-яА-Яa-zA-Z/]+)\s*=\s*(\d+(?:[\.,]\d+)?)", line_clean)
            for unit_raw, val_str in matches:
                val = float(val_str.replace(",", "."))
                for sub_u in unit_raw.lower().split("/"):
                    short_u = UNIT_MAP.get(sub_u.strip(), sub_u.strip()[:3])
                    conversions[short_u] = val

    if not aliases:
        clean_name = re.sub(r"[^\w\s\.]", " ", name.lower())
        words = [w for w in clean_name.split() if len(w) > 2]
        aliases = [name.lower()]
        if words:
            aliases.append(words[0])
        if len(words) >= 2:
            aliases.append(f"{words[0]} {words[1]}")

    return aliases, conversions


async def sync_catalog_from_bitrix() -> list[dict]:
    """Скачивает активные товары из Битрикс24 (crm.product.list) и обновляет каталог."""
    load_dotenv(override=True)
    bitrix_url = os.getenv("BITRIX_WEBHOOK_URL")
    if not bitrix_url or "bitrix24" not in bitrix_url:
        logger.warning("BITRIX_WEBHOOK_URL не задан в .env!")
        return []

    url = f"{bitrix_url.rstrip('/')}/crm.product.list.json"
    payload = {
        "select": ["ID", "NAME", "PRICE", "CURRENCY_ID", "MEASURE", "DESCRIPTION"],
        "filter": {"ACTIVE": "Y"},
    }

    products_b24 = []
    async with httpx.AsyncClient(timeout=20.0) as client:
        start = 0
        while True:
            payload["start"] = start
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            data = resp.json()
            products_b24.extend(data.get("result", []))
            if "next" in data:
                start = data["next"]
            else:
                break

    if not products_b24:
        logger.warning("В Битрикс24 не найдено товаров.")
        return []

    catalog_items = []
    for p in products_b24:
        b24_id = p.get("ID")
        name = (p.get("NAME") or "Товар").strip()
        price = float(p.get("PRICE") or 100.0)
        sku_code = f"B24-{b24_id}"

        aliases, conversions = _parse_description_meta(name, p.get("DESCRIPTION", ""))
        base_unit = "кг" if ("кг" in name.lower() and "2кг" not in name.lower() and "1кг" not in name.lower()) else "шт"

        catalog_items.append({
            "sku_id": sku_code,
            "sku": sku_code,
            "bitrix_product_id": int(b24_id),
            "name": name,
            "catalog_name": name,
            "price": price,
            "base_unit": base_unit,
            "aliases": aliases,
            "conversion_rates": conversions,
            "unit_conversions": conversions,
        })

    # Сохраняем кэш, чтобы matcher.py мог читать его в любой момент
    CATALOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(catalog_items, f, ensure_ascii=False, indent=2)

    logger.info("✅ Синхронизировано %d товаров из Битрикс24", len(catalog_items))
    return catalog_items