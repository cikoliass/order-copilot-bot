import json
import logging
from enum import Enum
from pathlib import Path
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field
from rapidfuzz import fuzz

logger = logging.getLogger(__name__)

CATALOG_PATH = Path(__file__).resolve().parent.parent / "data" / "catalog.json"


# --- Модели для parser.py (чтобы не было ImportError) ---
class ExtractedItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    raw_name: str = Field(..., description="Исходное название товара из текста")
    quantity: float = Field(default=1.0, description="Количество")
    unit: str = Field(default="шт", description="Единица измерения")
    note: Optional[str] = Field(default=None, description="Комментарий к позиции")


class ExtractedOrder(BaseModel):
    model_config = ConfigDict(extra="forbid")

    delivery_date: Optional[str] = Field(default=None, description="Дата доставки")
    items: list[ExtractedItem] = Field(default_factory=list, description="Позиции заказа")


# --- Модели для сопоставления и Co-Pilot UI ---
class MatchStatus(str, Enum):
    RECOGNIZED = "RECOGNIZED"
    NEEDS_REVIEW = "NEEDS_REVIEW"
    NOT_FOUND = "NOT_FOUND"


class CatalogSKU(BaseModel):
    sku_id: str = Field(default="SKU-000")
    name: str
    base_unit: str = "шт"
    price: float = 100.0
    aliases: list[str] = Field(default_factory=list)
    conversion_rates: dict[str, float] = Field(default_factory=dict)

    @classmethod
    def from_raw(cls, data: dict[str, Any]) -> "CatalogSKU":
        sku_id = data.get("sku_id") or data.get("sku") or "SKU-000"
        name = data.get("name") or data.get("catalog_name") or "Товар"
        base_unit = data.get("base_unit") or "шт"
        price = float(data.get("price") or 100.0)
        aliases = data.get("aliases") or []
        rates = data.get("conversion_rates") or data.get("unit_conversions") or {}
        return cls(
            sku_id=str(sku_id),
            name=name,
            base_unit=base_unit,
            price=price,
            aliases=aliases,
            conversion_rates=rates,
        )


class MatchedItem(BaseModel):
    raw_name: str
    raw_quantity: float
    raw_unit: str
    sku_id: Optional[str] = None
    catalog_name: Optional[str] = None
    base_unit: str = "шт"
    final_quantity: float = 1.0
    price: float = 0.0
    total_sum: float = 0.0
    confidence: float = 0.0
    status: MatchStatus = MatchStatus.NOT_FOUND
    recalculation_hint: Optional[str] = None
    note: Optional[str] = None


class CatalogMatcher:
    def __init__(self, catalog_path: Path = CATALOG_PATH):
        self.catalog_path = catalog_path
        self.catalog: list[CatalogSKU] = self._load_catalog()

    def _load_catalog(self) -> list[CatalogSKU]:
        """Если локального файла нет — не падаем, возвращаем пустой список до синхронизации с Битрикс24."""
        if not self.catalog_path.exists():
            logger.info("Локальный catalog.json не найден, ожидание загрузки из Битрикс24...")
            return []
        try:
            with open(self.catalog_path, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
            return [CatalogSKU.from_raw(item) for item in raw_data]
        except Exception as exc:
            logger.warning("Ошибка чтения каталога %s: %s", self.catalog_path, exc)
            return []

    def reload(self, raw_items: Optional[list[dict]] = None) -> None:
        """Обновляет каталог в памяти из переданного списка Битрикс24 или с диска."""
        if raw_items is not None:
            self.catalog = [CatalogSKU.from_raw(item) for item in raw_items]
        else:
            self.catalog = self._load_catalog()

    def match_single(self, raw_item: Any) -> MatchedItem:
        raw_name = getattr(raw_item, "raw_name", None) or getattr(raw_item, "name", None) or "Товар"
        raw_qty = float(getattr(raw_item, "quantity", None) or getattr(raw_item, "raw_quantity", 1.0))
        raw_unit = str(getattr(raw_item, "unit", None) or getattr(raw_item, "raw_unit", "шт")).lower().replace(".", "").strip()
        note = getattr(raw_item, "note", None)

        if not self.catalog:
            self.reload()

        best_sku: Optional[CatalogSKU] = None
        best_score = 0.0
        query = raw_name.lower().strip()

        for entry in self.catalog:
            candidates = [entry.name.lower()] + [a.lower() for a in entry.aliases]
            for cand in candidates:
                score = fuzz.WRatio(query, cand)
                if score > best_score:
                    best_score = score
                    best_sku = entry

        conf = round(best_score / 100.0, 2)

        if not best_sku or conf < 0.45:
            return MatchedItem(
                raw_name=raw_name,
                raw_quantity=raw_qty,
                raw_unit=raw_unit,
                sku_id=None,
                catalog_name=None,
                base_unit=raw_unit,
                final_quantity=raw_qty,
                price=0.0,
                total_sum=0.0,
                confidence=conf,
                status=MatchStatus.NOT_FOUND,
                note=note,
            )

        rates = {k.lower(): v for k, v in best_sku.conversion_rates.items()}
        recalc_hint = None

        if raw_unit in rates:
            factor = float(rates[raw_unit])
            final_qty = round(raw_qty * factor, 2)
            if raw_unit != best_sku.base_unit.lower() or factor != 1.0:
                recalc_hint = f"из {raw_qty:g} {raw_unit} × {factor:g}"
            unit_ok = True
        elif raw_unit == best_sku.base_unit.lower():
            final_qty = raw_qty
            unit_ok = True
        else:
            final_qty = raw_qty
            recalc_hint = f"ед. «{raw_unit}» без коэфф."
            unit_ok = False

        status = MatchStatus.RECOGNIZED if (conf >= 0.75 and unit_ok) else MatchStatus.NEEDS_REVIEW
        total_sum = round(final_qty * best_sku.price, 2)

        return MatchedItem(
            raw_name=raw_name,
            raw_quantity=raw_qty,
            raw_unit=raw_unit,
            sku_id=best_sku.sku_id,
            catalog_name=best_sku.name,
            base_unit=best_sku.base_unit,
            final_quantity=final_qty,
            price=best_sku.price,
            total_sum=total_sum,
            confidence=conf,
            status=status,
            recalculation_hint=recalc_hint,
            note=note,
        )

    def match_order(self, extracted_order: Any) -> list[MatchedItem]:
        items = getattr(extracted_order, "items", extracted_order)
        if not isinstance(items, list):
            return []
        return [self.match_single(it) for it in items]


catalog_matcher = CatalogMatcher()
matcher = catalog_matcher