from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, ConfigDict, Field, computed_field


class MatchStatus(str, Enum):
    RECOGNIZED = "RECOGNIZED"        # 🟢 Сопоставлено с высокой уверенностью (>= 0.85)
    NEEDS_REVIEW = "NEEDS_REVIEW"    # 🟡 Требует внимания оператора / повара (< 0.85)
    NOT_FOUND = "NOT_FOUND"          # 🟡 Не найдено в справочнике
    OUT_OF_STOCK = "OUT_OF_STOCK"    # 🔴 Закончился на складе (предложен аналог)


class RawItem(BaseModel):
    """Сырая товарная позиция, извлеченная нейросетью из текста/голоса."""
    model_config = ConfigDict(extra="ignore")

    raw_name: str = Field(..., description="Исходное наименование товара")
    quantity: float = Field(default=1.0, description="Количество")
    unit: str = Field(default="шт", description="Единица измерения (кг, коробка, мешок, шт)")
    note: Optional[str] = Field(default=None, description="Примечание/пожелание к позиции")


class ExtractedOrder(BaseModel):
    """Структурированный заказ, извлеченный LLM."""
    model_config = ConfigDict(extra="ignore")

    delivery_date: Optional[str] = Field(default=None, description="Желаемая дата/время доставки")
    items: list[RawItem] = Field(default_factory=list, description="Список товарных позиций")


class CatalogSKU(BaseModel):
    """Товарная позиция из каталога номенклатуры."""
    model_config = ConfigDict(extra="ignore")

    sku_id: str = Field(default="SKU-000")
    name: str = Field(..., description="Официальное наименование в 1С/Битрикс24")
    base_unit: str = Field(default="шт", description="Базовая единица учета (кг, шт, л)")
    price: float = Field(default=0.0, description="Цена за базовую единицу в рублях")
    stock_quantity: float = Field(default=999.0, description="Остаток на складе 1С")
    pack_multiple: Optional[float] = Field(default=None, description="Кратность заводской упаковки/ящика")
    analogs: list[str] = Field(default_factory=list, description="Список sku_id взаимозаменяемых аналогов")
    aliases: list[str] = Field(default_factory=list, description="Синонимы для быстрого матчинга")
    conversion_rates: dict[str, float] = Field(
        default_factory=dict,
        description="Коэффициенты пересчета тары в базовую единицу",
    )

    @classmethod
    def from_raw(cls, data: dict[str, Any]) -> "CatalogSKU":
        import re
        sku_id = data.get("sku_id") or data.get("sku") or "SKU-000"
        name = data.get("name") or data.get("catalog_name") or "Товар"
        base_unit = data.get("base_unit") or "шт"
        price = float(data.get("price") or 0.0)
        stock_quantity = float(data.get("stock_quantity") if data.get("stock_quantity") is not None else 999.0)
        pack_multiple = data.get("pack_multiple")
        analogs = data.get("analogs") or []
        aliases = data.get("aliases") or []
        rates = data.get("conversion_rates") or data.get("unit_conversions") or {}

        # Автодетекция кратности упаковки из названия, если не задана явно
        if pack_multiple is None:
            m_pack = re.search(r"(?:упаковка|пачка|коробка|ящик|фасовка)\s*(?:по\s*)?(\d+(?:[.,]\d+)?)\s*(?:шт|кг|л)?", name, re.IGNORECASE)
            if m_pack:
                try:
                    pack_multiple = float(m_pack.group(1).replace(",", "."))
                except Exception:
                    pass

        return cls(
            sku_id=str(sku_id),
            name=name,
            base_unit=base_unit,
            price=price,
            stock_quantity=stock_quantity,
            pack_multiple=pack_multiple,
            analogs=[str(a) for a in analogs if a],
            aliases=[str(a).lower().strip() for a in aliases if a],
            conversion_rates={k.lower().strip(): float(v) for k, v in rates.items()},
        )


class MatchedItem(BaseModel):
    """Спозиционированный товар с расчетом тары, суммой и индикатором уверенности."""
    model_config = ConfigDict(extra="ignore")

    raw_name: str
    raw_quantity: float
    raw_unit: str
    sku_id: Optional[str] = None
    catalog_name: Optional[str] = None
    final_quantity: float = 1.0
    base_unit: str = "шт"
    price: float = 0.0
    total_sum: float = 0.0
    confidence: float = 0.0
    status: MatchStatus = MatchStatus.NOT_FOUND
    recalculation_hint: Optional[str] = None
    note: Optional[str] = None

    # Кратность заводской упаковки
    pack_multiple: Optional[float] = None
    suggested_pack_quantity: Optional[float] = None
    pack_warning: Optional[str] = None

    # Остатки и автоподбор аналогов
    is_out_of_stock: bool = False
    suggested_analog_sku: Optional[str] = None
    suggested_analog_name: Optional[str] = None
    suggested_analog_price: Optional[float] = None
    suggested_analog_unit: Optional[str] = None


class OutboundButton(BaseModel):
    """Универсальная inline-кнопка для отправки в любой мессенджер."""
    text: str
    payload: str


class OutboundMessage(BaseModel):
    """Унифицированное исходящее сообщение."""
    platform: str
    chat_id: str
    text: str
    buttons: list[list[OutboundButton]] = Field(default_factory=list)
    parse_mode: str = "HTML"


class InboundMessage(BaseModel):
    """Унифицированное входящее сообщение из любого поддерживаемого мессенджера."""
    platform: str
    platform_user_id: str
    chat_id: str
    text: Optional[str] = None
    voice_bytes: Optional[bytes] = None
    voice_url: Optional[str] = None
    callback_data: Optional[str] = None
    message_id: Optional[str] = None

    @computed_field
    @property
    def session_id(self) -> str:
        """Глобально уникальный идентификатор сессии клиента."""
        return f"{self.platform}:{self.platform_user_id}"


class CartIntentResult(BaseModel):
    """Результат ИИ-анализа намерения шеф-повара относительно активной корзины."""
    model_config = ConfigDict(extra="ignore")

    action: str = "unknown"  # "remove", "update", "add", "confirm", "cancel", "new_order", "unknown"
    target_item: Optional[str] = None
    new_quantity: Optional[float] = None
    new_unit: Optional[str] = None
    items_to_add: list[Any] = Field(default_factory=list)
    delivery_date: Optional[str] = None

