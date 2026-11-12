import asyncio
import logging
import math
import re
from typing import Any, Optional
from rapidfuzz import fuzz
from app.core.config import settings
from app.core.redis_client import redis_client
from app.services.schemas import CatalogSKU, MatchStatus, MatchedItem, RawItem

logger = logging.getLogger(__name__)

# Словарь нормализации тары и единиц измерения
UNIT_ALIASES: dict[str, str] = {
    "кор": "кор", "коробка": "кор", "коробки": "кор", "коробок": "кор", "к": "кор",
    "ящ": "ящ", "ящик": "ящ", "ящика": "ящ", "ящиков": "ящ",
    "меш": "меш", "мешок": "меш", "мешка": "меш", "мешков": "меш",
    "кг": "кг", "килограмм": "кг", "килограмма": "кг", "килограммов": "кг",
    "г": "г", "гр": "г", "грамм": "г", "грамма": "г", "граммов": "г",
    "мл": "мл", "миллилитр": "мл", "миллилитра": "мл", "миллилитров": "мл",
    "шт": "шт", "штука": "шт", "штуки": "шт", "штук": "шт",
    "л": "л", "литр": "л", "литра": "л", "литров": "л",
    "уп": "уп", "упаковка": "уп", "упаковки": "уп", "упаковок": "уп",
    "пач": "пач", "пачка": "пач", "пачки": "пач", "пачек": "пач",
    "вед": "вед", "ведро": "вед", "ведра": "вед", "ведер": "вед",
    "бут": "бут", "бутылка": "бут", "бутылки": "бут", "бутылок": "бут",
    "лот": "лот", "лоток": "лот", "лотка": "лот", "лотков": "лот",
    "сет": "сет", "сетка": "сет", "сетки": "сет", "сеток": "сет",
}

PHYSICAL_CONVERSIONS: dict[tuple[str, str], float] = {
    ("г", "кг"): 0.001,
    ("кг", "г"): 1000.0,
    ("мл", "л"): 0.001,
    ("л", "мл"): 1000.0,
}


def normalize_unit(unit: str) -> str:
    cleaned = re.sub(r"[^\w]", "", unit.lower().strip())
    return UNIT_ALIASES.get(cleaned, cleaned)


class CatalogMatcher:
    """
    Потокобезопасный сопоставитель номенклатуры с автоматическим пересчетом тары.
    Все товары хранятся в оперативной памяти и загружаются напрямую из CRM Битрикс24.
    """

    def __init__(self):
        self.catalog: list[CatalogSKU] = []
        self._exact_alias_index: dict[str, CatalogSKU] = {}
        self._ambiguous_aliases: set[str] = set()
        self._stop_list: set[str] = set()
        self._stock_overrides: dict[str, float] = {}

    async def restore_state_from_redis(self) -> None:
        """Восстановление стоп-листа и остатков из Redis при рестарте."""
        try:
            persisted_stop = await redis_client.get_stop_list()
            self._stop_list.update(persisted_stop)
            persisted_stocks = await redis_client.get_stock_overrides()
            self._stock_overrides.update(persisted_stocks)
            logger.info("Восстановлено из Redis: стоп-лист %d шт., остатки %d шт.", len(self._stop_list), len(self._stock_overrides))
        except Exception as exc:
            logger.warning("Не удалось восстановить состояние стоп-листа из Redis: %s", exc)

    def mark_out_of_stock(self, sku_id: str) -> None:
        """Помещает товар в оперативный стоп-лист (остаток = 0)."""
        s = str(sku_id).strip()
        self._stop_list.add(s)
        try:
            asyncio.create_task(redis_client.add_stop_list_item(s))
        except RuntimeError:
            pass

    def mark_in_stock(self, sku_id: str, quantity: float = 999.0) -> None:
        """Снимает товар со стоп-листа и обновляет остаток."""
        s = str(sku_id).strip()
        self._stop_list.discard(s)
        self._stock_overrides[s] = quantity
        try:
            asyncio.create_task(redis_client.remove_stop_list_item(s))
            asyncio.create_task(redis_client.set_stock_override(s, quantity))
        except RuntimeError:
            pass

    def get_stock(self, sku: CatalogSKU) -> float:
        """Возвращает текущий остаток товара с учетом оперативного стоп-листа 1С."""
        if sku.sku_id in self._stop_list:
            return 0.0
        return self._stock_overrides.get(sku.sku_id, sku.stock_quantity)

    def find_best_analog(self, out_of_stock_sku: CatalogSKU, query: str = "") -> Optional[CatalogSKU]:
        """Ищет ближайший доступный на складе аналог (в той же категории/семействе товаров)."""
        # 1. Проверяем заранее заданные аналоги
        for a_id in out_of_stock_sku.analogs:
            cand = next((s for s in self.catalog if s.sku_id == a_id), None)
            if cand and self.get_stock(cand) > 0:
                return cand

        # 2. Нечеткий поиск похожих позиций с ненулевым остатком
        base_name = out_of_stock_sku.name.lower()
        search_query = query.lower().strip() or base_name
        words = [w for w in search_query.split() if len(w) >= 3]

        best_cand: Optional[CatalogSKU] = None
        best_score = 0.0

        for entry in self.catalog:
            if entry.sku_id == out_of_stock_sku.sku_id:
                continue
            if self.get_stock(entry) <= 0:
                continue
            if entry.base_unit != out_of_stock_sku.base_unit:
                continue

            cand_name = entry.name.lower()
            score = max(
                fuzz.token_set_ratio(search_query, cand_name),
                fuzz.token_sort_ratio(search_query, cand_name),
            )
            if words and words[0] in cand_name:
                score += 10.0

            if score > best_score:
                best_score = score
                best_cand = entry

        if best_cand and best_score >= 55.0:
            return best_cand
        return None

    def reload(self, raw_items: Optional[list[dict[str, Any]]] = None) -> None:
        """Перезагрузка каталога напрямую из списка товаров Битрикс24 (хранение в RAM)."""
        if raw_items is not None:
            self.catalog = [CatalogSKU.from_raw(it) for it in raw_items]
        else:
            self.catalog = []

        # Построение быстрого индекса точных совпадений без перезаписи дублей
        self._exact_alias_index.clear()
        self._ambiguous_aliases.clear()

        for sku in self.catalog:
            norm_name = sku.name.lower().strip()
            if norm_name in self._exact_alias_index and self._exact_alias_index[norm_name].sku_id != sku.sku_id:
                self._ambiguous_aliases.add(norm_name)
            else:
                self._exact_alias_index[norm_name] = sku

            for alias in sku.aliases:
                norm_a = alias.lower().strip()
                if not norm_a:
                    continue
                if norm_a in self._exact_alias_index and self._exact_alias_index[norm_a].sku_id != sku.sku_id:
                    self._ambiguous_aliases.add(norm_a)
                else:
                    self._exact_alias_index[norm_a] = sku

        # Удаляем коллизии из точного индекса
        for amb in self._ambiguous_aliases:
            self._exact_alias_index.pop(amb, None)

        logger.info("Каталог номенклатуры в памяти обновлен: %d товаров из Битрикс24", len(self.catalog))

    def _calc_fuzzy_score(self, query: str, cand: str) -> float:
        """Взвешенный скоринг с защитой от ложных совпадений одного слова."""
        score_sort = float(fuzz.token_sort_ratio(query, cand))
        score_set = float(fuzz.token_set_ratio(query, cand))

        q_tokens = set(re.findall(r"\w+", query.lower()))
        c_tokens = set(re.findall(r"\w+", cand.lower()))

        if not q_tokens or not c_tokens:
            return 0.0

        overlap = q_tokens.intersection(c_tokens)
        q_coverage = len(overlap) / len(q_tokens)

        # Если в запросе есть ключевые слова, которых нет в кандидате (например, «кокосовые»),
        # token_set_ratio штрафуется пропорционально доле покрытия
        if q_coverage < 1.0:
            effective_set_score = score_set * q_coverage
        else:
            effective_set_score = score_set * 0.95

        return max(score_sort, effective_set_score)

    def _match_single_sync(self, raw_item: RawItem) -> MatchedItem:
        raw_name = raw_item.raw_name.strip()
        raw_qty = float(raw_item.quantity if raw_item.quantity > 0 else 1.0)
        norm_raw_unit = normalize_unit(raw_item.unit)
        note = raw_item.note

        if not self.catalog:
            return MatchedItem(
                raw_name=raw_name,
                raw_quantity=raw_qty,
                raw_unit=norm_raw_unit,
                status=MatchStatus.NOT_FOUND,
                note=note,
            )

        query = raw_name.lower().strip()

        # 1. Точный поиск по индексу алиасов и названий O(1)
        best_sku: Optional[CatalogSKU] = self._exact_alias_index.get(query)
        best_score = 100.0 if best_sku else 0.0

        # 2. Нечеткий поиск через RapidFuzz при отсутствии точного совпадения
        if not best_sku:
            for entry in self.catalog:
                candidates = [entry.name.lower()] + entry.aliases
                for cand in candidates:
                    score = self._calc_fuzzy_score(query, cand)
                    if score > best_score:
                        best_score = float(score)
                        best_sku = entry
                        if best_score >= 98.0:
                            break
                if best_score >= 98.0:
                    break

        confidence = round(best_score / 100.0, 2)

        # Если товар не похож (score < 65%) — строго NOT_FOUND
        if not best_sku or confidence < 0.65:
            return MatchedItem(
                raw_name=raw_name,
                raw_quantity=raw_qty,
                raw_unit=norm_raw_unit,
                final_quantity=raw_qty,
                base_unit=norm_raw_unit,
                confidence=confidence,
                status=MatchStatus.NOT_FOUND,
                note=note,
            )

        # 3. Пересчет тары по матрице conversion_rates + физические единицы (г->кг, мл->л)
        base_unit = normalize_unit(best_sku.base_unit)
        rates = {normalize_unit(k): float(v) for k, v in best_sku.conversion_rates.items()}

        final_qty = raw_qty
        recalc_hint: Optional[str] = None
        unit_matched = True

        if norm_raw_unit in rates:
            factor = rates[norm_raw_unit]
            final_qty = round(raw_qty * factor, 3)
            if norm_raw_unit != base_unit or factor != 1.0:
                recalc_hint = f"из {raw_qty:g} {norm_raw_unit} × {factor:g}"
        elif (norm_raw_unit, base_unit) in PHYSICAL_CONVERSIONS:
            factor = PHYSICAL_CONVERSIONS[(norm_raw_unit, base_unit)]
            final_qty = round(raw_qty * factor, 3)
            recalc_hint = f"из {raw_qty:g} {norm_raw_unit} пересчитано в {final_qty:g} {base_unit}"
        elif norm_raw_unit == base_unit:
            final_qty = raw_qty
        else:
            # Тара не найдена в коэффициентах товара и не конвертируется стандартно
            unit_matched = False
            recalc_hint = f"неизвестная тара «{norm_raw_unit}», требуется уточнение"

        total_sum = round(final_qty * best_sku.price, 2)

        # 4. Проверка кратности упаковки
        pack_multiple = best_sku.pack_multiple
        suggested_pack_quantity: Optional[float] = None
        pack_warning: Optional[str] = None

        if settings.CHECK_PACK_MULTIPLES and pack_multiple and pack_multiple > 1.0:
            if norm_raw_unit == base_unit:
                rem = final_qty % pack_multiple
                if rem != 0:
                    suggested_qty = float(math.ceil(final_qty / pack_multiple) * pack_multiple)
                    suggested_pack_quantity = suggested_qty
                    pack_warning = (
                        f"Отгрузка кратно {pack_multiple:g} {base_unit} (у вас {final_qty:g} {base_unit}). "
                        f"Округлить до {suggested_qty:g} {base_unit}?"
                    )

        # 5. Проверка остатка на складе 1С и автоподбор аналогов
        is_out_of_stock = False
        suggested_analog_sku: Optional[str] = None
        suggested_analog_name: Optional[str] = None
        suggested_analog_price: Optional[float] = None
        suggested_analog_unit: Optional[str] = None

        stock = self.get_stock(best_sku)
        if settings.SUGGEST_OUT_OF_STOCK_ANALOGS and (stock <= 0 or best_sku.sku_id in self._stop_list):
            is_out_of_stock = True
            status = MatchStatus.OUT_OF_STOCK
            analog = self.find_best_analog(best_sku, query=raw_name)
            if analog:
                suggested_analog_sku = analog.sku_id
                suggested_analog_name = analog.name
                suggested_analog_price = analog.price
                suggested_analog_unit = analog.base_unit
        else:
            if confidence >= settings.CONFIDENCE_THRESHOLD and unit_matched and best_sku.price > 0:
                status = MatchStatus.RECOGNIZED
            else:
                status = MatchStatus.NEEDS_REVIEW

        return MatchedItem(
            raw_name=raw_name,
            raw_quantity=raw_qty,
            raw_unit=norm_raw_unit,
            sku_id=best_sku.sku_id,
            catalog_name=best_sku.name,
            final_quantity=final_qty,
            base_unit=base_unit,
            price=best_sku.price,
            total_sum=total_sum,
            confidence=confidence,
            status=status,
            recalculation_hint=recalc_hint,
            note=note,
            pack_multiple=pack_multiple,
            suggested_pack_quantity=suggested_pack_quantity,
            pack_warning=pack_warning,
            is_out_of_stock=is_out_of_stock,
            suggested_analog_sku=suggested_analog_sku,
            suggested_analog_name=suggested_analog_name,
            suggested_analog_price=suggested_analog_price,
            suggested_analog_unit=suggested_analog_unit,
        )

    def match_order_sync(self, raw_items: list[RawItem]) -> list[MatchedItem]:
        """Синхронная обработка списка позиций."""
        return [self._match_single_sync(item) for item in raw_items]

    async def match_order_async(self, raw_items: list[RawItem]) -> list[MatchedItem]:
        """
        Асинхронная неблокирующая обработка.
        Вызывает RapidFuzz строго в отдельном системном потоке через asyncio.to_thread().
        Если каталог еще не загружен — подтягивает его напрямую из Битрикс24 на лету.
        """
        if not self.catalog and settings.BITRIX_WEBHOOK_URL:
            from app.services.bitrix_sync import sync_catalog_from_bitrix
            try:
                synced = await sync_catalog_from_bitrix()
                if synced:
                    self.reload(synced)
            except Exception as exc:
                logger.error("Ошибка автозагрузки товаров из Битрикс24: %s", exc)

        return await asyncio.to_thread(self.match_order_sync, raw_items)


catalog_matcher = CatalogMatcher()
