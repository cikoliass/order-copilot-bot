import logging
from collections import defaultdict
from statistics import median
from typing import Any
from app.core.database import get_user_order_history
from app.services.matcher import catalog_matcher
from app.services.schemas import MatchStatus, MatchedItem

logger = logging.getLogger(__name__)


class OrderPredictor:
    """
    Интеллектуальный модуль прогнозирования заказов (Частотно-объемный скоринг).
    Формирует «Идеальный заказ» и подсказывает «Забытые товары».
    """

    async def predict_ideal_order(
        self, session_id: str, top_k: int = 6, min_frequency: float = 0.0
    ) -> list[MatchedItem]:
        """
        Формирует список рекомендуемых товаров на основе частоты и медианных объемов закупок клиента.
        """
        history = await get_user_order_history(session_id, limit=50)
        if not history:
            return []

        total_orders = len(history)
        sku_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "quantities": [], "last_item": None}
        )

        for order in history:
            seen_skus = set()
            for it in order:
                sku = it.get("sku_id") or it.get("sku")
                if not sku:
                    continue
                qty = float(it.get("final_quantity") or it.get("qty") or 1.0)
                if sku not in seen_skus:
                    sku_stats[sku]["count"] += 1
                    seen_skus.add(sku)
                sku_stats[sku]["quantities"].append(qty)
                if sku_stats[sku]["last_item"] is None:
                    sku_stats[sku]["last_item"] = it

        if not sku_stats:
            return []

        # Фильтрация по минимальной частоте
        filtered_stats = {
            sku: info
            for sku, info in sku_stats.items()
            if (info["count"] / total_orders) >= min_frequency
        }
        if not filtered_stats:
            return []

        # Ранжирование по частоте встречаемости в заказах и объему
        ranked = sorted(
            filtered_stats.items(),
            key=lambda kv: (kv[1]["count"] / total_orders, len(kv[1]["quantities"])),
            reverse=True,
        )

        cat_map = {item.sku_id: item for item in catalog_matcher.catalog}
        result: list[MatchedItem] = []

        for sku, info in ranked[:top_k]:
            cat_item = cat_map.get(sku)
            sample = info["last_item"]
            opt_qty = round(median(info["quantities"]), 2)

            if cat_item:
                price = cat_item.price
                unit = cat_item.base_unit
                name = cat_item.name
            else:
                price = float(sample.get("price") or 0.0)
                unit = sample.get("base_unit") or sample.get("unit") or "шт"
                name = sample.get("catalog_name") or sample.get("name") or "Товар"

            total = round(opt_qty * price, 2)
            freq_percent = int((info["count"] / total_orders) * 100)

            result.append(
                MatchedItem(
                    raw_name=name,
                    raw_quantity=opt_qty,
                    raw_unit=unit,
                    sku_id=sku,
                    catalog_name=name,
                    final_quantity=opt_qty,
                    base_unit=unit,
                    price=price,
                    total_sum=total,
                    confidence=1.0,
                    status=MatchStatus.RECOGNIZED,
                    recalculation_hint=f"в {freq_percent}% ваших заказов",
                )
            )

        return result

    async def suggest_missing_items(
        self,
        session_id: str,
        current_items: list[MatchedItem],
        min_frequency: float = 0.40,
        top_k: int = 3,
    ) -> list[MatchedItem]:
        """
        Анализирует регулярные закупки пользователя и находит товары с частотой >= min_frequency,
        которые он обычно заказывает, но забыл указать в текущей корзине.
        """
        ideal = await self.predict_ideal_order(session_id, top_k=10, min_frequency=min_frequency)
        if not ideal:
            return []

        current_skus = {it.sku_id for it in current_items if it.sku_id}
        current_names = {it.raw_name.lower().strip() for it in current_items}

        missing: list[MatchedItem] = []
        for item in ideal:
            if item.sku_id and item.sku_id in current_skus:
                continue
            if item.catalog_name and item.catalog_name.lower().strip() in current_names:
                continue
            missing.append(item)
            if len(missing) >= top_k:
                break

        return missing


order_predictor = OrderPredictor()
