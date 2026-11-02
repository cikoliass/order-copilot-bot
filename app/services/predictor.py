import json
import sqlite3
import logging
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from app.services.matcher import MatchedItem, MatchStatus, catalog_matcher

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).resolve().parent.parent.parent / "horeca_orders.db"


class OrderPredictor:
    """Анализирует прошлые заказы пользователя в SQLite и формирует идеальный заказ."""

    def _fetch_user_orders(self, telegram_user_id: str) -> list[list[dict[str, Any]]]:
        if not DB_PATH.exists():
            return []

        orders_items = []
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in cursor.fetchall()]

            for table in tables:
                try:
                    cursor.execute(f"PRAGMA table_info({table})")
                    columns = [row[1] for row in cursor.fetchall()]
                    # Колонка с товарами может называться items_json или items_data
                    json_col = next((c for c in ("items_json", "items_data") if c in columns), None)
                    if not json_col or "telegram_user_id" not in columns:
                        continue

                    cursor.execute(
                        f"SELECT {json_col} FROM {table} WHERE telegram_user_id = ? ORDER BY id DESC LIMIT 50",
                        (str(telegram_user_id),),
                    )
                    for (raw_json,) in cursor.fetchall():
                        if raw_json:
                            parsed = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
                            if isinstance(parsed, list) and parsed:
                                orders_items.append(parsed)
                except Exception as table_exc:
                    logger.warning("Ошибка чтения таблицы %s при прогнозе: %s", table, table_exc)
            conn.close()
        except Exception as exc:
            logger.error("Ошибка чтения БД для прогноза: %s", exc)

        return orders_items

    def predict_ideal_order(self, telegram_user_id: str, top_k: int = 6) -> list[MatchedItem]:
        history = self._fetch_user_orders(telegram_user_id)
        if not history:
            return []

        total_orders = len(history)
        sku_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "quantities": [], "last_item": None}
        )

        for order in history:
            seen_in_order = set()
            for item in order:
                sku = item.get("sku_id") or item.get("sku")
                if not sku:
                    continue
                qty = float(item.get("final_quantity") or item.get("qty") or 1.0)
                if sku not in seen_in_order:
                    sku_stats[sku]["count"] += 1
                    seen_in_order.add(sku)
                sku_stats[sku]["quantities"].append(qty)
                if sku_stats[sku]["last_item"] is None:
                    sku_stats[sku]["last_item"] = item

        ranked_skus = sorted(
            sku_stats.items(),
            key=lambda kv: (kv[1]["count"] / total_orders, len(kv[1]["quantities"])),
            reverse=True,
        )

        cat_map = {item.sku_id: item for item in catalog_matcher.catalog}

        def _build_matched_item(sku: str, info: dict[str, Any]) -> MatchedItem:
            freq = info["count"] / total_orders
            optimal_qty = round(median(info["quantities"]), 2)
            sample = info["last_item"]

            cat_item = cat_map.get(sku)
            if cat_item:
                price = cat_item.price
                base_unit = cat_item.base_unit
                catalog_name = cat_item.name
            else:
                price = float(sample.get("price") or 100.0)
                base_unit = sample.get("base_unit") or "шт"
                catalog_name = sample.get("catalog_name") or sample.get("raw_name") or "Товар"

            return MatchedItem(
                raw_name=catalog_name,
                raw_quantity=optimal_qty,
                raw_unit=base_unit,
                sku_id=sku,
                catalog_name=catalog_name,
                base_unit=base_unit,
                final_quantity=optimal_qty,
                price=price,
                total_sum=round(optimal_qty * price, 2),
                confidence=round(min(0.99, 0.70 + freq * 0.29), 2),
                status=MatchStatus.RECOGNIZED,
                recalculation_hint=f"В {int(freq * 100)}% ваших заказов",
                note="Подобрано ИИ по истории закупок",
            )

        predicted_items: list[MatchedItem] = []
        for sku, info in ranked_skus[:top_k]:
            freq = info["count"] / total_orders
            # Если заказов много (>=4), отсекаем совсем редкие разовые позиции (< 15%),
            # если только они не были заказаны хотя бы 2 раза
            if total_orders >= 4 and freq < 0.15 and info["count"] < 2:
                continue

            predicted_items.append(_build_matched_item(sku, info))

        # Фолбэк: если строгий фильтр частоты отсек все позиции (например, все заказы были разными),
        # берем самые частые позиции пользователя
        if not predicted_items and ranked_skus:
            for sku, info in ranked_skus[:min(top_k, 3)]:
                predicted_items.append(_build_matched_item(sku, info))

        return predicted_items

    def suggest_missing_items(
        self, telegram_user_id: str, current_items: list[MatchedItem], max_suggestions: int = 3
    ) -> list[MatchedItem]:
        ideal_items = self.predict_ideal_order(telegram_user_id, top_k=10)
        current_skus = {it.sku_id for it in current_items if it.sku_id}
        missing = [it for it in ideal_items if it.sku_id not in current_skus]
        return missing[:max_suggestions]


order_predictor = OrderPredictor()