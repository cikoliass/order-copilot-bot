import asyncio
import hashlib
import hmac
import html
import ipaddress
import json
import logging
import math
import re
import socket
import time
import urllib.parse
import uuid
from collections import defaultdict
from typing import Any, Optional
import httpx
from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from pydantic import BaseModel, Field
from rapidfuzz import fuzz
from app.core.config import settings
from app.core.database import OrderRecord, async_session_factory, save_order
from app.core.redis_client import redis_client
from app.services.asr_yandex import transcribe_voice_yandex
from app.services.bitrix_sync import HORECA_SYNONYMS, sync_catalog_from_bitrix
from app.services.dispatcher import dispatcher
from app.services.integrations import integrations_service
from app.services.matcher import catalog_matcher
from app.services.parser_yandex import analyze_cart_intent, extract_order_from_text
from app.services.predictor import order_predictor
from app.services.schemas import (
    InboundMessage,
    MatchStatus,
    MatchedItem,
    OutboundButton,
    OutboundMessage,
    RawItem,
    extract_chat_id_from_session,
)
from sqlalchemy import select

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhook", tags=["Webhooks"])

from contextlib import asynccontextmanager

# Менеджер распределенных блокировок сессий с защитой от утечек памяти (bounded cache)
class SessionLockManager:
    """Потокобезопасный пул блокировок сессий с подсчетом ссылок и защитой от гонок при вытеснении."""
    def __init__(self, maxsize: int = 5000):
        from collections import OrderedDict
        self._locks: OrderedDict[str, asyncio.Lock] = OrderedDict()
        self._refs: dict[str, int] = defaultdict(int)
        self._maxsize = maxsize

    def get_lock(self, session_id: str) -> asyncio.Lock:
        if session_id in self._locks:
            self._locks.move_to_end(session_id)
            return self._locks[session_id]

        if len(self._locks) >= self._maxsize:
            for k in list(self._locks.keys()):
                if not self._locks[k].locked() and self._refs.get(k, 0) == 0:
                    self._locks.pop(k, None)
                    self._refs.pop(k, None)
                    if len(self._locks) < self._maxsize:
                        break

        lock = asyncio.Lock()
        self._locks[session_id] = lock
        return lock

    @asynccontextmanager
    async def session_context(self, session_id: str):
        lock = self.get_lock(session_id)
        self._refs[session_id] += 1
        try:
            async with lock:
                yield
        finally:
            self._refs[session_id] = max(0, self._refs[session_id] - 1)

    def __getitem__(self, session_id: str) -> asyncio.Lock:
        return self.get_lock(session_id)


class BoundedTTLSet:
    """Кольцевой кэш дедупликации входящих обновлений со временем жизни TTL и фиксированной памятью."""
    def __init__(self, maxsize: int = 15000, ttl_sec: float = 300.0):
        from collections import OrderedDict
        self._maxsize = maxsize
        self._ttl_sec = ttl_sec
        self._items: OrderedDict[str, float] = OrderedDict()

    def is_duplicate_and_add(self, key: str) -> bool:
        now = time.time()
        while self._items and next(iter(self._items.values())) < now:
            self._items.popitem(last=False)

        if key in self._items:
            return True

        if len(self._items) >= self._maxsize:
            self._items.popitem(last=False)

        self._items[key] = now + self._ttl_sec
        return False


_session_locks = SessionLockManager(maxsize=5000)
_update_deduplicator = BoundedTTLSet(maxsize=15000, ttl_sec=300.0)

DIRECT_CONFIRM_PHRASES = {
    "1", "да", "ок", "ok", "yes", "беру", "берем", "подтверждаю", "подтвердить",
    "подтверждаю заказ", "подтвердить заказ", "отправляй", "отправь", "оформить",
    "оформи заказ", "оформи", "заказываю", "все верно", "всё верно", "добро",
    "погнали", "все ништяк везите", "ништяк везите", "везите", "вези",
}

DIRECT_CANCEL_PHRASES = {
    "2", "отмени все", "отмени всё", "отмени заказ", "отмена заказа", "отмени весь заказ",
    "сбрось заказ", "сбросить все", "сбросить всё", "сбрось все", "сбрось всё",
    "очисти корзину", "очистить корзину", "отмена", "отбой", "забей на заказ",
}

DIRECT_PREDICT_PHRASES = {
    "3", "повтор", "повтори", "идеальный", "идеальный заказ", "✨ идеальный заказ",
    "✨ собрать идеальный заказ",
}

NEGATION_PATTERNS = [
    r"\bне\b", r"\bнет\b", r"\bни\b", r"\bне\s+надо\b", r"\bне\s+отправляй\b",
    r"\bне\s+подтверждаю\b", r"\bстоп\b", r"\bstop\b",
]


def _fmt_msg(platform: str, plain: str, html_text: str) -> str:
    return plain if platform in ("vk", "max", "viber") else html_text


def _is_duplicate_update(update_key: str) -> bool:
    """Синхронная дедупликация через локальный кольцевой буфер (для обратной совместимости)."""
    return _update_deduplicator.is_duplicate_and_add(update_key)


async def _is_duplicate_update_async(update_key: str) -> bool:
    """Глобальная дедупликация через Redis + локальный кольцевой буфер."""
    if _update_deduplicator.is_duplicate_and_add(update_key):
        return True
    return await redis_client.check_and_set_dedup(update_key, ttl_sec=300)


FORBIDDEN_DEFAULT_SECRETS = frozenset({
    "",
    "super_secret_webhook_salt_123",
    "your_super_secret_webhook_salt_here",
    "changeme",
    "secret",
})


def verify_webhook_auth(
    request: Request,
    secret_header_name: Optional[str] = None,
    allow_query: bool = False,
) -> None:
    """
    Проверка подлинности вебхука по WEBHOOK_SECRET:
    - Защищает эндпоинты от подделки запросов и несанкционированного доступа.
    - Запрещает передачу секрета через URL-параметры (?secret=...) по умолчанию (защита от утечки в логах).
    - В тестовом окружении без секретов разрешает вызовы.
    """
    if settings.APP_ENV == "test":
        return

    expected = (settings.WEBHOOK_SECRET or "").strip()
    if expected in FORBIDDEN_DEFAULT_SECRETS or (settings.APP_ENV == "production" and len(expected) < 16):
        logger.critical("Запрос отклонен: WEBHOOK_SECRET не настроен или использует небезопасное дефолтное значение!")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server security misconfiguration: insecure or missing WEBHOOK_SECRET",
        )

    # 1. Проверяем специализированный заголовок (например, X-Telegram-Bot-Api-Secret-Token)
    if secret_header_name:
        val = request.headers.get(secret_header_name)
        if val and hmac.compare_digest(val, expected):
            return

    # 2. Проверяем стандартный X-Webhook-Secret или Authorization: Bearer
    val_generic = request.headers.get("X-Webhook-Secret")
    if val_generic and hmac.compare_digest(val_generic, expected):
        return

    auth_hdr = request.headers.get("Authorization", "")
    if auth_hdr.startswith("Bearer "):
        token = auth_hdr.split(" ", 1)[1].strip()
        if hmac.compare_digest(token, expected):
            return
    elif auth_hdr and hmac.compare_digest(auth_hdr, expected):
        return

    # 3. Query param ?secret=... (разрешен ТОЛЬКО если явно запрошен флагом allow_query)
    if allow_query:
        query_secret = request.query_params.get("secret") or request.query_params.get("token")
        if query_secret and hmac.compare_digest(query_secret, expected):
            return

    raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden: Invalid or missing webhook secret")


def classify_action_text(text: str) -> str | None:
    """
    Распознает команды подтверждения/отмены только при явном и однозначном намерении.
    Защищено от ложных срабатываний при отрицании («не отправляй», «не подтверждаю»)
    и при операциях с отдельными товарами («верни лимоны», «отмени лимоны»).
    """
    t = text.strip().lower()
    cleaned = re.sub(r"[^\w\s]", " ", t).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        return None

    # Отрицание: НИКОГДА не подтверждать
    for neg_pat in NEGATION_PATTERNS:
        if re.search(neg_pat, t):
            if any(confirm_w in t for confirm_w in ("отправ", "подтверд", "оформ", "заказ")):
                return None
            if "не надо" in t or "стоп" in t or "нет" in t:
                if len(cleaned.split()) <= 2:
                    return "copilot:cancel"
            return None

    if cleaned in DIRECT_CONFIRM_PHRASES:
        return "copilot:confirm"
    if cleaned in DIRECT_CANCEL_PHRASES:
        return "copilot:cancel"
    if cleaned in DIRECT_PREDICT_PHRASES:
        return "copilot:predict"

    if re.search(r"\d+\s*(?:кг|гр|г|мл|л|литр|шт|упак|пач|кор|бут|ящик|бан|порц)", t):
        return None

    # Если в фразе упоминается операция с товаром («отмени лимоны», «верни лимоны»):
    # НЕ сбрасывать корзину и НЕ подтверждать!
    words = cleaned.split()
    if len(words) > 1 and any(w.startswith(("отмен", "удал", "убер", "вычерк", "верн")) for w in words):
        return None

    return None


def format_copilot_card(
    items: list[MatchedItem],
    delivery_date: str | None = None,
    recognized_speech: str | None = None,
    missing_hints: list[MatchedItem] | None = None,
    platform: str = "telegram",
) -> str:
    """Генерация сводки Co-Pilot с бейджами 🟢 / 🟡 и пересчетом тары."""
    plain = platform in ("vk", "max", "viber")
    esc = (lambda s: s) if plain else html.escape
    arrow = "   ▫ " if plain else "   └ "
    lines: list[str] = []

    if recognized_speech:
        lines.append(f"🎙 Распознано: «{recognized_speech}»\n" if plain else f"🎙 <i>Распознано (SpeechKit): «{esc(recognized_speech)}»</i>\n")

    lines.append("📦 Состав заказа:" if plain else "📦 <b>Сводка заказа (Co-Pilot):</b>")
    if delivery_date:
        lines.append(f"📅 Дата доставки: {delivery_date}" if plain else f"📅 <b>Дата доставки:</b> {esc(delivery_date)}")
    lines.append("")

    total_sum, needs_review = 0.0, 0

    for idx, it in enumerate(items, 1):
        if it.status == MatchStatus.OUT_OF_STOCK or it.is_out_of_stock:
            icon, needs_review = "🔴", needs_review + 1
        elif it.status == MatchStatus.RECOGNIZED and it.sku_id is not None:
            icon = "🟢"
        else:
            icon, needs_review = "🟡", needs_review + 1

        if it.sku_id:
            recalc = f" ({it.recalculation_hint})" if plain else (f" <i>({esc(it.recalculation_hint)})</i>" if it.recalculation_hint else "")
            note = f"\n   💬 {it.note}" if plain else (f"\n   💬 <i>Комментарий: {esc(it.note)}</i>" if it.note else "")
            if plain:
                lines.append(
                    f"{idx}. {icon} {it.catalog_name or it.raw_name} [{it.sku_id}]\n"
                    f"{arrow}Заказ: {it.final_quantity:g} {it.base_unit}{recalc} × {it.price:g} ₽ = {it.total_sum:,.2f} ₽{note}"
                )
                if it.is_out_of_stock and it.suggested_analog_name:
                    lines.append(f"   🔄 Нет на складе! Предложен аналог: {it.suggested_analog_name} [{it.suggested_analog_sku}] ({it.suggested_analog_price:g} ₽)")
                if it.pack_warning:
                    lines.append(f"   📦 {it.pack_warning}")
            else:
                lines.append(
                    f"<b>{idx}.</b> {icon} <code>[{esc(it.sku_id)}]</code> <b>{esc(it.catalog_name or it.raw_name)}</b>\n"
                    f"{arrow}Заказ: <b>{it.final_quantity:g} {esc(it.base_unit)}</b>{recalc} × {it.price:g} ₽ = <b>{it.total_sum:,.2f} ₽</b>{note}"
                )
                if it.is_out_of_stock and it.suggested_analog_name:
                    lines.append(
                        f"   └ 🔄 <b>Нет на складе! Предложен аналог:</b> <code>[{esc(it.suggested_analog_sku or '')}]</code> "
                        f"<b>{esc(it.suggested_analog_name)}</b> — {it.suggested_analog_price:g} ₽/{esc(it.suggested_analog_unit or it.base_unit)}"
                    )
                if it.pack_warning:
                    lines.append(f"   └ 📦 <i>{esc(it.pack_warning)}</i>")
            total_sum += it.total_sum
        else:
            if plain:
                lines.append(f"{idx}. 🟡 {it.raw_name} (нет в каталоге)\n{arrow}Запрос: {it.raw_quantity:g} {it.raw_unit}")
            else:
                lines.append(f"<b>{idx}.</b> 🟡 <b>НЕ НАЙДЕНО В КАТАЛОГЕ</b>\n{arrow}Запрос: «{esc(it.raw_name)}» — {it.raw_quantity:g} {esc(it.raw_unit)}")

    lines.append(f"\n{'─' * (22 if plain else 28)}")
    lines.append(f"💰 Итого к оплате: {total_sum:,.2f} ₽" if plain else f"💰 <b>Итого по распознанным SKU:</b> {total_sum:,.2f} ₽")

    if needs_review > 0:
        lines.append(
            f"\n⚠️ Внимание (🔴/🟡): {needs_review} поз. требуют проверки.\nСверьте позиции или выберите замену перед отправкой."
            if plain else
            f"\n⚠️ <i>Позиций, требующих внимания (🔴/🟡): <b>{needs_review} шт.</b> Сверьте количество или подтвердите замену перед отправкой.</i>"
        )
    else:
        lines.append("✨ Все позиции найдены в каталоге" if plain else "✨ <i>Все позиции уверенно распознаны.</i>")

    if missing_hints:
        hints = ", ".join(
            f"{m.catalog_name or m.raw_name} ({m.final_quantity:g} {m.base_unit})" if plain
            else f"<b>{esc(m.catalog_name or m.raw_name)}</b> ({m.final_quantity:g} {esc(m.base_unit)})"
            for m in missing_hints
        )
        lines.append(f"\n💡 Не забудьте заказать: {hints}" if plain else f"\n💡 <i>Забытые товары (из регулярных закупок): {hints}</i>")

    lines.append("\n💡 Изменить заказ: «удали 2», «измени 1 на 10 кг» или «добавь 5 кг сахара»." if plain else "\n💡 <i>Чтобы изменить заказ, напишите: «удали 2», «измени 1 на 10 кг» или «добавь 5 кг сахара».</i>")
    res_text = "\n".join(lines)
    if len(res_text) > 3900:
        res_text = res_text[:3750]
        if not plain:
            for tag in ("i", "b", "code"):
                if res_text.count(f"<{tag}>") > res_text.count(f"</{tag}>"):
                    res_text += f"</{tag}>"
            res_text += "\n... <i>[Часть позиций скрыта из-за лимита длины]</i>"
        else:
            res_text += "\n... [Часть позиций скрыта из-за лимита длины]"
    return res_text


def get_default_buttons(items_data: Any = 0) -> list[list[OutboundButton]]:
    """Стандартные кнопки Co-Pilot с быстрыми действиями."""
    buttons = [
        [
            OutboundButton(text="✅ Подтвердить", payload="copilot:confirm"),
            OutboundButton(text="❌ Отменить", payload="copilot:cancel"),
        ],
        [
            OutboundButton(text="✨ Идеальный заказ", payload="copilot:predict"),
        ],
    ]

    items_list = items_data if isinstance(items_data, list) else [None] * (items_data if isinstance(items_data, int) else 0)
    action_row: list[OutboundButton] = []

    for i, it in enumerate(items_list, 1):
        if not it:
            continue
        is_oos = getattr(it, "is_out_of_stock", False) or (isinstance(it, dict) and it.get("is_out_of_stock"))
        analog = getattr(it, "suggested_analog_sku", None) or (isinstance(it, dict) and it.get("suggested_analog_sku"))
        pack = getattr(it, "suggested_pack_quantity", None) or (isinstance(it, dict) and it.get("suggested_pack_quantity"))
        if is_oos and analog:
            action_row.append(OutboundButton(text=f"🔄 Аналог #{i}", payload=f"copilot:analog:{i}"))
        elif pack:
            action_row.append(OutboundButton(text=f"📦 #{i} до {pack:g}", payload=f"copilot:round:{i}"))

    for k in range(0, len(action_row), 2):
        buttons.append(action_row[k:k + 2])

    n = len(items_list)
    if 1 <= n <= 4:
        buttons.append([OutboundButton(text=f"❌ Поз. #{i}", payload=f"copilot:del:{i}") for i in range(1, n + 1)])

    return buttons


def _find_cart_item_index(target_name: str, items: list[dict[str, Any]]) -> int:
    """Нечеткий поиск позиции в корзине с учетом кулинарных синонимов и сленга."""
    if not target_name or not items:
        return -1

    raw_clean = re.sub(r"^\d+(?:[.,]\d+)?\s*(?:кг|гр|г|л|литр|шт|упак|пач|кор|бут|ящик|бан|порц)?\s*", "", target_name.strip().lower()).strip()
    search = raw_clean or target_name.strip().lower()

    # Быстрый поиск по номеру позиции («1», «#1», «поз 2»)
    if search.isdigit():
        idx = int(search) - 1
        if 0 <= idx < len(items):
            return idx
    if search.startswith("#") and search[1:].isdigit():
        idx = int(search[1:]) - 1
        if 0 <= idx < len(items):
            return idx

    search_variants = {search}
    for k_syn, syn_list in HORECA_SYNONYMS.items():
        if k_syn in search:
            search_variants.update(syn_list)
        for s in syn_list:
            if s in search:
                search_variants.add(k_syn)
                search_variants.update(syn_list)

    best_idx, best_score = -1, 0.0

    for i, it in enumerate(items):
        candidates = [(it.get("raw_name") or "").lower(), (it.get("catalog_name") or "").lower()]
        for svar in search_variants:
            for cand in candidates:
                if not cand:
                    continue
                cand_lower = cand.strip()
                if svar == cand_lower:
                    score = 100.0
                elif svar in cand_lower or cand_lower in svar:
                    score = 92.0
                else:
                    score = max(
                        fuzz.token_set_ratio(svar, cand_lower),
                        fuzz.token_sort_ratio(svar, cand_lower),
                        fuzz.partial_ratio(svar, cand_lower),
                    )
                    for w in (w for w in svar.split() if len(w) >= 3):
                        if w in cand_lower:
                            score = max(score, 85.0)
                if score > best_score:
                    best_score, best_idx = score, i

    return best_idx if best_score >= 50.0 else -1


MAX_CART_ITEMS = 50


def _merge_items(existing: list[MatchedItem], new_items: list[MatchedItem]) -> list[MatchedItem]:
    for nm in new_items:
        merged = False
        if nm.sku_id:
            for ex in existing:
                if ex.sku_id == nm.sku_id and ex.base_unit == nm.base_unit:
                    ex.final_quantity = min(50000.0, round(ex.final_quantity + nm.final_quantity, 3))
                    ex.total_sum = round(ex.final_quantity * ex.price, 2)
                    ex.raw_quantity = min(50000.0, round(ex.raw_quantity + nm.raw_quantity, 3))
                    # Пересчитываем предупреждение кратности тары для нового объема
                    if ex.pack_multiple and ex.pack_multiple > 0 and ex.pack_multiple != 1.0:
                        ratio = ex.final_quantity / ex.pack_multiple
                        if math.isclose(ratio, round(ratio), rel_tol=1e-5, abs_tol=1e-5):
                            ex.pack_warning = None
                            ex.suggested_pack_quantity = None
                        else:
                            suggested_qty = float(math.ceil(round(ratio, 6)) * ex.pack_multiple)
                            ex.suggested_pack_quantity = suggested_qty
                            ex.pack_warning = (
                                f"Отгрузка кратно {ex.pack_multiple:g} {ex.base_unit} (у вас {ex.final_quantity:g} {ex.base_unit}). "
                                f"Округлить до {suggested_qty:g} {ex.base_unit}?"
                            )
                    merged = True
                    break
        else:
            # Дедупликация нераспознанных товаров по имени и единице измерения
            for ex in existing:
                if not ex.sku_id and ex.raw_name.lower().strip() == nm.raw_name.lower().strip() and ex.raw_unit == nm.raw_unit:
                    ex.raw_quantity = min(50000.0, round(ex.raw_quantity + nm.raw_quantity, 3))
                    ex.final_quantity = ex.raw_quantity
                    merged = True
                    break

        if not merged:
            if len(existing) >= MAX_CART_ITEMS:
                break
            existing.append(nm)
    return existing


async def _save_and_send_card(
    session_id: str,
    platform: str,
    chat_id: str,
    draft: dict[str, Any],
    prefix: str = "",
    missing_hints: list[MatchedItem] | None = None,
) -> None:
    await redis_client.set_order_draft(session_id, draft)
    matched_items = [MatchedItem.model_validate(it) for it in draft.get("matched_items", [])]
    card = prefix + format_copilot_card(
        items=matched_items,
        delivery_date=draft.get("delivery_date"),
        recognized_speech=draft.get("recognized_speech"),
        missing_hints=missing_hints,
        platform=platform,
    )
    await dispatcher.send(
        OutboundMessage(
            platform=platform,
            chat_id=chat_id,
            text=card,
            buttons=get_default_buttons(matched_items),
        )
    )


async def _apply_cart_delete(
    session_id: str, platform: str, chat_id: str, draft: dict[str, Any], idx: int, by_name: bool = False
) -> bool:
    items = draft.get("matched_items", [])
    if not (0 <= idx < len(items)):
        return False
    removed = items.pop(idx)
    name = removed.get("catalog_name") or removed.get("raw_name") or ""
    if not items:
        await redis_client.clear_order_draft(session_id)
        msg = _fmt_msg(platform, "🗑 Все позиции удалены. Корзина пуста. Отправьте новый заказ.", "🗑 <b>Все позиции удалены. Корзина пуста.</b> Отправьте новый заказ.")
        await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg))
        return True

    draft["matched_items"] = items
    plain_p = f"🗑 Позиция «{name}» удалена из заказа.\n\n" if by_name else f"🗑 Позиция #{idx + 1} «{name}» удалена.\n\n"
    html_p = f"🗑 <i>Позиция «{html.escape(name)}» удалена из заказа.</i>\n\n" if by_name else f"🗑 <i>Позиция #{idx + 1} «{html.escape(name)}» удалена.</i>\n\n"
    await _save_and_send_card(session_id, platform, chat_id, draft, prefix=_fmt_msg(platform, plain_p, html_p))
    return True


async def _apply_cart_update(
    session_id: str, platform: str, chat_id: str, draft: dict[str, Any], idx: int, new_qty: float, unit_str: str | None = None
) -> bool:
    items = draft.get("matched_items", [])
    if not (0 <= idx < len(items)):
        return False
    if math.isnan(new_qty) or math.isinf(new_qty):
        return False
    if new_qty <= 0:
        return await _apply_cart_delete(session_id, platform, chat_id, draft, idx)
    if new_qty > 50000:
        await dispatcher.send(OutboundMessage(
            platform=platform,
            chat_id=chat_id,
            text="⚠️ <i>Количество превышает допустимый максимум (50 000).</i>",
        ))
        return True
    matched_it = MatchedItem.model_validate(items[idx])
    target_unit = unit_str or matched_it.raw_unit or matched_it.base_unit
    re_matched = await catalog_matcher.match_order_async([
        RawItem(raw_name=matched_it.catalog_name or matched_it.raw_name, quantity=new_qty, unit=target_unit, note=matched_it.note)
    ])
    if not re_matched:
        return False
    items[idx] = re_matched[0].model_dump()
    draft["matched_items"] = items
    prefix = _fmt_msg(platform, f"✏️ Позиция #{idx + 1} изменена на {new_qty:g} {target_unit}.\n\n", f"✏️ <i>Позиция #{idx + 1} изменена на {new_qty:g} {html.escape(target_unit)}.</i>\n\n")
    await _save_and_send_card(session_id, platform, chat_id, draft, prefix=prefix)
    return True


async def _apply_analog_swap(
    session_id: str, platform: str, chat_id: str, draft: dict[str, Any], target_idx: int = -1
) -> bool:
    items = draft.get("matched_items", [])
    if target_idx < 0:
        for i, it in enumerate(items):
            if it.get("suggested_analog_name") or it.get("is_out_of_stock"):
                target_idx = i
                break
    if not (0 <= target_idx < len(items)) or not items[target_idx].get("suggested_analog_name"):
        return False

    old = items[target_idx]
    analog_name, qty = old["suggested_analog_name"], old["final_quantity"]
    unit = old.get("suggested_analog_unit") or old["base_unit"]
    re_matched = await catalog_matcher.match_order_async([RawItem(raw_name=analog_name, quantity=qty, unit=unit)])
    if not re_matched:
        return False

    new_item = re_matched[0]
    # Если заменяемый аналог уже есть в заказе, объединяем позиции без создания дублей
    merged = False
    for j, it in enumerate(items):
        if j != target_idx and it.get("sku_id") == new_item.sku_id and it.get("base_unit") == new_item.base_unit:
            it["final_quantity"] = min(50000.0, round(it["final_quantity"] + new_item.final_quantity, 3))
            it["raw_quantity"] = min(50000.0, round(it.get("raw_quantity", 0.0) + new_item.raw_quantity, 3))
            it["total_sum"] = round(it["final_quantity"] * it["price"], 2)
            pack_mult = it.get("pack_multiple")
            if pack_mult and pack_mult > 0 and pack_mult != 1.0:
                ratio = it["final_quantity"] / pack_mult
                if math.isclose(ratio, round(ratio), rel_tol=1e-5, abs_tol=1e-5):
                    it["pack_warning"] = None
                    it["suggested_pack_quantity"] = None
                else:
                    sug_q = float(math.ceil(round(ratio, 6)) * pack_mult)
                    it["suggested_pack_quantity"] = sug_q
                    it["pack_warning"] = (
                        f"Отгрузка кратно {pack_mult:g} {it['base_unit']} (у вас {it['final_quantity']:g} {it['base_unit']}). "
                        f"Округлить до {sug_q:g} {it['base_unit']}?"
                    )
            items.pop(target_idx)
            merged = True
            break
    if not merged:
        items[target_idx] = new_item.model_dump()
    draft["matched_items"] = items
    prefix = _fmt_msg(platform, f"🔄 Позиция #{target_idx + 1} заменена на аналог «{analog_name}»!\n\n", f"🔄 <i>Позиция #{target_idx + 1} заменена на аналог «{html.escape(analog_name)}»!</i>\n\n")
    await _save_and_send_card(session_id, platform, chat_id, draft, prefix=prefix)
    return True


async def _apply_round_pack(
    session_id: str, platform: str, chat_id: str, draft: dict[str, Any], target_idx: int = -1
) -> bool:
    items = draft.get("matched_items", [])
    if target_idx < 0:
        for i, it in enumerate(items):
            if it.get("suggested_pack_quantity"):
                target_idx = i
                break
    if not (0 <= target_idx < len(items)) or not items[target_idx].get("suggested_pack_quantity"):
        return False

    old = items[target_idx]
    new_qty, target_name, unit = float(old["suggested_pack_quantity"]), (old.get("catalog_name") or old["raw_name"]), old["base_unit"]
    re_matched = await catalog_matcher.match_order_async([RawItem(raw_name=target_name, quantity=new_qty, unit=unit)])
    if not re_matched:
        return False

    items[target_idx] = re_matched[0].model_dump()
    draft["matched_items"] = items
    prefix = _fmt_msg(platform, f"📦 Позиция #{target_idx + 1} округлена до упаковки ({new_qty:g} {unit})!\n\n", f"📦 <i>Позиция #{target_idx + 1} округлена до упаковки ({new_qty:g} {html.escape(unit)})!</i>\n\n")
    await _save_and_send_card(session_id, platform, chat_id, draft, prefix=prefix)
    return True


async def _apply_cart_add(
    session_id: str, platform: str, chat_id: str, draft: dict[str, Any], items_to_add: list[RawItem], delivery_date: str | None = None
) -> bool:
    if not items_to_add:
        return False
    new_matched = await catalog_matcher.match_order_async(items_to_add)
    existing = [MatchedItem.model_validate(it) for it in draft.get("matched_items", [])]
    merged = _merge_items(existing, new_matched)
    draft["matched_items"] = [it.model_dump() for it in merged]
    if delivery_date:
        draft["delivery_date"] = delivery_date
    prefix = _fmt_msg(platform, "➕ Добавлено в текущий заказ:\n\n", "➕ <i>Добавлено в текущий заказ:</i>\n\n")
    await _save_and_send_card(session_id, platform, chat_id, draft, prefix=prefix)
    return True


async def _apply_new_order(
    session_id: str, platform: str, chat_id: str, order_text: str, items_to_add: list[RawItem], delivery_date: str | None = None, creator_user_id: str | None = None
) -> bool:
    if not items_to_add:
        return False
    new_matched = await catalog_matcher.match_order_async(items_to_add)
    new_draft = {
        "raw_text": order_text,
        "delivery_date": delivery_date or "По регламенту",
        "matched_items": [it.model_dump() for it in new_matched],
        "creator_user_id": creator_user_id,
    }
    prefix = _fmt_msg(platform, "🆕 Предыдущий черновик отменен. Новый заказ:\n\n", "🆕 <i>Предыдущий черновик отменен. Новый заказ:</i>\n\n")
    await _save_and_send_card(session_id, platform, chat_id, new_draft, prefix=prefix)
    return True


async def _handle_cancel_command(session_id: str, platform: str, chat_id: str) -> None:
    await redis_client.clear_order_draft(session_id)
    msg = _fmt_msg(platform, "❌ Заказ отменен. Отправьте новый заказ текстом или голосом.", "❌ <b>Заказ отменен.</b> Отправьте новый заказ текстом или голосом.")
    await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg))


async def _handle_predict_command(session_id: str, platform: str, chat_id: str, platform_user_id: str | None = None) -> None:
    ideal_items = await order_predictor.predict_ideal_order(session_id, top_k=6)
    if not ideal_items:
        msg = _fmt_msg(platform, "✨ История заказов пока пуста. Оформите первый заказ, чтобы обучить систему.", "✨ <b>История заказов пока пуста.</b> Оформите первый заказ, чтобы обучить систему.")
        await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg))
        return

    prefix = _fmt_msg(platform, "✨ «Идеальный заказ» на основе ваших прошлых закупок:\n\n", "✨ <b>«Идеальный заказ» на основе ваших прошлых закупок:</b>\n\n")
    draft = {
        "raw_text": "[ИИ: Идеальный заказ]",
        "delivery_date": "Завтра",
        "matched_items": [it.model_dump() for it in ideal_items],
        "creator_user_id": platform_user_id,
    }
    await _save_and_send_card(session_id, platform, chat_id, draft, prefix=prefix)


async def execute_order_confirmation(
    session_id: str,
    platform: str,
    chat_id: str,
    platform_user_id: str,
) -> None:
    """
    Атомарная отправка заказа в 1С и Битрикс24 с защитой от повторных нажатий (Distributed Lock)
    и блокировкой при наличии нераспознанных позиций / товаров не на складе.
    """
    lock_token = str(uuid.uuid4())
    if not await redis_client.acquire_lock(session_id, timeout=settings.LOCK_TIMEOUT_SEC, lock_token=lock_token):
        await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text="⏳ <b>Заказ уже отправляется в 1С/Битрикс24, подождите...</b>"))
        return

    try:
        draft = await redis_client.get_order_draft(session_id)
        if not draft or not draft.get("matched_items"):
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text="⚠️ <b>Корзина пуста или устарела.</b> Отправьте состав заказа заново."))
            return

        client_1c_id = None
        try:
            from app.core.database import get_client_binding
            binding = await get_client_binding(platform, platform_user_id)
            creator_uid = draft.get("creator_user_id")
            if creator_uid and creator_uid != platform_user_id and settings.REQUIRE_CLIENT_AUTH:
                creator_binding = await get_client_binding(platform, str(creator_uid))
                if creator_binding and binding and creator_binding.counterparty_1c_id != binding.counterparty_1c_id:
                    await dispatcher.send(OutboundMessage(
                        platform=platform,
                        chat_id=chat_id,
                        text="⛔ <b>Ошибка доступа:</b> вы не можете подтвердить заказ, созданный сотрудником другого контрагента.",
                    ))
                    return
            if binding and binding.counterparty_1c_id:
                client_1c_id = binding.counterparty_1c_id
        except Exception as bind_err:
            logger.warning("Не удалось получить привязку клиента для 1С: %s", bind_err)

        all_items = [MatchedItem.model_validate(it) for it in draft["matched_items"]]

        # Актуализация остатков, стоп-листа и цен по живому каталогу перед отправкой в 1С
        cat_map = {sku.sku_id: sku for sku in catalog_matcher.catalog}
        cart_changed_by_sync = False
        insufficient_stock_items: list[tuple[MatchedItem, float]] = []

        for it in all_items:
            if it.sku_id:
                if cat_map and it.sku_id not in cat_map:
                    it.is_out_of_stock = True
                    it.status = MatchStatus.OUT_OF_STOCK
                    cart_changed_by_sync = True
                    continue

                if it.sku_id in cat_map:
                    live_sku = cat_map[it.sku_id]
                    live_stock = catalog_matcher.get_stock(live_sku)
                    if live_stock <= 0 or it.sku_id in catalog_matcher._stop_list:
                        it.is_out_of_stock = True
                        it.status = MatchStatus.OUT_OF_STOCK
                        analog = await catalog_matcher.find_best_analog_async(live_sku, query=it.raw_name)
                        if analog:
                            it.suggested_analog_sku = analog.sku_id
                            it.suggested_analog_name = analog.name
                            it.suggested_analog_price = analog.price
                            it.suggested_analog_unit = analog.base_unit
                        cart_changed_by_sync = True
                    elif it.final_quantity > live_stock:
                        insufficient_stock_items.append((it, live_stock))

                    if not math.isclose(it.price, live_sku.price, rel_tol=1e-4):
                        it.price = max(0.0, live_sku.price)
                        it.total_sum = round(it.final_quantity * it.price, 2)
                        cart_changed_by_sync = True

        if cart_changed_by_sync:
            draft["matched_items"] = [it.model_dump() for it in all_items]
            await redis_client.set_order_draft(session_id, draft)

        if insufficient_stock_items:
            details = ", ".join(
                f"«{it.catalog_name or it.raw_name}» (запрошено {it.final_quantity:g}, доступно {avail:g} {it.base_unit})"
                for it, avail in insufficient_stock_items
            )
            msg = (
                f"🚫 <b>Недостаточно товара на складе!</b>\n\n"
                f"Превышен доступный остаток: <b>{html.escape(details)}</b>.\n\n"
                f"<i>Уменьшите количество позиций перед подтверждением заказа.</i>"
            )
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg, buttons=get_default_buttons(all_items)))
            return

        # 1. Блокировка: товары отсутствуют на складе
        oos_items = [it for it in all_items if it.status == MatchStatus.OUT_OF_STOCK or it.is_out_of_stock]
        if oos_items:
            names = ", ".join(f"«{it.catalog_name or it.raw_name}»" for it in oos_items)
            msg = (
                f"🚫 <b>Невозможно подтвердить заказ!</b>\n\n"
                f"Товары отсутствуют на складе: <b>{html.escape(names)}</b>.\n\n"
                f"<i>Выберите предложенную замену (кнопка 🔄) или удалите позицию («удали #{all_items.index(oos_items[0]) + 1}») перед подтверждением.</i>"
            )
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg, buttons=get_default_buttons(all_items)))
            return

        # 2. Блокировка: ненайденные в каталоге товары (не выбрасывать молча!)
        not_found_items = [it for it in all_items if it.status == MatchStatus.NOT_FOUND or not it.sku_id]
        if not_found_items:
            names = ", ".join(f"«{it.raw_name}»" for it in not_found_items)
            msg = (
                f"🚫 <b>Невозможно подтвердить заказ!</b>\n\n"
                f"Позиции не найдены в каталоге поставщика: <b>{html.escape(names)}</b>.\n\n"
                f"<i>Удалите ненайденные позиции («удали #{all_items.index(not_found_items[0]) + 1}») или уточните их названия.</i>"
            )
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg, buttons=get_default_buttons(all_items)))
            return

        # 3. Блокировка: некорректная тара или неизвестный пересчет (NEEDS_REVIEW)
        review_items = [
            it for it in all_items
            if it.status == MatchStatus.NEEDS_REVIEW or (it.recalculation_hint and "неизвестная тара" in it.recalculation_hint)
        ]
        if review_items:
            names = ", ".join(f"«{it.catalog_name or it.raw_name}» ({it.raw_quantity:g} {it.raw_unit})" for it in review_items)
            msg = (
                f"⚠️ <b>Внимание: тара требует уточнения!</b>\n\n"
                f"Не удалось пересчитать единицы измерения: <b>{html.escape(names)}</b>.\n\n"
                f"<i>Уточните количество в базовых единицах (например: «измени #{all_items.index(review_items[0]) + 1} на 10 кг») перед отправкой.</i>"
            )
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg, buttons=get_default_buttons(all_items)))
            return

        valid_items = [it for it in all_items if it.sku_id]
        if not valid_items:
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text="⚠️ В заказе нет распознанных позиций для создания накладной в 1С."))
            return

        # 4. Блокировка: товары с нулевой или отрицательной ценой (защита от несанкционированной бесплатной отгрузки)
        zero_price_items = [it for it in valid_items if it.price <= 0 or math.isnan(it.price) or math.isinf(it.price)]
        if zero_price_items:
            names = ", ".join(f"«{it.catalog_name or it.raw_name}»" for it in zero_price_items)
            msg = (
                f"🚫 <b>Невозможно подтвердить заказ!</b>\n\n"
                f"Для позиций не установлена цена в 1С: <b>{html.escape(names)}</b>.\n\n"
                f"<i>Удалите позиции без цены («удали #{all_items.index(zero_price_items[0]) + 1}») или обратитесь к менеджеру.</i>"
            )
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg, buttons=get_default_buttons(all_items)))
            return

        # 5. Блокировка: некорректное количество товаров
        invalid_qty_items = [it for it in valid_items if it.final_quantity <= 0 or math.isnan(it.final_quantity) or math.isinf(it.final_quantity)]
        if invalid_qty_items:
            names = ", ".join(f"«{it.catalog_name or it.raw_name}»" for it in invalid_qty_items)
            msg = (
                f"🚫 <b>Невозможно подтвердить заказ!</b>\n\n"
                f"Некорректное количество товаров (должно быть > 0): <b>{html.escape(names)}</b>.\n\n"
                f"<i>Уточните количество позиций перед подтверждением.</i>"
            )
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg, buttons=get_default_buttons(all_items)))
            return

        delivery_date, raw_text = draft.get("delivery_date"), draft.get("raw_text", "")
        total_amount = sum(it.total_sum for it in valid_items)

        # Сохраняем черновик записи в БД перед отправкой
        order_record = await save_order(
            session_id=session_id,
            platform=platform,
            platform_user_id=platform_user_id,
            raw_text=raw_text,
            delivery_date=delivery_date,
            items_data=[it.model_dump() for it in valid_items],
            total_amount=total_amount,
            status="Отправка в учетные системы",
            document_number_1c=None,
        )

        result = await integrations_service.send_order(
            session_id=session_id,
            valid_items=valid_items,
            delivery_date=delivery_date,
            raw_text=raw_text,
            order_id=order_record.id,
            platform=platform,
            chat_id=chat_id,
            counterparty_1c_id=client_1c_id,
        )

        # Обновляем статус заказа в БД с защитой от сбоев
        from app.core.database import update_order_doc_number
        try:
            await update_order_doc_number(order_record.id, result.document_number, result.state)
        except Exception as upd_err:
            logger.warning("Не удалось обновить статус OrderRecord #%s: %s", order_record.id, upd_err)

        await redis_client.clear_order_draft(session_id)

        status_icon = "📦" if result.is_outbox else "✅"
        plain_state = "Заказ отправлен в 1С" if not result.is_outbox else "Заказ сохранен (буфер 1С)"
        plain_reply = (
            f"{status_icon} {plain_state}\n\n"
            f"📄 Номер документа: {result.document_number}\n"
            f"💰 Сумма: {total_amount:,.2f} ₽\n"
            f"📅 Дата доставки: {delivery_date or 'По регламенту'}\n\n"
            "Для нового заказа пришлите текст или голосовое."
        )
        html_reply = (
            f"{status_icon} <b>{result.state}</b>\n\n"
            f"📄 <b>Номер документа:</b> <code>{html.escape(result.document_number)}</code>\n"
            f"💰 <b>Сумма:</b> {total_amount:,.2f} ₽\n"
            f"📅 <b>Дата доставки:</b> {html.escape(delivery_date or 'По регламенту')}\n\n"
            "<i>Чтобы сделать новый заказ, просто пришлите текст или голосовое сообщение.</i>"
        )
        await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=_fmt_msg(platform, plain_reply, html_reply)))
    finally:
        await redis_client.release_lock(session_id, lock_token=lock_token)


async def try_handle_cart_edit(
    order_text: str,
    session_id: str,
    platform: str,
    chat_id: str,
    draft: dict[str, Any],
    platform_user_id: str = "",
) -> bool:
    """Интеллектуальная обработка управления корзиной (команды, сленг, YandexGPT)."""
    raw_lower = order_text.strip().lower()
    items = draft.get("matched_items", [])
    if not items:
        return False

    if m := re.match(r"^(?:удали|убери|минус|вычеркни|сними)\s+(?:поз(?:ицию)?\s*)?#?(\d+)$", raw_lower):
        try:
            del_idx = int(m.group(1)) - 1
            if await _apply_cart_delete(session_id, platform, chat_id, draft, del_idx):
                return True
        except (ValueError, OverflowError):
            pass

    if m := re.match(r"^(?:поменяй|измени|сделай)\s+(?:поз(?:ицию)?\s*)?#?(\d+)\s+(?:на\s+)?(\d+(?:[.,]\d+)?)\s*([a-zA-Zа-яА-ЯёЁ]+)?$", raw_lower):
        idx_str, qty_str, unit_str = m.groups()
        try:
            target_idx = int(idx_str) - 1
            qty_val = float(qty_str.replace(",", "."))
            if await _apply_cart_update(session_id, platform, chat_id, draft, target_idx, qty_val, unit_str):
                return True
        except (ValueError, OverflowError):
            pass

    has_negation = any(re.search(pat, raw_lower) for pat in NEGATION_PATTERNS)

    if not has_negation and (
        (m := re.match(r"^(?:copilot:analog:(\d+)|(?:замени|давай|положи|возьми)\s+(?:поз(?:ицию)?\s*)?#?(\d+)?\s*(?:на\s+)?аналог.*)$", raw_lower))
        or re.search(r"\b(?:давай|положи|возьми|замени\s+на)\s+аналог\b", raw_lower)
    ):
        try:
            idx = int(m.group(1) or m.group(2)) - 1 if m and (m.group(1) or m.group(2)) else -1
            if await _apply_analog_swap(session_id, platform, chat_id, draft, idx):
                return True
        except (ValueError, OverflowError):
            pass

    if not has_negation and (
        (m := re.match(r"^(?:copilot:round:(\d+)|(?:округли|кратно|добавь\s+до\s+пачки)\s*(?:поз(?:ицию)?\s*)?#?(\d+)?.*)$", raw_lower))
        or re.search(r"^(?:округли|давай\s+кратно|округляй)\b", raw_lower)
    ):
        try:
            idx = int(m.group(1) or m.group(2)) - 1 if m and (m.group(1) or m.group(2)) else -1
            if await _apply_round_pack(session_id, platform, chat_id, draft, idx):
                return True
        except (ValueError, OverflowError):
            pass

    cart_item_names = [f"{it.get('catalog_name') or it.get('raw_name')} ({it.get('final_quantity', 1):g} {it.get('base_unit', 'шт')})" for it in items]
    cart_intent = await analyze_cart_intent(order_text, cart_item_names)

    action = cart_intent.action
    if action == "remove":
        target = cart_intent.target_item or order_text
        match_idx = _find_cart_item_index(target, items)
        if match_idx >= 0:
            return await _apply_cart_delete(session_id, platform, chat_id, draft, match_idx, by_name=True)
        prefix = _fmt_msg(platform, f"⚠️ Позиция «{target}» не найдена в текущем заказе.\n\n", f"⚠️ <i>Позиция «{html.escape(target)}» не найдена в текущем заказе.</i>\n\n")
        await _save_and_send_card(session_id, platform, chat_id, draft, prefix=prefix)
        return True

    if action == "update":
        match_idx = _find_cart_item_index(cart_intent.target_item or "", items)
        if match_idx >= 0:
            matched_it = MatchedItem.model_validate(items[match_idx])
            new_qty = cart_intent.new_quantity if cart_intent.new_quantity is not None else matched_it.final_quantity
            if new_qty <= 0:
                return await _apply_cart_delete(session_id, platform, chat_id, draft, match_idx, by_name=True)
            unit = cart_intent.new_unit or matched_it.raw_unit or matched_it.base_unit
            return await _apply_cart_update(session_id, platform, chat_id, draft, match_idx, new_qty, unit)

    if action == "add":
        items_to_add = cart_intent.items_to_add or (await extract_order_from_text(order_text)).items
        return await _apply_cart_add(session_id, platform, chat_id, draft, items_to_add, cart_intent.delivery_date)

    if action == "confirm":
        # Если подтверждение пришло от LLM (а не от явной кнопки / точной фразы),
        # перепроверяем фразу на наличие отрицания и необратимость
        if classify_action_text(order_text) == "copilot:confirm":
            await execute_order_confirmation(session_id, platform, chat_id, platform_user_id)
            return True
        else:
            # Спрашиваем подтверждение с кнопкой, а не отправляем вслепую
            confirm_msg = "Подтверждаете отправку заказа в 1С?"
            await dispatcher.send(OutboundMessage(
                platform=platform,
                chat_id=chat_id,
                text=confirm_msg,
                buttons=[[OutboundButton(text="✅ Да, отправить", payload="copilot:confirm"), OutboundButton(text="❌ Нет", payload="copilot:cancel")]],
            ))
            return True

    if action == "cancel":
        if classify_action_text(order_text) == "copilot:cancel":
            await _handle_cancel_command(session_id, platform, chat_id)
            return True

    if action == "new_order":
        items_to_add = cart_intent.items_to_add or (await extract_order_from_text(order_text)).items
        return await _apply_new_order(session_id, platform, chat_id, order_text, items_to_add, cart_intent.delivery_date, creator_user_id=platform_user_id)

    if action == "swap_analog":
        return await _apply_analog_swap(session_id, platform, chat_id, draft)

    if action == "round_pack":
        return await _apply_round_pack(session_id, platform, chat_id, draft)

    return False


def _is_safe_ip(ip_obj: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """Проверка IP на отсутствие в приватных, петлевых, сервисных и зарезервированных диапазонах."""
    if isinstance(ip_obj, ipaddress.IPv6Address):
        if ip_obj.ipv4_mapped:
            ip_obj = ip_obj.ipv4_mapped
        elif ip_obj in ipaddress.IPv6Network("64:ff9b::/96"):
            # NAT64 well-known prefix: extract embedded IPv4
            embedded_ipv4 = ipaddress.IPv4Address(ip_obj.packed[-4:])
            return _is_safe_ip(embedded_ipv4)
        elif ip_obj in ipaddress.IPv6Network("2002::/16"):
            # 6to4 prefix: extract embedded IPv4 from bytes 2:6
            embedded_ipv4 = ipaddress.IPv4Address(ip_obj.packed[2:6])
            return _is_safe_ip(embedded_ipv4)
        elif ip_obj in ipaddress.IPv6Network("2001::/32"):
            # Teredo prefix: extract client IPv4 from last 4 bytes (inverted)
            embedded_ipv4 = ipaddress.IPv4Address(bytes(b ^ 0xFF for b in ip_obj.packed[-4:]))
            return _is_safe_ip(embedded_ipv4)

    if (
        ip_obj.is_private
        or ip_obj.is_loopback
        or ip_obj.is_link_local
        or ip_obj.is_reserved
        or ip_obj.is_multicast
        or ip_obj.is_unspecified
    ):
        return False

    if isinstance(ip_obj, ipaddress.IPv4Address):
        # RFC 1122: 0.0.0.0/8 ("This network")
        if ip_obj in ipaddress.IPv4Network("0.0.0.0/8"):
            return False
        # Carrier-Grade NAT (RFC 6598: 100.64.0.0/10)
        if ip_obj in ipaddress.IPv4Network("100.64.0.0/10"):
            return False
        # IETF Protocol Assignments (RFC 6890: 192.0.0.0/24)
        if ip_obj in ipaddress.IPv4Network("192.0.0.0/24"):
            return False
        # Network Benchmark tests (RFC 2544: 198.18.0.0/15)
        if ip_obj in ipaddress.IPv4Network("198.18.0.0/15"):
            return False
        # Class E Reserved (RFC 1112: 240.0.0.0/4)
        if ip_obj in ipaddress.IPv4Network("240.0.0.0/4"):
            return False

    return True


ALLOWED_MEDIA_HOSTS = frozenset({
    "api.telegram.org",
    "psv4.userapi.com",
    "platform-api2.max.ru",
    "dl-media.viber.com",
    "media.green-api.com",
    "storage.yandexcloud.net",
})

ALLOWED_MEDIA_SUFFIXES = (
    ".userapi.com",
    ".vkuseraudio.net",
    ".greenapi.com",
    ".green-api.com",
    ".viber.com",
)


def _is_safe_url_for_download(url: str) -> bool:
    """Строгая проверка URL: только HTTPS, только доверенные CDN мессенджеров и безопасные IP."""
    try:
        if len(url) > 2048:
            return False
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme != "https":
            return False
        if parsed.username or parsed.password:
            return False
        if parsed.fragment:
            return False

        hostname = (parsed.hostname or "").strip().lower()
        if not hostname:
            return False

        if parsed.port and parsed.port != 443:
            return False

        is_allowed_host = (
            hostname in ALLOWED_MEDIA_HOSTS
            or any(hostname.endswith(sfx) for sfx in ALLOWED_MEDIA_SUFFIXES)
        )
        if not is_allowed_host:
            return False

        addr_info = socket.getaddrinfo(hostname, 443)
        if not addr_info:
            return False
        for item in addr_info:
            ip_cand = ipaddress.ip_address(item[4][0])
            if not _is_safe_ip(ip_cand):
                return False
        return True
    except Exception:
        return False


async def _download_audio(url: str) -> bytes | None:
    is_safe = await asyncio.to_thread(_is_safe_url_for_download, url)
    if not is_safe:
        logger.error("Заблокирована попытка скачивания с недоверенного URL: %s", url)
        return None

    try:
        parsed = urllib.parse.urlparse(url)
        hostname = (parsed.hostname or "").lower()
        safe_log_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        expected_max_host = urllib.parse.urlparse(settings.MAX_API_BASE_URL).hostname or "platform-api2.max.ru"
        headers = {}
        if settings.MAX_BOT_TOKEN and hostname == expected_max_host.lower():
            headers["Authorization"] = settings.MAX_BOT_TOKEN

        max_bytes = 20 * 1024 * 1024
        async with asyncio.timeout(25.0):
            async with httpx.AsyncClient(timeout=10.0, follow_redirects=False) as client:
                async with client.stream("GET", url, headers=headers) as resp:
                    # Защита от DNS Rebinding (TOCTOU): проверяем реальный IP подключенного сокета
                    network_stream = resp.extensions.get("network_stream")
                    if network_stream is not None:
                        peername = network_stream.get_extra_info("peername") or network_stream.get_extra_info("server_addr")
                        if peername and isinstance(peername, tuple) and len(peername) >= 1:
                            actual_ip = ipaddress.ip_address(peername[0])
                            if not _is_safe_ip(actual_ip):
                                logger.critical("Заблокирована атака DNS Rebinding на %s (IP сокета: %s)", safe_log_url, actual_ip)
                                return None

                    if resp.status_code != 200:
                        logger.error("Ошибка скачивания аудио по URL %s: HTTP %d", safe_log_url, resp.status_code)
                        return None
                    chunks = []
                    total_bytes = 0
                    async for chunk in resp.aiter_bytes(chunk_size=65536):
                        total_bytes += len(chunk)
                        if total_bytes > max_bytes:
                            logger.error("Скачиваемый аудиофайл превышает лимит 20 МБ: %s", safe_log_url)
                            return None
                        chunks.append(chunk)
                    return b"".join(chunks)
    except Exception as exc:
        logger.error("Не удалось скачать аудио: %s", exc)
    return None


async def _download_tg_file(file_id: str) -> bytes | None:
    if not file_id or len(file_id) > 256 or not re.match(r"^[A-Za-z0-9_\-]+$", str(file_id)):
        logger.warning("Отклонен некорректный file_id Telegram: %s", file_id)
        return None
    token = settings.TELEGRAM_BOT_TOKEN
    if not token:
        return None
    try:
        async with asyncio.timeout(25.0):
            async with httpx.AsyncClient(proxy=settings.TELEGRAM_PROXY, timeout=10.0) as client:
                res = await client.get(
                    f"https://api.telegram.org/bot{token}/getFile",
                    params={"file_id": file_id},
                )
                if res.status_code == 200 and (fp := res.json().get("result", {}).get("file_path")):
                    fp_clean = str(fp).strip().replace("\\", "/")
                    if (
                        ".." in fp_clean
                        or fp_clean.startswith(("/", "./"))
                        or "//" in fp_clean
                        or not re.match(r"^[a-zA-Z0-9_\-\./]+$", fp_clean)
                    ):
                        logger.warning("Отклонен небезопасный file_path Telegram: %s", fp_clean)
                        return None

                    max_bytes = 20 * 1024 * 1024
                    async with client.stream("GET", f"https://api.telegram.org/file/bot{token}/{fp_clean}") as dl_resp:
                        if dl_resp.status_code != 200:
                            return None
                        chunks = []
                        total_bytes = 0
                        async for chunk in dl_resp.aiter_bytes(chunk_size=65536):
                            total_bytes += len(chunk)
                            if total_bytes > max_bytes:
                                logger.error("Аудиофайл Telegram превышает лимит 20 МБ")
                                return None
                            chunks.append(chunk)
                        return b"".join(chunks)
    except Exception as exc:
        safe_err = str(exc).replace(token, "***")
        safe_err = re.sub(r"bot[0-9]+:[A-Za-z0-9_\-]+", "bot***:***", safe_err)
        logger.warning("Не удалось скачать аудиофайл Telegram: %s", safe_err)
    return None


async def _process_inbound_core_impl(inbound: InboundMessage) -> None:
    """Внутренняя логика обработки входящих сообщений."""
    session_id, platform, chat_id = inbound.session_id, inbound.platform, inbound.chat_id

    # 1. Валидация B2B-контрагента (при включенном флаге REQUIRE_CLIENT_AUTH)
    if settings.REQUIRE_CLIENT_AUTH and settings.APP_ENV != "test":
        from app.core.database import get_client_binding
        client = await get_client_binding(platform, inbound.platform_user_id)
        if not client:
            if (inbound.text or "").strip().lower() in ("/start", "start"):
                msg = (
                    "👋 <b>Добро пожаловать в B2B HoReCa Order Co-Pilot!</b>\n\n"
                    "Ваш аккаунт еще не привязан к договору поставки.\n"
                    f"Ваш личный ID: <code>{html.escape(inbound.platform_user_id)}</code> ({platform}).\n\n"
                    "Передайте этот ID вашему персональному менеджеру для привязки карточки ресторана."
                )
            else:
                msg = (
                    "🔒 <b>Доступ заблокирован</b>\n\n"
                    "Вы не авторизованы как представитель контрагента.\n"
                    f"Передайте менеджеру ваш личный ID: <code>{html.escape(inbound.platform_user_id)}</code> ({platform}) для привязки к ИНН."
                )
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg))
            return

    # 2. Двойная защита от DoS и спама: лимит и на сессию/группу, и на конкретного пользователя
    user_rl_key = f"{platform}:user:{inbound.platform_user_id}"
    session_allowed = await redis_client.check_rate_limit(
        session_id, max_requests=settings.RATE_LIMIT_PER_MINUTE
    )
    user_allowed = await redis_client.check_rate_limit(
        user_rl_key, max_requests=settings.RATE_LIMIT_PER_MINUTE
    )
    if not session_allowed or not user_allowed:
        await dispatcher.send(OutboundMessage(
            platform=platform,
            chat_id=chat_id,
            text="⏳ <b>Слишком много запросов.</b> Пожалуйста, подождите минуту перед следующим сообщением.",
        ))
        return

    # Блокировка на уровне сессии предотвращает состояние гонки по черновику
    async with _session_locks.session_context(session_id):
        raw_callback = (inbound.callback_data or "").strip()
        # Ограничиваем длину callback_data для защиты от overflow и злоупотреблений
        if raw_callback and len(raw_callback) > 256:
            logger.warning("Отклонена аномально длинная callback_data (%d символов) от %s:%s", len(raw_callback), platform, inbound.platform_user_id)
            raw_callback = ""
        action = raw_callback or (classify_action_text(inbound.text) if inbound.text else None)
        if action:
            if action == "copilot:confirm":
                await execute_order_confirmation(session_id, platform, chat_id, inbound.platform_user_id)
                return
            if action == "copilot:cancel":
                await _handle_cancel_command(session_id, platform, chat_id)
                return
            if action == "copilot:predict":
                await _handle_predict_command(session_id, platform, chat_id, inbound.platform_user_id)
                return
            if action.startswith("copilot:del:"):
                try:
                    raw_idx_str = action.split(":")[-1]
                    if not raw_idx_str.isdigit() or len(raw_idx_str) > 4:
                        return
                    del_idx = int(raw_idx_str) - 1
                    draft = await redis_client.get_order_draft(session_id)
                    if draft and draft.get("matched_items"):
                        if not await _apply_cart_delete(session_id, platform, chat_id, draft, del_idx):
                            await dispatcher.send(OutboundMessage(
                                platform=platform,
                                chat_id=chat_id,
                                text="ℹ️ <i>Эта позиция уже была удалена ранее.</i>",
                            ))
                        return
                    else:
                        await dispatcher.send(OutboundMessage(
                            platform=platform,
                            chat_id=chat_id,
                            text="⚠️ <i>Корзина пуста или заказ уже оформлен.</i>",
                        ))
                        return
                except Exception as exc:
                    logger.warning("Ошибка обработки copilot:del: %s", exc)
                    return
            elif action.startswith("copilot:analog:"):
                try:
                    raw_idx_str = action.split(":")[-1]
                    if not raw_idx_str.isdigit() or len(raw_idx_str) > 4:
                        return
                    idx = int(raw_idx_str) - 1
                    if (draft := await redis_client.get_order_draft(session_id)) and draft.get("matched_items"):
                        if await _apply_analog_swap(session_id, platform, chat_id, draft, idx):
                            return
                except Exception as exc:
                    logger.warning("Ошибка обработки copilot:analog: %s", exc)
            elif action.startswith("copilot:round:"):
                try:
                    raw_idx_str = action.split(":")[-1]
                    if not raw_idx_str.isdigit() or len(raw_idx_str) > 4:
                        return
                    idx = int(raw_idx_str) - 1
                    if (draft := await redis_client.get_order_draft(session_id)) and draft.get("matched_items"):
                        if await _apply_round_pack(session_id, platform, chat_id, draft, idx):
                            return
                except Exception as exc:
                    logger.warning("Ошибка обработки copilot:round: %s", exc)

        voice_bytes = (
            inbound.voice_bytes
            or (await _download_tg_file(inbound.voice_file_id) if inbound.voice_file_id else None)
            or (await _download_audio(inbound.voice_url) if inbound.voice_url else None)
        )
        recognized_text: str | None = None
        if voice_bytes:
            try:
                recognized_text = await transcribe_voice_yandex(voice_bytes)
                if not recognized_text:
                    msg = _fmt_msg(
                        platform,
                        "🎙 Не удалось разобрать речь в сообщении. Пожалуйста, проговорите заказ четче или напишите текстом.",
                        "🎙 <i>Не удалось разобрать речь в сообщении. Пожалуйста, проговорите заказ четче или напишите текстом.</i>",
                    )
                    await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg))
                    return
            except Exception as exc:
                logger.error("Сбой распознавания аудио: %s", exc)
                await dispatcher.send(OutboundMessage(
                    platform=platform,
                    chat_id=chat_id,
                    text="❌ Не удалось распознать речь. Попробуйте написать заказ текстом.",
                ))
                return

        order_text = (recognized_text or inbound.text or "").strip().replace("\x00", "")
        # Удаляем Unicode control characters (кроме \n и \t) для защиты от Bidi-атак и скрытых инъекций
        order_text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f\u200b-\u200f\u2028-\u202f\u2060-\u206f\ufeff\ufff9-\ufffb]", "", order_text).strip()
        if not order_text:
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text="⚠️ Пожалуйста, отправьте заказ текстом или голосовым сообщением."))
            return

        # 3. Защита от DoS и слива LLM-токенов при аномально длинных входящих сообщениях
        if len(order_text) > settings.MAX_TEXT_LENGTH:
            await dispatcher.send(OutboundMessage(
                platform=platform,
                chat_id=chat_id,
                text=f"⚠️ Текст заказа слишком длинный (максимум {settings.MAX_TEXT_LENGTH} символов). Пожалуйста, сократите заказ.",
            ))
            return

        if order_text in ("/start", "start"):
            plain_welcome = (
                "👨‍🍳 B2B HoReCa Order Co-Pilot\n\n"
                "Принимаю заказы ресторанов в свободной форме текстом или голосовыми.\n\n"
                "Примеры:\n"
                "• «На завтра: 2 коробки черри, 5 кг огурцов и ящик сливок 33%»\n"
                "• Нажмите «✨ Идеальный заказ» для быстрой закупки по статистике.\n\n"
                "Команды:\n"
                "• /sync — Синхронизация каталога\n"
                "• /predict — Собрать идеальный заказ"
            )
            html_welcome = (
                "👨‍🍳 <b>B2B HoReCa Order Co-Pilot</b>\n\n"
                "Я принимаю заказы ресторанов в свободной форме <b>текстом</b> или <b>голосовыми</b> сообщениями.\n\n"
                "<b>Примеры:</b>\n"
                "• <i>«На завтра: 2 коробки черри, 5 кг огурцов бакинских и ящик сливок 33%»</i>\n"
                "• Нажмите <b>«✨ Идеальный заказ»</b> для быстрой закупки по вашей статистике.\n\n"
                "<b>Команды:</b>\n"
                "• <code>/sync</code> — Принудительная синхронизация с Битрикс24\n"
                "• <code>/predict</code> — Сформировать идеальный заказ"
            )
            await dispatcher.send(OutboundMessage(
                platform=platform,
                chat_id=chat_id,
                text=_fmt_msg(platform, plain_welcome, html_welcome),
                buttons=[[OutboundButton(text="✨ Идеальный заказ", payload="copilot:predict")]],
            ))
            return

        if order_text in ("/sync", "sync"):
            # Проверка прав администратора с жесткой изоляцией по платформе мессенджера
            is_admin = (
                settings.APP_ENV == "test"
                or f"{platform}:{inbound.platform_user_id}" in settings.ADMIN_USER_IDS
                or (inbound.platform_user_id in settings.ADMIN_USER_IDS and platform == "telegram")
            )
            if not is_admin:
                logger.warning("Попытка несанкционированного вызова /sync пользователем %s:%s", platform, inbound.platform_user_id)
                await dispatcher.send(OutboundMessage(
                    platform=platform,
                    chat_id=chat_id,
                    text="⛔ <i>Команда доступна только администраторам системы.</i>",
                ))
                return

            # Защита от частого дергания синхронизации (cooldown 30 сек через lock)
            sync_lock_key = "lock:catalog_sync_cooldown"
            acquired = await redis_client.acquire_lock(sync_lock_key, timeout=30)
            if not acquired:
                await dispatcher.send(OutboundMessage(
                    platform=platform,
                    chat_id=chat_id,
                    text="⏳ <i>Синхронизация уже запущена или выполнялась менее 30 сек. назад. Подождите...</i>",
                ))
                return

            try:
                synced = await sync_catalog_from_bitrix()
                if synced:
                    catalog_matcher.reload(synced)
                    msg = f"✅ <b>Каталог синхронизирован с Битрикс24!</b>\nЗагружено: <b>{len(synced)} позиций</b>."
                else:
                    msg = "⚠️ Каталог пуст или Битрикс24 недоступен. Используется локальный кэш."
            except Exception as exc:
                logger.error("Ошибка при выполнении /sync: %s", exc)
                msg = "❌ Ошибка синхронизации каталога. Пожалуйста, обратитесь к администратору."
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg))
            return

        if order_text in ("/predict", "predict", "✨ Собрать идеальный заказ"):
            await _handle_predict_command(session_id, platform, chat_id)
            return

        draft = await redis_client.get_order_draft(session_id)
        if draft and draft.get("matched_items"):
            if await try_handle_cart_edit(order_text, session_id, platform, chat_id, draft, inbound.platform_user_id):
                return

        extracted = await extract_order_from_text(order_text)
        if not extracted.items:
            await dispatcher.send(OutboundMessage(
                platform=platform,
                chat_id=chat_id,
                text="⚠️ Не удалось распознать товарные позиции в сообщении.\nПожалуйста, укажите названия и количества (например: <i>«2 коробки сливок и 5 кг томатов»</i>).",
            ))
            return

        matched_items = await catalog_matcher.match_order_async(extracted.items)

        is_appended = False
        if draft and draft.get("matched_items"):
            existing_items = [MatchedItem.model_validate(it) for it in draft["matched_items"]]
            matched_items = _merge_items(existing_items, matched_items)
            is_appended = True

        missing_items = await order_predictor.suggest_missing_items(session_id, matched_items, top_k=3)
        eff_delivery_date = extracted.delivery_date or (draft.get("delivery_date") if draft else None)

        new_draft = {
            "raw_text": order_text,
            "delivery_date": eff_delivery_date,
            "matched_items": [it.model_dump() for it in matched_items],
            "recognized_speech": recognized_text,
            "creator_user_id": (draft.get("creator_user_id") if draft else None) or inbound.platform_user_id,
        }
        prefix = _fmt_msg(platform, "➕ Добавлено к текущему заказу:\n\n", "➕ <i>Добавлено к текущему заказу:</i>\n\n") if is_appended else ""
        await _save_and_send_card(session_id, platform, chat_id, new_draft, prefix=prefix, missing_hints=missing_items)


async def process_inbound_core(inbound: InboundMessage) -> None:
    """Единое ядро обработки входящих сообщений изо всех каналов с глобальным перехватом ошибок."""
    try:
        await _process_inbound_core_impl(inbound)
    except Exception as exc:
        logger.exception("Критический сбой обработки сообщения (%s:%s): %s", inbound.platform, inbound.platform_user_id, exc)
        try:
            await dispatcher.send(OutboundMessage(
                platform=inbound.platform,
                chat_id=inbound.chat_id,
                text="⚠️ <b>Произошла временная ошибка при обработке заказа.</b> Пожалуйста, повторите попытку.",
            ))
        except Exception:
            pass


# =========================================================================
# ВЕБХУКИ МЕССЕНДЖЕРОВ С АВТОРИЗАЦИЕЙ И БЫСТРЫМ ОТВЕТОМ
# =========================================================================
@router.post("/telegram")
async def telegram_webhook(request: Request) -> dict[str, Any]:
    """Вебхук Telegram Bot API с авторизацией secret_token и защитой от дублей."""
    verify_webhook_auth(request, secret_header_name="X-Telegram-Bot-Api-Secret-Token")
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    if update_id := data.get("update_id"):
        if await _is_duplicate_update_async(f"tg:{update_id}"):
            return {"ok": True}

    inbound = None
    if cb := data.get("callback_query"):
        if isinstance(cb, dict):
            from_obj = cb.get("from")
            u_id = str(from_obj.get("id", "")) if isinstance(from_obj, dict) else ""
            msg_obj = cb.get("message")
            chat_obj = msg_obj.get("chat") if isinstance(msg_obj, dict) else None
            chat_id = str(chat_obj.get("id", u_id)) if isinstance(chat_obj, dict) else u_id
            cb_id = cb.get("id")
            if cb_id and settings.TELEGRAM_BOT_TOKEN:
                async def _ack_tg_cb(cid=cb_id):
                    try:
                        async with httpx.AsyncClient(proxy=settings.TELEGRAM_PROXY, timeout=5.0) as cl:
                            await cl.post(f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/answerCallbackQuery", json={"callback_query_id": cid})
                    except Exception:
                        pass
                asyncio.create_task(_ack_tg_cb())
            if u_id:
                inbound = InboundMessage(
                    platform="telegram", platform_user_id=u_id, chat_id=chat_id, callback_data=cb.get("data")
                )

    elif msg := data.get("message"):
        if isinstance(msg, dict):
            from_obj = msg.get("from")
            u_id = str(from_obj.get("id", "")) if isinstance(from_obj, dict) else ""
            chat_obj = msg.get("chat")
            chat_id = str(chat_obj.get("id", u_id)) if isinstance(chat_obj, dict) else u_id
            voice = msg.get("voice") or msg.get("audio") or msg.get("video_note")
            file_id = str(voice.get("file_id"))[:256] if isinstance(voice, dict) and voice.get("file_id") else None
            text = msg.get("text") or msg.get("caption")
            if u_id:
                inbound = InboundMessage(
                    platform="telegram",
                    platform_user_id=u_id,
                    chat_id=chat_id,
                    text=text,
                    voice_file_id=file_id,
                )

    if inbound:
        # Быстрый ответ с фоновой обработкой, либо синхронно в тестовом режиме
        if settings.APP_ENV == "test":
            await process_inbound_core(inbound)
        else:
            asyncio.create_task(process_inbound_core(inbound))

    return {"ok": True}


@router.post("/vk")
async def vk_webhook(request: Request) -> Response:
    """Вебхук ВКонтакте Callback API с обязательной валидацией secret / WEBHOOK_SECRET и confirmation."""
    try:
        data = await request.json()
    except Exception:
        return Response(content="Invalid JSON", status_code=400, media_type="text/plain")

    if not isinstance(data, dict):
        return Response(content="Expected JSON object", status_code=400, media_type="text/plain")

    event_type = data.get("type")
    if event_type == "confirmation":
        if settings.APP_ENV != "test" and settings.VK_SECRET_KEY:
            provided_secret = str(data.get("secret") or "")
            if not hmac.compare_digest(provided_secret, settings.VK_SECRET_KEY):
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid VK secret")
        elif settings.APP_ENV != "test":
            verify_webhook_auth(request)
        if not settings.VK_CONFIRMATION_CODE:
            logger.warning("VK_CONFIRMATION_CODE не настроен в .env")
        return Response(content=settings.VK_CONFIRMATION_CODE or "", media_type="text/plain")

    if settings.VK_SECRET_KEY:
        provided_secret = str(data.get("secret") or "")
        if not hmac.compare_digest(provided_secret, settings.VK_SECRET_KEY):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid VK secret")
    else:
        verify_webhook_auth(request)

    obj = data.get("object")
    if not isinstance(obj, dict):
        obj = {}
    inbound = None

    if event_type == "message_new":
        msg = obj.get("message")
        if not isinstance(msg, dict):
            msg = {}
        msg_id = msg.get("id") or msg.get("conversation_message_id")
        if msg_id and await _is_duplicate_update_async(f"vk:{msg_id}"):
            return Response(content="ok", media_type="text/plain")

        uid = str(msg.get("from_id") or "").strip()
        peer_id = str(msg.get("peer_id") or uid).strip()
        raw_atts = msg.get("attachments")
        attachments = raw_atts if isinstance(raw_atts, list) else []
        voice_url = None
        for att in attachments:
            if isinstance(att, dict) and att.get("type") in ("audio_message", "doc"):
                sub_dict = att.get(att.get("type"))
                if isinstance(sub_dict, dict):
                    voice_url = sub_dict.get("link_ogg") or sub_dict.get("url")
                    if voice_url:
                        break
        if uid:
            inbound = InboundMessage(
                platform="vk", platform_user_id=uid, chat_id=peer_id, text=msg.get("text", ""), voice_url=voice_url
            )

    elif event_type == "message_event":
        uid = str(obj.get("user_id") or "").strip()
        peer_id = str(obj.get("peer_id") or uid).strip()
        ev_id = obj.get("event_id")
        if ev_id and await _is_duplicate_update_async(f"vk_ev:{uid}:{ev_id}"):
            return Response(content="ok", media_type="text/plain")
        if ev_id:
            token = settings.VK_GROUP_TOKEN
            if token and uid.isdigit() and peer_id.lstrip("-").isdigit():
                async def _ack_vk_event(eid=ev_id, u=int(uid), p=int(peer_id), tok=token):
                    try:
                        async with httpx.AsyncClient(timeout=5.0) as cl:
                            await cl.post(
                                "https://api.vk.com/method/messages.sendMessageEventAnswer",
                                data={"event_id": eid, "user_id": u, "peer_id": p, "access_token": tok, "v": "5.199"},
                            )
                    except Exception:
                        pass
                asyncio.create_task(_ack_vk_event())
        raw_payload = obj.get("payload")
        action = None
        if isinstance(raw_payload, str):
            try:
                parsed_p = json.loads(raw_payload)
                action = parsed_p.get("action") if isinstance(parsed_p, dict) else str(parsed_p)
            except Exception:
                action = raw_payload
        elif isinstance(raw_payload, dict):
            action = raw_payload.get("action")
        elif raw_payload is not None:
            action = str(raw_payload)

        if uid:
            inbound = InboundMessage(
                platform="vk", platform_user_id=uid, chat_id=peer_id, callback_data=action
            )

    if inbound:
        if settings.APP_ENV == "test":
            await process_inbound_core(inbound)
        else:
            asyncio.create_task(process_inbound_core(inbound))

    return Response(content="ok", media_type="text/plain")


@router.post("/whatsapp")
async def whatsapp_webhook(request: Request) -> dict[str, str]:
    """Вебхук WhatsApp (Green-API) с авторизацией и дедупликацией."""
    verify_webhook_auth(request)
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    if data.get("typeWebhook") != "incomingMessageReceived":
        return {"status": "ok"}

    msg_id = data.get("idMessage") or data.get("receiptId")
    if msg_id and await _is_duplicate_update_async(f"wa:{msg_id}"):
        return {"status": "ok"}

    raw_mdata = data.get("messageData")
    msg_data = raw_mdata if isinstance(raw_mdata, dict) else {}
    raw_sdata = data.get("senderData")
    sender_data = raw_sdata if isinstance(raw_sdata, dict) else {}
    sender_id = str(sender_data.get("sender") or "").replace("@c.us", "").strip()
    chat_id = str(sender_data.get("chatId") or sender_id).strip()
    type_msg = msg_data.get("typeMessage")

    text, voice_url, callback_data = None, None, None
    if type_msg == "textMessage":
        raw_tdata = msg_data.get("textMessageData")
        text = raw_tdata.get("textMessage") if isinstance(raw_tdata, dict) else None
    elif type_msg == "extendedTextMessage":
        raw_edata = msg_data.get("extendedTextMessageData")
        text = raw_edata.get("text") if isinstance(raw_edata, dict) else None
    elif type_msg in ("audioMessage", "voiceMessage"):
        raw_fdata = msg_data.get("fileMessageData")
        voice_url = raw_fdata.get("downloadUrl") if isinstance(raw_fdata, dict) else None
    elif type_msg in ("buttonsResponseMessage", "templateButtonReplyMessage", "interactiveButtonsResponse"):
        btn = (
            msg_data.get("interactiveButtonsResponse")
            or msg_data.get("buttonsResponseMessage")
            or msg_data.get("buttonsResponseMessageData")
            or msg_data.get("templateButtonReplyMessage")
        )
        if isinstance(btn, dict):
            callback_data = btn.get("selectedId") or btn.get("selectedButtonId")
            text = btn.get("selectedDisplayText") or btn.get("selectedButtonText") or callback_data

    if sender_id and (text or voice_url or callback_data):
        inbound = InboundMessage(
            platform="whatsapp", platform_user_id=sender_id, chat_id=chat_id, text=text, voice_url=voice_url, callback_data=callback_data
        )
        if settings.APP_ENV == "test":
            await process_inbound_core(inbound)
        else:
            asyncio.create_task(process_inbound_core(inbound))

    return {"status": "ok"}


@router.post("/viber")
async def viber_webhook(
    request: Request,
    x_viber_content_signature: str | None = Header(None, alias="X-Viber-Content-Signature"),
) -> dict[str, Any]:
    """Вебхук Viber REST API с обязательной валидацией HMAC SHA-256 подписи и дедупликацией."""
    raw_body = await request.body()

    if settings.VIBER_AUTH_TOKEN:
        if not x_viber_content_signature:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Missing Viber signature header")
        sig = hmac.new(settings.VIBER_AUTH_TOKEN.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, x_viber_content_signature):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Viber signature")
    else:
        verify_webhook_auth(request)

    try:
        data = json.loads(raw_body.decode("utf-8", errors="replace")) if raw_body else {}
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON in Viber payload")

    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    if data.get("event") == "webhook":
        return {"status": 0, "message": "ok"}

    msg_token = data.get("message_token")
    if msg_token and await _is_duplicate_update_async(f"viber:{msg_token}"):
        return {"status": 0}

    if data.get("event") == "message":
        sender = data.get("sender")
        uid = str(sender.get("id", "")) if isinstance(sender, dict) else ""
        msg = data.get("message")
        if not isinstance(msg, dict):
            msg = {}
        voice_url = msg.get("media") if msg.get("type") in ("voice", "audio", "file") else None
        if uid:
            inbound = InboundMessage(
                platform="viber", platform_user_id=uid, chat_id=uid, text=msg.get("text"), voice_url=voice_url
            )
            if settings.APP_ENV == "test":
                await process_inbound_core(inbound)
            else:
                asyncio.create_task(process_inbound_core(inbound))

    return {"status": 0}


@router.post("/max")
async def max_webhook(request: Request) -> dict[str, str]:
    """Вебхук корпоративного мессенджера MAX с поддержкой событийного/прямого формата и дедупликацией."""
    verify_webhook_auth(request)
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="Expected JSON object")

    # Дедупликация входящих апдейтов MAX
    msg_obj = data.get("message") if isinstance(data.get("message"), dict) else {}
    body_obj = msg_obj.get("body") if isinstance(msg_obj.get("body"), dict) else {}
    update_key = (
        data.get("update_id")
        or data.get("message_id")
        or data.get("callback_id")
        or body_obj.get("message_id")
    )
    if update_key and await _is_duplicate_update_async(f"max:{update_key}"):
        return {"status": "ok"}

    uid, chat_id, text, voice_url, callback_data = "", "", None, None, None

    u_type = data.get("update_type")
    if u_type == "message_created":
        msg = msg_obj
        body = body_obj
        sender = msg.get("sender") if isinstance(msg.get("sender"), dict) else {}
        uid = str(sender.get("user_id") or msg.get("sender_id") or "").strip()
        if not uid:
            logger.warning("Отклонено сообщение MAX без user_id: %s", data)
            return {"status": "error", "message": "Missing sender user_id"}
        recipient_obj = msg.get("recipient") if isinstance(msg.get("recipient"), dict) else {}
        chat_id = str(recipient_obj.get("chat_id") or uid).strip()
        text = body.get("text")
        raw_atts = body.get("attachments")
        attachments = raw_atts if isinstance(raw_atts, list) else []
        voice_url = next(
            (att.get("payload", {}).get("url") if isinstance(att.get("payload"), dict) else att.get("url")
             for att in attachments if isinstance(att, dict) and att.get("type") in ("audio", "voice", "file")),
            None,
        )
    elif u_type == "message_callback":
        cb = data.get("callback") if isinstance(data.get("callback"), dict) else {}
        user_obj = cb.get("user") if isinstance(cb.get("user"), dict) else {}
        uid = str(user_obj.get("user_id") or cb.get("user_id") or "").strip()
        if not uid:
            logger.warning("Отклонен callback MAX без user_id: %s", data)
            return {"status": "error", "message": "Missing callback user_id"}
        cb_msg = cb.get("message") if isinstance(cb.get("message"), dict) else {}
        recipient_obj = cb_msg.get("recipient") if isinstance(cb_msg.get("recipient"), dict) else {}
        chat_id = str(recipient_obj.get("chat_id") or uid)
        callback_data = str(cb.get("payload") or "")
    else:
        # Прямой формат
        uid = str(data.get("user_id") or data.get("sender_id") or "").strip()
        if not uid or uid == "default_user":
            logger.warning("Отклонен прямой запрос MAX без валидного user_id: %s", data)
            return {"status": "error", "message": "Missing or invalid user_id"}
        chat_id = str(data.get("chat_id") or uid)
        text = data.get("text")
        voice_url = data.get("voice_url") or data.get("audio_url")
        callback_data = data.get("payload") or data.get("callback_data")

    inbound = InboundMessage(
        platform="max",
        platform_user_id=uid,
        chat_id=chat_id,
        text=text,
        voice_url=voice_url,
        callback_data=callback_data,
    )
    if settings.APP_ENV == "test":
        await process_inbound_core(inbound)
    else:
        asyncio.create_task(process_inbound_core(inbound))

    return {"status": "ok"}


# =========================================================================
# ВЕБХУКИ 1C И БИТРИКС24 С АВТОРИЗАЦИЕЙ И ТОЧНЫМ ПОИСКОМ
# =========================================================================
class OrderStatusUpdateRequest(BaseModel):
    document_number: str = Field(..., min_length=1, max_length=128)
    status: str = Field(..., min_length=1, max_length=255)
    comment: str | None = Field(default=None, max_length=1000)


def _escape_like_wildcards(val: str) -> str:
    """Экранирует специальные символы SQL LIKE (%, _, \\) для предотвращения wildcard-инъекций."""
    return val.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


@router.post("/1c/status", tags=["1C API"])
@router.post("/orders/{doc_num}/status", tags=["1C API"])
async def update_order_status(
    payload: OrderStatusUpdateRequest,
    request: Request,
    doc_num: str | None = None,
) -> dict[str, Any]:
    """Вебхук обновления статуса из 1С с точным поиском номера документа и валидацией формата."""
    verify_webhook_auth(request)
    target_doc = (doc_num or payload.document_number).strip()
    if not target_doc or len(target_doc) > 128 or not re.match(r"^[A-Za-zА-Яа-яЁё0-9_\-]+$", target_doc):
        raise HTTPException(status_code=400, detail="Invalid document number format")

    safe_target = _escape_like_wildcards(target_doc)

    async with async_session_factory() as session:
        # Точный поиск: равенство или точное совпадение токена в составном номере "ЗК-1 / Б24-2"
        # а также поддержка суффиксов очереди Outbox: "ЗК-1 (остальное в OUTBOX-2)"
        stmt = (
            select(OrderRecord)
            .where(
                (OrderRecord.document_number_1c == target_doc)
                | (OrderRecord.document_number_1c.like(f"{safe_target} / %", escape="\\"))
                | (OrderRecord.document_number_1c.like(f"% / {safe_target}", escape="\\"))
                | (OrderRecord.document_number_1c.like(f"% / {safe_target} / %", escape="\\"))
                | (OrderRecord.document_number_1c.like(f"{safe_target} %", escape="\\"))
                | (OrderRecord.document_number_1c.like(f"% / {safe_target} %", escape="\\"))
            )
            .order_by(OrderRecord.id.desc())
        )
        res = await session.execute(stmt)
        order = res.scalars().first()
        if not order:
            raise HTTPException(status_code=404, detail=f"Заказ {target_doc} не найден в базе данных.")

        order.status = payload.status
        await session.commit()

        chat_id = extract_chat_id_from_session(order.session_id)
        comment_part = f"\n💬 <i>{html.escape(payload.comment)}</i>" if payload.comment else ""
        text = (
            f"🚚 <b>Статус заказа <code>{html.escape(target_doc)}</code> обновлен!</b>\n\n"
            f"📌 Новый статус: <b>{html.escape(payload.status)}</b>"
            f"{comment_part}\n\n"
            f"💰 Сумма: {order.total_amount:,.2f} ₽"
        )
        await dispatcher.send(OutboundMessage(platform=order.platform, chat_id=chat_id, text=text))
        return {
            "status": "ok",
            "document_number": target_doc,
            "order_id": order.id,
            "new_status": payload.status,
            "notified_platform": order.platform,
        }


@router.post("/bitrix/product-sync", tags=["Catalog"])
@router.get("/catalog/sync", tags=["Catalog"])
async def bitrix_catalog_webhook(request: Request) -> dict[str, Any]:
    """Вебхук синхронизации каталога из Битрикс24 с защитой от параллельного шторма запросов."""
    verify_webhook_auth(request)
    sync_lock_key = "catalog_sync_cooldown"
    lock_token = str(uuid.uuid4())
    if not await redis_client.acquire_lock(sync_lock_key, timeout=15, lock_token=lock_token):
        return {
            "status": "skipped",
            "items_count": len(catalog_matcher.catalog),
            "message": "Синхронизация каталога уже выполняется.",
        }
    try:
        synced = await sync_catalog_from_bitrix()
        if synced:
            catalog_matcher.reload(synced)
            return {
                "status": "ok",
                "items_count": len(synced),
                "message": f"Каталог успешно синхронизирован: {len(synced)} позиций в памяти.",
            }
        return {
            "status": "warning",
            "items_count": len(catalog_matcher.catalog),
            "message": "Битрикс24 вернул пустой список товаров или недоступен. Локальный каталог не изменен.",
        }
    finally:
        await redis_client.release_lock(sync_lock_key, lock_token=lock_token)


class OneCStockUpdate(BaseModel):
    sku_id: str = Field(..., min_length=1, max_length=128)
    stock_quantity: float = Field(default=0.0, ge=0.0, le=1000000.0)
    in_stop_list: bool = False


@router.post("/1c/stock", tags=["1C Integration"])
async def onec_stock_webhook(payload: OneCStockUpdate, request: Request) -> dict[str, Any]:
    """Вебхук обновления остатков и стоп-листа из 1С с авторизацией."""
    verify_webhook_auth(request)
    is_oos = payload.in_stop_list or payload.stock_quantity <= 0
    if is_oos:
        catalog_matcher.mark_out_of_stock(payload.sku_id)
        action_desc = "помещен в стоп-лист (остаток 0)"
    else:
        catalog_matcher.mark_in_stock(payload.sku_id, payload.stock_quantity)
        action_desc = f"остаток обновлен: {payload.stock_quantity:g}"

    logger.info("1С Stock Webhook: товар %s %s", payload.sku_id, action_desc)
    return {
        "status": "ok",
        "sku_id": payload.sku_id,
        "stock": payload.stock_quantity,
        "is_out_of_stock": is_oos,
        "message": f"Товар {payload.sku_id} {action_desc}",
    }
