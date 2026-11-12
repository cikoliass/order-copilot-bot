import asyncio
import hashlib
import hmac
import html
import ipaddress
import json
import logging
import re
import time
import urllib.parse
from collections import defaultdict
from typing import Any, Optional
import httpx
from fastapi import APIRouter, Header, HTTPException, Request, Response, status
from pydantic import BaseModel
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
)
from sqlalchemy import select

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/webhook", tags=["Webhooks"])

# Очередь блокировок на уровне сессии для защиты от гонок сообщений
_session_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_processed_updates: dict[str, float] = {}

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
    """Дедупликация входящих вебхук-апдейтов."""
    now = time.time()
    to_del = [k for k, exp in _processed_updates.items() if now > exp]
    for k in to_del:
        _processed_updates.pop(k, None)

    if update_key in _processed_updates:
        return True
    _processed_updates[update_key] = now + 300.0
    return False


def verify_webhook_auth(
    request: Request,
    secret_header_name: Optional[str] = None,
    allow_query: bool = True,
) -> None:
    """
    Проверка подлинности вебхука по WEBHOOK_SECRET.
    Защищает эндпоинты от подделки запросов и несанкционированного доступа.
    """
    expected = settings.WEBHOOK_SECRET
    if not expected:
        return

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

    # 3. Query param ?secret=...
    if allow_query:
        query_secret = request.query_params.get("secret") or request.query_params.get("token")
        if query_secret and hmac.compare_digest(query_secret, expected):
            return

    # В тестовом окружении без секретов разрешаем вызовы
    if settings.APP_ENV == "test":
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
    return "\n".join(lines)


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


def _merge_items(existing: list[MatchedItem], new_items: list[MatchedItem]) -> list[MatchedItem]:
    for nm in new_items:
        merged = False
        if nm.sku_id:
            for ex in existing:
                if ex.sku_id == nm.sku_id and ex.base_unit == nm.base_unit:
                    ex.final_quantity = round(ex.final_quantity + nm.final_quantity, 3)
                    ex.total_sum = round(ex.final_quantity * ex.price, 2)
                    ex.raw_quantity = round(ex.raw_quantity + nm.raw_quantity, 3)
                    merged = True
                    break
        if not merged:
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

    items[target_idx] = re_matched[0].model_dump()
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
    session_id: str, platform: str, chat_id: str, order_text: str, items_to_add: list[RawItem], delivery_date: str | None = None
) -> bool:
    if not items_to_add:
        return False
    new_matched = await catalog_matcher.match_order_async(items_to_add)
    new_draft = {
        "raw_text": order_text,
        "delivery_date": delivery_date or "По регламенту",
        "matched_items": [it.model_dump() for it in new_matched],
    }
    prefix = _fmt_msg(platform, "🆕 Предыдущий черновик отменен. Новый заказ:\n\n", "🆕 <i>Предыдущий черновик отменен. Новый заказ:</i>\n\n")
    await _save_and_send_card(session_id, platform, chat_id, new_draft, prefix=prefix)
    return True


async def _handle_cancel_command(session_id: str, platform: str, chat_id: str) -> None:
    await redis_client.clear_order_draft(session_id)
    msg = _fmt_msg(platform, "❌ Заказ отменен. Отправьте новый заказ текстом или голосом.", "❌ <b>Заказ отменен.</b> Отправьте новый заказ текстом или голосом.")
    await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text=msg))


async def _handle_predict_command(session_id: str, platform: str, chat_id: str) -> None:
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
    if not await redis_client.acquire_lock(session_id, timeout=settings.LOCK_TIMEOUT_SEC):
        await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text="⏳ <b>Заказ уже отправляется в 1С/Битрикс24, подождите...</b>"))
        return

    try:
        draft = await redis_client.get_order_draft(session_id)
        if not draft or not draft.get("matched_items"):
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text="⚠️ <b>Корзина пуста или устарела.</b> Отправьте состав заказа заново."))
            return

        all_items = [MatchedItem.model_validate(it) for it in draft["matched_items"]]

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
        )

        # Обновляем статус заказа в БД
        from app.core.database import update_order_doc_number
        await update_order_doc_number(order_record.id, result.document_number, result.state)

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
        await redis_client.release_lock(session_id)


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
        if await _apply_cart_delete(session_id, platform, chat_id, draft, int(m.group(1)) - 1):
            return True

    if m := re.match(r"^(?:поменяй|измени|сделай)\s+(?:поз(?:ицию)?\s*)?#?(\d+)\s+(?:на\s+)?(\d+(?:[.,]\d+)?)\s*([a-zA-Zа-яА-ЯёЁ]+)?$", raw_lower):
        idx_str, qty_str, unit_str = m.groups()
        if await _apply_cart_update(session_id, platform, chat_id, draft, int(idx_str) - 1, float(qty_str.replace(",", ".")), unit_str):
            return True

    if (m := re.match(r"^(?:copilot:analog:(\d+)|(?:замени|давай|положи|возьми)\s+(?:поз(?:ицию)?\s*)?#?(\d+)?\s*(?:на\s+)?аналог.*)$", raw_lower)) or "аналог" in raw_lower:
        idx = int(m.group(1) or m.group(2)) - 1 if m and (m.group(1) or m.group(2)) else -1
        if await _apply_analog_swap(session_id, platform, chat_id, draft, idx):
            return True

    if (m := re.match(r"^(?:copilot:round:(\d+)|(?:округли|кратно|добавь\s+до\s+пачки)\s*(?:поз(?:ицию)?\s*)?#?(\d+)?.*)$", raw_lower)) or "округли" in raw_lower:
        idx = int(m.group(1) or m.group(2)) - 1 if m and (m.group(1) or m.group(2)) else -1
        if await _apply_round_pack(session_id, platform, chat_id, draft, idx):
            return True

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
            new_qty = cart_intent.new_quantity or matched_it.final_quantity
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
        return await _apply_new_order(session_id, platform, chat_id, order_text, items_to_add, cart_intent.delivery_date)

    if action == "swap_analog":
        return await _apply_analog_swap(session_id, platform, chat_id, draft)

    if action == "round_pack":
        return await _apply_round_pack(session_id, platform, chat_id, draft)

    return False


def _is_safe_url_for_download(url: str) -> bool:
    """Защита от SSRF (Server-Side Request Forgery) и запросов к локальным сетям."""
    try:
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        hostname = parsed.hostname
        if not hostname:
            return False
        if hostname.lower() in ("localhost", "127.0.0.1", "::1"):
            return False
        try:
            ip = ipaddress.ip_address(hostname)
            if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
                return False
        except ValueError:
            pass
        return True
    except Exception:
        return False


async def _download_audio(url: str) -> bytes | None:
    """Безопасное скачивание аудио с защитой от SSRF и утечки MAX_BOT_TOKEN."""
    if not _is_safe_url_for_download(url):
        logger.error("Заблокирована попытка SSRF по URL: %s", url)
        return None

    try:
        parsed = urllib.parse.urlparse(url)
        hostname = (parsed.hostname or "").lower()

        # Токен MAX передается ТОЛЬКО на доверенный домен max.ru, а не на evil.com?max.ru
        headers = {}
        if settings.MAX_BOT_TOKEN and (hostname == "platform-api2.max.ru" or hostname.endswith(".max.ru")):
            headers["Authorization"] = settings.MAX_BOT_TOKEN

        async with httpx.AsyncClient(timeout=25.0) as client:
            resp = await client.get(url, headers=headers)
            if resp.status_code == 200:
                return resp.content
            logger.error("Ошибка скачивания аудио по URL %s: HTTP %d", url, resp.status_code)
    except Exception as exc:
        logger.error("Не удалось скачать аудио по URL %s: %s", url, exc)
    return None


async def _download_tg_file(file_id: str) -> bytes | None:
    try:
        token = settings.TELEGRAM_BOT_TOKEN
        async with httpx.AsyncClient(proxy=settings.TELEGRAM_PROXY, timeout=10.0) as client:
            res = await client.get(f"https://api.telegram.org/bot{token}/getFile?file_id={file_id}")
            if res.status_code == 200 and (fp := res.json().get("result", {}).get("file_path")):
                dl = await client.get(f"https://api.telegram.org/file/bot{token}/{fp}")
                if dl.status_code == 200:
                    return dl.content
    except Exception as exc:
        logger.warning("Не удалось скачать аудиофайл Telegram: %s", exc)
    return None


async def process_inbound_core(inbound: InboundMessage) -> None:
    """Единое ядро обработки входящих сообщений изо всех каналов с блокировкой на сессию."""
    session_id, platform, chat_id = inbound.session_id, inbound.platform, inbound.chat_id

    # Блокировка на уровне сессии предотвращает состояние гонки по черновику
    async with _session_locks[session_id]:
        action = (inbound.callback_data or "").strip() or (classify_action_text(inbound.text) if inbound.text else None)
        if action:
            if action == "copilot:confirm":
                await execute_order_confirmation(session_id, platform, chat_id, inbound.platform_user_id)
                return
            if action == "copilot:cancel":
                await _handle_cancel_command(session_id, platform, chat_id)
                return
            if action == "copilot:predict":
                await _handle_predict_command(session_id, platform, chat_id)
                return
            if action.startswith("copilot:del:"):
                try:
                    del_idx = int(action.split(":")[-1]) - 1
                    if (draft := await redis_client.get_order_draft(session_id)) and draft.get("matched_items"):
                        await _apply_cart_delete(session_id, platform, chat_id, draft, del_idx)
                        return
                except Exception as exc:
                    logger.warning("Ошибка обработки copilot:del: %s", exc)
            elif action.startswith("copilot:analog:"):
                try:
                    idx = int(action.split(":")[-1]) - 1
                    if (draft := await redis_client.get_order_draft(session_id)) and draft.get("matched_items"):
                        if await _apply_analog_swap(session_id, platform, chat_id, draft, idx):
                            return
                except Exception as exc:
                    logger.warning("Ошибка обработки copilot:analog: %s", exc)
            elif action.startswith("copilot:round:"):
                try:
                    idx = int(action.split(":")[-1]) - 1
                    if (draft := await redis_client.get_order_draft(session_id)) and draft.get("matched_items"):
                        if await _apply_round_pack(session_id, platform, chat_id, draft, idx):
                            return
                except Exception as exc:
                    logger.warning("Ошибка обработки copilot:round: %s", exc)

        voice_bytes = inbound.voice_bytes or (await _download_audio(inbound.voice_url) if inbound.voice_url else None)
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
                logger.error("Сбой распознавания аудио SpeechKit: %s", exc)
                await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text="❌ Ошибка распознавания речи в Yandex SpeechKit. Попробуйте написать текстом."))
                return

        order_text = (recognized_text or inbound.text or "").strip()
        if not order_text:
            await dispatcher.send(OutboundMessage(platform=platform, chat_id=chat_id, text="⚠️ Пожалуйста, отправьте заказ текстом или голосовым сообщением."))
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
        }
        prefix = _fmt_msg(platform, "➕ Добавлено к текущему заказу:\n\n", "➕ <i>Добавлено к текущему заказу:</i>\n\n") if is_appended else ""
        await _save_and_send_card(session_id, platform, chat_id, new_draft, prefix=prefix, missing_hints=missing_items)


# =========================================================================
# ВЕБХУКИ МЕССЕНДЖЕРОВ С АВТОРИЗАЦИЕЙ И БЫСТРЫМ ОТВЕТОМ
# =========================================================================
@router.post("/telegram")
async def telegram_webhook(request: Request) -> dict[str, Any]:
    """Вебхук Telegram Bot API с авторизацией secret_token и защитой от дублей."""
    verify_webhook_auth(request, secret_header_name="X-Telegram-Bot-Api-Secret-Token")
    data = await request.json()

    if update_id := data.get("update_id"):
        if _is_duplicate_update(f"tg:{update_id}"):
            return {"ok": True}

    inbound = None
    if cb := data.get("callback_query"):
        u_id = str(cb.get("from", {}).get("id", ""))
        chat_id = str(cb.get("message", {}).get("chat", {}).get("id", u_id))
        inbound = InboundMessage(
            platform="telegram", platform_user_id=u_id, chat_id=chat_id, callback_data=cb.get("data")
        )

    elif msg := data.get("message"):
        u_id = str(msg.get("from", {}).get("id", ""))
        chat_id = str(msg.get("chat", {}).get("id", u_id))
        voice = msg.get("voice") or msg.get("audio") or msg.get("video_note")
        voice_bytes = await _download_tg_file(voice.get("file_id")) if voice and settings.TELEGRAM_BOT_TOKEN else None
        inbound = InboundMessage(
            platform="telegram", platform_user_id=u_id, chat_id=chat_id, text=msg.get("text"), voice_bytes=voice_bytes
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
    """Вебхук ВКонтакте Callback API с валидацией secret и confirmation."""
    data = await request.json()

    if settings.VK_SECRET_KEY and data.get("secret") != settings.VK_SECRET_KEY and data.get("type") != "confirmation":
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid VK secret")

    event_type = data.get("type")
    if event_type == "confirmation":
        return Response(content=settings.VK_CONFIRMATION_CODE, media_type="text/plain")

    obj = data.get("object", {})
    inbound = None

    if event_type == "message_new":
        msg = obj.get("message", {})
        msg_id = msg.get("id") or msg.get("conversation_message_id")
        if msg_id and _is_duplicate_update(f"vk:{msg_id}"):
            return Response(content="ok", media_type="text/plain")

        uid = str(msg.get("from_id", ""))
        voice_url = next(
            (att.get(att.get("type"), {}).get("link_ogg") or att.get(att.get("type"), {}).get("url")
             for att in msg.get("attachments", []) if att.get("type") in ("audio_message", "doc")),
            None,
        )
        inbound = InboundMessage(
            platform="vk", platform_user_id=uid, chat_id=str(msg.get("peer_id", uid)), text=msg.get("text", ""), voice_url=voice_url
        )

    elif event_type == "message_event":
        uid = str(obj.get("user_id", ""))
        raw_payload = obj.get("payload", {})
        if isinstance(raw_payload, str):
            try:
                raw_payload = json.loads(raw_payload)
            except Exception:
                raw_payload = {"action": raw_payload}
        inbound = InboundMessage(
            platform="vk", platform_user_id=uid, chat_id=str(obj.get("peer_id", uid)), callback_data=raw_payload.get("action")
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
    data = await request.json()
    if data.get("typeWebhook") != "incomingMessageReceived":
        return {"status": "ok"}

    msg_id = data.get("idMessage") or data.get("receiptId")
    if msg_id and _is_duplicate_update(f"wa:{msg_id}"):
        return {"status": "ok"}

    msg_data, sender_data = data.get("messageData", {}), data.get("senderData", {})
    sender_id = sender_data.get("sender", "").replace("@c.us", "")
    chat_id = sender_data.get("chatId", sender_id)
    type_msg = msg_data.get("typeMessage")

    text, voice_url, callback_data = None, None, None
    if type_msg == "textMessage":
        text = msg_data.get("textMessageData", {}).get("textMessage")
    elif type_msg == "extendedTextMessage":
        text = msg_data.get("extendedTextMessageData", {}).get("text")
    elif type_msg in ("audioMessage", "voiceMessage"):
        voice_url = msg_data.get("fileMessageData", {}).get("downloadUrl")
    elif type_msg in ("buttonsResponseMessage", "templateButtonReplyMessage", "interactiveButtonsResponse"):
        btn = (
            msg_data.get("interactiveButtonsResponse")
            or msg_data.get("buttonsResponseMessage")
            or msg_data.get("buttonsResponseMessageData")
            or msg_data.get("templateButtonReplyMessage")
            or {}
        )
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
    """Вебхук Viber REST API с обязательной валидацией HMAC SHA-256 подписи."""
    raw_body = await request.body()

    if settings.VIBER_AUTH_TOKEN:
        if not x_viber_content_signature:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Missing Viber signature header")
        sig = hmac.new(settings.VIBER_AUTH_TOKEN.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, x_viber_content_signature):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Viber signature")

    data = json.loads(raw_body.decode("utf-8")) if raw_body else {}
    if data.get("event") == "webhook":
        return {"status": 0, "message": "ok"}

    if data.get("event") == "message":
        uid = str(data.get("sender", {}).get("id", ""))
        msg = data.get("message", {})
        voice_url = msg.get("media") if msg.get("type") in ("voice", "audio", "file") else None
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
    """Вебхук корпоративного мессенджера MAX с поддержкой как событийного, так и прямого формата."""
    verify_webhook_auth(request)
    data = await request.json()

    # Универсальный парсер: обрабатывает оба формата (platform updates / direct)
    uid, chat_id, text, voice_url, callback_data = "", "", None, None, None

    u_type = data.get("update_type")
    if u_type == "message_created":
        msg = data.get("message", {})
        body = msg.get("body", {})
        sender = msg.get("sender", {})
        uid = str(sender.get("user_id") or "")
        chat_id = str(msg.get("recipient", {}).get("chat_id") or uid)
        text = body.get("text")
        voice_url = next(
            (att.get("payload", {}).get("url") if isinstance(att.get("payload"), dict) else att.get("url")
             for att in body.get("attachments", []) if att.get("type") in ("audio", "voice", "file")),
            None,
        )
    elif u_type == "message_callback":
        cb = data.get("callback", {})
        uid = str(cb.get("user", {}).get("user_id") or cb.get("user_id") or "")
        chat_id = str(cb.get("message", {}).get("recipient", {}).get("chat_id") or uid)
        callback_data = str(cb.get("payload") or "")
    else:
        # Прямой формат
        uid = str(data.get("user_id") or data.get("sender_id", "default_user"))
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
    document_number: str
    status: str
    comment: str | None = None


@router.post("/1c/status", tags=["1C API"])
@router.post("/orders/{doc_num}/status", tags=["1C API"])
async def update_order_status(
    payload: OrderStatusUpdateRequest,
    request: Request,
    doc_num: str | None = None,
) -> dict[str, Any]:
    """Вебхук обновления статуса из 1С с точным поиском номера документа."""
    verify_webhook_auth(request)
    target_doc = (doc_num or payload.document_number).strip()

    async with async_session_factory() as session:
        # Точный поиск: равенство или точное совпадение токена в составном номере "ЗК-1 / Б24-2"
        stmt = (
            select(OrderRecord)
            .where(
                (OrderRecord.document_number_1c == target_doc)
                | (OrderRecord.document_number_1c.like(f"{target_doc} / %"))
                | (OrderRecord.document_number_1c.like(f"% / {target_doc}"))
                | (OrderRecord.document_number_1c.like(f"% / {target_doc} / %"))
            )
            .order_by(OrderRecord.id.desc())
        )
        res = await session.execute(stmt)
        order = res.scalars().first()
        if not order:
            raise HTTPException(status_code=404, detail=f"Заказ {target_doc} не найден в базе данных.")

        order.status = payload.status
        await session.commit()

        chat_id = order.session_id.split(":", 1)[1] if ":" in order.session_id else order.session_id
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
    """Вебхук синхронизации каталога из Битрикс24 (с защитой от стирания каталога)."""
    verify_webhook_auth(request)
    synced = await sync_catalog_from_bitrix()
    if synced:
        catalog_matcher.reload(synced)
        return {
            "status": "ok",
            "items_count": len(synced),
            "message": f"Каталог успешно синхронизирован: {len(synced)} позиций в памяти.",
        }
    else:
        # Не вызываем reload([])! Каталог сохраняется.
        return {
            "status": "warning",
            "items_count": len(catalog_matcher.catalog),
            "message": "Битрикс24 вернул пустой список товаров или недоступен. Локальный каталог не изменен.",
        }


class OneCStockUpdate(BaseModel):
    sku_id: str
    stock_quantity: float = 0.0
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
