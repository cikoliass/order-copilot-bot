import io
import logging
from html import escape as escape_html
from typing import Optional

from aiogram import Bot, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, Message

from app.bot.keyboards import (
    IDEAL_ORDER_BUTTON_TEXT,
    get_copilot_main_kb,
    get_edit_selection_kb,
    get_item_edit_actions_kb,
    get_start_kb,
)
from app.bot.states import OrderFSM
from app.core.database import save_confirmed_order
from app.services.asr import transcribe_voice
from app.services.bitrix_sync import sync_catalog_from_bitrix
from app.services.client_1c import client_1c
from app.services.matcher import MatchStatus, MatchedItem, catalog_matcher
from app.services.parser import extract_order_from_text
from app.services.predictor import order_predictor

logger = logging.getLogger(__name__)
router = Router()
_users_submitting: set[int] = set()


def format_copilot_summary(
    items: list[MatchedItem],
    delivery_date: Optional[str] = None,
    recognized_speech: Optional[str] = None,
) -> str:
    """Форматирует карточку заказа с индикаторами 🟢 и 🟡."""
    lines: list[str] = []

    if recognized_speech:
        lines.append(f"🎙 <i>Расшифровка: «{escape_html(recognized_speech)}»</i>\n")

    lines.append("📦 <b>Сводка заказа (Co-Pilot):</b>")
    if delivery_date:
        lines.append(f"📅 <b>Дата доставки:</b> {escape_html(delivery_date)}")
    lines.append("")

    total_order_sum = 0.0
    needs_review_count = 0

    for idx, item in enumerate(items, start=1):
        is_ok = item.status == MatchStatus.RECOGNIZED and item.sku_id is not None
        icon = "🟢" if is_ok else "🟡"
        if not is_ok:
            needs_review_count += 1

        if item.sku_id:
            recalc_str = (
                f" <i>({escape_html(item.recalculation_hint)})</i>"
                if item.recalculation_hint
                else ""
            )
            note_str = (
                f"\n   💬 <i>Комментарий: {escape_html(item.note)}</i>"
                if item.note
                else ""
            )
            lines.append(
                f"<b>{idx}.</b> {icon} <code>[{escape_html(item.sku_id)}]</code> "
                f"<b>{escape_html(item.catalog_name or '')}</b>\n"
                f"   └ Заказ: <b>{item.final_quantity:g} {escape_html(item.base_unit)}</b>{recalc_str} "
                f"× {item.price:g} ₽ = <b>{item.total_sum:,.2f} ₽</b>{note_str}"
            )
            total_order_sum += item.total_sum
        else:
            lines.append(
                f"<b>{idx}.</b> 🟡 <b>НЕ НАЙДЕНО В КАТАЛОГЕ</b>\n"
                f"   └ Исходный запрос: «{escape_html(item.raw_name)}» — "
                f"{item.raw_quantity:g} {escape_html(item.raw_unit)}"
            )

    lines.append("\n" + "─" * 28)
    lines.append(f"💰 <b>Итого по распознанным SKU:</b> {total_order_sum:,.2f} ₽")

    if needs_review_count > 0:
        lines.append(
            f"⚠️ <i>Позиций, требующих внимания (🟡): <b>{needs_review_count} шт.</b> "
            "Проверьте их перед отправкой или отредактируйте.</i>"
        )
    else:
        lines.append("✨ <i>Все позиции уверенно распознаны и готовы к выгрузке.</i>")

    return "\n".join(lines)


async def _process_raw_order_text(
    message: Message,
    status_msg: Message,
    state: FSMContext,
    raw_text: str,
    is_voice: bool = False,
) -> None:
    """Единый пайплайн: LLM Extraction -> Catalog Matching -> Co-Pilot UI."""
    extracted = await extract_order_from_text(raw_text)
    if not extracted.items:
        await status_msg.edit_text(
            "⚠️ Не удалось обнаружить товарные позиции в сообщении.\n"
            "Пожалуйста, укажите наименования и количество (например: <i>«2 коробки молока и 5 кг муки»</i>).",
            parse_mode="HTML",
        )
        return

    matched_items = catalog_matcher.match_order(extracted)

    await state.set_state(OrderFSM.reviewing)
    await state.update_data(
        raw_text=raw_text,
        delivery_date=extracted.delivery_date,
        matched_items=[item.model_dump() for item in matched_items],
        recognized_speech=raw_text if is_voice else None,
    )

    summary_html = format_copilot_summary(
        matched_items,
        delivery_date=extracted.delivery_date,
        recognized_speech=raw_text if is_voice else None,
    )

    # ИИ-подсказка забытых регулярных товаров пользователя
    missing_items = order_predictor.suggest_missing_items(str(message.from_user.id), matched_items)
    if missing_items:
        hints = ", ".join(
            f"<b>{escape_html(m.catalog_name or '')}</b> "
            f"({m.final_quantity:g} {escape_html(m.base_unit)})"
            for m in missing_items
        )
        summary_html += f"\n\n💡 <i>На основе прошлых заказов вы обычно также берете: {hints}</i>"

    await status_msg.edit_text(
        summary_html,
        reply_markup=get_copilot_main_kb(),
        parse_mode="HTML",
    )


async def _prepare_ideal_order(user_id: str, state: FSMContext) -> Optional[str]:
    predicted_items = order_predictor.predict_ideal_order(user_id)
    if not predicted_items:
        return None

    await state.set_state(OrderFSM.reviewing)
    await state.update_data(
        raw_text="[ИИ-прогноз: Идеальный заказ]",
        delivery_date="Завтра",
        matched_items=[item.model_dump() for item in predicted_items],
        recognized_speech=None,
    )

    return (
        "<b>ИИ сформировал ваш «Идеальный заказ»</b>\n"
        "<i>Рассчитано на основе частоты и средних объемов ваших прошлых закупок:</i>\n\n"
        + format_copilot_summary(predicted_items, delivery_date="Завтра")
    )


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer(
        "👨‍🍳 <b>HoReCa Order Co-Pilot</b>\n\n"
        "Отправьте заказ в свободной форме <b>текстом</b> или <b>голосовым сообщением</b>.\n\n"
        "Нажмите «✨ Собрать идеальный заказ» на клавиатуре рядом с полем ввода, "
        "чтобы собрать заказ по истории закупок, "
        "или отправьте заказ текстом/голосом.\n\n"
        "🔧 <b>Команда:</b>\n"
        "• /sync — 🔄 Обновить базу товаров и цен из Битрикс24\n\n"
        "<b>Пример заказа:</b>\n"
        "<i>«На завтра: 2 коробки молока Простоквашино, ящик сливок Петмол 33 и 3 ведра Креметте»</i>",
        parse_mode="HTML",
        reply_markup=get_start_kb(),
    )


@router.message(Command("sync"))
async def cmd_sync_bitrix(message: Message) -> None:
    """Синхронизирует базу товаров из Битрикс24 по запросу."""
    status = await message.answer("🔄 <i>Загружаю каталог товаров и цен из Битрикс24...</i>", parse_mode="HTML")
    try:
        new_catalog = await sync_catalog_from_bitrix()
        if not new_catalog:
            await status.edit_text("⚠️ Не удалось загрузить товары из Битрикс24. Проверьте BITRIX_WEBHOOK_URL и наличие товаров.")
            return

        catalog_matcher.reload(new_catalog)
        preview = ", ".join(escape_html(item["name"]) for item in new_catalog[:5])
        await status.edit_text(
            f"✅ <b>База продуктов синхронизирована с Битрикс24!</b>\n"
            f"📦 Загружено позиций: <b>{len(new_catalog)} шт.</b>\n"
            f"📋 Примеры: <i>{preview}...</i>",
            parse_mode="HTML",
        )
    except Exception:
        logger.exception("Ошибка синхронизации с Битрикс24")
        await status.edit_text(
            "❌ Не удалось обновить каталог. Попробуйте позже или обратитесь к администратору.",
            parse_mode="HTML",
        )


@router.message(Command("predict"))
async def cmd_predict_ideal_order(message: Message, state: FSMContext) -> None:
    """Формирует предугаданный Идеальный заказ на основе прошлых заказов."""
    await _send_ideal_order(message, state)


@router.message(F.text == IDEAL_ORDER_BUTTON_TEXT)
async def handle_ideal_order_button(message: Message, state: FSMContext) -> None:
    await _send_ideal_order(message, state)


async def _send_ideal_order(message: Message, state: FSMContext) -> None:
    summary_html = await _prepare_ideal_order(str(message.from_user.id), state)
    if summary_html is None:
        await message.answer(
            "✨ <b>Пока нет истории для идеального заказа</b>\n"
            "Отправьте и подтвердите первый заказ — после этого кнопка сможет "
            "собрать корзину по вашим закупкам.",
            parse_mode="HTML",
        )
        return

    data = await state.get_data()
    previous_message_id = data.get("ideal_order_message_id")
    if previous_message_id:
        try:
            await message.bot.delete_message(
                chat_id=message.chat.id,
                message_id=previous_message_id,
            )
        except TelegramBadRequest as exc:
            logger.info("Не удалось удалить предыдущую карточку прогноза: %s", exc)
            try:
                await message.bot.edit_message_reply_markup(
                    chat_id=message.chat.id,
                    message_id=previous_message_id,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[]),
                )
            except TelegramBadRequest as markup_exc:
                logger.warning("Не удалось убрать кнопки старой карточки прогноза: %s", markup_exc)

    sent_message = await message.answer(
        summary_html,
        reply_markup=get_copilot_main_kb(),
        parse_mode="HTML",
    )
    await state.update_data(ideal_order_message_id=sent_message.message_id)


@router.message(OrderFSM.editing_item, F.text)
async def handle_single_item_edit(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    edit_idx: Optional[int] = data.get("editing_index")
    raw_items_dump: list[dict] = data.get("matched_items", [])
    matched_items = [MatchedItem.model_validate(x) for x in raw_items_dump]

    if edit_idx is None or edit_idx >= len(matched_items):
        await state.set_state(OrderFSM.reviewing)
        await message.answer("⚠️ Ошибка индекса позиции. Возвращаюсь к заказу.")
        return

    status_msg = await message.answer("🔄 <i>Пересчитываю позицию...</i>", parse_mode="HTML")
    extracted = await extract_order_from_text(message.text)

    if not extracted.items:
        await status_msg.edit_text(
            "⚠️ Не удалось распознать товар и количество. Попробуйте написать четче.",
            parse_mode="HTML",
        )
        return

    new_matched = catalog_matcher.match_order(extracted)
    matched_items[edit_idx : edit_idx + 1] = new_matched

    await state.set_state(OrderFSM.reviewing)
    await state.update_data(matched_items=[item.model_dump() for item in matched_items])

    summary_html = format_copilot_summary(
        matched_items,
        delivery_date=data.get("delivery_date"),
        recognized_speech=data.get("recognized_speech"),
    )
    await status_msg.edit_text(
        summary_html,
        reply_markup=get_copilot_main_kb(),
        parse_mode="HTML",
    )


@router.message(F.voice)
async def handle_voice_message(message: Message, bot: Bot, state: FSMContext) -> None:
    status_msg = await message.answer(
        "🎧 <i>Скачиваю аудио и распознаю речь (Whisper)...</i>",
        parse_mode="HTML",
    )
    try:
        file_info = await bot.get_file(message.voice.file_id)
        ogg_buffer = io.BytesIO()
        await bot.download_file(file_info.file_path, destination=ogg_buffer)

        raw_text = await transcribe_voice(ogg_buffer.getvalue())
        if not raw_text:
            await status_msg.edit_text("⚠️ Голосовое сообщение пустое или не содержит речи.")
            return

        await status_msg.edit_text(
            f"🎙 <i>Текст распознан: «{escape_html(raw_text)}»</i>\n"
            "⏳ <i>Сверяю со справочником Битрикс24...</i>",
            parse_mode="HTML",
        )
        await _process_raw_order_text(message, status_msg, state, raw_text, is_voice=True)

    except Exception:
        logger.exception("Ошибка при обработке голосового заказа")
        await status_msg.edit_text(
            "❌ Не удалось обработать аудио. Попробуйте отправить заказ текстом.",
            parse_mode="HTML",
        )


@router.message(F.text)
async def handle_text_message(message: Message, state: FSMContext) -> None:
    status_msg = await message.answer(
        "⏳ <i>Извлекаю позиции и сверяю с каталогом товаров...</i>",
        parse_mode="HTML",
    )
    try:
        await _process_raw_order_text(message, status_msg, state, message.text, is_voice=False)
    except Exception:
        logger.exception("Ошибка при обработке текстового заказа")
        await status_msg.edit_text(
            "❌ Не удалось разобрать заказ. Проверьте текст и попробуйте ещё раз.",
            parse_mode="HTML",
        )


@router.callback_query(F.data == "copilot:edit_menu")
async def cb_open_edit_menu(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    items = [MatchedItem.model_validate(x) for x in data.get("matched_items", [])]
    if not items:
        await callback.answer("Заказ пуст", show_alert=True)
        return

    await state.set_state(OrderFSM.selecting_item)
    await callback.message.edit_reply_markup(reply_markup=get_edit_selection_kb(items))
    await callback.answer("Выберите позицию для редактирования")


@router.callback_query(F.data.startswith("copilot:pick:"))
async def cb_pick_item_to_edit(callback: CallbackQuery, state: FSMContext) -> None:
    idx = int(callback.data.split(":")[-1])
    data = await state.get_data()
    items = [MatchedItem.model_validate(x) for x in data.get("matched_items", [])]

    if idx < 0 or idx >= len(items):
        await callback.answer("Позиция не найдена", show_alert=True)
        return

    target = items[idx]
    await state.set_state(OrderFSM.editing_item)
    await state.update_data(editing_index=idx)

    await callback.message.edit_text(
        f"✏️ <b>Редактирование позиции #{idx + 1}:</b>\n"
        f"Исходный запрос: «{escape_html(target.raw_name)}» "
        f"({target.raw_quantity:g} {escape_html(target.raw_unit)})\n"
        f"Текущее сопоставление: <b>{escape_html(target.catalog_name or 'Не распознано')}</b> "
        f"({target.final_quantity:g} {escape_html(target.base_unit)})\n\n"
        "👇 <i>Отправьте в чат новое название и количество или удалите позицию кнопкой ниже:</i>",
        reply_markup=get_item_edit_actions_kb(idx),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("copilot:delete:"))
async def cb_delete_item(callback: CallbackQuery, state: FSMContext) -> None:
    idx = int(callback.data.split(":")[-1])
    data = await state.get_data()
    items = [MatchedItem.model_validate(x) for x in data.get("matched_items", [])]

    if 0 <= idx < len(items):
        removed = items.pop(idx)
        await callback.answer(f"Удалено: {removed.catalog_name or removed.raw_name}")

    if not items:
        await state.clear()
        await callback.message.edit_text(
            "❌ Все позиции удалены. Заказ отменен.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[]),
        )
        return

    await state.set_state(OrderFSM.reviewing)
    await state.update_data(matched_items=[x.model_dump() for x in items])

    summary_html = format_copilot_summary(
        items,
        delivery_date=data.get("delivery_date"),
        recognized_speech=data.get("recognized_speech"),
    )
    await callback.message.edit_text(
        summary_html,
        reply_markup=get_copilot_main_kb(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "copilot:back")
async def cb_back_to_summary(callback: CallbackQuery, state: FSMContext) -> None:
    data = await state.get_data()
    items = [MatchedItem.model_validate(x) for x in data.get("matched_items", [])]
    await state.set_state(OrderFSM.reviewing)

    summary_html = format_copilot_summary(
        items,
        delivery_date=data.get("delivery_date"),
        recognized_speech=data.get("recognized_speech"),
    )
    await callback.message.edit_text(
        summary_html,
        reply_markup=get_copilot_main_kb(),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data == "copilot:cancel")
async def cb_cancel_order(callback: CallbackQuery, state: FSMContext) -> None:
    await state.clear()
    await callback.message.edit_text(
        "❌ <b>Заказ отменен.</b>\nОтправьте новое текстовое или голосовое сообщение, чтобы создать заявку.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[]),
        parse_mode="HTML",
    )
    await callback.answer("Заказ отменен")


@router.callback_query(F.data == "copilot:confirm")
async def cb_confirm_and_send_1c(callback: CallbackQuery, state: FSMContext) -> None:
    user_id = callback.from_user.id
    if user_id in _users_submitting:
        await callback.answer("Заказ уже отправляется. Подождите немного.")
        return

    _users_submitting.add(user_id)
    try:
        data = await state.get_data()
        items = [MatchedItem.model_validate(x) for x in data.get("matched_items", [])]
        valid_items = [item for item in items if item.sku_id is not None]

        if not valid_items:
            await callback.answer("Нет распознанных позиций с SKU для отправки!", show_alert=True)
            return

        await callback.answer("⏳ Отправляю заказ в 1С и Битрикс24...")
        await callback.message.edit_reply_markup(reply_markup=None)
        client_id = f"TG-{user_id}"
        delivery_date = data.get("delivery_date")

        try:
            res_1c = await client_1c.send_order(
                client_id=client_id,
                matched_items=valid_items,
                delivery_date=delivery_date,
            )
        except Exception:
            logger.exception("Ошибка отправки заказа в 1С")
            await callback.message.edit_reply_markup(reply_markup=get_copilot_main_kb())
            await callback.message.answer(
                "❌ Не удалось отправить заказ. Проверьте соединение и попробуйте ещё раз.",
                parse_mode="HTML",
            )
            return

        try:
            await save_confirmed_order(
                telegram_user_id=str(user_id),
                raw_text=data.get("raw_text", ""),
                delivery_date=delivery_date,
                items_data=[item.model_dump() for item in valid_items],
                document_number_1c=res_1c.document_number,
                state_1c=res_1c.state,
            )
        except Exception:
            logger.exception(
                "Заказ %s отправлен в 1С, но не сохранен в локальной истории",
                res_1c.document_number,
            )
            await state.clear()
            await callback.message.edit_text(
                "⚠️ <b>Заказ отправлен в 1С, но не удалось сохранить его в истории бота.</b>\n"
                f"📄 Номер документа: <code>{escape_html(res_1c.document_number)}</code>\n"
                "Проверьте документ в 1С, прежде чем отправлять заказ повторно.",
                parse_mode="HTML",
            )
            return

        await state.clear()
        original_summary = callback.message.html_text or ""
        await callback.message.edit_text(
            f"{original_summary}\n\n"
            f"✅ <b>Заказ успешно зарегистрирован!</b>\n"
            f"📄 <b>Документы:</b> <code>{escape_html(res_1c.document_number)}</code>\n"
            f"📌 <b>Статус:</b> {escape_html(res_1c.state)}",
            parse_mode="HTML",
        )
    finally:
        _users_submitting.discard(user_id)