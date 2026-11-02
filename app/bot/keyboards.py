from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardMarkup,
)
from app.services.matcher import MatchedItem

IDEAL_ORDER_BUTTON_TEXT = "✨ Собрать идеальный заказ"


def get_start_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=IDEAL_ORDER_BUTTON_TEXT)]],
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="Введите заказ или нажмите кнопку",
    )


def get_copilot_main_kb() -> InlineKeyboardMarkup:
    """Главная клавиатура под сводкой заказа."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Подтвердить и отправить в 1С",
                    callback_data="copilot:confirm",
                )
            ],
            [
                InlineKeyboardButton(
                    text="✏️ Изменить позицию",
                    callback_data="copilot:edit_menu",
                ),
                InlineKeyboardButton(
                    text="❌ Отмена",
                    callback_data="copilot:cancel",
                ),
            ],
        ]
    )


def get_edit_selection_kb(items: list[MatchedItem]) -> InlineKeyboardMarkup:
    """Клавиатура выбора конкретной позиции для редактирования или удаления."""
    rows: list[list[InlineKeyboardButton]] = []
    for idx, item in enumerate(items):
        title = item.catalog_name or item.raw_name
        btn_text = f"#{idx + 1} {title[:26]} ({item.final_quantity:g} {item.base_unit})"
        rows.append(
            [InlineKeyboardButton(text=btn_text, callback_data=f"copilot:pick:{idx}")]
        )

    rows.append(
        [InlineKeyboardButton(text="⬅️ Назад к заказу", callback_data="copilot:back")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_item_edit_actions_kb(item_idx: int) -> InlineKeyboardMarkup:
    """Кнопки при редактировании выбранной позиции (удалить позицию или вернуться)."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🗑 Удалить эту позицию из заказа",
                    callback_data=f"copilot:delete:{item_idx}",
                )
            ],
            [
                InlineKeyboardButton(
                    text="⬅️ Отмена редактирования",
                    callback_data="copilot:back",
                )
            ],
        ]
    )