from aiogram.fsm.state import State, StatesGroup


class OrderFSM(StatesGroup):
    reviewing = State()          # Просмотр сводки заказа и ожидание подтверждения
    selecting_item = State()     # Выбор номера позиции для изменения
    editing_item = State()       # Ввод нового названия/количества для выбранной позиции