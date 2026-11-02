import json
from openai import AsyncOpenAI

from app.core.config import settings
from app.services.matcher import ExtractedOrder

client = AsyncOpenAI(
    api_key=settings.GROQ_API_KEY,
    base_url=settings.GROQ_BASE_URL,
)

SYSTEM_PROMPT = """
Ты — AI-ассистент оптового дистрибьютора продуктов для ресторанов (HoReCa).
Твоя задача — извлечь из неструктурированного сообщения шеф-повара список товарных позиций и дату доставки.

Правила извлечения:
1. raw_name: укажи наименование продукта в именительном падеже без количества и тары (например: "помидоры розовые", "сливки петмол 33%", "картофель мытый").
2. quantity: переведи числительные и дроби в число ("полторы коробки" -> 1.5, "пару ящиков" -> 2.0).
3. unit: приведи единицу измерения к стандартному виду: "коробка", "кг", "г", "шт", "л", "упаковка", "мешок", "ведро", "лоток". Если единица не указана явно, ставь "шт" для штучных товаров или "кг" для весовых.
4. note: запиши пожелания к конкретной позиции ("поспелее", "крупный калибр") или верни null.
5. delivery_date: если упомянута дата или день доставки ("на завтра", "в пятницу"), укажи её строкой, иначе null.
"""


async def extract_order_from_text(raw_text: str) -> ExtractedOrder:
    """Извлекает позиции заказа из сырого текста через openai/gpt-oss-20b (Groq API)."""
    response = await client.beta.chat.completions.parse(
        model=settings.LLM_MODEL,  # "openai/gpt-oss-20b"
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": raw_text},
        ],
        response_format=ExtractedOrder,
        reasoning_effort="low",
        temperature=0.1,
    )

    parsed = response.choices[0].message.parsed
    if not parsed:
        return ExtractedOrder(items=[], delivery_date=None)

    return parsed