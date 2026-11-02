from openai import AsyncOpenAI
from app.core.config import settings

client = AsyncOpenAI(
    api_key=settings.GROQ_API_KEY,
    base_url=settings.GROQ_BASE_URL,
)


async def transcribe_voice(ogg_bytes: bytes, filename: str = "voice.ogg") -> str:
    """
    Отправляет байты голосового сообщения (.ogg) в Groq Whisper (whisper-large-v3)
    и возвращает расшифрованный текст заказа.
    """
    transcription = await client.audio.transcriptions.create(
        file=(filename, ogg_bytes, "audio/ogg"),
        model=settings.WHISPER_MODEL,  # "whisper-large-v3"
        language="ru",
        prompt=(
            "Заказ продуктов HoReCa: томаты розовые, черри, романо, айсберг, "
            "руккола, креметте, сливки Петмол 33%, пармалат, моцарелла, вырезка, "
            "рибай, куриное филе, лосось, коробка, мешок, упаковка, кг, шт."
        ),
        response_format="text",
    )
    return transcription.strip() if isinstance(transcription, str) else transcription.text.strip()