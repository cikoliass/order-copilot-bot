from pathlib import Path
from typing import Optional
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Telegram Bot
    BOT_TOKEN: str = Field(..., description="Токен Telegram-бота от @BotFather")

    # Groq API (OpenAI-совместимый клиент)
    GROQ_API_KEY: str = Field(..., description="API-ключ Groq")
    GROQ_BASE_URL: str = Field(
        default="https://api.groq.com/openai/v1",
        description="Base URL для Groq API",
    )
    LLM_MODEL: str = Field(
        default="openai/gpt-oss-20b",
        description="Модель в Groq для извлечения сущностей из заказа",
    )
    WHISPER_MODEL: str = Field(
        default="whisper-large-v3",
        description="Модель Whisper в Groq для распознавания голосовых",
    )

    # Хранилище и БД
    DATABASE_URL: str = Field(
        default=f"sqlite+aiosqlite:///{BASE_DIR / 'horeca_orders.db'}",
        description="Строка подключения SQLAlchemy 2.0",
    )
    REDIS_URL: Optional[str] = Field(default=None)

    # Интеграция с 1С
    USE_MOCK_1C: bool = Field(
        default=True,
        description="Использовать Mock-сервер вместо реальной 1С",
    )
    MOCK_1C_URL: str = Field(
        default="http://127.0.0.1:8080/api/v1/mock-1c/orders",
        description="Эндпоинт создания заказа в Mock 1C",
    )
    ONEC_URL: str = Field(
        default="http://localhost/horeca1c/hs/horeca/orders",
        description="Эндпоинт создания заказа в реальной 1С",
    )
    MOCK_1C_TIMEOUT: float = Field(default=10.0)

    # Каталог и параметры матчинга
    CATALOG_PATH: Path = Field(
        default=BASE_DIR / "app" / "data" / "catalog.json",
    )
    CONFIDENCE_THRESHOLD: float = Field(default=0.85, ge=0.0, le=1.0)


settings = Settings()