from pathlib import Path
from typing import Optional
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent.parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Core System
    APP_ENV: str = Field(default="production", description="Окружение приложения (production/development)")
    APP_PORT: int = Field(default=8001, description="Порт входящего шлюза FastAPI")
    APP_HOST: str = Field(default="127.0.0.1", description="Хост входящего шлюза FastAPI")
    WEBHOOK_SECRET: str = Field(default="", description="Секретный ключ вебхуков (ОБЯЗАТЕЛЕН в production)")
    ADMIN_USER_IDS: list[str] = Field(
        default_factory=list,
        description="Белый список ID администраторов для сервисных команд (/sync). Формат: platform:user_id",
    )
    REQUIRE_CLIENT_AUTH: bool = Field(
        default=False,
        description="Требовать ли обязательную привязку аккаунта мессенджера к B2B-контрагенту перед заказом",
    )
    MAX_TEXT_LENGTH: int = Field(
        default=3000,
        description="Максимальная длина текста входящего заказа (защита от DoS и слива LLM-токенов)",
    )
    RATE_LIMIT_PER_MINUTE: int = Field(
        default=30,
        description="Максимальное количество входящих сообщений в минуту на одного пользователя",
    )
    ALLOWED_ORIGINS: list[str] = Field(
        default_factory=list,
        description="Список разрешенных CORS origins (пустой список = запретить все cross-origin запросы)",
    )
    MAX_REQUEST_BODY_SIZE_MB: int = Field(
        default=25,
        description="Максимальный размер тела входящего HTTP-запроса в мегабайтах (защита от OOM/DoS)",
    )

    # Yandex Cloud (152-ФЗ)
    YANDEX_API_KEY: str = Field(default="", description="API-ключ сервисного аккаунта Yandex Cloud")
    YANDEX_FOLDER_ID: str = Field(default="", description="Идентификатор каталога в Yandex Cloud")
    YANDEX_STT_URL: str = Field(
        default="https://stt.api.cloud.yandex.net/speech/v1/stt:recognize",
        description="URL Yandex SpeechKit v1 STT",
    )
    YANDEX_GPT_URL: str = Field(
        default="https://llm.api.cloud.yandex.net/foundationModels/v1/completion",
        description="URL Yandex Foundation Models Completion",
    )
    YANDEX_GPT_MODEL: str = Field(
        default="yandexgpt/latest",
        description="Модель YandexGPT: yandexgpt/latest или yandexgpt-lite/latest",
    )

    # State Storage & Database
    REDIS_URL: str = Field(
        default="redis://127.0.0.1:6379/0",
        description="URL подключения к Redis",
    )
    DATABASE_URL: str = Field(
        default=f"sqlite+aiosqlite:///{BASE_DIR / 'orders_enterprise.db'}",
        description="URL подключения к БД (SQLite/PostgreSQL)",
    )

    # Telegram
    TELEGRAM_BOT_TOKEN: str = Field(
        default="",
        validation_alias=AliasChoices("TELEGRAM_BOT_TOKEN", "BOT_TOKEN"),
        description="Токен Telegram Bot API",
    )
    TELEGRAM_WEBHOOK_URL: str = Field(default="", description="Публичный URL вебхука Telegram")
    TELEGRAM_PROXY: Optional[str] = Field(
        default=None,
        description="HTTP/SOCKS5 прокси для обращений к Telegram Bot API (например: http://127.0.0.1:17890)",
    )

    # VK Callback API
    VK_GROUP_TOKEN: str = Field(default="", description="Токен группы ВКонтакте")
    VK_CONFIRMATION_CODE: str = Field(default="", description="Код подтверждения Callback API ВКонтакте")
    VK_SECRET_KEY: str = Field(default="", description="Секретный ключ Callback API ВКонтакте")

    # WhatsApp (Green-API)
    GREEN_API_INSTANCE_ID: str = Field(default="", description="Instance ID в Green-API")
    GREEN_API_TOKEN: str = Field(default="", description="API токен Green-API")

    # Viber
    VIBER_AUTH_TOKEN: str = Field(default="", description="Токен чат-бота Viber")
    VIBER_SENDER_NAME: str = Field(default="HoReCa Order Co-Pilot", description="Имя отправителя в Viber")

    # MAX Corporate Messenger
    MAX_BOT_TOKEN: str = Field(default="", description="Токен корпоративного бота MAX")
    MAX_API_BASE_URL: str = Field(default="https://platform-api2.max.ru", description="Базовый URL API MAX")

    # 1C Integration
    USE_MOCK_1C: bool = Field(default=True, description="Использовать Mock 1C вместо боевого сервера")
    MOCK_1C_URL: str = Field(
        default="http://127.0.0.1:8080/api/v1/mock-1c/orders",
        description="Эндпоинт Mock 1C",
    )
    ONEC_URL: str = Field(
        default="http://127.0.0.1:8080/api/v1/mock-1c/orders",
        description="Эндпоинт создания заказа в 1С (боевой или Mock)",
    )
    ONEC_USER: Optional[str] = Field(default=None, description="Пользователь HTTP-сервиса 1С")
    ONEC_PASSWORD: Optional[str] = Field(default=None, description="Пароль HTTP-сервиса 1С")
    ONEC_TIMEOUT_SEC: float = Field(default=5.0, description="Таймаут обращения к 1С в секундах")

    # Bitrix24 Integration
    BITRIX_WEBHOOK_URL: Optional[str] = Field(
        default=None,
        description="Входящий вебхук REST API Битрикс24",
    )
    BITRIX_TIMEOUT_SEC: float = Field(default=5.0, description="Таймаут обращения к Битрикс24 в секундах")

    # Matching & Logic
    CONFIDENCE_THRESHOLD: float = Field(
        default=0.85,
        ge=0.0,
        le=1.0,
        description="Порог уверенности сопоставления номенклатуры (🟢 / 🟡)",
    )
    LOCK_TIMEOUT_SEC: int = Field(default=60, description="Время жизни распределенного лока (TTL)")
    OUTBOX_POLL_INTERVAL_SEC: int = Field(
        default=60,
        description="Интервал проверки Outbox-очереди в секундах",
    )
    CHECK_PACK_MULTIPLES: bool = Field(
        default=True,
        description="Контролировать кратность заводских упаковок (ящиков/коробок)",
    )
    SUGGEST_OUT_OF_STOCK_ANALOGS: bool = Field(
        default=True,
        description="Автоматически предлагать аналоги при нулевом остатке товара на складе",
    )
    ENABLE_POLLING_BRIDGES: bool = Field(
        default=False,
        description="Включить фоновые long-polling мосты (для локальной разработки без вебхуков)",
    )


settings = Settings()
