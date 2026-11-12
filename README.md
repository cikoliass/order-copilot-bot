# B2B HoReCa Order Co-Pilot Enterprise

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-009688.svg)](https://fastapi.tiangolo.com)
[![152-ФЗ Yandex Cloud](https://img.shields.io/badge/Yandex_Cloud-152--ФЗ-red.svg)](https://cloud.yandex.ru)
[![Redis](https://img.shields.io/badge/Redis-Distributed_Lock-dc382d.svg)](https://redis.io)

Мультиплатформенный омниканальный шлюз для автоматизации B2B-закупок ресторанов, кафе и отелей (HoReCa). Позволяет шеф-поварам и закупщикам надиктовывать или писать заказы в свободной форме в любом привычном мессенджере с автоматической выгрузкой в 1С и CRM Битрикс24.

---

## 🚀 Архитектурные особенности

1. **Суверенный стек Yandex Cloud (152-ФЗ РФ):**
   - **ASR (Аудио):** Yandex SpeechKit REST API v1 (`oggopus`, `mp3`, `lpcm`), ограничение размера аудио и таймаут ffmpeg.
   - **LLM (Парсер):** Yandex Cloud Foundation Models (YandexGPT Pro / Lite) с резервным эвристическим парсером при отсутствии сети.
2. **Всеядная омниканальность (5 каналов через вебхуки с авторизацией):**
   - **Telegram:** `POST /webhook/telegram` (`X-Telegram-Bot-Api-Secret-Token`)
   - **ВКонтакте:** `POST /webhook/vk` (Callback API, валидация secret и confirmation)
   - **WhatsApp:** `POST /webhook/whatsapp` (Green-API / Cloud API, проверка secret)
   - **Viber:** `POST /webhook/viber` (HMAC SHA-256 сигнатура)
   - **MAX:** `POST /webhook/max` (Корпоративный мессенджер)
   - Изоляция сессий и последовательная обработка: `session_id = f"{platform}:{platform_user_id}"`.
3. **Защита от сбоев и Concurrency (Redis Distributed Lock):**
   - Блокировка повторных нажатий с безопасным снятием своего лока по токену владения: `redis.set(f"lock:order:{session_id}", token, nx=True, ex=60)`.
   - Сессионная очередь сообщений исключает гонки при одновременных апдейтах от одного повара.
   - Черновики изолированы в Redis по ключу `order:draft:{session_id}`.
4. **Неблокирующий Event Loop (Zero Lag):**
   - Потокобезопасный нечеткий поиск RapidFuzz вынесен из event loop в системный пул потоков через `asyncio.to_thread()`.
5. **Очередь повторной отправки (Outbox Pattern) и Идемпотентность:**
   - Гарантированная доставка в 1С и Битрикс24 при сбоях сети с экспоненциальным backoff.
   - Ключ идемпотентности (`idempotency_key`) предотвращает появление дублей в 1С при таймаутах.
   - Сохранение прогресса (`completed_docs`) исключает повторное создание документов.
   - Автоматическое уведомление клиента после финальной выгрузки из Outbox.
6. **Smart UX и предиктивный модуль:**
   - **Пересчет тары:** честный перевод физических величин (г ↔ кг, мл ↔ л) и коэффициентов из карточек Битрикс24.
   - **Блокировка некорректных заказов:** подтверждение блокируется при наличии товаров со стоп-листа или с нераспознанной тарой.
   - **Статусы уверенности:** `🟢 РАСПОЗНАНО` (conf >= 0.85) и `🟡 ТРЕБУЕТ ПРОВЕРКИ` (conf < 0.85).
   - **«Забытые товары»:** подсказка при оформлении заказа на основе частотного анализа прошлых закупок (`min_frequency >= 0.4`).
   - **«Идеальный заказ»:** мгновенная генерация корзины на основе частоты и медианных объемов закупок клиента.

---

## 📂 Структура проекта

```text
order-copilot-bot/
├── app/
│   ├── __init__.py
│   ├── main.py                  # Точка входа FastAPI, Lifespan, запуск воркера Outbox
│   ├── core/
│   │   ├── __init__.py
│   │   ├── config.py            # Pydantic Settings с полной валидацией .env
│   │   ├── redis_client.py      # Пул Redis, хелперы сессий и распределенных локов
│   │   └── database.py          # SQLAlchemy 2.0 Async, модели Orders и OrderOutbox
│   ├── api/
│   │   ├── __init__.py
│   │   └── gateway.py           # Роуты вебхуков для TG, VK, WhatsApp, Viber, MAX
│   └── services/
│       ├── __init__.py
│       ├── schemas.py           # DTO: InboundMessage, OutboundMessage, ExtractedOrder, MatchedItem
│       ├── asr_yandex.py        # Голос в текст через Yandex SpeechKit
│       ├── parser_yandex.py     # Парсер текста в JSON через YandexGPT API и эвристики
│       ├── matcher.py           # RapidFuzz в asyncio.to_thread, пересчет тары, скоринг
│       ├── predictor.py         # Частотно-объемный скоринг «Идеального заказа»
│       ├── integrations.py      # Клиенты 1С и Битрикс24 с Outbox-фоллбэком и идемпотентностью
│       ├── bitrix_sync.py       # Live-синхронизация товарного каталога CRM
│       └── dispatcher.py        # Унифицированная отправка сообщений и кнопок в любой мессенджер
├── mock_server/
│   ├── __init__.py
│   └── main.py                  # Эмулятор 1С на FastAPI (POST /orders, HTML-витрина)
├── tests/                       # Автотесты (pytest)
├── docker-compose.yml
├── Dockerfile
├── .dockerignore
├── .env.example
├── requirements.txt
└── README.md
```

---

## 🛠 Быстрый запуск

### 1. Установка зависимостей
```bash
python -m venv venv
venv\Scripts\activate   # Windows
# source venv/bin/activate # Linux / macOS

pip install -r requirements.txt
```

### 2. Настройка переменных окружения
Скопируйте пример конфига и заполните параметры:
```bash
cp .env.example .env
```

### 3. Запуск эмулятора 1С (порт 8080)
```bash
python -m mock_server.main
```
Витрина для жюри с автообновлением: [http://localhost:8080/orders](http://localhost:8080/orders)

### 4. Запуск основного бэкенда (порт 8001)
```bash
python -m app.main
```
Swagger UI документация: [http://localhost:8001/docs](http://localhost:8001/docs)
Проверка работоспособности: [http://localhost:8001/health](http://localhost:8001/health)

---

## 🌐 Настройка вебхуков

- **Telegram:** `https://your-domain.ru/webhook/telegram`
- **VKontakte:** Настройки сообщества -> Работа с API -> Callback API -> URL: `https://your-domain.ru/webhook/vk`
- **WhatsApp (Green-API):** Личный кабинет инстанса -> URL вебхука: `https://your-domain.ru/webhook/whatsapp`
- **Viber:** Отправка POST в `https://chatapi.viber.com/pa/set_webhook` с url `https://your-domain.ru/webhook/viber`
- **MAX:** Админка бота -> Webhook URL: `https://your-domain.ru/webhook/max`
