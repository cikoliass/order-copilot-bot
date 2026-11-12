FROM python:3.11-slim

WORKDIR /app

# Установка системных зависимостей (ffmpeg для SpeechKit)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Создание непривилегированного пользователя appuser для безопасной работы без root
RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

# Установка Python-зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода (.dockerignore отсекает .env, .git, venv и базы)
COPY . .

RUN chown -R appuser:appuser /app
USER appuser

EXPOSE 8001 8080

CMD ["python", "-m", "app.main"]
