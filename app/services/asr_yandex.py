import asyncio
import logging
import subprocess
from typing import Optional
import httpx
from app.core.config import settings

logger = logging.getLogger(__name__)

MAX_AUDIO_BYTES = 20 * 1024 * 1024  # 20 МБ лимит для защиты от OOM и DoS


def detect_audio_format(audio_bytes: bytes) -> str:
    """Определение формата аудиофайла по сигнатурам байт (magic bytes)."""
    if audio_bytes.startswith(b"OggS"):
        return "oggopus"
    if audio_bytes.startswith(b"ID3") or (len(audio_bytes) > 2 and audio_bytes[0] == 0xFF and (audio_bytes[1] & 0xE0) == 0xE0):
        return "mp3"
    if audio_bytes.startswith(b"RIFF") and len(audio_bytes) > 12 and audio_bytes[8:12] == b"WAVE":
        return "lpcm"
    return "unknown"


def _convert_audio_with_ffmpeg(audio_bytes: bytes) -> tuple[bytes, str]:
    """
    Синхронная конвертация любого аудио/видео (включая video_note из Telegram)
    в потоковый формат OGG Opus для Yandex SpeechKit.
    Флаг -vn отсекает видеопоток, извлекая только чистый звук.
    Таймаут 15 секунд предотвращает зависание процесса.
    """
    detected = detect_audio_format(audio_bytes)
    proc = None
    try:
        proc = subprocess.Popen(
            [
                "ffmpeg", "-y",
                "-nostdin",
                "-protocol_whitelist", "pipe",
                "-i", "pipe:0",
                "-vn",
                "-c:a", "libopus",
                "-b:a", "32k",
                "-f", "ogg",
                "pipe:1",
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        converted, err = proc.communicate(input=audio_bytes, timeout=15.0)
        if proc.returncode == 0 and converted:
            logger.info("ffmpeg: успешно извлечен и сконвертирован звук (%d байт).", len(converted))
            return converted, "oggopus"
        logger.warning("ffmpeg конвертация вернула код %s: %s", proc.returncode, err[:200] if err else "")
    except subprocess.TimeoutExpired:
        if proc:
            proc.kill()
        logger.error("Таймаут ffmpeg при конвертации аудио (более 15 сек).")
    except Exception as conv_err:
        logger.warning("Ошибка вызова ffmpeg (%s). Используем исходный формат: %s", conv_err, detected)
    return audio_bytes, (detected if detected != "unknown" else "oggopus")


async def transcribe_voice_yandex(
    audio_bytes: bytes,
    audio_format: str = "oggopus",
    folder_id: Optional[str] = None,
) -> str:
    """
    Асинхронное распознавание речи через Yandex SpeechKit API (v1/stt:recognize).
    Соответствует 152-ФЗ РФ.
    Поддерживает аудио OGG/Opus, MP3, WAV (lpcm) и видео-кружочки (video_note mp4).
    """
    if not audio_bytes:
        logger.warning("Переданы пустые байты аудио для распознавания SpeechKit.")
        return ""

    if len(audio_bytes) > MAX_AUDIO_BYTES:
        logger.warning("Размер аудио (%d байт) превышает допустимый лимит %d байт.", len(audio_bytes), MAX_AUDIO_BYTES)
        raise ValueError("Аудиофайл слишком большой. Максимальный размер 20 МБ.")

    api_key = settings.YANDEX_API_KEY
    target_folder_id = folder_id or settings.YANDEX_FOLDER_ID

    if settings.APP_ENV == "test" or not api_key:
        logger.warning("YANDEX_API_KEY не задан или тестовый режим. Распознавание речи недоступно.")
        return ""

    # Автоопределение формата: если аудио уже в поддерживаемом формате (OGG Opus или MP3),
    # отправляем напрямую в SpeechKit без накладных расходов на ffmpeg
    detected = detect_audio_format(audio_bytes)
    if detected in ("oggopus", "mp3"):
        audio_format = detected
    else:
        # Для видео-кружочков (mp4), webm, m4a конвертируем в OGG Opus
        audio_bytes, audio_format = await asyncio.to_thread(_convert_audio_with_ffmpeg, audio_bytes)

    headers = {
        "Authorization": f"Api-Key {api_key}",
    }

    params = {
        "topic": "general",
        "lang": "ru-RU",
        "format": audio_format,
    }
    if target_folder_id:
        params["folderId"] = target_folder_id

    fmt_lower = audio_format.lower()
    if "mp3" in fmt_lower:
        params["format"] = "mp3"
    elif "wav" in fmt_lower or "pcm" in fmt_lower:
        params["format"] = "lpcm"
        params["sampleRateHertz"] = 16000
    else:
        params["format"] = "oggopus"

    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            response = await client.post(
                settings.YANDEX_STT_URL,
                params=params,
                headers=headers,
                content=audio_bytes,
            )
            if response.status_code != 200:
                logger.error(
                    "SpeechKit вернул ошибку HTTP %d: %s",
                    response.status_code,
                    response.text,
                )
                return ""

            result_json = response.json()
            return result_json.get("result", "").strip()
        except Exception as exc:
            logger.error("Сбой запроса к Yandex SpeechKit STT: %s", exc)
            return ""
