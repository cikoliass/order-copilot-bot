import asyncio
import io
import json
import logging
from contextlib import asynccontextmanager
from typing import Any
import httpx
from fastapi import FastAPI, HTTPException, status, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from app.api.gateway import _is_duplicate_update_async, process_inbound_core, router as gateway_router
from app.core.config import settings
from app.core.database import engine, get_pending_outbox_entries, init_db
from app.core.redis_client import redis_client
from app.services.bitrix_sync import sync_catalog_from_bitrix
from app.services.integrations import run_outbox_worker
from app.services.matcher import catalog_matcher
from app.services.schemas import InboundMessage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger("order_copilot")

_outbox_task: Any = None
_tg_task: Any = None
_vk_task: Any = None
_wa_task: Any = None
_max_task: Any = None


async def _run_vk_polling_bridge(token: str) -> None:
    """Фоновый мост Long-Polling для ВКонтакте при локальной разработке."""
    try:
        async with httpx.AsyncClient(timeout=35.0) as client:
            res = await client.post("https://api.vk.com/method/groups.getById", data={"access_token": token, "v": "5.199"})
            g_data = res.json()
            groups = g_data.get("response", {}).get("groups", [])
            if not groups or "error" in g_data:
                logger.error("Не удалось получить данные группы VK: %s", g_data.get("error"))
                return

            group_id, group_name = groups[0]["id"], groups[0]["name"]
            await client.post(
                "https://api.vk.com/method/groups.setLongPollSettings",
                data={"group_id": group_id, "enabled": 1, "message_new": 1, "message_event": 1, "access_token": token, "v": "5.199"},
            )

            async def _get_lp_server():
                r = await client.post("https://api.vk.com/method/groups.getLongPollServer", data={"group_id": group_id, "access_token": token, "v": "5.199"})
                d = r.json().get("response", {})
                return d.get("server"), d.get("key"), d.get("ts")

            server, key, ts = await _get_lp_server()
            if not server:
                logger.error("Не удалось получить VK LongPoll сервер")
                return

            logger.info("📡 Запуск VK Long-Polling для сообщества «%s» (id=%s)...", group_name, group_id)
            while True:
                try:
                    resp = await client.get(f"{server}?act=a_check&key={key}&ts={ts}&wait=25")
                    data = resp.json()
                    if "failed" in data:
                        if data["failed"] == 1:
                            ts = data["ts"]
                        else:
                            server, key, ts = await _get_lp_server()
                        continue

                    ts = data.get("ts", ts)
                    for update in data.get("updates", []):
                        ev_type, obj = update.get("type"), update.get("object", {})
                        if ev_type == "message_new":
                            msg = obj.get("message", {})
                            msg_id = msg.get("id") or msg.get("conversation_message_id")
                            if msg_id and await _is_duplicate_update_async(f"vk:{msg_id}"):
                                continue
                            uid = str(msg.get("from_id", ""))
                            voice_url = next(
                                (att.get(att.get("type"), {}).get("link_ogg") or att.get(att.get("type"), {}).get("url")
                                 for att in msg.get("attachments", []) if att.get("type") in ("audio_message", "doc")),
                                None,
                            )
                            asyncio.create_task(process_inbound_core(InboundMessage(
                                platform="vk", platform_user_id=uid, chat_id=str(msg.get("peer_id", uid)), text=msg.get("text", ""), voice_url=voice_url
                            )))
                        elif ev_type == "message_event":
                            uid = str(obj.get("user_id", ""))
                            peer_id = str(obj.get("peer_id", uid))
                            ev_id = obj.get("event_id")
                            if ev_id and await _is_duplicate_update_async(f"vk_ev:{uid}:{ev_id}"):
                                continue
                            if ev_id:
                                try:
                                    await client.post(
                                        "https://api.vk.com/method/messages.sendMessageEventAnswer",
                                        data={"event_id": ev_id, "user_id": int(uid), "peer_id": int(peer_id), "access_token": token, "v": "5.199"},
                                    )
                                except Exception:
                                    pass
                            raw_p = obj.get("payload", {})
                            action = None
                            if isinstance(raw_p, str):
                                try:
                                    parsed_p = json.loads(raw_p)
                                    action = parsed_p.get("action") if isinstance(parsed_p, dict) else str(parsed_p)
                                except Exception:
                                    action = raw_p
                            elif isinstance(raw_p, dict):
                                action = raw_p.get("action")
                            asyncio.create_task(process_inbound_core(InboundMessage(
                                platform="vk", platform_user_id=uid, chat_id=peer_id, callback_data=action
                            )))
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("Ошибка цикла VK Long-Polling: %s", exc)
                    await asyncio.sleep(2)
    except asyncio.CancelledError:
        logger.info("VK Long-Polling остановлен.")
    except Exception as exc:
        logger.error("Критический сбой VK Long-Polling: %s", exc)


async def _run_telegram_polling_bridge(token: str) -> None:
    """Фоновый мост Long-Polling для Telegram при локальной разработке."""
    try:
        from aiogram import Dispatcher
        from aiogram.types import CallbackQuery, Message
        from app.services.dispatcher import create_telegram_bot, dispatcher

        bot = create_telegram_bot(token)
        dispatcher.set_telegram_bot(bot)
        dp = Dispatcher()

        @dp.message()
        async def on_message(message: Message):
            voice_bytes = None
            if media := (message.voice or message.audio or message.video_note):
                try:
                    await bot.send_chat_action(chat_id=message.chat.id, action="record_voice")
                    file_info = await bot.get_file(media.file_id)
                    buf = io.BytesIO()
                    await bot.download_file(file_info.file_path, destination=buf)
                    voice_bytes = buf.getvalue()
                except Exception as exc:
                    logger.warning("Ошибка скачивания голосового сообщения Telegram: %s", exc)
            else:
                try:
                    await bot.send_chat_action(chat_id=message.chat.id, action="typing")
                except Exception:
                    pass

            inbound = InboundMessage(
                platform="telegram",
                platform_user_id=str(message.from_user.id),
                chat_id=str(message.chat.id),
                text=message.text or message.caption,
                voice_bytes=voice_bytes,
                message_id=str(message.message_id),
            )
            try:
                await process_inbound_core(inbound)
            except Exception as exc:
                logger.exception("Ошибка обработки входящего сообщения: %s", exc)
                try:
                    await message.answer("⚠️ Ошибка при обработке заказа. Попробуйте еще раз.")
                except Exception:
                    pass

        @dp.callback_query()
        async def on_callback(callback: CallbackQuery):
            chat_id = str(callback.message.chat.id) if callback.message else str(callback.from_user.id)
            inbound = InboundMessage(
                platform="telegram",
                platform_user_id=str(callback.from_user.id),
                chat_id=chat_id,
                callback_data=callback.data,
            )
            try:
                await process_inbound_core(inbound)
            except Exception as exc:
                logger.exception("Ошибка обработки callback: %s", exc)
            try:
                await callback.answer()
            except Exception:
                pass

        logger.info("📡 Запуск Telegram Long-Polling (локальный режим без вебхука)...")
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    except asyncio.CancelledError:
        logger.info("Telegram Long-Polling остановлен.")
    except Exception as exc:
        logger.error("Ошибка Telegram Long-Polling: %s", exc)


async def _run_whatsapp_polling_bridge(instance_id: str, token: str) -> None:
    """Фоновый мост Long-Polling для WhatsApp (Green-API) при локальной разработке."""
    try:
        logger.info("📡 Запуск WhatsApp Long-Polling (Green-API локальный режим без вебхука)...")
        host = f"https://{instance_id[:4]}.api.greenapi.com" if len(instance_id) >= 4 else "https://api.green-api.com"
        url_recv = f"{host}/waInstance{instance_id}/receiveNotification/{token}"
        url_del = f"{host}/waInstance{instance_id}/deleteNotification/{token}"

        async with httpx.AsyncClient(timeout=35.0) as client:
            while True:
                try:
                    resp = await client.get(url_recv, params={"receiveTimeout": 10})
                    if resp.status_code == 200 and resp.text.strip():
                        item = resp.json()
                        if isinstance(item, dict) and "receiptId" in item:
                            receipt_id = item["receiptId"]
                            data = item.get("body", {})
                            msg_id = data.get("idMessage") or receipt_id
                            if msg_id and await _is_duplicate_update_async(f"wa:{msg_id}"):
                                try:
                                    await client.delete(f"{url_del}/{receipt_id}")
                                except Exception:
                                    pass
                                await asyncio.sleep(0.1)
                                continue

                            if data.get("typeWebhook") == "incomingMessageReceived":
                                msg_data = data.get("messageData", {})
                                sender_data = data.get("senderData", {})
                                sender_id = sender_data.get("sender", "").replace("@c.us", "")
                                chat_id = sender_data.get("chatId", sender_id)
                                type_msg = msg_data.get("typeMessage")

                                text, voice_url, callback_data = None, None, None
                                if type_msg == "textMessage":
                                    text = msg_data.get("textMessageData", {}).get("textMessage")
                                elif type_msg == "extendedTextMessage":
                                    text = msg_data.get("extendedTextMessageData", {}).get("text")
                                elif type_msg in ("audioMessage", "voiceMessage"):
                                    voice_url = msg_data.get("fileMessageData", {}).get("downloadUrl")
                                elif type_msg in ("buttonsResponseMessage", "templateButtonReplyMessage", "interactiveButtonsResponse"):
                                    btn = (
                                        msg_data.get("interactiveButtonsResponse")
                                        or msg_data.get("buttonsResponseMessage")
                                        or msg_data.get("buttonsResponseMessageData")
                                        or msg_data.get("templateButtonReplyMessage")
                                        or {}
                                    )
                                    callback_data = btn.get("selectedId") or btn.get("selectedButtonId")
                                    text = btn.get("selectedDisplayText") or btn.get("selectedButtonText") or callback_data

                                if sender_id and (text or voice_url or callback_data):
                                    asyncio.create_task(process_inbound_core(InboundMessage(
                                        platform="whatsapp", platform_user_id=sender_id, chat_id=chat_id, text=text, voice_url=voice_url, callback_data=callback_data
                                    )))

                            try:
                                await client.delete(f"{url_del}/{receipt_id}")
                            except Exception as del_exc:
                                logger.warning("Ошибка удаления уведомления WhatsApp %s: %s", receipt_id, del_exc)

                            await asyncio.sleep(0.2)
                            continue
                    elif resp.status_code != 200:
                        logger.warning("WhatsApp receiveNotification вернул HTTP %d: %s", resp.status_code, resp.text[:200])
                        await asyncio.sleep(3.0)

                    await asyncio.sleep(2.0)
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("Ошибка цикла WhatsApp Long-Polling: %s", exc)
                    await asyncio.sleep(2)
    except asyncio.CancelledError:
        logger.info("WhatsApp Long-Polling остановлен.")
    except Exception as exc:
        logger.error("Критический сбой WhatsApp Long-Polling: %s", exc)


async def _run_max_polling_bridge(token: str) -> None:
    """Фоновый мост Long-Polling для корпоративного мессенджера MAX (platform-api2.max.ru)."""
    try:
        logger.info("📡 Запуск MAX Long-Polling (platform-api2.max.ru локальный режим)...")
        base_url = "https://platform-api2.max.ru"
        headers = {"Authorization": token}
        marker: int | None = None

        async with httpx.AsyncClient(timeout=35.0) as client:
            while True:
                try:
                    params: dict[str, Any] = {"timeout": 25, "limit": 100}
                    if marker is not None:
                        params["marker"] = marker

                    resp = await client.get(f"{base_url}/updates", headers=headers, params=params)
                    if resp.status_code == 200:
                        data = resp.json()
                        marker = data.get("marker", marker)
                        for upd in data.get("updates", []):
                            update_key = (
                                upd.get("update_id")
                                or upd.get("callback", {}).get("callback_id")
                                or upd.get("message", {}).get("body", {}).get("message_id")
                                or upd.get("message", {}).get("message_id")
                            )
                            if update_key and await _is_duplicate_update_async(f"max:{update_key}"):
                                continue

                            u_type = upd.get("update_type")
                            if u_type == "bot_started":
                                uid = str(upd.get("user", {}).get("user_id") or upd.get("chat_id") or "")
                                if uid:
                                    asyncio.create_task(process_inbound_core(InboundMessage(platform="max", platform_user_id=uid, chat_id=uid, text="/start")))
                            elif u_type == "message_created":
                                msg = upd.get("message", {})
                                body, sender = msg.get("body", {}), msg.get("sender", {})
                                uid = str(sender.get("user_id", ""))
                                chat_id = str(msg.get("recipient", {}).get("chat_id") or uid)
                                voice_url = next(
                                    (att.get("payload", {}).get("url") if isinstance(att.get("payload"), dict) else att.get("url")
                                     for att in body.get("attachments", []) if att.get("type") in ("audio", "voice", "file")),
                                    None,
                                )
                                if uid and (body.get("text") or voice_url):
                                    asyncio.create_task(process_inbound_core(InboundMessage(platform="max", platform_user_id=uid, chat_id=chat_id, text=body.get("text"), voice_url=voice_url)))
                            elif u_type == "message_callback":
                                cb = upd.get("callback", {})
                                uid = str(cb.get("user", {}).get("user_id") or cb.get("user_id") or "")
                                chat_id = str(cb.get("message", {}).get("recipient", {}).get("chat_id") or uid)
                                if cb_id := cb.get("callback_id"):
                                    try:
                                        await client.post(f"{base_url}/answers?callback_id={cb_id}", headers=headers, json={"notification": "Обрабатываю..."})
                                    except Exception:
                                        pass
                                if uid and (payload := cb.get("payload")):
                                    asyncio.create_task(process_inbound_core(InboundMessage(platform="max", platform_user_id=uid, chat_id=chat_id, callback_data=str(payload))))
                    else:
                        logger.warning("MAX polling /updates вернул HTTP %d: %s", resp.status_code, resp.text[:200])
                        await asyncio.sleep(3.0)

                    await asyncio.sleep(0.5)
                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.warning("Ошибка цикла MAX Long-Polling: %s", exc)
                    await asyncio.sleep(2)
    except asyncio.CancelledError:
        logger.info("MAX Long-Polling остановлен.")
    except Exception as exc:
        logger.error("Критический сбой MAX Long-Polling: %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _outbox_task, _tg_task, _vk_task, _wa_task, _max_task
    logger.info("🚀 Инициализация ядра B2B HoReCa Order Co-Pilot (152-ФЗ)...")

    try:
        await init_db()
        logger.info("✅ База данных успешно инициализирована.")
    except Exception as exc:
        logger.exception("❌ Критический сбой инициализации базы данных: %s", exc)
        raise

    try:
        await redis_client.connect()
        await catalog_matcher.restore_state_from_redis()
    except Exception as exc:
        logger.warning("⚠️ Сбой при подключении к Redis: %s", exc)

    if settings.BITRIX_WEBHOOK_URL:
        try:
            items = await sync_catalog_from_bitrix()
            if items:
                catalog_matcher.reload(items)
                logger.info("✅ Загружено %d товаров напрямую из API Битрикс24 (хранение в RAM).", len(items))
        except Exception as exc:
            logger.warning("⚠️ Не удалось получить каталог Битрикс24 при старте: %s", exc)
    else:
        logger.warning("⚠️ BITRIX_WEBHOOK_URL не задан! Каталог товаров будет пустым.")

    _outbox_task = asyncio.create_task(run_outbox_worker())

    if settings.TELEGRAM_BOT_TOKEN:
        if settings.TELEGRAM_WEBHOOK_URL:
            try:
                wh_payload: dict[str, Any] = {"url": settings.TELEGRAM_WEBHOOK_URL}
                if settings.WEBHOOK_SECRET:
                    wh_payload["secret_token"] = settings.WEBHOOK_SECRET
                async with httpx.AsyncClient(proxy=settings.TELEGRAM_PROXY, timeout=10.0) as client:
                    res = await client.post(
                        f"https://api.telegram.org/bot{settings.TELEGRAM_BOT_TOKEN}/setWebhook",
                        json=wh_payload,
                    )
                    res_data = res.json()
                    logger.info("Telegram Webhook установлен: ok=%s, description=%s", res_data.get("ok"), res_data.get("description", ""))
            except Exception as exc:
                safe_err = str(exc)
                if settings.TELEGRAM_BOT_TOKEN:
                    safe_err = safe_err.replace(settings.TELEGRAM_BOT_TOKEN, "***BOT_TOKEN***")
                if settings.WEBHOOK_SECRET:
                    safe_err = safe_err.replace(settings.WEBHOOK_SECRET, "***WEBHOOK_SECRET***")
                logger.warning("Не удалось автоматически установить Telegram Webhook: %s", safe_err)
        elif settings.ENABLE_POLLING_BRIDGES:
            _tg_task = asyncio.create_task(_run_telegram_polling_bridge(settings.TELEGRAM_BOT_TOKEN))

    if settings.ENABLE_POLLING_BRIDGES:
        logger.info("📡 Режим Long-Polling ВКЛЮЧЕН (локальная разработка без вебхуков).")
        if settings.VK_GROUP_TOKEN:
            _vk_task = asyncio.create_task(_run_vk_polling_bridge(settings.VK_GROUP_TOKEN))

        if settings.GREEN_API_INSTANCE_ID and settings.GREEN_API_TOKEN:
            _wa_task = asyncio.create_task(_run_whatsapp_polling_bridge(settings.GREEN_API_INSTANCE_ID, settings.GREEN_API_TOKEN))

        if settings.MAX_BOT_TOKEN:
            _max_task = asyncio.create_task(_run_max_polling_bridge(settings.MAX_BOT_TOKEN))
    else:
        logger.info("🔒 Режим Вебхуков: Long-polling мосты отключены во избежание дублирования сообщений.")

    logger.info("⚡ Бэкенд запущен и готов к работе со всеми каналами: TG, VK, WhatsApp, Viber, MAX.")
    yield

    logger.info("🛑 Завершение работы бэкенда Order Co-Pilot...")
    tasks = [t for t in (_outbox_task, _tg_task, _vk_task, _wa_task, _max_task) if t]
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    await redis_client.close()
    await engine.dispose()
    try:
        from app.services.dispatcher import close_dispatcher_sessions
        await close_dispatcher_sessions()
    except Exception:
        pass
    logger.info("💤 Все соединения безопасно закрыты.")


app = FastAPI(
    title="B2B HoReCa Order Co-Pilot Enterprise",
    description="Мультиплатформенный шлюз приема заказов HoReCa (TG, VK, WhatsApp, Viber, MAX) на суверенном стеке Yandex Cloud (152-ФЗ)",
    version="2.0.0",
    lifespan=lifespan,
)

# Безопасная настройка CORS: исключаем одновременное использование allow_origins=["*"] и allow_credentials=True
cors_origins = settings.ALLOWED_ORIGINS
allow_wildcard = "*" in cors_origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=not allow_wildcard,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)


@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    """Принудительная установка заголовков безопасности HTTP (защита от XSS, clickjacking, MIME-sniffing)."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Permissions-Policy"] = "geolocation=(), camera=(), microphone=()"
    if request.url.scheme == "https" or request.headers.get("x-forwarded-proto") == "https":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


@app.middleware("http")
async def request_body_size_limiter(request: Request, call_next):
    """Защита от DoS и исчерпания памяти при отправке аномально больших payload (включая chunked transfer)."""
    max_bytes = settings.MAX_REQUEST_BODY_SIZE_MB * 1024 * 1024
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            if int(content_length) > max_bytes:
                return Response(
                    content=json.dumps({"detail": f"Payload Too Large: request body exceeds {settings.MAX_REQUEST_BODY_SIZE_MB}MB limit"}),
                    status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                    media_type="application/json",
                )
        except ValueError:
            pass

    # Защита от потокового/chunked исчерпания памяти без Content-Length
    received_bytes = 0
    original_receive = request._receive

    async def limited_receive():
        nonlocal received_bytes
        msg = await original_receive()
        if msg.get("type") == "http.request":
            received_bytes += len(msg.get("body", b""))
            if received_bytes > max_bytes:
                raise HTTPException(
                    status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                    detail=f"Payload Too Large: streaming body exceeds {settings.MAX_REQUEST_BODY_SIZE_MB}MB limit",
                )
        return msg

    request._receive = limited_receive
    try:
        return await call_next(request)
    except HTTPException as exc:
        if exc.status_code == status.HTTP_413_CONTENT_TOO_LARGE:
            return Response(
                content=json.dumps({"detail": exc.detail}),
                status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                media_type="application/json",
            )
        raise


app.include_router(gateway_router)


@app.get("/", tags=["Info"])
async def root_info() -> dict[str, Any]:
    return {
        "service": "B2B HoReCa Order Co-Pilot Enterprise",
        "version": "2.0.0",
        "status": "operational",
    }


@app.get("/health", status_code=status.HTTP_200_OK, tags=["Health"])
async def health_check() -> dict[str, Any]:
    redis_alive = await redis_client.ping()
    return {
        "status": "healthy" if redis_alive else "degraded",
        "redis_connected": redis_alive,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host=settings.APP_HOST, port=settings.APP_PORT, reload=False)
