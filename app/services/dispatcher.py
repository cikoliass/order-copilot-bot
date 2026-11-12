import html as html_lib
import json
import logging
import re
import time
from typing import Any
import httpx
from app.core.config import settings
from app.services.schemas import OutboundMessage

logger = logging.getLogger(__name__)


def html_to_plain_text(html_text: str) -> str:
    """Удаляет HTML-теги для мессенджеров, не поддерживающих HTML (VK, Viber, WhatsApp)."""
    t = re.sub(r"<(?:b|strong)>(.*?)</(?:b|strong)>", r"*\1*", html_text, flags=re.DOTALL)
    t = re.sub(r"<i>(.*?)</i>", r"_\1_", t, flags=re.DOTALL)
    t = re.sub(r"<code>(.*?)</code>", r"`\1`", t, flags=re.DOTALL)
    t = re.sub(r"<br\s*/?>|</p>", "\n", t, flags=re.IGNORECASE)
    t = re.sub(r"<[^>]+>", "", t)
    return html_lib.unescape(t).strip()


def clean_text_for_vk(text: str) -> str:
    """Форматирует текст специально для ВКонтакте (без HTML и сырого markdown)."""
    t = html_to_plain_text(text)
    t = re.sub(r"[*_`]", "", t)
    return re.sub(r"\n{3,}", "\n\n", t).strip()


_tg_bot: Any = None


def create_telegram_bot(token: str) -> Any:
    """Создает экземпляр aiogram Bot с защитой сокетов."""
    from aiogram import Bot
    from aiogram.client.session.aiohttp import AiohttpSession

    proxy = getattr(settings, "TELEGRAM_PROXY", None)
    session = AiohttpSession(proxy=proxy) if proxy else AiohttpSession()
    session._connector_init["force_close"] = True
    session._connector_init["enable_cleanup_closed"] = True
    return Bot(token=token, session=session)


def get_telegram_bot() -> Any:
    global _tg_bot
    if _tg_bot is None and settings.TELEGRAM_BOT_TOKEN and not settings.TELEGRAM_BOT_TOKEN.startswith("YOUR_"):
        try:
            _tg_bot = create_telegram_bot(settings.TELEGRAM_BOT_TOKEN)
        except Exception as exc:
            logger.warning("Не удалось инициализировать Telegram Bot: %s", exc)
    return _tg_bot


class OmnichannelDispatcher:
    """Универсальный диспетчер исходящих сообщений во все поддерживаемые мессенджеры."""

    def set_telegram_bot(self, bot: Any) -> None:
        global _tg_bot
        _tg_bot = bot

    async def send(self, msg: OutboundMessage) -> bool:
        handlers = {
            "telegram": self._send_telegram,
            "vk": self._send_vk,
            "whatsapp": self._send_whatsapp,
            "wa": self._send_whatsapp,
            "greenapi": self._send_whatsapp,
            "viber": self._send_viber,
            "max": self._send_max,
        }
        platform = msg.platform.lower().strip()
        handler = handlers.get(platform)
        if not handler:
            logger.error("Неизвестная платформа отправки: %s", platform)
            return False
        try:
            return await handler(msg)
        except Exception as exc:
            logger.exception("Ошибка отправки сообщения в %s: %s", platform, exc)
            return False

    async def _send_telegram(self, msg: OutboundMessage) -> bool:
        bot = get_telegram_bot()
        if not bot:
            logger.warning("TELEGRAM_BOT_TOKEN не задан. Сообщение не отправлено в Telegram.")
            return False

        from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

        reply_markup = None
        if msg.buttons:
            reply_markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=b.text, callback_data=b.payload) for b in row]
                for row in msg.buttons
            ])
        target_chat_id = int(msg.chat_id) if msg.chat_id.lstrip("-").isdigit() else msg.chat_id

        try:
            await bot.send_message(chat_id=target_chat_id, text=msg.text, parse_mode=msg.parse_mode, reply_markup=reply_markup)
            return True
        except Exception as exc:
            logger.warning("Сбой отправки с форматированием HTML (%s). Пробуем plain text...", exc)
            try:
                await bot.send_message(chat_id=target_chat_id, text=html_to_plain_text(msg.text), parse_mode=None, reply_markup=reply_markup)
                return True
            except Exception as exc2:
                logger.error("Ошибка Telegram API при отправке через aiogram: %s", exc2)
                return False

    async def _send_vk(self, msg: OutboundMessage) -> bool:
        token = settings.VK_GROUP_TOKEN
        if not token:
            logger.warning("VK_GROUP_TOKEN не задан. Сообщение не отправлено в VK.")
            return False

        payload: dict[str, Any] = {
            "access_token": token,
            "v": "5.199",
            "peer_id": int(msg.chat_id),
            "random_id": int(time.time() * 1000) % 2147483647,
            "message": clean_text_for_vk(msg.text),
        }

        if msg.buttons:
            vk_btns = [
                [
                    {
                        "action": {"type": "callback", "label": b.text[:40], "payload": json.dumps({"action": b.payload}, ensure_ascii=False)},
                        "color": "primary" if "Подтвердить" in b.text else "secondary",
                    }
                    for b in row
                ]
                for row in msg.buttons
            ]
            payload["keyboard"] = json.dumps({"inline": True, "buttons": vk_btns}, ensure_ascii=False)

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post("https://api.vk.com/method/messages.send", data=payload)
            data = resp.json()
            if "error" in data:
                logger.error("Ошибка VK API: %s", data["error"])
                return False
            return True

    async def _send_whatsapp(self, msg: OutboundMessage) -> bool:
        inst_id, token = settings.GREEN_API_INSTANCE_ID, settings.GREEN_API_TOKEN
        if not inst_id or not token:
            logger.warning("Параметры GREEN_API не заданы. Сообщение не отправлено в WhatsApp.")
            return False

        host = f"https://{inst_id[:4]}.api.greenapi.com" if len(inst_id) >= 4 else "https://api.green-api.com"
        chat_id = msg.chat_id if msg.chat_id.endswith(("@c.us", "@g.us")) else f"{msg.chat_id}@c.us"
        plain_text = html_to_plain_text(msg.text)

        flat_buttons = []
        if msg.buttons:
            for row in msg.buttons:
                for b in row:
                    flat_buttons.append({"buttonId": b.payload or b.text, "buttonText": b.text[:20]})
                    if len(flat_buttons) >= 3:
                        break
                if len(flat_buttons) >= 3:
                    break

            plain_text += (
                "\n\n📋 *Действия:*\n"
                "• Нажмите кнопку ниже или напишите *«Подтвердить»* (или *1*)\n"
                "• Чтобы отменить — *«Отменить»* (или *2*)\n\n"
                "✏️ *Правки:* напишите изменения обычным языком (например: _«добавь 2 кг картофеля»_)."
            )

        async with httpx.AsyncClient(timeout=10.0) as client:
            if flat_buttons:
                btn_payload = {
                    "chatId": chat_id,
                    "header": "📦 Заказ Co-Pilot",
                    "body": plain_text,
                    "footer": "Нажмите кнопку или ответьте текстом",
                    "buttons": flat_buttons,
                }
                try:
                    btn_resp = await client.post(f"{host}/waInstance{inst_id}/sendInteractiveButtonsReply/{token}", json=btn_payload)
                    if btn_resp.status_code == 200:
                        return True
                    logger.warning("Green-API sendInteractiveButtonsReply вернул %d: %s", btn_resp.status_code, btn_resp.text)
                except Exception as exc:
                    logger.warning("Ошибка вызова sendInteractiveButtonsReply: %s", exc)

            resp = await client.post(f"{host}/waInstance{inst_id}/sendMessage/{token}", json={"chatId": chat_id, "message": plain_text})
            if resp.status_code != 200:
                logger.error("Ошибка Green-API HTTP %d: %s", resp.status_code, resp.text)
                return False
            return True

    async def _send_viber(self, msg: OutboundMessage) -> bool:
        token = settings.VIBER_AUTH_TOKEN
        if not token:
            logger.warning("VIBER_AUTH_TOKEN не задан. Сообщение не отправлено в Viber.")
            return False

        payload: dict[str, Any] = {
            "receiver": msg.chat_id,
            "min_api_version": 7,
            "type": "text",
            "text": html_to_plain_text(msg.text),
            "sender": {"name": settings.VIBER_SENDER_NAME},
        }

        if msg.buttons:
            viber_buttons = [
                {
                    "ActionType": "reply",
                    "ActionBody": b.payload,
                    "Text": f"<font color='#ffffff'><b>{b.text}</b></font>",
                    "BgColor": "#28a745" if "Подтвердить" in b.text else "#dc3545",
                    "Rows": 1,
                    "Columns": 3 if len(row) > 1 else 6,
                }
                for row in msg.buttons for b in row
            ]
            payload["keyboard"] = {"Type": "keyboard", "DefaultHeight": False, "Buttons": viber_buttons}

        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post("https://chatapi.viber.com/pa/send_message", json=payload, headers={"X-Viber-Auth-Token": token})
            if resp.status_code != 200:
                logger.error("Ошибка Viber API HTTP %d: %s", resp.status_code, resp.text)
                return False
            return True

    async def _send_max(self, msg: OutboundMessage) -> bool:
        token = settings.MAX_BOT_TOKEN
        if not token:
            logger.warning("MAX_BOT_TOKEN не задан. Сообщение не отправлено в MAX.")
            return False

        base_url = "https://platform-api2.max.ru" if "max-chat.ru" in (settings.MAX_API_BASE_URL or "") else (settings.MAX_API_BASE_URL or "https://platform-api2.max.ru").rstrip("/")
        headers = {"Authorization": token, "Content-Type": "application/json"}
        payload: dict[str, Any] = {"text": clean_text_for_vk(msg.text)}

        if msg.buttons:
            max_btns = [[{"type": "callback", "text": b.text, "payload": b.payload or b.text} for b in row] for row in msg.buttons]
            payload["attachments"] = [{"type": "inline_keyboard", "payload": {"buttons": max_btns}}]

        async with httpx.AsyncClient(timeout=10.0) as client:
            for param in (f"chat_id={msg.chat_id}", f"user_id={msg.chat_id}"):
                resp = await client.post(f"{base_url}/messages?{param}", json=payload, headers=headers)
                if resp.status_code in (200, 201):
                    return True
                if resp.status_code not in (400, 404):
                    break
            logger.error("Ошибка MAX API (chat_id %s) HTTP %d: %s", msg.chat_id, resp.status_code, resp.text)
            return False


dispatcher = OmnichannelDispatcher()
