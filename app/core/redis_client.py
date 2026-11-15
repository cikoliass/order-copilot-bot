import json
import logging
import time
import uuid
from typing import Any, Optional
import redis.asyncio as aioredis
from app.core.config import settings

logger = logging.getLogger(__name__)

LUA_RELEASE_LOCK = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class RedisClient:
    """Пул соединений с Redis с поддержкой распределенных локов и кэширования сессий."""

    def __init__(self, url: str):
        self.url = url
        self._redis: Optional[aioredis.Redis] = None
        self._in_memory_fallback: dict[str, Any] = {}
        # lock_key -> (token, expire_at)
        self._in_memory_locks: dict[str, tuple[str, float]] = {}
        # lock_key -> token used by this client instance
        self._held_lock_tokens: dict[str, str] = {}
        self._use_fallback = False
        self._last_reconnect_attempt = 0.0

    async def _try_reconnect(self) -> None:
        """Попытка переподключения к Redis, если бот находится в fallback-режиме."""
        if not self._use_fallback:
            return
        now = time.time()
        if now - self._last_reconnect_attempt < 10.0:
            return
        self._last_reconnect_attempt = now
        try:
            if self._redis:
                await self._redis.ping()
                self._use_fallback = False
                logger.info("Подключение к Redis восстановлено: %s", self.url)
            else:
                await self.connect()
        except Exception:
            pass

    async def connect(self) -> None:
        try:
            self._redis = aioredis.from_url(
                self.url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=2.0,
                socket_connect_timeout=2.0,
            )
            await self._redis.ping()
            self._use_fallback = False
            logger.info("Успешное подключение к Redis: %s", self.url)
        except Exception as exc:
            self._use_fallback = True
            logger.warning(
                "Redis недоступен (%s). Переключение на встроенный in-memory fallback: %s",
                self.url,
                exc,
            )

    async def close(self) -> None:
        if self._redis and not self._use_fallback:
            try:
                await self._redis.aclose()
            except Exception as exc:
                logger.warning("Ошибка закрытия соединения с Redis: %s", exc)

    async def ping(self) -> bool:
        await self._try_reconnect()
        if self._use_fallback or not self._redis:
            return False
        try:
            return bool(await self._redis.ping())
        except Exception:
            return False

    async def acquire_lock(self, key: str, timeout: int = 60, lock_token: Optional[str] = None) -> bool:
        """
        Атомарный distributed lock с уникальным токеном владения.
        Возвращает True, если лок успешно взят, иначе False.
        """
        await self._try_reconnect()
        lock_key = f"lock:{key}"
        token = lock_token or str(uuid.uuid4())

        if not self._use_fallback and self._redis:
            try:
                acquired = await self._redis.set(lock_key, token, nx=True, ex=timeout)
                if acquired:
                    self._held_lock_tokens[lock_key] = token
                    return True
                return False
            except Exception as exc:
                logger.warning("Сбой Redis при acquire_lock: %s. Используем fallback-лок.", exc)

        now = time.time()
        # Очистка протухшего лока
        if lock_key in self._in_memory_locks:
            _, expire_at = self._in_memory_locks[lock_key]
            if now < expire_at:
                return False

        self._in_memory_locks[lock_key] = (token, now + timeout)
        self._held_lock_tokens[lock_key] = token
        return True

    async def release_lock(self, key: str, lock_token: Optional[str] = None) -> None:
        """Безопасное освобождение распределенного лока (не снимает чужой лок)."""
        await self._try_reconnect()
        lock_key = f"lock:{key}"
        token = lock_token or self._held_lock_tokens.pop(lock_key, None)

        if not token:
            # Если токен не найден, удаляем только если точно уверены
            return

        if not self._use_fallback and self._redis:
            try:
                await self._redis.eval(LUA_RELEASE_LOCK, 1, lock_key, token)
                return
            except Exception as exc:
                logger.warning("Сбой Redis при release_lock: %s", exc)

        if lock_key in self._in_memory_locks:
            held_token, _ = self._in_memory_locks[lock_key]
            if held_token == token:
                self._in_memory_locks.pop(lock_key, None)

    async def set_order_draft(self, session_id: str, data: dict, ttl: int = 86400) -> None:
        """Сохранение черновика заказа сессии в Redis с защитой от переполнения памяти."""
        await self._try_reconnect()
        key = f"order:draft:{session_id}"
        serialized = json.dumps(data, ensure_ascii=False)
        if not self._use_fallback and self._redis:
            try:
                await self._redis.set(key, serialized, ex=ttl)
                return
            except Exception as exc:
                logger.warning("Сбой Redis при сохранении черновика: %s", exc)

        now = time.time()
        self._in_memory_fallback[key] = (serialized, now + ttl)
        # Очистка устаревших черновиков для защиты от OOM/DoS
        if len(self._in_memory_fallback) > 5000:
            expired_keys = [
                k for k, v in self._in_memory_fallback.items()
                if isinstance(v, tuple) and len(v) == 2 and isinstance(v[1], (int, float)) and v[1] < now
            ]
            for ek in expired_keys[:1000]:
                self._in_memory_fallback.pop(ek, None)
            if len(self._in_memory_fallback) > 5000:
                for old_k in list(self._in_memory_fallback.keys())[:500]:
                    self._in_memory_fallback.pop(old_k, None)

    async def get_order_draft(self, session_id: str) -> Optional[dict]:
        """Получение сохраненного черновика заказа."""
        await self._try_reconnect()
        key = f"order:draft:{session_id}"
        if not self._use_fallback and self._redis:
            try:
                raw = await self._redis.get(key)
                if raw:
                    return json.loads(raw)
                return None
            except Exception as exc:
                logger.warning("Сбой Redis при чтении черновика: %s", exc)

        item = self._in_memory_fallback.get(key)
        if not item:
            return None
        raw, expire_at = item
        if time.time() > expire_at:
            self._in_memory_fallback.pop(key, None)
            return None
        return json.loads(raw)

    async def clear_order_draft(self, session_id: str) -> None:
        """Удаление черновика заказа после подтверждения или отмены."""
        await self._try_reconnect()
        key = f"order:draft:{session_id}"
        if not self._use_fallback and self._redis:
            try:
                await self._redis.delete(key)
                return
            except Exception as exc:
                logger.warning("Сбой Redis при удалении черновика: %s", exc)

        self._in_memory_fallback.pop(key, None)

    # Управление стоп-листом и остатками (персистентность между рестартами)
    async def add_stop_list_item(self, sku_id: str) -> None:
        await self._try_reconnect()
        key = "catalog:stop_list"
        if not self._use_fallback and self._redis:
            try:
                await self._redis.sadd(key, str(sku_id))
                return
            except Exception:
                pass
        self._in_memory_fallback.setdefault(key, set()).add(str(sku_id))

    async def remove_stop_list_item(self, sku_id: str) -> None:
        await self._try_reconnect()
        key = "catalog:stop_list"
        if not self._use_fallback and self._redis:
            try:
                await self._redis.srem(key, str(sku_id))
                return
            except Exception:
                pass
        if key in self._in_memory_fallback and isinstance(self._in_memory_fallback[key], set):
            self._in_memory_fallback[key].discard(str(sku_id))

    async def get_stop_list(self) -> set[str]:
        await self._try_reconnect()
        key = "catalog:stop_list"
        if not self._use_fallback and self._redis:
            try:
                members = await self._redis.smembers(key)
                return set(members or [])
            except Exception:
                pass
        res = self._in_memory_fallback.get(key)
        return set(res) if isinstance(res, set) else set()

    async def set_stock_override(self, sku_id: str, qty: float) -> None:
        await self._try_reconnect()
        key = "catalog:stocks"
        if not self._use_fallback and self._redis:
            try:
                await self._redis.hset(key, str(sku_id), str(qty))
                return
            except Exception:
                pass
        self._in_memory_fallback.setdefault(key, {})[str(sku_id)] = qty

    async def get_stock_overrides(self) -> dict[str, float]:
        await self._try_reconnect()
        key = "catalog:stocks"
        if not self._use_fallback and self._redis:
            try:
                raw_dict = await self._redis.hgetall(key)
                return {k: float(v) for k, v in raw_dict.items()}
            except Exception:
                pass
        res = self._in_memory_fallback.get(key)
        return dict(res) if isinstance(res, dict) else {}

    async def check_rate_limit(self, key: str, max_requests: int = 30, window_sec: int = 60) -> bool:
        """Rate-limiting по скользящему окну (Sliding Window Log) в Redis / fallback."""
        await self._try_reconnect()
        rl_key = f"rate_limit:{key}"
        now = time.time()
        if not self._use_fallback and self._redis:
            try:
                pipe = self._redis.pipeline()
                pipe.zremrangebyscore(rl_key, 0, now - window_sec)
                pipe.zadd(rl_key, {f"{now}:{uuid.uuid4().hex[:8]}": now})
                pipe.zcard(rl_key)
                pipe.expire(rl_key, window_sec)
                results = await pipe.execute()
                count = results[2]
                return count <= max_requests
            except Exception as exc:
                logger.warning("Сбой Redis при проверке rate limit: %s", exc)

        bucket = self._in_memory_fallback.setdefault(rl_key, [])
        if isinstance(bucket, list):
            self._in_memory_fallback[rl_key] = [ts for ts in bucket if ts > now - window_sec]
            self._in_memory_fallback[rl_key].append(now)
            allowed = len(self._in_memory_fallback[rl_key]) <= max_requests
            # Защита от утечки памяти: периодическая очистка протухших ключей rate_limit
            if len(self._in_memory_fallback) > 5000:
                stale_keys = [
                    k for k, v in self._in_memory_fallback.items()
                    if k.startswith("rate_limit:") and isinstance(v, list) and (not v or v[-1] <= now - window_sec)
                ]
                for sk in stale_keys[:1000]:
                    self._in_memory_fallback.pop(sk, None)
            return allowed
        return True


redis_client = RedisClient(settings.REDIS_URL)
