"""Notificaciones: Telegram (cola asíncrona) + bus de eventos para el dashboard."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Callable, Optional

import aiohttp

log = logging.getLogger("executor.notify")


class EventBus:
    """Eventos para el dashboard (toasts y actividad reciente)."""

    def __init__(self, maxlen: int = 200):
        self.recent: deque[dict] = deque(maxlen=maxlen)
        self._listeners: list[Callable[[dict], None]] = []
        self._seq = 0

    def subscribe(self, callback: Callable[[dict], None]) -> None:
        self._listeners.append(callback)

    def emit(self, kind: str, text: str, level: str = "info", **data) -> dict:
        self._seq += 1
        event = {"id": self._seq, "ts": time.time(), "kind": kind, "level": level, "text": text, "data": data}
        self.recent.append(event)
        for cb in list(self._listeners):
            try:
                cb(event)
            except Exception:  # pragma: no cover
                log.exception("EventBus: listener falló")
        return event


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.enabled = bool(token and chat_id)
        self._url = f"https://api.telegram.org/bot{token}/sendMessage" if token else ""
        self._chat_id = chat_id
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=500)
        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self.sent = 0
        self.failed = 0

    def start(self) -> None:
        if self.enabled and self._task is None:
            self._task = asyncio.create_task(self._worker(), name="telegram")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
        if self._session is not None and not self._session.closed:
            await self._session.close()

    def send(self, text: str) -> None:
        if not self.enabled:
            return
        try:
            self._queue.put_nowait(text)
        except asyncio.QueueFull:
            self.failed += 1

    async def _worker(self) -> None:
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10))
        while True:
            text = await self._queue.get()
            payload = {"chat_id": self._chat_id, "text": text[:4000], "parse_mode": "HTML",
                       "disable_web_page_preview": True}
            for attempt in range(3):
                try:
                    async with self._session.post(self._url, json=payload) as resp:
                        if resp.status == 200:
                            self.sent += 1
                            break
                        if resp.status == 429:
                            data = await resp.json(content_type=None)
                            await asyncio.sleep(float(data.get("parameters", {}).get("retry_after", 2)))
                            continue
                        log.error("Telegram %s: %s", resp.status, (await resp.text())[:200])
                        self.failed += 1
                        break
                except Exception as exc:
                    log.warning("Telegram: %s", exc)
                    await asyncio.sleep(1 + attempt)
            else:
                self.failed += 1
            await asyncio.sleep(0.05)
