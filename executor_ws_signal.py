"""Señales al Executor por una conexión WebSocket persistente (``/ws/signal``).

Cópialo junto a tu bot. Sustituye a ``POST /signal``: la conexión queda abierta (sin handshake TLS/HTTP por
señal), así que la orden sale antes. Si el WebSocket no está disponible, la señal se envía por ``POST /signal``
con el mismo ``signal_id``: el executor nunca la ejecuta dos veces.

::

    sock = ExecutorSignalSocket(EXECUTOR_URL, EXECUTOR_SECRET)   # abre la conexión al crearlo
    reply = sock.send({"action": "open", "trade_id": 1, "symbol": "BTCUSDT", "direction": "LONG",
                       "price": 65000, "quantity": 0.001})      # bloquea hasta la respuesta (o el fallback)

``send`` es síncrono y seguro desde cualquier hilo (los bots lo llaman con ``asyncio.to_thread`` o un hilo).
``EXECUTOR_URL`` puede terminar en ``/b/<bot>`` (identidad del bot). Requiere ``websockets`` (o solo usará POST).
"""

from __future__ import annotations

import asyncio
import itertools
import json
import secrets
import threading
import time
import urllib.error
import urllib.request
from typing import Any, Callable, Optional

USER_AGENT = "executor-ws-signal/1.0"


def new_signal_id() -> str:
    return f"{int(time.time() * 1000):x}-{secrets.token_hex(6)}"


class ExecutorSignalSocket:
    def __init__(self, base_url: str, secret: str, timeout: float = 5.0, http_timeout: float = 8.0,
                 logger: Optional[Callable[[str], Any]] = None, connect: bool = True) -> None:
        self.base_url = (base_url or "").strip().rstrip("/")
        self.secret = secret
        self.timeout = timeout
        self.http_timeout = http_timeout
        self.log = logger or (lambda m: print(m, flush=True))
        self.url = ("ws" + self.base_url[4:] if self.base_url.startswith("http") else self.base_url) + "/ws/signal"
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._ws: Any = None
        self._connected = threading.Event()
        self._waiters: dict = {}
        self._ids = itertools.count(1)
        self._lock = threading.Lock()
        self.sent_ws = self.sent_http = 0
        if connect and self.base_url:
            self.start()

    # ── conexión (hilo propio con su event loop) ──────────────────────────
    def start(self) -> None:
        with self._lock:
            if self._loop is not None:
                return
            try:
                import websockets  # noqa: F401
            except ImportError:
                self.log("[executor-ws] websockets no instalado: las señales irán por POST /signal")
                return
            loop = asyncio.new_event_loop()
            self._loop = loop
            threading.Thread(target=self._thread, args=(loop,), name="executor-ws-signal", daemon=True).start()

    def _thread(self, loop: asyncio.AbstractEventLoop) -> None:
        asyncio.set_event_loop(loop)
        loop.create_task(self._run())
        loop.run_forever()

    async def _connect(self):
        import websockets
        headers = {"X-Signal-Secret": self.secret, "User-Agent": USER_AGENT}
        try:
            return await websockets.connect(self.url, additional_headers=headers, ping_interval=20,
                                            ping_timeout=20, max_size=1 << 20, open_timeout=10)
        except TypeError:  # websockets < 14
            return await websockets.connect(self.url, extra_headers=headers, ping_interval=20,
                                            ping_timeout=20, max_size=1 << 20, open_timeout=10)

    async def _run(self) -> None:
        backoff = 0.5
        while True:
            try:
                ws = await self._connect()
                self._ws, backoff = ws, 0.5
                self._connected.set()
                self.log(f"[executor-ws] conectado a {self.url}")
                async for raw in ws:
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    fut = self._waiters.pop(msg.get("id"), None)
                    if fut is not None and not fut.done():
                        fut.set_result(msg)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.log(f"[executor-ws] conexión caída ({exc!r}); se reconecta")
            finally:
                self._connected.clear()
                self._ws = None
                for fut in list(self._waiters.values()):
                    if not fut.done():
                        fut.set_exception(ConnectionError("WebSocket /ws/signal cerrado"))
                self._waiters.clear()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 10.0)

    async def _send_ws(self, msg: dict) -> dict:
        if self._ws is None:
            raise ConnectionError("WebSocket /ws/signal no conectado")
        req_id = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._waiters[req_id] = fut
        try:
            await self._ws.send(json.dumps({**msg, "id": req_id}))
            return await asyncio.wait_for(fut, self.timeout)
        finally:
            self._waiters.pop(req_id, None)

    # ── envío ─────────────────────────────────────────────────────────────
    def send(self, payload: dict) -> dict:
        """Envía la señal (por WebSocket; si falla, por POST con el mismo ``signal_id``) y devuelve la respuesta
        del executor (``ok``, ``status``, ``detail``…). Nunca lanza: un fallo devuelve ``{"ok": False, ...}``."""
        msg = dict(payload)
        msg.setdefault("signal_id", new_signal_id())
        if self._loop is not None and self._connected.wait(timeout=2.0 if self.sent_ws == 0 else 0.0):
            try:
                reply = asyncio.run_coroutine_threadsafe(self._send_ws(msg), self._loop).result(self.timeout + 1)
                if int(reply.get("status") or 200) < 500:
                    self.sent_ws += 1
                    return reply
            except Exception as exc:
                self.log(f"[executor-ws] {msg.get('action')} {msg.get('symbol')}: {exc!r}; se envía por POST")
        return self._send_http(msg)

    def _send_http(self, msg: dict) -> dict:
        req = urllib.request.Request(
            f"{self.base_url}/signal", data=json.dumps(msg).encode("utf-8"), method="POST",
            headers={"Content-Type": "application/json", "X-Signal-Secret": self.secret, "User-Agent": USER_AGENT})
        try:
            with urllib.request.urlopen(req, timeout=self.http_timeout) as resp:
                self.sent_http += 1
                return json.loads(resp.read().decode("utf-8") or "{}")
        except urllib.error.HTTPError as exc:
            try:
                return {"ok": False, "status": exc.code, **json.loads(exc.read().decode("utf-8") or "{}")}
            except ValueError:
                return {"ok": False, "status": exc.code, "error": str(exc)}
        except Exception as exc:
            return {"ok": False, "status": 0, "error": repr(exc)}

    def close(self) -> None:
        loop = self._loop
        if loop is None:
            return
        async def _close():
            if self._ws is not None:
                await self._ws.close()
            for task in asyncio.all_tasks():
                if task is not asyncio.current_task():
                    task.cancel()
        try:
            asyncio.run_coroutine_threadsafe(_close(), loop).result(3)
        except Exception:
            pass
        loop.call_soon_threadsafe(loop.stop)
        self._loop = None
