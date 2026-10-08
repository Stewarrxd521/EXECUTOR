"""Streams WebSocket de Binance: mercado completo y datos de usuario.

* ``MarketData``: UNA conexión con ``!markPrice@arr@1s`` (mark price, índice y
  funding de todos los símbolos), ``!miniTicker@arr`` (último precio y 24h) y
  ``!contractInfo`` (listados, estados y brackets). Ya no hay que suscribir y
  reconectar por cada símbolo: el precio de cualquier par siempre está fresco.
* ``UserDataStream``: listenKey obtenido por la WS API (``userDataStream.start``)
  y renovado por WS; recibe ``ORDER_TRADE_UPDATE``, ``ACCOUNT_UPDATE``,
  ``ALGO_UPDATE``, ``ACCOUNT_CONFIG_UPDATE``... en tiempo real.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

import aiohttp

from .binance_api import BinanceWsApi, ServerClock
from .precision import safe_float

log = logging.getLogger("executor.streams")

Handler = Callable[[dict], Optional[Awaitable[None]]]


@dataclass
class MarkInfo:
    mark: float
    index: float
    funding_rate: float
    next_funding_ms: int
    ts: float


@dataclass
class MiniTicker:
    last: float
    open: float
    high: float
    low: float
    volume: float
    quote_volume: float
    ts: float

    @property
    def change_pct(self) -> float:
        return (self.last - self.open) / self.open * 100 if self.open else 0.0


class StreamConnection:
    """Conexión combinada con reconexión, backoff y estadísticas."""

    def __init__(self, name: str, url_factory: Callable[[], Awaitable[str]], on_message: Handler,
                 on_connect: Optional[Callable[[], Awaitable[None]]] = None,
                 idle_timeout: Optional[float] = 60.0):
        self.name = name
        self._url_factory = url_factory
        self._on_message = on_message
        self._on_connect = on_connect
        self._idle_timeout = idle_timeout
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = False
        self.connected = False
        self.connected_since = 0.0
        self.messages = 0
        self.reconnects = 0
        self.consecutive_failures = 0
        self.last_message_ts = 0.0
        self.last_error = ""

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name=f"stream-{self.name}")

    async def stop(self) -> None:
        self._stop = True
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._task is not None:
            self._task.cancel()
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def reconnect(self) -> None:
        """Fuerza una reconexión (p.ej. listenKey nuevo)."""
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()

    async def _run(self) -> None:
        delay = 1.0
        while not self._stop:
            try:
                url = await self._url_factory()
                if not url:
                    await asyncio.sleep(5)
                    continue
                if self._session is None or self._session.closed:
                    self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=10))
                async with self._session.ws_connect(url, heartbeat=30, autoping=True, max_msg_size=0,
                                                    compress=0) as ws:
                    self._ws = ws
                    self.connected = True
                    self.connected_since = time.time()
                    self.consecutive_failures = 0
                    log.info("Stream %s conectado", self.name)
                    if self._on_connect is not None:
                        asyncio.create_task(self._on_connect())
                    delay = 1.0
                    while not self._stop:
                        msg = await ws.receive(timeout=self._idle_timeout)
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self.messages += 1
                            self.last_message_ts = time.time()
                            try:
                                data = json.loads(msg.data)
                            except ValueError:
                                continue
                            payload = data.get("data", data) if isinstance(data, dict) else data
                            try:
                                result = self._on_message(payload)
                                if asyncio.iscoroutine(result):
                                    await result
                            except Exception:
                                log.exception("Stream %s: error procesando mensaje", self.name)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING,
                                          aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.consecutive_failures += 1
                self.last_error = repr(exc)
                log.warning("Stream %s: %r", self.name, exc)
            finally:
                if self.connected:
                    self.reconnects += 1
                self.connected = False
                self._ws = None
            if self._stop:
                break
            await asyncio.sleep(delay + random.random())
            delay = min(delay * 2, 30.0)

    def stats(self) -> dict:
        return {
            "connected": self.connected,
            "messages": self.messages,
            "reconnects": self.reconnects,
            "last_message_age_s": round(time.time() - self.last_message_ts, 1) if self.last_message_ts else None,
            "last_error": self.last_error,
            "uptime_s": int(time.time() - self.connected_since) if self.connected else 0,
        }


class MarketData:
    """Precios de todo el mercado por un único WebSocket."""

    STREAMS = ("!markPrice@arr@1s", "!miniTicker@arr", "!contractInfo")

    def __init__(self, stream_base_url: str, clock: ServerClock,
                 on_contract_info: Optional[Callable[[dict], None]] = None):
        self.base = stream_base_url.rstrip("/")
        self.clock = clock
        self.marks: dict[str, MarkInfo] = {}
        self.tickers: dict[str, MiniTicker] = {}
        self._on_contract_info = on_contract_info
        self._tick_listeners: list[Callable[[], None]] = []
        self.conn = StreamConnection("market", self._url, self._handle)

    async def _url(self) -> str:
        return f"{self.base}/market/stream?streams={'/'.join(self.STREAMS)}"

    def on_tick(self, callback: Callable[[], None]) -> None:
        """Callback tras cada lote de mark prices (≈1 s)."""
        self._tick_listeners.append(callback)

    def start(self) -> None:
        self.conn.start()

    async def stop(self) -> None:
        await self.conn.stop()

    def _handle(self, payload) -> None:
        now = time.time()
        if isinstance(payload, list):
            if not payload:
                return
            kind = payload[0].get("e")
            if kind == "markPriceUpdate":
                self.clock.observe(payload[0].get("E", 0))
                for item in payload:
                    sym = item.get("s")
                    price = safe_float(item.get("p"))
                    if sym and price > 0:
                        self.marks[sym] = MarkInfo(price, safe_float(item.get("i")), safe_float(item.get("r")),
                                                   int(item.get("T") or 0), now)
                for cb in self._tick_listeners:
                    try:
                        cb()
                    except Exception:
                        log.exception("MarketData: listener de tick falló")
            elif kind == "24hrMiniTicker":
                for item in payload:
                    sym = item.get("s")
                    if sym:
                        self.tickers[sym] = MiniTicker(
                            safe_float(item.get("c")), safe_float(item.get("o")), safe_float(item.get("h")),
                            safe_float(item.get("l")), safe_float(item.get("v")), safe_float(item.get("q")), now,
                        )
        elif isinstance(payload, dict) and payload.get("e") == "contractInfo":
            if self._on_contract_info is not None:
                self._on_contract_info(payload)

    def mark(self, symbol: str, max_age_s: Optional[float] = None) -> float:
        info = self.marks.get(symbol.upper())
        if info is None or (max_age_s is not None and time.time() - info.ts > max_age_s):
            return 0.0
        return info.mark

    def last(self, symbol: str) -> float:
        t = self.tickers.get(symbol.upper())
        return t.last if t else 0.0

    def price(self, symbol: str, max_age_s: Optional[float] = None) -> float:
        """Mark price fresco; si no hay, último precio negociado."""
        return self.mark(symbol, max_age_s) or (self.last(symbol) if max_age_s is None else 0.0)

    def symbol_view(self, symbol: str) -> dict:
        symbol = symbol.upper()
        m, t = self.marks.get(symbol), self.tickers.get(symbol)
        return {
            "symbol": symbol,
            "mark": m.mark if m else 0.0,
            "index": m.index if m else 0.0,
            "funding_rate": m.funding_rate if m else 0.0,
            "next_funding_ms": m.next_funding_ms if m else 0,
            "last": t.last if t else 0.0,
            "open": t.open if t else 0.0,
            "high": t.high if t else 0.0,
            "low": t.low if t else 0.0,
            "volume": t.volume if t else 0.0,
            "quote_volume": t.quote_volume if t else 0.0,
            "change_pct": t.change_pct if t else 0.0,
        }

    def market_rows(self) -> list:
        """Lista compacta para el panel de mercados: [símbolo, último, %24h, volumen USDT]."""
        rows = []
        for sym, t in self.tickers.items():
            rows.append([sym, t.last, round(t.change_pct, 2), round(t.quote_volume)])
        return rows

    def stats(self) -> dict:
        data = self.conn.stats()
        data.update({"symbols": len(self.marks), "tickers": len(self.tickers)})
        return data


class UserDataStream:
    """User Data Stream con listenKey gestionado 100% por la WS API."""

    KEEPALIVE_S = 30 * 60

    def __init__(self, stream_base_url: str, ws_api: BinanceWsApi, clock: ServerClock,
                 on_event: Callable[[dict], Awaitable[None]], on_connect: Callable[[], Awaitable[None]]):
        self.base = stream_base_url.rstrip("/")
        self.ws_api = ws_api
        self.clock = clock
        self._on_event = on_event
        self._listen_key = ""
        self._key_ts = 0.0
        self._keepalive_task: Optional[asyncio.Task] = None
        self._path_index = 0
        self._paths = ("/private/stream?streams={key}", "/stream?streams={key}")
        self.events = 0
        # Sin timeout de inactividad: la cuenta puede pasar horas sin eventos y
        # el heartbeat del WebSocket ya detecta conexiones muertas.
        self.conn = StreamConnection("user", self._url, self._handle, on_connect=on_connect, idle_timeout=None)

    async def _url(self) -> str:
        try:
            if not self._listen_key or time.time() - self._key_ts > 50 * 60:
                self._listen_key = await self.ws_api.start_user_stream()
                self._key_ts = time.time()
                log.info("User Data Stream: listenKey obtenido por WS API")
        except Exception as exc:
            self.conn.last_error = f"listenKey: {exc}"
            log.error("User Data Stream: no se pudo obtener listenKey: %s", exc)
            return ""
        # Si la ruta /private falla repetidamente se prueba la ruta clásica.
        if self.conn.consecutive_failures >= 3:
            self.conn.consecutive_failures = 0
            self._path_index = (self._path_index + 1) % len(self._paths)
            log.warning("User Data Stream: probando ruta alternativa %s", self._paths[self._path_index].split("?")[0])
        return self.base + self._paths[self._path_index].format(key=self._listen_key)

    def start(self) -> None:
        self.conn.start()
        if self._keepalive_task is None:
            self._keepalive_task = asyncio.create_task(self._keepalive(), name="user-stream-keepalive")

    async def stop(self) -> None:
        if self._keepalive_task is not None:
            self._keepalive_task.cancel()
        await self.conn.stop()

    async def _keepalive(self) -> None:
        while True:
            await asyncio.sleep(self.KEEPALIVE_S)
            if not self._listen_key:
                continue
            try:
                await self.ws_api.ping_user_stream()
                self._key_ts = time.time()
            except Exception as exc:
                log.warning("User Data Stream: keepalive falló (%s); se renovará el listenKey", exc)
                self._listen_key = ""
                await self.conn.reconnect()

    async def _handle(self, payload) -> None:
        if not isinstance(payload, dict):
            return
        self.events += 1
        if payload.get("E"):
            self.clock.observe(payload["E"])
        if payload.get("e") == "listenKeyExpired":
            log.warning("User Data Stream: listenKey expirado; renovando")
            self._listen_key = ""
            await self.conn.reconnect()
            return
        await self._on_event(payload)

    def stats(self) -> dict:
        data = self.conn.stats()
        data["events"] = self.events
        return data
