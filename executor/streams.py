"""Streams WebSocket de Binance: mercado y datos de usuario.

* ``MarketData``: una sola conexión de mercado.

  - Modo ``lite`` (por defecto, ``MARKET_STREAMS=lite``): solo los símbolos que
    hacen falta (posiciones, trades abiertos, grids y el símbolo que se mira en
    el dashboard) con ``<símbolo>@markPrice@1s`` / ``<símbolo>@miniTicker``,
    suscritos y desuscritos en caliente (SUBSCRIBE/UNSUBSCRIBE), más
    ``!contractInfo``. La lista completa de mercados (``!miniTicker@arr``) se pide
    solo como una foto puntual mientras un dashboard muestra la lista. Así el
    executor no recibe ~150-200 KB/s del mercado entero las 24 h: en Render cada
    paquete recibido genera tráfico de salida (ACK TCP/TLS) que se factura como
    ancho de banda «Service-Initiated».
  - Modo ``full``: el comportamiento anterior (``!markPrice@arr@1s`` y
    ``!miniTicker@arr`` de todos los símbolos).

* ``UserDataStream``: listenKey obtenido por la WS API (``userDataStream.start``)
  y renovado por WS; recibe ``ORDER_TRADE_UPDATE``, ``ACCOUNT_UPDATE``,
  ``ALGO_UPDATE``, ``ACCOUNT_CONFIG_UPDATE``... en tiempo real.

Las conexiones piden compresión ``permessage-deflate`` (si Binance la acepta,
llegan menos bytes) y cuentan los bytes enviados/recibidos (``stats()``).
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from collections import deque
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
        self.bytes_in = 0          # carga útil recibida (texto de los mensajes)
        self.bytes_out = 0         # carga útil enviada (SUBSCRIBE, etc.)
        self.frames_out = 0
        self.compressed = False    # Binance aceptó permessage-deflate
        self.started_ts = time.time()

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

    async def send_json(self, data: dict) -> bool:
        """Envía un mensaje por la conexión activa (False si no está conectada)."""
        ws = self._ws
        if ws is None or ws.closed or not self.connected:
            return False
        text = json.dumps(data, separators=(",", ":"))
        try:
            await ws.send_str(text)
        except (ConnectionError, RuntimeError) as exc:
            log.warning("Stream %s: no se pudo enviar %s (%r)", self.name, data.get("method"), exc)
            return False
        self.bytes_out += len(text)
        self.frames_out += 1
        return True

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
                                                    compress=15) as ws:
                    self._ws = ws
                    self.compressed = bool(getattr(ws, "compress", 0))
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
                            self.bytes_in += len(msg.data)
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

    def traffic(self) -> dict:
        hours = max((time.time() - self.started_ts) / 3600, 1 / 3600)
        return {"bytes_in": self.bytes_in, "bytes_out": self.bytes_out,
                "in_mb_h": round(self.bytes_in / 1e6 / hours, 3), "out_mb_h": round(self.bytes_out / 1e6 / hours, 3),
                "compressed": self.compressed}

    def stats(self) -> dict:
        return {
            "connected": self.connected,
            "messages": self.messages,
            "reconnects": self.reconnects,
            "last_message_age_s": round(time.time() - self.last_message_ts, 1) if self.last_message_ts else None,
            "last_error": self.last_error,
            "uptime_s": int(time.time() - self.connected_since) if self.connected else 0,
            "traffic": self.traffic(),
        }


class MarketData:
    """Precios de mercado por un único WebSocket (modo ``lite`` o ``full``)."""

    FULL_STREAMS = ("!markPrice@arr@1s", "!miniTicker@arr", "!contractInfo")
    BASE_STREAMS = ("!contractInfo",)
    ALL_TICKERS = "!miniTicker@arr"
    ALL_MARKS_3S = "!markPrice@arr"   # todo el mercado cada 3 s: más barato que muchos streams sueltos
    # Con más símbolos que esto se usa ALL_MARKS_3S (≈150 B × ~700 símbolos cada 3 s): por debajo,
    # los streams sueltos (≈190 B por símbolo) son más baratos. También evita el límite de 1024 streams.
    MANY_SYMBOLS = 300
    MAX_FRAMES_PER_S = 4        # Binance admite 10 mensajes/s por conexión: margen de sobra
    SNAPSHOT_ARRAYS = 3         # !miniTicker@arr solo trae lo que cambió en el último segundo
    WATCHDOG_S = 20.0           # sin datos de precio en 20 s con streams activos → reconexión
    SNAPSHOT_EVERY_S = 25.0     # foto de todos los mercados como mucho cada 25 s (solo con dashboard)
    UNSUBSCRIBE_GRACE_S = 60.0  # un símbolo que deja de hacer falta se mantiene 60 s (evita altas/bajas)
    STALE_S = 60.0              # (lite) un precio sin actualizar en 60 s no se usa para operar

    def __init__(self, stream_base_url: str, clock: ServerClock,
                 on_contract_info: Optional[Callable[[dict], None]] = None, mode: str = "lite"):
        self.base = stream_base_url.rstrip("/")
        self.clock = clock
        self.mode = "full" if str(mode).lower() == "full" else "lite"
        self.marks: dict[str, MarkInfo] = {}
        self.tickers: dict[str, MiniTicker] = {}
        self._on_contract_info = on_contract_info
        self._tick_listeners: list[Callable[[], None]] = []
        # Suscripciones dinámicas (modo lite)
        self._wanted: dict[str, float] = {}       # stream -> última vez que hizo falta
        self._active: set[str] = set()            # streams activos en la conexión actual
        self._dynamic = True                      # False si Binance rechaza SUBSCRIBE: se reconecta con la URL nueva
        self._req_id = 0
        self._url_streams: set[str] = set()
        self._pending: dict[int, tuple[str, list]] = {}
        self._snapshot_pending = False
        self._snapshot_asked = 0.0
        self._snapshot_arrays = 0
        self._snapshot_task: Optional[asyncio.Task] = None
        self._sent_ts: deque = deque(maxlen=32)
        self._last_price_ts = 0.0
        self.all_tickers_ts = 0.0
        self._lock = asyncio.Lock()
        self.conn = StreamConnection("market", self._url, self._handle, on_connect=self._on_connect,
                                     idle_timeout=60.0 if self.mode == "full" else None)

    # ── Streams deseados ──────────────────────────────────────────────────
    def _desired(self) -> set[str]:
        if self.mode == "full":
            return set(self.FULL_STREAMS)
        now = time.time()
        keep = {st for st, ts in self._wanted.items() if now - ts < self.UNSUBSCRIBE_GRACE_S}
        self._wanted = {st: ts for st, ts in self._wanted.items() if st in keep}
        fast = {st.split("@")[0] for st in keep if st.endswith("@markPrice@1s")}
        # Un símbolo con stream de 1 s no necesita además el de 3 s.
        keep = {st for st in keep if not (st.endswith("@markPrice") and st.split("@")[0] in fast)}
        slow = [st for st in keep if st.endswith("@markPrice")]
        if len(slow) > self.MANY_SYMBOLS:
            keep = (keep - set(slow)) | {self.ALL_MARKS_3S}
        return keep | set(self.BASE_STREAMS)

    _SYMBOL_RE = re.compile(r"\w{2,30}")  # letras/dígitos (también no ASCII): nada que rompa la URL

    @classmethod
    def valid_symbol(cls, symbol: str) -> bool:
        return bool(symbol) and bool(cls._SYMBOL_RE.fullmatch(symbol))

    def want(self, symbols, fast_symbols=(), ticker_symbols=()) -> None:
        """Marca los símbolos que necesitan precio (lo llama el servicio cada segundo).

        * ``symbols`` (posiciones, trades): mark price cada 3 s (``<s>@markPrice``).
        * ``fast_symbols`` (grids): cada 1 s (``<s>@markPrice@1s``).
        * ``ticker_symbols`` (lo que se mira en el dashboard): 1 s + estadísticas 24 h.
        """
        now = time.time()
        for sym in symbols:
            if self.valid_symbol(sym):
                self._wanted[f"{sym.lower()}@markPrice"] = now
        for sym in fast_symbols:
            if self.valid_symbol(sym):
                self._wanted[f"{sym.lower()}@markPrice@1s"] = now
        for sym in ticker_symbols:
            if self.valid_symbol(sym):
                self._wanted[f"{sym.lower()}@markPrice@1s"] = now
                self._wanted[f"{sym.lower()}@miniTicker"] = now

    async def _url(self) -> str:
        streams = sorted(self._desired()) if self.mode == "lite" else list(self.FULL_STREAMS)
        self._url_streams = set(streams)
        return f"{self.base}/market/stream?streams={'/'.join(streams)}"

    async def _on_connect(self) -> None:
        # Activos = exactamente los streams que iban en la URL de esta conexión.
        self._active = set(self._url_streams) if self.mode == "lite" else set(self.FULL_STREAMS)
        self._pending.clear()
        self._snapshot_pending = False
        self._last_price_ts = time.time()

    def _frame_budget(self) -> int:
        now = time.time()
        return self.MAX_FRAMES_PER_S - sum(1 for t in self._sent_ts if now - t < 1.0)

    async def _watchdog(self) -> None:
        """Con streams de precio activos, si no llega nada en WATCHDOG_S se reconecta."""
        if not any("@markPrice" in st or st.startswith("!markPrice") for st in self._active):
            return
        quiet = time.time() - max(self._last_price_ts, self.conn.connected_since)
        if quiet > self.WATCHDOG_S:
            log.warning("Mercado: %.0f s sin precios con %d stream(s) activos; reconectando", quiet,
                        len(self._active))
            self._last_price_ts = time.time()
            await self.conn.reconnect()

    async def sync_subscriptions(self) -> None:
        """Alinea los streams activos con los deseados (SUBSCRIBE/UNSUBSCRIBE en caliente).

        Envía como mucho MAX_FRAMES_PER_S mensajes por segundo; lo pendiente se
        completa en la siguiente llamada (el servicio la hace cada segundo).
        """
        if self.mode != "lite" or not self.conn.connected:
            return
        async with self._lock:
            await self._watchdog()
            if not self.conn.connected:
                return
            desired = self._desired()
            if self._snapshot_pending:
                if self._dynamic:
                    desired.add(self.ALL_TICKERS)
                elif self._snapshot_task is None or self._snapshot_task.done():
                    # Sin SUBSCRIBE: la foto va por una conexión aparte, de un solo uso.
                    self._snapshot_task = asyncio.create_task(self._snapshot_once())
            add = sorted(desired - self._active)
            remove = sorted(self._active - desired)
            if not add and not remove:
                return
            if not self._dynamic:
                log.info("Mercado: %d stream(s) nuevos / %d menos; se reconecta con la lista nueva",
                         len(add), len(remove))
                await self.conn.reconnect()
                return
            for method, streams in (("SUBSCRIBE", add), ("UNSUBSCRIBE", remove)):
                for i in range(0, len(streams), 100):
                    if self._frame_budget() <= 0:
                        return  # resto en el próximo segundo
                    chunk = streams[i:i + 100]
                    self._req_id += 1
                    self._sent_ts.append(time.time())
                    if await self.conn.send_json({"method": method, "params": chunk, "id": self._req_id}):
                        self._pending[self._req_id] = (method, chunk)
                        if method == "SUBSCRIBE":
                            self._active.update(chunk)
                        else:
                            self._active.difference_update(chunk)
                            self._forget(chunk)

    def _forget(self, removed: list[str]) -> None:
        """Al dejar de recibir el mark price de un símbolo se descarta (no se opera con un precio viejo)."""
        if self.ALL_MARKS_3S in removed:
            covered = {st.split("@")[0].upper() for st in self._active if "@markPrice" in st}
            for sym in [s for s in self.marks if s not in covered]:
                del self.marks[sym]
            return
        for st in removed:
            sym, _, kind = st.partition("@")
            if kind.startswith("markPrice") and not any(a.startswith(sym + "@markPrice") for a in self._active):
                if self.ALL_MARKS_3S not in self._active:
                    self.marks.pop(sym.upper(), None)

    async def _snapshot_once(self) -> None:
        """Foto de todos los mercados por una conexión de un solo uso (si SUBSCRIBE no funciona)."""
        url = f"{self.base}/market/stream?streams={self.ALL_TICKERS}"
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)) as session:
                async with session.ws_connect(url, compress=15, max_msg_size=0) as ws:
                    arrays, deadline = 0, time.time() + 6
                    while arrays < self.SNAPSHOT_ARRAYS and time.time() < deadline:
                        msg = await ws.receive(timeout=max(0.1, deadline - time.time()))
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            break
                        self.conn.bytes_in += len(msg.data)
                        data = json.loads(msg.data)
                        payload = data.get("data", data) if isinstance(data, dict) else data
                        if isinstance(payload, list):
                            arrays += 1
                            for item in payload:
                                self._set_ticker(item, time.time())
        except Exception as exc:
            log.warning("Mercado: foto de mercados no disponible (%r)", exc)
        finally:
            self.all_tickers_ts = time.time()
            self._snapshot_pending = False

    def request_all_tickers(self) -> None:
        """Pide una foto de todos los mercados (lista del dashboard). En ``full`` ya llegan solos."""
        if self.mode != "lite":
            return
        now = time.time()
        if now - self.all_tickers_ts < self.SNAPSHOT_EVERY_S:
            return
        if self._snapshot_pending and now - self._snapshot_asked < 10:
            return
        self._snapshot_pending = True
        self._snapshot_asked = now
        self._snapshot_arrays = 0

    def on_tick(self, callback: Callable[[], None]) -> None:
        """Callback tras cada actualización de mark price."""
        self._tick_listeners.append(callback)

    def start(self) -> None:
        self.conn.start()

    async def stop(self) -> None:
        if self._snapshot_task is not None:
            self._snapshot_task.cancel()
        await self.conn.stop()

    def seed(self, symbol: str, price: float) -> None:
        """Precio obtenido por otra vía (WS API) mientras llega el stream del símbolo."""
        if price > 0:
            old = self.marks.get(symbol.upper())
            self.marks[symbol.upper()] = MarkInfo(price, old.index if old else 0.0, old.funding_rate if old else 0.0,
                                                  old.next_funding_ms if old else 0, time.time())

    # ── Mensajes ──────────────────────────────────────────────────────────
    def _notify_tick(self) -> None:
        for cb in self._tick_listeners:
            try:
                cb()
            except Exception:
                log.exception("MarketData: listener de tick falló")

    def _set_mark(self, item: dict, now: float) -> None:
        sym = item.get("s")
        price = safe_float(item.get("p"))
        self._last_price_ts = now
        if sym and price > 0:
            self.marks[sym] = MarkInfo(price, safe_float(item.get("i")), safe_float(item.get("r")),
                                       int(item.get("T") or 0), now)

    def _set_ticker(self, item: dict, now: float) -> None:
        sym = item.get("s")
        if sym:
            self.tickers[sym] = MiniTicker(
                safe_float(item.get("c")), safe_float(item.get("o")), safe_float(item.get("h")),
                safe_float(item.get("l")), safe_float(item.get("v")), safe_float(item.get("q")), now,
            )

    def _handle(self, payload) -> Optional[Awaitable[None]]:
        now = time.time()
        if isinstance(payload, list):
            if not payload:
                return None
            kind = payload[0].get("e")
            if kind == "markPriceUpdate":
                self.clock.observe(payload[0].get("E", 0))
                for item in payload:
                    self._set_mark(item, now)
                self._notify_tick()
            elif kind == "24hrMiniTicker":
                for item in payload:
                    self._set_ticker(item, now)
                if self.mode == "full":
                    self.all_tickers_ts = now
                elif self._snapshot_pending:
                    # !miniTicker@arr solo trae lo que cambió en el último segundo: se toman
                    # SNAPSHOT_ARRAYS envíos y se deja de recibir el mercado entero.
                    self._snapshot_arrays += 1
                    if self._snapshot_arrays >= self.SNAPSHOT_ARRAYS:
                        self._snapshot_pending = False
                        self.all_tickers_ts = now
                        return self.sync_subscriptions()
            return None
        if not isinstance(payload, dict):
            return None
        kind = payload.get("e")
        if kind == "markPriceUpdate":
            self.clock.observe(payload.get("E", 0))
            self._set_mark(payload, now)
            self._notify_tick()
        elif kind == "24hrMiniTicker":
            self._last_price_ts = now
            self._set_ticker(payload, now)
        elif kind == "contractInfo":
            if self._on_contract_info is not None:
                self._on_contract_info(payload)
        elif "id" in payload and ("result" in payload or "error" in payload):
            req = self._pending.pop(payload.get("id"), None)
            if payload.get("error"):
                log.warning("Mercado: Binance rechazó %s %s (%s); se usará reconexión con la lista nueva",
                            req[0] if req else "?", req[1][:3] if req else "", payload.get("error"))
                self._dynamic = False
                return self.conn.reconnect()
        return None

    # ── Consultas ─────────────────────────────────────────────────────────
    def mark(self, symbol: str, max_age_s: Optional[float] = None) -> float:
        info = self.marks.get(symbol.upper())
        if max_age_s is None and self.mode == "lite":
            max_age_s = self.STALE_S
        if info is None or (max_age_s is not None and time.time() - info.ts > max_age_s):
            return 0.0
        return info.mark

    def last(self, symbol: str) -> float:
        t = self.tickers.get(symbol.upper())
        if t is None or (self.mode == "lite" and time.time() - t.ts > self.STALE_S):
            return 0.0
        return t.last

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
        data.update({"symbols": len(self.marks), "tickers": len(self.tickers), "mode": self.mode,
                     "streams": len(self._active), "dynamic_subscribe": self._dynamic,
                     "all_tickers_age_s": round(time.time() - self.all_tickers_ts) if self.all_tickers_ts else None})
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
