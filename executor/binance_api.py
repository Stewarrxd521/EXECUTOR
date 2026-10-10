"""Clientes de Binance USDⓈ-M Futures.

* ``BinanceWsApi``: WebSocket API (``ws-fapi``) — órdenes, cancelaciones,
  algo orders (TP/SL), consultas de cuenta y listenKey del User Data Stream.
  Una sola conexión persistente, peticiones concurrentes correlacionadas por
  ``id``, reconexión automática y medición de latencia.
* ``RestClient``: REST mínimo para lo que Binance NO ofrece por WebSocket
  (leverage, tipo de margen, modo de posición, margen aislado) y para el
  bootstrap del exchangeInfo. Rota entre ``PROXY_URLS`` con control de baneo
  por IP.
* ``ServerClock``: estima el desfase con el servidor a partir de la hora de
  los eventos del stream (sin llamar a ``/time``).
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import itertools
import json
import logging
import time
from collections import Counter, deque
from decimal import Decimal
from typing import Any, Optional
from urllib.parse import quote

import aiohttp
from yarl import URL

from .errors import BinanceAPIError
from .precision import fmt

log = logging.getLogger("executor.binance")

ORDER_METHODS = {"order.place", "algoOrder.place", "order.modify"}


class ServerClock:
    """Desfase reloj local ↔ Binance estimado con los eventos del stream.

    ``E - ahora`` = desfase − latencia; el máximo de las muestras recientes es
    la mejor cota del desfase real y deja el timestamp levemente *por detrás*
    del servidor, que es el lado seguro para ``recvWindow``.
    """

    def __init__(self, window: int = 120):
        self._samples: deque[float] = deque(maxlen=window)
        self._offset = 0.0
        self._penalty = 0.0

    def observe(self, server_ms: float) -> None:
        if not server_ms:
            return
        self._samples.append(float(server_ms) - time.time() * 1000)
        self._offset = max(self._samples)

    def now_ms(self) -> int:
        return int(time.time() * 1000 + self._offset - self._penalty)

    def on_timestamp_error(self, msg: str) -> None:
        """-1021: si íbamos adelantados se retrocede; si atrasados se reinicia."""
        if "ahead" in (msg or "").lower():
            self._penalty = min(self._penalty + 1000, 10_000)
        else:
            self._samples.clear()
            self._offset = 0.0
            self._penalty = 0.0

    @property
    def offset_ms(self) -> float:
        return round(self._offset - self._penalty, 1)


def _wire_value(value: Any) -> Any:
    """Normaliza valores para Binance: decimales y bools como texto exacto."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (Decimal, float)):
        return fmt(value)
    return value


def _clean(params: Optional[dict]) -> dict:
    return {k: _wire_value(v) for k, v in (params or {}).items() if v is not None}


class BinanceWsApi:
    def __init__(self, url: str, api_key: str, api_secret: str, clock: ServerClock,
                 recv_window_ms: int = 5000, name: str = "ws-api"):
        self.url = url
        self.api_key = api_key
        self._secret = api_secret.encode()
        self.clock = clock
        self.recv_window_ms = recv_window_ms
        self.name = name

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future] = {}
        self._ids = itertools.count(1)
        self._closed = False
        self._banned_until = 0.0

        self.connected = False
        self.connected_since = 0.0
        self.reconnects = 0
        self.requests = 0
        self.errors = 0
        self.last_error = ""
        self.latency_ms = 0.0
        self.rate_limits: dict[str, dict] = {}
        self.method_counts: Counter = Counter()

    # ── Conexión ──────────────────────────────────────────────────────────
    def _alive(self) -> bool:
        return bool(self._ws and not self._ws.closed and self._reader_task and not self._reader_task.done())

    async def connect(self) -> None:
        if self._alive():
            return
        async with self._connect_lock:
            if self._alive():
                return
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None, sock_connect=10))
            if self._ws is not None and not self._ws.closed:
                await self._ws.close()
            log.info("Conectando %s → %s", self.name, self.url)
            self._ws = await self._session.ws_connect(self.url, heartbeat=25, autoping=True, max_msg_size=0)
            if self.connected_since:
                self.reconnects += 1
            self.connected = True
            self.connected_since = time.time()
            self._reader_task = asyncio.create_task(self._reader(self._ws), name=f"{self.name}-reader")

    async def close(self) -> None:
        self._closed = True
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()
        if self._reader_task is not None:
            self._reader_task.cancel()
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self.connected = False

    async def _reader(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                    except ValueError:
                        log.warning("%s: mensaje no JSON: %r", self.name, msg.data[:200])
                        continue
                    fut = self._pending.pop(str(data.get("id")), None)
                    if fut is not None and not fut.done():
                        fut.set_result(data)
                elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                    break
        except Exception as exc:  # pragma: no cover - errores de red variables
            self.last_error = f"reader: {exc!r}"
        finally:
            self.connected = False
            err = ConnectionError(f"{self.name} desconectado")
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.set_exception(err)
            self._pending.clear()
            if not self._closed:
                log.warning("%s: conexión cerrada; se reconectará en la próxima petición", self.name)

    # ── Firma ─────────────────────────────────────────────────────────────
    def _sign(self, params: dict) -> str:
        payload = "&".join(f"{k}={params[k]}" for k in sorted(params) if k != "signature")
        return hmac.new(self._secret, payload.encode(), hashlib.sha256).hexdigest()

    def _prepare(self, params: Optional[dict], signed: bool, with_api_key: bool) -> dict:
        out = _clean(params)
        if signed or with_api_key:
            out["apiKey"] = self.api_key
        if signed:
            out["timestamp"] = self.clock.now_ms()
            out.setdefault("recvWindow", self.recv_window_ms)
            out["signature"] = self._sign(out)
        return out

    # ── Petición ──────────────────────────────────────────────────────────
    async def request(self, method: str, params: Optional[dict] = None, *, signed: bool = False,
                      with_api_key: bool = False, timeout: float = 10.0) -> Any:
        if self._banned_until > time.time():
            raise BinanceAPIError(-1003, f"WS API en pausa por rate limit ({self._banned_until - time.time():.0f}s)",
                                  method=method, retry_after_s=self._banned_until - time.time())

        transient_attempts = 0 if method in ORDER_METHODS else 2
        timestamp_retry = True
        send_retry = True
        while True:
            try:
                await self.connect()
            except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as exc:
                # Nada se envió: es seguro reintentar (OrderExecutor reintenta el código -1).
                self.errors += 1
                raise BinanceAPIError(-1, f"{self.name}: sin conexión: {exc!r}", method=method) from exc
            payload_params = self._prepare(params, signed, with_api_key)
            req_id = f"{next(self._ids)}"
            payload: dict = {"id": req_id, "method": method}
            if payload_params:
                payload["params"] = payload_params
            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            self._pending[req_id] = fut
            started = time.perf_counter()
            try:
                assert self._ws is not None
                await self._ws.send_str(json.dumps(payload, separators=(",", ":")))
            except Exception as exc:
                self._pending.pop(req_id, None)
                if send_retry:
                    send_retry = False
                    log.warning("%s: fallo enviando %s (%r); reconectando", self.name, method, exc)
                    continue
                raise BinanceAPIError(-1, f"no se pudo enviar {method}: {exc!r}", method=method) from exc

            self.requests += 1
            self.method_counts[method] += 1
            try:
                response = await asyncio.wait_for(fut, timeout)
            except (asyncio.TimeoutError, ConnectionError) as exc:
                self._pending.pop(req_id, None)
                self.errors += 1
                if method in ORDER_METHODS:
                    # Enviada pero sin confirmación: el llamador debe consultar el estado.
                    raise BinanceAPIError(-1007, f"sin respuesta de {method}: {exc!r}", method=method) from exc
                if transient_attempts > 0:
                    transient_attempts -= 1
                    continue
                raise BinanceAPIError(-1, f"sin respuesta de {method}: {exc!r}", method=method) from exc

            elapsed = (time.perf_counter() - started) * 1000
            self.latency_ms = elapsed if not self.latency_ms else self.latency_ms * 0.8 + elapsed * 0.2
            self._track_limits(response.get("rateLimits"))

            status = int(response.get("status") or 0)
            if status == 200:
                return response.get("result")

            error = response.get("error") or {}
            err = BinanceAPIError(int(error.get("code") or 0), error.get("msg", ""), http_status=status,
                                  method=method, transport="ws", params=payload_params)
            self.errors += 1
            self.last_error = str(err)
            if err.code == -1021 and timestamp_retry:
                timestamp_retry = False
                self.clock.on_timestamp_error(err.msg)
                continue
            if err.code == -1003 or status in (418, 429):
                self._banned_until = time.time() + max(err.retry_after_s, 10.0)
            if err.code in (-1000, -1001, -1008) and transient_attempts > 0:
                transient_attempts -= 1
                await asyncio.sleep(0.4)
                continue
            raise err

    def _track_limits(self, limits) -> None:
        for lim in limits or []:
            key = f"{lim.get('rateLimitType')}:{lim.get('intervalNum')}{lim.get('interval', '')[:1]}"
            self.rate_limits[key] = {"count": lim.get("count"), "limit": lim.get("limit")}

    def stats(self) -> dict:
        return {
            "connected": self._alive(),
            "url": self.url,
            "latency_ms": round(self.latency_ms, 1),
            "requests": self.requests,
            "errors": self.errors,
            "reconnects": self.reconnects,
            "last_error": self.last_error,
            "rate_limits": self.rate_limits,
            "uptime_s": int(time.time() - self.connected_since) if self._alive() else 0,
        }

    # ── Métodos ───────────────────────────────────────────────────────────
    async def place_order(self, **params) -> dict:
        params.setdefault("newOrderRespType", "RESULT")
        return await self.request("order.place", params, signed=True)

    async def cancel_order(self, symbol: str, order_id=None, client_id: Optional[str] = None) -> dict:
        return await self.request("order.cancel", {"symbol": symbol, "orderId": order_id,
                                                   "origClientOrderId": client_id if order_id is None else None},
                                  signed=True)

    async def modify_order(self, **params) -> dict:
        return await self.request("order.modify", params, signed=True)

    async def order_status(self, symbol: str, order_id=None, client_id: Optional[str] = None) -> dict:
        return await self.request("order.status", {"symbol": symbol, "orderId": order_id,
                                                   "origClientOrderId": client_id if order_id is None else None},
                                  signed=True)

    async def place_algo(self, **params) -> dict:
        params.setdefault("algoType", "CONDITIONAL")
        return await self.request("algoOrder.place", params, signed=True)

    async def cancel_algo(self, algo_id=None, client_algo_id: Optional[str] = None) -> dict:
        return await self.request("algoOrder.cancel", {"algoId": algo_id,
                                                       "clientAlgoId": client_algo_id if algo_id is None else None},
                                  signed=True)

    async def positions(self, symbol: Optional[str] = None) -> list:
        """account.position (v1): incluye leverage y positionSide de TODOS los símbolos."""
        result = await self.request("account.position", {"symbol": symbol}, signed=True)
        return result if isinstance(result, list) else []

    async def balances(self) -> list:
        result = await self.request("v2/account.balance", signed=True)
        return result if isinstance(result, list) else []

    async def account_status(self) -> dict:
        result = await self.request("v2/account.status", signed=True)
        return result if isinstance(result, dict) else {}

    async def ticker_price(self, symbol: str) -> float:
        result = await self.request("ticker.price", {"symbol": symbol})
        try:
            return float((result or {}).get("price", 0))
        except (TypeError, ValueError, AttributeError):
            return 0.0

    async def start_user_stream(self) -> str:
        result = await self.request("userDataStream.start", with_api_key=True)
        return (result or {}).get("listenKey", "")

    async def ping_user_stream(self) -> None:
        await self.request("userDataStream.ping", with_api_key=True)

    async def stop_user_stream(self) -> None:
        await self.request("userDataStream.stop", with_api_key=True)


class RestClient:
    """REST mínimo (solo endpoints sin equivalente WebSocket)."""

    def __init__(self, base_url: str, api_key: str, api_secret: str, clock: ServerClock,
                 proxies: list[str], recv_window_ms: int = 5000, proxy_all: bool = False):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._secret = api_secret.encode()
        self.clock = clock
        self.proxies = list(proxies)
        self.recv_window_ms = recv_window_ms
        self.proxy_all = proxy_all
        self._session: Optional[aiohttp.ClientSession] = None
        self._ban_until: dict[str, float] = {}
        self.calls: Counter = Counter()
        self.last_error = ""
        self.last_route = ""

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        return self._session

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    @staticmethod
    def proxy_label(proxy: Optional[str]) -> str:
        return proxy.split("@", 1)[-1] if proxy else "directo"

    def _routes(self, route: str) -> list[Optional[str]]:
        now = time.time()
        proxies = [p for p in self.proxies if self._ban_until.get(p, 0) <= now]
        direct_ok = self._ban_until.get("", 0) <= now
        if route == "proxy" or (route == "auto" and self.proxy_all):
            routes: list[Optional[str]] = list(proxies) or ([None] if not self.proxies and direct_ok else [])
        else:
            routes = ([None] if direct_ok else []) + list(proxies)
        if not routes:
            soonest = min([self._ban_until.get(p, 0) for p in self.proxies] + [self._ban_until.get("", 0)]) - now
            raise BinanceAPIError(-1003, f"todas las IPs REST están en pausa por rate limit (~{max(soonest, 0):.0f}s)",
                                  transport="rest", retry_after_s=max(soonest, 0))
        return routes

    def _query(self, params: dict, signed: bool) -> str:
        clean = _clean(params)
        if signed:
            clean["timestamp"] = self.clock.now_ms()
            clean.setdefault("recvWindow", self.recv_window_ms)
        # Se codifica primero y se firma EXACTAMENTE lo que viaja (símbolos no ASCII).
        query = "&".join(f"{k}={quote(str(clean[k]), safe='')}" for k in sorted(clean))
        if signed:
            sig = hmac.new(self._secret, query.encode(), hashlib.sha256).hexdigest()
            query = f"{query}&signature={sig}" if query else f"signature={sig}"
        return query

    async def request(self, http_method: str, path: str, params: Optional[dict] = None, *,
                      signed: bool = False, route: str = "auto", timeout: float = 10.0) -> Any:
        session = await self._get_session()
        last_exc: Optional[Exception] = None
        for proxy in self._routes(route):
            query = self._query(params or {}, signed)
            url = URL(f"{self.base_url}{path}" + (f"?{query}" if query else ""), encoded=True)
            headers = {"X-MBX-APIKEY": self.api_key} if self.api_key else {}
            self.calls[path] += 1
            self.last_route = self.proxy_label(proxy)
            try:
                async with session.request(http_method, url, headers=headers, proxy=proxy,
                                           timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
                    text = await resp.text()
                    if resp.status == 200:
                        try:
                            return json.loads(text) if text else {}
                        except ValueError:
                            return {"raw": text}
                    err = self._to_error(resp.status, text, path, resp.headers.get("Retry-After"))
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = BinanceAPIError(-1, f"{self.proxy_label(proxy)}: {exc!r}", method=path, transport="rest")
                log.warning("REST %s %s vía %s falló: %r", http_method, path, self.proxy_label(proxy), exc)
                continue

            self.last_error = str(err)
            last_exc = err
            if err.code == -1003 or err.http_status in (418, 429):
                self._ban_until[proxy or ""] = time.time() + max(err.retry_after_s, 30.0)
                log.error("REST: IP %s en pausa %.0fs por rate limit", self.proxy_label(proxy), max(err.retry_after_s, 30.0))
                continue
            if err.http_status in (403, 451) or err.code in (-2015, -1011):
                log.warning("REST %s vía %s rechazado (%s); probando la siguiente ruta", path, self.proxy_label(proxy), err)
                continue
            raise err
        raise last_exc or BinanceAPIError(-1, "sin rutas REST disponibles", method=path, transport="rest")

    @staticmethod
    def _to_error(status: int, text: str, path: str, retry_after: Optional[str]) -> BinanceAPIError:
        code, msg = 0, text[:300]
        try:
            data = json.loads(text)
            code, msg = int(data.get("code") or 0), str(data.get("msg") or text[:300])
        except (ValueError, TypeError, AttributeError):
            pass
        try:
            wait = float(retry_after) if retry_after else 0.0
        except ValueError:
            wait = 0.0
        return BinanceAPIError(code, msg, http_status=status, method=path, transport="rest", retry_after_s=wait)

    def stats(self) -> dict:
        return {
            "calls": dict(self.calls),
            "total": sum(self.calls.values()),
            "last_error": self.last_error,
            "last_route": self.last_route,
            "proxies": [self.proxy_label(p) for p in self.proxies],
            "banned": {self.proxy_label(k or None): round(v - time.time()) for k, v in self._ban_until.items() if v > time.time()},
        }

    # ── Endpoints sin WebSocket ───────────────────────────────────────────
    async def set_leverage(self, symbol: str, leverage: int) -> dict:
        return await self.request("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": int(leverage)},
                                  signed=True, route="proxy")

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict:
        return await self.request("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": margin_type}, signed=True)

    async def get_position_mode(self) -> bool:
        data = await self.request("GET", "/fapi/v1/positionSide/dual", {}, signed=True)
        return bool(data.get("dualSidePosition"))

    async def set_position_mode(self, hedge: bool) -> dict:
        return await self.request("POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": hedge}, signed=True)

    async def modify_margin(self, symbol: str, amount, position_side: str, add: bool) -> dict:
        return await self.request("POST", "/fapi/v1/positionMargin",
                                  {"symbol": symbol, "amount": amount, "type": 1 if add else 2,
                                   "positionSide": position_side}, signed=True)

    async def open_orders(self) -> list:
        data = await self.request("GET", "/fapi/v1/openOrders", {}, signed=True)
        return data if isinstance(data, list) else []

    async def open_algo_orders(self) -> list:
        data = await self.request("GET", "/fapi/v1/openAlgoOrders", {}, signed=True)
        if isinstance(data, dict):
            return data.get("orders") or data.get("algoOrders") or []
        return data if isinstance(data, list) else []

    async def exchange_info(self) -> dict:
        return await self.request("GET", "/fapi/v1/exchangeInfo", route="proxy" if self.proxies else "direct", timeout=20)

    async def leverage_brackets(self) -> list:
        data = await self.request("GET", "/fapi/v1/leverageBracket", {}, signed=True, timeout=20)
        return data if isinstance(data, list) else []
