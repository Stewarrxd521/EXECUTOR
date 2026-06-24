"""
futures_executor_ws.py — Executor de Futuros Binance 100% WebSocket para trading

- Órdenes de apertura/cierre por Binance USDⓈ-M Futures WebSocket API.
- Consultas de balance y posiciones por WebSocket API.
- Precios de mercado desde SymbolWebSocketPriceCache (ws.py / WS.py).
"""

import asyncio
import aiohttp
from aiohttp import web
import logging
import math
import re
from datetime import datetime, timezone
import os
import json
import uuid
import hmac
import hashlib
from dataclasses import dataclass
from typing import Optional

# ══════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════
BINANCE_API_KEY    = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
USE_TESTNET        = os.environ.get("USE_TESTNET", "false").lower() == "true"
SIGNAL_SECRET      = os.environ.get("SIGNAL_SECRET", "cambiar-por-secreto-seguro")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")

LEVERAGE        = int(os.environ.get("LEVERAGE", "4"))
HEDGE_MODE      = os.environ.get("HEDGE_MODE", "false").lower() == "true"
PORT            = int(os.environ.get("PORT", "10000"))
POSITION_POLL_S = int(os.environ.get("POSITION_POLL_S", "30"))

MIN_NOTIONAL_USDT = float(os.environ.get("MIN_NOTIONAL_USDT", "5.1"))
NOTIONAL_SAFETY_BUFFER_PCT = float(os.environ.get("NOTIONAL_SAFETY_BUFFER_PCT", "2.0"))

WS_API_URL = os.environ.get(
    "BINANCE_WS_FAPI_URL",
    "wss://testnet.binancefuture.com/ws-fapi/v1" if USE_TESTNET else "wss://ws-fapi.binance.com/ws-fapi/v1",
)

REST_FAPI_URL = os.environ.get(
    "BINANCE_REST_FAPI_URL",
    "https://testnet.binancefuture.com" if USE_TESTNET else "https://fapi.binance.com",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("Executor")


# ══════════════════════════════════════════════════════════
#  AJUSTE DE CANTIDAD / NOTIONAL MÍNIMO
# ══════════════════════════════════════════════════════════
def _step_decimals(step: float) -> int:
    """Cantidad de decimales implicada por un stepSize (p.ej. 0.001 -> 3)."""
    if step <= 0:
        return 8
    s = f"{step:.10f}".rstrip("0")
    if "." not in s:
        return 0
    return len(s.split(".")[1])


def floor_to_step(value: float, step: float) -> float:
    """Redondea `value` hacia abajo al múltiplo de `step` más cercano,
    evitando los errores típicos de coma flotante (0.1 + 0.2, etc.)."""
    if step <= 0:
        return value
    decimals = _step_decimals(step)
    units = math.floor(round(value / step, 8))
    return round(units * step, decimals)


def ceil_to_step(value: float, step: float) -> float:
    """Redondea `value` hacia arriba al múltiplo de `step` más cercano."""
    if step <= 0:
        return value
    decimals = _step_decimals(step)
    units = math.ceil(round(value / step, 8))
    return round(units * step, decimals)


def format_qty(value: float, step: float) -> str:
    """Formatea la cantidad con la cantidad de decimales del stepSize,
    sin notación científica ni decimales innecesarios. Para cantidades
    enteras usa round() en vez de int() truncado, para no perder una
    unidad por ruido de coma flotante (p.ej. 26.999999999 -> 26)."""
    decimals = _step_decimals(step)
    return f"{value:.{decimals}f}" if decimals > 0 else str(int(round(value)))


def resolve_safe_quantity(
    desired_notional: float,
    price: float,
    filters: dict,
    extra_buffer_pct: float = 0.0,
) -> tuple[float, float]:
    """
    Calcula la cantidad final a enviar a Binance a partir de un notional
    (tamaño de orden en USDT) objetivo y el precio de referencia, en vez
    de confiar ciegamente en la `quantity` que llega en la señal.

    - Convierte notional -> quantity con el precio más fresco disponible.
    - Redondea al stepSize (LOT_SIZE) del símbolo para evitar -1111/-1013.
    - Si el notional resultante queda por debajo del mínimo exigido
      (MIN_NOTIONAL_USDT, con colchón opcional), sube la cantidad al
      siguiente múltiplo de stepSize que sí lo cumpla.

    Devuelve (quantity, notional_final).
    """
    if price <= 0:
        raise ValueError("price debe ser > 0 para calcular la cantidad")

    # Default seguro: si no sabemos el stepSize real, asumimos cantidad
    # entera (stepSize=1) en vez de 0.001. Un entero SIEMPRE es múltiplo
    # válido de cualquier stepSize más fino (0.1, 0.01, 0.001...), así que
    # es el fallback universalmente seguro — al revés (asumir decimales en
    # un símbolo que en realidad exige enteros) es lo que dispara -1111.
    step = float(filters.get("stepSize", 1.0)) or 1.0
    min_qty = float(filters.get("minQty", step))
    min_notional = max(float(filters.get("min_notional", MIN_NOTIONAL_USDT)), MIN_NOTIONAL_USDT)
    min_notional *= (1 + extra_buffer_pct / 100.0)

    raw_qty = desired_notional / price
    qty = ceil_to_step(raw_qty, step)
    if qty < min_qty:
        qty = min_qty

    notional = qty * price
    if notional < min_notional:
        needed_qty = min_notional / price
        qty = ceil_to_step(needed_qty, step)
        if qty < min_qty:
            qty = min_qty
        notional = qty * price

    return qty, notional


# ══════════════════════════════════════════════════════════
#  MODELO DE TRADE
# ══════════════════════════════════════════════════════════
@dataclass
class Trade:
    id: int
    symbol: str
    direction: str  # LONG | SHORT
    entry_price: float
    quantity: float
    open_time: str
    leverage: int
    paper_trade_id: int = 0
    entry_order_id: str = ""
    current_price: float = 0.0
    status: str = "OPEN"  # OPEN | TP | SL | CLOSED | MANUAL | CLOSE_ALL
    close_price: float = 0.0
    close_time: str = ""
    pnl_usdt: float = 0.0
    roi_pct: float = 0.0
    order_assumed: bool = False

    @property
    def notional_usdt(self) -> float:
        return self.entry_price * self.quantity

    def update_unrealized(self, price: float):
        self.current_price = price
        if self.direction == "LONG":
            self.pnl_usdt = (price - self.entry_price) * self.quantity
        else:
            self.pnl_usdt = (self.entry_price - price) * self.quantity
        self.roi_pct = (self.pnl_usdt / self.notional_usdt * 100) if self.notional_usdt else 0.0


# ══════════════════════════════════════════════════════════
#  BINANCE WS API
# ══════════════════════════════════════════════════════════
class BinanceAPI:
    """Cliente mínimo para Binance Futures WebSocket API."""

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, ws_url: str = WS_API_URL):
        if not api_key or not api_secret:
            raise ValueError("BINANCE_API_KEY y BINANCE_API_SECRET son obligatorias")

        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self.testnet = testnet
        self.ws_url = ws_url

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future] = {}
        self._closed = False

        # Cache de filtros por símbolo (stepSize/minQty/minNotional) leídos
        # de /fapi/v1/exchangeInfo. Se usa para calcular la cantidad real a
        # enviar a partir del tamaño de orden deseado en USDT. Se carga
        # de UNA SOLA VEZ para todos los símbolos (no uno por símbolo) y
        # se refresca cada EXCHANGE_INFO_TTL_S, para minimizar peso REST.
        self._symbol_filters_cache: dict[str, dict] = {}
        self._symbol_filters_lock = asyncio.Lock()
        self._exchange_info_loaded_at: float = 0.0

        # Cache de leverage aplicado por símbolo, para no repetir la
        # llamada REST de set_leverage si el valor no cambió (velocidad).
        self._leverage_cache: dict[str, int] = {}
        self._leverage_lock = asyncio.Lock()

        # Freno de bloqueo de IP (Binance -1003 / HTTP 418): si Binance ya
        # nos banea por exceso de requests, dejamos de pegarle a REST hasta
        # que pase el tiempo indicado en el propio mensaje de error, en vez
        # de seguir reintentando y empeorar/alargar el bloqueo.
        self._rest_ban_until_ms: float = 0.0

    @staticmethod
    def _payload_string(params: dict) -> str:
        return "&".join(
            f"{k}={params[k]}"
            for k in sorted(params.keys())
            if k != "signature"
        )

    def _sign(self, params: dict) -> str:
        payload = self._payload_string(params)
        return hmac.new(self.api_secret, payload.encode("utf-8"), hashlib.sha256).hexdigest()

    def _is_rest_banned(self) -> bool:
        return self._rest_ban_until_ms > datetime.now(timezone.utc).timestamp() * 1000

    def _rest_ban_remaining_s(self) -> float:
        return max(0.0, self._rest_ban_until_ms / 1000 - datetime.now(timezone.utc).timestamp())

    def _note_possible_ip_ban(self, response_text: str):
        """
        Si la respuesta de Binance indica -1003 (demasiadas requests, IP
        baneada), guarda el timestamp hasta el que dura el bloqueo para
        que las próximas llamadas REST se omitan en vez de seguir
        golpeando la API y alargar/empeorar el bloqueo.
        """
        if "-1003" not in response_text:
            return
        match = re.search(r"banned until (\d+)", response_text)
        if not match:
            return
        until_ms = float(match.group(1))
        if until_ms > self._rest_ban_until_ms:
            self._rest_ban_until_ms = until_ms
            until_dt = datetime.fromtimestamp(until_ms / 1000, tz=timezone.utc)
            log.error(
                f"⛔ IP bloqueada por Binance (rate limit -1003) hasta {until_dt.strftime('%Y-%m-%d %H:%M:%S UTC')} "
                f"(~{self._rest_ban_remaining_s():.0f}s) — se omitirán llamadas REST hasta entonces"
            )

    def _check_rest_ban_or_raise(self):
        if self._is_rest_banned():
            raise RuntimeError(f"REST omitida: IP bloqueada por Binance (-1003), quedan ~{self._rest_ban_remaining_s():.0f}s")

    def _ws_alive(self) -> bool:
        return bool(
            self._ws and not self._ws.closed
            and self._reader_task and not self._reader_task.done()
        )

    async def _ensure_http_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))
        return self._session

    async def connect(self):
        if self._ws_alive():
            return

        async with self._connect_lock:
            if self._ws_alive():
                return

            # Limpiar conexión muerta (el reader pudo haber terminado sin
            # cerrar el socket explícitamente — quedaba "zombie")
            if self._ws is not None:
                try:
                    if not self._ws.closed:
                        await self._ws.close()
                except Exception:
                    pass
                self._ws = None
            if self._reader_task is not None and not self._reader_task.done():
                self._reader_task.cancel()

            await self._ensure_http_session()

            log.info(f"Conectando Binance WS API → {self.ws_url}")
            self._ws = await self._session.ws_connect(
                self.ws_url,
                autoping=True,
                heartbeat=30,
                max_msg_size=0,
            )
            self._reader_task = asyncio.create_task(self._reader())

    async def close(self):
        self._closed = True
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._reader_task and not self._reader_task.done():
            self._reader_task.cancel()
        if self._session and not self._session.closed:
            await self._session.close()

    async def _reader(self):
        assert self._ws is not None
        while not self._closed:
            try:
                msg = await self._ws.receive()
            except Exception as e:
                log.error(f"WS reader error: {e}")
                break

            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except Exception:
                    log.warning(f"WS no JSON: {msg.data!r}")
                    continue

                req_id = str(data.get("id")) if data.get("id") is not None else None
                fut = self._pending.pop(req_id, None) if req_id is not None else None
                if fut is not None and not fut.done():
                    fut.set_result(data)
                else:
                    log.debug(f"WS event no mapeado: {data}")
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

        err = ConnectionError("WebSocket API desconectado")
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(err)
        self._pending.clear()

    async def _request(self, method: str, params: Optional[dict] = None, signed: bool = False, timeout: float = 20.0, _retry: bool = True) -> dict:
        await self.connect()

        params = dict(params or {})
        if signed:
            params.setdefault("apiKey", self.api_key)
            params.setdefault("timestamp", int(datetime.now(timezone.utc).timestamp() * 1000))
            params.setdefault("recvWindow", 5000)
            params["signature"] = self._sign(params)

        req_id = str(uuid.uuid4())
        payload = {"id": req_id, "method": method}
        if params:
            payload["params"] = params

        loop = asyncio.get_running_loop()
        fut = loop.create_future()
        self._pending[req_id] = fut

        try:
            assert self._ws is not None
            await self._ws.send_json(payload)
        except Exception as e:
            self._pending.pop(req_id, None)
            if _retry:
                log.warning(f"_request: fallo enviando ({e!r}); reconectando y reintentando una vez")
                return await self._request(method, params, signed=False, timeout=timeout, _retry=False)
            raise

        try:
            response = await asyncio.wait_for(fut, timeout=timeout)
        except Exception as e:
            self._pending.pop(req_id, None)
            if _retry and not self._ws_alive():
                log.warning(f"_request: sin respuesta ({e!r}); conexión muerta, reconectando y reintentando una vez")
                return await self._request(method, params, signed=False, timeout=timeout, _retry=False)
            raise

        if response.get("status") != 200:
            err = response.get("error") or {}
            raise RuntimeError(f"Binance WS error {response.get('status')}: {err}")
        return response.get("result", response)

    async def account_balance(self) -> list[dict]:
        result = await self._request("account.balance", signed=True)
        return result if isinstance(result, list) else []

    async def position_information(self, symbol: Optional[str] = None) -> list[dict]:
        params = {}
        if symbol:
            params["symbol"] = symbol
        result = await self._request("account.position", params=params, signed=True)
        return result if isinstance(result, list) else []

    async def set_leverage(self, symbol: str, leverage: int, force: bool = False) -> dict:
        """
        Cambia el leverage inicial de un símbolo. Esto es exclusivamente
        REST en Binance (POST /fapi/v1/leverage) — la WS API no expone
        ningún método equivalente.
        """
        leverage = int(leverage)
        if not force and self._leverage_cache.get(symbol) == leverage:
            return {"symbol": symbol, "leverage": leverage, "cached": True}

        async with self._leverage_lock:
            if not force and self._leverage_cache.get(symbol) == leverage:
                return {"symbol": symbol, "leverage": leverage, "cached": True}

            self._check_rest_ban_or_raise()

            session = await self._ensure_http_session()
            params = {
                "symbol": symbol,
                "leverage": leverage,
                "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                "recvWindow": 5000,
            }
            query = self._payload_string(params)
            signature = self._sign(params)
            url = f"{REST_FAPI_URL}/fapi/v1/leverage?{query}&signature={signature}"
            headers = {"X-MBX-APIKEY": self.api_key}

            async with session.post(url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                text = await resp.text()
                if resp.status != 200:
                    self._note_possible_ip_ban(text)
                    raise RuntimeError(f"REST set_leverage error {resp.status}: {text}")
                try:
                    data = json.loads(text)
                except Exception:
                    data = {"raw": text}
                log.info(f"Leverage REST OK: {symbol} → {data.get('leverage', leverage)}x")
                self._leverage_cache[symbol] = leverage
                return data

    EXCHANGE_INFO_TTL_S = 6 * 3600  # los filtros de lote casi nunca cambian; refrescar cada 6h basta

    async def _load_all_symbol_filters(self, force_refresh: bool = False) -> None:
        """
        Carga TODOS los símbolos de /fapi/v1/exchangeInfo en UNA sola
        llamada REST y cachea sus filtros (en vez de una llamada por
        símbolo por señal, que es lo que terminó disparando el bloqueo de
        IP -1003 de Binance). El cache se reutiliza durante
        EXCHANGE_INFO_TTL_S, ya que estos filtros prácticamente no
        cambian día a día.
        """
        now = datetime.now(timezone.utc).timestamp()
        if not force_refresh and self._symbol_filters_cache and (now - self._exchange_info_loaded_at) < self.EXCHANGE_INFO_TTL_S:
            return

        async with self._symbol_filters_lock:
            now = datetime.now(timezone.utc).timestamp()
            if not force_refresh and self._symbol_filters_cache and (now - self._exchange_info_loaded_at) < self.EXCHANGE_INFO_TTL_S:
                return

            self._check_rest_ban_or_raise()

            session = await self._ensure_http_session()
            url = f"{REST_FAPI_URL}/fapi/v1/exchangeInfo"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                text = await resp.text()
                if resp.status != 200:
                    self._note_possible_ip_ban(text)
                    raise RuntimeError(f"REST exchangeInfo error {resp.status}: {text}")
                data = json.loads(text)

            new_cache: dict[str, dict] = {}
            for s in data.get("symbols", []):
                sym = s.get("symbol")
                if not sym:
                    continue

                qty_precision = int(s.get("quantityPrecision", 0))
                entry = {
                    "stepSize": round(10 ** (-qty_precision), 10),
                    "minQty": round(10 ** (-qty_precision), 10),
                    "qty_precision": qty_precision,
                    "min_notional": MIN_NOTIONAL_USDT,
                }

                lot_size_filter = None
                market_lot_size_filter = None
                for f in s.get("filters", []):
                    ftype = f.get("filterType")
                    if ftype == "LOT_SIZE":
                        lot_size_filter = f
                    elif ftype == "MARKET_LOT_SIZE":
                        market_lot_size_filter = f
                    elif ftype in ("MIN_NOTIONAL", "NOTIONAL"):
                        notional = f.get("notional") or f.get("minNotional")
                        if notional is not None:
                            entry["min_notional"] = float(notional)

                # Las órdenes que envía este executor son siempre MARKET,
                # así que MARKET_LOT_SIZE es el filtro que Binance
                # realmente valida; LOT_SIZE queda como respaldo.
                chosen = market_lot_size_filter or lot_size_filter
                if chosen:
                    entry["stepSize"] = float(chosen.get("stepSize", entry["stepSize"]))
                    entry["minQty"] = float(chosen.get("minQty", entry["minQty"]))

                # Piso de seguridad: nunca operar por debajo de MIN_NOTIONAL_USDT
                # aunque exchangeInfo reporte un valor menor.
                entry["min_notional"] = max(entry["min_notional"], MIN_NOTIONAL_USDT)
                new_cache[sym] = entry

            self._symbol_filters_cache = new_cache
            self._exchange_info_loaded_at = now
            log.info(f"exchangeInfo cargado de una sola vez: {len(new_cache)} símbolos cacheados")

    async def get_symbol_filters(self, symbol: str, force_refresh: bool = False) -> dict:
        """
        Devuelve los filtros de cantidad/notional del símbolo, usando el
        cache global poblado por _load_all_symbol_filters (una sola
        llamada REST para todos los símbolos en vez de una por símbolo).

        Si la carga falla (red caída, IP bloqueada por -1003, etc.) o el
        símbolo no aparece en exchangeInfo, se devuelve un default SEGURO
        de cantidad entera (stepSize=1): un entero siempre es múltiplo
        válido de cualquier stepSize más fino, así que es la opción que
        menos rechazos provoca cuando no tenemos el dato real — al revés
        (asumir decimales) es lo que dispara -1111.
        """
        safe_default = {"stepSize": 1.0, "minQty": 1.0, "qty_precision": 0, "min_notional": MIN_NOTIONAL_USDT}

        try:
            await self._load_all_symbol_filters(force_refresh=force_refresh)
        except Exception as e:
            log.warning(f"get_symbol_filters: no se pudo cargar exchangeInfo ({e}); usando default seguro (entero) para {symbol}")
            return safe_default

        cached = self._symbol_filters_cache.get(symbol)
        if cached:
            return cached

        log.error(f"get_symbol_filters: símbolo {symbol} no encontrado en exchangeInfo cacheado — usando default seguro (entero)")
        return safe_default

    async def create_market_order(
        self,
        symbol: str,
        side: str,
        quantity,
        position_side: Optional[str] = None,
        reduce_only: bool = False,
        new_order_resp_type: str = "RESULT",
    ) -> dict:
        params: dict = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            # Binance WS API exige los DECIMAL (price, quantity, etc.) como
            # strings, no como floats — enviar float puede introducir
            # ruido de precisión (p.ej. 0.1 + 0.2) que dispara -1111/-1013.
            "quantity": str(quantity),
            "newOrderRespType": new_order_resp_type,
        }
        if position_side:
            params["positionSide"] = position_side
        if reduce_only:
            params["reduceOnly"] = "true"
        result = await self._request("order.place", params=params, signed=True)
        return result if isinstance(result, dict) else {"raw": result}

    async def close_position_market(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        position_side: Optional[str] = None,
    ) -> dict:
        direction = direction.upper()
        close_side = "SELL" if direction == "LONG" else "BUY"
        return await self.create_market_order(
            symbol=symbol,
            side=close_side,
            quantity=quantity,
            position_side=position_side,
            reduce_only=True,
            new_order_resp_type="RESULT",
        )

    async def close_all_positions(self, symbol: Optional[str] = None) -> list[dict]:
        positions = await self.position_information(symbol=symbol)
        closed = []
        for p in positions:
            try:
                amt = float(p.get("positionAmt", 0))
            except Exception:
                amt = 0.0
            if abs(amt) <= 0:
                continue

            sym = p.get("symbol", symbol or "")
            pos_side = p.get("positionSide") or None

            if pos_side in ("LONG", "SHORT"):
                close_side = "SELL" if pos_side == "LONG" else "BUY"
                qty = abs(amt)
                result = await self.create_market_order(
                    symbol=sym,
                    side=close_side,
                    quantity=qty,
                    position_side=pos_side,
                    reduce_only=True,
                    new_order_resp_type="RESULT",
                )
            else:
                close_side = "SELL" if amt > 0 else "BUY"
                qty = abs(amt)
                result = await self.create_market_order(
                    symbol=sym,
                    side=close_side,
                    quantity=qty,
                    position_side="BOTH",
                    reduce_only=True,
                    new_order_resp_type="RESULT",
                )
            closed.append(result)
        return closed


# ══════════════════════════════════════════════════════════
#  GESTOR DE EJECUCIÓN
# ══════════════════════════════════════════════════════════
class ExecutionManager:
    """Ejecuta y rastrea posiciones reales en Binance Futures vía WS."""

    def __init__(self, binance_api, price_ws):
        self.api = binance_api
        self.price_ws = price_ws
        self._trades: dict[str, Trade] = {}
        self._closed: list[Trade] = []
        self._counter: int = 0
        self._lock = asyncio.Lock()
        self._balance: float = 0.0
        self._paper_id_map: dict[int, str] = {}
        self.trading_enabled: bool = True

    async def refresh_balance(self):
        try:
            balances = await self.api.account_balance()
            for b in balances:
                if b.get("asset") == "USDT":
                    self._balance = float(b.get("availableBalance", b.get("balance", 0)))
                    return
        except Exception as e:
            log.error(f"refresh_balance: {e}")

    @property
    def balance(self) -> float:
        return self._balance

    @property
    def open_trades(self) -> list[Trade]:
        return list(self._trades.values())

    @property
    def closed_trades(self) -> list[Trade]:
        return list(self._closed)

    @property
    def open_longs(self) -> list[Trade]:
        return [t for t in self.open_trades if t.direction == "LONG"]

    @property
    def open_shorts(self) -> list[Trade]:
        return [t for t in self.open_trades if t.direction == "SHORT"]

    @property
    def active_symbols(self) -> set:
        return set(self._trades.keys())

    @property
    def total_realized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self._closed)

    @property
    def unrealized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self.open_trades)

    @property
    def equity(self) -> float:
        return self._balance + self.unrealized_pnl

    def _sync_ws_symbols(self):
        try:
            self.price_ws.update_symbols(list(self._trades.keys()))
        except Exception as e:
            log.error(f"_sync_ws_symbols: {e}")

    async def _place_market_order_safe(
        self,
        symbol: str,
        side: str,
        qty_str: str,
        position_side: Optional[str],
        filters: dict,
        ref_price: float,
        reduce_only: bool = False,
    ) -> dict:
        """
        Envía la orden MARKET con la quantity ya calculada. Si Binance la
        rechaza específicamente por notional insuficiente (-4164) o por
        precisión/stepSize (-1013 / -1111) — típico cuando el precio se
        movió justo entre el cálculo y el envío — se recalcula la
        cantidad con un colchón de seguridad mayor sobre el notional
        mínimo y se reintenta UNA sola vez con un precio fresco.
        """
        try:
            return await self.api.create_market_order(
                symbol=symbol,
                side=side,
                quantity=qty_str,
                position_side=position_side,
                reduce_only=reduce_only,
                new_order_resp_type="RESULT",
            )
        except Exception as e:
            err = str(e)
            is_notional_or_precision = any(code in err for code in ("-4164", "-1013", "-1111", "Notional", "precision"))
            if not is_notional_or_precision:
                raise

            log.warning(f"_place_market_order_safe: {symbol} rechazada ({err}); recalculando con colchón mayor y reintentando una vez")

            try:
                fresh_price = float(self.price_ws.get_price(symbol) or 0.0) or ref_price
            except Exception:
                fresh_price = ref_price

            # Doble colchón de seguridad en el reintento (p.ej. 2% -> ~10%).
            retry_buffer = max(NOTIONAL_SAFETY_BUFFER_PCT * 5, 10.0)
            min_notional = max(float(filters.get("min_notional", MIN_NOTIONAL_USDT)), MIN_NOTIONAL_USDT)
            target_notional = min_notional * (1 + retry_buffer / 100.0)

            retry_qty, retry_notional = resolve_safe_quantity(target_notional, fresh_price, filters)
            retry_qty_str = format_qty(retry_qty, filters.get("stepSize", 0.001))

            log.info(f"_place_market_order_safe: reintento {symbol} qty={retry_qty_str} (notional≈${retry_notional:.4f}, precio={fresh_price})")

            return await self.api.create_market_order(
                symbol=symbol,
                side=side,
                quantity=retry_qty_str,
                position_side=position_side,
                reduce_only=reduce_only,
                new_order_resp_type="RESULT",
            )

    async def open_trade(
        self,
        symbol: str,
        direction: str,
        price: float,
        quantity: float,
        paper_trade_id: int = 0,
    ) -> Optional[Trade]:
        direction = direction.upper()
        side = "BUY" if direction == "LONG" else "SELL"
        position_side = direction if HEDGE_MODE else "BOTH"

        # Ya NO se bloquea si el símbolo ya tiene una posición abierta: la
        # señal se manda siempre. Si ya existía una posición en ese símbolo,
        # se fusiona con la nueva al registrar el trade (ver más abajo),
        # igual que hace Binance internamente con el neto por símbolo.

        order_assumed = False
        entry_order_id = ""

        # ── Cálculo de la cantidad real a enviar ──────────────────────
        # La señal trae price + quantity, que en conjunto definen el
        # TAMAÑO DE ORDEN deseado en USDT (notional = price * quantity).
        # Por liquidez/velocidad del precio, para cuando la orden se
        # envía ese precio puede haberse movido, dejando la quantity
        # original por debajo del notional mínimo que exige Binance
        # (5.1 USDT). En vez de enviar la quantity tal cual, recalculamos
        # la cantidad a partir del notional deseado y del precio más
        # fresco disponible (caché WS de precios), y la ajustamos al
        # stepSize del símbolo garantizando que el notional final nunca
        # quede por debajo del mínimo.
        desired_notional = price * quantity
        live_price = 0.0
        try:
            live_price = float(self.price_ws.get_price(symbol) or 0.0)
        except Exception:
            live_price = 0.0
        ref_price = live_price if live_price > 0 else price

        # SIEMPRE usar el precio del WebSocket como precio de entrada
        # de referencia, ignorando el precio que llega en la señal.
        # Si la señal manda price=1000 pero el WS reporta 1.2, se usa
        # 1.2 tanto para calcular la cantidad como para registrar la
        # entrada — así la posición refleja la realidad del mercado.
        filled_price = ref_price
        if live_price > 0 and abs(live_price - price) / max(price, 1e-9) > 0.01:
            log.info(
                f"open_trade: precio señal={price} IGNORADO — usando precio WS={live_price} para {symbol}"
            )

        # get_symbol_filters (exchangeInfo) y set_leverage son dos llamadas
        # REST independientes entre sí: se disparan en paralelo en vez de
        # una tras otra para no sumar sus latencias en el camino crítico.
        # En la práctica casi siempre son "gratis": filters queda cacheado
        # tras la primera vez por símbolo, y set_leverage se omite por
        # completo si el leverage no cambió (ver BinanceAPI.set_leverage).
        filters_result, leverage_result = await asyncio.gather(
            self.api.get_symbol_filters(symbol),
            self.api.set_leverage(symbol, LEVERAGE),
            return_exceptions=True,
        )

        if isinstance(filters_result, Exception):
            log.warning(f"open_trade: no se pudieron leer filtros de {symbol}, usando defaults ({filters_result})")
            filters = {"stepSize": 1.0, "minQty": 1.0, "min_notional": MIN_NOTIONAL_USDT}
        else:
            filters = filters_result

        if isinstance(leverage_result, Exception):
            log.warning(f"open_trade: no se pudo aplicar leverage para {symbol}: {leverage_result}")

        try:
            send_qty, send_notional = resolve_safe_quantity(
                desired_notional, ref_price, filters, extra_buffer_pct=NOTIONAL_SAFETY_BUFFER_PCT
            )
        except Exception as e:
            log.error(f"open_trade: no se pudo calcular quantity segura para {symbol}: {e}")
            return None

        if abs(send_qty - quantity) > 1e-12:
            log.info(
                f"open_trade: quantity ajustada para {symbol} → señal={quantity} (notional≈${desired_notional:.4f}) "
                f"→ enviada={send_qty} (notional≈${send_notional:.4f}, precio_ref={ref_price})"
            )
        quantity = send_qty

        qty_str = format_qty(quantity, filters.get("stepSize", 0.001))
        log.info(f"open_trade: enviando MARKET por WS → {symbol} {side} qty={qty_str} (notional≈${send_notional:.4f})")
        try:
            result = await self._place_market_order_safe(
                symbol=symbol,
                side=side,
                qty_str=qty_str,
                position_side=position_side,
                filters=filters,
                ref_price=ref_price,
            )
            entry_order_id = str(result.get("orderId", result.get("clientOrderId", "WS_ORDER")))
            avg = result.get("avgPrice") or result.get("price")
            try:
                avg_f = float(avg)
                if avg_f > 0:
                    filled_price = avg_f
            except Exception:
                pass
            # Si la cantidad final se ajustó en el reintento, refleja el valor
            # realmente ejecutado en el trade que se registra.
            try:
                executed_qty = float(result.get("origQty") or result.get("executedQty") or quantity)
                if executed_qty > 0:
                    quantity = executed_qty
            except Exception:
                pass
            log.info(f"MARKET WS OK: {symbol} {side} qty={quantity} id={entry_order_id} avg={filled_price}")
        except Exception as e_ord:
            err = str(e_ord)
            if "-2019" in err or "Margin is insufficient" in err:
                log.warning(f"[ASUMIDA] MARKET WS -2019 para {symbol} — posición registrada como abierta: {e_ord}")
                entry_order_id = "MARGIN_INSUFFICIENT"
                order_assumed = True
            else:
                log.error(f"open_trade: fallo enviando MARKET WS para {symbol}: {e_ord}")
                return None

        async with self._lock:
            existing = self._trades.get(symbol)

            if existing is None:
                self._counter += 1
                trade = Trade(
                    id=self._counter,
                    symbol=symbol,
                    direction=direction,
                    entry_price=filled_price,
                    quantity=quantity,
                    open_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                    leverage=LEVERAGE,
                    paper_trade_id=paper_trade_id,
                    entry_order_id=entry_order_id,
                    current_price=filled_price,
                    order_assumed=order_assumed,
                )
                self._trades[symbol] = trade
                self._paper_id_map[paper_trade_id] = symbol
                action_tag = "ABIERTO"
            else:
                # Ya había una posición en este símbolo: se fusiona usando
                # cantidad con signo (+ LONG / - SHORT), igual que el neto
                # real que mantiene Binance por símbolo.
                old_signed = existing.quantity if existing.direction == "LONG" else -existing.quantity
                delta_signed = quantity if direction == "LONG" else -quantity
                new_signed = old_signed + delta_signed

                if abs(new_signed) < 1e-9:
                    # La señal opuesta neteó la posición a cero -> queda cerrada
                    existing.status = "NETTED"
                    existing.close_price = filled_price
                    existing.close_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                    existing.update_unrealized(filled_price)
                    del self._trades[symbol]
                    self._paper_id_map.pop(existing.paper_trade_id, None)
                    self._closed.append(existing)
                    log.info(f"open_trade: {symbol} neteado a 0 con esta señal — posición cerrada")
                    trade = None
                    action_tag = "NETEADO"
                else:
                    new_direction = "LONG" if new_signed > 0 else "SHORT"
                    new_qty = abs(new_signed)
                    if new_direction == existing.direction:
                        # Misma dirección: se promedia el precio de entrada
                        existing.entry_price = (
                            (existing.entry_price * existing.quantity) + (filled_price * quantity)
                        ) / new_qty
                        action_tag = "AMPLIADO"
                    else:
                        # Dirección invertida: el remanente entra al precio nuevo
                        existing.entry_price = filled_price
                        action_tag = "INVERTIDO"
                    existing.direction = new_direction
                    existing.quantity = new_qty
                    existing.entry_order_id = entry_order_id
                    existing.order_assumed = existing.order_assumed or order_assumed
                    self._paper_id_map[paper_trade_id] = symbol
                    trade = existing

        self._sync_ws_symbols()
        await self.refresh_balance()

        if trade is None:
            return None

        assumed_tag = " [ASUMIDA — MARGIN INSUF]" if order_assumed else ""
        log.info(
            f"[REAL #{trade.id}] {action_tag} {trade.direction} {symbol} @ ${filled_price} | "
            f"Qty total: {trade.quantity} | Lev: {LEVERAGE}x | OrderId: {entry_order_id} | "
            f"Paper#{paper_trade_id}{assumed_tag}"
        )
        return trade

    async def close_trade(self, trade: Trade, close_price: float, reason: str) -> bool:
        async with self._lock:
            if trade.status != "OPEN":
                return False
            if trade.symbol not in self._trades:
                return False

            trade.status = reason
            trade.close_price = close_price
            trade.close_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

            if trade.direction == "LONG":
                trade.pnl_usdt = (close_price - trade.entry_price) * trade.quantity
            else:
                trade.pnl_usdt = (trade.entry_price - close_price) * trade.quantity

            trade.roi_pct = (trade.pnl_usdt / trade.notional_usdt * 100) if trade.notional_usdt else 0.0

            del self._trades[trade.symbol]
            self._paper_id_map.pop(trade.paper_trade_id, None)
            self._closed.append(trade)

        self._sync_ws_symbols()

        log.info(
            f"[REAL #{trade.id}] CERRADO {reason} {trade.symbol} @ ${close_price} | "
            f"PnL: {trade.pnl_usdt:+.4f} USDT ({trade.roi_pct:+.2f}%)"
        )
        await self.refresh_balance()
        return True

    async def force_close_trade(self, trade: Trade, reason: str = "MAIN_BOT", close_price: Optional[float] = None) -> bool:
        close_price = close_price if close_price and close_price > 0 else (trade.current_price if trade.current_price > 0 else trade.entry_price)
        try:
            await self.api.close_position_market(
                symbol=trade.symbol,
                direction=trade.direction,
                quantity=trade.quantity,
                position_side=(trade.direction if HEDGE_MODE else "BOTH"),
            )
            log.info(f"force_close: close_position_market OK para {trade.symbol}")
        except Exception as e:
            log.warning(f"force_close: error cerrando {trade.symbol}: {e}")
        return await self.close_trade(trade, close_price, reason)

    async def force_close_by_symbol(self, symbol: str) -> bool:
        try:
            closed = await self.api.close_all_positions(symbol=symbol)
            log.info(f"force_close_by_symbol {symbol}: {closed}")
            return bool(closed)
        except Exception as e:
            log.error(f"force_close_by_symbol {symbol}: {e}")
            return False

    async def close_all_global(self, reason: str = "CLOSE_ALL") -> list[Trade]:
        async with self._lock:
            trades_snapshot = list(self._trades.values())

        closed_trades = []
        for trade in trades_snapshot:
            try:
                closed = await self.force_close_trade(trade, reason=reason)
                if closed:
                    closed_trades.append(trade)
                    log.info(f"close_all_global: cerrado {trade.symbol} #{trade.id}")
            except Exception as e:
                log.error(f"close_all_global: error cerrando {trade.symbol}: {e}")

        log.info(f"close_all_global: {len(closed_trades)}/{len(trades_snapshot)} posiciones cerradas")
        return closed_trades

    async def poll_positions(self) -> list[Trade]:
        if not self._trades:
            return []

        try:
            positions = await self.api.position_information()
        except Exception as e:
            log.error(f"poll_positions: {e}")
            return []

        pos_by_symbol: dict[str, dict] = {}
        for p in positions:
            sym = p.get("symbol", "")
            try:
                amt = float(p.get("positionAmt", 0))
            except Exception:
                amt = 0.0
            if sym and abs(amt) > 0:
                pos_by_symbol[sym] = p

        async with self._lock:
            open_copy = dict(self._trades)

        missing: list[Trade] = [trade for symbol, trade in open_copy.items() if symbol not in pos_by_symbol]
        return missing

    def find_by_paper_id(self, paper_trade_id: int) -> Optional[Trade]:
        sym = self._paper_id_map.get(paper_trade_id)
        if sym:
            return self._trades.get(sym)
        return None


# ══════════════════════════════════════════════════════════
#  INSTANCIAS GLOBALES
# ══════════════════════════════════════════════════════════
execution_manager: Optional[ExecutionManager] = None

executor_status = {
    "signals_received": 0,
    "signals_open": 0,
    "signals_close": 0,
    "signals_rejected": 0,
    "manual_closes": 0,
    "last_signal_time": "Esperando señales...",
    "last_signal_detail": "",
    "started_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
}


# ══════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════
async def send_telegram(session: aiohttp.ClientSession, message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                log.error(f"Telegram error {resp.status}: {await resp.text()}")
    except Exception as e:
        log.error(f"Error Telegram: {e}")


def build_open_message(trade: Trade) -> str:
    emoji = "🟢" if trade.direction == "LONG" else "🔴"
    word = "LONG  ▲" if trade.direction == "LONG" else "SHORT ▼"
    base = trade.symbol.replace("USDT", "")
    assum = "\n⚠️ <i>Posición asumida (margin insuf., -2019)</i>" if trade.order_assumed else ""
    return (
        f"{emoji} <b>🏦 POSICIÓN REAL ABIERTA — {word}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Par:</b>       <code>{trade.symbol}</code>\n"
        f"💰 <b>Entrada:</b>  <code>${trade.entry_price:,.6f}</code>\n"
        f"📦 <b>Cantidad:</b> <code>{trade.quantity} {base}</code>\n"
        f"💹 <b>Notional:</b> <code>{trade.notional_usdt:.4f} USDT</code>\n"
        f"⚡ <b>Leverage:</b> <code>{trade.leverage}x</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔑 <b>OrderId:</b>  <code>{trade.entry_order_id}</code>\n"
        f"🆔 Real <b>#{trade.id}</b>  |  Paper <b>#{trade.paper_trade_id}</b>{assum}\n"
        f"⏱ {trade.open_time}\n"
        f"💼 Balance: <code>{execution_manager.balance:.2f} USDT</code>"
    )


def build_close_message(trade: Trade) -> str:
    reason_map = {
        "TP": ("✅", "TAKE PROFIT 🎯"),
        "SL": ("❌", "STOP LOSS 🛑"),
        "CLOSED": ("🔄", "CIERRE EXTERNO"),
        "MAIN_BOT": ("🔄", "CIERRE SEÑAL PRINCIPAL"),
        "CLOSE_ALL": ("🛑", "CIERRE GLOBAL (SEÑAL)"),
        "MANUAL": ("🖐", "CIERRE MANUAL (DASHBOARD)"),
        "NETTED": ("➖", "NETEADA POR SEÑAL OPUESTA"),
    }
    emoji, reason_str = reason_map.get(trade.status, ("⚠️", trade.status))
    dir_str = "🟢 LONG" if trade.direction == "LONG" else "🔴 SHORT"
    pnl_emoji = "💚" if trade.pnl_usdt >= 0 else "❗"

    closed_all = execution_manager.closed_trades
    wins = sum(1 for t in closed_all if t.status == "TP")
    total = len(closed_all)
    wr = f"{wins / total * 100:.1f}% ({wins}✅/{total - wins}❌)" if total else "N/A"

    return (
        f"{emoji} <b>🏦 POSICIÓN REAL CERRADA — {reason_str}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Par:</b>      <code>{trade.symbol}</code>  {dir_str}\n"
        f"💵 <b>Entrada:</b> <code>${trade.entry_price:,.6f}</code>\n"
        f"💵 <b>Salida:</b>  <code>${trade.close_price:,.6f}</code>\n"
        f"⚡ <b>Lev:</b>     <code>{trade.leverage}x</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{pnl_emoji} <b>PnL:</b>   <code>{trade.pnl_usdt:+.4f} USDT</code>\n"
        f"📊 <b>ROI:</b>   <code>{trade.roi_pct:+.2f}%</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Abierto:  {trade.open_time}\n"
        f"⏱ Cerrado:  {trade.close_time}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 <b>Balance:</b>  <code>{execution_manager.balance:.2f} USDT</code>\n"
        f"💼 <b>Equity:</b>   <code>{execution_manager.equity:.2f} USDT</code>\n"
        f"📈 <b>Win Rate:</b> <code>{wr}</code>\n"
        f"🆔 Real <b>#{trade.id}</b>  |  Paper <b>#{trade.paper_trade_id}</b>"
    )


# ══════════════════════════════════════════════════════════
#  MONITOR DE POSICIONES — SOLO ALERTA
# ══════════════════════════════════════════════════════════
async def position_monitor_loop(session: aiohttp.ClientSession):
    log.info(f"Position Monitor (sólo alertas) — poll cada {POSITION_POLL_S}s")
    await asyncio.sleep(10)
    alerted: set[str] = set()

    while True:
        try:
            missing = await execution_manager.poll_positions()
            missing_symbols = {t.symbol for t in missing}

            for trade in missing:
                if trade.symbol in alerted:
                    continue
                alerted.add(trade.symbol)
                log.warning(f"⚠️ {trade.symbol} (#{trade.id}) ya no aparece en Binance pero sigue OPEN localmente.")
                await send_telegram(
                    session,
                    f"⚠️ <b>POSIBLE CIERRE EXTERNO DETECTADO</b>\n"
                    f"📊 <code>{trade.symbol}</code> (Real #{trade.id} | Paper #{trade.paper_trade_id})\n"
                    f"Ya no aparece entre tus posiciones de Binance, pero el executor sigue registrándola como abierta.\n"
                    f"➡️ No se cerró automáticamente.",
                )

            alerted &= (missing_symbols | execution_manager.active_symbols)

        except Exception as e:
            log.error(f"position_monitor_loop: {e}")

        await asyncio.sleep(POSITION_POLL_S)


# ══════════════════════════════════════════════════════════
#  SYNC DE PRECIOS
# ══════════════════════════════════════════════════════════
async def price_sync_loop():
    log.info("Price Sync Loop — actualizando PnL desde caché WS cada 1s")
    while True:
        try:
            for trade in execution_manager.open_trades:
                price = execution_manager.price_ws.get_price(trade.symbol)
                if price:
                    trade.update_unrealized(price)
        except Exception as e:
            log.error(f"price_sync_loop: {e}")
        await asyncio.sleep(1)


# ══════════════════════════════════════════════════════════
#  HTTP SIGNAL HANDLER
# ══════════════════════════════════════════════════════════
async def signal_handler(request: web.Request) -> web.Response:
    secret = request.headers.get("X-Signal-Secret", "")
    if secret != SIGNAL_SECRET:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    action = data.get("action", "").lower()
    symbol = data.get("symbol", "").upper()
    trade_id = int(data.get("trade_id", 0))

    executor_status["signals_received"] += 1
    executor_status["last_signal_time"] = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    executor_status["last_signal_detail"] = f"{action.upper()} {symbol}"

    if action == "open":
        direction = data.get("direction", "").upper()
        price = float(data.get("price", 0))
        quantity = float(data.get("quantity", 0))

        if not symbol or not direction or price <= 0 or quantity <= 0:
            executor_status["signals_rejected"] += 1
            return web.json_response({"ok": False, "error": "missing or invalid open params"}, status=400)

        if not execution_manager.trading_enabled:
            executor_status["signals_rejected"] += 1
            log.warning(f"Señal OPEN ignorada para {symbol} — trading pausado manualmente desde el dashboard")
            return web.json_response({"ok": False, "error": "trading pausado manualmente desde el dashboard"}, status=200)

        async def _do_open():
            trade = await execution_manager.open_trade(symbol, direction, price, quantity, paper_trade_id=trade_id)
            if trade:
                executor_status["signals_open"] += 1
                async with aiohttp.ClientSession() as sess:
                    await send_telegram(sess, build_open_message(trade))
            else:
                executor_status["signals_rejected"] += 1

        asyncio.create_task(_do_open())
        return web.json_response({"ok": True, "action": "open", "symbol": symbol, "direction": direction})

    if action == "close":
        reason = data.get("reason", "MAIN_BOT").upper()
        close_price = float(data.get("close_price", 0))
        trade = execution_manager.find_by_paper_id(trade_id) or execution_manager._trades.get(symbol)

        async def _do_close():
            if trade:
                use_price = close_price if close_price > 0 else trade.current_price or trade.entry_price
                closed = await execution_manager.force_close_trade(trade, reason=reason, close_price=use_price)
                if closed:
                    executor_status["signals_close"] += 1
                    async with aiohttp.ClientSession() as sess:
                        await send_telegram(sess, build_close_message(trade))
            else:
                closed = await execution_manager.force_close_by_symbol(symbol)
                if closed:
                    executor_status["signals_close"] += 1
                    async with aiohttp.ClientSession() as sess:
                        await send_telegram(sess, f"🔄 <b>CIERRE FORZADO (sin estado local)</b>\n<code>{symbol}</code>")

        asyncio.create_task(_do_close())
        return web.json_response({"ok": True, "action": "close", "symbol": symbol})

    if action == "close_all":
        total_open = len(execution_manager.open_trades)

        async def _do_close_all():
            closed_trades = await execution_manager.close_all_global(reason="CLOSE_ALL")
            async with aiohttp.ClientSession() as sess:
                if not closed_trades:
                    await send_telegram(sess, "🛑 CIERRE GLOBAL ejecutado — no había posiciones abiertas.")
                    return
                for t in closed_trades:
                    await send_telegram(sess, build_close_message(t))
                await send_telegram(sess, f"🛑 CIERRE GLOBAL completado — {len(closed_trades)} posición(es) cerrada(s).")

        asyncio.create_task(_do_close_all())
        executor_status["signals_close"] += total_open
        return web.json_response({"ok": True, "action": "close_all", "positions_targeted": total_open})

    executor_status["signals_rejected"] += 1
    return web.json_response({"ok": False, "error": f"unknown action: {action}"}, status=400)


def _check_dashboard_token(request: web.Request) -> bool:
    return request.headers.get("X-Dashboard-Token", "") == SIGNAL_SECRET


async def manual_close_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    symbol = data.get("symbol", "").upper()
    trade = execution_manager._trades.get(symbol)
    if not trade:
        return web.json_response({"ok": False, "error": f"no hay posición abierta registrada para {symbol}"}, status=404)

    async def _do_manual_close():
        closed = await execution_manager.force_close_trade(trade, reason="MANUAL")
        if closed:
            executor_status["signals_close"] += 1
            executor_status["manual_closes"] += 1
            async with aiohttp.ClientSession() as sess:
                await send_telegram(sess, build_close_message(trade))

    asyncio.create_task(_do_manual_close())
    return web.json_response({"ok": True, "action": "manual_close", "symbol": symbol})


async def manual_close_all_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    total_open = len(execution_manager.open_trades)

    async def _do_close_all():
        closed_trades = await execution_manager.close_all_global(reason="MANUAL")
        async with aiohttp.ClientSession() as sess:
            if not closed_trades:
                await send_telegram(sess, "🖐 Cierre manual global ejecutado — no había posiciones abiertas.")
                return
            for t in closed_trades:
                await send_telegram(sess, build_close_message(t))
            await send_telegram(sess, f"🖐 Cierre manual global completado — {len(closed_trades)} posición(es) cerrada(s).")

    asyncio.create_task(_do_close_all())
    executor_status["signals_close"] += total_open
    executor_status["manual_closes"] += total_open
    return web.json_response({"ok": True, "action": "manual_close_all", "positions_targeted": total_open})


async def manual_toggle_trading_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    execution_manager.trading_enabled = not execution_manager.trading_enabled
    state = "ACTIVADO 🟢" if execution_manager.trading_enabled else "PAUSADO 🔴 (no se enviarán nuevas posiciones)"
    log.warning(f"Trading {state} manualmente desde el dashboard")
    return web.json_response({"ok": True, "trading_enabled": execution_manager.trading_enabled})


async def manual_set_leverage_handler(request: web.Request) -> web.Response:
    global LEVERAGE
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        data = await request.json()
        new_lev = int(data.get("leverage", 0))
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    if new_lev < 1 or new_lev > 125:
        return web.json_response({"ok": False, "error": "leverage debe estar entre 1 y 125"}, status=400)

    LEVERAGE = new_lev
    log.warning(f"Leverage por defecto cambiado desde el dashboard a {LEVERAGE}x (aplica a próximas posiciones)")
    return web.json_response({"ok": True, "leverage": LEVERAGE})


async def manual_clear_history_handler(request: web.Request) -> web.Response:
    """Borra el historial de operaciones cerradas y reinicia el PnL realizado."""
    if not _check_dashboard_token(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    em = execution_manager
    count = len(em._closed)
    em._closed.clear()

    # Reinicia también los contadores de señales para coherencia visual
    executor_status["signals_open"] = 0
    executor_status["signals_close"] = 0
    executor_status["signals_received"] = 0
    executor_status["signals_rejected"] = 0
    executor_status["manual_closes"] = 0
    executor_status["last_signal_time"] = "Historial borrado"
    executor_status["last_signal_detail"] = ""

    log.warning(f"Historial de operaciones cerradas borrado desde el dashboard ({count} operaciones eliminadas)")
    return web.json_response({"ok": True, "cleared": count})


async def api_state_handler(request: web.Request) -> web.Response:
    em = execution_manager

    def ser(t: Trade) -> dict:
        return {
            "id": t.id,
            "paper_trade_id": t.paper_trade_id,
            "symbol": t.symbol,
            "direction": t.direction,
            "entry_price": t.entry_price,
            "quantity": t.quantity,
            "notional": t.notional_usdt,
            "leverage": t.leverage,
            "open_time": t.open_time,
            "current_price": t.current_price,
            "status": t.status,
            "close_price": t.close_price,
            "close_time": t.close_time,
            "pnl_usdt": t.pnl_usdt,
            "roi_pct": t.roi_pct,
            "order_assumed": t.order_assumed,
            "entry_order_id": t.entry_order_id,
        }

    closed = em.closed_trades
    wins = sum(1 for t in closed if t.status == "TP")
    total = len(closed)

    return web.json_response({
        "balance": em.balance,
        "equity": em.equity,
        "realized_pnl": em.total_realized_pnl,
        "unrealized_pnl": em.unrealized_pnl,
        "wins": wins,
        "losses": total - wins,
        "win_rate": (wins / total * 100) if total else None,
        "open_count": len(em.open_trades),
        "open_longs": len(em.open_longs),
        "open_shorts": len(em.open_shorts),
        "open_trades": [ser(t) for t in em.open_trades],
        "closed_trades": [ser(t) for t in closed],
        "executor_status": executor_status,
        "ws_symbols": ", ".join(sorted(em.active_symbols)) or "ninguno",
        "leverage": LEVERAGE,
        "trading_enabled": em.trading_enabled,
        "testnet": USE_TESTNET,
    })


DASHBOARD_JS_TEMPLATE = """
<script>
const DASH_TOKEN = __DASH_TOKEN__;

async function refresh() {
  try {
    const r = await fetch('/api/state');
    const d = await r.json();
    const q = id => document.getElementById(id);

    q('bal').textContent = d.balance.toFixed(2) + ' USDT';
    q('eq').textContent = d.equity.toFixed(2) + ' USDT';
    q('rpnl').textContent = (d.realized_pnl >= 0 ? '+' : '') + d.realized_pnl.toFixed(4) + ' USDT';
    q('upnl').textContent = (d.unrealized_pnl >= 0 ? '+' : '') + d.unrealized_pnl.toFixed(4) + ' USDT';
    q('wr').textContent = d.win_rate != null ? d.win_rate.toFixed(1) + '% (' + d.wins + '✅/' + d.losses + '❌)' : 'N/A';
    q('pos').textContent = d.open_count + ' — ' + d.open_longs + 'L / ' + d.open_shorts + 'S';
    q('lev').textContent = d.leverage + 'x';
    q('sig_rx').textContent = d.executor_status.signals_received;
    q('sig_ok').textContent = d.executor_status.signals_open + ' abiertas / ' + d.executor_status.signals_close + ' cerradas';
    q('sig_rej').textContent = d.executor_status.signals_rejected;
    q('last_sig').textContent = d.executor_status.last_signal_time + ' — ' + d.executor_status.last_signal_detail;
    q('ws_sym').textContent = 'WS activo (ws api): ' + d.ws_symbols;
    q('close_all_btn').disabled = d.open_count === 0;

    const tState = q('trading_state');
    const tBtn = q('trading_toggle_btn');
    if (tState && tBtn) {
      tState.textContent = d.trading_enabled ? '🟢 ACTIVO' : '🔴 PAUSADO';
      tState.style.color = d.trading_enabled ? '#3fb950' : '#f85149';
      tBtn.textContent = d.trading_enabled ? '⏸ Pausar nuevas posiciones' : '▶ Reactivar nuevas posiciones';
    }

    const ob = document.getElementById('open_body');
    if (!d.open_trades.length) {
      ob.innerHTML = '<tr><td colspan="12" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones abiertas</td></tr>';
    } else {
      ob.innerHTML = d.open_trades.map(t => {
        const dir = t.direction === 'LONG' ? '🟢 LONG' : '🔴 SHORT';
        const pnl = t.pnl_usdt >= 0 ? '+' + t.pnl_usdt.toFixed(4) : t.pnl_usdt.toFixed(4);
        const roi = t.roi_pct >= 0 ? '+' + t.roi_pct.toFixed(2) + '%' : t.roi_pct.toFixed(2) + '%';
        const assum = t.order_assumed ? ' ⚠️' : '';
        return `<tr>
          <td>#${t.id}</td><td><b>${t.symbol}</b></td><td>${dir}</td><td>${t.leverage}x</td>
          <td>$${t.entry_price.toFixed(6)}</td><td>$${t.current_price.toFixed(6)}</td>
          <td>${pnl}</td><td>${roi}</td>
          <td>${t.notional.toFixed(4)} USDT</td><td>${t.quantity}</td>
          <td>${t.open_time}${assum}</td>
          <td><button class="btn-close" onclick="closeTrade('${t.symbol}')">Cerrar</button></td>
        </tr>`;
      }).join('');
    }

    const cb = document.getElementById('closed_body');
    const recent = d.closed_trades.slice(-30).reverse();
    if (!recent.length) {
      cb.innerHTML = '<tr><td colspan="9" style="color:#8b949e;text-align:center;padding:.8rem">Sin operaciones cerradas</td></tr>';
    } else {
      cb.innerHTML = recent.map(t => {
        const pnl = t.pnl_usdt >= 0 ? '+' + t.pnl_usdt.toFixed(4) : t.pnl_usdt.toFixed(4);
        const res = t.status;
        return `<tr>
          <td>#${t.id}</td><td>${t.symbol}</td><td>${t.direction}</td><td>${t.leverage}x</td>
          <td>$${t.entry_price.toFixed(6)}</td><td>$${t.close_price.toFixed(6)}</td>
          <td>${pnl}</td><td>${t.roi_pct.toFixed(2)}%</td>
          <td>${res}</td>
        </tr>`;
      }).join('');
    }
  } catch(e) { console.error(e); }
}

async function closeTrade(symbol) {
  if (!confirm('¿Cerrar manualmente la posición ' + symbol + '?')) return;
  try {
    const r = await fetch('/manual/close', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify({symbol: symbol})
    });
    const d = await r.json();
    if (!d.ok) alert('Error al cerrar ' + symbol + ': ' + (d.error || 'desconocido'));
    refresh();
  } catch(e) { alert('Error de red al cerrar ' + symbol); }
}

async function closeAllTrades() {
  if (!confirm('¿Cerrar TODAS las posiciones abiertas manualmente? Esta acción no se puede deshacer.')) return;
  try {
    const r = await fetch('/manual/close_all', {
      method: 'POST',
      headers: {'X-Dashboard-Token': DASH_TOKEN}
    });
    const d = await r.json();
    if (!d.ok) alert('Error al cerrar todas las posiciones: ' + (d.error || 'desconocido'));
    refresh();
  } catch(e) { alert('Error de red al cerrar todas las posiciones'); }
}

async function toggleTrading() {
  const active = document.getElementById('trading_state').textContent.includes('ACTIVO');
  const msg = active
    ? '¿Pausar el envío de NUEVAS posiciones? Las posiciones ya abiertas seguirán gestionándose con normalidad (cierres, PnL, etc).'
    : '¿Reactivar el envío de nuevas posiciones?';
  if (!confirm(msg)) return;
  try {
    const r = await fetch('/manual/toggle_trading', {
      method: 'POST',
      headers: {'X-Dashboard-Token': DASH_TOKEN}
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    refresh();
  } catch(e) { alert('Error de red al cambiar el estado de trading'); }
}

async function setLeverage() {
  const val = parseInt(document.getElementById('lev_input').value, 10);
  if (!val || val < 1 || val > 125) { alert('Leverage inválido (debe ser entre 1 y 125)'); return; }
  if (!confirm('¿Cambiar el leverage por defecto a ' + val + 'x para las próximas posiciones?')) return;
  try {
    const r = await fetch('/manual/set_leverage', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify({leverage: val})
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    refresh();
  } catch(e) { alert('Error de red al cambiar el leverage'); }
}

async function clearHistory() {
  if (!confirm('¿Borrar TODO el historial de operaciones cerradas y reiniciar el PnL realizado? Esta acción no se puede deshacer.')) return;
  try {
    const r = await fetch('/manual/clear_history', {
      method: 'POST',
      headers: {'X-Dashboard-Token': DASH_TOKEN}
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    alert('Historial borrado (' + d.cleared + ' operaciones eliminadas). PnL realizado reiniciado a 0.');
    refresh();
  } catch(e) { alert('Error de red al borrar historial'); }
}

refresh();
setInterval(refresh, 5000);
</script>
"""

async def dashboard_handler(request: web.Request) -> web.Response:
    em = execution_manager
    es = executor_status
    env = "TESTNET 🧪" if USE_TESTNET else "REAL 🔴"

    closed = em.closed_trades
    wins = sum(1 for t in closed if t.status == "TP")
    losses = len(closed) - wins
    wr_str = f"{wins / len(closed) * 100:.1f}%" if closed else "N/A"
    eq_col = "#3fb950" if em.equity >= em.balance else "#f85149"
    rp_col = "#3fb950" if em.total_realized_pnl >= 0 else "#f85149"
    up_col = "#3fb950" if em.unrealized_pnl >= 0 else "#f85149"
    dashboard_js = DASHBOARD_JS_TEMPLATE.replace("__DASH_TOKEN__", json.dumps(SIGNAL_SECRET))

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Futures Executor WS</title>
  <style>
    body{{font-family:Arial,Helvetica,sans-serif;background:#0d1117;color:#c9d1d9;padding:1.2rem}}
    h1{{color:#f0883e;margin-bottom:.8rem;font-size:1.35rem}}
    h2{{color:#58a6ff;margin:.9rem 0 .5rem;font-size:.95rem;display:flex;align-items:center;gap:.6rem}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:.6rem;margin-bottom:1.2rem}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:.75rem}}
    .card .label{{color:#8b949e;font-size:.7rem;margin-bottom:.25rem;text-transform:uppercase;letter-spacing:.04em}}
    .card .value{{color:#f0f6fc;font-size:.95rem;font-weight:bold}}
    .wrap{{overflow-x:auto;margin-bottom:1.2rem}}
    table{{width:100%;border-collapse:collapse;font-size:.78rem;min-width:700px}}
    th{{color:#8b949e;text-align:left;padding:.35rem .45rem;border-bottom:1px solid #30363d;white-space:nowrap;font-size:.71rem}}
    td{{padding:.3rem .45rem;border-bottom:1px solid #1c2128;white-space:nowrap}}
    tr:hover td{{background:#161b22}}
    .dot{{display:inline-block;width:8px;height:8px;background:#3fb950;border-radius:50%;margin-right:5px;animation:blink 1.5s infinite}}
    @keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
    .info-banner{{background:#161b22;border:1px solid #58a6ff;border-radius:8px;padding:.7rem 1rem;margin-bottom:1rem;color:#58a6ff;font-size:.82rem}}
    .btn-close{{background:#f85149;color:#fff;border:none;border-radius:4px;padding:.25rem .6rem;font-size:.7rem;cursor:pointer}}
    .btn-close-all{{background:#f85149;color:#fff;border:none;border-radius:5px;padding:.4rem .9rem;font-size:.78rem;cursor:pointer;font-weight:bold}}
    .btn-close-all:disabled{{background:#30363d;color:#6e7681;cursor:not-allowed}}
    .btn-toggle{{border:none;border-radius:5px;padding:.35rem .7rem;font-size:.72rem;cursor:pointer;font-weight:bold;width:100%;margin-top:.4rem;background:#30363d;color:#f0f6fc}}
    .lev-row{{display:flex;gap:.35rem;margin-top:.4rem}}
    .lev-row input{{width:55px;background:#0d1117;border:1px solid #30363d;border-radius:4px;color:#c9d1d9;padding:.2rem .3rem;font-size:.8rem}}
    .lev-row button{{background:#58a6ff;color:#0d1117;border:none;border-radius:4px;padding:.2rem .5rem;font-size:.72rem;cursor:pointer;font-weight:bold}}
  </style>
</head>
<body>
  <h1>⚡ Futures Executor WS — Binance USDT Perpetuos [{env}]</h1>
  <div class="info-banner">
    📡 Trading por <b>WebSocket API</b>. Precios en tiempo real vía <b>ws.py</b>. 
    Cambios de leverage por <b>REST</b> (la WS API no lo soporta). Leverage configurado: <b>{LEVERAGE}x</b>{' | Modo Hedge' if HEDGE_MODE else ' | Modo One-way'}.
  </div>

  <div class="grid">
    <div class="card"><div class="label">Balance USDT</div><div class="value" id="bal">{em.balance:.2f} USDT</div></div>
    <div class="card"><div class="label">Equity total</div><div class="value" id="eq" style="color:{eq_col}">{em.equity:.2f} USDT</div></div>
    <div class="card"><div class="label">PnL realizado</div><div class="value" id="rpnl" style="color:{rp_col}">{em.total_realized_pnl:+.4f} USDT</div></div>
    <div class="card"><div class="label">PnL no realizado</div><div class="value" id="upnl" style="color:{up_col}">{em.unrealized_pnl:+.4f} USDT</div></div>
    <div class="card"><div class="label">Win Rate</div><div class="value" id="wr">{wr_str} ({wins}✅/{losses}❌)</div></div>
    <div class="card"><div class="label">Posiciones abiertas</div><div class="value" id="pos">{len(em.open_trades)} — {len(em.open_longs)}L / {len(em.open_shorts)}S</div></div>
    <div class="card">
      <div class="label">Leverage</div><div class="value" id="lev">{LEVERAGE}x</div>
      <div class="lev-row">
        <input id="lev_input" type="number" min="1" max="125" value="{LEVERAGE}">
        <button onclick="setLeverage()">Aplicar</button>
      </div>
    </div>
    <div class="card">
      <div class="label">Trading</div>
      <div class="value" id="trading_state" style="color:{'#3fb950' if em.trading_enabled else '#f85149'}">{'🟢 ACTIVO' if em.trading_enabled else '🔴 PAUSADO'}</div>
      <button class="btn-toggle" id="trading_toggle_btn" onclick="toggleTrading()">{'⏸ Pausar nuevas posiciones' if em.trading_enabled else '▶ Reactivar nuevas posiciones'}</button>
    </div>
  </div>

  <h2>📡 Señales Recibidas</h2>
  <div class="grid">
    <div class="card"><div class="label">Total recibidas</div><div class="value" id="sig_rx">{es['signals_received']}</div></div>
    <div class="card"><div class="label">Ejecutadas</div><div class="value" id="sig_ok">{es['signals_open']} abiertas / {es['signals_close']} cerradas</div></div>
    <div class="card"><div class="label">Rechazadas</div><div class="value" id="sig_rej">{es['signals_rejected']}</div></div>
    <div class="card" style="grid-column:span 2"><div class="label">Última señal</div><div class="value" id="last_sig" style="font-size:.8rem">{es['last_signal_time']} — {es['last_signal_detail']}</div></div>
  </div>

  <h2>
    <span class="dot"></span>📊 Posiciones Reales Abiertas
    <button class="btn-close-all" id="close_all_btn" onclick="closeAllTrades()" {"disabled" if not em.open_trades else ""}>🛑 Cerrar TODO</button>
  </h2>
  <p id="ws_sym" style="color:#484f58;font-size:.72rem;margin-bottom:.4rem">WS activo: {", ".join(sorted(em.active_symbols)) or "ninguno"}</p>
  <div class="wrap"><table>
    <thead><tr>
      <th>ID</th><th>Par</th><th>Dirección</th><th>Lev</th><th>Entrada</th><th>Actual</th>
      <th>PnL</th><th>ROI%</th><th>Notional</th><th>Qty</th><th>Abierto</th><th>Acción</th>
    </tr></thead>
    <tbody id="open_body">
      <tr><td colspan="12" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones abiertas</td></tr>
    </tbody>
  </table></div>

  <h2>📋 Operaciones Cerradas (últimas 30)
    <button class="btn-close-all" style="background:#8b949e;font-size:.72rem;padding:.3rem .7rem" onclick="clearHistory()">🗑 Borrar historial y PnL</button>
  </h2>
  <div class="wrap"><table>
    <thead><tr>
      <th>#</th><th>Par</th><th>Dir</th><th>Lev</th><th>Entrada</th><th>Salida</th><th>PnL</th><th>ROI%</th><th>Resultado</th>
    </tr></thead>
    <tbody id="closed_body">
      <tr><td colspan="9" style="color:#8b949e;text-align:center;padding:.8rem">Sin operaciones cerradas</td></tr>
    </tbody>
  </table></div>

  <p style="color:#484f58;margin-top:.6rem;font-size:.7rem">
    Executor WS | Iniciado: {es['started_at']} | Actualizado: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
  </p>

  {dashboard_js}
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def start_http_server():
    app = web.Application()
    app.router.add_post("/signal", signal_handler)
    app.router.add_post("/manual/close", manual_close_handler)
    app.router.add_post("/manual/close_all", manual_close_all_handler)
    app.router.add_post("/manual/toggle_trading", manual_toggle_trading_handler)
    app.router.add_post("/manual/set_leverage", manual_set_leverage_handler)
    app.router.add_post("/manual/clear_history", manual_clear_history_handler)
    app.router.add_get("/", dashboard_handler)
    app.router.add_get("/health", dashboard_handler)
    app.router.add_get("/api/state", api_state_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"Executor HTTP activo en http://0.0.0.0:{PORT}")


async def main():
    global execution_manager

    env_tag = "TESTNET 🧪" if USE_TESTNET else "REAL 🔴"
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║   Futures Executor WS — Binance USDT Perpetuos       ║")
    log.info(f"║   Entorno: {env_tag:<44}║")
    log.info(f"║   Leverage: {LEVERAGE}x | Poll cierre ext.: {POSITION_POLL_S}s              ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        log.critical("BINANCE_API_KEY y BINANCE_API_SECRET son obligatorias")
        return

    try:
        try:
            from WS import SymbolWebSocketPriceCache
        except ImportError:
            from ws import SymbolWebSocketPriceCache
    except ImportError:
        log.critical("No se puede importar SymbolWebSocketPriceCache desde ws.py / WS.py")
        return

    try:
        api = BinanceAPI(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=USE_TESTNET)
        price_ws = SymbolWebSocketPriceCache([])
        price_ws.start()
        execution_manager = ExecutionManager(api, price_ws)
        if HEDGE_MODE:
            log.warning("HEDGE_MODE=true: asegúrate de que tu cuenta esté en Hedge Mode.")
    except Exception as e:
        log.critical(f"Error inicializando BinanceAPI WS / precios: {e}")
        return

    await execution_manager.refresh_balance()
    log.info(f"Balance USDT Futures: ${execution_manager.balance:.2f}")

    async with aiohttp.ClientSession() as sess:
        await send_telegram(
            sess,
            f"⚡ <b>Futures Executor WS iniciado</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Balance USDT:</b> <code>{execution_manager.balance:.2f} USDT</code>\n"
            f"⚡ <b>Leverage:</b> <code>{LEVERAGE}x</code>\n"
            f"📡 <b>Órdenes:</b> WebSocket API\n"
            f"📡 <b>Precios:</b> WebSocket (ws.py)\n"
            f"🔒 <b>Cierre:</b> señal explícita o botón manual\n"
            f"⚠️ <b>Error -2019:</b> posición puede registrarse como asumida",
        )

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            start_http_server(),
            price_sync_loop(),
            position_monitor_loop(session),
        )


if __name__ == "__main__":
    asyncio.run(main())
