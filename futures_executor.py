import asyncio
import hashlib
import hmac
import json
import logging
import math
import os
import re
import time
import uuid
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

import aiohttp
from aiohttp import web

try:
    import orjson

    _loads = orjson.loads

    def _dumps(obj) -> str:
        return orjson.dumps(obj).decode()
except ImportError:
    _loads = json.loads
    _dumps = json.dumps

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
USE_TESTNET = os.environ.get("USE_TESTNET", "false").lower() == "true"
SIGNAL_SECRET = os.environ.get("SIGNAL_SECRET", "cambiar-por-secreto-seguro")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

LEVERAGE = int(os.environ.get("LEVERAGE", "4"))
HEDGE_MODE = os.environ.get("HEDGE_MODE", "false").lower() == "true"
PORT = int(os.environ.get("PORT", "10000"))
POSITION_POLL_S = int(os.environ.get("POSITION_POLL_S", "30"))
BALANCE_POLL_S = int(os.environ.get("BALANCE_POLL_S", "60"))

MIN_NOTIONAL_USDT = float(os.environ.get("MIN_NOTIONAL_USDT", "5.1"))
NOTIONAL_SAFETY_BUFFER_PCT = float(os.environ.get("NOTIONAL_SAFETY_BUFFER_PCT", "2.0"))
MAX_PRICE_AGE_S = float(os.environ.get("MAX_PRICE_AGE_S", "5.0"))
MIN_VALID_PRICE = 0.00001

HIGH_PRICE_THRESHOLD = float(os.environ.get("HIGH_PRICE_THRESHOLD", "2.0"))
HIGH_PRICE_LEVERAGE = int(os.environ.get("HIGH_PRICE_LEVERAGE", "20"))

WS_API_URL = os.environ.get(
    "BINANCE_WS_FAPI_URL",
    "wss://testnet.binancefuture.com/ws-fapi/v1" if USE_TESTNET else "wss://ws-fapi.binance.com/ws-fapi/v1",
)
REST_FAPI_URL = os.environ.get(
    "BINANCE_REST_FAPI_URL",
    "https://testnet.binancefuture.com" if USE_TESTNET else "https://fapi.binance.com",
)

_raw_proxy_urls = os.environ.get("PROXY_URLS", "").strip()
if _raw_proxy_urls:
    PROXY_URLS = [u.strip() for u in _raw_proxy_urls.split(",") if u.strip()]
else:
    _legacy_fixie = os.environ.get("FIXIE_URL", "http://fixie:CuLSweHyTOG4Lg3@ventoux.usefixie.com:80").strip()
    PROXY_URLS = [_legacy_fixie] if _legacy_fixie else []

PRICE_STEP_TIERS: list[tuple[float, float]] = [
    (2.0, 0.1),
    (100.0, 0.01),
    (1000.0, 0.001),
    (10000.0, 0.0001),
    (100000.0, 0.00001),
]

BLOCKED_SYMBOLS: set[str] = {
    s.strip().upper()
    for s in os.environ.get("BLOCKED_SYMBOLS", "BTCUSDT,ETHUSDT,BTCUSDC,ETHUSDC").split(",")
    if s.strip()
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("Executor")

_FMT_MIN = "%Y-%m-%d %H:%M UTC"
_FMT_SEC = "%Y-%m-%d %H:%M:%S UTC"
_BAN_RE = re.compile(r"banned until (\d+)")
_RETRYABLE_ORDER_ERRORS = ("-4164", "-1013", "-1111", "Notional", "precision")

_bg_tasks: set = set()


def set_hedge_mode_runtime(value: bool) -> None:
    global HEDGE_MODE
    HEDGE_MODE = bool(value)


def _utc(fmt: str = _FMT_MIN) -> str:
    return time.strftime(fmt, time.gmtime())


def _now_ms() -> int:
    return int(time.time() * 1000)


def _spawn(coro) -> asyncio.Task:
    task = asyncio.create_task(coro)
    _bg_tasks.add(task)
    task.add_done_callback(_bg_tasks.discard)
    return task


@lru_cache(maxsize=512)
def _step_decimals(step: float) -> int:
    if step <= 0:
        return 8
    s = f"{step:.10f}".rstrip("0")
    return len(s.split(".")[1]) if "." in s else 0


def ceil_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return round(math.ceil(round(value / step, 8)) * step, _step_decimals(step))


def clamp_price(value: float, minimum: float = MIN_VALID_PRICE) -> float:
    try:
        price = float(value)
    except Exception:
        return 0.0
    if not math.isfinite(price):
        return 0.0
    return max(minimum, price)


def format_qty(value: float, step: float) -> str:
    decimals = _step_decimals(step)
    return f"{value:.{decimals}f}" if decimals > 0 else str(int(round(value)))


def resolve_safe_quantity(
    desired_notional: float,
    price: float,
    filters: dict,
    extra_buffer_pct: float = 0.0,
) -> tuple[float, float]:
    if price <= 0:
        raise ValueError("price debe ser > 0 para calcular la cantidad")

    step = float(filters.get("stepSize", 1.0)) or 1.0
    min_qty = float(filters.get("minQty", step))
    min_notional = max(float(filters.get("min_notional", MIN_NOTIONAL_USDT)), MIN_NOTIONAL_USDT)
    min_notional *= 1 + extra_buffer_pct / 100.0

    qty = max(ceil_to_step(desired_notional / price, step), min_qty)
    notional = qty * price
    if notional < min_notional:
        qty = max(ceil_to_step(min_notional / price, step), min_qty)
        notional = qty * price
    return qty, notional


def leverage_for_price(price: float) -> int:
    return HIGH_PRICE_LEVERAGE if price > HIGH_PRICE_THRESHOLD else LEVERAGE


def is_symbol_blocked(symbol: str) -> bool:
    return (symbol or "").upper() in BLOCKED_SYMBOLS


def step_for_price(price: float) -> float:
    step = 1.0
    for threshold, tier_step in PRICE_STEP_TIERS:
        if price <= threshold:
            break
        step = tier_step
    return step


def filters_for_price(price: float) -> dict:
    step = step_for_price(price)
    return {
        "stepSize": step,
        "minQty": step,
        "qty_precision": _step_decimals(step),
        "min_notional": MIN_NOTIONAL_USDT,
    }


@dataclass
class Trade:
    id: int
    symbol: str
    direction: str
    entry_price: float
    quantity: float
    open_time: str
    leverage: int
    paper_trade_id: int = 0
    entry_order_id: str = ""
    current_price: float = 0.0
    status: str = "OPEN"
    close_price: float = 0.0
    close_time: str = ""
    pnl_usdt: float = 0.0
    roi_pct: float = 0.0
    order_assumed: bool = False
    hedge_mode: bool = True
    step_size: float = 1.0

    @property
    def notional_usdt(self) -> float:
        return self.entry_price * self.quantity

    @property
    def position_side(self) -> str:
        return self.direction if self.hedge_mode else "BOTH"

    def calc_pnl(self, price: float) -> float:
        diff = price - self.entry_price if self.direction == "LONG" else self.entry_price - price
        return diff * self.quantity

    def update_unrealized(self, price: float):
        self.current_price = price
        self.pnl_usdt = self.calc_pnl(price)
        notional = self.entry_price * self.quantity
        self.roi_pct = self.pnl_usdt / notional * 100 if notional else 0.0


class BinanceAPI:
    LEVERAGE_FALLBACK_LADDER = [20, 15, 10, 5, 4]

    def __init__(self, api_key: str, api_secret: str, testnet: bool = False, ws_url: str = WS_API_URL):
        if not api_key or not api_secret:
            raise ValueError("BINANCE_API_KEY y BINANCE_API_SECRET son obligatorias")

        self.api_key = api_key
        self.api_secret = api_secret.encode("utf-8")
        self._hmac_base = hmac.new(self.api_secret, digestmod=hashlib.sha256)
        self._headers = {"X-MBX-APIKEY": api_key}
        self.testnet = testnet
        self.ws_url = ws_url

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._connect_lock = asyncio.Lock()
        self._pending: dict[str, asyncio.Future] = {}
        self._closed = False

        self._leverage_cache: dict[str, int] = {}
        self._leverage_lock = asyncio.Lock()
        self._rest_ban_until_ms: float = 0.0
        self._proxy_ban_until_ms: dict[str, float] = {}

    @staticmethod
    def _payload_string(params: dict) -> str:
        return "&".join(f"{k}={params[k]}" for k in sorted(params) if k != "signature")

    def _sign(self, params: dict) -> str:
        h = self._hmac_base.copy()
        h.update(self._payload_string(params).encode("utf-8"))
        return h.hexdigest()

    @staticmethod
    def _parse_ban_until(text: str) -> Optional[float]:
        if "-1003" not in text:
            return None
        match = _BAN_RE.search(text)
        return float(match.group(1)) if match else None

    def _is_rest_banned(self) -> bool:
        return self._rest_ban_until_ms > time.time() * 1000

    def _rest_ban_remaining_s(self) -> float:
        return max(0.0, self._rest_ban_until_ms / 1000 - time.time())

    def _note_possible_ip_ban(self, response_text: str):
        until_ms = self._parse_ban_until(response_text)
        if until_ms is None or until_ms <= self._rest_ban_until_ms:
            return
        self._rest_ban_until_ms = until_ms
        log.error(
            f"⛔ IP bloqueada por Binance (rate limit -1003) hasta {_utc_from_ms(until_ms)} "
            f"(~{self._rest_ban_remaining_s():.0f}s) — se omitirán llamadas REST hasta entonces"
        )

    def _check_rest_ban_or_raise(self):
        if self._is_rest_banned():
            raise RuntimeError(f"REST omitida: IP bloqueada por Binance (-1003), quedan ~{self._rest_ban_remaining_s():.0f}s")

    @staticmethod
    def _proxy_label(proxy_url: Optional[str]) -> str:
        if not proxy_url:
            return "directo (sin proxy)"
        return proxy_url.split("@", 1)[-1]

    def _is_proxy_banned(self, proxy_url: str) -> bool:
        return self._proxy_ban_until_ms.get(proxy_url, 0.0) > time.time() * 1000

    def _proxy_ban_remaining_s(self, proxy_url: str) -> float:
        return max(0.0, self._proxy_ban_until_ms.get(proxy_url, 0.0) / 1000 - time.time())

    def _note_possible_proxy_ban(self, proxy_url: str, response_text: str):
        until_ms = self._parse_ban_until(response_text)
        if until_ms is None or until_ms <= self._proxy_ban_until_ms.get(proxy_url, 0.0):
            return
        self._proxy_ban_until_ms[proxy_url] = until_ms
        log.error(
            f"⛔ IP {self._proxy_label(proxy_url)} bloqueada por Binance (-1003) hasta "
            f"{_utc_from_ms(until_ms)} (~{self._proxy_ban_remaining_s(proxy_url):.0f}s) "
            f"— se saltará a la siguiente IP de PROXY_URLS si hay alguna disponible"
        )

    def _ws_alive(self) -> bool:
        return bool(
            self._ws and not self._ws.closed
            and self._reader_task and not self._reader_task.done()
        )

    async def _ensure_http_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                connector=aiohttp.TCPConnector(limit=0, ttl_dns_cache=300),
            )
        return self._session

    async def connect(self):
        if self._ws_alive():
            return

        async with self._connect_lock:
            if self._ws_alive():
                return

            if self._ws is not None:
                try:
                    if not self._ws.closed:
                        await self._ws.close()
                except Exception:
                    pass
                self._ws = None
            if self._reader_task is not None and not self._reader_task.done():
                self._reader_task.cancel()

            session = await self._ensure_http_session()
            log.info(f"Conectando Binance WS API → {self.ws_url}")
            self._ws = await session.ws_connect(
                self.ws_url,
                autoping=True,
                heartbeat=30,
                max_msg_size=0,
                compress=0,
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
        ws = self._ws
        pending = self._pending
        while not self._closed:
            try:
                msg = await ws.receive()
            except Exception as e:
                log.error(f"WS reader error: {e}")
                break

            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    data = _loads(msg.data)
                except Exception:
                    log.warning(f"WS no JSON: {msg.data!r}")
                    continue

                req_id = data.get("id")
                fut = pending.pop(str(req_id), None) if req_id is not None else None
                if fut is not None and not fut.done():
                    fut.set_result(data)
            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                break

        err = ConnectionError("WebSocket API desconectado")
        for fut in list(pending.values()):
            if not fut.done():
                fut.set_exception(err)
        pending.clear()

    async def _request(self, method: str, params: Optional[dict] = None, signed: bool = False,
                       timeout: float = 20.0, _retry: bool = True) -> dict:
        await self.connect()

        p = dict(params) if params else {}
        if signed:
            p.setdefault("apiKey", self.api_key)
            p.setdefault("timestamp", _now_ms())
            p.setdefault("recvWindow", 5000)
            p["signature"] = self._sign(p)

        req_id = uuid.uuid4().hex
        payload = {"id": req_id, "method": method}
        if p:
            payload["params"] = p

        fut = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut

        try:
            await self._ws.send_str(_dumps(payload))
        except Exception as e:
            self._pending.pop(req_id, None)
            if _retry:
                log.warning(f"_request: fallo enviando ({e!r}); reconectando y reintentando una vez")
                return await self._request(method, params, signed, timeout, _retry=False)
            raise

        try:
            response = await asyncio.wait_for(fut, timeout=timeout)
        except Exception as e:
            self._pending.pop(req_id, None)
            if _retry and not self._ws_alive():
                log.warning(f"_request: sin respuesta ({e!r}); conexión muerta, reconectando y reintentando una vez")
                return await self._request(method, params, signed, timeout, _retry=False)
            raise

        if response.get("status") != 200:
            raise RuntimeError(f"Binance WS error {response.get('status')}: {response.get('error') or {}}")
        return response.get("result", response)

    async def _request_dict(self, method: str, params: dict) -> dict:
        result = await self._request(method, params, signed=True)
        return result if isinstance(result, dict) else {"raw": result}

    async def account_balance(self) -> list[dict]:
        result = await self._request("account.balance", signed=True)
        return result if isinstance(result, list) else []

    async def position_information(self, symbol: Optional[str] = None) -> list[dict]:
        result = await self._request("account.position", {"symbol": symbol} if symbol else {}, signed=True)
        return result if isinstance(result, list) else []

    async def set_leverage(self, symbol: str, leverage: int, force: bool = False) -> dict:
        leverage = int(leverage)
        if not force and self._leverage_cache.get(symbol) == leverage:
            return {"symbol": symbol, "leverage": leverage, "cached": True}

        async with self._leverage_lock:
            if not force and self._leverage_cache.get(symbol) == leverage:
                return {"symbol": symbol, "leverage": leverage, "cached": True}

            if not PROXY_URLS:
                data = await self._set_leverage_via_proxy(symbol, leverage, proxy_url=None)
                self._leverage_cache[symbol] = leverage
                return data

            candidates = [p for p in PROXY_URLS if not self._is_proxy_banned(p)]
            if not candidates:
                soonest = min(self._proxy_ban_remaining_s(p) for p in PROXY_URLS)
                raise RuntimeError(
                    f"REST omitida: IP bloqueada por Binance (-1003), quedan ~{soonest:.0f}s "
                    f"(las {len(PROXY_URLS)} IP(s) de PROXY_URLS están bloqueadas ahora mismo)"
                )

            last_err: Optional[Exception] = None
            for proxy_url in candidates:
                try:
                    data = await self._set_leverage_via_proxy(symbol, leverage, proxy_url=proxy_url)
                    self._leverage_cache[symbol] = leverage
                    return data
                except Exception as e:
                    last_err = e
                    if self._is_proxy_banned(proxy_url):
                        log.warning(
                            f"set_leverage: {symbol} — IP {self._proxy_label(proxy_url)} quedó bloqueada; "
                            f"probando con la siguiente IP de PROXY_URLS"
                        )
                        continue
                    if isinstance(e, (aiohttp.ClientError, asyncio.TimeoutError)):
                        log.warning(
                            f"set_leverage: {symbol} — fallo de conexión con {self._proxy_label(proxy_url)} "
                            f"({e!r}); probando con la siguiente IP de PROXY_URLS"
                        )
                        continue
                    raise
            raise last_err if last_err else RuntimeError("set_leverage: sin IPs disponibles en PROXY_URLS")

    async def _set_leverage_via_proxy(self, symbol: str, leverage: int, proxy_url: Optional[str]) -> dict:
        session = await self._ensure_http_session()
        params = {
            "symbol": symbol,
            "leverage": leverage,
            "timestamp": _now_ms(),
            "recvWindow": 5000,
        }
        url = f"{REST_FAPI_URL}/fapi/v1/leverage?{self._payload_string(params)}&signature={self._sign(params)}"

        request_kwargs = {"headers": self._headers, "timeout": aiohttp.ClientTimeout(total=10)}
        if proxy_url:
            request_kwargs["proxy"] = proxy_url

        async with session.post(url, **request_kwargs) as resp:
            text = await resp.text()
            if resp.status != 200:
                if proxy_url:
                    self._note_possible_proxy_ban(proxy_url, text)
                else:
                    self._note_possible_ip_ban(text)
                raise RuntimeError(f"REST set_leverage error {resp.status}: {text}")
            try:
                data = _loads(text)
            except Exception:
                data = {"raw": text}
            log.info(f"Leverage REST OK vía {self._proxy_label(proxy_url)}: {symbol} → {data.get('leverage', leverage)}x")
            return data

    async def set_leverage_with_fallback(self, symbol: str, preferred: int) -> int:
        ladder = [preferred] + [lv for lv in self.LEVERAGE_FALLBACK_LADDER if lv < preferred]
        last_err: Optional[Exception] = None
        for lv in ladder:
            try:
                await self.set_leverage(symbol, lv)
                if lv != preferred:
                    log.warning(f"set_leverage_with_fallback: {symbol} rechazó {preferred}x — aplicado {lv}x en su lugar")
                return lv
            except Exception as e:
                last_err = e
                log.warning(f"set_leverage_with_fallback: {symbol} rechazó {lv}x ({e}); probando siguiente de la escalera")
        log.error(f"set_leverage_with_fallback: {symbol} rechazó TODA la escalera de leverage ({ladder}): {last_err}")
        return preferred

    async def _rest_signed(self, http_method: str, path: str, params: dict, timeout: float = 10.0):
        self._check_rest_ban_or_raise()
        session = await self._ensure_http_session()
        params = dict(params or {})
        params.setdefault("timestamp", _now_ms())
        params.setdefault("recvWindow", 5000)
        url = f"{REST_FAPI_URL}{path}?{self._payload_string(params)}&signature={self._sign(params)}"
        async with session.request(http_method, url, headers=self._headers, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            text = await resp.text()
            if resp.status != 200:
                self._note_possible_ip_ban(text)
                raise RuntimeError(f"REST {http_method} {path} error {resp.status}: {text}")
            try:
                return _loads(text)
            except Exception:
                return {"raw": text}

    async def get_open_orders(self, symbol: Optional[str] = None) -> list[dict]:
        result = await self._rest_signed("GET", "/fapi/v1/openOrders", {"symbol": symbol} if symbol else {})
        return result if isinstance(result, list) else []

    async def cancel_all_open_orders(self, symbol: str) -> dict:
        return await self._rest_signed("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})

    async def create_tp_sl_order(
        self,
        symbol: str,
        side: str,
        trigger_price: float,
        order_type: str,
        position_side: Optional[str] = None,
        close_position: bool = True,
        quantity: Optional[float] = None,
        time_in_force: str = "GTC",
    ) -> dict:
        trigger_price = clamp_price(trigger_price)
        if trigger_price <= 0:
            raise ValueError("trigger_price inválido")

        use_close_position = close_position and quantity is None
        position_side = position_side or "BOTH"
        reduce_only = "true" if (not use_close_position and position_side not in ("LONG", "SHORT")) else None

        return await self.create_algo_order(
            symbol=symbol,
            side=side,
            order_type=order_type,
            triggerPrice=str(trigger_price),
            positionSide=position_side,
            closePosition="true" if use_close_position else None,
            quantity=None if use_close_position or quantity is None else str(quantity),
            reduceOnly=reduce_only,
            timeInForce=time_in_force,
            workingType="MARK_PRICE",
        )

    async def create_limit_order(
        self,
        symbol: str,
        side: str,
        quantity,
        price: float,
        position_side: Optional[str] = None,
        reduce_only: bool = False,
        time_in_force: str = "GTC",
    ) -> dict:
        params: dict = {
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "quantity": str(quantity),
            "price": str(price),
            "timeInForce": time_in_force,
            "newOrderRespType": "RESULT",
        }
        if position_side:
            params["positionSide"] = position_side
        if reduce_only and position_side not in ("LONG", "SHORT"):
            params["reduceOnly"] = "true"
        return await self._request_dict("order.place", params)

    async def create_algo_order(self, symbol: str, side: str, order_type: str, **extra) -> dict:
        params = {"symbol": symbol, "side": side, "algoType": "CONDITIONAL", "type": order_type}
        params.update({k: v for k, v in extra.items() if v is not None})
        return await self._request_dict("algoOrder.place", params)

    async def get_open_algo_orders(self, symbol: Optional[str] = None) -> list[dict]:
        result = await self._rest_signed("GET", "/fapi/v1/algoOpenOrders", {"symbol": symbol} if symbol else {})
        if isinstance(result, dict):
            return result.get("orders") or result.get("algoOrders") or []
        return result if isinstance(result, list) else []

    async def cancel_algo_order(self, algo_id) -> dict:
        return await self._request_dict("algoOrder.cancel", {"algoId": algo_id})

    async def cancel_all_algo_orders(self, symbol: str) -> dict:
        return await self._rest_signed("DELETE", "/fapi/v1/algoOpenOrders", {"symbol": symbol})

    async def cancel_symbol_orders(self, symbol: str) -> dict:
        normal, algo = await asyncio.gather(
            self.cancel_all_open_orders(symbol),
            self.cancel_all_algo_orders(symbol),
            return_exceptions=True,
        )
        result: dict = {"symbol": symbol}
        normal_err = normal if isinstance(normal, Exception) else None
        algo_err = algo if isinstance(algo, Exception) else None

        if normal_err:
            result["normal_error"] = str(normal_err)
        else:
            result["normal"] = normal
        if algo_err:
            result["algo_error"] = str(algo_err)
        else:
            result["algo"] = algo

        if normal_err and algo_err:
            raise RuntimeError(
                f"No se pudieron cancelar órdenes normales ni algo orders para {symbol}: {normal_err}; {algo_err}"
            )
        return result

    async def set_margin_type(self, symbol: str, margin_type: str) -> dict:
        return await self._rest_signed("POST", "/fapi/v1/marginType", {"symbol": symbol, "marginType": margin_type})

    async def get_position_mode(self) -> bool:
        result = await self._rest_signed("GET", "/fapi/v1/positionSide/dual", {})
        return bool(result.get("dualSidePosition"))

    async def set_position_mode(self, hedge: bool) -> dict:
        return await self._rest_signed(
            "POST", "/fapi/v1/positionSide/dual", {"dualSidePosition": "true" if hedge else "false"}
        )

    async def modify_position_margin(self, symbol: str, amount: float, position_side: str = "BOTH", add: bool = True) -> dict:
        params = {
            "symbol": symbol,
            "amount": str(abs(amount)),
            "type": 1 if add else 2,
            "positionSide": position_side,
        }
        return await self._rest_signed("POST", "/fapi/v1/positionMargin", params)

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
            "quantity": str(quantity),
            "newOrderRespType": new_order_resp_type,
        }
        if position_side:
            params["positionSide"] = position_side
        if reduce_only and position_side not in ("LONG", "SHORT"):
            params["reduceOnly"] = "true"
        return await self._request_dict("order.place", params)

    async def close_position_market(
        self,
        symbol: str,
        direction: str,
        quantity: float,
        position_side: Optional[str] = None,
    ) -> dict:
        return await self.create_market_order(
            symbol=symbol,
            side="SELL" if direction.upper() == "LONG" else "BUY",
            quantity=quantity,
            position_side=position_side,
            reduce_only=True,
        )

    async def close_all_positions(self, symbol: Optional[str] = None) -> list[dict]:
        positions = await self.position_information(symbol=symbol)
        coros = []
        for p in positions:
            try:
                amt = float(p.get("positionAmt", 0))
            except Exception:
                amt = 0.0
            if abs(amt) <= 0:
                continue

            pos_side = p.get("positionSide") or None
            if pos_side in ("LONG", "SHORT"):
                close_side = "SELL" if pos_side == "LONG" else "BUY"
            else:
                close_side = "SELL" if amt > 0 else "BUY"
                pos_side = "BOTH"

            coros.append(self.create_market_order(
                symbol=p.get("symbol", symbol or ""),
                side=close_side,
                quantity=abs(amt),
                position_side=pos_side,
                reduce_only=True,
            ))
        return list(await asyncio.gather(*coros)) if coros else []


def _utc_from_ms(ms: float) -> str:
    return time.strftime(_FMT_SEC, time.gmtime(ms / 1000))


class ExecutionManager:
    def __init__(self, binance_api, price_ws):
        self.api = binance_api
        self.price_ws = price_ws
        self._trades: dict[tuple[str, str], Trade] = {}
        self._closed: list[Trade] = []
        self._counter: int = 0
        self._lock = asyncio.Lock()
        self._balance: float = 0.0
        self._paper_id_map: dict[int, tuple[str, str]] = {}
        self.trading_enabled: bool = True
        self._last_balance_refresh: float = 0.0
        self._balance_refresh_lock = asyncio.Lock()

    async def refresh_balance(self, force: bool = False):
        if not force and (time.monotonic() - self._last_balance_refresh) < BALANCE_POLL_S:
            return
        async with self._balance_refresh_lock:
            if not force and (time.monotonic() - self._last_balance_refresh) < BALANCE_POLL_S:
                return
            try:
                for b in await self.api.account_balance():
                    if b.get("asset") == "USDT":
                        self._balance = float(b.get("availableBalance", b.get("balance", 0)))
                        break
                self._last_balance_refresh = time.monotonic()
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
        return [t for t in self._trades.values() if t.direction == "LONG"]

    @property
    def open_shorts(self) -> list[Trade]:
        return [t for t in self._trades.values() if t.direction == "SHORT"]

    @property
    def active_symbols(self) -> set:
        return {sym for sym, _d in self._trades}

    def trades_for_symbol(self, symbol: str) -> list[Trade]:
        symbol = symbol.upper()
        return [t for (sym, _d), t in self._trades.items() if sym == symbol]

    def get_trade(self, symbol: str, direction: Optional[str] = None) -> Optional[Trade]:
        symbol = symbol.upper()
        if direction:
            return self._trades.get((symbol, direction.upper()))
        matches = self.trades_for_symbol(symbol)
        return matches[0] if len(matches) == 1 else None

    @property
    def total_realized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self._closed)

    @property
    def unrealized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self._trades.values())

    @property
    def equity(self) -> float:
        return self._balance + self.unrealized_pnl

    def _sync_ws_symbols(self):
        try:
            self.price_ws.update_symbols(list(self.active_symbols))
        except Exception as e:
            log.error(f"_sync_ws_symbols: {e}")

    def _fresh_ws_price(self, symbol: str) -> float:
        try:
            p = self.price_ws.get_price(symbol, max_age_s=MAX_PRICE_AGE_S)
            return float(p) if p and p > 0 else 0.0
        except Exception:
            return 0.0

    async def get_entry_reference_price(
        self,
        symbol: str,
        extra_symbols: Optional[list[str]] = None,
        fallback_price: float = 0.0,
    ) -> float:
        p = self._fresh_ws_price(symbol)
        if p:
            return p

        def subscribe():
            try:
                wanted = self.active_symbols | {symbol}
                if extra_symbols:
                    wanted |= set(extra_symbols)
                self.price_ws.update_symbols(list(wanted))
            except Exception as e:
                log.warning(f"get_entry_reference_price: no se pudo suscribir {symbol} al WS: {e}")

        subscribe()

        loop = asyncio.get_running_loop()
        last_resub = loop.time()
        deadline = last_resub + 10.0
        while True:
            await asyncio.sleep(0.02)
            p = self._fresh_ws_price(symbol)
            if p:
                return p
            now = loop.time()
            if now >= deadline:
                break
            if now - last_resub >= 3.0:
                subscribe()
                last_resub = now

        if fallback_price and fallback_price > 0:
            log.warning(
                f"get_entry_reference_price: {symbol} sin precio WS tras esperar — se usa el precio de la señal "
                f"({fallback_price}) como último recurso para NO cancelar la apertura"
            )
            return float(fallback_price)

        log.error(
            f"get_entry_reference_price: {symbol} sin precio WS y sin fallback_price disponible "
            f"— no es posible abrir sin ningún precio"
        )
        return 0.0

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
        try:
            return await self.api.create_market_order(
                symbol=symbol, side=side, quantity=qty_str,
                position_side=position_side, reduce_only=reduce_only,
            )
        except Exception as e:
            err = str(e)
            if not any(code in err for code in _RETRYABLE_ORDER_ERRORS):
                raise

            log.warning(f"_place_market_order_safe: {symbol} rechazada ({err}); recalculando con colchón mayor y reintentando una vez")

            try:
                fresh_price = float(self.price_ws.get_price(symbol) or 0.0) or ref_price
            except Exception:
                fresh_price = ref_price

            retry_buffer = max(NOTIONAL_SAFETY_BUFFER_PCT * 5, 10.0)
            min_notional = max(float(filters.get("min_notional", MIN_NOTIONAL_USDT)), MIN_NOTIONAL_USDT)
            target_notional = min_notional * (1 + retry_buffer / 100.0)

            retry_qty, retry_notional = resolve_safe_quantity(target_notional, fresh_price, filters)
            retry_qty_str = format_qty(retry_qty, filters.get("stepSize", 0.001))
            log.info(f"_place_market_order_safe: reintento {symbol} qty={retry_qty_str} (notional≈${retry_notional:.4f}, precio={fresh_price})")

            return await self.api.create_market_order(
                symbol=symbol, side=side, quantity=retry_qty_str,
                position_side=position_side, reduce_only=reduce_only,
            )

    @staticmethod
    def _is_margin_insufficient(err: str) -> bool:
        return "-2019" in err or "Margin is insufficient" in err

    async def _place_entry_order(
        self,
        symbol: str,
        side: str,
        qty_str: str,
        direction: str,
        filters: dict,
        ref_price: float,
    ) -> tuple[Optional[dict], bool, bool]:
        try:
            result = await self._place_market_order_safe(
                symbol=symbol, side=side, qty_str=qty_str,
                position_side=direction, filters=filters, ref_price=ref_price,
            )
            return result, True, False
        except Exception as e_hedge:
            if self._is_margin_insufficient(str(e_hedge)):
                log.warning(f"_place_entry_order: {symbol} margen insuficiente en modo Hedge — posición asumida: {e_hedge}")
                return None, True, True

            log.warning(
                f"_place_entry_order: {symbol} rechazada en modo Hedge ({e_hedge}) por un motivo "
                f"distinto a margen — reintentando como One-way (positionSide=BOTH)"
            )
            try:
                result = await self._place_market_order_safe(
                    symbol=symbol, side=side, qty_str=qty_str,
                    position_side="BOTH", filters=filters, ref_price=ref_price,
                )
                return result, False, False
            except Exception as e_oneway:
                if self._is_margin_insufficient(str(e_oneway)):
                    log.warning(f"_place_entry_order: {symbol} margen insuficiente en reintento One-way — posición asumida: {e_oneway}")
                    return None, False, True
                log.error(f"_place_entry_order: {symbol} falló tanto en Hedge como en One-way: {e_oneway}")
                raise

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

        if is_symbol_blocked(symbol):
            log.warning(f"open_trade: {symbol} está en BLOCKED_SYMBOLS — apertura cancelada")
            return None

        ref_price = await self.get_entry_reference_price(symbol, fallback_price=price)
        if ref_price <= 0:
            log.error(f"open_trade: no se pudo obtener NINGÚN precio para {symbol} (ni WS ni precio de señal) — apertura cancelada")
            return None

        if price > 0 and abs(ref_price - price) / max(price, 1e-9) > 0.01:
            log.info(f"open_trade: precio de señal={price} es solo orientativo — se abre con precio real={ref_price} para {symbol}")

        desired_notional = (price if price > 0 else ref_price) * quantity
        filled_price = ref_price

        filters = filters_for_price(ref_price)
        step_size = float(filters["stepSize"])
        target_leverage = leverage_for_price(ref_price)

        try:
            send_qty, send_notional = resolve_safe_quantity(
                desired_notional, ref_price, filters, extra_buffer_pct=NOTIONAL_SAFETY_BUFFER_PCT
            )
        except Exception as e:
            log.error(f"open_trade: no se pudo calcular quantity segura para {symbol}: {e}")
            return None

        log.info(f"open_trade: {symbol} precio={ref_price} → stepSize={step_size}, leverage objetivo={target_leverage}x")
        applied_leverage = await self.api.set_leverage_with_fallback(symbol, target_leverage)

        if abs(send_qty - quantity) > 1e-12:
            log.info(
                f"open_trade: quantity ajustada para {symbol} → señal={quantity} (notional≈${desired_notional:.4f}) "
                f"→ enviada={send_qty} (notional≈${send_notional:.4f}, precio_ref={ref_price})"
            )
        quantity = send_qty

        qty_str = format_qty(quantity, step_size)
        log.info(f"open_trade: enviando MARKET por WS → {symbol} {side} qty={qty_str} (notional≈${send_notional:.4f}) [intento Hedge]")
        try:
            result, used_hedge, order_assumed = await self._place_entry_order(
                symbol=symbol, side=side, qty_str=qty_str,
                direction=direction, filters=filters, ref_price=ref_price,
            )
        except Exception as e_ord:
            log.error(f"open_trade: fallo enviando MARKET (Hedge y fallback One-way) para {symbol}: {e_ord}")
            return None

        if order_assumed:
            log.warning(f"[ASUMIDA] {symbol} — posición registrada como abierta pese a margen insuficiente")
            entry_order_id = "MARGIN_INSUFFICIENT"
        else:
            entry_order_id = str(result.get("orderId", result.get("clientOrderId", "WS_ORDER")))
            try:
                avg_f = float(result.get("avgPrice") or result.get("price"))
                if avg_f > 0:
                    filled_price = avg_f
            except Exception:
                pass
            try:
                executed_qty = float(result.get("origQty") or result.get("executedQty") or quantity)
                if executed_qty > 0:
                    quantity = executed_qty
            except Exception:
                pass
            log.info(
                f"MARKET WS OK: {symbol} {side} qty={quantity} id={entry_order_id} avg={filled_price} "
                f"modo={'Hedge' if used_hedge else 'One-way (fallback)'}"
            )

        def new_trade() -> Trade:
            self._counter += 1
            return Trade(
                id=self._counter,
                symbol=symbol,
                direction=direction,
                entry_price=filled_price,
                quantity=quantity,
                open_time=_utc(),
                leverage=applied_leverage,
                paper_trade_id=paper_trade_id,
                entry_order_id=entry_order_id,
                current_price=filled_price,
                order_assumed=order_assumed,
                hedge_mode=used_hedge,
                step_size=step_size,
            )

        cancel_residual = False
        async with self._lock:
            key = (symbol, direction)

            if used_hedge:
                existing = self._trades.get(key)
                if existing is None:
                    trade = new_trade()
                    self._trades[key] = trade
                    action_tag = "ABIERTO"
                else:
                    existing.step_size = min(existing.step_size, step_size)
                    new_qty = round(existing.quantity + quantity, _step_decimals(existing.step_size))
                    existing.entry_price = (
                        existing.entry_price * existing.quantity + filled_price * quantity
                    ) / new_qty
                    existing.quantity = new_qty
                    existing.leverage = applied_leverage
                    existing.entry_order_id = entry_order_id
                    existing.order_assumed = existing.order_assumed or order_assumed
                    existing.hedge_mode = True
                    trade = existing
                    action_tag = "AMPLIADO"
                self._paper_id_map[paper_trade_id] = key
            else:
                existing = next((t for (sym, _d), t in self._trades.items() if sym == symbol), None)

                if existing is None:
                    trade = new_trade()
                    self._trades[key] = trade
                    self._paper_id_map[paper_trade_id] = key
                    action_tag = "ABIERTO"
                else:
                    old_key = (existing.symbol, existing.direction)
                    old_signed = existing.quantity if existing.direction == "LONG" else -existing.quantity
                    delta_signed = quantity if direction == "LONG" else -quantity
                    existing.step_size = min(existing.step_size, step_size)
                    new_signed = round(old_signed + delta_signed, _step_decimals(existing.step_size))

                    if abs(new_signed) < 1e-9:
                        existing.status = "NETTED"
                        existing.close_price = filled_price
                        existing.close_time = _utc()
                        existing.update_unrealized(filled_price)
                        del self._trades[old_key]
                        self._paper_id_map.pop(existing.paper_trade_id, None)
                        self._closed.append(existing)
                        log.info(f"open_trade: {symbol} neteado a 0 con esta señal — posición cerrada")
                        trade = None
                        action_tag = "NETEADO"
                        cancel_residual = True
                    else:
                        new_direction = "LONG" if new_signed > 0 else "SHORT"
                        new_qty = abs(new_signed)
                        if new_direction == existing.direction:
                            existing.entry_price = (
                                existing.entry_price * existing.quantity + filled_price * quantity
                            ) / new_qty
                            action_tag = "AMPLIADO"
                        else:
                            existing.entry_price = filled_price
                            action_tag = "INVERTIDO"
                        existing.direction = new_direction
                        existing.quantity = new_qty
                        existing.leverage = applied_leverage
                        existing.entry_order_id = entry_order_id
                        existing.order_assumed = existing.order_assumed or order_assumed
                        existing.hedge_mode = False

                        new_key = (symbol, new_direction)
                        if new_key != old_key:
                            del self._trades[old_key]
                            self._trades[new_key] = existing
                        self._paper_id_map[paper_trade_id] = new_key
                        trade = existing

        self._sync_ws_symbols()
        _spawn(self.refresh_balance())

        if cancel_residual:
            try:
                await self.api.cancel_symbol_orders(symbol)
            except Exception as e:
                log.warning(f"open_trade: no se pudieron cancelar órdenes residuales de {symbol} tras neteo a 0: {e}")

        if trade is None:
            return None

        assumed_tag = " [ASUMIDA — MARGIN INSUF]" if order_assumed else ""
        log.info(
            f"[REAL #{trade.id}] {action_tag} {trade.direction} {symbol} @ ${filled_price} | "
            f"Qty total: {trade.quantity} | Lev: {applied_leverage}x | OrderId: {entry_order_id} | "
            f"Paper#{paper_trade_id}{assumed_tag}"
        )
        return trade

    async def close_trade(self, trade: Trade, close_price: float, reason: str) -> bool:
        async with self._lock:
            key = (trade.symbol, trade.direction)
            if trade.status != "OPEN" or self._trades.get(key) is not trade:
                return False

            trade.status = reason
            trade.close_price = close_price
            trade.close_time = _utc()
            trade.pnl_usdt = trade.calc_pnl(close_price)
            notional = trade.notional_usdt
            trade.roi_pct = trade.pnl_usdt / notional * 100 if notional else 0.0

            del self._trades[key]
            self._paper_id_map.pop(trade.paper_trade_id, None)
            self._closed.append(trade)

        try:
            await self.api.cancel_symbol_orders(trade.symbol)
        except Exception as e:
            log.warning(f"close_trade: no se pudieron cancelar órdenes residuales de {trade.symbol}: {e}")

        self._sync_ws_symbols()
        log.info(
            f"[REAL #{trade.id}] CERRADO {reason} {trade.symbol} @ ${close_price} | "
            f"PnL: {trade.pnl_usdt:+.4f} USDT ({trade.roi_pct:+.2f}%)"
        )
        _spawn(self.refresh_balance())
        return True

    async def force_close_trade(self, trade: Trade, reason: str = "MAIN_BOT", close_price: Optional[float] = None) -> bool:
        if not (close_price and close_price > 0):
            close_price = trade.current_price if trade.current_price > 0 else trade.entry_price
        try:
            await self.api.cancel_symbol_orders(trade.symbol)
        except Exception as e:
            log.warning(f"force_close: no se pudieron cancelar órdenes previas de {trade.symbol}: {e}")
        try:
            await self.api.close_position_market(
                symbol=trade.symbol,
                direction=trade.direction,
                quantity=trade.quantity,
                position_side=trade.position_side,
            )
            log.info(f"force_close: close_position_market OK para {trade.symbol}")
        except Exception as e:
            log.warning(f"force_close: error cerrando {trade.symbol}: {e}")
        return await self.close_trade(trade, close_price, reason)

    async def force_close_by_symbol(self, symbol: str) -> bool:
        try:
            await self.api.cancel_symbol_orders(symbol)
        except Exception as e:
            log.warning(f"force_close_by_symbol: no se pudieron cancelar órdenes previas de {symbol}: {e}")
        try:
            closed = await self.api.close_all_positions(symbol=symbol)
            log.info(f"force_close_by_symbol {symbol}: {closed}")
            return bool(closed)
        except Exception as e:
            log.error(f"force_close_by_symbol {symbol}: {e}")
            return False
        finally:
            try:
                await self.api.cancel_symbol_orders(symbol)
            except Exception as e:
                log.warning(f"force_close_by_symbol: no se pudieron cancelar órdenes residuales de {symbol}: {e}")

    async def close_all_global(self, reason: str = "CLOSE_ALL") -> list[Trade]:
        async with self._lock:
            snapshot = list(self._trades.values())

        async def _close_one(trade: Trade) -> Optional[Trade]:
            try:
                if await self.force_close_trade(trade, reason=reason):
                    log.info(f"close_all_global: cerrado {trade.symbol} #{trade.id}")
                    return trade
            except Exception as e:
                log.error(f"close_all_global: error cerrando {trade.symbol}: {e}")
            return None

        results = await asyncio.gather(*(_close_one(t) for t in snapshot))
        closed_trades = [t for t in results if t is not None]
        log.info(f"close_all_global: {len(closed_trades)}/{len(snapshot)} posiciones cerradas")
        return closed_trades

    async def poll_positions(self) -> list[Trade]:
        if not self._trades:
            return []

        try:
            positions = await self.api.position_information()
        except Exception as e:
            log.error(f"poll_positions: {e}")
            return []

        live_keys: set[tuple[str, str]] = set()
        for p in positions:
            sym = p.get("symbol", "")
            try:
                amt = float(p.get("positionAmt", 0))
            except Exception:
                amt = 0.0
            if not sym or abs(amt) <= 0:
                continue
            pos_side = p.get("positionSide", "BOTH")
            direction = ("LONG" if amt > 0 else "SHORT") if pos_side == "BOTH" else pos_side
            live_keys.add((sym, direction))

        return [t for key, t in list(self._trades.items()) if key not in live_keys]

    def find_by_paper_id(self, paper_trade_id: int) -> Optional[Trade]:
        key = self._paper_id_map.get(paper_trade_id)
        return self._trades.get(key) if key else None


execution_manager: Optional[ExecutionManager] = None

_STATUS_KEYS = (
    "signals_received", "signals_open", "signals_close", "signals_rejected", "manual_closes",
    "signals_tp_set", "signals_tp_closed", "signals_sl_set", "signals_sl_closed",
)
executor_status = {k: 0 for k in _STATUS_KEYS}
executor_status.update({
    "last_signal_time": "Esperando señales...",
    "last_signal_detail": "",
    "started_at": _utc(_FMT_SEC),
})

_TG_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
_tg_session: Optional[aiohttp.ClientSession] = None


async def send_telegram(message: str):
    global _tg_session
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if _tg_session is None or _tg_session.closed:
        _tg_session = aiohttp.ClientSession()
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with _tg_session.post(_TG_URL, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
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


_CLOSE_REASONS = {
    "TP": ("✅", "TAKE PROFIT 🎯"),
    "SL": ("❌", "STOP LOSS 🛑"),
    "CLOSED": ("🔄", "CIERRE EXTERNO"),
    "MAIN_BOT": ("🔄", "CIERRE SEÑAL PRINCIPAL"),
    "CLOSE_ALL": ("🛑", "CIERRE GLOBAL (SEÑAL)"),
    "MANUAL": ("🖐", "CIERRE MANUAL (DASHBOARD)"),
    "NETTED": ("➖", "NETEADA POR SEÑAL OPUESTA"),
}


def build_close_message(trade: Trade) -> str:
    emoji, reason_str = _CLOSE_REASONS.get(trade.status, ("⚠️", trade.status))
    dir_str = "🟢 LONG" if trade.direction == "LONG" else "🔴 SHORT"
    pnl_emoji = "💚" if trade.pnl_usdt >= 0 else "❗"

    closed_all = execution_manager._closed
    total = len(closed_all)
    wins = sum(1 for t in closed_all if t.status == "TP")
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


async def position_monitor_loop():
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
                    f"⚠️ <b>POSIBLE CIERRE EXTERNO DETECTADO</b>\n"
                    f"📊 <code>{trade.symbol}</code> (Real #{trade.id} | Paper #{trade.paper_trade_id})\n"
                    f"Ya no aparece entre tus posiciones de Binance, pero el executor sigue registrándola como abierta.\n"
                    f"➡️ No se cerró automáticamente."
                )

            alerted &= (missing_symbols | execution_manager.active_symbols)
        except Exception as e:
            log.error(f"position_monitor_loop: {e}")

        await asyncio.sleep(POSITION_POLL_S)


async def balance_sync_loop():
    log.info(f"Balance Sync Loop — refrescando balance cada {BALANCE_POLL_S}s")
    while True:
        await execution_manager.refresh_balance(force=True)
        await asyncio.sleep(BALANCE_POLL_S)


async def price_sync_loop():
    log.info("Price Sync Loop — actualizando PnL desde caché WS cada 1s")
    while True:
        try:
            get_price = execution_manager.price_ws.get_price
            for trade in list(execution_manager._trades.values()):
                price = get_price(trade.symbol)
                if price:
                    trade.update_unrealized(price)
        except Exception as e:
            log.error(f"price_sync_loop: {e}")
        await asyncio.sleep(1)


def _json_err(error: str, status: int = 400) -> web.Response:
    return web.json_response({"ok": False, "error": error}, status=status)


def _check_dashboard_token(request: web.Request) -> bool:
    return request.headers.get("X-Dashboard-Token", "") == SIGNAL_SECRET


async def _auth_json(request: web.Request):
    if not _check_dashboard_token(request):
        return None, _json_err("unauthorized", 401)
    try:
        return await request.json(), None
    except Exception:
        return None, _json_err("invalid json", 400)


_TP_SL_TYPES = {"open_tp": "TAKE_PROFIT_MARKET", "close_tp": "TAKE_PROFIT_MARKET",
                "open_sl": "STOP_MARKET", "close_sl": "STOP_MARKET"}
_NO_DIR_HINT = "(si hay LONG y SHORT simultáneas, especifica 'direction')"


async def signal_handler(request: web.Request) -> web.Response:
    if request.headers.get("X-Signal-Secret", "") != SIGNAL_SECRET:
        return _json_err("unauthorized", 401)

    try:
        data = await request.json()
    except Exception:
        return _json_err("invalid json", 400)

    action = data.get("action", "").lower()
    symbol = data.get("symbol", "").upper()
    trade_id = int(data.get("trade_id", 0))

    executor_status["signals_received"] += 1
    executor_status["last_signal_time"] = _utc("%H:%M:%S UTC")
    executor_status["last_signal_detail"] = f"{action.upper()} {symbol}"

    em = execution_manager

    if action == "open":
        direction = data.get("direction", "").upper()
        price = float(data.get("price", 0))
        quantity = float(data.get("quantity", 0))

        if not symbol or not direction or price <= 0 or quantity <= 0:
            executor_status["signals_rejected"] += 1
            return _json_err("missing or invalid open params", 400)

        if is_symbol_blocked(symbol):
            executor_status["signals_rejected"] += 1
            log.warning(f"Señal OPEN ignorada para {symbol} — símbolo bloqueado (BLOCKED_SYMBOLS)")
            return _json_err(f"{symbol} está bloqueado para operar", 200)

        if not em.trading_enabled:
            executor_status["signals_rejected"] += 1
            log.warning(f"Señal OPEN ignorada para {symbol} — trading pausado manualmente desde el dashboard")
            return _json_err("trading pausado manualmente desde el dashboard", 200)

        async def _do_open():
            trade = await em.open_trade(symbol, direction, price, quantity, paper_trade_id=trade_id)
            if trade:
                executor_status["signals_open"] += 1
                await send_telegram(build_open_message(trade))
            else:
                executor_status["signals_rejected"] += 1

        _spawn(_do_open())
        return web.json_response({"ok": True, "action": "open", "symbol": symbol, "direction": direction})

    if action == "close":
        reason = data.get("reason", "MAIN_BOT").upper()
        close_price = float(data.get("close_price", 0))
        trade = em.find_by_paper_id(trade_id) or em.get_trade(symbol, data.get("direction"))

        async def _do_close():
            if trade:
                use_price = close_price if close_price > 0 else trade.current_price or trade.entry_price
                if await em.force_close_trade(trade, reason=reason, close_price=use_price):
                    executor_status["signals_close"] += 1
                    await send_telegram(build_close_message(trade))
            elif await em.force_close_by_symbol(symbol):
                executor_status["signals_close"] += 1
                await send_telegram(f"🔄 <b>CIERRE FORZADO (sin estado local)</b>\n<code>{symbol}</code>")

        _spawn(_do_close())
        return web.json_response({"ok": True, "action": "close", "symbol": symbol})

    if action == "close_all":
        total_open = len(em._trades)

        async def _do_close_all():
            closed_trades = await em.close_all_global(reason="CLOSE_ALL")
            if not closed_trades:
                await send_telegram("🛑 CIERRE GLOBAL ejecutado — no había posiciones abiertas.")
                return
            for t in closed_trades:
                await send_telegram(build_close_message(t))
            await send_telegram(f"🛑 CIERRE GLOBAL completado — {len(closed_trades)} posición(es) cerrada(s).")

        _spawn(_do_close_all())
        executor_status["signals_close"] += total_open
        return web.json_response({"ok": True, "action": "close_all", "positions_targeted": total_open})

    if action in ("open_tp", "open_sl"):
        order_type = _TP_SL_TYPES[action]
        trigger_price = float(data.get("trigger_price", 0))
        trade = em.get_trade(symbol, data.get("direction"))

        if not trade or trigger_price <= 0:
            executor_status["signals_rejected"] += 1
            return _json_err(f"{action}: sin posición abierta para {symbol} o trigger_price inválido", 400)

        async def _do_open_algo():
            try:
                await _algo_set_tp_sl(trade, trigger_price, order_type)
                executor_status["signals_tp_set" if action == "open_tp" else "signals_sl_set"] += 1
                emoji, label = ("🎯", "TP") if action == "open_tp" else ("🛑", "SL")
                await send_telegram(f"{emoji} <b>{label} actualizado</b>\n<code>{symbol}</code> @ {trigger_price}")
            except Exception as e:
                executor_status["signals_rejected"] += 1
                log.error(f"signal {action}: fallo para {symbol}: {e}")

        _spawn(_do_open_algo())
        return web.json_response({"ok": True, "action": action, "symbol": symbol, "trigger_price": trigger_price})

    if action in ("close_tp", "close_sl"):
        order_type = _TP_SL_TYPES[action]

        if not symbol:
            executor_status["signals_rejected"] += 1
            return _json_err(f"{action}: falta symbol", 400)

        async def _do_close_algo():
            try:
                n = await _algo_cancel_tp_sl(symbol, order_type)
                executor_status["signals_tp_closed" if action == "close_tp" else "signals_sl_closed"] += 1
                emoji, label = ("🎯", "TP") if action == "close_tp" else ("🛑", "SL")
                await send_telegram(f"{emoji} <b>{label} cancelado</b>\n<code>{symbol}</code> — {n} orden(es)")
            except Exception as e:
                executor_status["signals_rejected"] += 1
                log.error(f"signal {action}: fallo para {symbol}: {e}")

        _spawn(_do_close_algo())
        return web.json_response({"ok": True, "action": action, "symbol": symbol})

    executor_status["signals_rejected"] += 1
    return _json_err(f"unknown action: {action}", 400)


async def manual_close_handler(request: web.Request) -> web.Response:
    data, err = await _auth_json(request)
    if err:
        return err

    symbol = data.get("symbol", "").upper()
    trade = execution_manager.get_trade(symbol, data.get("direction"))
    if not trade:
        return _json_err(f"no hay posición abierta registrada para {symbol} {_NO_DIR_HINT}", 404)

    async def _do_manual_close():
        if await execution_manager.force_close_trade(trade, reason="MANUAL"):
            executor_status["signals_close"] += 1
            executor_status["manual_closes"] += 1
            await send_telegram(build_close_message(trade))

    _spawn(_do_manual_close())
    return web.json_response({"ok": True, "action": "manual_close", "symbol": symbol})


async def manual_close_all_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)

    total_open = len(execution_manager._trades)

    async def _do_close_all():
        closed_trades = await execution_manager.close_all_global(reason="MANUAL")
        if not closed_trades:
            await send_telegram("🖐 Cierre manual global ejecutado — no había posiciones abiertas.")
            return
        for t in closed_trades:
            await send_telegram(build_close_message(t))
        await send_telegram(f"🖐 Cierre manual global completado — {len(closed_trades)} posición(es) cerrada(s).")

    _spawn(_do_close_all())
    executor_status["signals_close"] += total_open
    executor_status["manual_closes"] += total_open
    return web.json_response({"ok": True, "action": "manual_close_all", "positions_targeted": total_open})


def _position_side_for(direction: str) -> Optional[str]:
    return direction.upper() if HEDGE_MODE else "BOTH"


async def _algo_set_tp_sl(trade: "Trade", trigger_price: float, order_type: str) -> dict:
    api = execution_manager.api
    symbol = trade.symbol

    try:
        stale = [o.get("algoId") for o in await api.get_open_algo_orders(symbol) if o.get("type") == order_type]
        if stale:
            await asyncio.gather(*(api.cancel_algo_order(i) for i in stale))
    except Exception as e:
        log.warning(f"_algo_set_tp_sl: no se pudo limpiar {order_type} previo de {symbol}: {e}")

    return await api.create_tp_sl_order(
        symbol=symbol,
        side="SELL" if trade.direction == "LONG" else "BUY",
        trigger_price=trigger_price,
        order_type=order_type,
        position_side=trade.position_side,
        quantity=float(format_qty(trade.quantity, trade.step_size)),
    )


async def _algo_cancel_tp_sl(symbol: str, order_type: str) -> int:
    api = execution_manager.api
    ids = [o.get("algoId") for o in await api.get_open_algo_orders(symbol) if o.get("type") == order_type]
    if ids:
        await asyncio.gather(*(api.cancel_algo_order(i) for i in ids))
    return len(ids)


async def _manual_set_algo(request: web.Request, order_type: str, result_key: str, label: str) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        trigger_price = float(data.get("trigger_price", 0))
    except Exception:
        return _json_err("invalid json", 400)

    trade = execution_manager.get_trade(symbol, data.get("direction"))
    if not trade:
        return _json_err(f"sin posición abierta para {symbol} {_NO_DIR_HINT}", 404)
    if trigger_price <= 0:
        return _json_err("trigger_price inválido", 400)

    try:
        result = await _algo_set_tp_sl(trade, trigger_price, order_type)
        return web.json_response({"ok": True, "symbol": symbol, result_key: trigger_price, "result": result})
    except Exception as e:
        log.error(f"manual_set_{result_key}: fallo creando {label} para {symbol}: {e}")
        return _json_err(str(e), 502)


async def manual_set_tp_handler(request: web.Request) -> web.Response:
    return await _manual_set_algo(request, "TAKE_PROFIT_MARKET", "tp", "TP")


async def manual_set_sl_handler(request: web.Request) -> web.Response:
    return await _manual_set_algo(request, "STOP_MARKET", "sl", "SL")


async def manual_cancel_tp_sl_handler(request: web.Request) -> web.Response:
    data, err = await _auth_json(request)
    if err:
        return err
    try:
        symbol = data.get("symbol", "").upper()
        direction = data.get("direction")
    except Exception:
        return _json_err("invalid json", 400)

    trade_for_dir = execution_manager.get_trade(symbol, direction) if direction else None
    if trade_for_dir is not None:
        wanted_pos_side = direction.upper() if trade_for_dir.hedge_mode else None
    else:
        wanted_pos_side = direction.upper() if (direction and HEDGE_MODE) else None

    api = execution_manager.api
    try:
        ids = [
            o.get("algoId")
            for o in await api.get_open_algo_orders(symbol)
            if o.get("type") in ("STOP_MARKET", "TAKE_PROFIT_MARKET")
            and not (wanted_pos_side and o.get("positionSide") not in (wanted_pos_side, None))
        ]
        if ids:
            await asyncio.gather(*(api.cancel_algo_order(i) for i in ids))
        return web.json_response({"ok": True, "symbol": symbol, "cancelled": len(ids)})
    except Exception as e:
        return _json_err(str(e), 502)


async def manual_limit_order_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        side = data.get("side", "").upper()
        price = float(data.get("price", 0))
        quantity = float(data.get("quantity", 0))
        reduce_only = bool(data.get("reduce_only", False))
    except Exception:
        return _json_err("invalid json", 400)

    if not symbol or side not in ("BUY", "SELL") or price <= 0 or quantity <= 0:
        return _json_err("parámetros inválidos", 400)

    trade = execution_manager.get_trade(symbol, data.get("direction"))
    pos_side = trade.position_side if trade else ("BOTH" if not HEDGE_MODE else None)
    try:
        result = await execution_manager.api.create_limit_order(
            symbol=symbol, side=side, quantity=quantity, price=price,
            position_side=pos_side, reduce_only=reduce_only,
        )
        return web.json_response({"ok": True, "result": result})
    except Exception as e:
        log.error(f"manual_limit_order: fallo en LIMIT {symbol}: {e}")
        return _json_err(str(e), 502)


async def manual_set_symbol_leverage_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        leverage = int(data.get("leverage", 0))
    except Exception:
        return _json_err("invalid json", 400)

    if not symbol or leverage <= 0:
        return _json_err("parámetros inválidos", 400)

    try:
        applied = await execution_manager.api.set_leverage_with_fallback(symbol, leverage)
        for trade in execution_manager.trades_for_symbol(symbol):
            trade.leverage = applied
        return web.json_response({"ok": True, "symbol": symbol, "requested": leverage, "applied": applied})
    except Exception as e:
        return _json_err(str(e), 502)


async def manual_get_position_mode_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)

    try:
        account_hedge = await execution_manager.api.get_position_mode()
    except Exception as e:
        return _json_err(str(e), 502)

    return web.json_response({
        "ok": True,
        "account_hedge_mode": account_hedge,
        "local_hedge_mode": HEDGE_MODE,
        "in_sync": account_hedge == HEDGE_MODE,
        "open_positions": len(execution_manager._trades),
    })


async def manual_set_position_mode_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)

    try:
        data = await request.json()
        hedge_mode = bool(data.get("hedge_mode"))
    except Exception:
        return _json_err("invalid json", 400)

    open_count = len(execution_manager._trades)
    if open_count > 0:
        return _json_err(
            f"No se puede cambiar el modo de posición con {open_count} posición(es) "
            f"abierta(s) localmente. Cierra todas las posiciones primero (Binance "
            f"rechaza este cambio si la cuenta tiene posiciones u órdenes activas).",
            409,
        )

    try:
        await execution_manager.api.set_position_mode(hedge_mode)
    except Exception as e:
        err = str(e)
        if "-4059" in err:
            set_hedge_mode_runtime(hedge_mode)
            return web.json_response({"ok": True, "hedge_mode": hedge_mode, "note": "la cuenta ya estaba en ese modo"})
        return _json_err(
            f"Binance rechazó el cambio: {err}",
            409 if ("-4068" in err or "-4067" in err or "position" in err.lower()) else 502,
        )

    set_hedge_mode_runtime(hedge_mode)
    log.info(f"manual_set_position_mode_handler: modo de posición cambiado a {'HEDGE' if hedge_mode else 'ONE-WAY'}")
    return web.json_response({"ok": True, "hedge_mode": hedge_mode})


async def manual_set_margin_type_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        margin_type = data.get("margin_type", "").upper()
    except Exception:
        return _json_err("invalid json", 400)

    if margin_type not in ("ISOLATED", "CROSSED"):
        return _json_err("margin_type debe ser ISOLATED o CROSSED", 400)

    try:
        result = await execution_manager.api.set_margin_type(symbol, margin_type)
        return web.json_response({"ok": True, "symbol": symbol, "margin_type": margin_type, "result": result})
    except Exception as e:
        if "-4046" in str(e):
            return web.json_response({"ok": True, "symbol": symbol, "margin_type": margin_type, "note": "ya estaba en ese modo"})
        return _json_err(str(e), 502)


async def manual_modify_margin_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)
    try:
        data = await request.json()
        symbol = data.get("symbol", "").upper()
        amount = float(data.get("amount", 0))
        add = bool(data.get("add", True))
    except Exception:
        return _json_err("invalid json", 400)

    trade = execution_manager.get_trade(symbol, data.get("direction"))
    if not trade or amount <= 0:
        return _json_err(f"posición no encontrada o monto inválido {_NO_DIR_HINT}", 400)

    try:
        result = await execution_manager.api.modify_position_margin(
            symbol, amount, position_side=_position_side_for(trade.direction), add=add
        )
        return web.json_response({"ok": True, "symbol": symbol, "amount": amount, "add": add, "result": result})
    except Exception as e:
        return _json_err(str(e), 502)


async def manual_get_orders_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)
    symbol = request.query.get("symbol", "").upper()
    if not symbol:
        return _json_err("symbol requerido", 400)
    try:
        normal_orders, algo_orders = await asyncio.gather(
            execution_manager.api.get_open_orders(symbol),
            execution_manager.api.get_open_algo_orders(symbol),
            return_exceptions=True,
        )
        normal_orders = normal_orders if isinstance(normal_orders, list) else []
        algo_orders = algo_orders if isinstance(algo_orders, list) else []
        for o in algo_orders:
            o["_algo"] = True
        return web.json_response({"ok": True, "symbol": symbol, "orders": normal_orders + algo_orders})
    except Exception as e:
        return _json_err(str(e), 502)


async def manual_toggle_trading_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)

    execution_manager.trading_enabled = not execution_manager.trading_enabled
    state = "ACTIVADO 🟢" if execution_manager.trading_enabled else "PAUSADO 🔴 (no se enviarán nuevas posiciones)"
    log.warning(f"Trading {state} manualmente desde el dashboard")
    return web.json_response({"ok": True, "trading_enabled": execution_manager.trading_enabled})


async def manual_set_leverage_handler(request: web.Request) -> web.Response:
    global LEVERAGE
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)

    try:
        data = await request.json()
        new_lev = int(data.get("leverage", 0))
    except Exception:
        return _json_err("invalid json", 400)

    if new_lev < 1 or new_lev > 125:
        return _json_err("leverage debe estar entre 1 y 125", 400)

    LEVERAGE = new_lev
    log.warning(f"Leverage por defecto cambiado desde el dashboard a {LEVERAGE}x (aplica a próximas posiciones)")
    return web.json_response({"ok": True, "leverage": LEVERAGE})


async def manual_clear_history_handler(request: web.Request) -> web.Response:
    if not _check_dashboard_token(request):
        return _json_err("unauthorized", 401)

    count = len(execution_manager._closed)
    execution_manager._closed.clear()

    for k in _STATUS_KEYS:
        executor_status[k] = 0
    executor_status["last_signal_time"] = "Historial borrado"
    executor_status["last_signal_detail"] = ""

    log.warning(f"Historial de operaciones cerradas borrado desde el dashboard ({count} operaciones eliminadas)")
    return web.json_response({"ok": True, "cleared": count})


def _ser_trade(t: Trade) -> dict:
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


async def api_state_handler(request: web.Request) -> web.Response:
    em = execution_manager
    closed = em.closed_trades
    open_trades = em.open_trades
    wins = sum(1 for t in closed if t.status == "TP")
    total = len(closed)
    unrealized = sum(t.pnl_usdt for t in open_trades)

    return web.json_response({
        "balance": em.balance,
        "equity": em.balance + unrealized,
        "realized_pnl": sum(t.pnl_usdt for t in closed),
        "unrealized_pnl": unrealized,
        "wins": wins,
        "losses": total - wins,
        "win_rate": (wins / total * 100) if total else None,
        "open_count": len(open_trades),
        "open_longs": sum(1 for t in open_trades if t.direction == "LONG"),
        "open_shorts": sum(1 for t in open_trades if t.direction == "SHORT"),
        "open_trades": [_ser_trade(t) for t in open_trades],
        "closed_trades": [_ser_trade(t) for t in closed],
        "executor_status": executor_status,
        "ws_symbols": ", ".join(sorted(em.active_symbols)) or "ninguno",
        "leverage": LEVERAGE,
        "trading_enabled": em.trading_enabled,
        "testnet": USE_TESTNET,
        "hedge_mode": HEDGE_MODE,
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
          <td><button class="btn-close" onclick="closeTrade('${t.symbol}','${t.direction}')">Cerrar</button>
              <button class="btn-manage" onclick="openManageModal('${t.symbol}','${t.direction}',${t.entry_price},${t.quantity},${t.leverage})">⚙</button></td>
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

async function closeTrade(symbol, direction) {
  if (!confirm('¿Cerrar manualmente la posición ' + symbol + (direction ? ' (' + direction + ')' : '') + '?')) return;
  try {
    const body = {symbol: symbol};
    if (direction) body.direction = direction;
    const r = await fetch('/manual/close', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify(body)
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

async function setPositionMode(hedge) {
  const label = hedge ? 'Hedge Mode (LONG y SHORT independientes)' : 'One-way Mode (posición neta única)';
  if (!confirm('¿Cambiar el modo de posición de la cuenta a ' + label + '? Binance exige que NO haya posiciones ni órdenes abiertas para permitir el cambio.')) return;
  try {
    const r = await fetch('/manual/set_position_mode', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify({hedge_mode: hedge})
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return; }
    alert('Modo de posición actualizado a ' + (d.hedge_mode ? 'Hedge Mode' : 'One-way Mode') + '.');
    refresh();
  } catch(e) { alert('Error de red al cambiar el modo de posición'); }
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

let manageSymbol = null, manageDirection = null, manageEntry = 0, manageQty = 0, manageLev = 1;

function openManageModal(symbol, direction, entry, qty, lev) {
  manageSymbol = symbol; manageDirection = direction; manageEntry = entry; manageQty = qty; manageLev = lev || 1;
  document.getElementById('mm_title').textContent = '⚙ Gestionar ' + symbol + ' (' + direction + ')';
  document.getElementById('mm_lev_input').value = lev;
  document.getElementById('mm_entry_info').textContent =
    'Entrada: $' + entry + ' | Cantidad: ' + qty + ' | Margen ≈ ' + (entry * qty / (lev || 1)).toFixed(2) + ' USDT (' + lev + 'x)';
  showManageTab('tp');
  document.getElementById('manage_modal').style.display = 'flex';
  loadManageOrders();
}

function closeManageModal() { document.getElementById('manage_modal').style.display = 'none'; }

function showManageTab(tab) {
  ['tp','sl','limit','margin','lev'].forEach(t => {
    document.getElementById('mm_tab_' + t).style.display = (t === tab ? 'block' : 'none');
    document.getElementById('mm_btn_' + t).classList.toggle('active', t === tab);
  });
}

async function mmFetch(path, body) {
  try {
    const r = await fetch(path, {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (!d.ok) { alert('Error: ' + (d.error || 'desconocido')); return null; }
    return d;
  } catch(e) { alert('Error de red'); return null; }
}

async function loadManageOrders() {
  try {
    const r = await fetch('/manual/orders?symbol=' + manageSymbol, {headers: {'X-Dashboard-Token': DASH_TOKEN}});
    const d = await r.json();
    const box = document.getElementById('mm_orders_box');
    if (!d.ok || !d.orders.length) { box.textContent = 'Sin órdenes TP/SL/LIMIT activas.'; return; }
    box.innerHTML = d.orders.map(o => {
      const priceShown = o.triggerPrice || o.stopPrice || o.price;
      const id = o._algo ? o.algoId : o.orderId;
      const cancelFn = o._algo ? 'mmCancelAlgoOrder' : 'mmCancelOrder';
      return `<div>${o.type} ${o.side} @ ${priceShown} ` +
        `<button class="btn-mini-close" onclick="${cancelFn}('${id}')">✕</button></div>`;
    }).join('');
  } catch(e) {}
}

async function mmCancelOrder(orderId) {
  const r = await mmFetch('/manual/cancel_tp_sl', {symbol: manageSymbol, direction: manageDirection});
  if (r) { loadManageOrders(); }
}

async function mmCancelAlgoOrder(algoId) {
  const r = await mmFetch('/manual/cancel_tp_sl', {symbol: manageSymbol, direction: manageDirection});
  if (r) { loadManageOrders(); }
}

function mmClampPrice(price) {
  const n = Number(price);
  if (!Number.isFinite(n)) return 0;
  return Math.max(0.00001, n);
}

function mmCalcPriceFromUsdt(profitUsdt) {
  if (!manageQty || manageQty <= 0) return 0;
  const signedProfit = manageDirection === 'LONG' ? profitUsdt : -profitUsdt;
  return mmClampPrice(manageEntry + (signedProfit / manageQty));
}
function mmCalcPriceFromRoi(roiPct) {
  if (!manageQty || manageQty <= 0) return 0;
  const margin = (manageEntry * manageQty) / (manageLev || 1);
  const profitUsdt = (roiPct / 100) * margin;
  return mmCalcPriceFromUsdt(profitUsdt);
}

function mmCalcTPFromUsdt() {
  const v = parseFloat(document.getElementById('mm_tp_usdt').value);
  if (!Number.isFinite(v) || v <= 0) { alert('Ingresa una ganancia en USDT'); return; }
  const price = mmCalcPriceFromUsdt(Math.abs(v));
  if (!price) { alert('No se pudo calcular el TP'); return; }
  document.getElementById('mm_tp_price').value = price.toFixed(8);
}
function mmCalcTPFromRoi() {
  const v = parseFloat(document.getElementById('mm_tp_roi').value);
  if (!Number.isFinite(v) || v <= 0) { alert('Ingresa un ROI % objetivo'); return; }
  const price = mmCalcPriceFromRoi(Math.abs(v));
  if (!price) { alert('No se pudo calcular el TP'); return; }
  document.getElementById('mm_tp_price').value = price.toFixed(8);
}
function mmCalcSLFromUsdt() {
  const v = parseFloat(document.getElementById('mm_sl_usdt').value);
  if (!Number.isFinite(v) || v <= 0) { alert('Ingresa la pérdida máxima en USDT'); return; }
  const price = mmCalcPriceFromUsdt(-Math.abs(v));
  if (!price) { alert('No se pudo calcular el SL'); return; }
  document.getElementById('mm_sl_price').value = price.toFixed(8);
}
function mmCalcSLFromRoi() {
  const v = parseFloat(document.getElementById('mm_sl_roi').value);
  if (!Number.isFinite(v) || v <= 0) { alert('Ingresa la pérdida máxima en ROI %'); return; }
  const price = mmCalcPriceFromRoi(-Math.abs(v));
  if (!price) { alert('No se pudo calcular el SL'); return; }
  document.getElementById('mm_sl_price').value = price.toFixed(8);
}

async function mmSetTP() {
  const p = parseFloat(document.getElementById('mm_tp_price').value);
  if (!p || p <= 0) { alert('Precio de TP inválido'); return; }
  const d = await mmFetch('/manual/set_tp', {symbol: manageSymbol, trigger_price: p, direction: manageDirection});
  if (d) { alert('TP configurado en $' + p); loadManageOrders(); }
}

async function mmSetSL() {
  const p = parseFloat(document.getElementById('mm_sl_price').value);
  if (!p || p <= 0) { alert('Precio de SL inválido'); return; }
  const d = await mmFetch('/manual/set_sl', {symbol: manageSymbol, trigger_price: p, direction: manageDirection});
  if (d) { alert('SL configurado en $' + p); loadManageOrders(); }
}

async function mmCancelAllTpSl() {
  if (!confirm('¿Cancelar TODOS los TP/SL activos de ' + manageSymbol + ' (' + manageDirection + ')?')) return;
  const d = await mmFetch('/manual/cancel_tp_sl', {symbol: manageSymbol, direction: manageDirection});
  if (d) { alert('TP/SL cancelados (' + d.cancelled + ')'); loadManageOrders(); }
}

async function mmLimitOrder() {
  const side = document.getElementById('mm_limit_side').value;
  const price = parseFloat(document.getElementById('mm_limit_price').value);
  const qty = parseFloat(document.getElementById('mm_limit_qty').value);
  const reduceOnly = document.getElementById('mm_limit_reduce').checked;
  if (!price || !qty) { alert('Precio/cantidad inválidos'); return; }
  const d = await mmFetch('/manual/limit_order', {symbol: manageSymbol, side, price, quantity: qty, reduce_only: reduceOnly, direction: manageDirection});
  if (d) alert('Orden LIMIT enviada');
}

async function mmModifyMargin(add) {
  const amount = parseFloat(document.getElementById('mm_margin_amount').value);
  if (!amount || amount <= 0) { alert('Monto inválido'); return; }
  const d = await mmFetch('/manual/modify_margin', {symbol: manageSymbol, amount, add, direction: manageDirection});
  if (d) alert((add ? 'Margen añadido' : 'Margen retirado') + ': ' + amount + ' USDT');
}

async function mmSetMarginType(type) {
  if (!confirm('¿Cambiar tipo de margen de ' + manageSymbol + ' a ' + type + '?')) return;
  const d = await mmFetch('/manual/set_margin_type', {symbol: manageSymbol, margin_type: type});
  if (d) alert('Tipo de margen: ' + type);
}

async function mmSetSymbolLeverage() {
  const val = parseInt(document.getElementById('mm_lev_input').value, 10);
  if (!val || val < 1 || val > 125) { alert('Leverage inválido'); return; }
  const d = await mmFetch('/manual/set_symbol_leverage', {symbol: manageSymbol, leverage: val});
  if (d) { alert('Leverage aplicado: ' + d.applied + 'x' + (d.applied !== val ? ' (rechazado ' + val + 'x, se usó la escalera de respaldo)' : '')); refresh(); }
}

refresh();
setInterval(refresh, 5000);
</script>
"""

_DASHBOARD_JS = DASHBOARD_JS_TEMPLATE.replace("__DASH_TOKEN__", json.dumps(SIGNAL_SECRET))


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
    .btn-manage{{background:#30363d;color:#f0f6fc;border:none;border-radius:4px;padding:.25rem .5rem;font-size:.7rem;cursor:pointer;margin-left:.3rem}}
    .modal-overlay{{display:none;position:fixed;inset:0;background:rgba(0,0,0,.6);z-index:50;align-items:center;justify-content:center}}
    .modal-box{{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:1rem 1.2rem;width:min(480px,92vw);max-height:85vh;overflow-y:auto}}
    .modal-box h3{{color:#f0883e;margin:0 0 .7rem;font-size:1.05rem;display:flex;justify-content:space-between}}
    .mm-tabs{{display:flex;gap:.3rem;margin-bottom:.8rem;flex-wrap:wrap}}
    .mm-tab-btn{{background:#0d1117;color:#8b949e;border:1px solid #30363d;border-radius:6px;padding:.3rem .6rem;font-size:.72rem;cursor:pointer}}
    .mm-tab-btn.active{{background:#58a6ff;color:#0d1117;border-color:#58a6ff}}
    .mm-field{{margin-bottom:.6rem}}
    .mm-field label{{display:block;color:#8b949e;font-size:.72rem;margin-bottom:.2rem}}
    .mm-field input,.mm-field select{{width:100%;background:#0d1117;border:1px solid #30363d;border-radius:4px;color:#c9d1d9;padding:.35rem .5rem;font-size:.85rem;box-sizing:border-box}}
    .mm-field input[type=checkbox]{{width:auto}}
    .mm-action{{background:#3fb950;color:#0d1117;border:none;border-radius:5px;padding:.4rem .8rem;font-size:.8rem;cursor:pointer;font-weight:bold;margin-right:.4rem;margin-top:.2rem}}
    .mm-action.danger{{background:#f85149}}
    .btn-mini-close{{background:#f85149;color:#fff;border:none;border-radius:3px;padding:0 .35rem;font-size:.68rem;cursor:pointer;margin-left:.4rem}}
    #mm_orders_box{{background:#0d1117;border:1px solid #30363d;border-radius:6px;padding:.5rem;font-size:.74rem;margin-bottom:.8rem;color:#c9d1d9}}
  </style>
</head>
<body>
  <h1>⚡ Futures Executor WS — Binance USDT Perpetuos [{env}]</h1>
  <div class="info-banner">
    📡 Trading por <b>WebSocket API</b>. Precios en tiempo real vía <b>ws.py</b>. 
    Cambios de leverage por <b>REST{f' vía proxy ({len(PROXY_URLS)} IP(s))' if PROXY_URLS else ''}</b> (la WS API no lo soporta). Leverage configurado: <b>{LEVERAGE}x</b>{' | Modo Hedge' if HEDGE_MODE else ' | Modo One-way'}.
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
    <div class="card">
      <div class="label">Modo de posición</div>
      <div class="value" id="pos_mode_value">{'🔀 Hedge Mode' if HEDGE_MODE else '➡️ One-way Mode'}</div>
      <div class="lev-row">
        <button onclick="setPositionMode(false)" {"disabled" if not HEDGE_MODE else ""}>One-way</button>
        <button onclick="setPositionMode(true)" {"disabled" if HEDGE_MODE else ""}>Hedge</button>
      </div>
      <div style="font-size:.68rem;color:#8b949e;margin-top:.3rem">Requiere 0 posiciones/órdenes abiertas en Binance para poder cambiarlo.</div>
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
    Executor WS | Iniciado: {es['started_at']} | Actualizado: {_utc(_FMT_SEC)}
  </p>

  <div class="modal-overlay" id="manage_modal">
    <div class="modal-box">
      <h3><span id="mm_title">⚙ Gestionar</span><span style="cursor:pointer" onclick="closeManageModal()">✕</span></h3>
      <p id="mm_entry_info" style="color:#8b949e;font-size:.74rem;margin:-.3rem 0 .6rem"></p>
      <div id="mm_orders_box">Cargando órdenes...</div>
      <div class="mm-tabs">
        <button class="mm-tab-btn" id="mm_btn_tp" onclick="showManageTab('tp')">🎯 TP</button>
        <button class="mm-tab-btn" id="mm_btn_sl" onclick="showManageTab('sl')">🛡 SL</button>
        <button class="mm-tab-btn" id="mm_btn_limit" onclick="showManageTab('limit')">📋 Limit</button>
        <button class="mm-tab-btn" id="mm_btn_margin" onclick="showManageTab('margin')">💰 Margen</button>
        <button class="mm-tab-btn" id="mm_btn_lev" onclick="showManageTab('lev')">⚡ Leverage</button>
      </div>

      <div id="mm_tab_tp">
        <div class="mm-field"><label>Calcular por ganancia deseada (USDT)</label>
          <div style="display:flex;gap:.4rem">
            <input type="number" id="mm_tp_usdt" step="any" placeholder="ej. 7">
            <button class="mm-action" style="margin:0" onclick="mmCalcTPFromUsdt()">Calcular</button>
          </div>
        </div>
        <div class="mm-field"><label>Calcular por ROI % deseado (sobre el margen)</label>
          <div style="display:flex;gap:.4rem">
            <input type="number" id="mm_tp_roi" step="any" placeholder="ej. 50">
            <button class="mm-action" style="margin:0" onclick="mmCalcTPFromRoi()">Calcular</button>
          </div>
        </div>
        <div class="mm-field"><label>Precio de disparo (Take Profit)</label><input type="number" id="mm_tp_price" step="any"></div>
        <button class="mm-action" onclick="mmSetTP()">Establecer TP</button>
      </div>
      <div id="mm_tab_sl" style="display:none">
        <div class="mm-field"><label>Calcular por pérdida máxima (USDT)</label>
          <div style="display:flex;gap:.4rem">
            <input type="number" id="mm_sl_usdt" step="any" placeholder="ej. 5">
            <button class="mm-action" style="margin:0" onclick="mmCalcSLFromUsdt()">Calcular</button>
          </div>
        </div>
        <div class="mm-field"><label>Calcular por ROI % de pérdida máxima (sobre el margen)</label>
          <div style="display:flex;gap:.4rem">
            <input type="number" id="mm_sl_roi" step="any" placeholder="ej. 20">
            <button class="mm-action" style="margin:0" onclick="mmCalcSLFromRoi()">Calcular</button>
          </div>
        </div>
        <div class="mm-field"><label>Precio de disparo (Stop Loss)</label><input type="number" id="mm_sl_price" step="any"></div>
        <button class="mm-action danger" onclick="mmSetSL()">Establecer SL</button>
        <button class="mm-action" style="background:#8b949e" onclick="mmCancelAllTpSl()">Cancelar TP/SL</button>
      </div>
      <div id="mm_tab_limit" style="display:none">
        <div class="mm-field"><label>Lado</label>
          <select id="mm_limit_side"><option value="BUY">BUY</option><option value="SELL">SELL</option></select>
        </div>
        <div class="mm-field"><label>Precio</label><input type="number" id="mm_limit_price" step="any"></div>
        <div class="mm-field"><label>Cantidad</label><input type="number" id="mm_limit_qty" step="any"></div>
        <div class="mm-field"><label><input type="checkbox" id="mm_limit_reduce"> Reduce Only (cerrar parcial)</label></div>
        <button class="mm-action" onclick="mmLimitOrder()">Enviar LIMIT</button>
      </div>
      <div id="mm_tab_margin" style="display:none">
        <div class="mm-field"><label>Monto (USDT)</label><input type="number" id="mm_margin_amount" step="any"></div>
        <button class="mm-action" onclick="mmModifyMargin(true)">➕ Añadir margen</button>
        <button class="mm-action danger" onclick="mmModifyMargin(false)">➖ Retirar margen</button>
        <div class="mm-field" style="margin-top:.8rem"><label>Tipo de margen (símbolo sin posición abierta)</label></div>
        <button class="mm-action" onclick="mmSetMarginType('ISOLATED')">ISOLATED</button>
        <button class="mm-action" onclick="mmSetMarginType('CROSSED')">CROSSED</button>
      </div>
      <div id="mm_tab_lev" style="display:none">
        <div class="mm-field"><label>Leverage para este símbolo (con escalera de respaldo 10x→4x)</label>
          <input type="number" id="mm_lev_input" min="1" max="125">
        </div>
        <button class="mm-action" onclick="mmSetSymbolLeverage()">Aplicar Leverage</button>
      </div>
    </div>
  </div>

  {_DASHBOARD_JS}
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({"ok": True})


async def start_http_server():
    app = web.Application()
    app.router.add_post("/signal", signal_handler)
    app.router.add_post("/manual/close", manual_close_handler)
    app.router.add_post("/manual/close_all", manual_close_all_handler)
    app.router.add_post("/manual/toggle_trading", manual_toggle_trading_handler)
    app.router.add_post("/manual/set_leverage", manual_set_leverage_handler)
    app.router.add_post("/manual/clear_history", manual_clear_history_handler)
    app.router.add_post("/manual/set_tp", manual_set_tp_handler)
    app.router.add_post("/manual/set_sl", manual_set_sl_handler)
    app.router.add_post("/manual/cancel_tp_sl", manual_cancel_tp_sl_handler)
    app.router.add_post("/manual/limit_order", manual_limit_order_handler)
    app.router.add_post("/manual/set_symbol_leverage", manual_set_symbol_leverage_handler)
    app.router.add_post("/manual/set_margin_type", manual_set_margin_type_handler)
    app.router.add_get("/manual/position_mode", manual_get_position_mode_handler)
    app.router.add_post("/manual/set_position_mode", manual_set_position_mode_handler)
    app.router.add_post("/manual/modify_margin", manual_modify_margin_handler)
    app.router.add_get("/manual/orders", manual_get_orders_handler)
    app.router.add_get("/", dashboard_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/api/state", api_state_handler)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    log.info(f"Executor HTTP activo en http://0.0.0.0:{PORT}")


async def main():
    global execution_manager

    env_tag = "TESTNET 🧪" if USE_TESTNET else "REAL 🔴"
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║   Futures Executor WS — Binance USDT Perpetuos       ║")
    log.info(f"║   Entorno: {env_tag:<44}║")
    log.info(f"║   Leverage: {LEVERAGE}x | Poll cierre ext.: {POSITION_POLL_S}s              ║")
    log.info("╚══════════════════════════════════════════════════════╝")
    if PROXY_URLS:
        labels = ", ".join(BinanceAPI._proxy_label(p) for p in PROXY_URLS)
        log.info(
            f"Proxy(s) configurado(s) para leverage ({len(PROXY_URLS)}): {labels}. "
            f"Si Binance banea una IP (-1003), se salta a la siguiente automáticamente."
        )
    else:
        log.warning(
            "PROXY_URLS / FIXIE_URL no están configuradas — la llamada REST de leverage "
            "saldrá con la IP directa del proceso (sin whitelisting de IP fija)."
        )

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

    await execution_manager.refresh_balance(force=True)
    log.info(f"Balance USDT Futures: ${execution_manager.balance:.2f}")

    proxy_note = f"vía proxy ({len(PROXY_URLS)} IP(s), failover automático)" if PROXY_URLS else "⚠️ sin proxy configurado"
    await send_telegram(
        f"⚡ <b>Futures Executor WS iniciado</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Balance USDT:</b> <code>{execution_manager.balance:.2f} USDT</code>\n"
        f"⚡ <b>Leverage:</b> <code>{LEVERAGE}x</code>\n"
        f"📡 <b>Órdenes:</b> WebSocket API\n"
        f"📡 <b>Precios:</b> WebSocket (ws.py)\n"
        f"🌐 <b>Leverage (REST):</b> {proxy_note}\n"
        f"🔒 <b>Cierre:</b> señal explícita o botón manual\n"
        f"⚠️ <b>Error -2019:</b> posición puede registrarse como asumida"
    )

    try:
        await asyncio.gather(
            start_http_server(),
            price_sync_loop(),
            position_monitor_loop(),
            balance_sync_loop(),
        )
    finally:
        await api.close()
        if _tg_session and not _tg_session.closed:
            await _tg_session.close()


if __name__ == "__main__":
    asyncio.run(main())
