#!/usr/bin/env python3
"""
executor_client.py — uso completo del Futures Executor, por código y por endpoints.
=================================================================================

Un solo archivo, solo biblioteca estándar (los WebSocket opcionales usan
``aiohttp`` o ``websockets`` si están instalados). Cópialo junto a tu bot.

Contenido
---------
1. ``ExecutorBridge``  — reemplazo DIRECTO del ExecutorBridge que ya usan
   app.py / app_25.py / executor_bridge_con_ejemplo_stewar.py: misma firma,
   mismos payloads, mismas rutas (``POST /signal`` y ``GET /api/state``).
   Mejoras: no bloquea, mantiene el orden de las señales, reintenta si el
   executor está despertando (Render) o caído, nunca envía dos veces la misma
   señal (``signal_id``) y espera a enviar lo pendiente antes de salir.
2. ``ExecutorClient``  — todos los endpoints HTTP: estado, posiciones, órdenes,
   señales, apertura/cierre manual, TP/SL, leverage, margen, grids, errores…
3. ``SignalSocket`` / ``DashboardSocket`` — WebSockets ``/ws/signal`` y ``/ws``.
4. CLI — ``python executor_client.py --help``.

Variables de entorno (las mismas de app_25.py)::

    EXECUTOR_URL     https://tu-executor.onrender.com
    EXECUTOR_SECRET  igual a SIGNAL_SECRET del executor (por defecto "clave-secreta-aleatoria")
    EXECUTOR_TOKEN   solo si el executor define DASHBOARD_TOKEN

Migrar un bot desplegado
------------------------
Sustituye la clase ExecutorBridge del bot por este import; el resto del código
no cambia::

    from executor_client import ExecutorBridge

    bridge = ExecutorBridge(executor_url=EXECUTOR_URL, signal_secret=EXECUTOR_SECRET)
    bridge.notify_open(trade_id=1, symbol="BTCUSDT", direction="SHORT", price=65000,
                       quantity=0.001, notional=65, level=50)
    bridge.notify_close(trade_id=1, symbol="BTCUSDT", direction="SHORT", reason="TP",
                        close_price=64000, pnl=1.0)
    asyncio.create_task(bridge.poll_state_loop(lambda: running, on_state))

Los bots que siguen usando su ExecutorBridge original también funcionan: el
executor mantiene ``POST /signal`` y ``GET /api/state`` con el mismo formato.

Autenticación
-------------
* ``POST /signal`` y ``/ws/signal``: siempre el secreto (``X-Signal-Secret``).
  Si el executor no define SIGNAL_SECRET acepta "clave-secreta-aleatoria" y
  "cambiar-por-secreto-seguro" (los valores por defecto de los bots).
* Lectura (``GET``): abierta; si el executor define DASHBOARD_TOKEN pide
  ``X-Dashboard-Token`` (o el secreto). ``GET /api/state`` y ``/health`` siempre
  abiertos (salvo STATE_REQUIRES_AUTH=true).
* Escritura (``POST``/``DELETE`` de ``/api/*`` y ``/manual/*``): el secreto en
  ``X-Signal-Secret``, ``X-Dashboard-Token``, ``Authorization: Bearer <secreto>``
  o ``?secret=``. API_WRITE_OPEN=true en el executor lo desactiva.
* Con DASHBOARD_TOKEN definido y SIGNAL_SECRET sin definir, ``/api/*`` y ``/ws``
  solo aceptan el token (usa EXECUTOR_TOKEN); ``/signal`` sigue igual.

Referencia de endpoints
-----------------------
Respuesta REST: ``{"ok": true, "action": "<cmd>", "data": <resultado>, ...campos}``;
error: ``{"ok": false, "error": "...", "diagnosis"?: {...}}`` con código HTTP
400 (parámetros), 401 (secreto), 404 (no existe), 409 (conflicto), 502 (Binance
rechazó; ``diagnosis`` explica el código y la solución) o 503 (sin credenciales /
reiniciando). Los parámetros van en JSON, formulario o query string.

Compatibilidad (bots desplegados)::

    POST /signal                 señal (ver «Contrato de señales»)          → {"ok", "signal_id", ...}
         ?wait=1                 espera el resultado de la ejecución (≤ 7 s) → + "result"
         Idempotency-Key: <id>   reintento seguro (no se ejecuta dos veces)
    GET  /api/state[?limit=N]    estado para poll_state_loop: balance, equity, realized_pnl,
                                 unrealized_pnl, wins, losses, win_rate, open_trades,
                                 closed_trades, executor_status, leverage, trading_enabled…
    GET  /health                 salud de las conexiones (WS API, streams, user stream)
    GET  /ready                  200 cuando ya sincronizó la cuenta, 503 mientras arranca

Lectura::

    GET  /api/snapshot  (= /api/status)    todo el estado del dashboard
    GET  /api/account                      balance y resumen de cuenta
    GET  /api/positions[?symbol=]          posiciones reales + trade asociado
    GET  /api/orders[?symbol=]             órdenes abiertas y TP/SL (algo)
    GET  /api/trades[?status=all|open|closed&limit=&symbol=]
    GET  /api/trades.csv                   historial en CSV
    GET  /api/stats                        PnL, win rate, contadores de señales
    GET  /api/signals[?limit=&since_ts=&trade_id=&signal_id=]   qué pasó con cada señal
    GET  /api/settings                     configuración pública
    GET  /api/markets[?q=BTC]              mercado completo en vivo
    GET  /api/live[?symbols=A,B]           precios en vivo de los símbolos con posición
    GET  /api/symbol/{symbol}              reglas (step, tick, mínimo) y precio
    GET  /api/symbol/{symbol}/history      mark price 1/s
    GET  /api/exchange-info                estado del exchangeInfo local
    GET  /api/logs[?limit=]                log del executor
    GET  /api/commands                     catálogo de comandos
    GET  /api/position-mode                Hedge / One-way
    GET  /api/grids · /api/grids/{id}      bots Grid
    POST /api/grids/preview                vista previa de un grid
    GET  /api/errors[?limit=] · /api/errors/catalog · /api/errors/{code}

Operación (requiere el secreto)::

    POST /api/signal                 igual que /signal pero espera el resultado
    POST /api/open                   {symbol, direction, amount, size_mode?=notional|margin|qty,
                                      leverage?, order_type?=MARKET|LIMIT, price?, tp?, sl?}
    POST /api/close/{symbol}         {direction?, quantity?, reason?}  (también POST /api/close)
    POST /api/force-close/{symbol}   cierre forzado
    POST /api/close-all
    POST /api/set-tp/{symbol}        {trigger_price, direction?}
    POST /api/set-sl/{symbol}        {trigger_price, direction?}
    POST /api/cancel-tp-sl/{symbol}  {direction?, kind?=TP|SL}
    POST /api/limit-order            {symbol, side, price, quantity, reduce_only?, direction?}
    POST /api/cancel-order           {symbol, id, kind?=order|algo}
    POST /api/cancel-orders/{symbol}
    POST /api/leverage               {leverage}   leverage por defecto de las señales
    POST /api/leverage/{symbol}      {leverage}
    POST /api/margin-type/{symbol}   {margin_type: ISOLATED|CROSSED}
    POST /api/margin/{symbol}        {amount, add?=true, direction?}
    POST /api/position-mode          {hedge_mode: true|false}
    POST /api/trading                {enabled?}   sin enabled alterna pausa/activo
    POST /api/clear-history
    POST /api/grids                  {symbol, lower, upper, grids, investment, leverage?, mode?,
                                      spacing?, stop_loss?, take_profit?, trigger_price?}
    POST /api/grids/{id}/stop        {close_position?}
    DELETE /api/grids/{id}
    POST /api/errors/clear · /api/exchange-info/refresh · /api/sync · /api/sync-orders
    POST /api/command                {"cmd": "<comando>", "args": {...}}  cualquier comando

Rutas del executor anterior (mismos cuerpos y códigos)::

    POST /manual/close {symbol, direction?}   POST /manual/close_all
    POST /manual/toggle_trading               POST /manual/set_leverage {leverage}
    POST /manual/clear_history                POST /manual/set_tp|set_sl {symbol, trigger_price, direction?}
    POST /manual/cancel_tp_sl {symbol}        POST /manual/limit_order {symbol, side, price, quantity}
    POST /manual/set_symbol_leverage {symbol, leverage}
    POST /manual/set_margin_type {symbol, margin_type}
    GET  /manual/position_mode                POST /manual/set_position_mode {hedge_mode}
    POST /manual/modify_margin {symbol, amount, add?}
    GET  /manual/orders?symbol=

WebSockets::

    /ws/signal   envía señales sin REST. Autenticación: header X-Signal-Secret, ?secret= o
                 un primer mensaje {"secret": "..."}. Cada mensaje es una señal; la respuesta
                 lleva el mismo "id" y "status". {"action": "ping"} → pong.
    /ws          dashboard: {"op":"auth","token":""} → estado cada 1 s, eventos, errores, logs;
                 {"op":"cmd","id":1,"cmd":"positions","args":{}} → {"type":"reply","id":1,...}

Contrato de señales (``POST /signal``)
--------------------------------------
* ``open``: ``trade_id``, ``symbol``, ``direction`` (LONG/SHORT) y el tamaño:
  ``quantity`` (o ``qty``) con ``price`` (manda ``quantity × price``, como antes),
  o ``notional`` (o ``usdt``) o ``margin``. Opcionales: ``level``, ``leverage``,
  ``tp``/``sl`` (precios), ``signal_id``. Varios ``open`` con el mismo
  ``trade_id`` y símbolo son TRAMOS: se suman a la misma posición (lo que hace
  app_25 al promediar); un tramo que llega ≤ 30 s después del ``close`` se ignora.
* ``close``: ``trade_id`` y/o ``symbol`` (+ ``direction``), ``reason``,
  ``close_price``, ``pnl`` (PnL del bot, se compara con el real), ``quantity``
  (cierre parcial). Si llega antes que su ``open`` se aplica al abrirse.
* ``close_all``; ``open_tp``/``open_sl`` {symbol, trigger_price}; ``close_tp``/``close_sl`` {symbol}.

curl
----
::

    curl -X POST "$EXECUTOR_URL/signal" -H "Content-Type: application/json" \\
         -H "X-Signal-Secret: $EXECUTOR_SECRET" \\
         -d '{"action":"open","trade_id":1,"symbol":"BTCUSDT","direction":"SHORT","quantity":0.001,"price":65000}'
    curl "$EXECUTOR_URL/api/state"
    curl "$EXECUTOR_URL/api/positions?symbol=BTCUSDT"
    curl -X POST "$EXECUTOR_URL/api/close/BTCUSDT" -H "X-Signal-Secret: $EXECUTOR_SECRET"
    curl -X POST "$EXECUTOR_URL/api/set-sl/BTCUSDT" -H "X-Signal-Secret: $EXECUTOR_SECRET" \\
         -H "Content-Type: application/json" -d '{"trigger_price": 67000}'
    curl "$EXECUTOR_URL/api/errors/-2019"

CLI
---
::

    python executor_client.py state
    python executor_client.py positions
    python executor_client.py open BTCUSDT SHORT --quantity 0.001 --price 65000 --trade-id 7
    python executor_client.py close BTCUSDT --direction SHORT --trade-id 7 --reason TP
    python executor_client.py signals --limit 20
    python executor_client.py cmd set_sl symbol=BTCUSDT trigger_price=67000
    python executor_client.py call GET /api/live symbols=BTCUSDT,ETHUSDT
    python executor_client.py watch
    python executor_client.py demo          # recorrido de solo lectura
"""

from __future__ import annotations

import argparse
import asyncio
import atexit
import http.client
import itertools
import json
import os
import re
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
import weakref
from dataclasses import dataclass
from typing import Any, AsyncIterator, Callable, Iterable, Optional, Union

__all__ = [
    "DEFAULT_SECRET", "ExecutorSignalConfig", "_ExecutorSignalConfig", "ExecutorBridge", "ExecutorClient",
    "ExecutorError", "SignalSocket", "DashboardSocket", "new_signal_id", "main",
]

__version__ = "1.0.0"
DEFAULT_SECRET = "clave-secreta-aleatoria"
USER_AGENT = f"executor-client/{__version__}"
# Códigos que indican «inténtalo otra vez» (executor despertando, reiniciando o saturado).
RETRY_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})
_TRANSIENT = (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError, http.client.HTTPException)


def new_signal_id() -> str:
    """Identificador único de señal: un reintento con el mismo id no se ejecuta dos veces."""
    return uuid.uuid4().hex[:20]


def _clean_url(url: str) -> str:
    url = (url or "").strip().rstrip("/")
    if url and "://" not in url:
        url = "https://" + url
    return url


def _decode(raw: bytes) -> Any:
    text = raw.decode("utf-8", "replace")
    try:
        return json.loads(text)
    except ValueError:
        return text


def _http(req: urllib.request.Request, timeout: float) -> tuple[int, Any]:
    """Ejecuta la petición y devuelve (código, cuerpo). Los 4xx/5xx no lanzan excepción."""
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, _decode(resp.read())
    except urllib.error.HTTPError as err:
        try:
            body = _decode(err.read())
        except Exception:  # pragma: no cover
            body = ""
        return err.code, body


def _error_text(status: int, body: Any) -> str:
    if isinstance(body, dict):
        msg = str(body.get("error") or body.get("detail") or "")
    else:
        msg = str(body)[:200]
    return f"HTTP {status}" + (f": {msg}" if msg else "")


# ═════════════════════════════════════════════════════════════════════════════
# 1. ExecutorBridge — reemplazo directo del de app.py / app_25.py
# ═════════════════════════════════════════════════════════════════════════════
@dataclass
class ExecutorSignalConfig:
    executor_url: str = ""
    signal_secret: str = DEFAULT_SECRET
    poll_secs: int = 5
    timeout_signal: int = 8
    timeout_state: int = 8
    retries: int = 4              # reintentos de una señal (executor despertando / caído)
    retry_backoff: float = 2.0    # espera 2 s, 4 s, 8 s, 16 s… (máx. 20 s) entre reintentos
    state_limit: int = 0          # cerrados en /api/state (0 = lo que decida el executor)
    flush_on_exit: float = 75.0   # al salir, espera hasta N s a que se envíe lo pendiente (cubre los reintentos)


_ExecutorSignalConfig = ExecutorSignalConfig  # nombre usado dentro de app_25.py

_BRIDGES: "weakref.WeakSet[ExecutorBridge]" = weakref.WeakSet()


@atexit.register
def _flush_bridges_at_exit() -> None:
    for bridge in list(_BRIDGES):
        try:
            if bridge.pending and not bridge.flush(bridge.config.flush_on_exit):
                bridge._log(f"[executor] {bridge.pending} señal(es) sin enviar al salir (executor sin respuesta)")
        except Exception:  # pragma: no cover
            pass


class ExecutorBridge:
    """Envía señales de apertura/cierre al Executor y consulta su estado.

    Misma interfaz que el ExecutorBridge de app.py / app_25.py:
    ``send_signal_sync``, ``send_signal_async``, ``fetch_state_sync``,
    ``notify_open``, ``notify_close``, ``notify_async``, ``poll_state_loop``.

    * ``notify_*`` / ``notify_async`` nunca bloquean: encolan la señal y un hilo
      la envía en orden (un ``close`` no adelanta a su ``open``). Funcionan con o
      sin event loop y devuelven el ``signal_id``.
    * Reintenta ante timeouts, 5xx y arranque en frío; el ``signal_id`` evita que
      un reintento abra dos veces.
    * Al terminar el proceso espera (``flush_on_exit``) a enviar lo pendiente.
    """

    def __init__(
        self,
        executor_url: str = "",
        signal_secret: str = DEFAULT_SECRET,
        poll_secs: int = 5,
        logger: Optional[Callable[[str], None]] = None,
        **options: Any,
    ) -> None:
        self.config = ExecutorSignalConfig(
            executor_url=_clean_url(executor_url),
            signal_secret=signal_secret,
            poll_secs=int(poll_secs),
            **options,
        )
        self.logger = logger or print
        self.sent = 0
        self.failed = 0
        self.last_error = ""
        self.last_response: Optional[dict] = None
        self.last_state: Optional[dict] = None
        self._queue: list[dict] = []
        self._cond = threading.Condition()
        self._busy = 0
        self._worker: Optional[threading.Thread] = None
        _BRIDGES.add(self)

    # ── utilidades ──────────────────────────────────────────────────────────
    def _log(self, message: str) -> None:
        try:
            self.logger(message)
        except Exception:
            pass

    def _build_signal_request(self, payload: dict[str, Any]) -> urllib.request.Request:
        body = json.dumps(payload, default=str).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "X-Signal-Secret": self.config.signal_secret,
            "User-Agent": USER_AGENT,
        }
        if payload.get("signal_id"):
            headers["Idempotency-Key"] = str(payload["signal_id"])
        return urllib.request.Request(
            f"{self.config.executor_url}/signal", data=body, headers=headers, method="POST"
        )

    # ── envío ─────────────────────────────────────────────────────────────
    def send_signal_sync(self, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Envía una señal y devuelve la respuesta del executor (None si no llegó).

        No lanza excepciones: registra el error, como el bridge original.
        """
        if not self.config.executor_url:
            return None
        payload = dict(payload)
        payload.setdefault("signal_id", new_signal_id())
        what = f"{payload.get('action')} {payload.get('symbol') or ''}".strip()
        attempts = 1 + max(0, int(self.config.retries))
        problem = ""
        for attempt in range(attempts):
            if attempt:
                delay = min(20.0, self.config.retry_backoff * (2 ** (attempt - 1)))
                self._log(f"[executor] reintento {attempt}/{attempts - 1} de {what} en {delay:.0f}s ({problem})")
                time.sleep(delay)
            try:
                status, body = _http(self._build_signal_request(payload), self.config.timeout_signal)
            except _TRANSIENT + (OSError,) as exc:
                problem = f"sin respuesta: {exc}"
                continue
            except Exception as exc:  # URL inválida, payload no serializable…: nunca se propaga
                self.failed += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                self._log(f"[executor] error enviando {what}: {self.last_error}")
                return None
            if status in RETRY_STATUSES:
                problem = _error_text(status, body)
                continue
            if status >= 400 or not isinstance(body, dict):
                self.failed += 1
                self.last_error = _error_text(status, body)
                hint = (" — el secreto no coincide: EXECUTOR_SECRET / signal_secret debe ser igual a "
                        "SIGNAL_SECRET del executor") if status == 401 else ""
                self._log(f"[executor] error enviando {what}: {self.last_error}{hint}")
                return body if isinstance(body, dict) else None
            self.sent += 1
            self.last_response = body
            if body.get("ok", True):
                extra = " (ya recibida)" if body.get("duplicate") else ""
                self._log(f"[executor] ✓ señal enviada: {what}{extra}")
            else:
                self.last_error = str(body.get("error") or body.get("detail") or "rechazada")
                self._log(f"[executor] señal {what} rechazada por el executor: {self.last_error}")
            return body
        self.failed += 1
        self.last_error = problem
        self._log(f"[executor] error enviando {what}: {problem} (tras {attempts} intentos)")
        return None

    async def send_signal_async(self, payload: dict[str, Any]) -> Optional[dict[str, Any]]:
        """Versión no bloqueante para usar desde el event loop."""
        return await asyncio.to_thread(self.send_signal_sync, payload)

    def notify_async(self, payload: dict[str, Any]) -> str:
        """Encola la señal (sin bloquear, con o sin event loop) y devuelve su signal_id."""
        if not self.config.executor_url:
            return ""
        payload = dict(payload)
        payload.setdefault("signal_id", new_signal_id())
        with self._cond:
            self._queue.append(payload)
            if self._worker is None or not self._worker.is_alive():
                self._worker = threading.Thread(target=self._run_worker, name="executor-bridge", daemon=True)
                self._worker.start()
            self._cond.notify_all()
        return str(payload["signal_id"])

    def _run_worker(self) -> None:
        while True:
            with self._cond:
                while not self._queue:
                    if not self._cond.wait(timeout=60):
                        if not self._queue:
                            self._worker = None
                            return
                payload = self._queue.pop(0)
                self._busy += 1
            try:
                self.send_signal_sync(payload)
            except Exception as exc:  # pragma: no cover - send_signal_sync no lanza
                self._log(f"[executor] error inesperado enviando señal: {exc}")
            finally:
                with self._cond:
                    self._busy -= 1
                    self._cond.notify_all()

    @property
    def pending(self) -> int:
        """Señales encoladas o enviándose."""
        with self._cond:
            return len(self._queue) + self._busy

    def flush(self, timeout: float = 30.0) -> bool:
        """Espera a que se envíen las señales encoladas. True si no queda ninguna."""
        deadline = time.monotonic() + max(0.0, timeout)
        with self._cond:
            while self._queue or self._busy:
                left = deadline - time.monotonic()
                if left <= 0:
                    return False
                self._cond.wait(timeout=min(left, 0.5))
        return True

    def close(self, timeout: float = 30.0) -> bool:
        return self.flush(timeout)

    # ── atajos de señales (mismos payloads que app_25.py) ──────────────────
    def notify_open(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        price: float,
        quantity: float,
        notional: float = 0.0,
        level: float = 0.0,
        **extra: Any,
    ) -> str:
        """Notifica una apertura (o un tramo más del mismo trade_id) sin bloquear.

        ``extra`` admite ``leverage``, ``tp``, ``sl``, ``margin``, ``signal_id``…
        """
        payload = {
            "action": "open",
            "trade_id": trade_id,
            "symbol": symbol,
            "direction": direction,
            "price": price,
            "quantity": quantity,
            "notional": notional,
            "level": level,
        }
        payload.update(extra)
        return self.notify_async(payload)

    def notify_close(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        reason: str,
        close_price: float,
        pnl: Optional[float] = None,
        **extra: Any,
    ) -> str:
        """Notifica un cierre sin bloquear. ``pnl`` es el PnL calculado por el bot."""
        payload = {
            "action": "close",
            "trade_id": trade_id,
            "symbol": symbol,
            "direction": direction,
            "reason": reason,
            "close_price": close_price,
        }
        if pnl is not None:
            payload["pnl"] = pnl
        payload.update(extra)
        return self.notify_async(payload)

    def notify_close_all(self, **extra: Any) -> str:
        return self.notify_async({"action": "close_all", **extra})

    # ── estado ────────────────────────────────────────────────────────────
    def fetch_state_sync(self) -> Optional[dict[str, Any]]:
        """Lee /api/state del Executor (None si no responde)."""
        if not self.config.executor_url:
            return None
        url = f"{self.config.executor_url}/api/state"
        if self.config.state_limit:
            url += f"?limit={int(self.config.state_limit)}"
        headers = {"User-Agent": USER_AGENT, "X-Signal-Secret": self.config.signal_secret}
        try:
            status, body = _http(urllib.request.Request(url, headers=headers, method="GET"),
                                 self.config.timeout_state)
        except Exception:
            return None
        if status != 200 or not isinstance(body, dict):
            return None
        self.last_state = body
        return body

    async def poll_state_loop(
        self,
        running: Callable[[], bool],
        on_state: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Consulta /api/state cada ``poll_secs`` mientras ``running()`` sea True.

        ``on_state`` puede ser una función normal o una corrutina.
        """
        if not self.config.executor_url:
            self._log("[executor] EXECUTOR_URL no configurado — PnL real no disponible")
            return
        self._log(f"[executor] Polling de estado activo → {self.config.executor_url}")
        while running():
            try:
                data = await asyncio.to_thread(self.fetch_state_sync)
                if data is not None:
                    result = on_state(data)
                    if asyncio.iscoroutine(result):
                        await result
            except asyncio.CancelledError:
                if not running():
                    break
            except Exception as exc:
                self._log(f"[executor] Error consultando estado: {exc}")
            try:
                await asyncio.sleep(self.config.poll_secs)
            except asyncio.CancelledError:
                if not running():
                    break


# ═════════════════════════════════════════════════════════════════════════════
# 2. ExecutorClient — todos los endpoints HTTP
# ═════════════════════════════════════════════════════════════════════════════
class ExecutorError(Exception):
    """El executor respondió con error (``status`` HTTP, ``error`` y cuerpo completo)."""

    def __init__(self, status: int, error: str, body: Any = None):
        super().__init__(f"HTTP {status}: {error}")
        self.status = status
        self.error = error
        self.body = body

    @property
    def diagnosis(self) -> Optional[dict]:
        """Explicación del código de Binance (si la orden la rechazó Binance)."""
        return self.body.get("diagnosis") if isinstance(self.body, dict) else None


def _q(value: Any) -> str:
    return urllib.parse.quote(str(value), safe="")


def _clean(args: dict) -> dict:
    return {k: v for k, v in args.items() if v is not None}


class ExecutorClient:
    """Cliente HTTP de todo el executor (sincrónico; ``ExecutorClient.aio`` para asyncio)."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        secret: Optional[str] = None,
        token: Optional[str] = None,
        timeout: float = 15.0,
        retries: int = 2,
    ) -> None:
        self.base_url = _clean_url(base_url or os.getenv("EXECUTOR_URL", "") or "http://127.0.0.1:10000")
        self.token = token if token is not None else os.getenv("EXECUTOR_TOKEN", "")
        # Con solo EXECUTOR_TOKEN no se envía el secreto por defecto (el token basta).
        self.secret = secret if secret is not None else os.getenv("EXECUTOR_SECRET", "" if self.token else DEFAULT_SECRET)
        self.timeout = timeout
        self.retries = retries

    def __repr__(self) -> str:
        return f"ExecutorClient({self.base_url!r})"

    # ── núcleo ────────────────────────────────────────────────────────────
    def request(
        self,
        method: str,
        path: str,
        body: Optional[dict] = None,
        query: Optional[dict] = None,
        *,
        headers: Optional[dict] = None,
        timeout: Optional[float] = None,
        retry: Optional[bool] = None,
        raise_for_status: bool = True,
    ) -> Any:
        """Petición genérica. Devuelve el JSON (o texto). Lanza ``ExecutorError`` si HTTP ≥ 400.

        Solo se reintentan las lecturas y las peticiones con Idempotency-Key.
        """
        method = method.upper()
        url = self.base_url + (path if path.startswith("/") else "/" + path)
        if query:
            q = {k: (",".join(map(str, v)) if isinstance(v, (list, tuple, set)) else v)
                 for k, v in query.items() if v is not None}
            if q:
                url += ("&" if "?" in url else "?") + urllib.parse.urlencode(q)
        hdrs = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if self.secret:
            hdrs["X-Signal-Secret"] = self.secret
        if self.token:
            hdrs["X-Dashboard-Token"] = self.token
        data = None
        if body is not None or method in ("POST", "PUT", "PATCH"):
            data = json.dumps(body or {}).encode("utf-8")
            hdrs["Content-Type"] = "application/json"
        hdrs.update(headers or {})
        if retry is None:
            retry = method in ("GET", "HEAD") or "Idempotency-Key" in hdrs
        attempts = 1 + (max(0, self.retries) if retry else 0)
        last_exc: Optional[BaseException] = None
        status, payload = 0, None
        for attempt in range(attempts):
            if attempt:
                time.sleep(min(10.0, 1.5 * (2 ** (attempt - 1))))
            req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
            try:
                status, payload = _http(req, timeout or self.timeout)
            except _TRANSIENT as exc:
                last_exc = exc
                continue
            last_exc = None
            if status in RETRY_STATUSES and attempt + 1 < attempts:
                continue
            break
        if last_exc is not None:
            raise ExecutorError(0, f"sin conexión con {self.base_url}: {last_exc}") from last_exc
        if raise_for_status and status >= 400:
            err = payload.get("error", "") if isinstance(payload, dict) else str(payload)[:300]
            raise ExecutorError(status, str(err), payload)
        return payload

    def get(self, path: str, **query: Any) -> Any:
        return self.request("GET", path, query=_clean(query))

    def post(self, path: str, body: Optional[dict] = None, **query: Any) -> Any:
        return self.request("POST", path, _clean(body or {}), query=_clean(query))

    def delete(self, path: str, **query: Any) -> Any:
        return self.request("DELETE", path, query=_clean(query))

    @staticmethod
    def _data(reply: Any) -> Any:
        """Extrae ``data`` de la respuesta REST ``{"ok": true, "action", "data"}``."""
        if isinstance(reply, dict) and reply.get("ok") is True and "data" in reply:
            return reply["data"]
        return reply

    def _rget(self, path: str, **query: Any) -> Any:
        return self._data(self.get(path, **query))

    def _rpost(self, path: str, body: Optional[dict] = None, **query: Any) -> Any:
        return self._data(self.post(path, body, **query))

    def command(self, cmd: str, **args: Any) -> Any:
        """Cualquier comando de ``GET /api/commands`` vía ``POST /api/command``."""
        return self._data(self.request("POST", "/api/command", {"cmd": cmd, "args": _clean(args)}))

    # ── compatibilidad / salud ─────────────────────────────────────────────
    def state(self, limit: Optional[int] = None) -> dict:
        """``GET /api/state`` (mismo formato que consume poll_state_loop)."""
        return self.get("/api/state", limit=limit)

    def health(self) -> dict:
        return self.get("/health")

    def ready(self) -> dict:
        """``GET /ready``: ``{"ok": bool, "ready", "accepting_signals", ...}`` (no lanza con 503)."""
        return self.request("GET", "/ready", raise_for_status=False, retry=False)

    def is_ready(self) -> bool:
        try:
            reply = self.ready()
        except ExecutorError:
            return False
        return isinstance(reply, dict) and bool(reply.get("ok"))

    def wait_ready(self, timeout: float = 120.0, interval: float = 2.0) -> bool:
        """Espera a que el executor despierte (Render) y sincronice la cuenta."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.is_ready():
                return True
            time.sleep(interval)
        return False

    # ── lectura ───────────────────────────────────────────────────────────
    def snapshot(self) -> dict:
        return self._rget("/api/snapshot")

    def status(self) -> dict:
        return self._rget("/api/status")

    def account(self) -> dict:
        return self._rget("/api/account")

    def positions(self, symbol: Optional[str] = None) -> list:
        return self._rget("/api/positions", symbol=symbol)

    def orders(self, symbol: Optional[str] = None) -> list:
        return self._rget("/api/orders", symbol=symbol)

    def trades(self, status: str = "all", limit: Optional[int] = None, symbol: Optional[str] = None) -> dict:
        return self._rget("/api/trades", status=status, limit=limit, symbol=symbol)

    def trades_csv(self) -> str:
        return self.request("GET", "/api/trades.csv", headers={"Accept": "text/csv"})

    def stats(self) -> dict:
        return self._rget("/api/stats")

    def signals(self, limit: Optional[int] = None, since_ts: Optional[float] = None,
                trade_id: Optional[int] = None, signal_id: Optional[str] = None) -> list:
        return self._rget("/api/signals", limit=limit, since_ts=since_ts, trade_id=trade_id, signal_id=signal_id)

    def settings(self) -> dict:
        return self._rget("/api/settings")

    def markets(self, q: Optional[str] = None) -> list:
        return self._rget("/api/markets", q=q)

    def live(self, symbols: Union[None, str, Iterable[str]] = None) -> dict:
        if symbols is not None and not isinstance(symbols, str):
            symbols = ",".join(symbols)
        return self._rget("/api/live", symbols=symbols)

    def symbol(self, symbol: str) -> dict:
        return self._rget(f"/api/symbol/{_q(symbol.upper())}")

    def price_history(self, symbol: str) -> dict:
        return self._rget(f"/api/symbol/{_q(symbol.upper())}/history")

    def exchange_info(self) -> dict:
        return self._rget("/api/exchange-info")

    def logs(self, limit: Optional[int] = None) -> list:
        return self._rget("/api/logs", limit=limit)

    def commands(self) -> list:
        return self._rget("/api/commands")

    def position_mode(self) -> dict:
        return self._rget("/api/position-mode")

    def errors(self, limit: Optional[int] = None) -> dict:
        return self._rget("/api/errors", limit=limit)

    def error_catalog(self) -> Any:
        return self._rget("/api/errors/catalog")

    def explain_error(self, code: int) -> dict:
        return self._rget(f"/api/errors/{_q(code)}")

    # ── señales (mismo canal que los bots) ─────────────────────────────────
    def signal(self, payload: dict, wait: bool = True, signal_id: Optional[str] = None) -> dict:
        """``POST /signal``. Con ``wait`` incluye ``result`` (lo que pasó en Binance).

        Lleva Idempotency-Key, así que se reintenta sin riesgo de duplicar.
        """
        payload = _clean(dict(payload))
        sid = str(signal_id or payload.get("signal_id") or new_signal_id())
        payload["signal_id"] = sid
        return self.request("POST", "/signal", payload, query={"wait": 1 if wait else None},
                            headers={"Idempotency-Key": sid}, timeout=max(self.timeout, 20.0))

    def open_signal(self, symbol: str, direction: str, *, trade_id: int = 0, price: Optional[float] = None,
                    quantity: Optional[float] = None, notional: Optional[float] = None,
                    margin: Optional[float] = None, level: Optional[float] = None, leverage: Optional[int] = None,
                    tp: Optional[float] = None, sl: Optional[float] = None, wait: bool = True, **extra: Any) -> dict:
        return self.signal({"action": "open", "trade_id": trade_id, "symbol": symbol.upper(),
                            "direction": direction.upper(), "price": price, "quantity": quantity,
                            "notional": notional, "margin": margin, "level": level, "leverage": leverage,
                            "tp": tp, "sl": sl, **extra}, wait=wait)

    def close_signal(self, symbol: str = "", direction: Optional[str] = None, *, trade_id: int = 0,
                     reason: str = "MAIN_BOT", close_price: Optional[float] = None,
                     quantity: Optional[float] = None, pnl: Optional[float] = None, wait: bool = True,
                     **extra: Any) -> dict:
        return self.signal({"action": "close", "trade_id": trade_id, "symbol": symbol.upper(),
                            "direction": direction.upper() if direction else None, "reason": reason,
                            "close_price": close_price, "quantity": quantity, "pnl": pnl, **extra}, wait=wait)

    def close_all_signal(self, wait: bool = True) -> dict:
        return self.signal({"action": "close_all"}, wait=wait)

    def tp_signal(self, symbol: str, trigger_price: float, direction: Optional[str] = None, wait: bool = True) -> dict:
        return self.signal({"action": "open_tp", "symbol": symbol.upper(), "direction": direction,
                            "trigger_price": trigger_price}, wait=wait)

    def sl_signal(self, symbol: str, trigger_price: float, direction: Optional[str] = None, wait: bool = True) -> dict:
        return self.signal({"action": "open_sl", "symbol": symbol.upper(), "direction": direction,
                            "trigger_price": trigger_price}, wait=wait)

    # ── operación manual ──────────────────────────────────────────────────
    def open(self, symbol: str, direction: str, amount: float, size_mode: str = "notional",
             leverage: Optional[int] = None, order_type: str = "MARKET", price: Optional[float] = None,
             tp: Optional[float] = None, sl: Optional[float] = None) -> dict:
        """Abre una posición (``size_mode``: notional USDT, margin USDT o qty en monedas)."""
        return self._rpost("/api/open", {"symbol": symbol.upper(), "direction": direction.upper(), "amount": amount,
                                         "size_mode": size_mode, "leverage": leverage, "order_type": order_type,
                                         "price": price, "tp": tp, "sl": sl})

    def close(self, symbol: str, direction: Optional[str] = None, quantity: Optional[float] = None,
              reason: Optional[str] = None) -> dict:
        return self._rpost(f"/api/close/{_q(symbol.upper())}",
                           {"direction": direction, "quantity": quantity, "reason": reason})

    def force_close(self, symbol: str, direction: Optional[str] = None) -> dict:
        return self._rpost(f"/api/force-close/{_q(symbol.upper())}", {"direction": direction})

    def close_all(self) -> dict:
        return self._rpost("/api/close-all")

    def set_tp(self, symbol: str, trigger_price: float, direction: Optional[str] = None) -> dict:
        return self._rpost(f"/api/set-tp/{_q(symbol.upper())}", {"trigger_price": trigger_price, "direction": direction})

    def set_sl(self, symbol: str, trigger_price: float, direction: Optional[str] = None) -> dict:
        return self._rpost(f"/api/set-sl/{_q(symbol.upper())}", {"trigger_price": trigger_price, "direction": direction})

    def cancel_tp_sl(self, symbol: str, direction: Optional[str] = None, kind: Optional[str] = None) -> dict:
        return self._rpost(f"/api/cancel-tp-sl/{_q(symbol.upper())}", {"direction": direction, "kind": kind})

    def limit_order(self, symbol: str, side: str, price: float, quantity: float, reduce_only: bool = False,
                    direction: Optional[str] = None) -> dict:
        return self._rpost("/api/limit-order", {"symbol": symbol.upper(), "side": side.upper(), "price": price,
                                                "quantity": quantity, "reduce_only": reduce_only,
                                                "direction": direction})

    def cancel_order(self, symbol: str, order_id: int, kind: str = "order") -> dict:
        return self._rpost("/api/cancel-order", {"symbol": symbol.upper(), "id": order_id, "kind": kind})

    def cancel_orders(self, symbol: str) -> dict:
        return self._rpost(f"/api/cancel-orders/{_q(symbol.upper())}")

    def set_leverage(self, leverage: int) -> dict:
        """Leverage por defecto de las señales."""
        return self._rpost("/api/leverage", {"leverage": leverage})

    def set_symbol_leverage(self, symbol: str, leverage: int) -> dict:
        return self._rpost(f"/api/leverage/{_q(symbol.upper())}", {"leverage": leverage})

    def set_margin_type(self, symbol: str, margin_type: str) -> dict:
        return self._rpost(f"/api/margin-type/{_q(symbol.upper())}", {"margin_type": margin_type})

    def modify_margin(self, symbol: str, amount: float, add: bool = True, direction: Optional[str] = None) -> dict:
        return self._rpost(f"/api/margin/{_q(symbol.upper())}", {"amount": amount, "add": add,
                                                                 "direction": direction})

    def set_position_mode(self, hedge_mode: bool) -> dict:
        return self._rpost("/api/position-mode", {"hedge_mode": hedge_mode})

    def set_trading(self, enabled: Optional[bool] = None) -> dict:
        """Pausa (False) / reactiva (True) las aperturas por señal; None alterna."""
        return self._rpost("/api/trading", {"enabled": enabled})

    def clear_history(self) -> dict:
        return self._rpost("/api/clear-history")

    def sync(self) -> dict:
        return self._rpost("/api/sync")

    def sync_orders(self) -> dict:
        return self._rpost("/api/sync-orders")

    def refresh_exchange_info(self) -> dict:
        return self._rpost("/api/exchange-info/refresh")

    def clear_errors(self) -> dict:
        return self._rpost("/api/errors/clear")

    # ── grids ─────────────────────────────────────────────────────────────
    def grids(self) -> list:
        return self._rget("/api/grids")

    def grid(self, grid_id: str) -> dict:
        return self._rget(f"/api/grids/{_q(grid_id)}")

    def grid_preview(self, symbol: str, lower: float, upper: float, grids: int, investment: float,
                     **opts: Any) -> dict:
        return self._rpost("/api/grids/preview", {"symbol": symbol.upper(), "lower": lower, "upper": upper,
                                                  "grids": grids, "investment": investment, **opts})

    def grid_create(self, symbol: str, lower: float, upper: float, grids: int, investment: float,
                    **opts: Any) -> dict:
        """``opts``: leverage, mode (LONG/SHORT/NEUTRAL), spacing, stop_loss, take_profit, trigger_price…"""
        return self._rpost("/api/grids", {"symbol": symbol.upper(), "lower": lower, "upper": upper,
                                          "grids": grids, "investment": investment, **opts})

    def grid_stop(self, grid_id: str, close_position: Optional[bool] = None) -> dict:
        return self._rpost(f"/api/grids/{_q(grid_id)}/stop", {"close_position": close_position})

    def grid_delete(self, grid_id: str) -> dict:
        return self._data(self.delete(f"/api/grids/{_q(grid_id)}"))

    # ── rutas antiguas /manual/* ───────────────────────────────────────────
    def manual(self, name: str, method: str = "POST", **body: Any) -> dict:
        """Rutas del executor anterior, p. ej. ``manual("close", symbol="BTCUSDT")``."""
        path = f"/manual/{name.strip('/')}"
        if method.upper() == "GET":
            return self.get(path, **body)
        return self.post(path, body)

    # ── asyncio ───────────────────────────────────────────────────────────
    @property
    def aio(self) -> "_AsyncFacade":
        """Versión asyncio: ``await client.aio.positions()`` (cada llamada va a un hilo)."""
        return _AsyncFacade(self)


class _AsyncFacade:
    def __init__(self, client: ExecutorClient):
        self._client = client

    def __getattr__(self, name: str):
        fn = getattr(self._client, name)
        if not callable(fn):
            return fn

        async def call(*args: Any, **kwargs: Any) -> Any:
            return await asyncio.to_thread(fn, *args, **kwargs)
        return call


# ═════════════════════════════════════════════════════════════════════════════
# 3. WebSockets (opcional: requiere aiohttp o websockets)
# ═════════════════════════════════════════════════════════════════════════════
def _ws_url(base_url: str, path: str) -> str:
    base = _clean_url(base_url)
    if base.startswith("https://"):
        base = "wss://" + base[8:]
    elif base.startswith("http://"):
        base = "ws://" + base[7:]
    return base + path


class _WSConn:
    """Adaptador mínimo sobre aiohttp o websockets."""

    def __init__(self, ws: Any, session: Any = None):
        self.ws = ws
        self.session = session

    @classmethod
    async def connect(cls, url: str, headers: dict) -> "_WSConn":
        try:
            import aiohttp
        except ImportError:
            aiohttp = None
        if aiohttp is not None:
            session = aiohttp.ClientSession(trust_env=True)
            try:
                ws = await session.ws_connect(url, headers=headers, heartbeat=25, max_msg_size=8 << 20)
            except BaseException:
                await session.close()
                raise
            return cls(ws, session)
        try:
            import websockets
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("instala aiohttp o websockets para usar los WebSocket") from exc
        try:
            ws = await websockets.connect(url, additional_headers=headers, max_size=8 << 20)
        except TypeError:  # websockets < 14
            ws = await websockets.connect(url, extra_headers=headers, max_size=8 << 20)
        return cls(ws)

    async def send(self, data: dict) -> None:
        text = json.dumps(data)
        if self.session is not None:
            await self.ws.send_str(text)
        else:
            await self.ws.send(text)

    async def recv(self) -> Optional[dict]:
        """Siguiente mensaje JSON; None si se cerró la conexión."""
        while True:
            if self.session is not None:
                msg = await self.ws.receive()
                import aiohttp
                if msg.type == aiohttp.WSMsgType.TEXT:
                    text = msg.data
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING,
                                  aiohttp.WSMsgType.ERROR):
                    return None
                else:
                    continue
            else:
                try:
                    text = await self.ws.recv()
                except Exception:
                    return None
            try:
                data = json.loads(text)
            except ValueError:
                continue
            if isinstance(data, dict):
                return data

    async def close(self) -> None:
        try:
            await self.ws.close()
        finally:
            if self.session is not None:
                await self.session.close()


class SignalSocket:
    """Señales por WebSocket (``/ws/signal``), una conexión persistente.

    ::

        async with SignalSocket(EXECUTOR_URL, EXECUTOR_SECRET) as ws:
            reply = await ws.send({"action": "open", "trade_id": 1, "symbol": "BTCUSDT",
                                   "direction": "SHORT", "quantity": 0.001}, wait=True)
    """

    def __init__(self, base_url: Optional[str] = None, secret: Optional[str] = None, timeout: float = 20.0):
        self.url = _ws_url(base_url or os.getenv("EXECUTOR_URL", "http://127.0.0.1:10000"), "/ws/signal")
        self.secret = secret if secret is not None else os.getenv("EXECUTOR_SECRET", DEFAULT_SECRET)
        self.timeout = timeout
        self._conn: Optional[_WSConn] = None
        self._ids = itertools.count(1)
        self._waiters: dict[Any, asyncio.Future] = {}
        self._reader: Optional[asyncio.Task] = None

    async def connect(self) -> "SignalSocket":
        self._conn = await _WSConn.connect(self.url, {"X-Signal-Secret": self.secret, "User-Agent": USER_AGENT})
        self._reader = asyncio.create_task(self._read())
        return self

    async def _read(self) -> None:
        conn = self._conn
        assert conn is not None
        try:
            while True:
                msg = await conn.recv()
                if msg is None:
                    break
                fut = self._waiters.pop(msg.get("id"), None)
                if fut is not None and not fut.done():
                    fut.set_result(msg)
        finally:
            for fut in self._waiters.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("WebSocket /ws/signal cerrado"))
            self._waiters.clear()
            if self._conn is conn:
                self._conn = None
            try:
                await conn.close()
            except Exception:
                pass

    async def send(self, payload: dict, wait: bool = False) -> dict:
        """Envía una señal y devuelve la respuesta (``status``, ``ok``, ``result`` si ``wait``).

        Si la conexión se cayó, reconecta. El ``signal_id`` hace seguro reenviar.
        """
        if self._conn is None or self._reader is None or self._reader.done():
            await self.connect()
        assert self._conn is not None
        req_id = next(self._ids)
        msg = dict(payload)
        msg.setdefault("signal_id", new_signal_id())
        msg.update({"id": req_id, "wait": wait})
        fut = asyncio.get_running_loop().create_future()
        self._waiters[req_id] = fut
        try:
            await self._conn.send(msg)
            return await asyncio.wait_for(fut, self.timeout)
        finally:
            self._waiters.pop(req_id, None)

    async def ping(self) -> dict:
        return await self.send({"action": "ping"})

    async def close(self) -> None:
        if self._reader:
            self._reader.cancel()
        if self._conn:
            conn, self._conn = self._conn, None
            await conn.close()

    async def __aenter__(self) -> "SignalSocket":
        return await self.connect()

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()


class DashboardSocket:
    """Estado en vivo (``/ws``): el mismo flujo que usa el dashboard.

    ::

        async with DashboardSocket(EXECUTOR_URL) as dash:
            print(await dash.command("positions"))
            async for msg in dash:            # {"type": "state"|"event"|"error"|"logs"|...}
                if msg["type"] == "event":
                    print(msg["event"])
    """

    def __init__(self, base_url: Optional[str] = None, token: Optional[str] = None, timeout: float = 30.0):
        self.url = _ws_url(base_url or os.getenv("EXECUTOR_URL", "http://127.0.0.1:10000"), "/ws")
        self.token = token if token is not None else os.getenv("EXECUTOR_TOKEN", "")
        self.timeout = timeout
        self.state: Optional[dict] = None
        self._conn: Optional[_WSConn] = None
        self._ids = itertools.count(1)
        self._waiters: dict[Any, asyncio.Future] = {}
        self._inbox: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._reader: Optional[asyncio.Task] = None

    async def connect(self) -> "DashboardSocket":
        self._conn = await _WSConn.connect(self.url, {"User-Agent": USER_AGENT})
        await self._conn.send({"op": "auth", "token": self.token})
        reply = await asyncio.wait_for(self._conn.recv(), self.timeout)
        if not reply or not reply.get("ok"):
            await self._conn.close()
            raise ExecutorError(401, (reply or {}).get("error", "token inválido (EXECUTOR_TOKEN)"), reply)
        self._reader = asyncio.create_task(self._read())
        return self

    async def _read(self) -> None:
        assert self._conn is not None
        try:
            while True:
                msg = await self._conn.recv()
                if msg is None:
                    break
                if msg.get("type") == "reply":
                    fut = self._waiters.pop(msg.get("id"), None)
                    if fut is not None and not fut.done():
                        fut.set_result(msg)
                    continue
                if msg.get("type") == "state":
                    self.state = msg.get("data")
                self._push(msg)
        finally:
            for fut in self._waiters.values():
                if not fut.done():
                    fut.set_exception(ConnectionError("WebSocket /ws cerrado"))
            self._waiters.clear()
            self._push(None)  # fin del stream para «async for»

    def _push(self, msg: Optional[dict]) -> None:
        if self._inbox.full():  # cliente lento: se descarta lo más viejo
            self._inbox.get_nowait()
        self._inbox.put_nowait(msg)

    async def command(self, cmd: str, **args: Any) -> Any:
        """Ejecuta un comando (``GET /api/commands``) y devuelve ``data`` (lanza ExecutorError)."""
        assert self._conn is not None, "usa 'async with DashboardSocket(...)'"
        req_id = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._waiters[req_id] = fut
        await self._conn.send({"op": "cmd", "id": req_id, "cmd": cmd, "args": _clean(args)})
        try:
            reply = await asyncio.wait_for(fut, self.timeout)
        finally:
            self._waiters.pop(req_id, None)
        if not reply.get("ok"):
            raise ExecutorError(int(reply.get("status") or 400), str(reply.get("error")), reply)
        return reply.get("data")

    async def select(self, symbol: str) -> None:
        """Recibe además ``{"type": "ticker"}`` del símbolo cada segundo."""
        assert self._conn is not None
        await self._conn.send({"op": "select", "symbol": symbol.upper()})

    async def markets(self, on: bool = True) -> None:
        assert self._conn is not None
        await self._conn.send({"op": "markets", "on": on})

    def __aiter__(self) -> AsyncIterator[dict]:
        return self._iterate()

    async def _iterate(self) -> AsyncIterator[dict]:
        while True:
            msg = await self._inbox.get()
            if msg is None:
                return
            yield msg

    async def close(self) -> None:
        if self._reader:
            self._reader.cancel()
        if self._conn:
            await self._conn.close()

    async def __aenter__(self) -> "DashboardSocket":
        return await self.connect()

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()


# ═════════════════════════════════════════════════════════════════════════════
# 4. CLI
# ═════════════════════════════════════════════════════════════════════════════
def _parse_value(text: str) -> Any:
    """clave=valor: true/false/null, números «limpios» y JSON; el resto queda como texto
    (un id como 0012 o 1e5 no se convierte en número)."""
    low = text.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none"):
        return None
    if re.fullmatch(r"-?(0|[1-9]\d*)", text):
        return int(text)
    if re.fullmatch(r"-?(0|[1-9]\d*)\.\d+", text):
        return float(text)
    if text[:1] in "[{":
        try:
            return json.loads(text)
        except ValueError:
            pass
    return text


def _kv(pairs: Iterable[str]) -> dict:
    out: dict = {}
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"argumento inválido {pair!r}: usa clave=valor")
        k, v = pair.split("=", 1)
        out[k.strip()] = _parse_value(v)
    return out


def _print(data: Any) -> None:
    if isinstance(data, str):
        print(data)
    else:
        print(json.dumps(data, indent=2, ensure_ascii=False, default=str))


def _state_line(st: Any) -> str:
    if not isinstance(st, dict):
        return f"{time.strftime('%H:%M:%S')}  respuesta inesperada: {str(st)[:120]}"
    return (f"{time.strftime('%H:%M:%S')}  balance={st.get('balance', 0):.2f}  equity={st.get('equity', 0):.2f}  "
            f"realizado={st.get('realized_pnl', 0):+.4f}  no realizado={st.get('unrealized_pnl', 0):+.4f}  "
            f"abiertas={st.get('open_count', 0)}  win rate={st.get('win_rate', 0)}%  "
            f"trading={'ON' if st.get('trading_enabled') else 'PAUSA'}  ready={st.get('ready')}")


def _demo(client: ExecutorClient) -> None:
    """Recorrido de solo lectura por la API (no envía órdenes)."""
    print(f"→ Executor: {client.base_url}")
    print("→ GET /ready (espera a que despierte si está dormido)…")
    print("   listo" if client.wait_ready(timeout=90) else "   aún sincronizando (continúo)")
    health = client.health()
    print(f"→ GET /health: ok={health.get('ok')} versión={health.get('version')} "
          f"WS API={health.get('ws_api', {}).get('connected')} mercado={health.get('market', {}).get('connected')}")
    st = client.state(limit=5)
    print("→ GET /api/state:", _state_line(st))
    acct = client.account()
    print(f"→ GET /api/account: disponible={acct.get('available')} wallet={acct.get('wallet')} "
          f"hedge={acct.get('hedge_mode')}")
    pos = client.positions()
    print(f"→ GET /api/positions: {len(pos)} posición(es)")
    for p in pos[:10]:
        print(f"   {p.get('symbol')} {p.get('direction')} qty={p.get('qty')} entrada={p.get('entry')} "
              f"pnl={p.get('pnl')}")
    print(f"→ GET /api/orders: {len(client.orders())} orden(es)")
    sigs = client.signals(limit=5)
    print(f"→ GET /api/signals: últimas {len(sigs)}")
    for s in sigs:
        print(f"   {s.get('action')} {s.get('symbol')} {s.get('direction')} ok={s.get('ok')} — {s.get('detail')}")
    print(f"→ GET /api/commands: {len(client.commands())} comandos disponibles")
    exp = client.explain_error(-2019)
    print(f"→ GET /api/errors/-2019: {exp.get('title')} — {exp.get('solution', '')}")
    print("Listo. Para operar: open / close / cmd … (python executor_client.py --help)")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="executor_client.py",
        description="Cliente del Futures Executor (señales, estado y API completa).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="Variables: EXECUTOR_URL, EXECUTOR_SECRET, EXECUTOR_TOKEN. Referencia completa: "
               "python -c \"import executor_client; help(executor_client)\"")
    p.add_argument("--url", default=os.getenv("EXECUTOR_URL", "http://127.0.0.1:10000"), help="URL del executor")
    p.add_argument("--secret", default=None, help="SIGNAL_SECRET (por defecto EXECUTOR_SECRET o el de los bots)")
    p.add_argument("--token", default=os.getenv("EXECUTOR_TOKEN", ""), help="DASHBOARD_TOKEN (si existe)")
    p.add_argument("--timeout", type=float, default=15.0)
    sub = p.add_subparsers(dest="cmd", required=True)

    for name, helptext in [("health", "GET /health"), ("ready", "GET /ready"), ("snapshot", "GET /api/snapshot"),
                           ("account", "GET /api/account"), ("stats", "GET /api/stats"),
                           ("settings", "GET /api/settings"), ("commands", "GET /api/commands"),
                           ("grids", "GET /api/grids"), ("exchange-info", "GET /api/exchange-info"),
                           ("position-mode", "GET /api/position-mode"), ("csv", "GET /api/trades.csv"),
                           ("demo", "recorrido de solo lectura por la API")]:
        sub.add_parser(name, help=helptext)
    s = sub.add_parser("state", help="GET /api/state")
    s.add_argument("--limit", type=int)
    for name in ("positions", "orders"):
        s = sub.add_parser(name, help=f"GET /api/{name}")
        s.add_argument("symbol", nargs="?")
    s = sub.add_parser("trades", help="GET /api/trades")
    s.add_argument("--status", default="all", choices=["all", "open", "closed"])
    s.add_argument("--limit", type=int)
    s.add_argument("--symbol")
    s = sub.add_parser("signals", help="GET /api/signals")
    s.add_argument("--limit", type=int, default=30)
    s.add_argument("--trade-id", type=int)
    s.add_argument("--signal-id")
    s = sub.add_parser("live", help="GET /api/live")
    s.add_argument("symbols", nargs="?")
    s = sub.add_parser("symbol", help="GET /api/symbol/{symbol}")
    s.add_argument("symbol")
    s = sub.add_parser("errors", help="GET /api/errors")
    s.add_argument("--limit", type=int, default=30)
    s = sub.add_parser("explain", help="explica un código de error de Binance")
    s.add_argument("code", type=int)
    s = sub.add_parser("logs", help="GET /api/logs")
    s.add_argument("--limit", type=int, default=100)

    s = sub.add_parser("open", help="señal open (POST /signal, como los bots)")
    s.add_argument("symbol")
    s.add_argument("direction", choices=["LONG", "SHORT", "long", "short"])
    s.add_argument("--trade-id", type=int, default=0)
    s.add_argument("--price", type=float)
    s.add_argument("--quantity", type=float)
    s.add_argument("--notional", type=float)
    s.add_argument("--margin", type=float)
    s.add_argument("--level", type=float)
    s.add_argument("--leverage", type=int)
    s.add_argument("--tp", type=float)
    s.add_argument("--sl", type=float)
    s.add_argument("--no-wait", action="store_true", help="no esperar el resultado")
    s = sub.add_parser("close", help="señal close (POST /signal, como los bots)")
    s.add_argument("symbol")
    s.add_argument("--direction", choices=["LONG", "SHORT", "long", "short"])
    s.add_argument("--trade-id", type=int, default=0)
    s.add_argument("--reason", default="MANUAL")
    s.add_argument("--close-price", type=float)
    s.add_argument("--quantity", type=float)
    s.add_argument("--pnl", type=float)
    s.add_argument("--no-wait", action="store_true")
    s = sub.add_parser("close-all", help="señal close_all")
    s.add_argument("--no-wait", action="store_true")

    s = sub.add_parser("manual-open", help="POST /api/open (apertura manual)")
    s.add_argument("symbol")
    s.add_argument("direction", choices=["LONG", "SHORT", "long", "short"])
    s.add_argument("amount", type=float)
    s.add_argument("--size-mode", default="notional", choices=["notional", "margin", "qty"])
    s.add_argument("--leverage", type=int)
    s.add_argument("--limit-price", type=float, help="orden LIMIT a este precio")
    s.add_argument("--tp", type=float)
    s.add_argument("--sl", type=float)
    s = sub.add_parser("manual-close", help="POST /api/close/{symbol}")
    s.add_argument("symbol")
    s.add_argument("--direction")
    s.add_argument("--quantity", type=float)
    for name in ("tp", "sl"):
        s = sub.add_parser(name, help=f"POST /api/set-{name}/{{symbol}}")
        s.add_argument("symbol")
        s.add_argument("trigger_price", type=float)
        s.add_argument("--direction")
    s = sub.add_parser("trading", help="POST /api/trading (on / off / toggle)")
    s.add_argument("mode", choices=["on", "off", "toggle"])
    s = sub.add_parser("leverage", help="POST /api/leverage[/{symbol}]")
    s.add_argument("leverage", type=int)
    s.add_argument("--symbol")

    s = sub.add_parser("cmd", help="cualquier comando: cmd NOMBRE clave=valor …")
    s.add_argument("name")
    s.add_argument("args", nargs="*")
    s = sub.add_parser("call", help="petición libre: call MÉTODO /ruta clave=valor …")
    s.add_argument("method")
    s.add_argument("path")
    s.add_argument("args", nargs="*")
    s = sub.add_parser("watch", help="muestra /api/state cada N segundos")
    s.add_argument("--every", type=float, default=5.0)
    s = sub.add_parser("stream", help="eventos en vivo por WebSocket /ws (requiere aiohttp o websockets)")
    s.add_argument("--state", action="store_true", help="incluir el estado completo cada segundo")
    return p


def run_cli(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    client = ExecutorClient(args.url, args.secret, args.token, timeout=args.timeout)
    c = args.cmd
    simple = {"health": client.health, "ready": client.ready, "snapshot": client.snapshot, "account": client.account,
              "stats": client.stats, "settings": client.settings, "commands": client.commands,
              "grids": client.grids, "exchange-info": client.exchange_info, "position-mode": client.position_mode,
              "csv": client.trades_csv}
    try:
        if c in simple:
            _print(simple[c]())
        elif c == "demo":
            _demo(client)
        elif c == "state":
            _print(client.state(args.limit))
        elif c == "positions":
            _print(client.positions(args.symbol))
        elif c == "orders":
            _print(client.orders(args.symbol))
        elif c == "trades":
            _print(client.trades(args.status, args.limit, args.symbol))
        elif c == "signals":
            _print(client.signals(args.limit, trade_id=args.trade_id, signal_id=args.signal_id))
        elif c == "live":
            _print(client.live(args.symbols))
        elif c == "symbol":
            _print(client.symbol(args.symbol))
        elif c == "errors":
            _print(client.errors(args.limit))
        elif c == "explain":
            _print(client.explain_error(args.code))
        elif c == "logs":
            for line in client.logs(args.limit):
                print(f"{line.get('level', '')[:4]} {line.get('name', '')}: {line.get('msg', '')}"
                      if isinstance(line, dict) else line)
        elif c == "open":
            _print(client.open_signal(args.symbol, args.direction, trade_id=args.trade_id, price=args.price,
                                      quantity=args.quantity, notional=args.notional, margin=args.margin,
                                      level=args.level, leverage=args.leverage, tp=args.tp, sl=args.sl,
                                      wait=not args.no_wait))
        elif c == "close":
            _print(client.close_signal(args.symbol, args.direction, trade_id=args.trade_id, reason=args.reason,
                                       close_price=args.close_price, quantity=args.quantity, pnl=args.pnl,
                                       wait=not args.no_wait))
        elif c == "close-all":
            _print(client.close_all_signal(wait=not args.no_wait))
        elif c == "manual-open":
            _print(client.open(args.symbol, args.direction, args.amount, args.size_mode, args.leverage,
                               "LIMIT" if args.limit_price else "MARKET", args.limit_price, args.tp, args.sl))
        elif c == "manual-close":
            _print(client.close(args.symbol, args.direction, args.quantity))
        elif c in ("tp", "sl"):
            fn = client.set_tp if c == "tp" else client.set_sl
            _print(fn(args.symbol, args.trigger_price, args.direction))
        elif c == "trading":
            _print(client.set_trading(None if args.mode == "toggle" else args.mode == "on"))
        elif c == "leverage":
            _print(client.set_symbol_leverage(args.symbol, args.leverage) if args.symbol
                   else client.set_leverage(args.leverage))
        elif c == "cmd":
            _print(client.command(args.name, **_kv(args.args)))
        elif c == "call":
            kv = _kv(args.args)
            method = args.method.upper()
            if method in ("GET", "DELETE", "HEAD"):
                _print(client.request(method, args.path, query=kv))
            else:
                _print(client.request(method, args.path, kv))
        elif c == "watch":
            while True:
                try:
                    print(_state_line(client.state(limit=1)), flush=True)
                except ExecutorError as exc:
                    print(f"{time.strftime('%H:%M:%S')}  {exc}", flush=True)
                time.sleep(max(1.0, args.every))
        elif c == "stream":
            asyncio.run(_stream(client, args.state))
    except ExecutorError as exc:
        print(f"error: {exc}", file=sys.stderr)
        if exc.diagnosis:
            _print(exc.diagnosis)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


async def _stream(client: ExecutorClient, with_state: bool) -> None:
    async with DashboardSocket(client.base_url, client.token or client.secret) as dash:
        async for msg in dash:
            kind = msg.get("type")
            if kind == "event":
                ev = msg["event"]
                print(f"{time.strftime('%H:%M:%S')}  [{ev.get('kind')}] {ev.get('text', ev)}", flush=True)
            elif kind == "error":
                e = msg["entry"]
                print(f"{time.strftime('%H:%M:%S')}  ERROR {e.get('code')} {e.get('symbol', '')} "
                      f"{e.get('title', '')} — {e.get('solution', '')}", flush=True)
            elif kind == "state" and with_state:
                st = msg["data"]
                acct = st.get("account", {})
                print(f"{time.strftime('%H:%M:%S')}  equity={acct.get('equity', acct.get('wallet'))} "
                      f"posiciones={len(st.get('positions', []))}", flush=True)


def main(argv: Optional[list[str]] = None) -> int:
    return run_cli(argv)


if __name__ == "__main__":
    sys.exit(main())
