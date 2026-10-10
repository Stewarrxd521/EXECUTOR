"""Gestión de operaciones por señal (compatibles con app.py / app_25.py) y manuales.

Contrato con los bots ya desplegados (``ExecutorBridge``)::

    POST /signal  {"action": "open",  "trade_id", "symbol", "direction", "price", "quantity",
                   "notional"?, "level"?, "tp"?, "sl"?, "leverage"?, "signal_id"?}
    POST /signal  {"action": "close", "trade_id", "symbol", "direction", "reason", "close_price",
                   "pnl"?, "quantity"?}
    POST /signal  {"action": "close_all" | "open_tp" | "open_sl" | "close_tp" | "close_sl", ...}

Garantías para los bots:

* Cada tramo se ejecuta (mismo ``trade_id`` con distinto ``level``): el filtro
  antiduplicados viene apagado y, si se activa, solo descarta repeticiones
  idénticas o con el mismo ``signal_id`` (responde 409).
* Un ``close`` que llega mientras su ``open`` está en vuelo espera a que termine.
* Un ``close`` que llega ANTES que su ``open`` queda en espera ~30 s y se aplica
  en cuanto la apertura se llena (no quedan posiciones huérfanas).
* El cierre usa el tamaño REAL de la posición (consultado por WS API), así un
  tramo que entró justo antes también se cierra.
* ``trade_id`` se resuelve junto con el símbolo: dos bots con ids repetidos no
  se cierran el uno al otro.
* Se guardan los metadatos del bot (level, notional pedido, precio de la
  señal, pnl del bot, close_price del bot) para conciliar bot vs. real.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import time
from collections import Counter, OrderedDict, deque
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, Optional

from .account import OrderUpdate, Position
from .core import Core, utc_now_str
from .errors import Action, BinanceAPIError, ErrorDoctor
from .orders import OrderContext, OrderExecutor
from .precision import D, fmt, parse_bool, safe_float
from .storage import JsonStore

log = logging.getLogger("executor.trading")

PENDING_CLOSE_TTL_S = 30.0   # cierre que llegó antes que su apertura
INFLIGHT_WAIT_S = 30.0       # máximo que un close espera a su open en vuelo
READY_WAIT_S = 30.0          # máximo que una señal espera la 1.ª sincronización de cuenta
SIGNAL_ID_TTL_S = 900.0      # un signal_id explícito (reintentos del cliente) no se ejecuta dos veces
RECENT_CLOSE_TTL_S = 30.0    # un tramo (open) que llega tras el close de su trade_id no reabre
UNKNOWN_OPEN_CHECKS = (1.0, 3.0, 8.0)  # relecturas de la posición tras una apertura de estado desconocido

CLOSE_REASONS = {
    "TP": ("✅", "TAKE PROFIT 🎯"),
    "SL": ("❌", "STOP LOSS 🛑"),
    "CLOSED": ("🔄", "CIERRE EXTERNO"),
    "EXTERNAL": ("🔄", "CIERRE EXTERNO (Binance)"),
    "LIQUIDATION": ("💀", "LIQUIDACIÓN"),
    "ADL": ("⚠️", "AUTO-DELEVERAGE"),
    "MAIN_BOT": ("🔄", "CIERRE SEÑAL PRINCIPAL"),
    "CLOSE_ALL": ("🛑", "CIERRE GLOBAL (SEÑAL)"),
    "MANUAL": ("🖐", "CIERRE MANUAL"),
    "MANUAL_WEB": ("🖐", "CIERRE MANUAL (WEB)"),
    "NETTED": ("➖", "NETEADA POR SEÑAL OPUESTA"),
}

_TP_ALIASES = ("tp", "tp_price", "take_profit")
_SL_ALIASES = ("sl", "sl_price", "stop_loss")


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
    step_size: str = "1"
    fees_usdt: float = 0.0
    realized_exchange: float = 0.0
    source: str = "signal"
    tp_price: float = 0.0
    sl_price: float = 0.0
    opened_ts: float = field(default_factory=time.time)
    closed_ts: float = 0.0
    last_fill_ts: float = field(default_factory=time.time)
    notes: str = ""
    # Metadatos del bot que envía las señales (conciliación bot vs. real).
    paper_ids: list = field(default_factory=list)
    tranches: list = field(default_factory=list)
    signal_entry_price: float = 0.0
    bot_pnl: float = 0.0
    has_bot_pnl: bool = False
    signal_close_price: float = 0.0
    closed_by: str = ""

    @property
    def key(self) -> tuple[str, str]:
        return self.symbol, self.direction

    @property
    def notional_usdt(self) -> float:
        return self.entry_price * self.quantity

    @property
    def margin_usdt(self) -> float:
        return self.notional_usdt / self.leverage if self.leverage else self.notional_usdt

    @property
    def roe_pct(self) -> float:
        return self.pnl_usdt / self.margin_usdt * 100 if self.margin_usdt else 0.0

    def price_pnl(self, price: float) -> float:
        sign = 1 if self.direction == "LONG" else -1
        return (price - self.entry_price) * self.quantity * sign

    def update_unrealized(self, price: float) -> None:
        if price <= 0:
            return
        self.current_price = price
        self.pnl_usdt = self.price_pnl(price)
        self.roi_pct = self.pnl_usdt / self.notional_usdt * 100 if self.notional_usdt else 0.0

    def to_dict(self) -> dict:
        data = asdict(self)
        data["notional"] = self.notional_usdt
        data["margin"] = self.margin_usdt
        data["roe_pct"] = self.roe_pct
        data["levels"] = [t.get("level") for t in self.tranches if t.get("level")]
        data["pnl_diff"] = (self.pnl_usdt - self.bot_pnl) if self.has_bot_pnl else None
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "Trade":
        names = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in data.items() if k in names})


@dataclass
class Signal:
    """Señal normalizada (lo que llegó por /signal, /ws/signal o la API)."""

    action: str
    symbol: str
    direction: str
    trade_id: int
    data: dict
    signal_id: str
    source: str
    ip: str = ""
    dedupe_key: str = ""


def _fresh_status() -> dict:
    return {
        "signals_received": 0, "signals_open": 0, "signals_close": 0, "signals_rejected": 0,
        "signals_duplicate": 0, "signals_unauthorized": 0, "manual_closes": 0, "signals_tp_set": 0,
        "signals_tp_closed": 0, "signals_sl_set": 0, "signals_sl_closed": 0,
        "last_signal_time": "Esperando señales...", "last_signal_detail": "", "started_at": utc_now_str(),
    }


def _first(data: dict, keys: tuple, default: Any = None) -> Any:
    for k in keys:
        if data.get(k) not in (None, ""):
            return data[k]
    return default


class TradeManager:
    def __init__(self, core: Core, orders: OrderExecutor):
        self.core = core
        self.orders = orders
        self.settings = core.settings
        self.trades: dict[tuple[str, str], Trade] = {}
        self.closed: list[Trade] = []
        self.counter = 0
        self.paper_map: dict[tuple[int, str], tuple[str, str]] = {}
        self.status = _fresh_status()
        self.trading_enabled = True
        self.realized_total = 0.0
        self.leverage_override: Optional[int] = None
        self.signal_log: deque[dict] = deque(maxlen=300)
        self.grid_owner: Callable[[str, str], bool] = lambda symbol, direction: False
        self.accepting = True
        self.ready = asyncio.Event()
        self._session_start = time.time()
        self._locks: dict[str, asyncio.Lock] = {}
        self._closing: set[tuple[str, str]] = set()
        self._dedupe: dict[str, float] = {}
        self._inflight: Counter = Counter()
        self._pending_close: dict[tuple, dict] = {}
        self._rejected_ids: OrderedDict = OrderedDict()
        self._tasks: set[asyncio.Task] = set()
        self._unauth_warned: OrderedDict = OrderedDict()
        self._unknown_open: dict[tuple[str, str], float] = {}
        self._reconciling: set[tuple[str, str]] = set()
        self._skipped_closed: dict[str, float] = {}
        self._recent_closed: dict[tuple[int, str, str], float] = {}
        # Orden por símbolo: un close espera a los open aceptados ANTES que él, y un
        # open espera a los close aceptados antes que él (nunca a los posteriores).
        self._open_tasks: dict[str, list[tuple[str, asyncio.Task]]] = {}
        self._close_tasks: dict[str, list[tuple[str, asyncio.Task]]] = {}
        self._last_close_fill: dict[str, OrderUpdate] = {}
        self._pending_external: set[tuple[str, str]] = set()
        self.store = JsonStore(core.settings.data_dir / "trades.json", self._serialize)

    # ── Persistencia ──────────────────────────────────────────────────────
    def _serialize(self) -> dict:
        return {
            "counter": self.counter,
            "trading_enabled": self.trading_enabled,
            "leverage_override": self.leverage_override,
            "env_leverage_at_override": self.settings.leverage_env if self.leverage_override else None,
            "realized_total": self.realized_total,
            "status": self.status,
            "open": [t.to_dict() for t in self.trades.values()],
            "closed": [t.to_dict() for t in self.closed[-500:]],
        }

    def load(self) -> None:
        data = self.store.load({}) or {}
        self.counter = int(data.get("counter", 0))
        if not data.get("trading_enabled", True):
            if self.settings.restore_trading_pause:
                self.trading_enabled = False
                log.warning("Trading PAUSADO (restaurado de la sesión anterior): las aperturas por señal se "
                            "rechazarán hasta reactivarlo en el dashboard (RESTORE_TRADING_PAUSE=false lo evita)")
            else:
                log.info("Pausa de trading anterior descartada (RESTORE_TRADING_PAUSE=false)")
        override, env_at = data.get("leverage_override"), data.get("env_leverage_at_override")
        if override and env_at == self.settings.leverage_env:
            self.settings.leverage = self.leverage_override = int(override)
            log.info("Leverage por defecto %sx (fijado desde el dashboard)", override)
        elif override:
            log.info("LEVERAGE del entorno (%sx) reemplaza el guardado desde el dashboard (%sx)",
                     self.settings.leverage_env, override)
        started = self.status["started_at"]
        self.status.update(data.get("status", {}))
        self.status["started_at"] = started
        for raw in data.get("open", []):
            t = Trade.from_dict(raw)
            self.trades[t.key] = t
            for pid in set(t.paper_ids or []) | ({t.paper_trade_id} if t.paper_trade_id else set()):
                self.paper_map[(int(pid), t.symbol)] = t.key
        self.closed = [Trade.from_dict(raw) for raw in data.get("closed", [])]
        self.realized_total = float(data.get("realized_total", sum(t.pnl_usdt for t in self.closed)))
        if self.trades or self.closed:
            log.info("Estado restaurado: %d abiertas, %d cerradas", len(self.trades), len(self.closed))

    def save(self) -> None:
        self.store.schedule()

    # ── Consultas ─────────────────────────────────────────────────────────
    def _lock(self, symbol: str) -> asyncio.Lock:
        return self._locks.setdefault(symbol, asyncio.Lock())

    @property
    def open_trades(self) -> list[Trade]:
        return list(self.trades.values())

    def get_trade(self, symbol: str, direction: Optional[str] = None) -> Optional[Trade]:
        symbol = symbol.upper()
        if direction:
            return self.trades.get((symbol, direction.upper()))
        matches = [t for (s, _), t in self.trades.items() if s == symbol]
        return matches[0] if len(matches) == 1 else None

    def find_by_paper_id(self, paper_id: int, symbol: Optional[str] = None,
                         direction: Optional[str] = None) -> Optional[Trade]:
        """Trade de un ``trade_id`` del bot, validando símbolo y dirección."""
        if not paper_id:
            return None
        if symbol:
            key = self.paper_map.get((int(paper_id), symbol.upper()))
            trade = self.trades.get(key) if key else None
            if trade and direction and trade.hedge_mode and trade.direction != direction.upper():
                return None
            return trade
        cands = [self.trades.get(k) for (pid, _), k in self.paper_map.items() if pid == int(paper_id)]
        cands = [t for t in cands if t is not None]
        return cands[0] if len(cands) == 1 else None

    def _map_paper(self, trade: Trade, paper_id: int) -> None:
        if not paper_id:
            return
        old = self.paper_map.get((paper_id, trade.symbol))
        if old and old != trade.key and old in self.trades:
            log.warning("trade_id %s de %s pasa de %s a %s", paper_id, trade.symbol, old[1], trade.direction)
        self.paper_map[(paper_id, trade.symbol)] = trade.key
        if paper_id not in trade.paper_ids:
            trade.paper_ids.append(paper_id)

    def _rekey_paper(self, old_key: tuple[str, str], new_key: tuple[str, str]) -> None:
        for k, v in list(self.paper_map.items()):
            if v == old_key:
                self.paper_map[k] = new_key

    def _unmap_trade(self, trade: Trade) -> None:
        for k, v in list(self.paper_map.items()):
            if v == trade.key:
                del self.paper_map[k]

    @property
    def realized_pnl(self) -> float:
        return self.realized_total

    @property
    def unrealized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self.trades.values())

    def leverage_for_price(self, price: float) -> int:
        s = self.settings
        return s.high_price_leverage if price > s.high_price_threshold else s.leverage

    def set_default_leverage(self, leverage: int) -> None:
        self.settings.leverage = self.leverage_override = int(leverage)
        self.save()

    def refresh_marks(self) -> None:
        for t in self.trades.values():
            price = self.core.market.price(t.symbol)
            if price:
                t.update_unrealized(price)

    def stats(self) -> dict:
        wins = sum(1 for t in self.closed if t.pnl_usdt > 0)
        total = len(self.closed)
        wins_tp = sum(1 for t in self.closed if t.status == "TP")
        return {
            "wins": wins, "losses": total - wins,
            "win_rate": (wins / total * 100) if total else None,
            "wins_tp": wins_tp, "losses_tp": total - wins_tp,
            "win_rate_tp": (wins_tp / total * 100) if total else None,
            "realized_pnl": self.realized_total, "unrealized_pnl": self.unrealized_pnl,
            "fees": sum(t.fees_usdt for t in self.closed),
            "closed_count": total, "open_count": len(self.trades),
        }

    def inflight(self, symbol: Optional[str] = None) -> int:
        return sum(n for (s, _), n in self._inflight.items() if symbol is None or s == symbol)

    # ── Registro de señales ───────────────────────────────────────────────
    def _log_signal(self, action: str, symbol: str, direction: str, ok: bool, detail: str,
                    sig: Optional[Signal] = None, **extra) -> dict:
        direction = direction if direction in ("LONG", "SHORT", "BOTH") else ""
        entry = {"ts": time.time(), "action": str(action)[:32], "symbol": str(symbol)[:32], "direction": direction,
                 "ok": ok, "detail": str(detail)[:400]}
        if sig is not None:
            entry.update({"signal_id": sig.signal_id, "trade_id": sig.trade_id, "source": sig.source})
            if sig.data.get("level") not in (None, ""):
                entry["level"] = sig.data.get("level")
        entry.update(extra)
        self.signal_log.append(entry)
        return entry

    def signals(self, limit: int = 100, since_ts: float = 0.0, trade_id: Optional[int] = None,
                signal_id: Optional[str] = None) -> list[dict]:
        rows = [e for e in self.signal_log if e["ts"] > since_ts]
        if trade_id is not None:
            rows = [e for e in rows if e.get("trade_id") == trade_id]
        if signal_id:
            rows = [e for e in rows if e.get("signal_id") == signal_id]
        return rows[-limit:][::-1] if limit else rows[::-1]

    def note_unauthorized(self, ip: str, data: Any, source: str = "http") -> None:
        """Una señal con secreto incorrecto queda visible (dashboard, /api/signals)."""
        self.status["signals_unauthorized"] += 1
        action = str(data.get("action", ""))[:32] if isinstance(data, dict) else ""
        symbol = str(data.get("symbol", "")).upper()[:32] if isinstance(data, dict) else ""
        ip = str(ip)[:64]
        now = time.time()
        last = self._unauth_warned.get("*", 0)
        if now - last < 10:
            return  # ya registrado: solo cuenta (no inunda el registro de señales)
        self._unauth_warned["*"] = now
        self._log_signal(action or "?", symbol, "", False,
                         "secreto incorrecto (X-Signal-Secret no coincide con SIGNAL_SECRET)",
                         source=source, ip=ip)
        if now - last > 60:
            log.warning("Señal %s %s rechazada desde %s: secreto incorrecto. El valor debe coincidir con "
                        "EXECUTOR_SECRET (app_25) / ExecutorBridge(signal_secret=...)", action, symbol, ip or "?")

    def _reject(self, sig: Signal, error: str, status: int = 400, **extra) -> tuple[int, dict, None]:
        self.status["signals_rejected"] += 1
        self._log_signal(sig.action, sig.symbol, sig.direction, False, error, sig)
        if sig.action == "open" and sig.trade_id and status != 503:  # 503 = reintentable, no es definitivo
            self._mark_rejected(sig.trade_id, sig.symbol)
        log.warning("Señal %s %s %s rechazada: %s", sig.action.upper(), sig.symbol, sig.direction, error)
        return status, {"ok": False, "error": error, "signal_id": sig.signal_id, **extra}, None

    def _mark_rejected(self, trade_id: int, symbol: str) -> None:
        if (trade_id, symbol) in self.paper_map:
            return  # ese trade_id ya tiene posición abierta (otro tramo sí entró)
        if any(s == symbol for s, _ in self._reconciling):
            return  # otro tramo de estado desconocido aún se verifica
        self._rejected_ids[(trade_id, symbol)] = time.time()
        while len(self._rejected_ids) > 1000:
            self._rejected_ids.popitem(last=False)

    # ── Antiduplicados ────────────────────────────────────────────────────
    # * signal_id / idempotency_key / header Idempotency-Key explícitos: siempre
    #   (un reintento del cliente no abre dos veces). Los bots desplegados no los
    #   envían, así que sus tramos con el mismo trade_id nunca se descartan.
    # * Mismo contenido: solo si SIGNAL_DEDUPE_TTL_S > 0.
    @staticmethod
    def _dedupe_key(data: dict) -> str:
        canon = json.dumps({k: v for k, v in data.items() if k not in ("secret", "token", "wait")},
                           sort_keys=True, default=str)
        return "sha1:" + hashlib.sha1(canon.encode()).hexdigest()

    def _dedupe_ttl(self, key: str) -> float:
        ttl = self.settings.signal_dedupe_ttl_s
        return max(ttl, SIGNAL_ID_TTL_S) if key.startswith("id:") else ttl

    def _dedupe_seen(self, key: str) -> bool:
        now = time.time()
        self._dedupe = {k: ts for k, ts in self._dedupe.items() if now - ts < self._dedupe_ttl(k)}
        return key in self._dedupe

    def _release_dedupe(self, sig: Signal) -> None:
        if sig.dedupe_key:
            self._dedupe.pop(sig.dedupe_key, None)

    # ── Entrada de señales ────────────────────────────────────────────────
    def handle_signal(self, data: dict, source: str = "http", ip: str = "",
                      signal_id: str = "") -> tuple[int, dict]:
        """Valida, responde al instante y ejecuta en segundo plano (como antes)."""
        status, body, _ = self._dispatch(data, source, ip, signal_id)
        return status, body

    async def submit_signal(self, data: dict, source: str = "http", ip: str = "", wait: bool = False,
                            signal_id: str = "") -> tuple[int, dict]:
        """Como handle_signal; con ``wait`` espera el resultado (hasta SIGNAL_WAIT_TIMEOUT_S)."""
        status, body, task = self._dispatch(data, source, ip, signal_id)
        if wait and task is not None:
            try:
                outcome = await asyncio.wait_for(asyncio.shield(task), self.settings.signal_wait_timeout_s)
                body["result"] = outcome
                if isinstance(outcome, dict) and not outcome.get("ok", True):
                    body["ok"] = False
                    body["error"] = outcome.get("detail", "la señal no se pudo ejecutar")
            except asyncio.TimeoutError:
                body["pending"] = True
                body["detail"] = "sigue ejecutándose; consulta GET /api/signals?signal_id=" + body.get("signal_id", "")
        return status, body

    def _spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    def _dispatch(self, data: Any, source: str, ip: str, signal_id: str) -> tuple[int, dict, Optional[asyncio.Task]]:
        if not isinstance(data, dict):
            return 400, {"ok": False, "error": "invalid json"}, None
        data = {k: v for k, v in data.items() if k not in ("secret", "token")}
        action = str(data.get("action", "")).lower().strip()
        symbol = str(data.get("symbol", "")).upper().strip()
        direction = str(data.get("direction", "")).upper().strip()
        try:
            trade_id = int(float(data.get("trade_id", 0) or 0))
        except (TypeError, ValueError, OverflowError):
            trade_id = 0
        explicit_id = str(signal_id or data.get("signal_id") or data.get("idempotency_key") or "")[:100]
        sig = Signal(action, symbol, direction, trade_id, data, explicit_id or secrets.token_hex(6), source, ip)

        self.status["signals_received"] += 1
        self.status["last_signal_time"] = time.strftime("%H:%M:%S UTC", time.gmtime())
        self.status["last_signal_detail"] = f"{action.upper()} {symbol} {direction}".strip()

        if not self.accepting:
            return self._reject(sig, "executor reiniciándose: reintenta en unos segundos", 503, retry=True)

        if explicit_id or self.settings.signal_dedupe_ttl_s > 0:
            sig.dedupe_key = f"id:{explicit_id}" if explicit_id else self._dedupe_key(data)
            if self._dedupe_seen(sig.dedupe_key):
                self.status["signals_duplicate"] += 1
                if explicit_id:
                    # Reintento de una señal ya recibida: se confirma sin ejecutarla otra vez.
                    self._log_signal(action, symbol, direction, True, "reintento ignorado (signal_id ya recibido)",
                                     sig)
                    return 200, {"ok": True, "duplicate": True, "action": action, "symbol": symbol,
                                 "signal_id": sig.signal_id, "detail": "señal ya recibida: no se ejecuta otra vez"}, None
                self._log_signal(action, symbol, direction, False, "señal duplicada (mismo contenido)", sig)
                return 409, {"ok": False, "duplicate": True, "error": "señal duplicada", "signal_id": sig.signal_id}, None

        handler = {
            "open": self._accept_open, "close": self._accept_close, "close_all": self._accept_close_all,
            "open_tp": self._accept_protect, "open_sl": self._accept_protect,
            "close_tp": self._accept_unprotect, "close_sl": self._accept_unprotect,
        }.get(action)
        if handler is None:
            return self._reject(sig, f"acción desconocida (unknown action): {action or '(vacía)'}")
        status, body, task = handler(sig)
        if task is not None and sig.dedupe_key:
            self._dedupe[sig.dedupe_key] = time.time()
        body.setdefault("signal_id", sig.signal_id)
        return status, body, task

    # ── open ──────────────────────────────────────────────────────────────
    def _accept_open(self, sig: Signal):
        d, symbol, direction = sig.data, sig.symbol, sig.direction
        if not symbol or direction not in ("LONG", "SHORT"):
            return self._reject(sig, "faltan symbol o direction (LONG/SHORT)")
        quantity = safe_float(_first(d, ("quantity", "qty", "executed_qty")))
        notional = safe_float(_first(d, ("notional", "usdt")))
        margin = safe_float(d.get("margin"))
        if quantity <= 0 and notional <= 0 and margin <= 0 and self.settings.default_notional_usdt <= 0:
            return self._reject(sig, "falta el tamaño: envía quantity/qty, notional/usdt o margin "
                                     "(o configura DEFAULT_NOTIONAL_USDT)")
        if symbol in self.settings.blocked_symbols:
            return self._reject(sig, f"{symbol} está bloqueado (BLOCKED_SYMBOLS)", 200)
        if not self.trading_enabled:
            return self._reject(sig, "trading pausado desde el dashboard", 200)
        ok, why = self.core.exinfo.tradable(symbol)
        if not ok:
            return self._reject(sig, why, 200)
        if self.grid_owner(symbol, direction):
            return self._reject(sig, f"{symbol} {direction} está gestionado por un bot Grid", 200)
        self._inflight[(symbol, direction)] += 1
        prior_closes = self._pending_tasks(self._close_tasks, symbol, direction)
        task = self._spawn(self._signal_open(sig, quantity, notional, margin, prior_closes))
        self._track(self._open_tasks, symbol, direction, task)
        return 200, {"ok": True, "action": "open", "symbol": symbol, "direction": direction}, task

    # ── Orden de señales por símbolo ───────────────────────────────────────
    @staticmethod
    def _track(registry: dict, symbol: str, direction: Optional[str], task: asyncio.Task) -> None:
        rows = [(d, t) for d, t in registry.get(symbol, []) if not t.done()]
        rows.append((direction or "", task))
        registry[symbol] = rows

    @staticmethod
    def _pending_tasks(registry: dict, symbol: str, direction: Optional[str]) -> list[asyncio.Task]:
        if not symbol:  # close solo con trade_id: espera todas las aperturas en vuelo
            return [t for rows in registry.values() for _, t in rows if not t.done()]
        return [t for d, t in registry.get(symbol, []) if not t.done() and (not direction or not d or d == direction)]

    @staticmethod
    async def _wait_tasks(tasks: list[asyncio.Task]) -> None:
        pending = [t for t in tasks if not t.done()]
        if pending:
            await asyncio.wait(pending, timeout=INFLIGHT_WAIT_S)

    def _note_closed(self, trade_id: int, symbol: str, direction: Optional[str]) -> None:
        if trade_id and symbol:
            self._recent_closed[(trade_id, symbol, direction or "")] = time.time()

    def _closed_recently(self, trade_id: int, symbol: str, direction: str) -> Optional[float]:
        if not trade_id:
            return None
        now = time.time()
        self._recent_closed = {k: ts for k, ts in self._recent_closed.items() if now - ts < RECENT_CLOSE_TTL_S}
        ts = self._recent_closed.get((trade_id, symbol, direction)) or self._recent_closed.get((trade_id, symbol, ""))
        return now - ts if ts else None

    async def _await_ready(self) -> None:
        if self.ready.is_set():
            return
        try:
            await asyncio.wait_for(self.ready.wait(), READY_WAIT_S)
        except asyncio.TimeoutError:
            log.warning("La cuenta aún no se sincronizó; se ejecuta la señal igualmente")

    async def _signal_open(self, sig: Signal, quantity: float, notional: float, margin: float,
                           prior_closes: Optional[list] = None) -> dict:
        d, symbol, direction = sig.data, sig.symbol, sig.direction
        price = safe_float(d.get("price"))
        level = safe_float(d.get("level"))
        lev = int(safe_float(d.get("leverage")))
        lev = lev if 1 <= lev <= 125 else None
        trade: Optional[Trade] = None
        note = ""
        unknown = False
        baseline = 0.0
        already_closed: Optional[float] = None
        try:
            await self._await_ready()
            await self._wait_tasks(prior_closes or [])
            already_closed = self._closed_recently(sig.trade_id, symbol, direction)
            if already_closed is None:
                # Tamaño como antes: quantity × price; notional/margin solo si no hay quantity.
                if quantity > 0 and price > 0:
                    q_notional = quantity * price
                    if notional > 0 and abs(q_notional - notional) / max(notional, q_notional) > 0.10:
                        note = f"notional {notional:g} ≠ quantity×price {q_notional:.4g}: se usa quantity"
                    notional = 0.0
                if margin > 0 and notional <= 0 and quantity <= 0:
                    ref = price or self.core.market.price(symbol) or 1.0
                    notional = margin * (lev or self.leverage_for_price(ref))
                self._unknown_open.pop((symbol, direction), None)
                before = self.core.account.position(symbol, direction)
                baseline = before.qty if before is not None else 0.0
                trade = await self.open_trade(symbol, direction, signal_price=price, quantity=quantity,
                                              notional=notional, paper_trade_id=sig.trade_id, source="signal",
                                              leverage=lev, level=level, signal_id=sig.signal_id)
                already_closed = self._skipped_closed.pop(sig.signal_id, None)
                unknown = trade is None and self._unknown_open.pop((symbol, direction), None) is not None
        except Exception:
            unknown = True
            log.exception("Apertura %s %s falló de forma inesperada", direction, symbol)
        finally:
            key = (symbol, direction)
            self._inflight[key] -= 1
            if self._inflight[key] <= 0:
                del self._inflight[key]

        if already_closed is not None:
            self.status["signals_rejected"] += 1
            entry = self._log_signal("open", symbol, direction, False,
                                     f"tramo ignorado: el bot cerró el trade_id {sig.trade_id} hace "
                                     f"{already_closed:.0f}s", sig)
            return {"ok": False, "detail": entry["detail"]}
        if trade is None:
            self.status["signals_rejected"] += 1
            self._release_dedupe(sig)
            if unknown:
                # Pudo ejecutarse en Binance: no se marca como rechazada (su close debe
                # funcionar) y se relee la posición real para registrarla si existe.
                self._reconciling.add((symbol, direction))
                self._spawn(self._reconcile_unknown_open(symbol, direction, sig.trade_id, baseline))
                entry = self._log_signal("open", symbol, direction, False,
                                         "estado desconocido: se verifica la posición real en Binance", sig)
            else:
                if sig.trade_id:
                    self._mark_rejected(sig.trade_id, symbol)
                entry = self._log_signal("open", symbol, direction, False, "no ejecutada (ver pestaña Errores)", sig)
            return {"ok": False, "detail": entry["detail"]}

        self.status["signals_open"] += 1
        self._rejected_ids.pop((sig.trade_id, symbol), None)
        # ¿Llegó antes un close para esta apertura? Se marca YA (sin esperas de por medio)
        # para que un tramo posterior del mismo trade_id no reabra mientras se cierra.
        pending = self._take_pending_close(sig.trade_id, symbol, direction)
        if pending is not None:
            self._note_closed(sig.trade_id, symbol, direction)
        detail = f"#{trade.id} qty={trade.quantity:g} @ {trade.entry_price:g}" + (" (ASUMIDA)" if trade.order_assumed else "")
        if note:
            detail += f" — {note}"
        protections = []
        for kind, aliases in (("TP", _TP_ALIASES), ("SL", _SL_ALIASES)):
            trig = safe_float(_first(d, aliases)) if pending is None else 0.0
            if trig > 0:
                try:
                    await self.set_protection(symbol, trade.direction, kind, trig)
                    protections.append(kind)
                except BinanceAPIError as err:
                    detail += f" — {kind} no colocado: {ErrorDoctor.diagnose(err).info.title}"
        if protections:
            detail += " — " + "/".join(protections) + " colocado(s)"
        self._log_signal("open", symbol, direction, True, detail, sig, executor_trade_id=trade.id)

        if pending is not None and self.trades.get(trade.key) is trade:
            log.warning("Cierre diferido aplicado a %s %s (llegó antes que la apertura)", symbol, direction)
            ok = await self.close_trade(trade, pending["reason"], close_price=pending["close_price"],
                                        bot_pnl=pending["bot_pnl"], closed_by="signal")
            if ok:
                self.status["signals_close"] += 1
            else:
                self._recent_closed.pop((sig.trade_id, symbol, direction), None)
            self._log_signal("close", symbol, direction, ok, "cierre diferido aplicado (llegó antes que la apertura)",
                             sig)
        return {"ok": True, "detail": detail, "trade": trade.to_dict()}

    async def _reconcile_unknown_open(self, symbol: str, direction: str, paper_id: int, baseline: float) -> None:
        """Tras una apertura de estado desconocido, registra lo que Binance sí abrió.

        Solo se atribuye a la señal la cantidad que supera lo que ya había
        (``baseline``) y lo que ya está registrado: nunca una posición ajena.
        """
        try:
            for delay in UNKNOWN_OPEN_CHECKS:
                await asyncio.sleep(delay)
                pending = None
                async with self._lock(symbol):
                    pos, confirmed = await self._fresh_position(symbol, direction)
                    trade = self.trades.get((symbol, direction))
                    tracked = trade.quantity if trade is not None else 0.0
                    real = pos.qty if pos is not None else 0.0
                    tol = float(self.core.exinfo.get(symbol, pos.entry_price if pos else 0.0).step_size) / 2
                    if pos is None or real - max(baseline, tracked) <= tol:
                        if confirmed and delay == UNKNOWN_OPEN_CHECKS[-1]:
                            log.info("%s %s: la apertura de estado desconocido no se ejecutó", direction, symbol)
                        continue
                    if self.grid_owner(symbol, direction):
                        return
                    if trade is None:
                        trade = self.adopt_position(pos, paper_id=paper_id, source="signal")
                        self._announce_open(trade)
                    else:
                        trade.quantity = pos.qty
                        trade.entry_price = pos.entry_price or trade.entry_price
                        if paper_id:
                            self._map_paper(trade, paper_id)
                        self.save()
                    log.warning("%s %s: la apertura de estado desconocido SÍ se ejecutó (posición %s); registrada",
                                direction, symbol, pos.qty)
                    self._rejected_ids.pop((paper_id, symbol), None)
                    pending = self._take_pending_close(paper_id, symbol, direction) if paper_id else None
                    if pending is not None:
                        self._note_closed(paper_id, symbol, direction)
                if pending is not None and self.trades.get(trade.key) is trade:
                    ok = await self.close_trade(trade, pending["reason"], close_price=pending["close_price"],
                                                bot_pnl=pending["bot_pnl"], closed_by="signal")
                    if ok:
                        self.status["signals_close"] += 1
                return
        finally:
            self._reconciling.discard((symbol, direction))

    # ── close ─────────────────────────────────────────────────────────────
    def _accept_close(self, sig: Signal):
        d = sig.data
        if not sig.symbol and not sig.trade_id:
            return self._reject(sig, "falta symbol o trade_id")
        reason = str(d.get("reason") or "MAIN_BOT").upper()
        close_price = safe_float(d.get("close_price")) or safe_float(d.get("price"))
        quantity = safe_float(_first(d, ("quantity", "qty")))
        bot_pnl = safe_float(d["pnl"]) if d.get("pnl") not in (None, "") else None
        prior_opens = self._pending_tasks(self._open_tasks, sig.symbol, sig.direction or None)
        task = self._spawn(self._signal_close(sig, reason, close_price, quantity, bot_pnl, prior_opens))
        if sig.symbol:
            self._track(self._close_tasks, sig.symbol, sig.direction or None, task)
        return 200, {"ok": True, "action": "close", "symbol": sig.symbol}, task

    async def _wait_inflight(self, symbol: str, direction: Optional[str]) -> None:
        """Espera a que terminen las aperturas en vuelo del símbolo."""
        if not symbol:
            return
        deadline = time.time() + INFLIGHT_WAIT_S
        while time.time() < deadline:
            busy = any(n > 0 for (s, dr), n in self._inflight.items()
                       if s == symbol and (not direction or dr == direction))
            if not busy:
                break
            await asyncio.sleep(0.05)
        async with self._lock(symbol):
            pass  # hace cola detrás de quien tenga el lock del símbolo

    def _resolve_trade(self, trade_id: int, symbol: str, direction: Optional[str]) -> Optional[Trade]:
        trade = self.find_by_paper_id(trade_id, symbol or None, direction) if trade_id else None
        if trade is None and symbol:
            trade = self.get_trade(symbol, direction)
        return trade

    async def _signal_close(self, sig: Signal, reason: str, close_price: float, quantity: float,
                            bot_pnl: Optional[float], prior_opens: Optional[list] = None) -> dict:
        symbol, direction = sig.symbol, sig.direction or None
        await self._await_ready()
        await self._wait_tasks(prior_opens or [])  # solo las aperturas aceptadas antes que este close
        if symbol:
            async with self._lock(symbol):
                pass  # hace cola detrás de quien tenga el lock del símbolo
        trade = self._resolve_trade(sig.trade_id, symbol, direction)
        detail = ""
        if trade is not None:
            symbol = symbol or trade.symbol
            ok = await self.close_trade(trade, reason, close_price=close_price, max_qty=quantity or None,
                                        bot_pnl=bot_pnl, closed_by="signal")
            detail = "cerrada" if ok else "error al cerrar (ver pestaña Errores)"
            if ok and not quantity:
                self._note_closed(sig.trade_id, trade.symbol, trade.direction)
        elif (sig.trade_id and (sig.trade_id, symbol) in self._rejected_ids
              and not await self._untracked_position(symbol, direction)):
            ok = False
            detail = f"cierre ignorado: el trade_id {sig.trade_id} nunca se abrió en el executor"
        else:
            ok = await self.close_symbol(symbol, direction, reason, paper_id=sig.trade_id, bot_pnl=bot_pnl,
                                         closed_by="signal") if symbol else False
            if ok:
                detail = "cerrada (posición sin trade local)"
                if not quantity:
                    self._note_closed(sig.trade_id, symbol, direction)
            elif sig.trade_id:
                self._remember_pending_close(sig.trade_id, symbol, direction, reason, close_price, bot_pnl)
                detail = (f"sin posición todavía: el cierre queda en espera {PENDING_CLOSE_TTL_S:.0f}s "
                          "por si la apertura llega tarde")
            else:
                detail = "sin posición abierta (close sin trade_id: no queda en espera)"
        if ok:
            self.status["signals_close"] += 1
        else:
            self._release_dedupe(sig)
        self._log_signal("close", symbol or (trade.symbol if trade else ""), sig.direction, ok, detail, sig,
                         reason=reason)
        return {"ok": ok, "detail": detail}

    def _remember_pending_close(self, trade_id: int, symbol: str, direction: Optional[str], reason: str,
                                close_price: float, bot_pnl: Optional[float]) -> None:
        self._pending_close[(trade_id, symbol, direction or "")] = {
            "reason": reason, "close_price": close_price, "bot_pnl": bot_pnl, "ts": time.time()}

    def _take_pending_close(self, trade_id: int, symbol: str, direction: str) -> Optional[dict]:
        now = time.time()
        self._pending_close = {k: v for k, v in self._pending_close.items() if now - v["ts"] < PENDING_CLOSE_TTL_S}
        for key in ((trade_id, symbol, direction), (trade_id, symbol, ""), (trade_id, "", "")):
            if key in self._pending_close:
                return self._pending_close.pop(key)
        return None

    async def _untracked_position(self, symbol: str, direction: Optional[str]) -> bool:
        """¿Hay una posición real (no de un grid) en el símbolo? Red de seguridad del close."""
        if not symbol:
            return False

        def found() -> bool:
            return any((not direction or p.direction == direction) and not self.grid_owner(symbol, p.direction)
                       for p in self.core.account.positions_for(symbol))

        if found():
            return True
        if any(s == symbol for s, _ in self._reconciling):
            await self._fresh_position(symbol, direction or "LONG")
            return found()
        return False

    # ── close_all / TP / SL ───────────────────────────────────────────────
    def _accept_close_all(self, sig: Signal):
        total = len(self.trades)
        task = self._spawn(self._signal_close_all(sig))
        return 200, {"ok": True, "action": "close_all", "positions_targeted": total}, task

    async def _signal_close_all(self, sig: Signal) -> dict:
        await self._await_ready()
        deadline = time.time() + INFLIGHT_WAIT_S
        while self._inflight and time.time() < deadline:
            await asyncio.sleep(0.05)
        closed = await self.close_all("CLOSE_ALL")
        self.status["signals_close"] += len(closed)
        detail = f"{len(closed)} posición(es) cerradas"
        self._log_signal("close_all", "", "", True, detail, sig)
        return {"ok": True, "detail": detail, "closed": len(closed)}

    def _accept_protect(self, sig: Signal):
        trigger = safe_float(sig.data.get("trigger_price")) or safe_float(sig.data.get("price"))
        kind = "TP" if sig.action == "open_tp" else "SL"
        trade = self._resolve_trade(sig.trade_id, sig.symbol, sig.direction or None) if sig.symbol else None
        pos = self.core.account.position(sig.symbol, sig.direction or None) if sig.symbol else None
        if trigger <= 0 or not sig.symbol or (trade is None and pos is None and not self.inflight(sig.symbol)):
            return self._reject(sig, f"{sig.action}: sin posición abierta para {sig.symbol} o trigger_price inválido")
        task = self._spawn(self._signal_protect(sig, kind, trigger))
        return 200, {"ok": True, "action": sig.action, "symbol": sig.symbol, "trigger_price": trigger}, task

    async def _signal_protect(self, sig: Signal, kind: str, trigger: float) -> dict:
        symbol = sig.symbol
        await self._await_ready()
        await self._wait_inflight(symbol, sig.direction or None)
        trade = self._resolve_trade(sig.trade_id, symbol, sig.direction or None)
        pos = self.core.account.position(symbol, sig.direction or None)
        direction = trade.direction if trade else (pos.direction if pos else sig.direction)
        if direction not in ("LONG", "SHORT"):
            self.status["signals_rejected"] += 1
            entry = self._log_signal(sig.action, symbol, sig.direction, False,
                                     "sin posición abierta (o LONG y SHORT simultáneas sin direction)", sig)
            return {"ok": False, "detail": entry["detail"]}
        try:
            await self.set_protection(symbol, direction, kind, trigger)
            self.status["signals_tp_set" if kind == "TP" else "signals_sl_set"] += 1
            self._log_signal(sig.action, symbol, direction, True, f"trigger {trigger:g}", sig)
            return {"ok": True, "detail": f"{kind} @ {trigger:g}"}
        except BinanceAPIError as err:
            self.status["signals_rejected"] += 1
            detail = ErrorDoctor.diagnose(err).summary()
            self._log_signal(sig.action, symbol, direction, False, detail, sig)
            return {"ok": False, "detail": detail}

    def _accept_unprotect(self, sig: Signal):
        if not sig.symbol:
            return self._reject(sig, f"{sig.action}: falta symbol")
        order_type = "TAKE_PROFIT_MARKET" if sig.action == "close_tp" else "STOP_MARKET"
        task = self._spawn(self._signal_unprotect(sig, order_type))
        return 200, {"ok": True, "action": sig.action, "symbol": sig.symbol}, task

    async def _signal_unprotect(self, sig: Signal, order_type: str) -> dict:
        await self._await_ready()
        n = await self.orders.cancel_protection(sig.symbol, sig.direction or None, {order_type})
        self.status["signals_tp_closed" if sig.action == "close_tp" else "signals_sl_closed"] += 1
        self._log_signal(sig.action, sig.symbol, sig.direction, True, f"{n} orden(es) canceladas", sig)
        label = "TP" if sig.action == "close_tp" else "SL"
        self.core.telegram.send(f"{'🎯' if label == 'TP' else '🛑'} <b>{label} cancelado</b>\n"
                                f"<code>{sig.symbol}</code> — {n} orden(es)")
        return {"ok": True, "detail": f"{n} orden(es) canceladas", "cancelled": n}

    async def drain(self, timeout: float = 20.0) -> int:
        """Deja de aceptar señales y espera a las que están en ejecución."""
        self.accepting = False
        pending = [t for t in self._tasks if not t.done()]
        if pending:
            log.info("Esperando %d señal(es) en ejecución antes de apagar…", len(pending))
            await asyncio.wait(pending, timeout=timeout)
        return sum(1 for t in pending if not t.done())

    # ── Apertura ──────────────────────────────────────────────────────────
    async def reference_price(self, symbol: str, fallback: float = 0.0) -> float:
        price = self.core.market.mark(symbol, max_age_s=self.settings.max_price_age_s)
        if price:
            return price
        try:
            price = await self.core.ws.ticker_price(symbol)
        except BinanceAPIError as err:
            self.orders._record(err, "ticker.price", symbol)
            price = 0.0
        if price:
            return price
        if fallback > 0:
            log.warning("%s sin precio de mercado: se usa el de la señal (%s)", symbol, fallback)
        return fallback

    async def open_trade(self, symbol: str, direction: str, *, signal_price: float = 0.0, quantity: float = 0.0,
                         notional: float = 0.0, paper_trade_id: int = 0, source: str = "signal",
                         leverage: Optional[int] = None, level: float = 0.0,
                         signal_id: str = "") -> Optional[Trade]:
        symbol, direction = symbol.upper(), direction.upper()
        side = "BUY" if direction == "LONG" else "SELL"
        async with self._lock(symbol):
            if source == "signal" and paper_trade_id:
                # Un close de este trade_id pudo terminar mientras se esperaba el lock.
                age = self._closed_recently(paper_trade_id, symbol, direction)
                if age is not None:
                    self._skipped_closed[signal_id] = age
                    return None
            ref = await self.reference_price(symbol, signal_price)
            if ref <= 0:
                self.core.bus.emit("open_failed", f"{symbol}: sin precio disponible", "error", symbol=symbol)
                return None
            rules = self.core.exinfo.get(symbol, ref)
            if notional > 0:
                desired = notional
            elif quantity > 0:
                desired = quantity * (signal_price if signal_price > 0 else ref)
            else:
                desired = self.settings.default_notional_usdt
            target_lev = leverage or self.leverage_for_price(ref)
            applied_lev = await self.orders.ensure_leverage(symbol, target_lev)
            confirmed = self.core.account.leverage.get(symbol) == applied_lev
            if not confirmed:
                msg = (f"{symbol}: no se pudo confirmar el leverage {applied_lev}x en Binance "
                       f"(¿PROXY_URLS/FIXIE_URL configurado?)")
                if self.settings.leverage_required:
                    self.core.bus.emit("open_failed", msg + " — apertura cancelada (LEVERAGE_REQUIRED)", "error")
                    return None
                log.warning("%s; se abre con el leverage que ya tenga el símbolo", msg)

            qty = rules.qty_for_notional(desired, ref, market=True, buffer_pct=self.settings.notional_buffer_pct,
                                         min_notional_floor=self.settings.min_notional_usdt)
            ctx = OrderContext(symbol, side, direction, "open", rules, market=True,
                               desired_notional=desired, ref_price=ref)
            log.info("Apertura %s %s: qty=%s (≈%.2f USDT, ref=%s, lev=%sx)", direction, symbol, fmt(qty),
                     float(qty) * ref, ref, applied_lev)
            owner = {"kind": "open", "symbol": symbol, "direction": direction}
            tranche = {"level": level, "notional_req": desired, "signal_price": signal_price,
                       "signal_id": signal_id, "ts": time.time()}
            try:
                fill = await self.orders.market(ctx, qty, prefix=f"X{paper_trade_id or 0}", owner=owner)
            except BinanceAPIError as err:
                diag = ErrorDoctor.diagnose(err)
                if diag.action == Action.CHECK_STATUS:
                    self._unknown_open[(symbol, direction)] = time.time()
                if diag.info.code == -2019 and self.settings.assume_on_margin_error:
                    log.warning("[ASUMIDA] %s %s: margen insuficiente, se registra como abierta", direction, symbol)
                    trade = self._register_entry(symbol, direction, ref, float(qty), applied_lev, paper_trade_id,
                                                 "MARGIN_INSUFFICIENT", self.core.account.is_hedge, rules.step_size,
                                                 assumed=True, source=source)
                    if trade is not None:
                        trade.tranches.append({**tranche, "qty": float(qty), "fill_price": ref, "assumed": True})
                        self._announce_open(trade)
                    return trade
                self.core.bus.emit("open_failed", f"{symbol}: {diag.info.title}", "error", symbol=symbol,
                                   code=diag.code, solution=diag.info.solution)
                self.core.telegram.send(f"⚠️ <b>Apertura fallida</b> {direction} <code>{symbol}</code>\n"
                                        f"[{diag.code}] {diag.info.title}\n💡 {diag.info.solution}")
                return None
            if not fill.ok:
                self.core.bus.emit("open_failed", f"{symbol}: la orden no se ejecutó ({fill.status})", "error")
                return None
            hedge = fill.position_side in ("LONG", "SHORT")
            trade = self._register_entry(symbol, direction, fill.avg_price or ref, fill.qty, applied_lev,
                                         paper_trade_id, fill.order_id, hedge, fill.step, source=source)
            for cid in fill.client_ids:
                self.orders.own(cid, trade if trade else owner)
            self.core.account.mark_updated(symbol, direction)
            if fill.fixes:
                log.info("Apertura %s corregida automáticamente: %s", symbol, "; ".join(fill.fixes))
            if trade is not None:
                trade.last_fill_ts = time.time()
                if signal_price and not trade.signal_entry_price:
                    trade.signal_entry_price = signal_price
                trade.tranches.append({**tranche, "qty": fill.qty, "fill_price": fill.avg_price,
                                       "order_id": fill.order_id, "leverage": applied_lev,
                                       "leverage_target": target_lev})
                self.save()
                self._announce_open(trade)
            return trade

    def _register_entry(self, symbol, direction, price, qty, leverage, paper_id, order_id, hedge, step,
                        assumed=False, source="signal") -> Optional[Trade]:
        key = (symbol, direction)
        now = utc_now_str()
        if hedge:
            existing = self.trades.get(key)
        else:
            existing = next((t for (s, _), t in self.trades.items() if s == symbol), None)

        if existing is None:
            self.counter += 1
            trade = Trade(id=self.counter, symbol=symbol, direction=direction, entry_price=price, quantity=qty,
                          open_time=now, leverage=leverage, paper_trade_id=paper_id, entry_order_id=str(order_id),
                          current_price=price, order_assumed=assumed, hedge_mode=hedge, step_size=fmt(step),
                          source=source)
            self.trades[key] = trade
            self._map_paper(trade, paper_id)
            self.save()
            return trade

        if hedge or existing.direction == direction:
            new_qty = existing.quantity + qty
            existing.entry_price = (existing.entry_price * existing.quantity + price * qty) / new_qty
            existing.quantity = float(D(fmt(new_qty)))
            tag = "AMPLIADA"
        else:
            # One-way: neto con signo (+ LONG / − SHORT), igual que Binance.
            signed = (existing.quantity if existing.direction == "LONG" else -existing.quantity) + \
                     (qty if direction == "LONG" else -qty)
            if abs(signed) < 1e-12:
                existing.status = "NETTED"
                self._finalize(existing, "NETTED", price, closed_by="signal")
                return None
            new_dir = "LONG" if signed > 0 else "SHORT"
            if new_dir != existing.direction:
                existing.entry_price = price
            old_key = existing.key
            del self.trades[old_key]
            existing.direction = new_dir
            existing.quantity = abs(signed)
            self.trades[existing.key] = existing
            self._rekey_paper(old_key, existing.key)
            tag = "INVERTIDA" if new_dir == direction else "REDUCIDA"
        existing.leverage = leverage
        existing.entry_order_id = str(order_id)
        existing.order_assumed = existing.order_assumed or assumed
        existing.hedge_mode = hedge
        if paper_id:
            existing.paper_trade_id = paper_id
            self._map_paper(existing, paper_id)
        log.info("[#%d] %s %s %s → qty %s @ %.8g", existing.id, tag, existing.direction, symbol,
                 existing.quantity, existing.entry_price)
        self.save()
        return existing

    def adopt_position(self, pos: Position, paper_id: int = 0, source: str = "adoptada") -> Trade:
        """Registra como trade una posición real que el executor no tenía."""
        existing = self.trades.get((pos.symbol, pos.direction))
        if existing is not None:
            if paper_id:
                self._map_paper(existing, paper_id)
            return existing
        rules = self.core.exinfo.get(pos.symbol, pos.entry_price)
        self.counter += 1
        trade = Trade(id=self.counter, symbol=pos.symbol, direction=pos.direction, entry_price=pos.entry_price,
                      quantity=pos.qty, open_time=utc_now_str(),
                      leverage=self.core.account.leverage.get(pos.symbol) or self.settings.leverage,
                      paper_trade_id=paper_id, current_price=self.core.market.price(pos.symbol) or pos.entry_price,
                      hedge_mode=pos.side != "BOTH", step_size=fmt(rules.step_size), source=source)
        self.trades[trade.key] = trade
        self._map_paper(trade, paper_id)
        self.save()
        log.info("Posición %s %s %s registrada como trade #%d (%s)", pos.symbol, pos.direction, pos.qty,
                 trade.id, source)
        return trade

    def adopt_untracked(self) -> int:
        """Tras reiniciar sin trades.json: registra las posiciones reales sin trade."""
        n = 0
        for pos in list(self.core.account.positions.values()):
            if self._trade_for_position(pos.symbol, pos.side) is not None:
                continue
            if self.grid_owner(pos.symbol, pos.direction):
                continue
            self.adopt_position(pos, source="adoptada")
            n += 1
        return n

    def _announce_open(self, trade: Trade) -> None:
        tag = " [ASUMIDA]" if trade.order_assumed else ""
        log.info("[#%d] ABIERTA %s %s @ %.8g qty=%s lev=%sx%s", trade.id, trade.direction, trade.symbol,
                 trade.entry_price, trade.quantity, trade.leverage, tag)
        self.core.bus.emit("open", f"{trade.direction} {trade.symbol} @ {trade.entry_price:.8g}{tag}",
                           "success" if not trade.order_assumed else "warning", symbol=trade.symbol)
        emoji = "🟢" if trade.direction == "LONG" else "🔴"
        word = "LONG ▲" if trade.direction == "LONG" else "SHORT ▼"
        assumed = "\n⚠️ <i>Posición asumida (margen insuficiente, -2019)</i>" if trade.order_assumed else ""
        self.core.telegram.send(
            f"{emoji} <b>POSICIÓN ABIERTA — {word}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Par:</b> <code>{trade.symbol}</code>\n"
            f"💰 <b>Entrada:</b> <code>{trade.entry_price:,.8g}</code>\n"
            f"📦 <b>Cantidad:</b> <code>{trade.quantity:g}</code>\n"
            f"💹 <b>Notional:</b> <code>{trade.notional_usdt:.2f} USDT</code>\n"
            f"⚡ <b>Leverage:</b> <code>{trade.leverage}x</code>\n"
            f"🆔 #{trade.id} | Paper #{trade.paper_trade_id}{assumed}\n"
            f"💼 Disponible: <code>{self.core.account.usdt().available:.2f} USDT</code>"
        )

    # ── Cierre ────────────────────────────────────────────────────────────
    async def _fresh_position(self, symbol: str, direction: str) -> tuple[Optional[Position], bool]:
        """Posición real del símbolo consultada AHORA por WS API.

        Devuelve (posición o None, dato_confirmado). Si la consulta falla se usa
        la caché y ``dato_confirmado`` es False.
        """
        if not self.settings.has_credentials:
            return self.core.account.position(symbol, direction), False
        asked = time.time()
        try:
            rows = await self.core.ws.positions(symbol)
            self.core.account.apply_symbol_positions(symbol, rows, as_of=asked)
            return self.core.account.position(symbol, direction), True
        except Exception as exc:
            log.warning("No se pudo releer la posición de %s (%s); se usa la caché", symbol, exc)
            return self.core.account.position(symbol, direction), False

    async def close_trade(self, trade: Trade, reason: str, close_price: float = 0.0,
                          max_qty: Optional[float] = None, bot_pnl: Optional[float] = None,
                          closed_by: str = "signal") -> bool:
        key = trade.key
        if key in self._closing or self.trades.get(key) is not trade:
            return False
        self._closing.add(key)
        try:
            async with self._lock(trade.symbol):
                if self.trades.get(key) is not trade:
                    return False
                if close_price:
                    trade.signal_close_price = close_price
                pos, known = await self._fresh_position(trade.symbol, trade.direction)
                market_px = self.core.market.price(trade.symbol)
                if pos is None and (known or trade.order_assumed):
                    if not trade.order_assumed:
                        log.info("%s %s: Binance confirma que ya no hay posición; se cierra el registro",
                                 trade.symbol, trade.direction)
                    self._finalize(trade, reason, close_price or market_px or trade.entry_price,
                                   bot_pnl=bot_pnl, closed_by=closed_by)
                    return True
                base = pos.qty if pos is not None else trade.quantity
                rules = self.core.exinfo.get(trade.symbol, market_px)
                step = float(rules.qty_step(True))
                qty = min(base, max_qty) if max_qty and max_qty > 0 else base
                partial = bool(max_qty) and qty < base - step / 2
                if not partial:
                    await self.orders.cancel_protection(trade.symbol, trade.direction)
                side = "SELL" if trade.direction == "LONG" else "BUY"
                ctx = OrderContext(trade.symbol, side, trade.direction, "close", rules, reduce_only=True)
                filled, fill_price = 0.0, 0.0
                for attempt in range(2):
                    try:
                        fill = await self.orders.market(ctx, D(fmt(qty)), prefix=f"C{trade.id}", owner=trade)
                        filled, fill_price = fill.qty, fill.avg_price
                        break
                    except BinanceAPIError as err:
                        diag = ErrorDoctor.diagnose(err)
                        if diag.action in (Action.REFRESH_POSITION, Action.ALREADY_DONE):
                            pos2, known2 = await self._fresh_position(trade.symbol, trade.direction)
                            if pos2 is None:
                                log.info("%s: la posición ya estaba cerrada en Binance (%s)", trade.symbol,
                                         diag.info.name)
                                break
                            if attempt == 0:
                                qty = min(pos2.qty, qty) if partial else pos2.qty
                                continue
                        self.core.bus.emit("close_failed", f"{trade.symbol}: {diag.info.title}", "error",
                                           symbol=trade.symbol, code=diag.code, solution=diag.info.solution)
                        self.core.telegram.send(f"⚠️ <b>No se pudo cerrar</b> <code>{trade.symbol}</code>\n"
                                                f"[{diag.code}] {diag.info.title}\n💡 {diag.info.solution}")
                        return False
                if partial:
                    trade.quantity = max(0.0, base - (filled or qty))
                    self.save()
                    self.core.bus.emit("partial_close", f"Cierre parcial {trade.symbol}: {filled or qty:g}", "info")
                    return True
                # Residuo (p. ej. un fill que llegó mientras se cerraba): una orden más.
                pos3, _ = await self._fresh_position(trade.symbol, trade.direction)
                if pos3 is not None and pos3.qty > 0:
                    log.warning("%s %s: quedó un residuo de %s tras el cierre; se envía otra orden",
                                trade.symbol, trade.direction, pos3.qty)
                    try:
                        await self.orders.market(ctx, D(fmt(pos3.qty)), prefix=f"C{trade.id}", owner=trade)
                    except BinanceAPIError as err:
                        self.orders._record(err, "close.residual", trade.symbol)
                self._finalize(trade, reason, fill_price or close_price or market_px or trade.entry_price,
                               bot_pnl=bot_pnl, closed_by=closed_by)
                return True
        finally:
            self._closing.discard(key)

    def _finalize(self, trade: Trade, reason: str, close_price: float, bot_pnl: Optional[float] = None,
                  closed_by: str = "") -> None:
        trade.status = reason
        trade.close_price = close_price
        trade.close_time = utc_now_str()
        trade.closed_ts = time.time()
        trade.pnl_usdt = trade.realized_exchange - trade.fees_usdt if trade.realized_exchange else trade.price_pnl(close_price)
        trade.roi_pct = trade.pnl_usdt / trade.notional_usdt * 100 if trade.notional_usdt else 0.0
        if bot_pnl is not None:
            trade.bot_pnl, trade.has_bot_pnl = float(bot_pnl), True
        trade.closed_by = closed_by or trade.closed_by
        self.realized_total += trade.pnl_usdt
        self._unmap_trade(trade)
        self.trades.pop(trade.key, None)
        self.closed.append(trade)
        if len(self.closed) > 1000:
            self.closed = self.closed[-500:]
        self.save()
        emoji, label = CLOSE_REASONS.get(reason, ("⚠️", reason))
        log.info("[#%d] CERRADA %s %s @ %.8g | PnL %+.4f USDT (%+.2f%%)", trade.id, reason, trade.symbol,
                 close_price, trade.pnl_usdt, trade.roi_pct)
        self.core.bus.emit("close", f"{trade.symbol} cerrada ({label}) PnL {trade.pnl_usdt:+.4f} USDT",
                           "success" if trade.pnl_usdt >= 0 else "warning", symbol=trade.symbol)
        self.core.telegram.send(self.close_message(trade))

    def close_message(self, trade: Trade) -> str:
        emoji, label = CLOSE_REASONS.get(trade.status, ("⚠️", trade.status))
        st = self.stats()
        wr = f"{st['win_rate']:.1f}% ({st['wins']}✅/{st['losses']}❌)" if st["win_rate"] is not None else "N/A"
        bot = f"\n🤖 PnL del bot: <code>{trade.bot_pnl:+.4f}</code>" if trade.has_bot_pnl else ""
        return (
            f"{emoji} <b>POSICIÓN CERRADA — {label}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Par:</b> <code>{trade.symbol}</code> {'🟢 LONG' if trade.direction == 'LONG' else '🔴 SHORT'}\n"
            f"💵 <b>Entrada:</b> <code>{trade.entry_price:,.8g}</code>\n"
            f"💵 <b>Salida:</b> <code>{trade.close_price:,.8g}</code>\n"
            f"{'💚' if trade.pnl_usdt >= 0 else '❗'} <b>PnL:</b> <code>{trade.pnl_usdt:+.4f} USDT</code> "
            f"(ROI {trade.roi_pct:+.2f}% · ROE {trade.roe_pct:+.2f}%){bot}\n"
            f"⏱ {trade.open_time} → {trade.close_time}\n"
            f"📈 <b>Win rate:</b> <code>{wr}</code>\n🆔 #{trade.id} | Paper #{trade.paper_trade_id}"
        )

    async def close_symbol(self, symbol: str, direction: Optional[str], reason: str, paper_id: int = 0,
                           bot_pnl: Optional[float] = None, closed_by: str = "signal") -> bool:
        """Cierra una posición real sin trade local (excepto si es de un grid).

        La posición se registra primero como trade (origen «externa») para que
        el cierre quede en el historial y en el PnL realizado.
        """
        if not symbol:
            return False
        symbol = symbol.upper()

        def candidates() -> list[Position]:
            return [p for p in self.core.account.positions_for(symbol)
                    if (not direction or p.direction == direction.upper())
                    and not self.grid_owner(p.symbol, p.direction)]

        positions = candidates()
        if not positions:
            await self._fresh_position(symbol, direction or "LONG")
            positions = candidates()
        if not positions:
            log.info("close %s %s: no hay posición real que cerrar", symbol, direction or "")
            return False
        ok_any = False
        for pos in positions:
            trade = self.adopt_position(pos, paper_id=paper_id, source="externa")
            ok = await self.close_trade(trade, reason, bot_pnl=bot_pnl, closed_by=closed_by)
            ok_any = ok_any or ok
        return ok_any

    async def close_all(self, reason: str, closed_by: str = "signal") -> list[Trade]:
        targets = list(self.trades.values())
        results = await asyncio.gather(*(self.close_trade(t, reason, closed_by=closed_by) for t in targets),
                                       return_exceptions=True)
        closed = [t for t, ok in zip(targets, results) if ok is True]
        # Posiciones reales sin trade local (ni grid) también se cierran.
        tracked = {t.key for t in targets}
        for pos in list(self.core.account.positions.values()):
            if (pos.symbol, pos.direction) in tracked or self.grid_owner(pos.symbol, pos.direction):
                continue
            await self.close_symbol(pos.symbol, pos.direction, reason, closed_by=closed_by)
        log.info("Cierre global: %d/%d trades cerrados", len(closed), len(targets))
        self.core.telegram.send(f"🛑 <b>Cierre global</b> ({reason}) — {len(closed)} posición(es) cerradas")
        return closed

    # ── TP / SL ───────────────────────────────────────────────────────────
    async def set_protection(self, symbol: str, direction: str, kind: str, trigger: float) -> dict:
        symbol, direction = symbol.upper(), direction.upper()
        trade = self.get_trade(symbol, direction)
        pos = self.core.account.position(symbol, direction)
        qty = pos.qty if pos else (trade.quantity if trade else 0.0)
        result = await self.orders.place_protection(symbol, direction, kind, trigger, qty=qty or None)
        if trade is not None:
            if kind.upper() == "TP":
                trade.tp_price = trigger
            else:
                trade.sl_price = trigger
            self.save()
        self.core.bus.emit("protect", f"{kind.upper()} {symbol} {direction} @ {trigger:g}", "success")
        self.core.telegram.send(f"{'🎯' if kind.upper() == 'TP' else '🛑'} <b>{kind.upper()} configurado</b>\n"
                                f"<code>{symbol}</code> {direction} @ {trigger:g}")
        return result

    # ── Eventos de Binance ────────────────────────────────────────────────
    def on_order_update(self, upd: OrderUpdate) -> None:
        owner = self.orders.client_owner.get(upd.client_id)
        if upd.is_fill:
            if upd.realized_pnl or upd.reduce_only or upd.orig_type in ("TAKE_PROFIT_MARKET", "STOP_MARKET",
                                                                         "TAKE_PROFIT", "STOP", "LIQUIDATION"):
                self._last_close_fill[upd.symbol] = upd
            if isinstance(owner, dict):
                owner_trade = self.trades.get((owner.get("symbol"), owner.get("direction")))
                if owner.get("kind") == "limit_entry":
                    # Orden LIMIT manual de entrada: cada fill amplía/crea el trade.
                    rules = self.core.exinfo.get(upd.symbol, upd.last_price)
                    owner_trade = self._register_entry(
                        upd.symbol, owner["direction"], upd.last_price, upd.last_qty, int(owner.get("leverage") or 1),
                        0, upd.order_id, upd.position_side in ("LONG", "SHORT"), rules.step_size, source="manual")
                    if owner_trade is not None:
                        owner_trade.last_fill_ts = time.time()
                        if upd.status == "FILLED":
                            self._announce_open(owner_trade)
                if owner_trade is not None:
                    owner_trade.fees_usdt += upd.fee_usdt
                return
            if isinstance(owner, Trade):
                owner.fees_usdt += upd.fee_usdt
                owner.realized_exchange += upd.realized_pnl
                if owner.status != "OPEN" and upd.status == "FILLED" and owner.realized_exchange:
                    before = owner.pnl_usdt
                    owner.pnl_usdt = owner.realized_exchange - owner.fees_usdt
                    owner.roi_pct = owner.pnl_usdt / owner.notional_usdt * 100 if owner.notional_usdt else 0.0
                    self.realized_total += owner.pnl_usdt - before
                    self.save()
            else:
                trade = self._trade_for_position(upd.symbol, upd.position_side)
                if trade is not None and owner is None:
                    trade.fees_usdt += upd.fee_usdt
                    trade.realized_exchange += upd.realized_pnl

    def _trade_for_position(self, symbol: str, position_side: str) -> Optional[Trade]:
        if position_side in ("LONG", "SHORT"):
            return self.trades.get((symbol, position_side))
        return next((t for (s, _), t in self.trades.items() if s == symbol), None)

    def on_positions_changed(self, keys: list[tuple[str, str]]) -> None:
        for symbol, side in keys:
            trade = self._trade_for_position(symbol, side)
            if trade is None or trade.key in self._closing or trade.order_assumed:
                continue
            pos = self.core.account.position(symbol, trade.direction)
            if pos is None:
                self._schedule_external_check(trade)
            elif abs(pos.qty - trade.quantity) > 1e-12 and not self._lock(symbol).locked():
                trade.quantity = pos.qty
                if pos.entry_price:
                    trade.entry_price = pos.entry_price
                self.save()

    def _schedule_external_check(self, trade: Trade) -> None:
        if trade.key not in self._pending_external:
            self._pending_external.add(trade.key)
            asyncio.create_task(self._confirm_external_close(trade))

    async def _confirm_external_close(self, trade: Trade) -> None:
        try:
            await asyncio.sleep(1.5)
            if trade.key in self._closing or self.trades.get(trade.key) is not trade:
                return
            if self._lock(trade.symbol).locked() or self.inflight(trade.symbol):
                return
            pos, known = await self._fresh_position(trade.symbol, trade.direction)
            if pos is not None:
                return
            fill = self._last_close_fill.get(trade.symbol)
            reason, price = "EXTERNAL", self.core.market.price(trade.symbol) or trade.entry_price
            if fill is not None and time.time() * 1000 - fill.trade_time < 120_000:
                price = fill.avg_price or fill.last_price or price
                cid = fill.client_id.lower()
                if fill.orig_type in ("TAKE_PROFIT_MARKET", "TAKE_PROFIT"):
                    reason = "TP"
                elif fill.orig_type in ("STOP_MARKET", "STOP"):
                    reason = "SL"
                elif fill.orig_type == "LIQUIDATION" or cid.startswith("autoclose"):
                    reason = "LIQUIDATION"
                elif cid.startswith("adl_autoclose"):
                    reason = "ADL"
            log.warning("%s %s cerrada fuera del executor (%s)", trade.symbol, trade.direction, reason)
            self._finalize(trade, reason, price, closed_by="binance")
        finally:
            self._pending_external.discard(trade.key)

    def reconcile_after_sync(self, as_of: Optional[float] = None) -> None:
        """Tras leer posiciones reales: verifica lo que ya no existe.

        No cierra directamente: programa una verificación por símbolo (WS API)
        y omite las operaciones que se movieron después de pedir el snapshot.
        """
        for trade in list(self.trades.values()):
            if trade.order_assumed or trade.key in self._closing:
                continue
            # Solo se omiten fills de ESTA sesión posteriores a pedir el snapshot
            # (los trades cargados del disco son anteriores al arranque).
            if as_of is not None and trade.last_fill_ts >= max(as_of - 1, self._session_start):
                continue
            if self._lock(trade.symbol).locked() or self.inflight(trade.symbol):
                continue
            pos = self.core.account.position(trade.symbol, trade.direction)
            if pos is None:
                self._schedule_external_check(trade)
            elif abs(pos.qty - trade.quantity) > 1e-12:
                trade.quantity = pos.qty
                trade.entry_price = pos.entry_price or trade.entry_price

    # ── Acciones manuales ─────────────────────────────────────────────────
    def set_trading(self, enabled: Any) -> bool:
        self.trading_enabled = parse_bool(enabled, self.trading_enabled)
        self.save()
        return self.trading_enabled

    def clear_history(self) -> int:
        n = len(self.closed)
        self.closed.clear()
        self.realized_total = 0.0
        started = self.status["started_at"]
        self.status = _fresh_status()
        self.status["started_at"] = started
        self.status["last_signal_time"] = "Historial borrado"
        self.signal_log.clear()
        self.save()
        return n
