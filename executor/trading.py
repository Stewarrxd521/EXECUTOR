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
import re
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
CLOSE_WAIT_S = 60.0          # un cierre espera a que termine otro cierre del mismo trade (no se descarta)
CLOSE_RETRY_DELAYS = (0.0, 3.0, 10.0)  # reintentos de un close tras un fallo transitorio
LOT_CLOSE_ORDERS = 6         # órdenes por cierre de lote mientras Binance llene en parte
LOT_NOTE = "lote"  # notes de un registro que es solo el lote de un trade_id (la posición siguió abierta)

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

_BOT_RE = re.compile(r"[a-z0-9_.-]{1,32}")


def normalize_bot(value: Any) -> str:
    """Nombre del bot que envía la señal (campo ``bot`` o ruta ``/b/<bot>/signal``)."""
    bot = str(value or "").strip().lower()
    return bot if _BOT_RE.fullmatch(bot) else ""


def _split_ref(ref: Any) -> tuple[int, str]:
    """(trade_id, bot) de un lote."""
    text = str(ref or "0")
    bot, _, tid = text.rpartition(":")
    try:
        return int(float(tid or 0)), bot
    except (TypeError, ValueError, OverflowError):
        return 0, bot


def lot_ref(bot: str, trade_id: Any) -> Any:
    """Identificador del lote: el trade_id (int) o «bot:trade_id» si el bot se identifica."""
    try:
        tid = int(float(trade_id or 0))
    except (TypeError, ValueError, OverflowError):
        tid = 0
    return f"{bot}:{tid}" if bot and tid else tid


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
    # Multiplicador: cantidad que pidió el bot y factor medio aplicado (real ≈ bot × factor).
    bot_quantity: float = 0.0
    multiplier: float = 1.0
    # Lote de cada trade_id ("5" o "bot:5"): cantidad real que abrió y aún no cerró el executor.
    # "0" = sin trade_id (manual/API). Lo que la posición tenga de más (adoptada, abierta fuera) no
    # es de ningún lote. Las aperturas asumidas (-2019, sin orden real) van aparte.
    lots: dict = field(default_factory=dict)
    assumed_lots: dict = field(default_factory=dict)
    bot: str = ""
    # PnL propio de un lote: Binance calcula el realizado con la entrada media de la posición; el
    # ajuste lo lleva a la entrada del lote (y el resto de la posición recibe el ajuste contrario).
    pnl_adjust: float = 0.0
    ref_entry: float = 0.0
    # True desde que la posición tiene lotes: lo que quede sin lote ya no es «adoptado» (es de otro bot).
    lot_tracked: bool = False
    # TP/SL colocado desde la señal de un trade_id: su fill se descuenta primero de ese lote.
    protect_lots: dict = field(default_factory=dict)

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
        data["lots_view"] = self.lots_view()
        mult = self.multiplier or 1.0
        data["bot_pnl_scaled"] = self.bot_pnl * mult if self.has_bot_pnl else None
        data["pnl_diff"] = (self.pnl_usdt - self.bot_pnl * mult) if self.has_bot_pnl else None
        return data

    @staticmethod
    def lot_key(ref: Any) -> str:
        if isinstance(ref, str) and ":" in ref:
            return ref
        return str(lot_ref("", ref))

    def add_lot(self, ref: Any, qty: float, assumed: bool = False) -> None:
        lots = self.assumed_lots if assumed else self.lots
        key = self.lot_key(ref)
        lots[key] = float(D(fmt(lots.get(key, 0.0) + qty)))
        self.lot_tracked = True

    def reduce_lots(self, qty: float, first: str = "", newer: Optional[dict] = None) -> list[str]:
        """Descuenta ``qty`` que se cerró fuera del executor: primero del lote ``first`` (el trade_id
        que puso ese TP/SL), luego de todos en proporción, sin contar lo que se abrió DESPUÉS de ese
        fill (``newer``: cantidad de cada lote llenada más tarde). Devuelve los lotes que quedaron a cero."""
        emptied = []
        newer = newer or {}
        if first and first in self.lots:
            take = min(qty, max(0.0, self.lots[first] - newer.get(first, 0.0)))
            self.lots[first] -= take
            qty -= take
        weights = {k: max(0.0, v - newer.get(k, 0.0)) for k, v in self.lots.items()}
        total = sum(weights.values())
        pool = max(total, self.real_quantity - sum(newer.values()))  # incluye lo que no es de ningún lote
        if qty > 0 and total > 0 and pool > 0:
            share = min(1.0, qty / pool)
            self.lots = {k: v - weights[k] * share for k, v in self.lots.items()}
        for k, v in list(self.lots.items()):
            if v <= 1e-12:
                del self.lots[k]
                emptied.append(k)
            else:
                self.lots[k] = float(D(fmt(v)))
        return emptied

    @property
    def real_quantity(self) -> float:
        """Cantidad con orden real (sin las aperturas asumidas)."""
        return self.quantity if not self.order_assumed else 0.0

    def effective_lots(self, real_qty: Optional[float] = None) -> dict[str, float]:
        """Cantidad de cada lote frente a la posición real: si Binance tiene menos de lo registrado
        (TP/SL o cierres hechos fuera del executor), cada lote baja en proporción."""
        real = self.real_quantity if real_qty is None else real_qty
        total = sum(self.lots.values())
        factor = min(1.0, real / total) if total > 0 else 0.0
        return {k: v * factor for k, v in self.lots.items() if v > 0}

    def fit_lots(self, real_qty: Optional[float] = None) -> None:
        """Fija en los lotes una reducción ya confirmada (cada lote en proporción)."""
        real = self.real_quantity if real_qty is None else real_qty
        if self.lots and real < sum(self.lots.values()) - 1e-12:
            self.lots = {k: float(D(fmt(v))) for k, v in self.effective_lots(real).items()}

    def unassigned_qty(self, real_qty: Optional[float] = None) -> float:
        """Cantidad de la posición que no es de ningún lote (adoptada o abierta fuera del executor)."""
        real = self.real_quantity if real_qty is None else real_qty
        return max(0.0, real - sum(self.lots.values()))

    def lot_tranches(self, ref: Any) -> list[dict]:
        key = self.lot_key(ref)
        return [t for t in self.tranches
                if self.lot_key(lot_ref(t.get("bot", ""), t.get("trade_id", 0))) == key and "trade_id" in t]

    def lot_multiplier(self, ref: Any) -> float:
        """Factor del multiplicador que recibió ese trade_id (cantidad real / cantidad del bot)."""
        rows = [t for t in self.lot_tranches(ref) if not t.get("assumed")]
        real = sum(safe_float(t.get("qty")) for t in rows)
        bot = sum(safe_float(t.get("bot_qty")) for t in rows)
        return real / bot if real > 0 and bot > 0 else (self.multiplier or 1.0)

    def lot_entry(self, ref: Any) -> float:
        """Precio medio de entrada de ese trade_id (sus fills); sin datos, el de la posición."""
        rows = [t for t in self.lot_tranches(ref) if not t.get("assumed") and safe_float(t.get("fill_price")) > 0]
        qty = sum(safe_float(t.get("qty")) for t in rows)
        return sum(safe_float(t["fill_price"]) * safe_float(t.get("qty")) for t in rows) / qty if qty > 0 \
            else self.entry_price

    def lots_view(self) -> list[list]:
        rows = [[k, v] for k, v in self.effective_lots().items()]
        rows += [[k, v] for k, v in self.assumed_lots.items() if v > 0 and k not in self.lots]
        rest = self.unassigned_qty()
        if rest > 0 and self.lots:
            rows.append([None, rest])

        def order(r):
            tid = str(r[0]).rsplit(":", 1)[-1]
            return (r[0] is None, int(tid) if tid.isdigit() else 0, str(r[0]))
        return sorted(rows, key=order)

    def booked_pnl(self, close_price: float) -> float:
        """PnL de un trade cerrado: el realizado de Binance (o el de precio si aún no llegó) menos
        comisiones, más el ajuste a la entrada propia del lote."""
        if self.realized_exchange:
            base = self.realized_exchange
        else:
            entry = self.ref_entry or self.entry_price
            base = (close_price - entry) * self.quantity * (1 if self.direction == "LONG" else -1)
        return base - self.fees_usdt + self.pnl_adjust

    def sync_quantity(self, real_qty: float) -> None:
        """Ajusta la cantidad a la real conservando la proporción bot/real.

        Proporcional y reversible: una lectura atrasada que muestra menos cantidad y
        la lectura correcta posterior dejan ``bot_quantity`` como estaba.
        """
        if self.quantity > 0 and self.bot_quantity > 0 and real_qty >= 0:
            self.bot_quantity = self.bot_quantity * real_qty / self.quantity
        self.quantity = real_qty

    def note_tranche_scale(self, factor: float, bot_qty: float, real_qty: float) -> None:
        """Acumula la cantidad pedida por el bot y recalcula el factor medio del trade."""
        factor = factor or 1.0
        bot_qty = bot_qty if bot_qty > 0 else (real_qty / factor if factor else real_qty)
        prev_bot = self.bot_quantity
        self.bot_quantity = prev_bot + bot_qty
        if self.bot_quantity > 0:
            self.multiplier = round((prev_bot * (self.multiplier or 1.0) + bot_qty * factor) / self.bot_quantity, 6)

    @classmethod
    def from_dict(cls, data: dict) -> "Trade":
        names = set(cls.__dataclass_fields__)
        trade = cls(**{k: v for k, v in data.items() if k in names})
        if "bot_quantity" not in data:  # guardado antes del multiplicador: todo cuenta como x1
            trade.bot_quantity = trade.quantity / (trade.multiplier or 1.0)
        if trade.lots or trade.assumed_lots:
            trade.lot_tracked = True
        return trade


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
    bot: str = ""

    @property
    def ref(self) -> Any:
        """Lote de la señal: su trade_id, o «bot:trade_id» si el bot se identifica."""
        return lot_ref(self.bot, self.trade_id)


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
        self.multiplier = None  # Multiplier (lo asigna el servicio)
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
        self._open_alerts: dict[str, float] = {}
        self._close_retryable: dict[tuple[str, str], bool] = {}
        self._no_lot: set[tuple[str, str]] = set()  # el último close_trade no encontró lote del trade_id
        self._lot_attempts: dict[tuple, dict] = {}   # cierre de lote sin respuesta: se verifica antes de reenviar
        self._symbol_retryable: dict[str, bool] = {}  # close_symbol falló por un error transitorio
        self._settled: dict[str, dict] = {}           # signal_id -> lo que ya ejecutó su orden sin respuesta
        self._reconciling: set[tuple[str, str]] = set()
        self._skipped_closed: dict[str, float] = {}
        self._recent_closed: dict[tuple[int, str, str], float] = {}
        # Orden por símbolo: un close espera a los open aceptados ANTES que él, y un
        # open espera a los close aceptados antes que él (nunca a los posteriores).
        self._open_tasks: dict[str, list[tuple[str, asyncio.Task]]] = {}
        self._close_tasks: dict[str, list[tuple[str, asyncio.Task]]] = {}
        self._close_all_tasks: set[asyncio.Task] = set()
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
            if "lots" not in raw:
                self._migrate_lots(t)
        self.closed = [Trade.from_dict(raw) for raw in data.get("closed", [])]
        self.realized_total = float(data.get("realized_total", sum(t.pnl_usdt for t in self.closed)))
        if self.trades or self.closed:
            log.info("Estado restaurado: %d abiertas, %d cerradas", len(self.trades), len(self.closed))

    def save(self) -> None:
        self.store.schedule()

    @staticmethod
    def _migrate_lots(trade: Trade) -> None:
        """Trade guardado antes de los lotes por trade_id: no se sabe cuánto abrió cada uno, así
        que la cantidad se reparte a partes iguales entre sus trade_id."""
        ids = [int(p) for p in dict.fromkeys(trade.paper_ids or []) if p] or \
              ([trade.paper_trade_id] if trade.paper_trade_id else [])
        if not ids:
            return  # sin trade_id: toda la posición queda sin lote (como una adoptada)
        share = trade.quantity / len(ids)
        trade.lots = {Trade.lot_key(pid): share for pid in ids}
        trade.lot_tracked = True
        if len(ids) > 1:
            log.warning("%s %s: posición abierta antes de los lotes por trade_id; %s se reparte a partes "
                        "iguales entre los trade_id %s", trade.symbol, trade.direction, trade.quantity,
                        ", ".join(map(str, ids)))

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
            if sig.bot:
                entry["bot"] = sig.bot
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

    def _alert_open_failed(self, sig: Signal, why: str) -> None:
        """Aviso visible (dashboard + Telegram) cuando una apertura del bot NO se ejecuta."""
        key = f"{sig.symbol}|{why[:40]}"
        now = time.time()
        if now - self._open_alerts.get(key, 0) < 60:
            return
        self._open_alerts[key] = now
        if len(self._open_alerts) > 256:
            self._open_alerts = {k: v for k, v in self._open_alerts.items() if now - v < 60}
        tid = f" trade_id {sig.trade_id}" if sig.trade_id else ""
        self.core.bus.emit("signal_failed", f"Apertura NO ejecutada: {sig.direction} {sig.symbol}{tid} — {why}",
                           "error", symbol=sig.symbol)
        self.core.telegram.send(f"⚠️ <b>Señal OPEN no ejecutada</b> {sig.direction} <code>{sig.symbol}</code>{tid}\n"
                                f"{why}")

    def _reject(self, sig: Signal, error: str, status: int = 400, **extra) -> tuple[int, dict, None]:
        self.status["signals_rejected"] += 1
        self._log_signal(sig.action, sig.symbol, sig.direction, False, error, sig)
        if sig.action == "open" and status != 503:
            self._alert_open_failed(sig, error)
        if sig.action == "open" and sig.trade_id and status != 503:  # 503 = reintentable, no es definitivo
            self._mark_rejected(sig.ref, sig.symbol)
        log.warning("Señal %s %s %s rechazada: %s", sig.action.upper(), sig.symbol, sig.direction, error)
        return status, {"ok": False, "error": error, "signal_id": sig.signal_id, **extra}, None

    def _mark_rejected(self, ref: Any, symbol: str) -> None:
        key = Trade.lot_key(ref)
        if any(key in t.lots or key in t.assumed_lots for (s, _), t in self.trades.items() if s == symbol):
            return  # ese trade_id ya tiene posición abierta (otro tramo sí entró)
        if any(s == symbol for s, _ in self._reconciling):
            return  # otro tramo de estado desconocido aún se verifica
        self._rejected_ids[(ref, symbol)] = time.time()
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
        sig = Signal(action, symbol, direction, trade_id, data, explicit_id or secrets.token_hex(6), source, ip,
                     bot=normalize_bot(data.get("bot") or data.get("bot_id")))

        self.status["signals_received"] += 1
        self.status["last_signal_time"] = time.strftime("%H:%M:%S UTC", time.gmtime())
        self.status["last_signal_detail"] = f"{action.upper()} {symbol} {direction}".strip()

        if not self.accepting:
            return self._reject(sig, "executor reiniciándose: reintenta en unos segundos", 503, retry=True)

        # El antiduplicado por contenido NUNCA aplica a «open»: los tramos de un mismo
        # trade_id pueden llegar idénticos y en el mismo segundo (app_25 al promediar).
        content_dedupe = self.settings.signal_dedupe_ttl_s > 0 and action != "open"
        if explicit_id or content_dedupe:
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
        prior_closes += [t for t in self._close_all_tasks if not t.done()]  # un close_all anterior va primero
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
        factor = 1.0
        bot_quantity = quantity
        already_closed: Optional[float] = None
        try:
            await self._await_ready()
            await self._wait_tasks(prior_closes or [])
            already_closed = self._closed_recently(sig.ref, symbol, direction)
            if already_closed is None:
                # Multiplicador (manual o automático por balance): escala el tamaño pedido.
                factor = self.multiplier.effective() if self.multiplier is not None else 1.0
                if quantity <= 0 and notional <= 0 and margin <= 0:
                    notional = self.settings.default_notional_usdt  # señal sin tamaño (app.py): también se escala
                if factor != 1.0:
                    quantity, notional, margin = quantity * factor, notional * factor, margin * factor
                # Tamaño como antes: quantity × price; notional/margin solo si no hay quantity.
                if quantity > 0 and price > 0:
                    q_notional = quantity * price
                    if notional > 0 and abs(q_notional - notional) / max(notional, q_notional) > 0.10:
                        note = f"notional {notional:g} ≠ quantity×price {q_notional:.4g}: se usa quantity"
                    notional = 0.0
                if margin > 0 and notional <= 0 and quantity <= 0:
                    ref = price or await self.reference_price(symbol) or 1.0
                    notional = margin * (lev or self.leverage_for_price(ref))
                self._unknown_open.pop((symbol, direction), None)
                before = self.core.account.position(symbol, direction)
                baseline = before.qty if before is not None else 0.0
                trade = await self.open_trade(symbol, direction, signal_price=price, quantity=quantity,
                                              notional=notional, paper_trade_id=sig.trade_id, source="signal",
                                              bot=sig.bot,
                                              leverage=lev, level=level, signal_id=sig.signal_id,
                                              multiplier=factor, bot_quantity=bot_quantity)
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
                self._spawn(self._reconcile_unknown_open(symbol, direction, sig.trade_id, baseline, factor,
                                                         bot_quantity, bot=sig.bot))
                entry = self._log_signal("open", symbol, direction, False,
                                         "estado desconocido: se verifica la posición real en Binance", sig)
            else:
                if sig.trade_id:
                    self._mark_rejected(sig.ref, symbol)
                entry = self._log_signal("open", symbol, direction, False, "no ejecutada (ver pestaña Errores)", sig)
                self._alert_open_failed(sig, "Binance la rechazó (ver pestaña Errores)")
            return {"ok": False, "detail": entry["detail"]}

        self.status["signals_open"] += 1
        self._rejected_ids.pop((sig.ref, symbol), None)
        # ¿Llegó antes un close para esta apertura? Se marca YA (sin esperas de por medio)
        # para que un tramo posterior del mismo trade_id no reabra mientras se cierra.
        pending = self._take_pending_close(sig.ref, symbol, direction)
        if pending is not None:
            self._note_closed(sig.ref, symbol, direction)
        detail = f"#{trade.id} qty={trade.quantity:g} @ {trade.entry_price:g}" + (" (ASUMIDA)" if trade.order_assumed else "")
        if factor != 1.0:
            detail += f" — multiplicador x{factor:g}"
        if note:
            detail += f" — {note}"
        protections = []
        for kind, aliases in (("TP", _TP_ALIASES), ("SL", _SL_ALIASES)):
            trig = safe_float(_first(d, aliases)) if pending is None else 0.0
            if trig > 0:
                try:
                    await self.set_protection(symbol, trade.direction, kind, trig, lot=sig.ref)
                    protections.append(kind)
                    detail += self._shared_protection_note(trade, sig, kind)
                except BinanceAPIError as err:
                    detail += f" — {kind} no colocado: {ErrorDoctor.diagnose(err).info.title}"
        if protections:
            detail += " — " + "/".join(protections) + " colocado(s)"
        self._log_signal("open", symbol, direction, True, detail, sig, executor_trade_id=trade.id)

        if pending is not None and self.trades.get(trade.key) is trade:
            log.warning("Cierre diferido aplicado a %s %s (llegó antes que la apertura)", symbol, direction)
            ok = await self._deferred_close(symbol, direction, sig.trade_id, sig.bot, pending, sig.signal_id)
            if not ok:
                self._recent_closed.pop((sig.ref, symbol, direction), None)
        return {"ok": True, "detail": detail, "trade": trade.to_dict()}

    async def _deferred_close(self, symbol: str, direction: str, trade_id: int, bot: str, pending: dict,
                              origin: str) -> bool:
        """Aplica un close que llegó antes que su apertura, con los mismos reintentos que un close normal."""
        sig = Signal("close", symbol, direction, trade_id, {"reason": pending["reason"]}, f"{origin}:diferido"[:100],
                     "diferido", bot=bot)
        result = await self._signal_close(sig, pending["reason"], pending["close_price"], 0.0, pending["bot_pnl"])
        return bool(result.get("ok"))

    async def _reconcile_unknown_open(self, symbol: str, direction: str, paper_id: int, baseline: float,
                                      factor: float = 1.0, bot_quantity: float = 0.0, bot: str = "") -> None:
        """Tras una apertura de estado desconocido, registra lo que Binance sí abrió.

        Solo se atribuye a la señal la cantidad que supera lo que ya había
        (``baseline``) y lo que ya es de otros lotes: nunca una posición ajena.
        """
        ref = lot_ref(bot, paper_id)
        try:
            for delay in UNKNOWN_OPEN_CHECKS:
                await asyncio.sleep(delay)
                pending = None
                async with self._lock(symbol):
                    pos, confirmed = await self._fresh_position(symbol, direction)
                    trade = self.trades.get((symbol, direction))
                    # Lo ya asignado a lotes (no trade.quantity: una sincronización pudo sumar ya el fill).
                    tracked = sum(trade.lots.values()) if trade is not None and not trade.order_assumed else 0.0
                    real = pos.qty if pos is not None else 0.0
                    tol = float(self.core.exinfo.get(symbol, pos.entry_price if pos else 0.0).step_size) / 2
                    if pos is None or real - max(baseline, tracked) <= tol:
                        if confirmed and delay == UNKNOWN_OPEN_CHECKS[-1]:
                            log.info("%s %s: la apertura de estado desconocido no se ejecutó", direction, symbol)
                        continue
                    if self.grid_owner(symbol, direction):
                        return
                    added = real - max(baseline, tracked)
                    # Precio de ese fill: lo que movió la entrada media de Binance (o la entrada si no había).
                    prev_qty, prev_entry = real - added, (trade.entry_price if trade is not None else 0.0)
                    fill_px = (pos.entry_price * real - prev_entry * prev_qty) / added \
                        if trade is not None and prev_entry and added > 0 else pos.entry_price
                    if fill_px <= 0:
                        fill_px = pos.entry_price
                    if trade is None:
                        trade = self.adopt_position(pos, paper_id=paper_id, source="signal")
                        trade.bot_quantity, trade.multiplier = 0.0, 1.0
                        trade.note_tranche_scale(factor, bot_quantity, pos.qty)
                        trade.add_lot(ref, added)  # lo que ya había antes no es de esta señal
                        self._announce_open(trade)
                    else:
                        trade.note_tranche_scale(factor, bot_quantity, added)
                        if trade.order_assumed:
                            trade.order_assumed = False  # ahora hay posición real
                        trade.quantity = pos.qty
                        trade.entry_price = pos.entry_price or trade.entry_price
                        trade.add_lot(ref, added)
                        if paper_id:
                            self._map_paper(trade, paper_id)
                    tranche = {"level": 0.0, "trade_id": paper_id, "qty": added, "fill_price": fill_px,
                               "bot_qty": bot_quantity if bot_quantity > 0 else added / (factor or 1.0),
                               "multiplier": factor, "ts": time.time(), "reconciled": True,
                               "fill_ms": self.core.clock.now_ms()}
                    if bot:
                        tranche["bot"] = bot
                    trade.tranches.append(tranche)
                    self.save()
                    log.warning("%s %s: la apertura de estado desconocido SÍ se ejecutó (posición %s); registrada",
                                direction, symbol, pos.qty)
                    self._rejected_ids.pop((ref, symbol), None)
                    pending = self._take_pending_close(ref, symbol, direction) if paper_id else None
                    if pending is not None:
                        self._note_closed(ref, symbol, direction)
                if pending is not None and self.trades.get(trade.key) is trade:
                    await self._deferred_close(symbol, direction, paper_id, bot, pending, f"reconcile-{paper_id}")
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

    def _resolve_trade(self, trade_id: int, symbol: str, direction: Optional[str], ref: Any = None) -> Optional[Trade]:
        if trade_id:
            # Primero la posición que tiene el lote de ese trade_id (y de ese bot): el número solo
            # puede repetirse entre bots.
            key = Trade.lot_key(ref if ref else trade_id)
            holders = [t for t in self.trades.values() if (key in t.lots or key in t.assumed_lots)
                       and (not symbol or t.symbol == symbol) and (not direction or t.direction == direction)]
            if len(holders) == 1:
                return holders[0]
        trade = self.find_by_paper_id(trade_id, symbol or None, direction) if trade_id else None
        if trade is None and symbol:
            trade = self.get_trade(symbol, direction)
        return trade

    async def _signal_close(self, sig: Signal, reason: str, close_price: float, quantity: float,
                            bot_pnl: Optional[float], prior_opens: Optional[list] = None) -> dict:
        """Cierre por señal. Un error inesperado (p. ej. sin conexión) se reintenta: un
        cierre perdido deja la posición abierta."""
        delays = CLOSE_RETRY_DELAYS
        for attempt, delay in enumerate(delays):
            if delay:
                await asyncio.sleep(delay)
            final = attempt == len(delays) - 1
            try:
                result = await self._signal_close_once(sig, reason, close_price, quantity, bot_pnl,
                                                       prior_opens if attempt == 0 else None, final=final)
            except Exception as exc:
                log.exception("Cierre %s %s (trade_id %s) falló (intento %d)", sig.symbol, sig.direction,
                              sig.trade_id, attempt + 1)
                last = f"{type(exc).__name__}: {exc}"
                continue
            if result.get("retryable"):  # sin conexión / estado desconocido: se relee y se reintenta
                log.warning("Cierre %s %s (trade_id %s): fallo transitorio, reintento %d", sig.symbol,
                            sig.direction, sig.trade_id, attempt + 1)
                last = result.get("detail", "fallo transitorio")
                continue
            return result  # un rechazo definitivo de Binance ya se avisa en close_trade (close_failed + Telegram)
        self.status["signals_rejected"] += 1
        self._log_signal("close", sig.symbol, sig.direction, False, f"no se pudo cerrar: {last}", sig)
        self._alert_close_failed(sig, last)
        return {"ok": False, "detail": last}

    def _alert_close_failed(self, sig: Signal, why: str) -> None:
        tid = f" trade_id {sig.trade_id}" if sig.trade_id else ""
        self.core.bus.emit("signal_failed", f"Cierre NO ejecutado: {sig.direction} {sig.symbol}{tid} — {why}. "
                                            "Revisa la posición", "error", symbol=sig.symbol)
        self.core.telegram.send(f"🚨 <b>Señal CLOSE no ejecutada</b> {sig.direction} <code>{sig.symbol}</code>{tid}\n"
                                f"{why}\nRevisa la posición en Binance")

    async def _signal_close_once(self, sig: Signal, reason: str, close_price: float, quantity: float,
                                 bot_pnl: Optional[float], prior_opens: Optional[list] = None,
                                 final: bool = True) -> dict:
        symbol, direction = sig.symbol, sig.direction or None
        await self._await_ready()
        await self._wait_tasks(prior_opens or [])  # solo las aperturas aceptadas antes que este close
        if symbol:
            async with self._lock(symbol):
                pass  # hace cola detrás de quien tenga el lock del símbolo
        trade = self._resolve_trade(sig.trade_id, symbol, direction, sig.ref)
        detail = ""
        ref = sig.ref
        if trade is not None:
            symbol = symbol or trade.symbol
            lk = Trade.lot_key(ref)
            owns = lk in trade.lots or lk in trade.assumed_lots
            if sig.trade_id and not owns and (ref, trade.symbol) in self._rejected_ids:
                ok = False
                detail = f"cierre ignorado: el trade_id {sig.trade_id} nunca se abrió en el executor"
            elif sig.trade_id and not owns and not quantity and \
                    self._closed_recently(ref, trade.symbol, trade.direction) is not None:
                ok, detail = True, f"el trade_id {sig.trade_id} ya estaba cerrado"  # close repetido: no se toca nada
            else:
                if quantity:
                    # Cierre parcial: la cantidad del bot se escala con el factor que recibió ese trade_id.
                    quantity *= trade.lot_multiplier(ref) if sig.trade_id else (trade.multiplier or 1.0)
                self._close_retryable.pop(trade.key, None)
                self._no_lot.discard(trade.key)
                # Con trade_id solo se cierra lo de ese trade_id; sin trade_id, la posición entera.
                ok = await self.close_trade(trade, reason, close_price=close_price, max_qty=quantity or None,
                                            bot_pnl=bot_pnl, closed_by="signal", alert=final, lot=ref,
                                            signal_id=sig.signal_id)
                if not ok and not final and self._close_retryable.pop(trade.key, False):
                    return {"ok": False, "retryable": True, "detail": "fallo transitorio de conexión con Binance"}
                detail = "cerrada" if ok else "error al cerrar (ver pestaña Errores)"
                if ok and sig.trade_id and self.trades.get(trade.key) is trade:
                    detail = f"cerrado el trade_id {sig.trade_id}; la posición sigue abierta con {trade.quantity:g}"
                if not ok and trade.key in self._no_lot:
                    self._no_lot.discard(trade.key)
                    if self._closed_recently(ref, trade.symbol, trade.direction) is not None:
                        ok, detail = True, f"el trade_id {sig.trade_id} ya estaba cerrado"
                    else:
                        # Su apertura puede llegar tarde: el cierre queda en espera (como sin posición).
                        self._remember_pending_close(ref, trade.symbol, sig.direction or None, reason,
                                                     close_price, bot_pnl)
                        detail = (f"la posición {trade.symbol} {trade.direction} no tiene cantidad del trade_id "
                                  f"{sig.trade_id} (es de otros trade_id): no se toca; el cierre queda en espera "
                                  f"{PENDING_CLOSE_TTL_S:.0f}s por si su apertura llega tarde")
                elif not ok and trade.status != "OPEN":
                    ok, detail = True, "la posición ya se cerró (otro cierre total llegó antes)"
                if ok and not quantity and sig.trade_id:
                    self._note_closed(ref, trade.symbol, trade.direction)
        elif (sig.trade_id and (ref, symbol) in self._rejected_ids
              and not await self._untracked_position(symbol, direction)):
            ok = False
            detail = f"cierre ignorado: el trade_id {sig.trade_id} nunca se abrió en el executor"
        else:
            self._symbol_retryable.pop(symbol, None)
            ok = await self.close_symbol(symbol, direction, reason, paper_id=sig.trade_id, bot_pnl=bot_pnl,
                                         closed_by="signal", lot=ref) if symbol else False
            if not ok and not final and self._symbol_retryable.pop(symbol, False):
                return {"ok": False, "retryable": True, "detail": "fallo transitorio de conexión con Binance"}
            if ok:
                detail = "cerrada (posición sin trade local)"
                if not quantity:
                    self._note_closed(ref, symbol, direction)
            elif sig.trade_id:
                self._remember_pending_close(ref, symbol, direction, reason, close_price, bot_pnl)
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
        prior_opens = self._pending_tasks(self._open_tasks, "", None)  # todas las aperturas ya aceptadas
        task = self._spawn(self._signal_close_all(sig, prior_opens))
        self._close_all_tasks.add(task)
        task.add_done_callback(self._close_all_tasks.discard)
        return 200, {"ok": True, "action": "close_all", "positions_targeted": total}, task

    async def _signal_close_all(self, sig: Signal, prior_opens: Optional[list] = None) -> dict:
        await self._await_ready()
        await self._wait_tasks(prior_opens or [])  # no espera (ni cierra) aperturas recibidas después
        closed = await self.close_all("CLOSE_ALL")
        self.status["signals_close"] += len(closed)
        detail = f"{len(closed)} posición(es) cerradas"
        self._log_signal("close_all", "", "", True, detail, sig)
        return {"ok": True, "detail": detail, "closed": len(closed)}

    def _accept_protect(self, sig: Signal):
        trigger = safe_float(sig.data.get("trigger_price")) or safe_float(sig.data.get("price"))
        kind = "TP" if sig.action == "open_tp" else "SL"
        trade = self._resolve_trade(sig.trade_id, sig.symbol, sig.direction or None, sig.ref) if sig.symbol else None
        pos = self.core.account.position(sig.symbol, sig.direction or None) if sig.symbol else None
        if trigger <= 0 or not sig.symbol or (trade is None and pos is None and not self.inflight(sig.symbol)):
            return self._reject(sig, f"{sig.action}: sin posición abierta para {sig.symbol} o trigger_price inválido")
        task = self._spawn(self._signal_protect(sig, kind, trigger))
        return 200, {"ok": True, "action": sig.action, "symbol": sig.symbol, "trigger_price": trigger}, task

    async def _signal_protect(self, sig: Signal, kind: str, trigger: float) -> dict:
        symbol = sig.symbol
        await self._await_ready()
        await self._wait_inflight(symbol, sig.direction or None)
        trade = self._resolve_trade(sig.trade_id, symbol, sig.direction or None, sig.ref)
        pos = self.core.account.position(symbol, sig.direction or None)
        direction = trade.direction if trade else (pos.direction if pos else sig.direction)
        if direction not in ("LONG", "SHORT"):
            self.status["signals_rejected"] += 1
            entry = self._log_signal(sig.action, symbol, sig.direction, False,
                                     "sin posición abierta (o LONG y SHORT simultáneas sin direction)", sig)
            return {"ok": False, "detail": entry["detail"]}
        try:
            await self.set_protection(symbol, direction, kind, trigger, lot=sig.ref)
            self.status["signals_tp_set" if kind == "TP" else "signals_sl_set"] += 1
            note = self._shared_protection_note(self.trades.get((symbol, direction)), sig, kind)
            self._log_signal(sig.action, symbol, direction, True, f"trigger {trigger:g}{note}", sig)
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

    def _shared_protection_note(self, trade: Optional[Trade], sig: Signal, kind: str) -> str:
        """Binance aplica el TP/SL a la posición entera: si en ella hay lotes de otros trade_id, al
        dispararse también los cierra. Se avisa (dashboard, log y detalle de la señal)."""
        if trade is None or not sig.trade_id:
            return ""
        own = Trade.lot_key(sig.ref)
        others = [k for k in trade.lots if k != own]
        if not others:
            return ""
        msg = (f"{trade.symbol} {trade.direction}: el {kind} del trade_id {sig.trade_id} protege la posición "
               f"entera, que también tiene lotes de otros trade_id ({', '.join(others)}); si se dispara, los "
               f"cierra también")
        log.warning(msg)
        self.core.bus.emit("protect_shared", msg, "warning", symbol=trade.symbol)
        return f" — ⚠️ el {kind} cubre también otros trade_id"

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
                         signal_id: str = "", multiplier: float = 1.0, bot_quantity: float = 0.0,
                         bot: str = "") -> Optional[Trade]:
        symbol, direction = symbol.upper(), direction.upper()
        side = "BUY" if direction == "LONG" else "SELL"
        async with self._lock(symbol):
            if source == "signal" and paper_trade_id:
                # Un close de este trade_id pudo terminar mientras se esperaba el lock.
                age = self._closed_recently(lot_ref(bot, paper_trade_id), symbol, direction)
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
            tranche = {"level": level, "notional_req": desired, "signal_price": signal_price, "trade_id": paper_trade_id,
                       "signal_id": signal_id, "ts": time.time(), "multiplier": multiplier,
                       "bot_qty": bot_quantity}
            if bot:
                tranche["bot"] = bot
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
                                                 assumed=True, source=source, bot=bot)
                    if trade is not None:
                        trade.tranches.append({**tranche, "qty": float(qty), "fill_price": ref, "assumed": True})
                        if source == "signal":
                            trade.note_tranche_scale(multiplier, bot_quantity, float(qty))
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
                                         paper_trade_id, fill.order_id, hedge, fill.step, source=source, bot=bot)
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
                                       "fill_ms": fill.update_ms or self.core.clock.now_ms(),
                                       "order_id": fill.order_id, "leverage": applied_lev,
                                       "leverage_target": target_lev})
                if source == "signal":
                    trade.note_tranche_scale(multiplier, bot_quantity, fill.qty)
                self.save()
                self._announce_open(trade)
            return trade

    def _register_entry(self, symbol, direction, price, qty, leverage, paper_id, order_id, hedge, step,
                        assumed=False, source="signal", bot="") -> Optional[Trade]:
        key = (symbol, direction)
        now = utc_now_str()
        ref = lot_ref(bot, paper_id)
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
            trade.add_lot(ref, qty, assumed=assumed)
            self.trades[key] = trade
            self._map_paper(trade, paper_id)
            self.save()
            return trade

        if assumed and not existing.order_assumed:
            # Apertura asumida (-2019, sin orden real) sobre una posición real: solo se anota su lote.
            existing.add_lot(ref, qty, assumed=True)
            tag = "AMPLIADA (asumida)"
        elif hedge or existing.direction == direction:
            if existing.order_assumed and not assumed:
                # Primera orden real sobre lo asumido: la cantidad pasa a ser la real.
                existing.quantity, existing.entry_price, existing.order_assumed = 0.0, price, False
            new_qty = existing.quantity + qty
            existing.entry_price = (existing.entry_price * existing.quantity + price * qty) / new_qty
            existing.quantity = float(D(fmt(new_qty)))
            existing.add_lot(ref, qty, assumed=assumed)
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
                existing.lots = {Trade.lot_key(ref): abs(signed)}  # lo que queda es de la señal nueva
            else:
                existing.fit_lots(abs(signed))  # reducida: cada lote en proporción
            old_key = existing.key
            del self.trades[old_key]
            existing.direction = new_dir
            existing.quantity = abs(signed)
            self.trades[existing.key] = existing
            self._rekey_paper(old_key, existing.key)
            tag = "INVERTIDA" if new_dir == direction else "REDUCIDA"
        if not (assumed and not existing.order_assumed):
            existing.leverage = leverage
            existing.entry_order_id = str(order_id)
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
                      hedge_mode=pos.side != "BOTH", step_size=fmt(rules.step_size), source=source,
                      bot_quantity=pos.qty, multiplier=1.0)
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
                          closed_by: str = "signal", alert: bool = True, lot: Any = 0,
                          signal_id: str = "") -> bool:
        """Cierra el trade con órdenes reduceOnly.

        Con ``lot`` (el trade_id de la señal, o «bot:trade_id») solo se cierra lo de ese
        trade_id; ver ``_close_lot``. Sin ``lot``: la posición entera (o ``max_qty``).
        """
        key = trade.key
        # Otro cierre del mismo trade en curso (otro trade_id, el dashboard, close_all) va primero.
        deadline = time.time() + CLOSE_WAIT_S
        while key in self._closing and time.time() < deadline:
            await asyncio.sleep(0.05)
        if key in self._closing:
            return False
        if self.trades.get(key) is not trade:
            return trade.status != "OPEN"  # otro cierre (total) ya la cerró mientras esperaba
        self._closing.add(key)
        try:
            async with self._lock(trade.symbol):
                if self.trades.get(key) is not trade:
                    return trade.status != "OPEN"
                if lot:
                    return await self._close_lot(trade, reason, close_price, max_qty, bot_pnl, closed_by, alert, lot,
                                                 signal_id)
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
                if partial:
                    # Cantidad parcial al múltiplo exacto del step (p. ej. 0.003 × 3 = 0.009000000000000001).
                    rounded = float(rules.round_qty(D(fmt(qty)), True, "nearest"))
                    if rounded <= 0:
                        log.warning("%s: cierre parcial de %s es menor que el step %s; se ignora", trade.symbol,
                                    qty, step)
                        return False
                    qty = min(rounded, base)
                    partial = qty < base - step / 2
                if not partial:
                    qty = base  # cierre total: la cantidad real exacta (nunca una fracción del step)
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
                                if partial:
                                    # Descuenta lo que ya se cerró (p. ej. partes de una orden dividida).
                                    remaining = qty - max(0.0, base - pos2.qty)
                                    remaining = float(rules.round_qty(D(fmt(max(0.0, remaining))), True, "nearest"))
                                    if remaining <= 0:
                                        break
                                    qty = min(pos2.qty, remaining)
                                else:
                                    qty = pos2.qty
                                continue
                        # Solo se reintenta automáticamente un cierre TOTAL (cada intento relee la posición
                        # real). Un parcial nunca: si una parte ya se ejecutó (orden dividida o respuesta
                        # perdida), repetirlo cerraría de más; se avisa para revisarlo.
                        retryable = not partial and diag.action in (Action.RETRY, Action.CHECK_STATUS)
                        self._close_retryable[key] = retryable
                        if alert or not retryable:
                            self._alert_close_error(trade, diag)
                        return False
                if partial:
                    done = filled or qty
                    self._reduce_lots(trade, base, done)
                    if base < trade.quantity:
                        trade.sync_quantity(base)  # reducción externa aún no registrada
                    if trade.bot_quantity > 0:
                        trade.bot_quantity = max(0.0, trade.bot_quantity - done / (trade.multiplier or 1.0))
                    trade.quantity = max(0.0, base - done)
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

    def _alert_close_error(self, trade: Trade, diag) -> None:
        self.core.bus.emit("close_failed", f"{trade.symbol}: {diag.info.title}", "error",
                           symbol=trade.symbol, code=diag.code, solution=diag.info.solution)
        self.core.telegram.send(f"⚠️ <b>No se pudo cerrar</b> <code>{trade.symbol}</code>\n"
                                f"[{diag.code}] {diag.info.title}\n💡 {diag.info.solution}")

    async def _close_lot(self, trade: Trade, reason: str, close_price: float, max_qty: Optional[float],
                         bot_pnl: Optional[float], closed_by: str, alert: bool, lot: Any, signal_id: str = "") -> bool:
        """Cierra solo lo del trade_id ``lot`` (con el lock del símbolo y el trade en ``_closing``).

        * Su lote entero (o ``max_qty`` si es menor): aunque sea el último lote, si la posición
          real tiene más (otro bot en el mismo símbolo), el resto sigue abierto.
        * Un trade_id sin lote solo cierra algo si la posición nunca tuvo lotes (adoptada tras
          perder ``trades.json``); si no, no toca nada (``self._no_lot``).
        * Cada cierre queda en el historial con su propio registro (PnL, comisiones, bot_pnl).
        * Antes de calcular, se confirman (por clientOrderId) los cierres anteriores de esta
          posición cuya respuesta se perdió: nunca se cierra dos veces ni con lotes viejos.
        """
        key, lk = trade.key, Trade.lot_key(lot)
        tid, bot = _split_ref(lot)
        args = {"reason": reason, "close_price": close_price, "max_qty": max_qty, "bot_pnl": bot_pnl,
                "closed_by": closed_by, "signal_id": signal_id}
        market_px = self.core.market.price(trade.symbol)
        rules = self.core.exinfo.get(trade.symbol, market_px or trade.entry_price)
        step = float(rules.qty_step(True))
        tol = step / 2

        # 1) Cierres anteriores sin respuesta (de cualquier trade_id de esta posición).
        for k in [k for k in self._lot_attempts if k[0] == key]:
            attempt = self._lot_attempts[k]
            executed = await self._settle_attempt(trade, k[1], attempt, tol)
            if executed is None:
                self._close_retryable[key] = True
                log.warning("%s %s: no se pudo confirmar si se ejecutó un cierre anterior (trade_id %s); "
                            "se espera antes de cerrar nada más", trade.symbol, trade.direction, k[1])
                return False
            if self.trades.get(key) is not trade:
                return True  # ese cierre ya dejó la posición en cero
        # Reintento de una señal cuya orden anterior sí se ejecutó (confirmado ahora o antes, p. ej. por la
        # verificación periódica): solo falta lo que no se cerró de lo pedido.
        done = self._settled.pop(signal_id, None) if signal_id else None
        if done is not None and done["executed"] > tol:
            left = done["qty"] - done["executed"]
            if done["orphan"] or left <= tol:
                return True
            max_qty = None if done["whole"] else left

        pos, known = await self._fresh_position(trade.symbol, trade.direction)
        base = pos.qty if pos is not None else (0.0 if known or trade.order_assumed else trade.real_quantity)

        # 2) Parte asumida (-2019, sin orden real): se descarta sin enviar nada.
        lot_qty = trade.effective_lots(base).get(lk, 0.0)
        assumed = trade.assumed_lots.get(lk, 0.0)
        if lot_qty <= tol and assumed > 0:
            trade.assumed_lots.pop(lk, None)
            if trade.order_assumed and not trade.assumed_lots:
                trade.paper_trade_id = tid or trade.paper_trade_id
                self._finalize(trade, reason, close_price or market_px or trade.entry_price, bot_pnl=bot_pnl,
                               closed_by=closed_by)
            else:
                record = self._lot_trade(trade, lot, assumed, close_price)
                record.order_assumed = True
                if trade.order_assumed:
                    trade.quantity = max(0.0, trade.quantity - assumed)
                trade.tranches = [t for t in trade.tranches if t not in record.tranches]
                self._unmap_lot(trade, lot)
                self._book_close(record, reason, close_price or market_px or trade.entry_price, bot_pnl=bot_pnl,
                                 closed_by=closed_by)
            self._note_closed(lot, trade.symbol, trade.direction)
            self.save()
            return True

        # 3) Cuánto cerrar.
        orphan = False
        if lot_qty <= tol:
            if trade.lot_tracked or trade.lots or trade.assumed_lots or base <= tol:
                self._no_lot.add(key)  # la posición es de otros trade_id (o abierta por fuera): no se toca
                return False
            lot_qty, orphan = base, True  # posición que nunca tuvo lotes (adoptada): es de quien la cierre
        whole = not (max_qty and max_qty > 0 and max_qty < lot_qty - tol)
        qty = min(lot_qty if whole else max_qty, base)
        rounded = float(rules.round_qty(D(fmt(qty)), True, "nearest"))
        if rounded <= 0:
            log.warning("%s: el cierre de %s del trade_id %s es menor que el step %s; se ignora", trade.symbol,
                        qty, lot, step)
            return False
        qty = min(rounded, base)
        full = qty >= base - tol
        if full:
            qty = base
            await self.orders.cancel_protection(trade.symbol, trade.direction)
        record = self._lot_trade(trade, lot, qty, close_price)
        side = "SELL" if trade.direction == "LONG" else "BUY"
        ctx = OrderContext(trade.symbol, side, trade.direction, "close", rules, reduce_only=True)

        # 4) Órdenes reduceOnly a nombre del registro del lote. El intento se anota ANTES de enviar:
        #    si la respuesta se pierde, su fill (o la consulta por clientOrderId) lo confirma después.
        attempt = {"record": record, "orphan": orphan, "args": args, "qty": qty, "whole": whole, "active": True}
        self._lot_attempts[(key, lk)] = attempt
        filled = notional = 0.0
        for _ in range(LOT_CLOSE_ORDERS):
            want = float(rules.round_qty(D(fmt(max(0.0, qty - filled))), True, "nearest"))
            if want <= 0:
                break
            try:
                fill = await self.orders.market(ctx, D(fmt(want)), prefix=f"C{trade.id}", owner=record)
            except BinanceAPIError as err:
                diag = ErrorDoctor.diagnose(err)
                if diag.action in (Action.REFRESH_POSITION, Action.ALREADY_DONE):
                    pos2, _ = await self._fresh_position(trade.symbol, trade.direction)
                    if pos2 is None:
                        log.info("%s %s: la posición ya no existe en Binance (%s)", trade.symbol, trade.direction,
                                 diag.info.name)
                        if filled <= tol:
                            self._lot_attempts.pop((key, lk), None)
                            self._finalize(trade, reason, close_price or market_px or trade.entry_price,
                                           bot_pnl=bot_pnl, closed_by=closed_by)
                            return True
                        break
                    qty = min(qty, filled + pos2.qty)
                    continue
                # Pudo ejecutarse sin respuesta: se confirma por clientOrderId antes de enviar nada más.
                attempt["active"] = False
                retryable = diag.action in (Action.RETRY, Action.CHECK_STATUS)
                self._close_retryable[key] = retryable
                if alert or not retryable:
                    self._alert_close_error(trade, diag)
                return False
            except Exception:
                attempt["active"] = False
                raise
            filled += fill.qty
            notional += fill.qty * (fill.avg_price or market_px or trade.entry_price)
            if fill.qty <= tol:
                break
        self._lot_attempts.pop((key, lk), None)
        if filled <= tol:
            log.warning("%s %s: la orden de cierre del trade_id %s no se ejecutó", trade.symbol, trade.direction, lot)
            return False
        fill_price = notional / filled if filled else 0.0
        remaining = max(0.0, base - filled)
        if full:
            pos3, _ = await self._fresh_position(trade.symbol, trade.direction)
            remaining = pos3.qty if pos3 is not None else 0.0
            if remaining > tol and filled >= qty - tol:
                # Se abrió algo por fuera mientras se cerraba: no es de este trade_id, sigue abierto.
                log.warning("%s %s: tras cerrar el trade_id %s quedan %s abiertos por fuera (sin TP/SL)",
                            trade.symbol, trade.direction, lot, remaining)
        self._finish_lot(trade, record, lk, orphan, filled, base, remaining,
                         fill_price or close_price or market_px or trade.entry_price, args)
        if filled < qty - tol:
            # Binance no llenó todo lo pedido: lo que falta sigue siendo de ese trade_id.
            log.warning("%s %s: el cierre del trade_id %s se llenó en parte (%s de %s)", trade.symbol,
                        trade.direction, lot, filled, qty)
            self._close_retryable[key] = whole  # un lote entero se reintenta; una cantidad parcial no
            if alert or not whole:
                self.core.bus.emit("close_failed", f"{trade.symbol}: cierre del trade_id {lot} incompleto "
                                   f"({filled:g} de {qty:g})", "error", symbol=trade.symbol)
                self.core.telegram.send(f"⚠️ <b>Cierre incompleto</b> <code>{trade.symbol}</code> trade_id {lot}\n"
                                        f"Se cerró {filled:g} de {qty:g}")
            return False
        return True

    async def _settle_attempt(self, trade: Trade, lk: str, attempt: dict, tol: float) -> Optional[float]:
        """Confirma por clientOrderId cuánto ejecutó un cierre de lote sin respuesta y lo registra.
        Devuelve la cantidad ejecutada, o None si Binance aún no lo confirma."""
        executed = await self._attempt_executed(trade.symbol, attempt["record"])
        if executed is None:
            return None
        self._lot_attempts.pop((trade.key, lk), None)
        if executed > tol:
            log.warning("%s %s: el cierre anterior del trade_id %s sí se ejecutó (%s)", trade.symbol,
                        trade.direction, lk, executed)
            pos, _ = await self._fresh_position(trade.symbol, trade.direction)
            base = pos.qty if pos is not None else 0.0
            args = attempt["args"]
            price = args["close_price"] or self.core.market.price(trade.symbol) or trade.entry_price
            self._finish_lot(trade, attempt["record"], lk, attempt["orphan"], executed, base + executed, base,
                             price, args)
            if args.get("signal_id"):
                now = time.time()
                self._settled = {k: v for k, v in self._settled.items() if now - v["ts"] < 300}
                self._settled[args["signal_id"]] = {"executed": executed, "qty": attempt["qty"],
                                                    "whole": attempt["whole"], "orphan": attempt["orphan"], "ts": now}
        return executed

    async def verify_lot_attempts(self, only_key: Optional[tuple] = None) -> None:
        """Confirma los cierres de lote cuya respuesta se perdió (por su fill o por consulta periódica),
        aunque el bot ya no reintente. Solo registra lo ejecutado; no envía órdenes."""
        keys = {k for (k, _), a in list(self._lot_attempts.items())
                if not a.get("active") and (only_key is None or k == only_key)}
        for key in keys:
            trade = self.trades.get(key)
            if trade is None:
                for k in [k for k in self._lot_attempts if k[0] == key]:
                    self._lot_attempts.pop(k, None)
                continue
            deadline = time.time() + CLOSE_WAIT_S
            while key in self._closing and time.time() < deadline:
                await asyncio.sleep(0.05)
            if key in self._closing:
                continue
            self._closing.add(key)
            try:
                async with self._lock(trade.symbol):
                    tol = (safe_float(trade.step_size) or 0.0) / 2 or 1e-12
                    for k in [k for k in self._lot_attempts if k[0] == key]:
                        attempt = self._lot_attempts.get(k)
                        if attempt is None or attempt.get("active") or self.trades.get(key) is not trade:
                            continue
                        if await self._settle_attempt(trade, k[1], attempt, tol) is None:
                            break
            except Exception:  # pragma: no cover
                log.exception("No se pudo verificar el cierre pendiente de %s", key)
            finally:
                self._closing.discard(key)

    def _finish_lot(self, trade: Trade, record: Trade, lk: str, orphan: bool, done: float, base: float,
                    remaining: float, price: float, args: dict) -> None:
        """Registra el cierre de ``done`` del lote ``lk`` y deja el trade con lo que sigue abierto."""
        step = safe_float(trade.step_size) or 0.0
        tol = step / 2 if step else 1e-12
        sign = 1 if trade.direction == "LONG" else -1
        record.quantity = done
        record.bot_quantity = done / (record.multiplier or 1.0)
        record.ref_entry = trade.entry_price  # entrada media: la que usa Binance para el realizado
        lot_done = True
        assumed_left = {k: v for k, v in trade.assumed_lots.items() if k != lk and v > 0}
        if remaining <= tol and not assumed_left:
            # Posición cerrada: el registro del lote se queda con todo lo del trade.
            record.id = trade.id
            record.fees_usdt += trade.fees_usdt
            record.realized_exchange += trade.realized_exchange
            record.pnl_adjust = trade.pnl_adjust  # lo que los lotes ya cerrados dejaron a los que quedaban
            record.tranches = trade.tranches
            record.open_time, record.opened_ts = trade.open_time, trade.opened_ts
            trade.status = args["reason"]  # quien esperaba para cerrarla sabe que ya está cerrada
            self._unmap_trade(trade)
            self.trades.pop(trade.key, None)
            for k in [k for k in self._lot_attempts if k[0] == trade.key]:
                self._lot_attempts.pop(k, None)
        else:
            share = min(1.0, done / base) if base > 0 else 1.0
            entry_fees = trade.fees_usdt * share  # su parte de las comisiones de apertura
            trade.fees_usdt -= entry_fees
            record.fees_usdt += entry_fees
            if not orphan:
                # PnL propio: la entrada del lote frente a la media de la posición.
                adj = (trade.entry_price - record.entry_price) * done * sign
                record.pnl_adjust += adj
                trade.pnl_adjust -= adj
            lots = trade.effective_lots(base)
            rest = lots.get(lk, 0.0) - (0.0 if orphan else done)
            closed_lot = not orphan and rest <= tol
            if closed_lot:
                lots.pop(lk, None)
                trade.assumed_lots.pop(lk, None)
                trade.tranches = [t for t in trade.tranches if t not in record.tranches]
            elif not orphan:
                lots[lk] = rest  # cierre parcial o no se llenó todo: lo demás sigue siendo de ese trade_id
                lot_done = False
            trade.lots = {k: float(D(fmt(v))) for k, v in lots.items()}
            if closed_lot:
                self._unmap_lot(trade, lk)
                self._drop_lot_protection(trade, lk)
            trade.quantity = float(D(fmt(remaining)))
            if trade.bot_quantity > 0:
                trade.bot_quantity = max(0.0, trade.bot_quantity - record.bot_quantity)
            if remaining <= tol:
                # Sin posición real: solo quedan aperturas asumidas (-2019) de otros trade_id.
                trade.lots, trade.order_assumed = {}, True
                trade.quantity = sum(assumed_left.values())
        if lot_done and not orphan:
            tid, bot = _split_ref(lk)
            self._note_closed(lot_ref(bot, tid), trade.symbol, trade.direction)
        self.save()
        log.info("[#%d] %s %s: cerrado el trade_id %s (%s); siguen abiertos %s", trade.id, trade.direction,
                 trade.symbol, record.paper_trade_id, done, remaining)
        self._book_close(record, args["reason"], price, bot_pnl=args["bot_pnl"], closed_by=args["closed_by"])

    def _drop_lot_protection(self, trade: Trade, lk: str) -> None:
        """El trade_id que puso un TP/SL ya cerró: su orden (de cantidad fija) se cancela para que no
        cierre después la cantidad de otros trade_id."""
        kinds = [k for k, owner in trade.protect_lots.items() if owner == lk]
        if not kinds:
            return
        types = {"TAKE_PROFIT_MARKET" if k == "TP" else "STOP_MARKET" for k in kinds}
        for k in kinds:
            trade.protect_lots.pop(k, None)
            if k == "TP":
                trade.tp_price = 0.0
            else:
                trade.sl_price = 0.0
        log.info("%s %s: se cancela el %s del trade_id %s (ya cerrado)", trade.symbol, trade.direction,
                 "/".join(kinds), lk)
        self._spawn(self.orders.cancel_protection(trade.symbol, trade.direction, types))

    async def _attempt_executed(self, symbol: str, record: Trade) -> Optional[float]:
        """Cantidad ejecutada por las órdenes de un intento (por su clientOrderId); None = sin confirmar."""
        cids = [cid for cid, owner in list(self.orders.client_owner.items()) if owner is record]
        total = 0.0
        for cid in cids:
            status, definitive = await self.orders.lookup_order(symbol, client_id=cid)
            if not definitive:
                return None
            if status:
                total += safe_float(status.get("executedQty"))
        return total

    def _lot_trade(self, trade: Trade, lot: Any, qty: float, close_price: float) -> Trade:
        """Registro propio del cierre de un trade_id (dueño de su orden: recibe su PnL y comisiones)."""
        tid, bot = _split_ref(lot)
        tranches = trade.lot_tranches(lot)
        opened = min((t.get("ts") or trade.opened_ts for t in tranches), default=trade.opened_ts)
        self.counter += 1
        lot_mult = trade.lot_multiplier(lot)
        return Trade(id=self.counter, symbol=trade.symbol, direction=trade.direction,
                     entry_price=trade.lot_entry(lot), quantity=qty,
                     open_time=time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(opened)),
                     leverage=trade.leverage, paper_trade_id=tid, current_price=trade.current_price,
                     hedge_mode=trade.hedge_mode, step_size=trade.step_size, source=trade.source,
                     tp_price=trade.tp_price, sl_price=trade.sl_price, opened_ts=opened, paper_ids=[tid] if tid else [],
                     tranches=tranches, multiplier=lot_mult, notes=LOT_NOTE, bot=bot,
                     bot_quantity=qty / (lot_mult or 1.0), signal_close_price=close_price,
                     signal_entry_price=next((t["signal_price"] for t in tranches if t.get("signal_price")), 0.0))

    @staticmethod
    def _reduce_lots(trade: Trade, base: float, done: float) -> None:
        """Cierre parcial por cantidad sin trade_id: primero lo que no es de nadie y luego cada lote en
        proporción."""
        lots = trade.effective_lots(base)
        rest = max(0.0, done - trade.unassigned_qty(base))
        total = sum(lots.values())
        if rest > 0 and total > 0:
            factor = max(0.0, (total - rest) / total)
            lots = {k: v * factor for k, v in lots.items()}
        trade.lots = {k: float(D(fmt(v))) for k, v in lots.items() if v > 1e-12}

    def _unmap_lot(self, trade: Trade, lot: Any) -> None:
        """El trade_id deja de estar en la posición (salvo que otro bot use el mismo número)."""
        tid, _ = _split_ref(lot)
        if not tid or any(_split_ref(k)[0] == tid for k in list(trade.lots) + list(trade.assumed_lots)):
            return
        if self.paper_map.get((tid, trade.symbol)) == trade.key:
            del self.paper_map[(tid, trade.symbol)]
        trade.paper_ids = [p for p in trade.paper_ids if int(p) != tid]
        if trade.paper_trade_id == tid:
            trade.paper_trade_id = int(trade.paper_ids[-1]) if trade.paper_ids else 0

    def _finalize(self, trade: Trade, reason: str, close_price: float, bot_pnl: Optional[float] = None,
                  closed_by: str = "") -> None:
        self._unmap_trade(trade)
        self.trades.pop(trade.key, None)
        self._book_close(trade, reason, close_price, bot_pnl=bot_pnl, closed_by=closed_by)

    def _book_close(self, trade: Trade, reason: str, close_price: float, bot_pnl: Optional[float] = None,
                    closed_by: str = "") -> None:
        """Pasa al historial un trade (o el lote de un trade_id) ya cerrado, con su PnL."""
        trade.status = reason
        trade.close_price = close_price
        trade.close_time = utc_now_str()
        trade.closed_ts = time.time()
        trade.pnl_usdt = trade.booked_pnl(close_price)
        trade.roi_pct = trade.pnl_usdt / trade.notional_usdt * 100 if trade.notional_usdt else 0.0
        if bot_pnl is not None:
            trade.bot_pnl, trade.has_bot_pnl = float(bot_pnl), True
        trade.closed_by = closed_by or trade.closed_by
        self.realized_total += trade.pnl_usdt
        self.closed.append(trade)
        if len(self.closed) > 1000:
            self.closed = self.closed[-500:]
        self.save()
        emoji, label = CLOSE_REASONS.get(reason, ("⚠️", reason))
        what = f"trade_id {trade.paper_trade_id} de {trade.symbol} cerrado" if trade.notes == LOT_NOTE \
            else f"{trade.symbol} cerrada"
        log.info("[#%d] CERRADA %s %s @ %.8g | PnL %+.4f USDT (%+.2f%%)", trade.id, reason, trade.symbol,
                 close_price, trade.pnl_usdt, trade.roi_pct)
        self.core.bus.emit("close", f"{what} ({label}) PnL {trade.pnl_usdt:+.4f} USDT",
                           "success" if trade.pnl_usdt >= 0 else "warning", symbol=trade.symbol)
        self.core.telegram.send(self.close_message(trade))

    def close_message(self, trade: Trade) -> str:
        emoji, label = CLOSE_REASONS.get(trade.status, ("⚠️", trade.status))
        st = self.stats()
        wr = f"{st['win_rate']:.1f}% ({st['wins']}✅/{st['losses']}❌)" if st["win_rate"] is not None else "N/A"
        bot = f"\n🤖 PnL del bot: <code>{trade.bot_pnl:+.4f}</code>" if trade.has_bot_pnl else ""
        title = f"TRADE_ID #{trade.paper_trade_id} CERRADO" if trade.notes == LOT_NOTE else "POSICIÓN CERRADA"
        rest = ""
        if trade.notes == LOT_NOTE:
            parent = self.trades.get(trade.key)
            rest = f"\n📦 Sigue abierta: <code>{parent.quantity:g}</code> (otros trade_id)" if parent else ""
        return (
            f"{emoji} <b>{title} — {label}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Par:</b> <code>{trade.symbol}</code> {'🟢 LONG' if trade.direction == 'LONG' else '🔴 SHORT'}\n"
            f"💵 <b>Entrada:</b> <code>{trade.entry_price:,.8g}</code>\n"
            f"💵 <b>Salida:</b> <code>{trade.close_price:,.8g}</code>\n"
            f"{'💚' if trade.pnl_usdt >= 0 else '❗'} <b>PnL:</b> <code>{trade.pnl_usdt:+.4f} USDT</code> "
            f"(ROI {trade.roi_pct:+.2f}% · ROE {trade.roe_pct:+.2f}%){bot}\n"
            f"⏱ {trade.open_time} → {trade.close_time}\n"
            f"📈 <b>Win rate:</b> <code>{wr}</code>\n🆔 #{trade.id} | Paper #{trade.paper_trade_id}{rest}"
        )

    async def close_symbol(self, symbol: str, direction: Optional[str], reason: str, paper_id: int = 0,
                           bot_pnl: Optional[float] = None, closed_by: str = "signal", lot: Any = 0) -> bool:
        """Cierra una posición real sin trade local (excepto si es de un grid).

        La posición se registra primero como trade (origen «externa») para que
        el cierre quede en el historial y en el PnL realizado. Con ``lot`` solo
        se cierra lo que no es de otros trade_id.
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
            # Una posición ya registrada (de otros trade_id) no se asocia a este trade_id.
            trade = self.trades.get((pos.symbol, pos.direction)) if lot else None
            trade = trade or self.adopt_position(pos, paper_id=paper_id, source="externa")
            ok = await self.close_trade(trade, reason, bot_pnl=bot_pnl, closed_by=closed_by, lot=lot)
            self._no_lot.discard(trade.key)
            if not ok and self._close_retryable.get(trade.key):
                self._symbol_retryable[symbol] = True
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
    async def set_protection(self, symbol: str, direction: str, kind: str, trigger: float, lot: Any = 0) -> dict:
        """TP/SL de la posición. ``lot``: el trade_id que lo pidió (su fill se descuenta primero de su lote)."""
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
            trade.protect_lots[kind.upper()] = Trade.lot_key(lot) if lot else ""
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
                if owner.status != "OPEN" and upd.status == "FILLED":
                    before = owner.pnl_usdt
                    owner.pnl_usdt = owner.booked_pnl(owner.close_price)
                    owner.roi_pct = owner.pnl_usdt / owner.notional_usdt * 100 if owner.notional_usdt else 0.0
                    self.realized_total += owner.pnl_usdt - before
                    self.save()
                elif owner.notes == LOT_NOTE and owner.status == "OPEN" and upd.status == "FILLED":
                    self._settle_lot_attempt(owner)  # cierre cuya respuesta se perdió: ya se ejecutó
            else:
                trade = self._trade_for_position(upd.symbol, upd.position_side)
                if trade is not None and owner is None:
                    trade.fees_usdt += upd.fee_usdt
                    trade.realized_exchange += upd.realized_pnl
                    closing = (upd.side == "SELL") == (trade.direction == "LONG")
                    if closing and trade.lots and not trade.order_assumed:
                        self._external_reduction(trade, upd)

    def _settle_lot_attempt(self, record: Trade) -> None:
        """Llegó el fill de un cierre de lote que había fallado sin respuesta: se confirma ya."""
        for (key, _), attempt in list(self._lot_attempts.items()):
            if attempt["record"] is record and not attempt.get("active"):
                self._spawn(self.verify_lot_attempts(key))
                return

    def _external_reduction(self, trade: Trade, upd: OrderUpdate) -> None:
        """Un fill que reduce la posición sin ser del executor (TP/SL, ADL, liquidación, cierre a mano
        en Binance): se descuenta de los lotes en ese momento, una sola vez por fill. Si es el TP/SL
        que puso un trade_id, primero de ese lote."""
        kind = "TP" if upd.orig_type.startswith("TAKE_PROFIT") else "SL" if upd.orig_type.startswith("STOP") else ""
        first = trade.protect_lots.get(kind, "") if kind else ""
        newer: dict[str, float] = {}
        if upd.trade_time:
            # Tramos que Binance llenó después de este fill: esa cantidad aún no existía cuando se cerró.
            for t in trade.tranches:
                if safe_float(t.get("fill_ms")) > upd.trade_time and not t.get("assumed"):
                    k = Trade.lot_key(lot_ref(t.get("bot", ""), t.get("trade_id", 0)))
                    newer[k] = newer.get(k, 0.0) + safe_float(t.get("qty"))
        for lk in trade.reduce_lots(upd.last_qty, first, newer):
            self._unmap_lot(trade, lk)
            tid, bot = _split_ref(lk)
            self._note_closed(lot_ref(bot, tid), trade.symbol, trade.direction)  # su close posterior no toca nada
        self.save()

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
                trade.sync_quantity(pos.qty)
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
                trade.sync_quantity(pos.qty)
                trade.entry_price = pos.entry_price or trade.entry_price
            if pos is not None:
                # Lectura completa posterior a todo lo de esta posición (p. ej. tras un reinicio o un corte
                # del user stream): lo que se cerró fuera mientras tanto se fija en los lotes.
                trade.fit_lots(pos.qty)

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
