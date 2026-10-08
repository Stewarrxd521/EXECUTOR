"""Gestión de operaciones por señal (compatibles con app.py) y manuales."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Callable, Optional

from .account import OrderUpdate
from .core import Core, utc_now_str
from .errors import Action, BinanceAPIError, ErrorDoctor
from .orders import OrderContext, OrderExecutor
from .precision import D, fmt, safe_float
from .storage import JsonStore

log = logging.getLogger("executor.trading")

CLOSE_REASONS = {
    "TP": ("✅", "TAKE PROFIT 🎯"),
    "SL": ("❌", "STOP LOSS 🛑"),
    "CLOSED": ("🔄", "CIERRE EXTERNO"),
    "EXTERNAL": ("🔄", "CIERRE EXTERNO (Binance)"),
    "LIQUIDATION": ("💀", "LIQUIDACIÓN"),
    "ADL": ("⚠️", "AUTO-DELEVERAGE"),
    "MAIN_BOT": ("🔄", "CIERRE SEÑAL PRINCIPAL"),
    "CLOSE_ALL": ("🛑", "CIERRE GLOBAL (SEÑAL)"),
    "MANUAL": ("🖐", "CIERRE MANUAL (DASHBOARD)"),
    "NETTED": ("➖", "NETEADA POR SEÑAL OPUESTA"),
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
    step_size: str = "1"
    fees_usdt: float = 0.0
    realized_exchange: float = 0.0
    source: str = "signal"
    tp_price: float = 0.0
    sl_price: float = 0.0
    opened_ts: float = field(default_factory=time.time)
    closed_ts: float = 0.0
    notes: str = ""

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
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "Trade":
        names = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in data.items() if k in names})


def _fresh_status() -> dict:
    return {
        "signals_received": 0, "signals_open": 0, "signals_close": 0, "signals_rejected": 0,
        "manual_closes": 0, "signals_tp_set": 0, "signals_tp_closed": 0, "signals_sl_set": 0,
        "signals_sl_closed": 0, "last_signal_time": "Esperando señales...", "last_signal_detail": "",
        "started_at": utc_now_str(),
    }


class TradeManager:
    def __init__(self, core: Core, orders: OrderExecutor):
        self.core = core
        self.orders = orders
        self.settings = core.settings
        self.trades: dict[tuple[str, str], Trade] = {}
        self.closed: list[Trade] = []
        self.counter = 0
        self.paper_map: dict[int, tuple[str, str]] = {}
        self.status = _fresh_status()
        self.trading_enabled = True
        self.signal_log: deque[dict] = deque(maxlen=200)
        self.grid_owner: Callable[[str, str], bool] = lambda symbol, direction: False
        self._locks: dict[str, asyncio.Lock] = {}
        self._closing: set[tuple[str, str]] = set()
        self._dedupe: dict[tuple, float] = {}
        self._last_close_fill: dict[str, OrderUpdate] = {}
        self._pending_external: set[tuple[str, str]] = set()
        self.store = JsonStore(core.settings.data_dir / "trades.json", self._serialize)

    # ── Persistencia ──────────────────────────────────────────────────────
    def _serialize(self) -> dict:
        return {
            "counter": self.counter,
            "trading_enabled": self.trading_enabled,
            "leverage": self.settings.leverage,
            "status": self.status,
            "open": [t.to_dict() for t in self.trades.values()],
            "closed": [t.to_dict() for t in self.closed[-500:]],
        }

    def load(self) -> None:
        data = self.store.load({}) or {}
        self.counter = int(data.get("counter", 0))
        self.trading_enabled = bool(data.get("trading_enabled", True))
        if data.get("leverage"):
            self.settings.leverage = int(data["leverage"])
        started = self.status["started_at"]
        self.status.update(data.get("status", {}))
        self.status["started_at"] = started
        for raw in data.get("open", []):
            t = Trade.from_dict(raw)
            self.trades[t.key] = t
            if t.paper_trade_id:
                self.paper_map[t.paper_trade_id] = t.key
        self.closed = [Trade.from_dict(raw) for raw in data.get("closed", [])]
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

    def find_by_paper_id(self, paper_id: int) -> Optional[Trade]:
        key = self.paper_map.get(paper_id) if paper_id else None
        return self.trades.get(key) if key else None

    @property
    def realized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self.closed)

    @property
    def unrealized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self.trades.values())

    def leverage_for_price(self, price: float) -> int:
        s = self.settings
        return s.high_price_leverage if price > s.high_price_threshold else s.leverage

    def refresh_marks(self) -> None:
        for t in self.trades.values():
            price = self.core.market.price(t.symbol)
            if price:
                t.update_unrealized(price)

    def stats(self) -> dict:
        wins = sum(1 for t in self.closed if t.pnl_usdt > 0)
        total = len(self.closed)
        return {
            "wins": wins, "losses": total - wins,
            "win_rate": (wins / total * 100) if total else None,
            "realized_pnl": self.realized_pnl, "unrealized_pnl": self.unrealized_pnl,
            "fees": sum(t.fees_usdt for t in self.closed),
        }

    # ── Señales (HTTP / WebSocket) ────────────────────────────────────────
    def _log_signal(self, action: str, symbol: str, direction: str, ok: bool, detail: str) -> None:
        self.signal_log.append({"ts": time.time(), "action": action, "symbol": symbol, "direction": direction,
                                "ok": ok, "detail": detail})

    def _reject(self, action: str, symbol: str, direction: str, error: str, status: int = 400) -> tuple[int, dict]:
        self.status["signals_rejected"] += 1
        self._log_signal(action, symbol, direction, False, error)
        log.warning("Señal %s %s rechazada: %s", action.upper(), symbol, error)
        return status, {"ok": False, "error": error}

    def _is_duplicate(self, data: dict) -> bool:
        ttl = self.settings.signal_dedupe_ttl_s
        if ttl <= 0:
            return False
        key = (str(data.get("action", "")).lower(), str(data.get("symbol", "")).upper(),
               str(data.get("direction", "")).upper(), str(data.get("trade_id", "")),
               str(data.get("trigger_price", "")))
        now = time.time()
        self._dedupe = {k: ts for k, ts in self._dedupe.items() if now - ts < ttl}
        if key in self._dedupe:
            return True
        self._dedupe[key] = now
        return False

    def handle_signal(self, data: dict, source: str = "http") -> tuple[int, dict]:
        """Valida la señal, responde al instante y ejecuta en segundo plano."""
        action = str(data.get("action", "")).lower().strip()
        symbol = str(data.get("symbol", "")).upper().strip()
        direction = str(data.get("direction", "")).upper().strip()
        try:
            trade_id = int(float(data.get("trade_id", 0) or 0))
        except (TypeError, ValueError):
            trade_id = 0

        self.status["signals_received"] += 1
        self.status["last_signal_time"] = time.strftime("%H:%M:%S UTC", time.gmtime())
        self.status["last_signal_detail"] = f"{action.upper()} {symbol} {direction}".strip()

        if self._is_duplicate(data):
            return self._reject(action, symbol, direction, f"señal duplicada en menos de {self.settings.signal_dedupe_ttl_s:.0f}s", 200)

        if action == "open":
            price = safe_float(data.get("price"))
            quantity = safe_float(data.get("quantity", data.get("qty")))
            notional = safe_float(data.get("notional", data.get("usdt")))
            margin = safe_float(data.get("margin"))
            if not symbol or direction not in ("LONG", "SHORT"):
                return self._reject(action, symbol, direction, "faltan symbol o direction (LONG/SHORT)")
            if quantity <= 0 and notional <= 0 and margin <= 0 and self.settings.default_notional_usdt <= 0:
                return self._reject(action, symbol, direction,
                                    "falta el tamaño: envía quantity/qty, notional/usdt o margin "
                                    "(o configura DEFAULT_NOTIONAL_USDT)")
            if symbol in self.settings.blocked_symbols:
                return self._reject(action, symbol, direction, f"{symbol} está bloqueado (BLOCKED_SYMBOLS)", 200)
            if not self.trading_enabled:
                return self._reject(action, symbol, direction, "trading pausado desde el dashboard", 200)
            ok, why = self.core.exinfo.tradable(symbol)
            if not ok:
                return self._reject(action, symbol, direction, why, 200)
            if self.grid_owner(symbol, direction):
                return self._reject(action, symbol, direction, f"{symbol} {direction} está gestionado por un bot Grid", 200)
            asyncio.create_task(self._signal_open(symbol, direction, price, quantity, notional, margin, trade_id, source))
            return 200, {"ok": True, "action": "open", "symbol": symbol, "direction": direction}

        if action == "close":
            if not symbol and not trade_id:
                return self._reject(action, symbol, direction, "falta symbol o trade_id")
            reason = str(data.get("reason", "MAIN_BOT")).upper() or "MAIN_BOT"
            close_price = safe_float(data.get("close_price"))
            quantity = safe_float(data.get("quantity", data.get("qty")))
            asyncio.create_task(self._signal_close(symbol, direction or None, trade_id, reason, close_price, quantity))
            return 200, {"ok": True, "action": "close", "symbol": symbol}

        if action == "close_all":
            total = len(self.trades)
            asyncio.create_task(self._signal_close_all("CLOSE_ALL"))
            return 200, {"ok": True, "action": "close_all", "positions_targeted": total}

        if action in ("open_tp", "open_sl"):
            trigger = safe_float(data.get("trigger_price"))
            trade = self.get_trade(symbol, direction or None)
            pos = self.core.account.position(symbol, direction or None) if symbol else None
            if trigger <= 0 or (trade is None and pos is None):
                return self._reject(action, symbol, direction,
                                    f"{action}: sin posición abierta para {symbol} o trigger_price inválido")
            kind = "TP" if action == "open_tp" else "SL"
            asyncio.create_task(self._signal_protect(symbol, (trade.direction if trade else pos.direction), kind, trigger))
            return 200, {"ok": True, "action": action, "symbol": symbol, "trigger_price": trigger}

        if action in ("close_tp", "close_sl"):
            if not symbol:
                return self._reject(action, symbol, direction, f"{action}: falta symbol")
            kind = "TAKE_PROFIT_MARKET" if action == "close_tp" else "STOP_MARKET"
            asyncio.create_task(self._signal_unprotect(symbol, direction or None, kind, action))
            return 200, {"ok": True, "action": action, "symbol": symbol}

        return self._reject(action, symbol, direction, f"acción desconocida: {action or '(vacía)'}")

    async def _signal_open(self, symbol, direction, price, quantity, notional, margin, trade_id, source) -> None:
        if margin > 0 and notional <= 0:
            notional = margin * self.leverage_for_price(price or self.core.market.price(symbol) or 1.0)
        trade = await self.open_trade(symbol, direction, signal_price=price, quantity=quantity,
                                      notional=notional, paper_trade_id=trade_id, source="signal")
        if trade is not None:
            self.status["signals_open"] += 1
            self._log_signal("open", symbol, direction, True,
                             f"#{trade.id} qty={trade.quantity} @ {trade.entry_price:g}" + (" (ASUMIDA)" if trade.order_assumed else ""))
        else:
            self.status["signals_rejected"] += 1
            self._log_signal("open", symbol, direction, False, "no ejecutada (ver pestaña Errores)")

    async def _signal_close(self, symbol, direction, trade_id, reason, close_price, quantity) -> None:
        trade = self.find_by_paper_id(trade_id) or (self.get_trade(symbol, direction) if symbol else None)
        if trade is not None:
            ok = await self.close_trade(trade, reason, close_price=close_price, max_qty=quantity or None)
        else:
            ok = await self.close_symbol(symbol, direction, reason)
        if ok:
            self.status["signals_close"] += 1
        self._log_signal("close", symbol or (trade.symbol if trade else ""), direction or "", ok,
                         "cerrada" if ok else "sin posición o error al cerrar")

    async def _signal_close_all(self, reason: str) -> None:
        closed = await self.close_all(reason)
        self.status["signals_close"] += len(closed)
        self._log_signal("close_all", "", "", True, f"{len(closed)} posición(es) cerradas")

    async def _signal_protect(self, symbol, direction, kind, trigger) -> None:
        try:
            await self.set_protection(symbol, direction, kind, trigger)
            self.status["signals_tp_set" if kind == "TP" else "signals_sl_set"] += 1
            self._log_signal(f"open_{kind.lower()}", symbol, direction, True, f"trigger {trigger:g}")
        except BinanceAPIError as err:
            self.status["signals_rejected"] += 1
            self._log_signal(f"open_{kind.lower()}", symbol, direction, False, ErrorDoctor.diagnose(err).summary())

    async def _signal_unprotect(self, symbol, direction, order_type, action) -> None:
        n = await self.orders.cancel_protection(symbol, direction, {order_type})
        self.status["signals_tp_closed" if action == "close_tp" else "signals_sl_closed"] += 1
        self._log_signal(action, symbol, direction or "", True, f"{n} orden(es) canceladas")
        label = "TP" if action == "close_tp" else "SL"
        self.core.telegram.send(f"{'🎯' if label == 'TP' else '🛑'} <b>{label} cancelado</b>\n<code>{symbol}</code> — {n} orden(es)")

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
                         leverage: Optional[int] = None) -> Optional[Trade]:
        symbol, direction = symbol.upper(), direction.upper()
        side = "BUY" if direction == "LONG" else "SELL"
        async with self._lock(symbol):
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

            qty = rules.qty_for_notional(desired, ref, market=True, buffer_pct=self.settings.notional_buffer_pct,
                                         min_notional_floor=self.settings.min_notional_usdt)
            ctx = OrderContext(symbol, side, direction, "open", rules, market=True,
                               desired_notional=desired, ref_price=ref)
            log.info("Apertura %s %s: qty=%s (≈%.2f USDT, ref=%s, lev=%sx)", direction, symbol, fmt(qty),
                     float(qty) * ref, ref, applied_lev)
            owner = {"kind": "open", "symbol": symbol, "direction": direction}
            try:
                fill = await self.orders.market(ctx, qty, prefix=f"X{paper_trade_id or 0}", owner=owner)
            except BinanceAPIError as err:
                diag = ErrorDoctor.diagnose(err)
                if diag.info.code == -2019 and self.settings.assume_on_margin_error:
                    log.warning("[ASUMIDA] %s %s: margen insuficiente, se registra como abierta", direction, symbol)
                    trade = self._register_entry(symbol, direction, ref, float(qty), applied_lev, paper_trade_id,
                                                 "MARGIN_INSUFFICIENT", self.core.account.is_hedge, rules.step_size,
                                                 assumed=True, source=source)
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
            if fill.fixes:
                log.info("Apertura %s corregida automáticamente: %s", symbol, "; ".join(fill.fixes))
            if trade is not None:
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
            if paper_id:
                self.paper_map[paper_id] = key
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
                self._finalize(existing, "NETTED", price)
                return None
            new_dir = "LONG" if signed > 0 else "SHORT"
            if new_dir != existing.direction:
                existing.entry_price = price
            del self.trades[existing.key]
            existing.direction = new_dir
            existing.quantity = abs(signed)
            self.trades[existing.key] = existing
            tag = "INVERTIDA" if new_dir == direction else "REDUCIDA"
        existing.leverage = leverage
        existing.entry_order_id = str(order_id)
        existing.order_assumed = existing.order_assumed or assumed
        existing.hedge_mode = hedge
        if paper_id:
            self.paper_map[paper_id] = existing.key
        log.info("[#%d] %s %s %s → qty %s @ %.8g", existing.id, tag, existing.direction, symbol,
                 existing.quantity, existing.entry_price)
        self.save()
        return existing

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
    async def close_trade(self, trade: Trade, reason: str, close_price: float = 0.0,
                          max_qty: Optional[float] = None) -> bool:
        key = trade.key
        if key in self._closing or self.trades.get(key) is not trade:
            return False
        self._closing.add(key)
        try:
            async with self._lock(trade.symbol):
                pos = self.core.account.position(trade.symbol, trade.direction)
                if pos is None and trade.order_assumed:
                    self._finalize(trade, reason, close_price or self.core.market.price(trade.symbol) or trade.entry_price)
                    return True
                await self.orders.cancel_protection(trade.symbol, trade.direction)
                qty = pos.qty if pos else trade.quantity
                if max_qty and max_qty > 0:
                    qty = min(qty, max_qty)
                partial = bool(pos and qty < pos.qty - 1e-12)
                rules = self.core.exinfo.get(trade.symbol, self.core.market.price(trade.symbol))
                side = "SELL" if trade.direction == "LONG" else "BUY"
                ctx = OrderContext(trade.symbol, side, trade.direction, "close", rules, reduce_only=True)
                fill_price = 0.0
                try:
                    fill = await self.orders.market(ctx, D(fmt(qty)), prefix=f"C{trade.id}", owner=trade)
                    fill_price = fill.avg_price
                except BinanceAPIError as err:
                    diag = ErrorDoctor.diagnose(err)
                    if diag.action not in (Action.REFRESH_POSITION, Action.ALREADY_DONE):
                        self.core.bus.emit("close_failed", f"{trade.symbol}: {diag.info.title}", "error",
                                           symbol=trade.symbol, code=diag.code, solution=diag.info.solution)
                        self.core.telegram.send(f"⚠️ <b>No se pudo cerrar</b> <code>{trade.symbol}</code>\n"
                                                f"[{diag.code}] {diag.info.title}\n💡 {diag.info.solution}")
                        return False
                    log.info("%s: la posición ya estaba cerrada en Binance (%s)", trade.symbol, diag.info.name)
                if partial:
                    trade.quantity = max(0.0, trade.quantity - qty)
                    self.save()
                    self.core.bus.emit("partial_close", f"Cierre parcial {trade.symbol}: {qty:g}", "info")
                    return True
                self._finalize(trade, reason, fill_price or close_price or self.core.market.price(trade.symbol)
                               or trade.entry_price)
                return True
        finally:
            self._closing.discard(key)

    def _finalize(self, trade: Trade, reason: str, close_price: float) -> None:
        trade.status = reason
        trade.close_price = close_price
        trade.close_time = utc_now_str()
        trade.closed_ts = time.time()
        trade.pnl_usdt = trade.realized_exchange - trade.fees_usdt if trade.realized_exchange else trade.price_pnl(close_price)
        trade.roi_pct = trade.pnl_usdt / trade.notional_usdt * 100 if trade.notional_usdt else 0.0
        self.trades.pop(trade.key, None)
        self.paper_map.pop(trade.paper_trade_id, None)
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
        return (
            f"{emoji} <b>POSICIÓN CERRADA — {label}</b>\n━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>Par:</b> <code>{trade.symbol}</code> {'🟢 LONG' if trade.direction == 'LONG' else '🔴 SHORT'}\n"
            f"💵 <b>Entrada:</b> <code>{trade.entry_price:,.8g}</code>\n"
            f"💵 <b>Salida:</b> <code>{trade.close_price:,.8g}</code>\n"
            f"{'💚' if trade.pnl_usdt >= 0 else '❗'} <b>PnL:</b> <code>{trade.pnl_usdt:+.4f} USDT</code> "
            f"(ROI {trade.roi_pct:+.2f}% · ROE {trade.roe_pct:+.2f}%)\n"
            f"⏱ {trade.open_time} → {trade.close_time}\n"
            f"📈 <b>Win rate:</b> <code>{wr}</code>\n🆔 #{trade.id} | Paper #{trade.paper_trade_id}"
        )

    async def close_symbol(self, symbol: str, direction: Optional[str], reason: str) -> bool:
        """Cierra una posición real sin trade local (excepto si es de un grid)."""
        if not symbol:
            return False
        positions = [p for p in self.core.account.positions_for(symbol)
                     if (direction is None or p.direction == direction.upper())]
        positions = [p for p in positions if not self.grid_owner(p.symbol, p.direction)]
        if not positions:
            log.info("close %s %s: no hay posición real que cerrar", symbol, direction or "")
            return False
        ok_any = False
        for pos in positions:
            async with self._lock(symbol):
                await self.orders.cancel_protection(symbol, pos.direction)
                rules = self.core.exinfo.get(symbol, self.core.market.price(symbol))
                side = "SELL" if pos.direction == "LONG" else "BUY"
                ctx = OrderContext(symbol, side, pos.direction, "close", rules, reduce_only=True)
                try:
                    await self.orders.market(ctx, D(fmt(pos.qty)), prefix="CX")
                    ok_any = True
                    self.core.bus.emit("close", f"{symbol} {pos.direction} cerrada ({reason})", "success")
                    self.core.telegram.send(f"🔄 <b>CIERRE {reason}</b> (sin trade local)\n<code>{symbol}</code> {pos.direction}")
                except BinanceAPIError as err:
                    diag = ErrorDoctor.diagnose(err)
                    self.core.bus.emit("close_failed", f"{symbol}: {diag.info.title}", "error")
        return ok_any

    async def close_all(self, reason: str) -> list[Trade]:
        targets = list(self.trades.values())
        results = await asyncio.gather(*(self.close_trade(t, reason) for t in targets), return_exceptions=True)
        closed = [t for t, ok in zip(targets, results) if ok is True]
        # Posiciones reales sin trade local (ni grid) también se cierran.
        tracked = {t.key for t in targets}
        for pos in list(self.core.account.positions.values()):
            if (pos.symbol, pos.direction) in tracked or self.grid_owner(pos.symbol, pos.direction):
                continue
            await self.close_symbol(pos.symbol, pos.direction, reason)
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
                    if owner_trade is not None and upd.status == "FILLED":
                        self._announce_open(owner_trade)
                if owner_trade is not None:
                    owner_trade.fees_usdt += upd.fee_usdt
                return
            if isinstance(owner, Trade):
                owner.fees_usdt += upd.fee_usdt
                owner.realized_exchange += upd.realized_pnl
                if owner.status != "OPEN" and upd.status == "FILLED" and owner.realized_exchange:
                    owner.pnl_usdt = owner.realized_exchange - owner.fees_usdt
                    owner.roi_pct = owner.pnl_usdt / owner.notional_usdt * 100 if owner.notional_usdt else 0.0
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
                if trade.key not in self._pending_external:
                    self._pending_external.add(trade.key)
                    asyncio.create_task(self._confirm_external_close(trade))
            elif abs(pos.qty - trade.quantity) > 1e-12 and trade.key not in self._closing:
                trade.quantity = pos.qty
                if pos.entry_price:
                    trade.entry_price = pos.entry_price
                self.save()

    async def _confirm_external_close(self, trade: Trade) -> None:
        try:
            await asyncio.sleep(1.5)
            if trade.key in self._closing or self.trades.get(trade.key) is not trade:
                return
            if self.core.account.position(trade.symbol, trade.direction) is not None:
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
            self._finalize(trade, reason, price)
        finally:
            self._pending_external.discard(trade.key)

    def reconcile_after_sync(self) -> None:
        """Tras leer posiciones reales: cierra localmente lo que ya no existe."""
        for trade in list(self.trades.values()):
            if trade.order_assumed or trade.key in self._closing:
                continue
            pos = self.core.account.position(trade.symbol, trade.direction)
            if pos is None:
                log.warning("Reconciliación: %s %s ya no existe en Binance", trade.symbol, trade.direction)
                self._finalize(trade, "EXTERNAL", self.core.market.price(trade.symbol) or trade.entry_price)
            elif abs(pos.qty - trade.quantity) > 1e-12:
                trade.quantity = pos.qty
                trade.entry_price = pos.entry_price or trade.entry_price

    # ── Acciones manuales ─────────────────────────────────────────────────
    def clear_history(self) -> int:
        n = len(self.closed)
        self.closed.clear()
        started = self.status["started_at"]
        self.status = _fresh_status()
        self.status["started_at"] = started
        self.status["last_signal_time"] = "Historial borrado"
        self.signal_log.clear()
        self.save()
        return n
