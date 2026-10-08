"""Bots Grid para Futuros USDⓈ-M (LONG, SHORT y NEUTRAL).

Modelo por *celdas*: entre cada par de niveles consecutivos hay una celda con
UNA orden límite viva.

* Celda LONG: compra en el nivel inferior → al llenarse vende en el superior
  (ganancia = un grid) → vuelve a comprar en el inferior.
* Celda SHORT: vende en el nivel superior → al llenarse recompra en el
  inferior → vuelve a vender en el superior.

Modos:

* LONG: todas las celdas LONG. Las celdas por encima del precio arrancan con
  inventario comprado a mercado (como el grid de Binance).
* SHORT: todas SHORT; las celdas por debajo del precio arrancan vendidas.
* NEUTRAL: celdas LONG bajo el precio y SHORT encima, sin posición inicial.

Las órdenes se colocan por la WS API y los fills llegan por el User Data
Stream (sin polling). El estado se persiste y al reiniciar se reconcilia con
``order.status`` (WebSocket).
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from typing import Optional

from .account import OrderUpdate
from .core import Core
from .errors import BinanceAPIError, ErrorDoctor
from .orders import OrderContext, OrderExecutor
from .precision import D, ceil_step, decimals_of, floor_step, fmt, parse_bool, safe_float
from .storage import JsonStore

log = logging.getLogger("executor.grid")

MODES = ("LONG", "SHORT", "NEUTRAL")
SPACINGS = ("ARITHMETIC", "GEOMETRIC")
ACTIVE = ("PENDING", "STARTING", "RUNNING")
MAKER_FEE = 0.0002


class GridError(ValueError):
    """Configuración de grid inválida (mensaje listo para el usuario)."""

    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


@dataclass
class GridConfig:
    symbol: str
    lower: float
    upper: float
    grids: int
    mode: str = "NEUTRAL"
    spacing: str = "ARITHMETIC"
    investment: float = 0.0
    leverage: int = 5
    stop_loss: float = 0.0
    take_profit: float = 0.0
    trigger_price: float = 0.0
    close_on_stop: bool = True
    post_only: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> "GridConfig":
        try:
            cfg = cls(
                symbol=str(data.get("symbol", "")).upper().strip(),
                lower=float(data.get("lower", 0)),
                upper=float(data.get("upper", 0)),
                grids=int(float(data.get("grids", 0))),
                mode=str(data.get("mode", "NEUTRAL")).upper(),
                spacing=str(data.get("spacing", "ARITHMETIC")).upper(),
                investment=float(data.get("investment", 0)),
                leverage=int(float(data.get("leverage", 5))),
                stop_loss=float(data.get("stop_loss") or 0),
                take_profit=float(data.get("take_profit") or 0),
                trigger_price=float(data.get("trigger_price") or 0),
                close_on_stop=parse_bool(data.get("close_on_stop"), True),
                post_only=parse_bool(data.get("post_only"), False),
            )
        except (TypeError, ValueError) as exc:
            raise GridError(f"parámetros numéricos inválidos: {exc}") from exc
        if not cfg.symbol:
            raise GridError("falta el símbolo")
        if cfg.mode not in MODES:
            raise GridError("mode debe ser LONG, SHORT o NEUTRAL")
        if cfg.spacing not in SPACINGS:
            raise GridError("spacing debe ser ARITHMETIC o GEOMETRIC")
        if cfg.lower <= 0 or cfg.upper <= cfg.lower:
            raise GridError("el precio superior debe ser mayor que el inferior (y ambos > 0)")
        if cfg.grids < 2:
            raise GridError("se necesitan al menos 2 grids")
        if cfg.investment <= 0:
            raise GridError("la inversión (margen USDT) debe ser > 0")
        if not 1 <= cfg.leverage <= 125:
            raise GridError("leverage entre 1 y 125")
        return cfg


@dataclass
class GridCell:
    index: int
    low: str
    high: str
    qty: str
    kind: str  # LONG | SHORT
    state: str = "WAIT_OPEN"  # WAIT_OPEN | HOLD
    order_id: int = 0
    client_id: str = ""
    order_side: str = ""
    order_price: str = ""
    open_price: float = 0.0
    trades: int = 0
    profit: float = 0.0
    retries: int = 0
    seq: int = 0
    last_fill_id: int = 0
    last_error: str = ""

    def pending(self) -> tuple[str, str]:
        """(side, price) de la orden que la celda debe tener viva."""
        if self.kind == "LONG":
            return ("BUY", self.low) if self.state == "WAIT_OPEN" else ("SELL", self.high)
        return ("SELL", self.high) if self.state == "WAIT_OPEN" else ("BUY", self.low)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class GridBot:
    id: str
    config: GridConfig
    levels: list[str] = field(default_factory=list)
    cells: list[GridCell] = field(default_factory=list)
    qty_per_grid: str = "0"
    status: str = "PENDING"
    hedge: bool = True
    created_ts: float = field(default_factory=time.time)
    started_ts: float = 0.0
    stopped_ts: float = 0.0
    stop_reason: str = ""
    start_price: float = 0.0
    trigger_above: bool = True
    grid_profit: float = 0.0
    fees: float = 0.0
    matched: int = 0
    close_pnl: float = 0.0
    leverage_applied: int = 0
    last_error: str = ""

    @property
    def symbol(self) -> str:
        return self.config.symbol

    def position_side(self, kind: str) -> str:
        return kind if self.hedge else "BOTH"

    def inventory(self) -> tuple[float, float]:
        """(qty long, qty short) que el grid mantiene abiertas."""
        lng = sum(float(c.qty) for c in self.cells if c.state == "HOLD" and c.kind == "LONG")
        sht = sum(float(c.qty) for c in self.cells if c.state == "HOLD" and c.kind == "SHORT")
        return lng, sht

    def unrealized(self, mark: float) -> float:
        if not mark:
            return 0.0
        total = 0.0
        for c in self.cells:
            if c.state != "HOLD" or not c.open_price:
                continue
            q = float(c.qty)
            total += (mark - c.open_price) * q if c.kind == "LONG" else (c.open_price - mark) * q
        return total

    def summary(self, mark: float) -> dict:
        upnl = self.unrealized(mark) if self.status in ACTIVE else 0.0
        total = self.grid_profit + upnl + self.close_pnl - self.fees
        runtime = (self.stopped_ts or time.time()) - (self.started_ts or self.created_ts)
        days = max(runtime / 86400, 1e-9)
        lng, sht = self.inventory()
        cfg = self.config
        return {
            "id": self.id,
            "symbol": cfg.symbol,
            "mode": cfg.mode,
            "spacing": cfg.spacing,
            "lower": cfg.lower,
            "upper": cfg.upper,
            "grids": cfg.grids,
            "investment": cfg.investment,
            "leverage": self.leverage_applied or cfg.leverage,
            "stop_loss": cfg.stop_loss,
            "take_profit": cfg.take_profit,
            "trigger_price": cfg.trigger_price,
            "status": self.status,
            "stop_reason": self.stop_reason,
            "qty_per_grid": self.qty_per_grid,
            "grid_profit": self.grid_profit,
            "unrealized": upnl,
            "fees": self.fees,
            "close_pnl": self.close_pnl,
            "total_pnl": total,
            "total_pct": total / cfg.investment * 100 if cfg.investment else 0.0,
            "apr": (self.grid_profit / cfg.investment) * (365 / days) * 100 if cfg.investment and runtime > 600 else None,
            "matched": self.matched,
            "inventory_long": lng,
            "inventory_short": sht,
            "open_orders": sum(1 for c in self.cells if c.order_id or c.client_id),
            "runtime_s": int(runtime),
            "created_ts": self.created_ts,
            "mark": mark,
            "last_error": self.last_error,
            "in_range": cfg.lower <= mark <= cfg.upper if mark else None,
        }

    def detail(self, mark: float) -> dict:
        data = self.summary(mark)
        data["levels"] = self.levels
        data["cells"] = [c.to_dict() for c in self.cells]
        return data

    def to_dict(self) -> dict:
        data = asdict(self)
        data["config"] = asdict(self.config)
        return data

    @classmethod
    def from_dict(cls, data: dict) -> "GridBot":
        data = dict(data)
        cfg = GridConfig(**data.pop("config"))
        cells = [GridCell(**c) for c in data.pop("cells", [])]
        names = set(cls.__dataclass_fields__)
        bot = cls(id=data.pop("id"), config=cfg, cells=cells, **{k: v for k, v in data.items() if k in names})
        return bot


def compute_levels(cfg: GridConfig, tick: Decimal) -> list[Decimal]:
    lower, upper, n = D(cfg.lower), D(cfg.upper), cfg.grids
    raw: list[Decimal] = []
    if cfg.spacing == "ARITHMETIC":
        step = (upper - lower) / n
        raw = [lower + step * k for k in range(n + 1)]
    else:
        ratio = (upper / lower) ** (Decimal(1) / Decimal(n))
        raw = [lower * ratio ** k for k in range(n + 1)]
    levels = [floor_step(p, tick) if i == n else ceil_step(p, tick) if i == 0 else floor_step(p + tick / 2, tick)
              for i, p in enumerate(raw)]
    for a, b in zip(levels, levels[1:]):
        if b <= a:
            raise GridError(f"el rango es demasiado estrecho para {n} grids con tickSize {fmt(tick)}; "
                            f"reduce la cantidad de grids o amplía el rango")
    return levels


class GridManager:
    def __init__(self, core: Core, orders: OrderExecutor):
        self.core = core
        self.orders = orders
        self.bots: dict[str, GridBot] = {}
        self.store = JsonStore(core.settings.data_dir / "grids.json", self._serialize)
        self._sem = asyncio.Semaphore(core.settings.grid_order_concurrency)
        self._locks: dict[str, asyncio.Lock] = {}
        self._by_order: dict[int, tuple[str, int]] = {}
        self._by_client: dict[str, tuple[str, int]] = {}
        self._cancelling: set[str] = set()
        self._auto_stopping: set[str] = set()
        self.trade_owner = lambda symbol, direction: False

    # ── Persistencia ──────────────────────────────────────────────────────
    def _serialize(self) -> dict:
        return {"bots": [b.to_dict() for b in self.bots.values()]}

    def load(self) -> None:
        data = self.store.load({}) or {}
        for raw in data.get("bots", []):
            try:
                bot = GridBot.from_dict(raw)
            except Exception as exc:
                log.error("Grid: no se pudo restaurar un bot (%s)", exc)
                continue
            self.bots[bot.id] = bot
            for cell in bot.cells:
                self._index(bot, cell)
        if self.bots:
            log.info("Grid: %d bots restaurados (%d activos)", len(self.bots),
                     sum(1 for b in self.bots.values() if b.status in ACTIVE))

    def save(self) -> None:
        self.store.schedule()

    def _lock(self, bot_id: str) -> asyncio.Lock:
        return self._locks.setdefault(bot_id, asyncio.Lock())

    def _index(self, bot: GridBot, cell: GridCell) -> None:
        if cell.order_id:
            self._by_order[cell.order_id] = (bot.id, cell.index)
        if cell.client_id:
            self._by_client[cell.client_id] = (bot.id, cell.index)

    # ── Consultas ─────────────────────────────────────────────────────────
    def active_bots(self) -> list[GridBot]:
        return [b for b in self.bots.values() if b.status in ACTIVE or b.status == "STOPPING"]

    def owns(self, symbol: str, direction: str) -> bool:
        for b in self.active_bots():
            if b.symbol != symbol:
                continue
            if not b.hedge or b.config.mode in ("NEUTRAL", direction):
                return True
        return False

    def list(self) -> list[dict]:
        out = []
        for b in sorted(self.bots.values(), key=lambda x: x.created_ts, reverse=True):
            out.append(b.summary(self.core.market.price(b.symbol)))
        return out

    def detail(self, bot_id: str) -> dict:
        bot = self.bots.get(bot_id)
        if bot is None:
            raise GridError("grid no encontrado", 404)
        return bot.detail(self.core.market.price(bot.symbol))

    # ── Diseño del grid ───────────────────────────────────────────────────
    def preview(self, data: dict) -> dict:
        cfg = GridConfig.from_dict(data)
        mark = self.core.market.price(cfg.symbol)
        rules = self.core.exinfo.get(cfg.symbol, mark or (cfg.lower + cfg.upper) / 2)
        max_cells = min(self.core.settings.grid_max_levels, max(2, rules.max_num_orders - 10))
        if cfg.grids > max_cells:
            raise GridError(f"máximo {max_cells} grids para {cfg.symbol} (límite de órdenes abiertas)")
        levels = compute_levels(cfg, rules.tick_size)
        lv = [float(x) for x in levels]
        mids = [(a + b) / 2 for a, b in zip(lv, lv[1:])]
        avg = sum(mids) / len(mids)
        total_notional = cfg.investment * cfg.leverage
        qty = floor_step(D(total_notional) / (D(cfg.grids) * D(avg)), rules.step_size)
        min_qty = max(rules.min_qty, ceil_step(rules.min_notional * D("1.02") / D(lv[0]), rules.step_size))
        min_investment = float(min_qty) * avg * cfg.grids / cfg.leverage
        gaps = [(b - a) / a * 100 for a, b in zip(lv, lv[1:])]
        warnings, errors = [], []
        if qty < min_qty:
            errors.append(f"inversión insuficiente: con {cfg.grids} grids y {cfg.leverage}x el mínimo es "
                          f"≈{min_investment:.2f} USDT (cantidad mínima por grid {fmt(min_qty)})")
        if min(gaps) < MAKER_FEE * 2 * 100 * 2:
            warnings.append("el beneficio por grid es muy bajo frente a las comisiones; usa menos grids o un rango mayor")
        if mark and not cfg.lower <= mark <= cfg.upper:
            warnings.append("el precio actual está fuera del rango: el grid quedará en un solo lado")
        if cfg.leverage > 20:
            warnings.append("leverage alto: el riesgo de liquidación aumenta mucho en grids")
        if rules.max_leverage and cfg.leverage > rules.max_leverage:
            warnings.append(f"{cfg.symbol} admite como máximo {rules.max_leverage}x; se usará ese valor")
        if rules.source != "snapshot":
            warnings.append("símbolo fuera del exchangeInfo local: se usan reglas aproximadas")
        if self.trade_owner(cfg.symbol, "LONG") or self.trade_owner(cfg.symbol, "SHORT"):
            errors.append(f"{cfg.symbol} tiene operaciones por señal abiertas: ciérralas antes de crear un grid")
        if any(b.symbol == cfg.symbol for b in self.active_bots()):
            errors.append(f"ya hay un grid activo en {cfg.symbol}")
        hold = self._initial_hold_count(cfg, levels, mark) if mark else 0
        return {
            "config": asdict(cfg),
            "mark": mark,
            "levels": [fmt(x) for x in levels],
            "qty_per_grid": fmt(qty),
            "notional_per_grid": float(qty) * avg,
            "total_notional": float(qty) * avg * cfg.grids,
            "margin_required": float(qty) * avg * cfg.grids / cfg.leverage,
            "min_investment": min_investment,
            "profit_per_grid_pct": [round(min(gaps), 4), round(max(gaps), 4)],
            "net_profit_per_grid_pct": [round(min(gaps) - MAKER_FEE * 200, 4), round(max(gaps) - MAKER_FEE * 200, 4)],
            "initial_position_qty": float(qty) * hold,
            "tick_size": fmt(rules.tick_size),
            "step_size": fmt(rules.step_size),
            "price_decimals": decimals_of(rules.tick_size),
            "warnings": warnings,
            "errors": errors,
        }

    @staticmethod
    def _initial_hold_count(cfg: GridConfig, levels: list[Decimal], mark: float) -> int:
        lv = [float(x) for x in levels]
        if cfg.mode == "LONG":
            return sum(1 for a in lv[:-1] if a >= mark)
        if cfg.mode == "SHORT":
            return sum(1 for b in lv[1:] if b <= mark)
        return 0

    def _build_cells(self, cfg: GridConfig, levels: list[Decimal], qty: Decimal, mark: float) -> list[GridCell]:
        cells = []
        for i, (a, b) in enumerate(zip(levels, levels[1:])):
            low, high = float(a), float(b)
            if cfg.mode == "LONG":
                kind, state = "LONG", ("HOLD" if low >= mark else "WAIT_OPEN")
            elif cfg.mode == "SHORT":
                kind, state = "SHORT", ("HOLD" if high <= mark else "WAIT_OPEN")
            else:
                if high <= mark:
                    kind = "LONG"
                elif low >= mark:
                    kind = "SHORT"
                else:
                    kind = "LONG" if (mark - low) > (high - mark) else "SHORT"
                state = "WAIT_OPEN"
            cells.append(GridCell(index=i, low=fmt(a), high=fmt(b), qty=fmt(qty), kind=kind, state=state))
        return cells

    # ── Ciclo de vida ─────────────────────────────────────────────────────
    async def create(self, data: dict) -> GridBot:
        preview = self.preview(data)
        if preview["errors"]:
            raise GridError("; ".join(preview["errors"]))
        cfg = GridConfig.from_dict(data)
        mark = preview["mark"]
        if not mark:
            raise GridError(f"no hay precio de mercado para {cfg.symbol}")
        bot = GridBot(id=secrets.token_hex(3), config=cfg, levels=preview["levels"],
                      qty_per_grid=preview["qty_per_grid"], hedge=self.core.account.is_hedge,
                      start_price=mark)
        if cfg.trigger_price > 0:
            bot.trigger_above = cfg.trigger_price > mark
        self.bots[bot.id] = bot
        self.save()
        self.core.bus.emit("grid", f"Grid {cfg.symbol} {cfg.mode} creado ({cfg.grids} grids)", "success", bot=bot.id)
        if cfg.trigger_price > 0:
            log.info("Grid %s: esperando trigger %s", bot.id, cfg.trigger_price)
            return bot
        asyncio.create_task(self.start(bot))
        return bot

    async def start(self, bot: GridBot) -> None:
        async with self._lock(bot.id):
            if bot.status not in ("PENDING",):
                return
            cfg = bot.config
            bot.status = "STARTING"
            bot.started_ts = time.time()
            mark = self.core.market.price(cfg.symbol)
            bot.start_price = mark or bot.start_price
            rules = self.core.exinfo.get(cfg.symbol, mark)
            levels = [D(x) for x in bot.levels]
            bot.cells = self._build_cells(cfg, levels, D(bot.qty_per_grid), bot.start_price)
            bot.hedge = self.core.account.is_hedge
            bot.leverage_applied = await self.orders.ensure_leverage(cfg.symbol, cfg.leverage)

            hold_long = [c for c in bot.cells if c.state == "HOLD" and c.kind == "LONG"]
            hold_short = [c for c in bot.cells if c.state == "HOLD" and c.kind == "SHORT"]
            for group, side, kind in ((hold_long, "BUY", "LONG"), (hold_short, "SELL", "SHORT")):
                if not group:
                    continue
                total = sum((D(c.qty) for c in group), Decimal(0))
                ctx = OrderContext(cfg.symbol, side, kind, "grid", rules, market=True,
                                   desired_notional=float(total) * bot.start_price, ref_price=bot.start_price)
                try:
                    fill = await self.orders.market(ctx, total, prefix=f"G{bot.id}i", owner=("grid-init", bot.id))
                except BinanceAPIError as err:
                    diag = ErrorDoctor.diagnose(err)
                    bot.status = "ERROR"
                    bot.last_error = diag.summary()
                    self.save()
                    self.core.bus.emit("grid", f"Grid {cfg.symbol}: no se pudo abrir la posición inicial — "
                                               f"{diag.info.title}", "error", bot=bot.id)
                    return
                for c in group:
                    c.open_price = fill.avg_price or bot.start_price
            await self._place_all(bot)
            bot.status = "RUNNING"
            self.save()
            log.info("Grid %s %s %s en marcha: %d celdas, qty/grid %s", bot.id, cfg.symbol, cfg.mode,
                     len(bot.cells), bot.qty_per_grid)
            self.core.bus.emit("grid", f"Grid {cfg.symbol} en marcha ({len(bot.cells)} órdenes)", "success", bot=bot.id)
            self.core.telegram.send(
                f"🤖 <b>Grid iniciado</b> <code>{cfg.symbol}</code> {cfg.mode}\n"
                f"Rango {cfg.lower:g} – {cfg.upper:g} · {cfg.grids} grids · {cfg.spacing.lower()}\n"
                f"Inversión {cfg.investment:g} USDT · {bot.leverage_applied}x · qty/grid {bot.qty_per_grid}")

    async def _place_all(self, bot: GridBot) -> None:
        await asyncio.gather(*(self._place_cell(bot, c) for c in bot.cells))

    async def _place_cell(self, bot: GridBot, cell: GridCell) -> None:
        if bot.status not in ("STARTING", "RUNNING"):
            return
        cfg = bot.config
        side, price = cell.pending()
        rules = self.core.exinfo.get(cfg.symbol, float(price))
        ctx = OrderContext(cfg.symbol, side, cell.kind, "grid", rules, market=False,
                           desired_notional=float(cell.qty) * float(price), ref_price=float(price))
        cell.seq += 1
        client_id = f"G{bot.id}-{cell.index}-{cell.seq}"
        async with self._sem:
            try:
                result, params = await self.orders.limit(ctx, D(cell.qty), D(price), client_id=client_id,
                                                         owner=("grid", bot.id, cell.index), post_only=cfg.post_only)
            except BinanceAPIError as err:
                diag = ErrorDoctor.diagnose(err)
                cell.last_error = diag.summary()
                cell.retries += 1
                bot.last_error = f"celda {cell.index}: {diag.info.title}"
                self.save()
                if cell.retries <= 5 and diag.retryable:
                    asyncio.create_task(self._retry_cell(bot, cell, 3.0 * cell.retries))
                return
        cell.order_id = int(result.get("orderId") or 0)
        cell.client_id = params.get("newClientOrderId", client_id)
        cell.order_side, cell.order_price = side, price
        cell.retries = 0
        cell.last_error = ""
        self._index(bot, cell)
        self.save()
        if result.get("status") == "FILLED":
            # Se llenó al instante (cruzó el libro): procesar como fill.
            await self._on_cell_filled(bot, cell, safe_float(result.get("avgPrice")) or float(price), cell.order_id)

    async def _retry_cell(self, bot: GridBot, cell: GridCell, delay: float) -> None:
        await asyncio.sleep(delay)
        async with self._lock(bot.id):
            if bot.status == "RUNNING" and not cell.order_id:
                await self._place_cell(bot, cell)

    async def _on_cell_filled(self, bot: GridBot, cell: GridCell, fill_price: float, order_id: int) -> None:
        """Transición de la celda tras un fill. Debe llamarse con el lock del bot."""
        if order_id and order_id == cell.last_fill_id:
            return  # el mismo fill ya se procesó (respuesta + evento)
        cell.last_fill_id = order_id or cell.last_fill_id
        self._by_order.pop(cell.order_id, None)
        self._by_client.pop(cell.client_id, None)
        cell.order_id, cell.client_id = 0, ""
        q = float(cell.qty)
        if cell.state == "WAIT_OPEN":
            cell.state = "HOLD"
            cell.open_price = fill_price
        else:
            profit = (fill_price - cell.open_price) * q if cell.kind == "LONG" else (cell.open_price - fill_price) * q
            cell.profit += profit
            cell.trades += 1
            bot.grid_profit += profit
            bot.matched += 1
            cell.state = "WAIT_OPEN"
            cell.open_price = 0.0
            self.core.bus.emit("grid_fill", f"Grid {bot.symbol}: {profit:+.4f} USDT (celda {cell.index})",
                               "info", bot=bot.id)
        self.save()
        await self._place_cell(bot, cell)

    async def stop(self, bot_id: str, close_position: Optional[bool] = None, reason: str = "MANUAL") -> GridBot:
        bot = self.bots.get(bot_id)
        if bot is None:
            raise GridError("grid no encontrado", 404)
        if bot.status in ("STOPPED", "ERROR") and not any(c.order_id for c in bot.cells):
            bot.status = "STOPPED"
            return bot
        close = bot.config.close_on_stop if close_position is None else close_position
        async with self._lock(bot.id):
            prev = bot.status
            bot.status = "STOPPING"
            self._cancelling.add(bot.id)
            try:
                jobs = []
                for cell in bot.cells:
                    if cell.order_id or cell.client_id:
                        jobs.append(self._cancel_cell(bot, cell))
                await asyncio.gather(*jobs, return_exceptions=True)
                mark = self.core.market.price(bot.symbol)
                if close and prev != "PENDING":
                    bot.close_pnl = bot.unrealized(mark)
                    await self._close_inventory(bot)
                    for c in bot.cells:
                        if c.state == "HOLD":
                            c.state = "WAIT_OPEN"
                bot.status = "STOPPED"
                bot.stopped_ts = time.time()
                bot.stop_reason = reason
                self.save()
            finally:
                self._cancelling.discard(bot.id)
        s = bot.summary(self.core.market.price(bot.symbol))
        self.core.bus.emit("grid", f"Grid {bot.symbol} detenido ({reason}) — PnL {s['total_pnl']:+.4f} USDT",
                           "info", bot=bot.id)
        self.core.telegram.send(f"⏹ <b>Grid detenido</b> <code>{bot.symbol}</code> ({reason})\n"
                                f"Beneficio de grid: {bot.grid_profit:+.4f} USDT · ciclos {bot.matched}\n"
                                f"PnL total: {s['total_pnl']:+.4f} USDT ({s['total_pct']:+.2f}%)")
        return bot

    async def _cancel_cell(self, bot: GridBot, cell: GridCell) -> None:
        async with self._sem:
            ok = await self.orders.cancel(bot.symbol, order_id=cell.order_id or None,
                                          client_id=None if cell.order_id else cell.client_id)
        if ok:
            self._by_order.pop(cell.order_id, None)
            self._by_client.pop(cell.client_id, None)
            cell.order_id, cell.client_id = 0, ""

    async def _close_inventory(self, bot: GridBot) -> None:
        lng, sht = bot.inventory()
        rules = self.core.exinfo.get(bot.symbol, self.core.market.price(bot.symbol))
        jobs = []
        if bot.hedge:
            for qty, kind in ((lng, "LONG"), (sht, "SHORT")):
                pos = self.core.account.position(bot.symbol, kind)
                qty = min(qty, pos.qty) if pos else 0.0
                if qty > 0:
                    jobs.append((kind, qty))
        else:
            net = lng - sht
            pos = self.core.account.position(bot.symbol)
            if pos and net:
                jobs.append(("LONG" if net > 0 else "SHORT", min(abs(net), pos.qty)))
        for kind, qty in jobs:
            side = "SELL" if kind == "LONG" else "BUY"
            ctx = OrderContext(bot.symbol, side, kind, "close", rules, reduce_only=True)
            try:
                await self.orders.market(ctx, rules.round_qty(qty, market=True, mode="nearest"), prefix=f"G{bot.id}x")
            except BinanceAPIError as err:
                diag = ErrorDoctor.diagnose(err)
                bot.last_error = f"cierre: {diag.info.title}"
                self.core.bus.emit("grid", f"Grid {bot.symbol}: no se pudo cerrar la posición — {diag.info.title}",
                                   "error", bot=bot.id)

    def delete(self, bot_id: str) -> None:
        bot = self.bots.get(bot_id)
        if bot is None:
            raise GridError("grid no encontrado", 404)
        if bot.status in ACTIVE or bot.status == "STOPPING":
            raise GridError("detén el grid antes de eliminarlo")
        del self.bots[bot_id]
        self.save()

    # ── Eventos ───────────────────────────────────────────────────────────
    def _lookup(self, upd: OrderUpdate) -> tuple[Optional[GridBot], Optional[GridCell]]:
        owner = self.orders.client_owner.get(upd.client_id)
        ref = None
        if isinstance(owner, tuple) and owner and owner[0] == "grid":
            ref = (owner[1], owner[2])
        ref = ref or self._by_client.get(upd.client_id) or self._by_order.get(upd.order_id)
        if ref is None:
            return None, None
        bot = self.bots.get(ref[0])
        if bot is None or ref[1] >= len(bot.cells):
            return None, None
        return bot, bot.cells[ref[1]]

    def on_order_update(self, upd: OrderUpdate) -> bool:
        owner = self.orders.client_owner.get(upd.client_id)
        if isinstance(owner, tuple) and owner and owner[0] == "grid-init":
            bot = self.bots.get(owner[1])
            if bot is not None:
                bot.fees += upd.fee_usdt
            return True
        bot, cell = self._lookup(upd)
        if bot is None or cell is None:
            return False
        if upd.is_fill:
            bot.fees += upd.fee_usdt
        if not cell.order_id and upd.order_id and upd.status in ("NEW", "PARTIALLY_FILLED"):
            cell.order_id = upd.order_id
            cell.client_id = upd.client_id
            self._index(bot, cell)
        if upd.status == "FILLED":
            asyncio.create_task(self._handle_fill(bot, cell, upd))
        elif upd.status in ("CANCELED", "EXPIRED", "EXPIRED_IN_MATCH") and bot.id not in self._cancelling:
            if bot.status == "RUNNING":
                asyncio.create_task(self._handle_external_cancel(bot, cell, upd))
        return True

    async def _handle_external_cancel(self, bot: GridBot, cell: GridCell, upd: OrderUpdate) -> None:
        async with self._lock(bot.id):
            if bot.status != "RUNNING" or bot.id in self._cancelling or upd.order_id != cell.order_id:
                return
            log.warning("Grid %s: orden de la celda %d quedó %s fuera del bot; se recoloca",
                        bot.id, cell.index, upd.status)
            self._by_order.pop(cell.order_id, None)
            self._by_client.pop(cell.client_id, None)
            cell.order_id, cell.client_id = 0, ""
        asyncio.create_task(self._retry_cell(bot, cell, 2.0))

    async def _handle_fill(self, bot: GridBot, cell: GridCell, upd: OrderUpdate) -> None:
        async with self._lock(bot.id):
            if bot.status not in ("RUNNING", "STARTING"):
                return
            if upd.order_id == cell.last_fill_id:
                return
            if cell.order_id and upd.order_id != cell.order_id:
                return
            await self._on_cell_filled(bot, cell, upd.avg_price or upd.last_price or float(cell.pending()[1]),
                                       upd.order_id)

    def on_tick(self) -> None:
        """Se llama con cada lote de mark prices: triggers, SL y TP."""
        for bot in list(self.bots.values()):
            if bot.status not in ("PENDING", "RUNNING"):
                continue
            price = self.core.market.mark(bot.symbol)
            if not price:
                continue
            cfg = bot.config
            if bot.status == "PENDING" and cfg.trigger_price > 0:
                hit = price >= cfg.trigger_price if bot.trigger_above else price <= cfg.trigger_price
                if hit:
                    log.info("Grid %s: trigger %s alcanzado (precio %s)", bot.id, cfg.trigger_price, price)
                    asyncio.create_task(self.start(bot))
                continue
            if bot.status != "RUNNING":
                continue
            if bot.id in self._auto_stopping:
                continue
            ref = bot.start_price or price
            reason = ""
            if cfg.stop_loss > 0 and ((ref > cfg.stop_loss and price <= cfg.stop_loss) or
                                      (ref < cfg.stop_loss and price >= cfg.stop_loss)):
                reason = "STOP_LOSS"
            elif cfg.take_profit > 0 and ((ref < cfg.take_profit and price >= cfg.take_profit) or
                                          (ref > cfg.take_profit and price <= cfg.take_profit)):
                reason = "TAKE_PROFIT"
            if reason:
                log.warning("Grid %s %s: %s alcanzado (precio %s)", bot.id, bot.symbol, reason, price)
                self._auto_stopping.add(bot.id)
                asyncio.create_task(self._auto_stop(bot, reason))

    async def _auto_stop(self, bot: GridBot, reason: str) -> None:
        try:
            await self.stop(bot.id, close_position=True, reason=reason)
        finally:
            self._auto_stopping.discard(bot.id)

    async def reconcile(self) -> None:
        """Tras arrancar o reconectar: compara cada celda con Binance (WS)."""
        for bot in list(self.bots.values()):
            if bot.status not in ("RUNNING", "STARTING"):
                continue
            if bot.status == "STARTING" and not any(c.order_id for c in bot.cells):
                bot.status = "ERROR"
                bot.last_error = "el bot se interrumpió durante el arranque; revísalo y vuelve a crearlo"
                continue
            bot.status = "RUNNING"
            async with self._lock(bot.id):
                await asyncio.gather(*(self._reconcile_cell(bot, c) for c in bot.cells), return_exceptions=True)
        self.save()

    async def _reconcile_cell(self, bot: GridBot, cell: GridCell) -> None:
        if not cell.order_id and not cell.client_id:
            await self._place_cell(bot, cell)
            return
        async with self._sem:
            st, definitive = await self.orders.lookup_order(bot.symbol, order_id=cell.order_id or None,
                                                            client_id=None if cell.order_id else cell.client_id)
        if not definitive:
            return  # sin respuesta clara: se reintentará en la próxima reconciliación
        status = (st or {}).get("status")
        if status in ("NEW", "PARTIALLY_FILLED"):
            if st.get("orderId"):
                cell.order_id = int(st["orderId"])
                self._index(bot, cell)
            return
        if status == "FILLED":
            await self._on_cell_filled(bot, cell, safe_float(st.get("avgPrice")) or float(cell.pending()[1]),
                                       int(st.get("orderId") or 0))
            return
        self._by_order.pop(cell.order_id, None)
        cell.order_id, cell.client_id = 0, ""
        await self._place_cell(bot, cell)
