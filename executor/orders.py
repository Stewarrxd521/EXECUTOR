"""Envío de órdenes con autocorrección guiada por el ErrorDoctor.

Cada rechazo de Binance se diagnostica y, si el catálogo indica una
corrección automática (precisión, notional mínimo, positionSide, reduceOnly,
estado desconocido, banda de precio...), se aplica y se reintenta de forma
acotada. Todo queda registrado en el ``ErrorJournal`` con la nota de qué se
corrigió.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from .core import Core, new_client_id
from .errors import Action, BinanceAPIError, ErrorDoctor
from .exchange_info import SymbolRules
from .precision import D, floor_step, fmt, safe_float

log = logging.getLogger("executor.orders")

LEVERAGE_LADDER = [125, 100, 75, 50, 25, 20, 15, 10, 8, 5, 4, 3, 2, 1]
_MIN_NOTIONAL_RE = re.compile(r"no smaller than\s+([\d.]+)", re.I)


@dataclass
class OrderContext:
    symbol: str
    side: str  # BUY | SELL
    direction: str  # LONG | SHORT (posición afectada)
    purpose: str  # open | close | grid | limit | protect
    rules: SymbolRules
    market: bool = True
    reduce_only: bool = False
    desired_notional: float = 0.0
    ref_price: float = 0.0


@dataclass
class FillResult:
    qty: float = 0.0
    avg_price: float = 0.0
    order_ids: list = field(default_factory=list)
    client_ids: list = field(default_factory=list)
    position_side: str = "BOTH"
    step: Decimal = Decimal("1")
    fixes: list = field(default_factory=list)
    status: str = ""

    @property
    def ok(self) -> bool:
        return self.qty > 0

    @property
    def order_id(self) -> str:
        return str(self.order_ids[-1]) if self.order_ids else ""


class OrderExecutor:
    def __init__(self, core: Core):
        self.core = core
        self.client_owner: dict[str, object] = {}

    # ── Helpers ───────────────────────────────────────────────────────────
    def position_side(self, direction: str) -> str:
        return self.core.account.position_side_for(direction)

    def mark(self, symbol: str) -> float:
        return self.core.market.price(symbol)

    def _record(self, err: Exception, where: str, symbol: str, **ctx):
        diag = ErrorDoctor.diagnose(err, **ctx)
        entry = self.core.journal.record(diag, where, symbol)
        level = logging.ERROR if diag.info.severity in ("error", "critical") else logging.WARNING
        log.log(level, "%s %s: %s", where, symbol, diag.summary())
        return diag, entry

    def build_params(self, ctx: OrderContext, qty: Decimal, price: Optional[Decimal] = None,
                     tif: str = "GTC", client_id: Optional[str] = None, prefix: str = "X",
                     post_only: bool = False) -> dict:
        ps = self.position_side(ctx.direction)
        params = {
            "symbol": ctx.symbol,
            "side": ctx.side,
            "type": "MARKET" if ctx.market else "LIMIT",
            "quantity": fmt(qty),
            "positionSide": ps,
            "newClientOrderId": client_id or new_client_id(prefix),
        }
        if not ctx.market:
            params["price"] = fmt(price)
            params["timeInForce"] = "GTX" if post_only else tif
        if ctx.reduce_only and ps == "BOTH":
            params["reduceOnly"] = "true"
        return params

    # ── Envío con autocorrección ──────────────────────────────────────────
    def own(self, client_id: str, owner) -> None:
        """Asocia un clientOrderId a su dueño (trade/grid) para atribuir fills."""
        if owner is None:
            return
        self.client_owner[client_id] = owner
        while len(self.client_owner) > 5000:
            self.client_owner.pop(next(iter(self.client_owner)))

    async def submit(self, ctx: OrderContext, params: dict, where: str, max_fixes: int = 6,
                     owner=None) -> tuple[dict, list]:
        """Envía ``order.place`` aplicando las correcciones del doctor.

        Devuelve (respuesta, notas_de_corrección).
        """
        notes: list[str] = []
        entry = None
        fixes = notional_bumps = halvings = precision_rounds = 0
        band_fixed = False
        qty = D(params["quantity"])
        price = D(params["price"]) if "price" in params else None
        while True:
            self.own(params["newClientOrderId"], owner)
            try:
                result = await self.core.ws.place_order(**params)
                if entry is not None:
                    self.core.journal.mark_fixed(entry, "; ".join(notes) or "reintento exitoso")
                return result or {}, notes
            except BinanceAPIError as err:
                diag, entry = self._record(err, where, ctx.symbol, params=dict(params))
                fixes += 1
                if fixes > max_fixes:
                    raise
                action = diag.action
                rules = ctx.rules

                if action == Action.CHECK_STATUS:
                    await asyncio.sleep(1.0)  # dar tiempo a que el motor registre la orden
                    status, definitive = await self.lookup_order(ctx.symbol, client_id=params["newClientOrderId"])
                    if status and status.get("status") not in ("REJECTED", "EXPIRED", "CANCELED"):
                        notes.append("estado confirmado con order.status (no se duplicó la orden)")
                        self.core.journal.mark_fixed(entry, notes[-1])
                        return status, notes
                    if not definitive:
                        # No se pudo confirmar: reenviar podría duplicar la orden.
                        raise
                    params["newClientOrderId"] = new_client_id(params["newClientOrderId"].split("-")[0])
                    notes.append("orden no registrada en Binance; reenviada con nuevo clientOrderId")
                    continue
                if action in (Action.RETRY, Action.RESYNC_TIME):
                    await asyncio.sleep(0.3 * fixes)
                    notes.append("reintento tras error transitorio")
                    continue
                if action == Action.NEW_CLIENT_ID:
                    params["newClientOrderId"] = new_client_id(params["newClientOrderId"].split("-")[0])
                    notes.append("nuevo clientOrderId")
                    continue
                if action == Action.DROP_REDUCE_ONLY and "reduceOnly" in params:
                    params.pop("reduceOnly")
                    notes.append("reduceOnly retirado")
                    continue
                if action == Action.FLIP_POSITION_SIDE:
                    if params.get("positionSide") in ("LONG", "SHORT"):
                        params["positionSide"] = "BOTH"
                        if ctx.reduce_only:
                            params["reduceOnly"] = "true"
                        self.core.account.hedge_mode = False
                    else:
                        params["positionSide"] = ctx.direction
                        params.pop("reduceOnly", None)
                        self.core.account.hedge_mode = True
                    notes.append(f"positionSide → {params['positionSide']} (modo real de la cuenta)")
                    continue
                if action in (Action.FIX_PRECISION, Action.FIX_PRICE_TICK) and precision_rounds < 3:
                    precision_rounds += 1
                    coarsen_tick = action == Action.FIX_PRICE_TICK or (
                        not ctx.market and precision_rounds >= 2)
                    if coarsen_tick and price is not None:
                        rules = self.core.exinfo.learn_coarser_tick(ctx.symbol, rules)
                        price = rules.round_price(price, "down" if ctx.side == "BUY" else "up")
                        params["price"] = fmt(price)
                        notes.append(f"tickSize ajustado a {fmt(rules.tick_size)}")
                    else:
                        rules = self.core.exinfo.learn_coarser_qty(ctx.symbol, rules)
                        if ctx.reduce_only:
                            qty = floor_step(qty, rules.qty_step(ctx.market))
                        else:
                            ref = float(price) if price is not None else (self.mark(ctx.symbol) or ctx.ref_price)
                            new_qty = rules.qty_for_notional(ctx.desired_notional or float(qty) * ref, ref,
                                                             market=ctx.market)
                            overshoot = self.core.settings.max_notional_overshoot_x
                            if ctx.desired_notional and float(new_qty) * ref > ctx.desired_notional * overshoot:
                                notes.append(f"stepSize {fmt(rules.qty_step(ctx.market))} obligaría a un notional "
                                             f"> x{overshoot} del deseado: se aborta")
                                raise
                            qty = new_qty
                        if qty <= 0:
                            raise
                        params["quantity"] = fmt(qty)
                        notes.append(f"stepSize ajustado a {fmt(rules.qty_step(ctx.market))} → qty {fmt(qty)}")
                    ctx.rules = rules
                    continue
                if action in (Action.RAISE_NOTIONAL, Action.RAISE_QTY) and not ctx.reduce_only and notional_bumps < 2:
                    notional_bumps += 1
                    m = _MIN_NOTIONAL_RE.search(diag.msg)
                    if m:
                        rules = self.core.exinfo.learn_min_notional(ctx.symbol, m.group(1))
                        ctx.rules = rules
                    ref = float(price) if price is not None else (self.mark(ctx.symbol) or ctx.ref_price)
                    step = rules.qty_step(ctx.market)
                    target = rules.qty_for_notional(0, ref, market=ctx.market, buffer_pct=10.0,
                                                    min_notional_floor=self.core.settings.min_notional_usdt)
                    qty = max(qty + step, target)
                    params["quantity"] = fmt(qty)
                    notes.append(f"cantidad subida a {fmt(qty)} para cubrir el mínimo (≈{float(qty) * ref:.2f} USDT)")
                    continue
                if (action == Action.REDUCE_SIZE and self.core.settings.margin_auto_reduce
                        and not ctx.reduce_only and halvings < 2):
                    halvings += 1
                    step = rules.qty_step(ctx.market)
                    new_qty = floor_step(qty / 2, step)
                    ref = float(price) if price is not None else (self.mark(ctx.symbol) or ctx.ref_price)
                    if new_qty >= rules.qty_min(ctx.market) and float(new_qty) * ref >= float(rules.min_notional):
                        qty = new_qty
                        params["quantity"] = fmt(qty)
                        notes.append(f"margen insuficiente: cantidad reducida a {fmt(qty)}")
                        continue
                    raise
                if action == Action.CLAMP_PRICE and not band_fixed:
                    band_fixed = True
                    mark = self.mark(ctx.symbol) or ctx.ref_price
                    if not mark:
                        raise
                    if ctx.market:
                        # MARKET fuera de banda → LIMIT IOC dentro de la banda.
                        target = D(mark) * (D("1.005") if ctx.side == "BUY" else D("0.995"))
                        price = rules.clamp_to_band(rules.round_price(target, "up" if ctx.side == "BUY" else "down"), mark)
                        params.update({"type": "LIMIT", "timeInForce": "IOC", "price": fmt(price)})
                        notes.append(f"MARKET convertida a LIMIT IOC @ {fmt(price)} (banda PERCENT_PRICE)")
                    elif price is not None:
                        price = rules.clamp_to_band(price, mark)
                        params["price"] = fmt(price)
                        notes.append(f"precio ajustado a la banda permitida: {fmt(price)}")
                    continue
                if action == Action.ALREADY_DONE:
                    self.core.journal.mark_fixed(entry, "sin cambios necesarios")
                    return {"status": "ALREADY_DONE"}, notes
                raise

    async def lookup_order(self, symbol: str, order_id=None, client_id: Optional[str] = None) -> tuple[Optional[dict], bool]:
        """(orden | None, respuesta_definitiva). Definitiva = Binance respondió."""
        try:
            return await self.core.ws.order_status(symbol, order_id=order_id, client_id=client_id), True
        except BinanceAPIError as err:
            if err.code in (-2013, -2011):
                return None, True
            self._record(err, "order.status", symbol)
            return None, False

    async def order_status(self, symbol: str, order_id=None, client_id: Optional[str] = None) -> Optional[dict]:
        try:
            return await self.core.ws.order_status(symbol, order_id=order_id, client_id=client_id)
        except BinanceAPIError as err:
            if err.code in (-2013, -2011):
                return None
            self._record(err, "order.status", symbol)
            return None

    # ── Órdenes de mercado (con división por marketMaxQty) ────────────────
    async def market(self, ctx: OrderContext, qty: Decimal, prefix: str = "X", owner=None) -> FillResult:
        ctx.market = True
        rules = ctx.rules
        max_q = rules.qty_max(True)
        chunks = [qty]
        if max_q > 0 and qty > max_q:
            chunks = []
            remaining = qty
            while remaining > 0:
                part = min(remaining, max_q)
                chunks.append(part)
                remaining -= part
        fill = FillResult(step=rules.qty_step(True))
        notional = 0.0
        for part in chunks:
            params = self.build_params(ctx, part, prefix=prefix)
            result, notes = await self.submit(ctx, params, where=f"{ctx.purpose}.market", owner=owner)
            fill.fixes.extend(notes)
            if result.get("status") == "ALREADY_DONE":
                fill.status = "ALREADY_DONE"
                continue
            executed = safe_float(result.get("executedQty")) or (
                safe_float(result.get("origQty")) if result.get("status") in ("FILLED", "NEW") else 0.0)
            avg = safe_float(result.get("avgPrice")) or safe_float(result.get("price")) or self.mark(ctx.symbol)
            fill.status = result.get("status", "")
            if executed > 0:
                fill.qty += executed
                notional += executed * avg
            fill.order_ids.append(result.get("orderId", ""))
            fill.client_ids.append(params["newClientOrderId"])
            fill.position_side = params.get("positionSide", "BOTH")
            fill.step = ctx.rules.qty_step(True)
        fill.avg_price = notional / fill.qty if fill.qty else 0.0
        return fill

    async def limit(self, ctx: OrderContext, qty: Decimal, price: Decimal, tif: str = "GTC",
                    client_id: Optional[str] = None, prefix: str = "L", owner=None,
                    post_only: bool = False) -> tuple[dict, dict]:
        ctx.market = False
        params = self.build_params(ctx, qty, price=price, tif=tif, client_id=client_id, prefix=prefix,
                                   post_only=post_only)
        result, notes = await self.submit(ctx, params, where=f"{ctx.purpose}.limit", owner=owner)
        return result, params

    async def cancel(self, symbol: str, order_id=None, client_id: Optional[str] = None) -> bool:
        try:
            await self.core.ws.cancel_order(symbol, order_id=order_id, client_id=client_id)
            return True
        except BinanceAPIError as err:
            diag, entry = self._record(err, "order.cancel", symbol)
            if diag.action == Action.ALREADY_DONE:
                self.core.journal.mark_fixed(entry, "la orden ya no estaba activa")
                self.core.account.orders.pop(int(order_id or 0), None)
                return True
            return False

    async def cancel_algo(self, algo_id, symbol: str = "") -> bool:
        try:
            await self.core.ws.cancel_algo(algo_id=algo_id)
            self.core.account.algos.pop(int(algo_id), None)
            return True
        except BinanceAPIError as err:
            diag, entry = self._record(err, "algoOrder.cancel", symbol)
            if diag.action == Action.ALREADY_DONE:
                self.core.journal.mark_fixed(entry, "el algo order ya no estaba activo")
                self.core.account.algos.pop(int(algo_id), None)
                return True
            return False

    async def cancel_symbol_orders(self, symbol: str, position_side: Optional[str] = None,
                                   include_normal: bool = True, include_algo: bool = True,
                                   client_prefix: Optional[str] = None) -> int:
        """Cancela por WebSocket (una a una) las órdenes conocidas del símbolo."""
        jobs = []
        if include_normal:
            for o in self.core.account.orders_for(symbol):
                if position_side and o.position_side != position_side:
                    continue
                if client_prefix and not o.client_id.startswith(client_prefix):
                    continue
                jobs.append(self.cancel(symbol, order_id=o.id))
        if include_algo:
            for o in self.core.account.algos_for(symbol, position_side):
                jobs.append(self.cancel_algo(o.id, symbol))
        if not jobs:
            return 0
        results = await asyncio.gather(*jobs, return_exceptions=True)
        return sum(1 for r in results if r is True)

    # ── Leverage (único REST en el flujo de apertura, cacheado) ───────────
    async def ensure_leverage(self, symbol: str, target: int) -> int:
        rules = self.core.exinfo.get(symbol)
        if rules.max_leverage:
            target = min(target, rules.max_leverage)
        current = self.core.account.leverage.get(symbol)
        if current == target:
            return target
        ladder = [target] + [lv for lv in LEVERAGE_LADDER if lv < target]
        for lv in ladder:
            if current == lv:
                return lv
            try:
                await self.core.rest.set_leverage(symbol, lv)
                self.core.account.leverage[symbol] = lv
                if lv != target:
                    log.warning("Leverage %s: %sx rechazado, aplicado %sx", symbol, target, lv)
                return lv
            except BinanceAPIError as err:
                diag, entry = self._record(err, "leverage", symbol)
                if diag.action == Action.LOWER_LEVERAGE:
                    self.core.journal.mark_fixed(entry, f"se prueba un leverage menor que {lv}x")
                    continue
                log.warning("Leverage %s: error no relacionado con el valor (%s); se usa el actual (%sx)",
                            symbol, diag.info.title, current or "?")
                return current or target
        return current or target

    # ── TP / SL (Algo Order API por WebSocket) ────────────────────────────
    async def place_protection(self, symbol: str, direction: str, kind: str, trigger: float,
                               qty: Optional[float] = None) -> dict:
        kind = kind.upper()
        order_type = "TAKE_PROFIT_MARKET" if kind == "TP" else "STOP_MARKET"
        rules = self.core.exinfo.get(symbol, trigger)
        ps = self.position_side(direction)
        side = "SELL" if direction == "LONG" else "BUY"
        trig = rules.round_price(trigger, "nearest")
        # Reemplaza el TP/SL anterior del mismo tipo y lado.
        for o in self.core.account.algos_for(symbol, ps, {order_type}):
            await self.cancel_algo(o.id, symbol)
        params = {
            "symbol": symbol, "side": side, "type": order_type, "triggerPrice": fmt(trig),
            "workingType": "MARK_PRICE", "positionSide": ps, "clientAlgoId": new_client_id(kind),
        }
        if qty and qty > 0:
            params["quantity"] = fmt(rules.round_qty(qty, market=True, mode="nearest"))
            if ps == "BOTH":
                params["reduceOnly"] = "true"
        else:
            params["closePosition"] = "true"
        entry = None
        notes: list[str] = []
        for _ in range(4):
            try:
                result = await self.core.ws.place_algo(**params)
                self.core.account.register_algo_result(result)
                if entry is not None:
                    self.core.journal.mark_fixed(entry, "; ".join(notes))
                return result or {}
            except BinanceAPIError as err:
                diag, entry = self._record(err, f"{kind}.place", symbol)
                if diag.action == Action.FLIP_POSITION_SIDE:
                    if params["positionSide"] in ("LONG", "SHORT"):
                        params["positionSide"] = "BOTH"
                        if "quantity" in params:
                            params["reduceOnly"] = "true"
                    else:
                        params["positionSide"] = direction
                        params.pop("reduceOnly", None)
                    notes.append(f"positionSide → {params['positionSide']}")
                    continue
                if diag.action == Action.DROP_REDUCE_ONLY and "reduceOnly" in params:
                    params.pop("reduceOnly")
                    notes.append("reduceOnly retirado")
                    continue
                if diag.action == Action.FIX_PRICE_TICK:
                    rules = self.core.exinfo.learn_coarser_tick(symbol, rules)
                    params["triggerPrice"] = fmt(rules.round_price(trig))
                    notes.append(f"trigger redondeado a tick {fmt(rules.tick_size)}")
                    continue
                if diag.action == Action.USE_QUANTITY and params.get("closePosition"):
                    pos = self.core.account.position(symbol, direction)
                    if not pos:
                        raise
                    params.pop("closePosition")
                    params["quantity"] = fmt(rules.round_qty(pos.qty, market=True, mode="nearest"))
                    if params["positionSide"] == "BOTH":
                        params["reduceOnly"] = "true"
                    notes.append("closePosition → quantity + reduceOnly")
                    continue
                raise
        raise BinanceAPIError(0, f"no se pudo crear el {kind} de {symbol}")

    async def cancel_protection(self, symbol: str, direction: Optional[str] = None,
                                kinds: Optional[set] = None) -> int:
        ps = self.position_side(direction) if direction else None
        types = kinds or {"TAKE_PROFIT_MARKET", "STOP_MARKET", "TAKE_PROFIT", "STOP", "TRAILING_STOP_MARKET"}
        targets = self.core.account.algos_for(symbol, ps, types)
        results = await asyncio.gather(*(self.cancel_algo(o.id, symbol) for o in targets), return_exceptions=True)
        return sum(1 for r in results if r is True)
