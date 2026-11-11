"""Protección de funding: cubre la posición durante el cobro de un funding caro.

El funding se cobra sobre la posición que hay en el instante del cobro. En modo Hedge, un LONG y un SHORT del
mismo símbolo y la misma cantidad se compensan: uno paga exactamente lo que el otro recibe. Este módulo aprovecha
eso con cada símbolo que tiene posición real:

1. Poco antes de la hora de funding (``FUNDING_HEDGE_LEAD_MS``) mira el funding que se va a cobrar. Si la
   posición neta (LONG − SHORT) lo **paga** y es caro (``FUNDING_HEDGE_MIN_RATE_PCT``; con cobros seguidos,
   ``FUNDING_HEDGE_FREQ_RATE_PCT``), abre en el lado contrario exactamente esa cantidad neta.
2. Mantiene la cobertura hasta que Binance avisa del cobro (``ACCOUNT_UPDATE`` con motivo ``FUNDING_FEE``) y,
   como pronto, ``FUNDING_HEDGE_LAG_MS`` después de la hora. Binance no garantiza el instante exacto del cobro
   (puede retrasarse unos segundos), así que sin aviso espera ``FUNDING_HEDGE_MAX_HOLD_MS``.
3. Cierra la cobertura. Se repite en cada cobro mientras la posición siga abierta.

La cobertura es un lote más del libro (``funding:<hora del cobro>``): se abre y se cierra con las mismas órdenes
por lote que usan los bots, así que nunca toca la cantidad de otro ``trade_id``, queda en el historial con su
PnL y comisiones, y si el executor se reinicia con una cobertura abierta, se cierra al arrancar.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from dataclasses import asdict, dataclass
from typing import Callable, Optional

from .core import Core
from .errors import BinanceAPIError
from .precision import fmt, safe_float
from .tasks import spawn
from .trading import FUNDING_BOT, Trade, TradeManager, lot_ref

log = logging.getLogger("executor.funding")

BOT = FUNDING_BOT               # «bot» dueño de los lotes de cobertura: su lote es «funding:<tid>»
ARM_MS = 30_000                 # un cobro se prepara cuando falta este tiempo
MIN_LEAD_MS = 150               # con menos margen que este ya no se abre (la orden no llegaría antes del cobro)
FEE_SETTLE_MS = 300             # tras el aviso de cobro de ESTE símbolo se espera esto antes de cerrar
INTERVALS_REFRESH_S = 1800.0    # cada cuánto se releen los intervalos de funding
DEFAULT_INTERVAL_H = 8.0
CLOSE_RETRY_S = (1.0, 2.0, 5.0, 10.0, 30.0)


@dataclass
class Plan:
    """Lo que se haría con un símbolo en su próximo cobro."""
    symbol: str
    funding_ms: int
    rate_pct: float            # funding que se va a cobrar, en %
    interval_h: float
    threshold_pct: float
    net_qty: float             # LONG − SHORT (sin coberturas propias)
    hedge: bool = False
    direction: str = ""        # lado de la cobertura
    qty: float = 0.0
    reason: str = ""


@dataclass
class HedgeRecord:
    symbol: str
    direction: str
    qty: float
    funding_ms: int
    rate_pct: float
    state: str = "abriendo"    # abriendo | abierta | cerrando | cerrada | fallida
    opened_ms: int = 0
    closed_ms: int = 0
    saved_usdt: float = 0.0    # funding que habría pagado la posición sin cubrir (estimado)
    detail: str = ""


class FundingGuard:
    def __init__(self, core: Core, trades: TradeManager, grid_owner: Callable[[str, str], bool]):
        self.core = core
        self.settings = core.settings
        self.trades = trades
        self.grid_owner = grid_owner
        self.intervals: dict[str, float] = {}          # símbolo -> horas entre cobros (fundingInfo / observado)
        self._intervals_ts = 0.0
        self._seen_funding: dict[str, int] = {}        # símbolo -> último next_funding_ms visto
        self._handled: set[tuple[str, int]] = set()    # (símbolo, cobro) ya preparado
        self._active: dict[tuple[str, int], HedgeRecord] = {}
        self._fees: dict[str, int] = {}                # símbolo ("" = sin símbolo) -> hora del último aviso de cobro
        self._fee_signal = asyncio.Event()
        self._stray: set[tuple[str, str, str]] = set()
        self._one_way_warned = False
        self._tasks: set[asyncio.Task] = set()
        self.recent: deque[HedgeRecord] = deque(maxlen=30)

    # ── Configuración ─────────────────────────────────────────────────────
    @property
    def enabled(self) -> bool:
        return bool(self.settings.funding_hedge and self.settings.has_credentials)

    def interval_h(self, symbol: str) -> float:
        return self.intervals.get(symbol.upper(), DEFAULT_INTERVAL_H)

    def threshold_pct(self, interval_h: float) -> float:
        """|funding| en % desde el que se cubre: más bajo cuando se cobra seguido."""
        s = self.settings
        if s.funding_hedge_freq_hours > 0 and interval_h <= s.funding_hedge_freq_hours + 1e-9:
            return min(s.funding_hedge_min_rate_pct, s.funding_hedge_freq_rate_pct)
        return s.funding_hedge_min_rate_pct

    # ── Tareas ────────────────────────────────────────────────────────────
    def _spawn(self, coro) -> asyncio.Task:
        task = spawn(coro)   # su error queda en el log
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def run(self) -> None:
        """Bucle principal: vigila los cobros próximos de cada símbolo con posición."""
        await self.trades.ready.wait()
        while True:
            try:
                if self.enabled:
                    if time.time() - self._intervals_ts > INTERVALS_REFRESH_S:
                        self._intervals_ts = time.time()
                        self._spawn(self.refresh_intervals())
                    self._tick()
                    self._sweep_strays()
            except Exception:
                log.exception("Protección de funding: fallo en la vigilancia")
            await asyncio.sleep(0.25)

    async def stop(self) -> None:
        for task in list(self._tasks):
            task.cancel()

    async def refresh_intervals(self) -> None:
        try:
            rows = await self.core.rest.funding_info()
        except Exception as exc:  # sin intervalos se asume 8 h (y se aprende de los cobros observados)
            log.warning("Funding: no se pudieron leer los intervalos de cobro (%s)", exc)
            return
        fresh = {}
        for row in rows:
            hours = safe_float(row.get("fundingIntervalHours"))
            if row.get("symbol") and hours > 0:
                fresh[str(row["symbol"]).upper()] = hours
        if fresh:
            self.intervals.update(fresh)

    # ── Decisión ──────────────────────────────────────────────────────────
    def _own_qty(self, symbol: str, direction: str) -> float:
        """Cantidad de coberturas propias abiertas en ese lado (no cuenta como posición del usuario)."""
        trade = self.trades.get_trade(symbol, direction)
        if trade is None or trade.symbol != symbol or trade.direction != direction:
            return 0.0
        return sum(q for k, q in trade.lots.items() if k.startswith(BOT + ":"))

    def _symbols(self) -> set[str]:
        return {p.symbol for p in self.core.account.positions.values() if p.qty > 0}

    def fast_symbols(self) -> set[str]:
        """Símbolos con un cobro en los próximos 2 minutos: su precio y su funding se piden cada segundo."""
        if not self.enabled:
            return set()
        now = self.core.clock.now_ms()
        out = set()
        for symbol in self._symbols():
            info = self.core.market.marks.get(symbol)
            if info is not None and 0 < info.next_funding_ms - now <= 120_000:
                out.add(symbol)
        return out

    def plan(self, symbol: str) -> Optional[Plan]:
        """Qué se haría con ``symbol`` en su próximo cobro (None: sin datos de funding)."""
        symbol = symbol.upper()
        info = self.core.market.marks.get(symbol)
        if info is None or not info.next_funding_ms or time.time() - info.ts > 90:
            return None
        acct = self.core.account
        long_pos, short_pos = acct.position(symbol, "LONG"), acct.position(symbol, "SHORT")
        long_qty = (long_pos.qty if long_pos else 0.0) - self._own_qty(symbol, "LONG")
        short_qty = (short_pos.qty if short_pos else 0.0) - self._own_qty(symbol, "SHORT")
        net = long_qty - short_qty
        interval = self.interval_h(symbol)
        plan = Plan(symbol, info.next_funding_ms, info.funding_rate * 100, interval, self.threshold_pct(interval), net)
        rules = self.core.exinfo.get(symbol, info.mark)
        step = float(rules.qty_step(True))
        qty = float(rules.round_qty(abs(net), True, "nearest"))
        pays = (net > 0 and info.funding_rate > 0) or (net < 0 and info.funding_rate < 0)
        if not acct.is_hedge:
            plan.reason = "la cuenta está en modo One-way (la cobertura necesita modo Hedge)"
        elif self.grid_owner(symbol, "LONG") or self.grid_owner(symbol, "SHORT"):
            plan.reason = "el símbolo tiene un grid activo"
        elif qty < step / 2:
            plan.reason = "sin posición neta (LONG y SHORT ya se compensan)"
        elif abs(plan.rate_pct) < plan.threshold_pct - 1e-9:
            plan.reason = f"funding {plan.rate_pct:+.4f}% por debajo del umbral {plan.threshold_pct:g}%"
        elif not pays and self.settings.funding_hedge_only_paying:
            plan.reason = f"funding {plan.rate_pct:+.4f}% a favor: la posición lo cobra"
        elif qty * info.mark < float(rules.min_notional):
            plan.reason = (f"la posición ({qty * info.mark:.2f} USDT) es menor que la orden mínima de Binance "
                           f"({fmt(rules.min_notional)} USDT)")
        else:
            plan.hedge, plan.qty = True, qty
            plan.direction = "SHORT" if net > 0 else "LONG"
            plan.reason = (f"funding {plan.rate_pct:+.4f}% (umbral {plan.threshold_pct:g}%, cobro cada "
                           f"{interval:g} h)")
        return plan

    def _tick(self) -> None:
        now = self.core.clock.now_ms()
        for symbol in self._symbols():
            info = self.core.market.marks.get(symbol)
            if info is None or not info.next_funding_ms:
                continue
            funding_ms = info.next_funding_ms
            seen = self._seen_funding.get(symbol)
            if seen and funding_ms > seen:
                hours = (funding_ms - seen) / 3_600_000
                if hours in (1.0, 2.0, 4.0, 8.0):   # intervalo observado entre dos cobros seguidos
                    self.intervals[symbol] = hours
            self._seen_funding[symbol] = funding_ms
            left = funding_ms - now
            if MIN_LEAD_MS < left <= ARM_MS and (symbol, funding_ms) not in self._handled:
                self._handled.add((symbol, funding_ms))
                self._spawn(self._cycle(symbol, funding_ms))
        if len(self._handled) > 2000:
            self._handled = {k for k in self._handled if k[1] > now - 3_600_000}

    # ── Aviso de cobro (user stream) ──────────────────────────────────────
    def on_account_update(self, evt: dict) -> None:
        """``ACCOUNT_UPDATE`` con motivo ``FUNDING_FEE``: Binance ya cobró (campo ``S``: el símbolo; en posiciones
        aisladas va en ``P``; si no viene, no se sabe de qué símbolo es)."""
        a = evt.get("a") or {}
        if a.get("m") != "FUNDING_FEE":
            return
        symbol = str(a.get("S") or "").upper()
        if not symbol:
            symbols = {str(p.get("s") or "").upper() for p in a.get("P") or []}
            symbol = symbols.pop() if len(symbols) == 1 else ""
        self._fees[symbol] = int(evt.get("E") or evt.get("T") or self.core.clock.now_ms())
        self._fee_signal.set()

    def _fee_seen(self, symbol: str, funding_ms: int) -> Optional[bool]:
        """True: cobro de este símbolo avisado · False: avisado sin símbolo · None: aún no."""
        floor = funding_ms - 2000
        if self._fees.get(symbol, 0) >= floor:
            return True
        if self._fees.get("", 0) >= floor:
            return False
        return None

    # ── Ciclo de un cobro ─────────────────────────────────────────────────
    async def _sleep_until(self, target_ms: int) -> None:
        while True:
            left = target_ms - self.core.clock.now_ms()
            if left <= 0:
                return
            await asyncio.sleep(min(left, 200) / 1000)

    async def _cycle(self, symbol: str, funding_ms: int) -> None:
        s = self.settings
        await self._sleep_until(funding_ms - s.funding_hedge_lead_ms)
        if not self.enabled:
            return
        plan = self.plan(symbol)
        if plan is None or plan.funding_ms != funding_ms:
            return
        if not plan.hedge:
            if not self.core.account.is_hedge and not self._one_way_warned and \
                    abs(plan.rate_pct) >= plan.threshold_pct:
                self._one_way_warned = True
                self._alert(f"{symbol}: funding {plan.rate_pct:+.4f}% sin cubrir — {plan.reason}", "warning")
            log.debug("Funding %s: no se cubre (%s)", symbol, plan.reason)
            return
        if funding_ms - self.core.clock.now_ms() < MIN_LEAD_MS:
            self._alert(f"{symbol}: funding {plan.rate_pct:+.4f}% sin cubrir — ya no daba tiempo a abrir la "
                        f"cobertura antes del cobro", "error")
            return
        tid = funding_ms // 1000
        ref = lot_ref(BOT, tid)
        key = Trade.lot_key(ref)
        rec = HedgeRecord(symbol, plan.direction, plan.qty, funding_ms, plan.rate_pct)
        self._active[(symbol, funding_ms)] = rec
        self.recent.append(rec)
        info = self.core.market.marks.get(symbol)
        rec.saved_usdt = plan.qty * (info.mark if info else 0.0) * abs(plan.rate_pct) / 100
        try:
            log.warning("Funding %s: %s — se cubre con %s %s", symbol, plan.reason, plan.direction, fmt(plan.qty))
            trade = None
            try:
                trade = await self.trades.open_trade(
                    symbol, plan.direction, quantity=plan.qty, paper_trade_id=tid, source="funding", bot=BOT,
                    exact_qty=True, allow_assumed=False)
            except BinanceAPIError as err:
                rec.detail = str(err)
            got = trade.lots.get(key, 0.0) if trade is not None else 0.0
            rec.opened_ms = self.core.clock.now_ms()
            if got <= 0:
                rec.state = "fallida"
                rec.detail = rec.detail or "Binance no aceptó la orden de cobertura (ver pestaña Errores)"
                self._alert(f"{symbol}: NO se pudo cubrir el funding {plan.rate_pct:+.4f}% "
                            f"({plan.direction} {fmt(plan.qty)}) — {rec.detail}", "error")
                return
            rec.state, rec.qty = "abierta", got
            late = rec.opened_ms - funding_ms
            note = f" ⚠️ abierta {late} ms DESPUÉS de la hora de cobro" if late > 0 else \
                f" {-late} ms antes del cobro"
            self._alert(f"{symbol}: posición cubierta ({plan.direction} {fmt(got)}){note} — "
                        f"funding {plan.rate_pct:+.4f}% ≈ {rec.saved_usdt:.4f} USDT", "success", telegram=True)
            await self._wait_charged(symbol, funding_ms)
            rec.state = "cerrando"
            await self._close(symbol, plan.direction, ref, rec)
        finally:
            self._active.pop((symbol, funding_ms), None)

    async def _wait_charged(self, symbol: str, funding_ms: int) -> None:
        """Espera a que Binance cobre: como pronto ``lag`` después de la hora; después, hasta el aviso de cobro
        (o ``max_hold`` si no llega)."""
        s = self.settings
        await self._sleep_until(funding_ms + s.funding_hedge_lag_ms)
        deadline = funding_ms + max(s.funding_hedge_max_hold_ms, s.funding_hedge_lag_ms)
        while self.core.clock.now_ms() < deadline:
            seen = self._fee_seen(symbol, funding_ms)
            if seen is True:
                await asyncio.sleep(FEE_SETTLE_MS / 1000)
                return
            if seen is False:
                # Aviso sin símbolo: pudo ser el cobro de otro par; se da un margen más.
                await asyncio.sleep(s.funding_hedge_lag_ms / 1000)
                return
            self._fee_signal.clear()
            left = (deadline - self.core.clock.now_ms()) / 1000
            try:
                await asyncio.wait_for(self._fee_signal.wait(), timeout=max(0.01, min(left, 0.5)))
            except asyncio.TimeoutError:
                pass
        log.warning("Funding %s: Binance no avisó del cobro en %d ms; se cierra la cobertura", symbol,
                    s.funding_hedge_max_hold_ms)

    async def _close(self, symbol: str, direction: str, ref: str, rec: Optional[HedgeRecord] = None) -> None:
        """Cierra el lote de cobertura (solo ese lote); insiste hasta que ya no esté."""
        key = Trade.lot_key(ref)
        attempt = 0
        while True:
            trade = self.trades.get_trade(symbol, direction)
            if trade is None or trade.direction != direction or trade.lots.get(key, 0.0) <= 0:
                break
            ok = False
            try:
                ok = await self.trades.close_trade(trade, "FUNDING", closed_by="funding", lot=ref, alert=attempt > 0)
            except Exception:
                log.exception("Funding %s: error al cerrar la cobertura", symbol)
            trade = self.trades.get_trade(symbol, direction)
            if ok and (trade is None or trade.lots.get(key, 0.0) <= 0):
                break
            if attempt in (0, 4) or attempt % 20 == 19:
                self._alert(f"{symbol}: la cobertura de funding ({direction}) sigue abierta; se reintenta el "
                            f"cierre", "error", telegram=True)
            await asyncio.sleep(CLOSE_RETRY_S[min(attempt, len(CLOSE_RETRY_S) - 1)])
            attempt += 1
        if rec is not None:
            rec.state, rec.closed_ms = "cerrada", self.core.clock.now_ms()
            held = rec.closed_ms - rec.opened_ms
            self._alert(f"{symbol}: cobertura de funding cerrada ({direction} {fmt(rec.qty)}, {held} ms abierta)",
                        "info")

    # ── Coberturas huérfanas (reinicio a mitad de un cobro, cierre fallido) ─
    def _sweep_strays(self) -> None:
        now = self.core.clock.now_ms()
        hold = max(self.settings.funding_hedge_max_hold_ms, self.settings.funding_hedge_lag_ms)
        for trade in list(self.trades.trades.values()):
            for key in list(trade.lots):
                if not key.startswith(BOT + ":"):
                    continue
                tid = int(safe_float(key.split(":", 1)[1]))
                mark = (trade.symbol, trade.direction, key)
                in_cycle = any(sym == trade.symbol and ms // 1000 == tid for sym, ms in self._active)
                if in_cycle or mark in self._stray or now < tid * 1000 + hold:
                    continue
                self._stray.add(mark)
                log.warning("Funding %s: cobertura %s de un cobro anterior sigue abierta; se cierra", trade.symbol,
                            trade.direction)
                self._spawn(self._close_stray(trade.symbol, trade.direction, key))

    async def _close_stray(self, symbol: str, direction: str, key: str) -> None:
        try:
            await self._close(symbol, direction, key)
        finally:
            self._stray.discard((symbol, direction, key))

    # ── Avisos y vista ────────────────────────────────────────────────────
    def _alert(self, text: str, level: str, telegram: bool = False) -> None:
        getattr(log, "error" if level == "error" else "warning" if level == "warning" else "info")("Funding %s", text)
        self.core.bus.emit("funding", f"Funding {text}", level)
        if telegram or level == "error":
            icon = {"error": "🚨", "warning": "⚠️", "success": "🛡"}.get(level, "ℹ️")
            self.core.telegram.send(f"{icon} <b>Funding</b> {text}")

    def view(self) -> dict:
        s = self.settings
        plans = [p for p in (self.plan(sym) for sym in sorted(self._symbols())) if p is not None]
        return {
            "enabled": self.enabled,
            "min_rate_pct": s.funding_hedge_min_rate_pct,
            "freq_hours": s.funding_hedge_freq_hours,
            "freq_rate_pct": s.funding_hedge_freq_rate_pct,
            "lead_ms": s.funding_hedge_lead_ms,
            "lag_ms": s.funding_hedge_lag_ms,
            "max_hold_ms": s.funding_hedge_max_hold_ms,
            "only_paying": s.funding_hedge_only_paying,
            "symbols": [asdict(p) for p in plans],
            "recent": [asdict(r) for r in list(self.recent)[::-1]],
        }
