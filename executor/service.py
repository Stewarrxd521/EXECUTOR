"""Servicio principal: conecta streams, cuenta, trading, grids y dashboard."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from typing import Any, Awaitable, Callable, Optional

from . import __version__
from .account import AccountState
from .binance_api import BinanceWsApi, RestClient, ServerClock
from .config import BUNDLED_EXCHANGE_INFO, DEFAULT_SIGNAL_SECRET, Settings
from .core import Core
from .errors import BinanceAPIError, ErrorDoctor, ErrorJournal
from .exchange_info import ExchangeInfo, apply_brackets, parse_exchange_info
from .grid import GridManager
from .notifier import EventBus, TelegramNotifier
from .orders import OrderContext, OrderExecutor
from .precision import D, fmt, safe_float
from .streams import MarketData, UserDataStream
from .trading import TradeManager

log = logging.getLogger("executor.service")


class LogBuffer(logging.Handler):
    """Últimas líneas de log para la pestaña Sistema del dashboard."""

    def __init__(self, maxlen: int = 400):
        super().__init__(level=logging.INFO)
        self.lines: deque[dict] = deque(maxlen=maxlen)
        self.seq = 0
        self.setFormatter(logging.Formatter("%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self.seq += 1
            self.lines.append({"id": self.seq, "ts": record.created, "level": record.levelname,
                               "name": record.name.replace("executor.", ""), "msg": self.format(record)[:600]})
        except Exception:  # pragma: no cover
            pass


class CommandError(Exception):
    pass


class ExecutorService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.started_ts = time.time()
        clock = ServerClock()
        ws = BinanceWsApi(settings.ws_api_url, settings.api_key, settings.api_secret, clock, settings.recv_window_ms)
        rest = RestClient(settings.rest_url, settings.api_key, settings.api_secret, clock, settings.proxy_urls,
                          settings.recv_window_ms, settings.rest_proxy_all)
        exinfo = ExchangeInfo(settings.data_dir, BUNDLED_EXCHANGE_INFO, settings.min_notional_usdt)
        market = MarketData(settings.stream_base_url, clock, on_contract_info=exinfo.apply_contract_info)
        self.core = Core(
            settings=settings, clock=clock, ws=ws, rest=rest, account=AccountState(settings.hedge_mode_hint),
            market=market, exinfo=exinfo, journal=ErrorJournal(), bus=EventBus(),
            telegram=TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id),
        )
        self.orders = OrderExecutor(self.core)
        self.trades = TradeManager(self.core, self.orders)
        self.grids = GridManager(self.core, self.orders)
        self.trades.grid_owner = self.grids.owns
        self.grids.trade_owner = lambda s, d: self.trades.get_trade(s, d) is not None
        self.user = UserDataStream(settings.stream_base_url, ws, clock, self._on_user_event, self._on_user_connect)
        market.on_tick(self._on_market_tick)
        self.logs = LogBuffer()
        logging.getLogger().addHandler(self.logs)
        self.history: dict[str, deque] = {}
        self.watch: dict[str, float] = {}
        self._tasks: list[asyncio.Task] = []
        self._balance_dirty = False
        self._last_balance = 0.0
        self.exinfo_refreshing = False

    # ── Ciclo de vida ─────────────────────────────────────────────────────
    async def start(self) -> None:
        s = self.settings
        s.data_dir.mkdir(parents=True, exist_ok=True)
        self.core.exinfo.load()
        self.trades.load()
        self.grids.load()
        self.core.telegram.start()
        self.core.market.start()

        if s.signal_secret == DEFAULT_SIGNAL_SECRET:
            log.warning("SIGNAL_SECRET usa el valor por defecto: cámbialo para proteger /signal y el dashboard")
        if not s.proxy_urls:
            log.warning("PROXY_URLS no configurado: las pocas llamadas REST saldrán con la IP directa del servidor")

        if not len(self.core.exinfo) and s.exchange_info_bootstrap:
            self._spawn(self._bootstrap_exinfo())

        if s.has_credentials:
            try:
                await self.sync_account("arranque")
            except Exception as exc:
                diag = ErrorDoctor.diagnose(exc)
                self.core.journal.record(diag, "sync.arranque")
                log.error("No se pudo leer la cuenta al arrancar: %s", diag.summary())
            self.user.start()
            if s.seed_open_orders_rest:
                self._spawn(self.seed_open_orders())
            self._spawn(self._balance_loop())
            self._spawn(self._verify_loop())
        else:
            log.critical("BINANCE_API_KEY / BINANCE_API_SECRET no configuradas: solo datos de mercado y dashboard")
        if s.exchange_info_refresh_h > 0:
            self._spawn(self._exinfo_loop())
        self._spawn(self._history_loop())

        acct = self.core.account.summary(self.core.market.marks)
        self.core.telegram.send(
            f"⚡ <b>Futures Executor v{__version__} iniciado</b> [{s.env_label}]\n"
            f"💰 Disponible: <code>{acct['available']:.2f} USDT</code>\n"
            f"⚡ Leverage por defecto: <code>{s.leverage}x</code> · Modo {'Hedge' if acct['hedge_mode'] else 'One-way'}\n"
            f"📡 Órdenes y cuenta: WebSocket API · Precios: stream global\n"
            f"📚 exchangeInfo local: {len(self.core.exinfo)} símbolos · 🤖 Grids activos: {len(self.grids.active_bots())}"
        )

    def _spawn(self, coro: Awaitable) -> None:
        self._tasks.append(asyncio.create_task(coro))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        self.trades.store.save_now()
        self.grids.store.save_now()
        await self.user.stop()
        await self.core.market.stop()
        await self.core.telegram.stop()
        await self.core.ws.close()
        await self.core.rest.close()

    # ── Sincronización de cuenta (WS API) ─────────────────────────────────
    async def sync_account(self, reason: str) -> None:
        positions = await self.core.ws.positions()
        self.core.account.apply_positions_snapshot(positions)
        await self.refresh_balance(force=True)
        self.trades.reconcile_after_sync()
        acct = self.core.account
        log.info("Cuenta sincronizada (%s): %d posiciones, modo %s, %d leverages en caché",
                 reason, len(acct.positions), "Hedge" if acct.is_hedge else "One-way", len(acct.leverage))

    async def refresh_balance(self, force: bool = False) -> None:
        if not force and time.time() - self._last_balance < 3:
            self._balance_dirty = True
            return
        self._last_balance = time.time()
        self._balance_dirty = False
        try:
            self.core.account.apply_balances(await self.core.ws.balances())
        except BinanceAPIError as err:
            self.orders._record(err, "balance", "")

    async def seed_open_orders(self) -> None:
        """Lectura inicial (REST, una vez) de órdenes y algo orders abiertas."""
        try:
            orders, algos = await asyncio.gather(self.core.rest.open_orders(), self.core.rest.open_algo_orders())
            self.core.account.seed_orders(orders)
            self.core.account.seed_algos(algos)
            log.info("Órdenes abiertas iniciales: %d normales, %d TP/SL", len(orders), len(algos))
        except Exception as exc:
            diag = ErrorDoctor.diagnose(exc)
            self.core.journal.record(diag, "seed.open_orders")
            log.warning("No se pudieron leer las órdenes abiertas por REST (%s); se completarán con eventos WS",
                        diag.info.title)

    async def _on_user_connect(self) -> None:
        try:
            await self.sync_account("user stream conectado")
            await self.grids.reconcile()
        except Exception as exc:
            self.core.journal.record(ErrorDoctor.diagnose(exc), "sync.reconnect")

    async def _balance_loop(self) -> None:
        while True:
            await asyncio.sleep(2)
            if self._balance_dirty or time.time() - self._last_balance > self.settings.balance_poll_s:
                await self.refresh_balance(force=True)

    async def _verify_loop(self) -> None:
        """Verificación de baja frecuencia por si se perdiera algún evento."""
        while True:
            await asyncio.sleep(max(300, self.settings.position_poll_s * 10))
            try:
                if not self.user.conn.connected:
                    await self.sync_account("verificación")
            except Exception as exc:
                self.core.journal.record(ErrorDoctor.diagnose(exc), "sync.verify")

    async def _exinfo_loop(self) -> None:
        while True:
            await asyncio.sleep(600)
            if self.core.exinfo.age_hours() >= self.settings.exchange_info_refresh_h:
                await self.refresh_exchange_info("refresco programado")

    async def _bootstrap_exinfo(self) -> None:
        """Genera el snapshot inicial, reintentando con espera creciente."""
        delay = 30.0
        while not len(self.core.exinfo):
            try:
                await self.refresh_exchange_info("bootstrap")
                return
            except Exception:
                log.info("exchangeInfo: nuevo intento de bootstrap en %.0fs", delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 1800)

    async def refresh_exchange_info(self, reason: str) -> dict:
        if self.exinfo_refreshing:
            raise CommandError("ya hay una actualización en curso")
        self.exinfo_refreshing = True
        try:
            payload = await self.core.rest.exchange_info()
            rules = parse_exchange_info(payload)
            if not rules:
                raise CommandError("la respuesta de exchangeInfo no contiene símbolos")
            source = "fapi/v1/exchangeInfo"
            if self.settings.has_credentials:
                try:
                    if apply_brackets(rules, await self.core.rest.leverage_brackets()):
                        source += " + leverageBracket"
                except Exception as exc:
                    log.info("exchangeInfo: sin brackets de leverage (%s)", exc)
            self.core.exinfo.replace_all(rules, source)
            self.core.bus.emit("exinfo", f"exchangeInfo actualizado ({len(rules)} símbolos, {reason})", "success")
            return self.core.exinfo.stats()
        except BinanceAPIError as err:
            diag = ErrorDoctor.diagnose(err)
            self.core.journal.record(diag, "exchangeInfo")
            log.error("No se pudo descargar exchangeInfo (%s): %s", reason, diag.summary())
            raise
        finally:
            self.exinfo_refreshing = False

    # ── Eventos ───────────────────────────────────────────────────────────
    async def _on_user_event(self, evt: dict) -> None:
        kind = evt.get("e")
        acct = self.core.account
        if kind == "ACCOUNT_UPDATE":
            keys = acct.apply_account_update(evt)
            self.trades.on_positions_changed(keys)
            self._balance_dirty = True
        elif kind == "ORDER_TRADE_UPDATE":
            upd = acct.apply_order_update(evt)
            if not self.grids.on_order_update(upd):
                self.trades.on_order_update(upd)
            if upd.is_fill and upd.status == "FILLED":
                if upd.orig_type in ("TAKE_PROFIT_MARKET", "STOP_MARKET", "TAKE_PROFIT", "STOP"):
                    label = "TP" if upd.orig_type.startswith("TAKE") else "SL"
                    self.core.bus.emit("fill", f"{label} ejecutado: {upd.symbol} @ {upd.avg_price:g}",
                                       "success" if label == "TP" else "warning", symbol=upd.symbol)
                elif upd.orig_type == "LIQUIDATION":
                    self.core.bus.emit("fill", f"LIQUIDACIÓN en {upd.symbol}", "error", symbol=upd.symbol)
            self._balance_dirty = True
        elif kind == "ALGO_UPDATE":
            o = acct.apply_algo_update(evt)
            if o.get("X") == "REJECTED" or o.get("rm"):
                self.core.bus.emit("algo", f"{o.get('o')} {o.get('s')} rechazado: {o.get('rm', '')}", "error")
        elif kind == "ACCOUNT_CONFIG_UPDATE":
            acct.apply_config_update(evt)
        elif kind == "MARGIN_CALL":
            self.core.bus.emit("margin_call", "⚠️ MARGIN CALL: el margen de la cuenta está en riesgo", "error")
            self.core.telegram.send("🚨 <b>MARGIN CALL</b> — revisa tus posiciones y margen")
        elif kind == "CONDITIONAL_ORDER_TRIGGER_REJECT":
            info = evt.get("or", {})
            self.core.bus.emit("algo", f"Orden condicional {info.get('s', '')} rechazada al dispararse: "
                                       f"{info.get('r', '')}", "error")

    def _on_market_tick(self) -> None:
        self.trades.refresh_marks()
        self.grids.on_tick()

    async def _history_loop(self) -> None:
        """Historial de mark price (1/s) de los símbolos observados (gráfico)."""
        while True:
            await asyncio.sleep(1)
            now = time.time()
            wanted = {s for s, ts in self.watch.items() if now - ts < 120}
            wanted |= {p.symbol for p in self.core.account.positions.values()}
            wanted |= {b.symbol for b in self.grids.active_bots()}
            for sym in wanted:
                price = self.core.market.mark(sym)
                if price:
                    self.history.setdefault(sym, deque(maxlen=1800)).append((int(now), price))
            for sym in list(self.history):
                if sym not in wanted and now - (self.history[sym][-1][0] if self.history[sym] else 0) > 600:
                    del self.history[sym]

    # ── Vistas ────────────────────────────────────────────────────────────
    def positions_view(self) -> list[dict]:
        acct, market = self.core.account, self.core.market
        rows = []
        seen = set()
        for pos in acct.positions.values():
            mark = market.price(pos.symbol)
            lev = acct.leverage.get(pos.symbol, 0) or 1
            trade = self.trades._trade_for_position(pos.symbol, pos.side)
            pnl = pos.pnl(mark)
            notional = pos.qty * (mark or pos.entry_price)
            margin = pos.isolated_wallet if pos.margin_type == "isolated" and pos.isolated_wallet else notional / lev
            algos = acct.algos_for(pos.symbol, pos.side)
            tp = next((a.trigger_price for a in algos if a.type.startswith("TAKE_PROFIT")), 0.0)
            sl = next((a.trigger_price for a in algos if a.type.startswith("STOP")), 0.0)
            grid = self.grids.owns(pos.symbol, pos.direction)
            seen.add((pos.symbol, pos.direction))
            rows.append({
                "symbol": pos.symbol, "direction": pos.direction, "position_side": pos.side, "qty": pos.qty,
                "entry": pos.entry_price, "break_even": pos.break_even, "mark": mark, "pnl": pnl,
                "roe": pnl / margin * 100 if margin else 0.0, "notional": notional, "margin": margin,
                "leverage": lev, "margin_type": pos.margin_type, "liq": pos.liquidation_price, "tp": tp, "sl": sl,
                "source": "grid" if grid else (trade.source if trade else "externa"),
                "trade_id": trade.id if trade else None, "paper_id": trade.paper_trade_id if trade else None,
                "open_time": trade.open_time if trade else "", "assumed": False,
                "fees": trade.fees_usdt if trade else 0.0,
            })
        for t in self.trades.open_trades:
            if t.key in seen or (t.symbol, "BOTH") in seen:
                continue
            rows.append({
                "symbol": t.symbol, "direction": t.direction, "position_side": t.direction if t.hedge_mode else "BOTH",
                "qty": t.quantity, "entry": t.entry_price, "break_even": 0.0, "mark": t.current_price, "pnl": t.pnl_usdt,
                "roe": t.roe_pct, "notional": t.notional_usdt, "margin": t.margin_usdt, "leverage": t.leverage,
                "margin_type": "", "liq": 0.0, "tp": t.tp_price, "sl": t.sl_price,
                "source": t.source, "trade_id": t.id, "paper_id": t.paper_trade_id, "open_time": t.open_time,
                "assumed": t.order_assumed or not self.settings.has_credentials, "fees": t.fees_usdt,
            })
        rows.sort(key=lambda r: (r["symbol"], r["direction"]))
        return rows

    def orders_view(self) -> list[dict]:
        rows = [o.to_dict() for o in self.core.account.orders.values()]
        rows += [o.to_dict() for o in self.core.account.algos.values()]
        for r in rows:
            cid = r.get("client_id", "")
            r["origin"] = ("grid" if cid.startswith("G") else "tp/sl" if r["kind"] == "algo"
                           else "executor" if cid[:1] in ("X", "C", "M", "L") else "externa")
        rows.sort(key=lambda r: r["time"], reverse=True)
        return rows

    def health(self) -> dict:
        c = self.core
        return {
            "ws_api": c.ws.stats(),
            "market": c.market.stats(),
            "user": self.user.stats() if self.settings.has_credentials else {"connected": False, "disabled": True},
            "rest": c.rest.stats(),
            "clock_offset_ms": c.clock.offset_ms,
            "exchange_info": {**c.exinfo.stats(), "refreshing": self.exinfo_refreshing},
            "telegram": {"enabled": c.telegram.enabled, "sent": c.telegram.sent, "failed": c.telegram.failed},
            "credentials": self.settings.has_credentials,
        }

    def snapshot(self) -> dict:
        c = self.core
        trades = self.trades
        stats = trades.stats()
        return {
            "ts": time.time(),
            "version": __version__,
            "env": self.settings.env_label,
            "uptime_s": int(time.time() - self.started_ts),
            "account": c.account.summary(c.market.marks),
            "stats": stats,
            "status": trades.status,
            "trading_enabled": trades.trading_enabled,
            "leverage": self.settings.leverage,
            "settings": self.settings.public_view(),
            "positions": self.positions_view(),
            "orders": self.orders_view(),
            "grids": self.grids.list(),
            "closed": [t.to_dict() for t in trades.closed[-150:]][::-1],
            "signals": list(trades.signal_log)[-80:][::-1],
            "health": self.health(),
            "errors_total": len(c.journal),
            "error_counts": c.journal.counts(),
        }

    def symbol_view(self, symbol: str) -> dict:
        symbol = symbol.upper()
        c = self.core
        view = c.market.symbol_view(symbol)
        rules = c.exinfo.get(symbol, view["mark"] or view["last"])
        view["rules"] = rules.to_public()
        view["leverage"] = c.account.leverage.get(symbol, 0)
        view["margin_type"] = c.account.margin_type.get(symbol, "")
        view["known"] = c.exinfo.known(symbol)
        return view

    def api_state(self) -> dict:
        """Formato compatible con el /api/state anterior (lo consume app.py)."""
        t = self.trades
        acct = self.core.account.summary(self.core.market.marks)
        stats = t.stats()
        ser = lambda tr: {k: v for k, v in tr.to_dict().items()}  # noqa: E731
        return {
            "balance": acct["available"],
            "equity": acct["available"] + t.unrealized_pnl,
            "wallet_balance": acct["wallet"],
            "margin_balance": acct["margin_balance"],
            "realized_pnl": stats["realized_pnl"],
            "unrealized_pnl": stats["unrealized_pnl"],
            "wins": stats["wins"],
            "losses": stats["losses"],
            "win_rate": stats["win_rate"],
            "open_count": len(t.trades),
            "open_longs": sum(1 for x in t.trades.values() if x.direction == "LONG"),
            "open_shorts": sum(1 for x in t.trades.values() if x.direction == "SHORT"),
            "open_trades": [ser(x) for x in t.open_trades],
            "closed_trades": [ser(x) for x in t.closed[-200:]],
            "executor_status": t.status,
            "ws_symbols": ", ".join(sorted({x.symbol for x in t.open_trades})) or "ninguno",
            "leverage": self.settings.leverage,
            "trading_enabled": t.trading_enabled,
            "testnet": self.settings.testnet,
            "hedge_mode": acct["hedge_mode"],
            "grids": [{k: g[k] for k in ("id", "symbol", "mode", "status", "grid_profit", "total_pnl")}
                      for g in self.grids.list()],
        }

    # ── Comandos del dashboard ────────────────────────────────────────────
    async def execute(self, cmd: str, args: dict) -> Any:
        handler: Optional[Callable[[dict], Awaitable[Any]]] = getattr(self, f"_cmd_{cmd}", None)
        if handler is None:
            raise CommandError(f"comando desconocido: {cmd}")
        return await handler(args or {})

    @staticmethod
    def _symbol(args: dict) -> str:
        symbol = str(args.get("symbol", "")).upper().strip()
        if not symbol:
            raise CommandError("falta el símbolo")
        return symbol

    @staticmethod
    def _direction(args: dict, required: bool = True) -> Optional[str]:
        d = str(args.get("direction", "")).upper().strip()
        if d not in ("LONG", "SHORT"):
            if required:
                raise CommandError("direction debe ser LONG o SHORT")
            return None
        return d

    def _require_credentials(self) -> None:
        if not self.settings.has_credentials:
            raise CommandError("configura BINANCE_API_KEY y BINANCE_API_SECRET para operar")

    async def _cmd_close_position(self, a: dict):
        self._require_credentials()
        symbol, direction = self._symbol(a), self._direction(a, required=False)
        trade = self.trades.get_trade(symbol, direction)
        if trade is not None:
            ok = await self.trades.close_trade(trade, "MANUAL")
        else:
            ok = await self.trades.close_symbol(symbol, direction, "MANUAL")
        if not ok:
            raise CommandError(f"no se pudo cerrar {symbol} (revisa la pestaña Errores)")
        self.trades.status["manual_closes"] += 1
        return {"closed": True}

    async def _cmd_close_all(self, a: dict):
        self._require_credentials()
        closed = await self.trades.close_all("MANUAL")
        self.trades.status["manual_closes"] += len(closed)
        return {"closed": len(closed)}

    async def _cmd_toggle_trading(self, a: dict):
        t = self.trades
        t.trading_enabled = bool(a["enabled"]) if "enabled" in a else not t.trading_enabled
        t.save()
        log.warning("Trading %s desde el dashboard", "ACTIVADO" if t.trading_enabled else "PAUSADO")
        return {"trading_enabled": t.trading_enabled}

    async def _cmd_set_default_leverage(self, a: dict):
        lev = int(safe_float(a.get("leverage")))
        if not 1 <= lev <= 125:
            raise CommandError("leverage entre 1 y 125")
        self.settings.leverage = lev
        self.trades.save()
        return {"leverage": lev}

    async def _cmd_clear_history(self, a: dict):
        return {"cleared": self.trades.clear_history()}

    async def _cmd_open(self, a: dict):
        self._require_credentials()
        symbol, direction = self._symbol(a), self._direction(a)
        if symbol in self.settings.blocked_symbols and not a.get("ignore_block"):
            raise CommandError(f"{symbol} está en BLOCKED_SYMBOLS")
        if self.grids.owns(symbol, direction):
            raise CommandError(f"{symbol} {direction} está gestionado por un bot Grid")
        amount = safe_float(a.get("amount"))
        mode = str(a.get("size_mode", "notional"))
        lev = int(safe_float(a.get("leverage"))) or None
        if amount <= 0:
            raise CommandError("indica un tamaño mayor que 0")
        price = self.core.market.price(symbol)
        if not price:
            raise CommandError(f"sin precio de mercado para {symbol}")
        lev_eff = lev or self.trades.leverage_for_price(price)
        notional = amount * lev_eff if mode == "margin" else (amount * price if mode == "qty" else amount)
        order_type = str(a.get("order_type", "MARKET")).upper()
        if order_type == "LIMIT":
            limit_price = safe_float(a.get("price"))
            if limit_price <= 0:
                raise CommandError("precio límite inválido")
            await self.orders.ensure_leverage(symbol, lev_eff)
            rules = self.core.exinfo.get(symbol, limit_price)
            qty = rules.qty_for_notional(notional, limit_price, market=False,
                                         min_notional_floor=self.settings.min_notional_usdt)
            side = "BUY" if direction == "LONG" else "SELL"
            ctx = OrderContext(symbol, side, direction, "limit", rules, market=False,
                               desired_notional=notional, ref_price=limit_price)
            owner = {"kind": "limit_entry", "symbol": symbol, "direction": direction, "leverage": lev_eff}
            result, params = await self.orders.limit(ctx, qty, rules.round_price(limit_price, "down" if side == "BUY" else "up"),
                                                     prefix="M", owner=owner)
            self.core.bus.emit("order", f"LIMIT {direction} {symbol} {fmt(qty)} @ {params.get('price')}", "success")
            return {"order_id": result.get("orderId"), "qty": fmt(qty), "price": params.get("price")}
        trade = await self.trades.open_trade(symbol, direction, notional=notional, source="manual", leverage=lev)
        if trade is None:
            raise CommandError(f"no se pudo abrir {direction} {symbol} (revisa la pestaña Errores)")
        protections = []
        for kind in ("tp", "sl"):
            trig = safe_float(a.get(kind))
            if trig > 0:
                try:
                    await self.trades.set_protection(symbol, trade.direction, kind.upper(), trig)
                    protections.append(kind.upper())
                except BinanceAPIError as err:
                    diag = ErrorDoctor.diagnose(err)
                    self.core.bus.emit("protect", f"{kind.upper()} no colocado: {diag.info.title}", "error")
        return {"trade": trade.to_dict(), "protections": protections}

    async def _cmd_set_tp(self, a: dict):
        return await self._protect(a, "TP")

    async def _cmd_set_sl(self, a: dict):
        return await self._protect(a, "SL")

    async def _protect(self, a: dict, kind: str):
        self._require_credentials()
        symbol, direction = self._symbol(a), self._direction(a)
        trigger = safe_float(a.get("trigger_price"))
        if trigger <= 0:
            raise CommandError("precio de disparo inválido")
        return await self.trades.set_protection(symbol, direction, kind, trigger)

    async def _cmd_cancel_tp_sl(self, a: dict):
        symbol, direction = self._symbol(a), self._direction(a, required=False)
        kinds = None
        if a.get("kind") in ("TP", "SL"):
            kinds = {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"} if a["kind"] == "TP" else {"STOP_MARKET", "STOP"}
        return {"cancelled": await self.orders.cancel_protection(symbol, direction, kinds)}

    async def _cmd_cancel_order(self, a: dict):
        symbol = self._symbol(a)
        oid = int(safe_float(a.get("id")))
        ok = await (self.orders.cancel_algo(oid, symbol) if a.get("kind") == "algo"
                    else self.orders.cancel(symbol, order_id=oid))
        if not ok:
            raise CommandError("no se pudo cancelar la orden (ver Errores)")
        return {"cancelled": oid}

    async def _cmd_cancel_symbol_orders(self, a: dict):
        symbol = self._symbol(a)
        return {"cancelled": await self.orders.cancel_symbol_orders(symbol)}

    async def _cmd_limit_order(self, a: dict):
        self._require_credentials()
        symbol = self._symbol(a)
        side = str(a.get("side", "")).upper()
        price, qty = safe_float(a.get("price")), safe_float(a.get("quantity"))
        if side not in ("BUY", "SELL") or price <= 0 or qty <= 0:
            raise CommandError("parámetros inválidos (side, price, quantity)")
        reduce_only = bool(a.get("reduce_only"))
        direction = self._direction(a, required=False) or (
            ("SHORT" if side == "BUY" else "LONG") if reduce_only else ("LONG" if side == "BUY" else "SHORT"))
        rules = self.core.exinfo.get(symbol, price)
        ctx = OrderContext(symbol, side, direction, "limit", rules, market=False, reduce_only=reduce_only,
                           desired_notional=price * qty, ref_price=price)
        result, params = await self.orders.limit(ctx, rules.round_qty(qty, market=False, mode="nearest"),
                                                 rules.round_price(price), prefix="M")
        return {"order_id": result.get("orderId"), "price": params.get("price"), "qty": params.get("quantity")}

    async def _cmd_set_symbol_leverage(self, a: dict):
        self._require_credentials()
        symbol = self._symbol(a)
        lev = int(safe_float(a.get("leverage")))
        if not 1 <= lev <= 125:
            raise CommandError("leverage entre 1 y 125")
        applied = await self.orders.ensure_leverage(symbol, lev)
        for t in self.trades.open_trades:
            if t.symbol == symbol:
                t.leverage = applied
        return {"requested": lev, "applied": applied}

    async def _cmd_set_margin_type(self, a: dict):
        self._require_credentials()
        symbol = self._symbol(a)
        mt = str(a.get("margin_type", "")).upper()
        if mt not in ("ISOLATED", "CROSSED"):
            raise CommandError("margin_type debe ser ISOLATED o CROSSED")
        try:
            await self.core.rest.set_margin_type(symbol, mt)
        except BinanceAPIError as err:
            if err.code != -4046:
                raise
        self.core.account.margin_type[symbol] = "isolated" if mt == "ISOLATED" else "cross"
        return {"margin_type": mt}

    async def _cmd_position_mode(self, a: dict):
        self._require_credentials()
        hedge = await self.core.rest.get_position_mode()
        self.core.account.hedge_mode = hedge
        return {"hedge_mode": hedge}

    async def _cmd_set_position_mode(self, a: dict):
        self._require_credentials()
        hedge = bool(a.get("hedge_mode"))
        if self.core.account.positions or self.core.account.orders:
            raise CommandError("cierra todas las posiciones y cancela las órdenes antes de cambiar el modo")
        try:
            await self.core.rest.set_position_mode(hedge)
        except BinanceAPIError as err:
            if err.code != -4059:
                raise
        self.core.account.hedge_mode = hedge
        self.settings.hedge_mode_hint = hedge
        return {"hedge_mode": hedge}

    async def _cmd_modify_margin(self, a: dict):
        self._require_credentials()
        symbol, direction = self._symbol(a), self._direction(a)
        amount = safe_float(a.get("amount"))
        if amount <= 0:
            raise CommandError("monto inválido")
        ps = self.core.account.position_side_for(direction)
        await self.core.rest.modify_margin(symbol, D(fmt(amount)), ps, bool(a.get("add", True)))
        return {"amount": amount}

    async def _cmd_symbol_info(self, a: dict):
        symbol = self._symbol(a)
        self.watch[symbol] = time.time()
        return self.symbol_view(symbol)

    async def _cmd_price_history(self, a: dict):
        symbol = self._symbol(a)
        self.watch[symbol] = time.time()
        return {"symbol": symbol, "points": list(self.history.get(symbol, []))}

    async def _cmd_grid_preview(self, a: dict):
        return self.grids.preview(a)

    async def _cmd_grid_create(self, a: dict):
        self._require_credentials()
        bot = await self.grids.create(a)
        return bot.summary(self.core.market.price(bot.symbol))

    async def _cmd_grid_stop(self, a: dict):
        bot = await self.grids.stop(str(a.get("id")), close_position=a.get("close_position"), reason="MANUAL")
        return bot.summary(self.core.market.price(bot.symbol))

    async def _cmd_grid_detail(self, a: dict):
        return self.grids.detail(str(a.get("id")))

    async def _cmd_grid_delete(self, a: dict):
        self.grids.delete(str(a.get("id")))
        return {"deleted": a.get("id")}

    async def _cmd_explain_error(self, a: dict):
        try:
            code = int(str(a.get("code", "")).strip())
        except ValueError as exc:
            raise CommandError("escribe un código numérico, p. ej. -2019 o 418") from exc
        return ErrorDoctor.explain(code)

    async def _cmd_error_catalog(self, a: dict):
        return ErrorDoctor.catalog()

    async def _cmd_errors(self, a: dict):
        return {"entries": self.core.journal.entries(int(safe_float(a.get("limit"), 150))),
                "counts": self.core.journal.counts()}

    async def _cmd_clear_errors(self, a: dict):
        self.core.journal.clear()
        return {"cleared": True}

    async def _cmd_refresh_exchange_info(self, a: dict):
        return await self.refresh_exchange_info("manual")

    async def _cmd_sync_account(self, a: dict):
        self._require_credentials()
        await self.sync_account("manual")
        return {"positions": len(self.core.account.positions)}

    async def _cmd_sync_orders(self, a: dict):
        self._require_credentials()
        await self.seed_open_orders()
        return {"orders": len(self.core.account.orders), "algos": len(self.core.account.algos)}

    async def _cmd_logs(self, a: dict):
        return list(self.logs.lines)[-300:]
