"""Servicio principal: conecta streams, cuenta, trading, grids y dashboard."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import deque
from typing import Any, Awaitable, Callable, Optional

from . import __version__
from .account import AccountState
from .binance_api import BinanceWsApi, RestClient, ServerClock
from .config import Settings
from .core import Core
from .errors import BinanceAPIError, ErrorDoctor, ErrorJournal
from .exchange_info import ExchangeInfo, apply_brackets, parse_exchange_info
from .grid import GridManager
from .notifier import EventBus, TelegramNotifier
from .orders import OrderContext, OrderExecutor
from .precision import D, fmt, parse_bool, safe_float
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
    """Error de un comando con su código HTTP (400 por defecto)."""

    status = 400

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        if status is not None:
            self.status = status


class NotFoundError(CommandError):
    status = 404


class ConflictError(CommandError):
    status = 409


class ExecutorService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.started_ts = time.time()
        clock = ServerClock()
        ws = BinanceWsApi(settings.ws_api_url, settings.api_key, settings.api_secret, clock, settings.recv_window_ms)
        rest = RestClient(settings.rest_url, settings.api_key, settings.api_secret, clock, settings.proxy_urls,
                          settings.recv_window_ms, settings.rest_proxy_all)
        exinfo = ExchangeInfo(settings.data_dir, settings.exchange_info_file, settings.min_notional_usdt)
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
        self.keepalive = {"enabled": bool(settings.keepalive_url and settings.keepalive_s), "ok": 0, "errors": 0,
                          "last_status": None}

    # ── Ciclo de vida ─────────────────────────────────────────────────────
    async def start(self) -> None:
        s = self.settings
        s.data_dir.mkdir(parents=True, exist_ok=True)
        self.core.exinfo.load()
        self.trades.load()
        self.grids.load()
        self.core.telegram.start()
        self.core.market.start()

        if s.signal_secret_is_default:
            log.warning("SIGNAL_SECRET no está definido: se aceptan los secretos por defecto de los bridges "
                        "(%s). Defínelo y usa el mismo valor en EXECUTOR_SECRET (app_25) / "
                        "ExecutorBridge(signal_secret=...)", ", ".join(s.signal_secrets))
            if s.dashboard_token:
                log.warning("DASHBOARD_TOKEN definido: la API HTTP y el dashboard solo aceptan ese token "
                            "(los secretos por defecto de los bots solo sirven para /signal)")
        if not s.proxy_urls:
            log.critical("PROXY_URLS / FIXIE_URL no configurado: el cambio de leverage (REST) saldrá con la IP "
                         "directa del servidor; si Binance la bloquea, se abrirá con el leverage que ya tenga "
                         "el símbolo (LEVERAGE_REQUIRED=true para rechazar la apertura en ese caso)")

        if not len(self.core.exinfo) and s.exchange_info_bootstrap:
            self._spawn(self._bootstrap_exinfo())

        if s.has_credentials:
            try:
                await self.sync_account("arranque")
                if s.adopt_positions:
                    n = self.trades.adopt_untracked()
                    if n:
                        log.warning("%d posición(es) real(es) sin trade registrado se adoptaron como trades", n)
            except Exception as exc:
                diag = ErrorDoctor.diagnose(exc)
                self.core.journal.record(diag, "sync.arranque")
                log.error("No se pudo leer la cuenta al arrancar: %s", diag.summary())
            self.user.start()
            self._spawn(self._ready_fallback())
            if s.seed_open_orders_rest:
                self._spawn(self.seed_open_orders())
            self._spawn(self._balance_loop())
            self._spawn(self._verify_loop())
        else:
            log.critical("BINANCE_API_KEY / BINANCE_API_SECRET no configuradas: solo datos de mercado y dashboard")
            self.trades.ready.set()
        if s.exchange_info_refresh_h > 0:
            self._spawn(self._exinfo_loop())
        if self.keepalive["enabled"]:
            self._spawn(self._keepalive_loop())
        self._spawn(self._history_loop())
        if os.environ.get("RENDER") and not str(s.data_dir).startswith(("/var/data", "/data", "/mnt")):
            log.warning("DATA_DIR=%s está en el disco efímero de Render: trades y grids se pierden en cada "
                        "deploy. Monta un Render Disk y apunta DATA_DIR a él (p. ej. /var/data)", s.data_dir)
        if not self.trades.trading_enabled:
            self.core.bus.emit("trading", "Trading PAUSADO (restaurado de la sesión anterior)", "warning")

        acct = self.core.account.summary(self.core.market.marks)
        self.core.telegram.send(
            f"⚡ <b>Futures Executor v{__version__} iniciado</b> [{s.env_label}]\n"
            f"💰 Disponible: <code>{acct['available']:.2f} USDT</code>\n"
            f"⚡ Leverage por defecto: <code>{s.leverage}x</code> · Modo {'Hedge' if acct['hedge_mode'] else 'One-way'}\n"
            f"📡 Órdenes y cuenta: WebSocket API · Precios: stream global\n"
            f"📚 exchangeInfo local: {len(self.core.exinfo)} símbolos · 🤖 Grids activos: {len(self.grids.active_bots())}"
            + ("\n⏸ <b>Trading PAUSADO</b> (restaurado)" if not self.trades.trading_enabled else "")
        )

    async def _ready_fallback(self) -> None:
        """Si la 1.ª sincronización tarda, no se bloquean las señales para siempre."""
        await asyncio.sleep(30)
        if not self.trades.ready.is_set():
            log.warning("La cuenta no se pudo sincronizar en 30 s; las señales se ejecutan igualmente")
            self.trades.ready.set()

    async def _keepalive_loop(self) -> None:
        """Mantiene despierto el servicio en planes de Render que se duermen."""
        import aiohttp

        url = f"{self.settings.keepalive_url}/health"
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
            while True:
                await asyncio.sleep(self.settings.keepalive_s)
                try:
                    async with session.get(url) as resp:
                        self.keepalive["last_status"] = resp.status
                        self.keepalive["ok"] += 1
                except Exception as exc:
                    self.keepalive["errors"] += 1
                    self.keepalive["last_status"] = repr(exc)[:120]

    def _spawn(self, coro: Awaitable) -> None:
        self._tasks.append(asyncio.create_task(coro))

    async def stop(self) -> None:
        pending = await self.trades.drain(20.0)
        if pending:
            log.error("%d señal(es) seguían ejecutándose al apagar", pending)
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
        asked = time.time()
        positions = await self.core.ws.positions()
        self.core.account.apply_positions_snapshot(positions, as_of=asked)
        await self.refresh_balance(force=True)
        self.trades.reconcile_after_sync(as_of=asked)
        self.trades.ready.set()
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
            self.watch = {s: ts for s, ts in self.watch.items() if now - ts < 120}
            wanted = set(self.watch)
            wanted |= {p.symbol for p in self.core.account.positions.values()}
            wanted |= {b.symbol for b in self.grids.active_bots()}
            for sym in wanted:
                price = self.core.market.mark(sym)
                if price:
                    self.history.setdefault(sym, deque(maxlen=1800)).append((int(now), price))
            for sym in list(self.history):
                if sym not in wanted and now - (self.history[sym][-1][0] if self.history[sym] else 0) > 600:
                    del self.history[sym]

    def note_watch(self, symbol: str) -> None:
        """Marca un símbolo como observado (historial del gráfico). Solo símbolos reales."""
        if len(symbol) <= 30 and (self.core.exinfo.known(symbol) or self.core.market.mark(symbol)):
            self.watch[symbol] = time.time()

    # ── Vistas ────────────────────────────────────────────────────────────
    def positions_view(self, symbol: Optional[str] = None) -> list[dict]:
        acct, market = self.core.account, self.core.market
        rows = []
        seen = set()
        for pos in acct.positions.values():
            if symbol and pos.symbol != symbol:
                continue
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
                "paper_ids": trade.paper_ids if trade else [], "levels": trade.to_dict()["levels"] if trade else [],
                "open_time": trade.open_time if trade else "", "assumed": False,
                "fees": trade.fees_usdt if trade else 0.0,
            })
        for t in self.trades.open_trades:
            if symbol and t.symbol != symbol:
                continue
            if t.key in seen or (t.symbol, "BOTH") in seen:
                continue
            rows.append({
                "symbol": t.symbol, "direction": t.direction, "position_side": t.direction if t.hedge_mode else "BOTH",
                "qty": t.quantity, "entry": t.entry_price, "break_even": 0.0, "mark": t.current_price, "pnl": t.pnl_usdt,
                "roe": t.roe_pct, "notional": t.notional_usdt, "margin": t.margin_usdt, "leverage": t.leverage,
                "margin_type": "", "liq": 0.0, "tp": t.tp_price, "sl": t.sl_price,
                "source": t.source, "trade_id": t.id, "paper_id": t.paper_trade_id, "paper_ids": t.paper_ids,
                "levels": t.to_dict()["levels"], "open_time": t.open_time,
                "assumed": t.order_assumed or not self.settings.has_credentials, "fees": t.fees_usdt,
            })
        rows.sort(key=lambda r: (r["symbol"], r["direction"]))
        return rows

    def orders_view(self, symbol: Optional[str] = None) -> list[dict]:
        rows = [o.to_dict() for o in self.core.account.orders.values()]
        rows += [o.to_dict() for o in self.core.account.algos.values()]
        if symbol:
            rows = [r for r in rows if r["symbol"] == symbol]
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
            "ready": self.trades.ready.is_set(),
            "accepting_signals": self.trades.accepting,
            "trading_enabled": self.trades.trading_enabled,
            "proxy_configured": bool(self.settings.proxy_urls),
            "signals_inflight": self.trades.inflight(),
            "keepalive": self.keepalive,
        }

    def account_view(self) -> dict:
        c = self.core
        data = c.account.summary(c.market.marks)
        data["balances"] = {k: vars(v) for k, v in c.account.balances.items()}
        data["leverage_default"] = self.settings.leverage
        return data

    def trades_view(self, status: str = "all", limit: int = 200, symbol: Optional[str] = None) -> dict:
        t = self.trades
        open_rows = [x.to_dict() for x in t.open_trades if not symbol or x.symbol == symbol]
        closed = [x for x in t.closed if not symbol or x.symbol == symbol]
        closed_rows = [x.to_dict() for x in (closed[-limit:] if limit else closed)][::-1]
        out: dict = {}
        if status in ("open", "all"):
            out["open"] = open_rows
        if status in ("closed", "all"):
            out["closed"] = closed_rows
        return out

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
        view["tradable"], view["tradable_note"] = c.exinfo.tradable(symbol)
        return view

    def live_view(self, symbols: Optional[list[str]] = None) -> dict:
        """Precios en vivo (estilo /api/live de app_25): por defecto los símbolos con posición."""
        market = self.core.market
        wanted = symbols or sorted({p.symbol for p in self.core.account.positions.values()}
                                   | {t.symbol for t in self.trades.open_trades}
                                   | {b.symbol for b in self.grids.active_bots()})
        prices = {}
        for sym in wanted:
            v = market.symbol_view(sym)
            prices[sym] = {"mark": v["mark"], "last": v["last"], "change_pct": v["change_pct"],
                           "funding_rate": v["funding_rate"]}
        return {"ts": time.time(), "prices": prices,
                "positions": [{k: p[k] for k in ("symbol", "direction", "qty", "entry", "mark", "pnl", "roe")}
                              for p in self.positions_view()]}

    def api_state(self, limit: int = 200) -> dict:
        """Formato compatible con el /api/state anterior (lo consumen app.py / app_25.py).

        ``wins``/``losses``/``win_rate`` conservan la definición antigua (cierres
        con motivo TP); la definición por PnL va en ``wins_pnl``/``win_rate_pnl``.
        """
        t = self.trades
        acct = self.core.account.summary(self.core.market.marks)
        stats = t.stats()
        closed = t.closed[-limit:] if limit else t.closed
        return {
            "balance": acct["available"],
            "equity": acct["available"] + t.unrealized_pnl,
            "wallet_balance": acct["wallet"],
            "margin_balance": acct["margin_balance"],
            "realized_pnl": stats["realized_pnl"],
            "unrealized_pnl": stats["unrealized_pnl"],
            "wins": stats["wins_tp"],
            "losses": stats["losses_tp"],
            "win_rate": stats["win_rate_tp"],
            "wins_pnl": stats["wins"],
            "losses_pnl": stats["losses"],
            "win_rate_pnl": stats["win_rate"],
            "open_count": len(t.trades),
            "open_longs": sum(1 for x in t.trades.values() if x.direction == "LONG"),
            "open_shorts": sum(1 for x in t.trades.values() if x.direction == "SHORT"),
            "open_trades": [x.to_dict() for x in t.open_trades],
            "closed_trades": [x.to_dict() for x in closed],
            "executor_status": t.status,
            "ws_symbols": ", ".join(sorted({x.symbol for x in t.open_trades})) or "ninguno",
            "leverage": self.settings.leverage,
            "trading_enabled": t.trading_enabled,
            "testnet": self.settings.testnet,
            "hedge_mode": acct["hedge_mode"],
            "ready": t.ready.is_set(),
            "proxy_configured": bool(self.settings.proxy_urls),
            "version": __version__,
            "grids": [{k: g[k] for k in ("id", "symbol", "mode", "status", "grid_profit", "total_pnl")}
                      for g in self.grids.list()],
        }

    # ── Comandos (dashboard /ws, POST /api/command y rutas REST) ───────────
    async def execute(self, cmd: str, args: dict) -> Any:
        handler: Optional[Callable[[dict], Awaitable[Any]]] = getattr(self, f"_cmd_{cmd}", None)
        if handler is None or cmd not in COMMANDS:
            raise NotFoundError(f"comando desconocido: {cmd} (ver GET /api/commands)")
        if not isinstance(args, dict):
            raise CommandError("args debe ser un objeto JSON")
        return await handler(args)

    @staticmethod
    def _symbol(args: dict) -> str:
        symbol = str(args.get("symbol", "")).upper().strip()
        if not symbol:
            raise CommandError("falta el símbolo (symbol)")
        return symbol

    @staticmethod
    def _direction(args: dict, required: bool = True) -> Optional[str]:
        d = str(args.get("direction", "")).upper().strip()
        if d not in ("LONG", "SHORT"):
            if required:
                raise CommandError("direction debe ser LONG o SHORT")
            return None
        return d

    def _resolve_direction(self, args: dict, symbol: str) -> str:
        """Dirección explícita o, si el símbolo tiene una sola posición, la suya."""
        d = self._direction(args, required=False)
        if d:
            return d
        trade = self.trades.get_trade(symbol)
        if trade is not None:
            return trade.direction
        positions = self.core.account.positions_for(symbol)
        if len(positions) == 1:
            return positions[0].direction
        if not positions and not any(t.symbol == symbol for t in self.trades.open_trades):
            raise NotFoundError(f"no hay posición abierta para {symbol}")
        raise CommandError(f"{symbol} tiene LONG y SHORT simultáneas: especifica direction")

    def _require_credentials(self) -> None:
        if not self.settings.has_credentials:
            raise CommandError("configura BINANCE_API_KEY y BINANCE_API_SECRET para operar", 503)

    # Lectura ---------------------------------------------------------------
    async def _cmd_state(self, a: dict):
        return self.api_state(int(safe_float(a.get("limit"), 200)))

    async def _cmd_snapshot(self, a: dict):
        return self.snapshot()

    async def _cmd_health(self, a: dict):
        return self.health()

    async def _cmd_account(self, a: dict):
        return self.account_view()

    async def _cmd_positions(self, a: dict):
        symbol = str(a.get("symbol", "")).upper() or None
        return self.positions_view(symbol)

    async def _cmd_orders(self, a: dict):
        symbol = str(a.get("symbol", "")).upper() or None
        return self.orders_view(symbol)

    async def _cmd_trades(self, a: dict):
        status = str(a.get("status", "all")).lower()
        if status not in ("open", "closed", "all"):
            raise CommandError("status debe ser open, closed o all")
        return self.trades_view(status, int(safe_float(a.get("limit"), 200)), str(a.get("symbol", "")).upper() or None)

    async def _cmd_stats(self, a: dict):
        return {**self.trades.stats(), "status": self.trades.status, "grids": len(self.grids.active_bots())}

    async def _cmd_signals(self, a: dict):
        tid = a.get("trade_id")
        return self.trades.signals(int(safe_float(a.get("limit"), 100)), safe_float(a.get("since_ts")),
                                   int(safe_float(tid)) if tid not in (None, "") else None,
                                   str(a.get("signal_id") or "") or None)

    async def _cmd_settings(self, a: dict):
        return self.settings.public_view()

    async def _cmd_markets(self, a: dict):
        rows = self.core.market.market_rows()
        q = str(a.get("q", "")).upper()
        if q:
            rows = [r for r in rows if q in r[0]]
        return [{"symbol": r[0], "last": r[1], "change_pct": r[2], "quote_volume": r[3]} for r in rows]

    async def _cmd_live(self, a: dict):
        syms = [x.strip().upper() for x in str(a.get("symbols", "")).split(",") if x.strip()]
        return self.live_view(syms or None)

    async def _cmd_symbol_info(self, a: dict):
        symbol = self._symbol(a)
        self.note_watch(symbol)
        return self.symbol_view(symbol)

    async def _cmd_price_history(self, a: dict):
        symbol = self._symbol(a)
        self.note_watch(symbol)
        return {"symbol": symbol, "points": list(self.history.get(symbol, []))}

    async def _cmd_exchange_info(self, a: dict):
        return self.core.exinfo.stats()

    async def _cmd_logs(self, a: dict):
        return list(self.logs.lines)[-int(safe_float(a.get("limit"), 300)):]

    async def _cmd_commands(self, a: dict):
        return [{"cmd": k, **v} for k, v in sorted(COMMANDS.items())]

    # Operación -------------------------------------------------------------
    async def _cmd_signal(self, a: dict):
        """Envía una señal (mismo formato que POST /signal) y espera el resultado."""
        status, body = await self.trades.submit_signal(dict(a), source="api", wait=parse_bool(a.get("wait"), True))
        if status >= 400:
            raise CommandError(body.get("error", "señal rechazada"), status)
        if body.get("ok") is False and not body.get("duplicate"):
            # Rechazada al validar (409) o falló al ejecutarse en Binance (502).
            raise CommandError(body.get("error") or "señal rechazada", 502 if "result" in body else 409)
        return body

    async def _cmd_close_position(self, a: dict):
        self._require_credentials()
        symbol, direction = self._symbol(a), self._direction(a, required=False)
        reason = str(a.get("reason") or "MANUAL").upper()
        closed_by = str(a.get("closed_by") or "dashboard")
        qty = safe_float(a.get("quantity")) or None
        if parse_bool(a.get("force"), False):
            # Cierre forzado: libera marcas de "cerrando" atascadas y cierra lo real.
            for key in [k for k in self.trades._closing if k[0] == symbol]:
                self.trades._closing.discard(key)
        trade = self.trades.get_trade(symbol, direction)
        if trade is None and direction is None and len([t for t in self.trades.open_trades if t.symbol == symbol]) > 1:
            raise CommandError(f"{symbol} tiene LONG y SHORT simultáneas: especifica direction")
        if trade is not None:
            ok = await self.trades.close_trade(trade, reason, max_qty=qty, closed_by=closed_by)
        else:
            if not self.core.account.positions_for(symbol):
                await self.trades._fresh_position(symbol, direction or "LONG")
            if not [p for p in self.core.account.positions_for(symbol) if not direction or p.direction == direction]:
                raise NotFoundError(f"no hay posición abierta para {symbol}")
            ok = await self.trades.close_symbol(symbol, direction, reason, closed_by=closed_by)
        if not ok:
            raise CommandError(f"no se pudo cerrar {symbol} (revisa la pestaña Errores)", 502)
        self.trades.status["manual_closes"] += 1
        return {"closed": True, "symbol": symbol, "direction": direction or (trade.direction if trade else None)}

    async def _cmd_close_all(self, a: dict):
        self._require_credentials()
        closed = await self.trades.close_all(str(a.get("reason") or "MANUAL").upper(), closed_by="dashboard")
        self.trades.status["manual_closes"] += len(closed)
        return {"closed": len(closed)}

    async def _cmd_toggle_trading(self, a: dict):
        t = self.trades
        enabled = t.set_trading(a["enabled"]) if "enabled" in a else t.set_trading(not t.trading_enabled)
        log.warning("Trading %s", "ACTIVADO" if enabled else "PAUSADO")
        return {"trading_enabled": enabled}

    async def _cmd_set_default_leverage(self, a: dict):
        lev = int(safe_float(a.get("leverage")))
        if not 1 <= lev <= 125:
            raise CommandError("leverage entre 1 y 125")
        self.trades.set_default_leverage(lev)
        return {"leverage": lev}

    async def _cmd_clear_history(self, a: dict):
        return {"cleared": self.trades.clear_history()}

    async def _cmd_open(self, a: dict):
        self._require_credentials()
        symbol, direction = self._symbol(a), self._direction(a)
        if symbol in self.settings.blocked_symbols and not parse_bool(a.get("ignore_block")):
            raise ConflictError(f"{symbol} está en BLOCKED_SYMBOLS (envía ignore_block=true para forzar)")
        if self.grids.owns(symbol, direction):
            raise ConflictError(f"{symbol} {direction} está gestionado por un bot Grid")
        amount = safe_float(a.get("amount"))
        mode = str(a.get("size_mode", "notional")).lower()
        if mode not in ("notional", "margin", "qty"):
            raise CommandError("size_mode debe ser notional, margin o qty")
        lev = int(safe_float(a.get("leverage"))) or None
        if lev is not None and not 1 <= lev <= 125:
            raise CommandError("leverage entre 1 y 125")
        if amount <= 0:
            raise CommandError("indica un tamaño mayor que 0 (amount)")
        price = self.core.market.price(symbol) or await self.trades.reference_price(symbol)
        if not price:
            raise CommandError(f"sin precio de mercado para {symbol}", 503)
        lev_eff = lev or self.trades.leverage_for_price(price)
        notional = amount * lev_eff if mode == "margin" else (amount * price if mode == "qty" else amount)
        order_type = str(a.get("order_type", "MARKET")).upper()
        if order_type not in ("MARKET", "LIMIT"):
            raise CommandError("order_type debe ser MARKET o LIMIT")
        if order_type == "LIMIT":
            limit_price = safe_float(a.get("price"))
            if limit_price <= 0:
                raise CommandError("precio límite inválido (price)")
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
            raise CommandError(f"no se pudo abrir {direction} {symbol} (revisa la pestaña Errores)", 502)
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
        symbol = self._symbol(a)
        direction = self._resolve_direction(a, symbol)
        trigger = safe_float(a.get("trigger_price")) or safe_float(a.get("price"))
        if trigger <= 0:
            raise CommandError("precio de disparo inválido (trigger_price)")
        result = await self.trades.set_protection(symbol, direction, kind, trigger)
        return {"symbol": symbol, "direction": direction, kind.lower(): trigger, "result": result}

    async def _cmd_cancel_tp_sl(self, a: dict):
        symbol, direction = self._symbol(a), self._direction(a, required=False)
        kinds = None
        kind = str(a.get("kind", "")).upper()
        if kind in ("TP", "SL"):
            kinds = {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"} if kind == "TP" else {"STOP_MARKET", "STOP"}
        return {"symbol": symbol, "cancelled": await self.orders.cancel_protection(symbol, direction, kinds)}

    async def _cmd_cancel_order(self, a: dict):
        symbol = self._symbol(a)
        oid = int(safe_float(a.get("id", a.get("order_id", a.get("algo_id")))))
        if oid <= 0:
            raise CommandError("id de orden inválido")
        is_algo = str(a.get("kind", "")).lower() == "algo" or bool(a.get("algo_id"))
        ok = await (self.orders.cancel_algo(oid, symbol) if is_algo else self.orders.cancel(symbol, order_id=oid))
        if not ok:
            raise CommandError("no se pudo cancelar la orden (ver Errores)", 502)
        return {"cancelled": oid}

    async def _cmd_cancel_symbol_orders(self, a: dict):
        symbol = self._symbol(a)
        return {"symbol": symbol, "cancelled": await self.orders.cancel_symbol_orders(symbol)}

    async def _cmd_limit_order(self, a: dict):
        self._require_credentials()
        symbol = self._symbol(a)
        side = str(a.get("side", "")).upper()
        price, qty = safe_float(a.get("price")), safe_float(a.get("quantity", a.get("qty")))
        if side not in ("BUY", "SELL") or price <= 0 or qty <= 0:
            raise CommandError("parámetros inválidos (side, price, quantity)")
        reduce_only = parse_bool(a.get("reduce_only"), False)
        trade = self.trades.get_trade(symbol, self._direction(a, required=False))
        direction = self._direction(a, required=False) or (trade.direction if trade else None) or (
            ("SHORT" if side == "BUY" else "LONG") if reduce_only else ("LONG" if side == "BUY" else "SHORT"))
        rules = self.core.exinfo.get(symbol, price)
        ctx = OrderContext(symbol, side, direction, "limit", rules, market=False, reduce_only=reduce_only,
                           desired_notional=price * qty, ref_price=price)
        result, params = await self.orders.limit(ctx, rules.round_qty(qty, market=False, mode="nearest"),
                                                 rules.round_price(price), prefix="M")
        return {"order_id": result.get("orderId"), "price": params.get("price"), "qty": params.get("quantity"),
                "result": result}

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
        return {"symbol": symbol, "requested": lev, "applied": applied,
                "confirmed": self.core.account.leverage.get(symbol) == applied}

    async def _cmd_set_margin_type(self, a: dict):
        self._require_credentials()
        symbol = self._symbol(a)
        mt = str(a.get("margin_type", "")).upper()
        if mt == "CROSS":
            mt = "CROSSED"
        if mt not in ("ISOLATED", "CROSSED"):
            raise CommandError("margin_type debe ser ISOLATED o CROSSED")
        note = ""
        try:
            result = await self.core.rest.set_margin_type(symbol, mt)
        except BinanceAPIError as err:
            if err.code != -4046:
                raise
            result, note = {}, "ya estaba en ese modo"
        self.core.account.margin_type[symbol] = "isolated" if mt == "ISOLATED" else "cross"
        return {"symbol": symbol, "margin_type": mt, "result": result, **({"note": note} if note else {})}

    async def _cmd_position_mode(self, a: dict):
        self._require_credentials()
        hedge = await self.core.rest.get_position_mode()
        self.core.account.hedge_mode = hedge
        return {"hedge_mode": hedge, "account_hedge_mode": hedge, "local_hedge_mode": self.core.account.is_hedge,
                "in_sync": True, "open_positions": len(self.core.account.positions)}

    async def _cmd_set_position_mode(self, a: dict):
        self._require_credentials()
        if "hedge_mode" not in a:
            raise CommandError("falta hedge_mode (true/false)")
        hedge = parse_bool(a.get("hedge_mode"), False)
        if self.core.account.positions or self.core.account.orders:
            raise ConflictError("cierra todas las posiciones y cancela las órdenes antes de cambiar el modo")
        note = ""
        try:
            await self.core.rest.set_position_mode(hedge)
        except BinanceAPIError as err:
            if err.code != -4059:
                raise
            note = "la cuenta ya estaba en ese modo"
        self.core.account.hedge_mode = hedge
        self.settings.hedge_mode_hint = hedge
        return {"hedge_mode": hedge, **({"note": note} if note else {})}

    async def _cmd_modify_margin(self, a: dict):
        self._require_credentials()
        symbol = self._symbol(a)
        direction = self._resolve_direction(a, symbol)
        amount = safe_float(a.get("amount"))
        if amount <= 0:
            raise CommandError("monto inválido (amount)")
        add = parse_bool(a.get("add"), True)
        ps = self.core.account.position_side_for(direction)
        result = await self.core.rest.modify_margin(symbol, D(fmt(amount)), ps, add)
        return {"symbol": symbol, "amount": amount, "add": add, "result": result}

    # Grids -----------------------------------------------------------------
    async def _cmd_grids(self, a: dict):
        return self.grids.list()

    async def _cmd_grid_preview(self, a: dict):
        return self.grids.preview(a)

    async def _cmd_grid_create(self, a: dict):
        self._require_credentials()
        bot = await self.grids.create(a)
        return bot.summary(self.core.market.price(bot.symbol))

    async def _cmd_grid_stop(self, a: dict):
        close = parse_bool(a["close_position"], True) if a.get("close_position") not in (None, "") else None
        bot = await self.grids.stop(str(a.get("id")), close_position=close, reason="MANUAL")
        return bot.summary(self.core.market.price(bot.symbol))

    async def _cmd_grid_detail(self, a: dict):
        return self.grids.detail(str(a.get("id")))

    async def _cmd_grid_delete(self, a: dict):
        self.grids.delete(str(a.get("id")))
        return {"deleted": a.get("id")}

    # Errores y mantenimiento -----------------------------------------------
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


def _c(write: bool, desc: str, args: str = "", rest: str = "") -> dict:
    return {"write": write, "description": desc, "args": args, "rest": rest}


# Registro único de comandos: lo usan /api/command, /ws, las rutas REST,
# GET /api/commands y el cliente executor_client.py.
COMMANDS: dict[str, dict] = {
    # Lectura
    "state": _c(False, "Estado compatible con /api/state", "limit?", "GET /api/state"),
    "snapshot": _c(False, "Todo el estado del dashboard", "", "GET /api/snapshot"),
    "health": _c(False, "Salud de conexiones", "", "GET /health"),
    "account": _c(False, "Balance y resumen de cuenta", "", "GET /api/account"),
    "positions": _c(False, "Posiciones reales y trades", "symbol?", "GET /api/positions"),
    "orders": _c(False, "Órdenes abiertas y TP/SL", "symbol?", "GET /api/orders"),
    "trades": _c(False, "Operaciones abiertas/cerradas", "status?=all|open|closed, limit?, symbol?", "GET /api/trades"),
    "stats": _c(False, "PnL, win rate y contadores", "", "GET /api/stats"),
    "signals": _c(False, "Registro de señales y su resultado", "limit?, since_ts?, trade_id?, signal_id?",
                  "GET /api/signals"),
    "settings": _c(False, "Configuración pública", "", "GET /api/settings"),
    "markets": _c(False, "Mercado completo en vivo", "q?", "GET /api/markets"),
    "live": _c(False, "Precios en vivo de los símbolos con posición", "symbols?=A,B", "GET /api/live"),
    "symbol_info": _c(False, "Reglas y precio de un símbolo", "symbol", "GET /api/symbol/{symbol}"),
    "price_history": _c(False, "Historial de mark price (1/s)", "symbol", "GET /api/symbol/{symbol}/history"),
    "exchange_info": _c(False, "Estado del exchangeInfo local", "", "GET /api/exchange-info"),
    "logs": _c(False, "Últimas líneas del log", "limit?", "GET /api/logs"),
    "commands": _c(False, "Este catálogo", "", "GET /api/commands"),
    "grids": _c(False, "Bots Grid", "", "GET /api/grids"),
    "grid_detail": _c(False, "Detalle de un bot Grid", "id", "GET /api/grids/{id}"),
    "grid_preview": _c(False, "Vista previa de un grid", "symbol, lower, upper, grids, investment, leverage?, mode?, "
                                                        "spacing?", "POST /api/grids/preview"),
    "explain_error": _c(False, "Explica un código de error de Binance", "code", "GET /api/errors/{code}"),
    "error_catalog": _c(False, "Catálogo completo de errores", "", "GET /api/errors/catalog"),
    "errors": _c(False, "Errores registrados", "limit?", "GET /api/errors"),
    # Operación
    "signal": _c(True, "Envía una señal (como POST /signal) y espera el resultado", "action, ...",
                 "POST /api/signal"),
    "open": _c(True, "Abre posición manual", "symbol, direction, amount, size_mode?=notional|margin|qty, leverage?, "
                                            "order_type?=MARKET|LIMIT, price?, tp?, sl?", "POST /api/open"),
    "close_position": _c(True, "Cierra la posición de un símbolo", "symbol, direction?, quantity?, reason?, force?",
                         "POST /api/close/{symbol}"),
    "close_all": _c(True, "Cierra todas las posiciones (salvo grids)", "", "POST /api/close-all"),
    "set_tp": _c(True, "Take profit (algo order)", "symbol, trigger_price, direction?", "POST /api/set-tp/{symbol}"),
    "set_sl": _c(True, "Stop loss (algo order)", "symbol, trigger_price, direction?", "POST /api/set-sl/{symbol}"),
    "cancel_tp_sl": _c(True, "Cancela TP/SL", "symbol, direction?, kind?=TP|SL", "POST /api/cancel-tp-sl/{symbol}"),
    "limit_order": _c(True, "Orden límite manual", "symbol, side, price, quantity, reduce_only?, direction?",
                      "POST /api/limit-order"),
    "cancel_order": _c(True, "Cancela una orden", "symbol, id, kind?=order|algo", "POST /api/cancel-order"),
    "cancel_symbol_orders": _c(True, "Cancela todas las órdenes del símbolo", "symbol",
                               "POST /api/cancel-orders/{symbol}"),
    "set_symbol_leverage": _c(True, "Leverage de un símbolo", "symbol, leverage", "POST /api/leverage/{symbol}"),
    "set_default_leverage": _c(True, "Leverage por defecto de las señales", "leverage", "POST /api/leverage"),
    "set_margin_type": _c(True, "Margen ISOLATED/CROSSED", "symbol, margin_type", "POST /api/margin-type/{symbol}"),
    "modify_margin": _c(True, "Añade/retira margen aislado", "symbol, amount, add?, direction?",
                        "POST /api/margin/{symbol}"),
    "position_mode": _c(False, "Modo Hedge/One-way de la cuenta", "", "GET /api/position-mode"),
    "set_position_mode": _c(True, "Cambia el modo de posición", "hedge_mode", "POST /api/position-mode"),
    "toggle_trading": _c(True, "Pausa/reactiva aperturas por señal", "enabled?", "POST /api/trading"),
    "clear_history": _c(True, "Borra el historial y reinicia el PnL", "", "POST /api/clear-history"),
    "grid_create": _c(True, "Crea un bot Grid", "symbol, lower, upper, grids, investment, leverage?, mode?, "
                                                "spacing?, stop_loss?, take_profit?, trigger_price?", "POST /api/grids"),
    "grid_stop": _c(True, "Detiene un bot Grid", "id, close_position?", "POST /api/grids/{id}/stop"),
    "grid_delete": _c(True, "Elimina un bot Grid detenido", "id", "DELETE /api/grids/{id}"),
    "clear_errors": _c(True, "Limpia el registro de errores", "", "POST /api/errors/clear"),
    "refresh_exchange_info": _c(True, "Descarga un exchangeInfo nuevo (REST)", "", "POST /api/exchange-info/refresh"),
    "sync_account": _c(True, "Relee la cuenta por WS API", "", "POST /api/sync"),
    "sync_orders": _c(True, "Relee las órdenes abiertas (REST)", "", "POST /api/sync-orders"),
}
