"""
futures_executor.py v3.0 — Executor de Futuros Binance
══════════════════════════════════════════════════════════════════════
Recibe señales HTTP de app.py y ejecuta órdenes REALES en Binance
Futures USDT-M Perpetuos.

FLUJO:
  app.py → POST /signal → Executor → Binance Futures → Telegram

SEÑALES SOPORTADAS (JSON body):
  Apertura: {"action":"open",      "trade_id":1, "symbol":"BTCUSDT",
              "direction":"LONG",  "price":50000.0, "quantity":0.001}

  Cierre:   {"action":"close",     "trade_id":1, "symbol":"BTCUSDT",
              "direction":"LONG",  "reason":"TP", "close_price":50500.0}

  Global:   {"action":"close_all"}

COMPORTAMIENTO:
  • Quantity y dirección se usan TAL CUAL llegan de app.py (sin recalcular)
  • Sin TP/SL automáticos — app.py gestiona el momento del cierre
  • Apertura via Algo Order (STOP_MARKET) como método principal
    → fallback automático a Market Order si falla
  • Error -2019 (margin insuficiente) → se registra igualmente como
    posición abierta (la orden se asume enviada)
  • Cierre via close_all_positions() de BinanceAPI:
    cancela TP/SL + Algo Orders + posición de mercado en un paso

VARIABLES DE ENTORNO:
  BINANCE_API_KEY      — clave API Binance
  BINANCE_API_SECRET   — secreto API Binance
  SIGNAL_SECRET        — token compartido con app.py (cabecera X-Signal-Secret)
  TELEGRAM_BOT_TOKEN   — token bot Telegram
  TELEGRAM_CHAT_ID     — chat ID Telegram
  USE_TESTNET          — "true" para testnet (default: false)
  PORT                 — puerto HTTP (default: 10000)
  LEVERAGE             — apalancamiento a usar en cada posición (default: 1)
  POSITION_POLL_S      — segundos entre polls de posiciones (default: 30)
"""

import asyncio
import aiohttp
from aiohttp import web
import logging
from datetime import datetime, timezone
import os
import json
from dataclasses import dataclass, field
from typing import Optional

# ══════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════
BINANCE_API_KEY    = os.environ.get("BINANCE_API_KEY",    "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
USE_TESTNET        = os.environ.get("USE_TESTNET", "false").lower() == "true"

SIGNAL_SECRET = os.environ.get("SIGNAL_SECRET", "cambiar-por-secreto-seguro")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "")

LEVERAGE        = int(os.environ.get("LEVERAGE",        "1"))
PORT            = int(os.environ.get("PORT",            "10000"))
POSITION_POLL_S = int(os.environ.get("POSITION_POLL_S", "30"))

BINANCE_FAPI_WS = "wss://fstream.binance.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("Executor")


# ══════════════════════════════════════════════════════════
#  MODELO DE TRADE
# ══════════════════════════════════════════════════════════
@dataclass
class Trade:
    id             : int
    symbol         : str
    direction      : str    # "LONG" | "SHORT"
    entry_price    : float
    quantity       : float
    open_time      : str
    leverage       : int
    paper_trade_id : int    = 0
    entry_order_id : str    = ""
    current_price  : float  = 0.0
    status         : str    = "OPEN"   # OPEN | TP | SL | CLOSED | MANUAL | CLOSE_ALL
    close_price    : float  = 0.0
    close_time     : str    = ""
    pnl_usdt       : float  = 0.0
    roi_pct        : float  = 0.0
    order_assumed  : bool   = False    # True si se registró pese a error -2019

    @property
    def notional_usdt(self) -> float:
        return self.entry_price * self.quantity

    def update_unrealized(self, price: float):
        self.current_price = price
        if self.direction == "LONG":
            self.pnl_usdt = (price - self.entry_price) * self.quantity
        else:
            self.pnl_usdt = (self.entry_price - price) * self.quantity
        self.roi_pct = (self.pnl_usdt / self.notional_usdt * 100) if self.notional_usdt else 0.0


# ══════════════════════════════════════════════════════════
#  GESTOR DE EJECUCIÓN
# ══════════════════════════════════════════════════════════
class ExecutionManager:
    """Ejecuta y rastrea posiciones reales en Binance Futures."""

    def __init__(self, binance_api):
        self.api            = binance_api
        self._trades        : dict[str, Trade] = {}
        self._closed        : list[Trade]      = []
        self._counter       : int = 0
        self._lock          = asyncio.Lock()
        self._balance       : float = 0.0
        self._paper_id_map  : dict[int, str] = {}

    # ── Balance real ──────────────────────────────────────
    async def refresh_balance(self):
        loop = asyncio.get_event_loop()
        try:
            balances = await loop.run_in_executor(
                None, self.api.client.futures_account_balance
            )
            for b in balances:
                if b.get("asset") == "USDT":
                    self._balance = float(b.get("availableBalance", b.get("balance", 0)))
                    return
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
        return [t for t in self.open_trades if t.direction == "LONG"]

    @property
    def open_shorts(self) -> list[Trade]:
        return [t for t in self.open_trades if t.direction == "SHORT"]

    @property
    def active_symbols(self) -> set:
        return set(self._trades.keys())

    @property
    def total_realized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self._closed)

    @property
    def unrealized_pnl(self) -> float:
        return sum(t.pnl_usdt for t in self.open_trades)

    @property
    def equity(self) -> float:
        return self._balance + self.unrealized_pnl

    # ── Apertura de posición ─────────────────────────────
    async def open_trade(
        self,
        symbol         : str,
        direction      : str,
        price          : float,
        quantity       : float,
        paper_trade_id : int = 0,
    ) -> Optional[Trade]:
        """
        Abre una posición usando los parámetros exactos recibidos de app.py.

        Estrategia de ejecución (en orden de preferencia):
          1. Algo Order  (STOP_MARKET) — evita rechazos de Binance
          2. Market Order              — fallback si algo falla
          3. Error -2019               — se registra la posición como asumida
        """
        direction = direction.upper()
        side      = "BUY" if direction == "LONG" else "SELL"

        # Verificar que no exista ya una posición para este símbolo
        async with self._lock:
            if symbol in self._trades:
                log.warning(f"open_trade: {symbol} ya tiene posición registrada — ignorando")
                return None

        loop = asyncio.get_event_loop()

        # ── Configurar leverage ───────────────────────────
        try:
            await loop.run_in_executor(
                None, lambda: self.api.set_leverage(symbol, LEVERAGE)
            )
            log.info(f"Leverage {LEVERAGE}x configurado → {symbol}")
        except Exception as e:
            log.warning(f"open_trade: no se pudo setear leverage {symbol}: {e}")

        # ── Intentar abrir posición ───────────────────────
        entry_order_id = ""
        order_assumed  = False

        # ── Intento 1: Algo Order (STOP_MARKET) ──────────
        log.info(f"open_trade: intentando Algo Order STOP_MARKET → {symbol} {side} qty={quantity} trigger={price}")
        try:
            algo_result = await loop.run_in_executor(
                None,
                lambda: self.api.create_algo_order(
                    symbol        = symbol,
                    side          = side,
                    order_type    = "STOP_MARKET",
                    quantity      = quantity,
                    trigger_price = price,
                    position_side = None,
                    time_in_force = "GTC",
                )
            )
            if algo_result and (algo_result.get("algoId") or algo_result.get("orderId")):
                entry_order_id = str(algo_result.get("algoId", algo_result.get("orderId", "ALGO")))
                log.info(f"Algo Order OK: {symbol} {side} qty={quantity} algoId={entry_order_id}")
            else:
                raise ValueError(f"create_algo_order retornó resultado inválido: {algo_result}")

        except Exception as e_algo:
            err_algo = str(e_algo)

            # -2019: margen insuficiente → registrar como asumida
            if "-2019" in err_algo or "Margin is insufficient" in err_algo:
                log.warning(
                    f"[ASUMIDA] Algo Order -2019 para {symbol} — "
                    f"posición registrada como abierta: {e_algo}"
                )
                entry_order_id = "MARGIN_INSUFFICIENT"
                order_assumed  = True

            else:
                # ── Intento 2: Market Order ───────────────
                log.warning(
                    f"open_trade: Algo Order falló para {symbol} ({e_algo}). "
                    f"Intentando Market Order..."
                )
                try:
                    mkt_result = await loop.run_in_executor(
                        None,
                        lambda: self.api.create_market_order(symbol, side, quantity)
                    )
                    if mkt_result and mkt_result.get("orderId"):
                        entry_order_id = str(mkt_result["orderId"])
                        log.info(
                            f"Market Order OK: {symbol} {side} qty={quantity} "
                            f"id={entry_order_id}"
                        )
                    else:
                        raise ValueError(f"create_market_order retornó resultado inválido: {mkt_result}")

                except Exception as e_mkt:
                    err_mkt = str(e_mkt)

                    # -2019 en Market Order → registrar como asumida
                    if "-2019" in err_mkt or "Margin is insufficient" in err_mkt:
                        log.warning(
                            f"[ASUMIDA] Market Order -2019 para {symbol} — "
                            f"posición registrada como abierta: {e_mkt}"
                        )
                        entry_order_id = "MARGIN_INSUFFICIENT"
                        order_assumed  = True
                    else:
                        log.error(
                            f"open_trade: TODOS los métodos fallaron para {symbol}. "
                            f"Algo={e_algo} | Market={e_mkt}"
                        )
                        return None

        # ── Registrar trade en estado interno ─────────────
        async with self._lock:
            if symbol in self._trades:
                log.warning(f"open_trade: race condition — {symbol} ya registrado")
                return None

            self._counter += 1
            trade = Trade(
                id             = self._counter,
                symbol         = symbol,
                direction      = direction,
                entry_price    = price,
                quantity       = quantity,
                open_time      = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                leverage       = LEVERAGE,
                paper_trade_id = paper_trade_id,
                entry_order_id = entry_order_id,
                current_price  = price,
                order_assumed  = order_assumed,
            )
            self._trades[symbol]              = trade
            self._paper_id_map[paper_trade_id] = symbol

        assumed_tag = " [ASUMIDA — MARGIN INSUF]" if order_assumed else ""
        log.info(
            f"[REAL #{trade.id}] ABIERTO {direction} {symbol} @ ${price} "
            f"| Qty: {quantity} | Lev: {LEVERAGE}x "
            f"| OrderId: {entry_order_id} | Paper#{paper_trade_id}{assumed_tag}"
        )
        await self.refresh_balance()
        return trade

    # ── Cierre (sólo actualiza estado interno) ────────────
    async def close_trade(self, trade: Trade, close_price: float, reason: str) -> bool:
        async with self._lock:
            if trade.status != "OPEN":
                return False
            if trade.symbol not in self._trades:
                return False

            trade.status      = reason
            trade.close_price = close_price
            trade.close_time  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

            if trade.direction == "LONG":
                trade.pnl_usdt = (close_price - trade.entry_price) * trade.quantity
            else:
                trade.pnl_usdt = (trade.entry_price - close_price) * trade.quantity

            trade.roi_pct = (
                (trade.pnl_usdt / trade.notional_usdt * 100)
                if trade.notional_usdt else 0.0
            )

            del self._trades[trade.symbol]
            self._paper_id_map.pop(trade.paper_trade_id, None)
            self._closed.append(trade)

        log.info(
            f"[REAL #{trade.id}] CERRADO {reason} {trade.symbol} "
            f"@ ${close_price} | PnL: {trade.pnl_usdt:+.4f} USDT ({trade.roi_pct:+.2f}%)"
        )
        await self.refresh_balance()
        return True

    # ── Cierre forzado — envía orden al exchange ──────────
    async def force_close_trade(self, trade: Trade, reason: str = "MAIN_BOT") -> bool:
        """
        Cancela todas las órdenes del símbolo (TP/SL/Algo) y cierra la
        posición a mercado usando close_all_positions() de BinanceAPI.
        """
        loop = asyncio.get_event_loop()
        close_price = trade.current_price if trade.current_price > 0 else trade.entry_price

        try:
            await loop.run_in_executor(
                None,
                lambda: self.api.close_all_positions(symbol=trade.symbol)
            )
            log.info(f"force_close: close_all_positions OK para {trade.symbol}")
        except Exception as e:
            log.warning(f"force_close: error en close_all_positions {trade.symbol}: {e}")

        return await self.close_trade(trade, close_price, reason)

    # ── Cierre forzado por símbolo (sin trade local) ──────
    async def force_close_by_symbol(self, symbol: str) -> bool:
        """
        Cierra posición directamente en Binance sin requerir un trade
        local registrado. Útil cuando el executor se reinició y perdió
        el estado pero el bot principal pide un cierre.
        """
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                None,
                lambda: self.api.close_all_positions(symbol=symbol)
            )
            log.info(f"force_close_by_symbol {symbol}: {result}")
            return True
        except Exception as e:
            log.error(f"force_close_by_symbol {symbol}: {e}")
            return False

    # ── Cierre global de todas las posiciones ─────────────
    async def close_all_global(self) -> list[Trade]:
        async with self._lock:
            trades_snapshot = list(self._trades.values())

        closed_trades = []
        for trade in trades_snapshot:
            try:
                closed = await self.force_close_trade(trade, reason="CLOSE_ALL")
                if closed:
                    closed_trades.append(trade)
                    log.info(f"close_all_global: cerrado {trade.symbol} #{trade.id}")
            except Exception as e:
                log.error(f"close_all_global: error cerrando {trade.symbol}: {e}")

        log.info(
            f"close_all_global: {len(closed_trades)}/{len(trades_snapshot)} posiciones cerradas"
        )
        return closed_trades

    # ── Poll de posiciones reales en Binance ──────────────
    async def poll_positions(self) -> list[tuple]:
        """
        Consulta todas las posiciones abiertas en Binance Futures.
        - Si sigue abierta → actualiza precio unrealized.
        - Si desapareció   → la detecta como cerrada externamente.
        Retorna lista de (Trade, close_price, reason).
        """
        if not self._trades:
            return []

        loop = asyncio.get_event_loop()
        closed_events: list[tuple] = []

        try:
            positions = await loop.run_in_executor(
                None, self.api.client.futures_position_information
            )
        except Exception as e:
            log.error(f"poll_positions: {e}")
            return []

        # Mapa symbol → datos Binance (sólo posiciones con cantidad ≠ 0)
        pos_by_symbol: dict[str, dict] = {}
        for p in positions:
            sym = p.get("symbol", "")
            amt = float(p.get("positionAmt", 0))
            if sym and abs(amt) > 0:
                pos_by_symbol[sym] = p

        async with self._lock:
            open_copy = dict(self._trades)

        for symbol, trade in open_copy.items():
            if symbol in pos_by_symbol:
                p       = pos_by_symbol[symbol]
                mark_px = float(
                    p.get("markPrice") or p.get("entryPrice") or trade.entry_price
                )
                trade.update_unrealized(mark_px)
            else:
                # Posición desapareció → fue cerrada externamente (TP/SL/liquidación)
                cp = trade.current_price if trade.current_price > 0 else trade.entry_price
                closed_events.append((trade, cp, "CLOSED"))

        return closed_events

    def find_by_paper_id(self, paper_trade_id: int) -> Optional[Trade]:
        sym = self._paper_id_map.get(paper_trade_id)
        if sym:
            return self._trades.get(sym)
        return None

    def update_price(self, symbol: str, price: float):
        t = self._trades.get(symbol)
        if t:
            t.update_unrealized(price)


# ══════════════════════════════════════════════════════════
#  INSTANCIAS GLOBALES
# ══════════════════════════════════════════════════════════
execution_manager: Optional[ExecutionManager] = None

executor_status = {
    "signals_received"   : 0,
    "signals_open"       : 0,
    "signals_close"      : 0,
    "signals_rejected"   : 0,
    "last_signal_time"   : "Esperando señales...",
    "last_signal_detail" : "",
    "started_at"         : datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
}


# ══════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════
async def send_telegram(session: aiohttp.ClientSession, message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url     = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}
    try:
        async with session.post(
            url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            if resp.status != 200:
                log.error(f"Telegram error {resp.status}: {await resp.text()}")
    except Exception as e:
        log.error(f"Error Telegram: {e}")


def build_open_message(trade: Trade) -> str:
    emoji  = "🟢" if trade.direction == "LONG" else "🔴"
    word   = "LONG  ▲" if trade.direction == "LONG" else "SHORT ▼"
    base   = trade.symbol.replace("USDT", "")
    assum  = "\n⚠️ <i>Posición asumida (margin insuf., -2019)</i>" if trade.order_assumed else ""
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


def build_close_message(trade: Trade) -> str:
    reason_map = {
        "TP"           : ("✅", "TAKE PROFIT 🎯"),
        "SL"           : ("❌", "STOP LOSS 🛑"),
        "CLOSED"       : ("🔄", "CIERRE EXTERNO"),
        "MAIN_BOT"     : ("🔄", "CIERRE SEÑAL PRINCIPAL"),
        "CLOSE_ALL"    : ("🛑", "CIERRE GLOBAL"),
        "MANUAL"       : ("🖐", "CIERRE MANUAL"),
    }
    emoji, reason_str = reason_map.get(trade.status, ("⚠️", trade.status))
    dir_str   = "🟢 LONG" if trade.direction == "LONG" else "🔴 SHORT"
    pnl_emoji = "💚" if trade.pnl_usdt >= 0 else "❗"

    closed_all = execution_manager.closed_trades
    wins  = sum(1 for t in closed_all if t.status == "TP")
    total = len(closed_all)
    wr    = f"{wins / total * 100:.1f}% ({wins}✅/{total - wins}❌)" if total else "N/A"

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


# ══════════════════════════════════════════════════════════
#  WEBSOCKET — Precios Futures en tiempo real
# ══════════════════════════════════════════════════════════
async def ws_price_loop(session: aiohttp.ClientSession):
    log.info("WebSocket Futures Price — iniciado")
    last_symbols: frozenset = frozenset()
    reconnect_delay = 3

    while True:
        symbols = frozenset(execution_manager.active_symbols)

        if not symbols:
            last_symbols = symbols
            await asyncio.sleep(2)
            reconnect_delay = 3
            continue

        if symbols != last_symbols:
            log.info(f"WS Futures: conectando {len(symbols)} símbolo(s)")

        streams = "/".join(f"{s.lower()}@miniTicker" for s in sorted(symbols))
        url     = f"{BINANCE_FAPI_WS}/stream?streams={streams}"

        try:
            async with session.ws_connect(
                url,
                heartbeat=20,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as ws:
                last_symbols    = symbols
                reconnect_delay = 3
                log.info("WS Futures: conectado ✅")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            data   = json.loads(msg.data)
                            ticker = data.get("data", {})
                            sym    = ticker.get("s")
                            price  = float(ticker.get("c") or 0)
                            if sym and price > 0:
                                execution_manager.update_price(sym, price)
                        except Exception:
                            pass
                    elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSE):
                        log.warning("WS Futures: cerrado, reconectando...")
                        break

                    new_sym = frozenset(execution_manager.active_symbols)
                    if new_sym != last_symbols:
                        log.info("WS Futures: símbolos cambiaron, reconectando...")
                        break

        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error(f"WS Futures error: {e}")
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30)


# ══════════════════════════════════════════════════════════
#  MONITOR DE POSICIONES — Poll cada POSITION_POLL_S seg.
# ══════════════════════════════════════════════════════════
async def position_monitor_loop(session: aiohttp.ClientSession):
    log.info(f"Position Monitor — poll cada {POSITION_POLL_S}s")
    await asyncio.sleep(10)

    while True:
        try:
            closed_events = await execution_manager.poll_positions()
            for trade, close_price, reason in closed_events:
                closed = await execution_manager.close_trade(trade, close_price, reason)
                if closed:
                    await send_telegram(session, build_close_message(trade))
        except Exception as e:
            log.error(f"position_monitor_loop: {e}")

        await asyncio.sleep(POSITION_POLL_S)


# ══════════════════════════════════════════════════════════
#  ENDPOINT /signal — Recibe señales de app.py
# ══════════════════════════════════════════════════════════
async def signal_handler(request: web.Request) -> web.Response:
    """
    POST /signal
    Headers: X-Signal-Secret: <SIGNAL_SECRET>

    Body JSON (apertura):
      {"action":"open", "trade_id":1, "symbol":"BTCUSDT",
       "direction":"LONG", "price":50000.0, "quantity":0.001}

    Body JSON (cierre):
      {"action":"close", "trade_id":1, "symbol":"BTCUSDT",
       "direction":"LONG", "reason":"TP", "close_price":50500.0}

    Body JSON (cierre global):
      {"action":"close_all"}
    """
    secret = request.headers.get("X-Signal-Secret", "")
    if secret != SIGNAL_SECRET:
        log.warning(f"signal_handler: secreto inválido desde {request.remote}")
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    action   = data.get("action", "").lower()
    symbol   = data.get("symbol", "").upper()
    trade_id = int(data.get("trade_id", 0))

    executor_status["signals_received"]   += 1
    executor_status["last_signal_time"]    = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    executor_status["last_signal_detail"]  = f"{action.upper()} {symbol}"

    log.info(f"Señal recibida: action={action} symbol={symbol} trade_id={trade_id}")

    # ── APERTURA ─────────────────────────────────────────
    if action == "open":
        direction = data.get("direction", "").upper()
        price     = float(data.get("price", 0))
        quantity  = float(data.get("quantity", 0))

        if not symbol or not direction or price <= 0 or quantity <= 0:
            executor_status["signals_rejected"] += 1
            log.error(
                f"signal_handler open: parámetros inválidos — "
                f"symbol={symbol} direction={direction} price={price} quantity={quantity}"
            )
            return web.json_response(
                {"ok": False, "error": "missing or invalid open params"},
                status=400,
            )

        async def _do_open():
            trade = await execution_manager.open_trade(
                symbol         = symbol,
                direction      = direction,
                price          = price,
                quantity       = quantity,
                paper_trade_id = trade_id,
            )
            if trade:
                executor_status["signals_open"] += 1
                async with aiohttp.ClientSession() as sess:
                    await send_telegram(sess, build_open_message(trade))
            else:
                executor_status["signals_rejected"] += 1
                log.warning(f"open_trade rechazado para {symbol} ({direction})")

        asyncio.create_task(_do_open())
        return web.json_response({
            "ok": True, "action": "open", "symbol": symbol,
            "direction": direction, "quantity": quantity, "price": price,
        })

    # ── CIERRE ────────────────────────────────────────────
    elif action == "close":
        reason      = data.get("reason", "MAIN_BOT").upper()
        close_price = float(data.get("close_price", 0))

        # Buscar trade local: primero por paper_id, luego por símbolo
        trade = execution_manager.find_by_paper_id(trade_id)
        if not trade:
            trade = execution_manager._trades.get(symbol)

        async def _do_close():
            if trade:
                # Caso normal: tenemos el trade registrado
                use_price = close_price if close_price > 0 else trade.current_price or trade.entry_price
                closed = await execution_manager.force_close_trade(trade, reason=reason)
                if closed:
                    trade.close_price = use_price  # refinar precio si vino en señal
                    executor_status["signals_close"] += 1
                    async with aiohttp.ClientSession() as sess:
                        await send_telegram(sess, build_close_message(trade))
            else:
                # Caso sin estado local: cerrar directamente en Binance
                log.warning(
                    f"signal_handler close: sin trade local para "
                    f"paper#{trade_id} / {symbol} — cerrando directo en Binance"
                )
                closed = await execution_manager.force_close_by_symbol(symbol)
                if closed:
                    executor_status["signals_close"] += 1
                    async with aiohttp.ClientSession() as sess:
                        await send_telegram(
                            sess,
                            f"🔄 <b>CIERRE FORZADO (sin estado local)</b>\n"
                            f"📊 <code>{symbol}</code>\n"
                            f"⚠️ <i>El executor no tenía este trade registrado — "
                            f"cerrado directo en Binance</i>\n"
                            f"🆔 Paper#{trade_id} | Razón: {reason}",
                        )

        asyncio.create_task(_do_close())
        return web.json_response({"ok": True, "action": "close", "symbol": symbol})

    # ── CIERRE GLOBAL ─────────────────────────────────────
    elif action == "close_all":
        total_open = len(execution_manager.open_trades)
        log.warning(f"signal_handler: CIERRE GLOBAL — cerrando {total_open} posiciones")

        async def _do_close_all():
            closed_trades = await execution_manager.close_all_global()
            async with aiohttp.ClientSession() as sess:
                if not closed_trades:
                    await send_telegram(
                        sess,
                        "🛑 CIERRE GLOBAL ejecutado — no había posiciones abiertas.",
                    )
                    return
                for t in closed_trades:
                    await send_telegram(sess, build_close_message(t))
                await send_telegram(
                    sess,
                    f"🛑 CIERRE GLOBAL completado — {len(closed_trades)} posición(es) cerrada(s).",
                )

        asyncio.create_task(_do_close_all())
        executor_status["signals_close"] += total_open
        return web.json_response({
            "ok": True, "action": "close_all", "positions_targeted": total_open,
        })

    else:
        executor_status["signals_rejected"] += 1
        return web.json_response(
            {"ok": False, "error": f"unknown action: {action}"}, status=400
        )


# ══════════════════════════════════════════════════════════
#  API STATE
# ══════════════════════════════════════════════════════════
async def api_state_handler(request: web.Request) -> web.Response:
    em = execution_manager

    def ser(t: Trade) -> dict:
        return {
            "id"            : t.id,
            "paper_trade_id": t.paper_trade_id,
            "symbol"        : t.symbol,
            "direction"     : t.direction,
            "entry_price"   : t.entry_price,
            "quantity"      : t.quantity,
            "notional"      : t.notional_usdt,
            "leverage"      : t.leverage,
            "open_time"     : t.open_time,
            "current_price" : t.current_price,
            "status"        : t.status,
            "close_price"   : t.close_price,
            "close_time"    : t.close_time,
            "pnl_usdt"      : t.pnl_usdt,
            "roi_pct"       : t.roi_pct,
            "order_assumed" : t.order_assumed,
            "entry_order_id": t.entry_order_id,
        }

    closed = em.closed_trades
    wins   = sum(1 for t in closed if t.status == "TP")
    total  = len(closed)

    return web.json_response({
        "balance"        : em.balance,
        "equity"         : em.equity,
        "realized_pnl"   : em.total_realized_pnl,
        "unrealized_pnl" : em.unrealized_pnl,
        "wins"           : wins,
        "losses"         : total - wins,
        "win_rate"       : (wins / total * 100) if total else None,
        "open_count"     : len(em.open_trades),
        "open_longs"     : len(em.open_longs),
        "open_shorts"    : len(em.open_shorts),
        "open_trades"    : [ser(t) for t in em.open_trades],
        "closed_trades"  : [ser(t) for t in closed],
        "executor_status": executor_status,
        "ws_symbols"     : ", ".join(sorted(em.active_symbols)) or "ninguno",
        "leverage"       : LEVERAGE,
        "testnet"        : USE_TESTNET,
    })


# ══════════════════════════════════════════════════════════
#  DASHBOARD HTML
# ══════════════════════════════════════════════════════════
DASHBOARD_JS = """
<script>
async function refresh() {
  try {
    const r = await fetch('/api/state');
    const d = await r.json();

    const q = id => document.getElementById(id);
    q('bal').textContent   = d.balance.toFixed(2) + ' USDT';
    q('eq').textContent    = d.equity.toFixed(2)  + ' USDT';
    q('rpnl').textContent  = (d.realized_pnl >= 0 ? '+' : '') + d.realized_pnl.toFixed(4) + ' USDT';
    q('upnl').textContent  = (d.unrealized_pnl >= 0 ? '+' : '') + d.unrealized_pnl.toFixed(4) + ' USDT';
    q('wr').textContent    = d.win_rate != null ? d.win_rate.toFixed(1) + '% (' + d.wins + '✅/' + d.losses + '❌)' : 'N/A';
    q('pos').textContent   = d.open_count + ' — ' + d.open_longs + 'L / ' + d.open_shorts + 'S';
    q('lev').textContent   = d.leverage + 'x';
    q('sig_rx').textContent  = d.executor_status.signals_received;
    q('sig_ok').textContent  = d.executor_status.signals_open + ' abiertas / ' + d.executor_status.signals_close + ' cerradas';
    q('sig_rej').textContent = d.executor_status.signals_rejected;
    q('last_sig').textContent= d.executor_status.last_signal_time + ' — ' + d.executor_status.last_signal_detail;
    q('ws_sym').textContent  = 'WS activo: ' + d.ws_symbols;

    const ob = document.getElementById('open_body');
    if (!d.open_trades.length) {
      ob.innerHTML = '<tr><td colspan="11" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones abiertas</td></tr>';
    } else {
      ob.innerHTML = d.open_trades.map(t => {
        const dir = t.direction === 'LONG' ? '<span style="color:#3fb950">🟢 LONG</span>' : '<span style="color:#f85149">🔴 SHORT</span>';
        const pnl = t.pnl_usdt >= 0 ? `<span style="color:#3fb950">+${t.pnl_usdt.toFixed(4)}</span>` : `<span style="color:#f85149">${t.pnl_usdt.toFixed(4)}</span>`;
        const roi = t.roi_pct  >= 0 ? `<span style="color:#3fb950">+${t.roi_pct.toFixed(2)}%</span>` : `<span style="color:#f85149">${t.roi_pct.toFixed(2)}%</span>`;
        const assum = t.order_assumed ? ' ⚠️' : '';
        return `<tr>
          <td>#${t.id}</td><td><b>${t.symbol}</b></td><td>${dir}</td><td>${t.leverage}x</td>
          <td>$${t.entry_price.toFixed(6)}</td><td>$${t.current_price.toFixed(6)}</td>
          <td>${pnl}</td><td>${roi}</td>
          <td>${t.notional.toFixed(4)} USDT</td><td>${t.quantity}</td>
          <td>${t.open_time}${assum}</td>
        </tr>`;
      }).join('');
    }

    const cb = document.getElementById('closed_body');
    const recent = d.closed_trades.slice(-30).reverse();
    if (!recent.length) {
      cb.innerHTML = '<tr><td colspan="9" style="color:#8b949e;text-align:center;padding:.8rem">Sin operaciones cerradas</td></tr>';
    } else {
      cb.innerHTML = recent.map(t => {
        const pnl = t.pnl_usdt >= 0 ? `<span style="color:#3fb950">+${t.pnl_usdt.toFixed(4)}</span>` : `<span style="color:#f85149">${t.pnl_usdt.toFixed(4)}</span>`;
        const res = t.status === 'TP' ? '<span style="color:#3fb950">✅ TP</span>' :
                    t.status === 'SL' ? '<span style="color:#f85149">❌ SL</span>' :
                    `<span style="color:#8b949e">${t.status}</span>`;
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
refresh();
setInterval(refresh, 5000);
</script>
"""


async def dashboard_handler(request: web.Request) -> web.Response:
    em  = execution_manager
    es  = executor_status
    env = "TESTNET 🧪" if USE_TESTNET else "REAL 🔴"

    closed = em.closed_trades
    wins   = sum(1 for t in closed if t.status == "TP")
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
  <title>Futures Executor v3</title>
  <style>
    body{{font-family:'Courier New',monospace;background:#0d1117;color:#c9d1d9;padding:1.2rem}}
    h1{{color:#f0883e;margin-bottom:.8rem;font-size:1.3rem}}
    h2{{color:#58a6ff;margin:.9rem 0 .5rem;font-size:.95rem}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:.6rem;margin-bottom:1.2rem}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:.75rem}}
    .card .label{{color:#8b949e;font-size:.7rem;margin-bottom:.25rem;text-transform:uppercase;letter-spacing:.04em}}
    .card .value{{color:#f0f6fc;font-size:.95rem;font-weight:bold}}
    .ok{{color:#3fb950}} .warn{{color:#d29922}} .err{{color:#f85149}} .orange{{color:#f0883e}}
    .wrap{{overflow-x:auto;margin-bottom:1.2rem}}
    table{{width:100%;border-collapse:collapse;font-size:.77rem;min-width:600px}}
    th{{color:#8b949e;text-align:left;padding:.35rem .45rem;border-bottom:1px solid #30363d;white-space:nowrap;font-size:.71rem}}
    td{{padding:.3rem .45rem;border-bottom:1px solid #1c2128;white-space:nowrap}}
    tr:hover td{{background:#161b22}}
    .dot{{display:inline-block;width:8px;height:8px;background:#3fb950;border-radius:50%;margin-right:5px;animation:blink 1.5s infinite}}
    @keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
    .info-banner{{background:#161b22;border:1px solid #58a6ff;border-radius:6px;padding:.6rem 1rem;margin-bottom:1rem;color:#58a6ff;font-size:.82rem}}
  </style>
</head>
<body>
  <h1>⚡ Futures Executor v3 — Binance USDT Perpetuos [{env}]</h1>

  <div class="info-banner">
    📡 Recibe señales de <b>app.py</b> y ejecuta en Binance Futures.
    Apertura vía <b>Algo Order</b> (STOP_MARKET) → fallback a Market Order.
    Sin TP/SL propios — el cierre lo gestiona app.py.
    Leverage: <b>{LEVERAGE}x</b> | Poll posiciones: <b>{POSITION_POLL_S}s</b>
  </div>

  <div class="grid">
    <div class="card"><div class="label">Balance USDT</div>
      <div class="value ok" id="bal">{em.balance:.2f} USDT</div></div>
    <div class="card"><div class="label">Equity total</div>
      <div class="value" style="color:{eq_col}" id="eq">{em.equity:.2f} USDT</div></div>
    <div class="card"><div class="label">PnL realizado</div>
      <div class="value" style="color:{rp_col}" id="rpnl">{em.total_realized_pnl:+.4f} USDT</div></div>
    <div class="card"><div class="label">PnL no realizado</div>
      <div class="value" style="color:{up_col}" id="upnl">{em.unrealized_pnl:+.4f} USDT</div></div>
    <div class="card"><div class="label">Win Rate</div>
      <div class="value warn" id="wr">{wr_str} ({wins}✅/{losses}❌)</div></div>
    <div class="card"><div class="label">Posiciones abiertas</div>
      <div class="value" id="pos">{len(em.open_trades)} — {len(em.open_longs)}L / {len(em.open_shorts)}S</div></div>
    <div class="card"><div class="label">Leverage configurado</div>
      <div class="value warn" id="lev">{LEVERAGE}x</div></div>
  </div>

  <h2>📡 Señales Recibidas de app.py</h2>
  <div class="grid">
    <div class="card"><div class="label">Total recibidas</div>
      <div class="value orange" id="sig_rx">{es['signals_received']}</div></div>
    <div class="card"><div class="label">Ejecutadas</div>
      <div class="value ok" id="sig_ok">{es['signals_open']} abiertas / {es['signals_close']} cerradas</div></div>
    <div class="card"><div class="label">Rechazadas</div>
      <div class="value err" id="sig_rej">{es['signals_rejected']}</div></div>
    <div class="card" style="grid-column:span 2"><div class="label">Última señal</div>
      <div class="value" style="font-size:.8rem" id="last_sig">{es['last_signal_time']} — {es['last_signal_detail']}</div></div>
  </div>

  <h2><span class="dot"></span>📊 Posiciones Reales Abiertas</h2>
  <p id="ws_sym" style="color:#484f58;font-size:.72rem;margin-bottom:.4rem">
    WS activo: {", ".join(sorted(em.active_symbols)) or "ninguno"}
  </p>
  <div class="wrap"><table>
    <thead><tr>
      <th>ID</th><th>Par</th><th>Dirección</th><th>Lev</th>
      <th>Entrada</th><th>Precio actual</th>
      <th>PnL (USDT)</th><th>ROI%</th><th>Notional</th><th>Qty</th><th>Abierto</th>
    </tr></thead>
    <tbody id="open_body">
      <tr><td colspan="11" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones abiertas</td></tr>
    </tbody>
  </table></div>

  <h2>📋 Operaciones Cerradas (últimas 30)</h2>
  <div class="wrap"><table>
    <thead><tr>
      <th>#</th><th>Par</th><th>Dir</th><th>Lev</th>
      <th>Entrada</th><th>Salida</th><th>PnL (USDT)</th><th>ROI%</th><th>Resultado</th>
    </tr></thead>
    <tbody id="closed_body">
      <tr><td colspan="9" style="color:#8b949e;text-align:center;padding:.8rem">Sin operaciones cerradas</td></tr>
    </tbody>
  </table></div>

  <p style="color:#484f58;margin-top:.6rem;font-size:.7rem">
    Futures Executor v3 | Leverage: {LEVERAGE}x | Poll: {POSITION_POLL_S}s |
    Iniciado: {es['started_at']} |
    Actualizado: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
  </p>

  {DASHBOARD_JS}
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


# ══════════════════════════════════════════════════════════
#  HTTP SERVER
# ══════════════════════════════════════════════════════════
async def start_http_server():
    app = web.Application()
    app.router.add_post("/signal",    signal_handler)
    app.router.add_get("/",           dashboard_handler)
    app.router.add_get("/health",     dashboard_handler)
    app.router.add_get("/api/state",  api_state_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"Executor HTTP activo en http://0.0.0.0:{PORT}")


# ══════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════
async def main():
    global execution_manager

    env_tag = "TESTNET 🧪" if USE_TESTNET else "REAL 🔴"
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║   Futures Executor v3 — Binance USDT Perpetuos        ║")
    log.info(f"║   Entorno: {env_tag:<44}║")
    log.info(f"║   Leverage: {LEVERAGE}x | Poll: {POSITION_POLL_S}s                       ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    try:
        from binance_api_mejorado import BinanceAPI
    except ImportError:
        log.critical("No se puede importar BinanceAPI desde binance_api_mejorado.py")
        return

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        log.critical("BINANCE_API_KEY y BINANCE_API_SECRET son obligatorias")
        return

    try:
        api = BinanceAPI(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=USE_TESTNET)
        execution_manager = ExecutionManager(api)
        log.info(f"BinanceAPI inicializada | Testnet: {USE_TESTNET}")
    except Exception as e:
        log.critical(f"Error inicializando BinanceAPI: {e}")
        return

    await execution_manager.refresh_balance()
    log.info(f"Balance USDT Futures: ${execution_manager.balance:.2f}")

    async with aiohttp.ClientSession() as sess:
        await send_telegram(
            sess,
            f"⚡ <b>Futures Executor v3 — {('TESTNET' if USE_TESTNET else 'REAL')} INICIADO</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Balance USDT:</b> <code>{execution_manager.balance:.2f} USDT</code>\n"
            f"⚡ <b>Leverage:</b> <code>{LEVERAGE}x</code>\n"
            f"⏱ <b>Poll posiciones:</b> cada <b>{POSITION_POLL_S}s</b>\n"
            f"📡 <b>Método apertura:</b> Algo Order → Market Order (fallback)\n"
            f"🔒 <b>Secreto compartido:</b> configurado ✅\n"
            f"⚠️ <b>Error -2019:</b> posición se registra como asumida",
        )

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            start_http_server(),
            ws_price_loop(session),
            position_monitor_loop(session),
        )


if __name__ == "__main__":
    asyncio.run(main())
