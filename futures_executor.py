"""
futures_executor.py v4.0 — Executor de Futuros Binance
══════════════════════════════════════════════════════════════════════
Recibe señales HTTP de app.py y ejecuta órdenes REALES en Binance
Futures USDT-M Perpetuos.

FLUJO:
  app.py → POST /signal → Executor → Binance Futures → Telegram

SEÑALES SOPORTADAS (JSON body):
  Apertura: {"action":"open",      "trade_id":1, "symbol":"BTCUSDT",
              "direction":"LONG",  "price":50000.0, "quantity":0.001}

  Cierre:   {"action":"close",     "trade_id":1, "symbol":"BTCUSDT",
              "direction":"LONG", "reason":"TP", "close_price":50500.0}

  Global:   {"action":"close_all"}

CIERRE MANUAL (NUEVO en v4.0):
  El dashboard (GET /) tiene un botón "Cerrar" por posición y un botón
  "Cerrar TODO" arriba de la tabla. Disparan POST /manual/close y
  POST /manual/close_all respectivamente. Estas rutas usan el mismo
  SIGNAL_SECRET (inyectado en el HTML) como token de autorización.

COMPORTAMIENTO:
  • Quantity y dirección se usan TAL CUAL llegan de app.py (sin recalcular)
  • Sin TP/SL automáticos — app.py gestiona el momento del cierre
  • Apertura via Algo Order (STOP_MARKET) como método principal
    → fallback automático a Market Order si falla
  • Error -2019 (margin insuficiente) → se registra igualmente como
    posición abierta (la orden se asume enviada)
  • Cierre via close_all_positions() de BinanceAPI:
    cancela TP/SL + Algo Orders + posición de mercado en un paso
  • Precios en tiempo real: vía ws.py (SymbolWebSocketPriceCache), NO por
    polling REST. El executor sólo se suscribe a los símbolos con
    posición abierta y actualiza la suscripción dinámicamente.
  • NINGUNA posición se cierra automáticamente "porque sí". El executor
    sólo cierra cuando:
      1. Llega una señal explícita {"action":"close"/"close_all"} de app.py
      2. Se presiona el botón de cierre manual en el dashboard
    Si una posición registrada localmente desaparece de Binance (cierre
    externo / liquidación / cierre manual hecho directo en el exchange),
    el executor SOLO envía una alerta por Telegram — no la marca como
    cerrada ni dispara ninguna orden por su cuenta.

VARIABLES DE ENTORNO:
  BINANCE_API_KEY      — clave API Binance
  BINANCE_API_SECRET   — secreto API Binance
  SIGNAL_SECRET        — token compartido con app.py (cabecera X-Signal-Secret)
                          también se usa como token del botón de cierre manual
  TELEGRAM_BOT_TOKEN   — token bot Telegram
  TELEGRAM_CHAT_ID     — chat ID Telegram
  USE_TESTNET          — "true" para testnet (default: false)
  PORT                 — puerto HTTP (default: 10000)
  LEVERAGE             — apalancamiento a usar en cada posición (default: 1)
  POSITION_POLL_S      — segundos entre verificaciones de cierre externo
                          (sólo genera alertas, nunca cierra por su cuenta)
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

    def __init__(self, binance_api, price_ws):
        self.api            = binance_api
        self.price_ws       = price_ws   # SymbolWebSocketPriceCache (ws.py)
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

    # ── Sincroniza la suscripción WS con los símbolos abiertos ───
    def _sync_ws_symbols(self):
        """Le dice a ws.py qué símbolos escuchar: sólo los que tienen
        posición abierta en este momento. Se llama cada vez que se abre
        o se cierra un trade."""
        try:
            self.price_ws.update_symbols(list(self._trades.keys()))
        except Exception as e:
            log.error(f"_sync_ws_symbols: {e}")

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

        # Empezar a recibir precio en tiempo real (WS) para este símbolo
        self._sync_ws_symbols()

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

        # Ya no hace falta el precio en tiempo real de este símbolo
        # (a menos que se haya abierto otra posición distinta en él)
        self._sync_ws_symbols()

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

        Sólo se llama desde:
          • signal_handler (señal explícita "close"/"close_all" de app.py)
          • manual_close_handler / manual_close_all_handler (botón del dashboard)
        Nunca se llama automáticamente por desaparición de una posición.
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
    async def close_all_global(self, reason: str = "CLOSE_ALL") -> list[Trade]:
        async with self._lock:
            trades_snapshot = list(self._trades.values())

        closed_trades = []
        for trade in trades_snapshot:
            try:
                closed = await self.force_close_trade(trade, reason=reason)
                if closed:
                    closed_trades.append(trade)
                    log.info(f"close_all_global: cerrado {trade.symbol} #{trade.id}")
            except Exception as e:
                log.error(f"close_all_global: error cerrando {trade.symbol}: {e}")

        log.info(
            f"close_all_global: {len(closed_trades)}/{len(trades_snapshot)} posiciones cerradas"
        )
        return closed_trades

    # ── Verificación de posibles cierres externos (SOLO ALERTA) ──
    async def poll_positions(self) -> list[Trade]:
        """
        Consulta las posiciones reales en Binance Futures y devuelve los
        trades registrados localmente que ya NO aparecen en el exchange
        (posible cierre externo, liquidación, o cierre hecho directo en
        Binance fuera de este executor).

        IMPORTANTE: esta función no cierra nada ni cambia el estado de
        ningún trade. Sólo informa para que se envíe una alerta — el
        cierre real sólo ocurre vía señal explícita de app.py o el botón
        de cierre manual del dashboard.
        """
        if not self._trades:
            return []

        loop = asyncio.get_event_loop()

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

        missing: list[Trade] = [
            trade for symbol, trade in open_copy.items()
            if symbol not in pos_by_symbol
        ]
        return missing

    def find_by_paper_id(self, paper_trade_id: int) -> Optional[Trade]:
        sym = self._paper_id_map.get(paper_trade_id)
        if sym:
            return self._trades.get(sym)
        return None


# ══════════════════════════════════════════════════════════
#  INSTANCIAS GLOBALES
# ══════════════════════════════════════════════════════════
execution_manager: Optional[ExecutionManager] = None

executor_status = {
    "signals_received"   : 0,
    "signals_open"       : 0,
    "signals_close"      : 0,
    "signals_rejected"   : 0,
    "manual_closes"      : 0,
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
        "CLOSE_ALL"    : ("🛑", "CIERRE GLOBAL (SEÑAL)"),
        "MANUAL"       : ("🖐", "CIERRE MANUAL (DASHBOARD)"),
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
#  MONITOR DE POSICIONES — SOLO ALERTA, NUNCA CIERRA
# ══════════════════════════════════════════════════════════
async def position_monitor_loop(session: aiohttp.ClientSession):
    """
    Cada POSITION_POLL_S segundos revisa si alguna posición registrada
    localmente desapareció de Binance. NO cierra nada por su cuenta:
    sólo avisa por Telegram para que el operador decida — vía botón de
    cierre manual del dashboard, o confirmando con una señal de app.py.
    """
    log.info(f"Position Monitor (sólo alertas, sin auto-cierre) — poll cada {POSITION_POLL_S}s")
    await asyncio.sleep(10)
    alerted: set[str] = set()

    while True:
        try:
            missing = await execution_manager.poll_positions()
            missing_symbols = {t.symbol for t in missing}

            for trade in missing:
                if trade.symbol in alerted:
                    continue
                alerted.add(trade.symbol)
                log.warning(
                    f"⚠️ {trade.symbol} (#{trade.id}) ya no aparece en Binance "
                    f"pero sigue registrado como OPEN localmente — NO se cierra automáticamente."
                )
                await send_telegram(
                    session,
                    f"⚠️ <b>POSIBLE CIERRE EXTERNO DETECTADO</b>\n"
                    f"📊 <code>{trade.symbol}</code> (Real #{trade.id} | Paper #{trade.paper_trade_id})\n"
                    f"Ya no aparece entre tus posiciones de Binance, pero el "
                    f"executor sigue registrándola como abierta.\n"
                    f"➡️ No se cerró automáticamente. Verifica en Binance y, si "
                    f"corresponde, usa el botón <b>Cerrar</b> del dashboard para "
                    f"sincronizar el registro local.",
                )

            # Deja de alertar para símbolos que ya no están "missing"
            # (volvieron a aparecer) o que ya no están registrados.
            alerted &= (missing_symbols | execution_manager.active_symbols)

        except Exception as e:
            log.error(f"position_monitor_loop: {e}")

        await asyncio.sleep(POSITION_POLL_S)


# ══════════════════════════════════════════════════════════
#  SYNC DE PRECIOS — lee la caché WS (ws.py), sin red, cada 1s
# ══════════════════════════════════════════════════════════
async def price_sync_loop():
    """
    Actualiza el precio actual / PnL no realizado de cada trade abierto
    leyendo la caché en memoria de ws.py (SymbolWebSocketPriceCache).
    No hace ninguna llamada de red — los precios ya llegan en tiempo
    real por WebSocket; este loop sólo los traslada a cada Trade.
    """
    log.info("Price Sync Loop — actualizando PnL desde caché WS cada 1s")
    while True:
        try:
            for trade in execution_manager.open_trades:
                price = execution_manager.price_ws.get_price(trade.symbol)
                if price:
                    trade.update_unrealized(price)
        except Exception as e:
            log.error(f"price_sync_loop: {e}")
        await asyncio.sleep(1)


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
            closed_trades = await execution_manager.close_all_global(reason="CLOSE_ALL")
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
#  ENDPOINTS /manual/close, /manual/close_all — botón del dashboard
# ══════════════════════════════════════════════════════════
def _check_dashboard_token(request: web.Request) -> bool:
    token = request.headers.get("X-Dashboard-Token", "")
    return token == SIGNAL_SECRET


async def manual_close_handler(request: web.Request) -> web.Response:
    """
    POST /manual/close
    Header: X-Dashboard-Token: <SIGNAL_SECRET>
    Body: {"symbol": "BTCUSDT"}

    Disparado por el botón "Cerrar" de una fila en el dashboard.
    Es la ÚNICA forma (junto a /manual/close_all y la señal "close" de
    app.py) de cerrar una posición — nada se cierra solo.
    """
    if not _check_dashboard_token(request):
        log.warning(f"manual_close_handler: token inválido desde {request.remote}")
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    symbol = data.get("symbol", "").upper()
    trade  = execution_manager._trades.get(symbol)

    if not trade:
        return web.json_response(
            {"ok": False, "error": f"no hay posición abierta registrada para {symbol}"},
            status=404,
        )

    log.warning(f"🖐 CIERRE MANUAL solicitado desde dashboard → {symbol} (#{trade.id})")

    async def _do_manual_close():
        closed = await execution_manager.force_close_trade(trade, reason="MANUAL")
        if closed:
            executor_status["signals_close"]  += 1
            executor_status["manual_closes"]  += 1
            async with aiohttp.ClientSession() as sess:
                await send_telegram(sess, build_close_message(trade))

    asyncio.create_task(_do_manual_close())
    return web.json_response({"ok": True, "action": "manual_close", "symbol": symbol})


async def manual_close_all_handler(request: web.Request) -> web.Response:
    """
    POST /manual/close_all
    Header: X-Dashboard-Token: <SIGNAL_SECRET>

    Disparado por el botón "Cerrar TODO" del dashboard.
    """
    if not _check_dashboard_token(request):
        log.warning(f"manual_close_all_handler: token inválido desde {request.remote}")
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    total_open = len(execution_manager.open_trades)
    log.warning(f"🖐 CIERRE MANUAL GLOBAL solicitado desde dashboard — {total_open} posiciones")

    async def _do_close_all():
        closed_trades = await execution_manager.close_all_global(reason="MANUAL")
        async with aiohttp.ClientSession() as sess:
            if not closed_trades:
                await send_telegram(
                    sess, "🖐 Cierre manual global ejecutado — no había posiciones abiertas.",
                )
                return
            for t in closed_trades:
                await send_telegram(sess, build_close_message(t))
            await send_telegram(
                sess,
                f"🖐 Cierre manual global completado — {len(closed_trades)} posición(es) cerrada(s).",
            )

    asyncio.create_task(_do_close_all())
    executor_status["signals_close"] += total_open
    executor_status["manual_closes"] += total_open
    return web.json_response({
        "ok": True, "action": "manual_close_all", "positions_targeted": total_open,
    })


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
DASHBOARD_JS_TEMPLATE = """
<script>
const DASH_TOKEN = __DASH_TOKEN__;

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
    q('ws_sym').textContent  = 'WS activo (ws.py): ' + d.ws_symbols;
    q('close_all_btn').disabled = d.open_count === 0;

    const ob = document.getElementById('open_body');
    if (!d.open_trades.length) {
      ob.innerHTML = '<tr><td colspan="12" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones abiertas</td></tr>';
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
          <td><button class="btn-close" onclick="closeTrade('${t.symbol}')">Cerrar</button></td>
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
                    t.status === 'MANUAL' ? '<span style="color:#58a6ff">🖐 MANUAL</span>' :
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

async function closeTrade(symbol) {
  if (!confirm('¿Cerrar manualmente la posición ' + symbol + '?')) return;
  try {
    const r = await fetch('/manual/close', {
      method: 'POST',
      headers: {'Content-Type': 'application/json', 'X-Dashboard-Token': DASH_TOKEN},
      body: JSON.stringify({symbol: symbol})
    });
    const d = await r.json();
    if (!d.ok) alert('Error al cerrar ' + symbol + ': ' + (d.error || 'desconocido'));
    refresh();
  } catch(e) { alert('Error de red al cerrar ' + symbol); }
}

async function closeAllTrades() {
  if (!confirm('¿Cerrar TODAS las posiciones abiertas manualmente? Esta acción no se puede deshacer.')) return;
  try {
    const r = await fetch('/manual/close_all', {
      method: 'POST',
      headers: {'X-Dashboard-Token': DASH_TOKEN}
    });
    const d = await r.json();
    if (!d.ok) alert('Error al cerrar todas las posiciones: ' + (d.error || 'desconocido'));
    refresh();
  } catch(e) { alert('Error de red al cerrar todas las posiciones'); }
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

    # El token del dashboard se inyecta como JSON para evitar problemas de
    # escapado de comillas en el secreto (viene de una variable de entorno).
    dashboard_js = DASHBOARD_JS_TEMPLATE.replace("__DASH_TOKEN__", json.dumps(SIGNAL_SECRET))

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Futures Executor v4</title>
  <style>
    body{{font-family:'Courier New',monospace;background:#0d1117;color:#c9d1d9;padding:1.2rem}}
    h1{{color:#f0883e;margin-bottom:.8rem;font-size:1.3rem}}
    h2{{color:#58a6ff;margin:.9rem 0 .5rem;font-size:.95rem;display:flex;align-items:center;gap:.6rem}}
    .grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(155px,1fr));gap:.6rem;margin-bottom:1.2rem}}
    .card{{background:#161b22;border:1px solid #30363d;border-radius:6px;padding:.75rem}}
    .card .label{{color:#8b949e;font-size:.7rem;margin-bottom:.25rem;text-transform:uppercase;letter-spacing:.04em}}
    .card .value{{color:#f0f6fc;font-size:.95rem;font-weight:bold}}
    .ok{{color:#3fb950}} .warn{{color:#d29922}} .err{{color:#f85149}} .orange{{color:#f0883e}}
    .wrap{{overflow-x:auto;margin-bottom:1.2rem}}
    table{{width:100%;border-collapse:collapse;font-size:.77rem;min-width:660px}}
    th{{color:#8b949e;text-align:left;padding:.35rem .45rem;border-bottom:1px solid #30363d;white-space:nowrap;font-size:.71rem}}
    td{{padding:.3rem .45rem;border-bottom:1px solid #1c2128;white-space:nowrap}}
    tr:hover td{{background:#161b22}}
    .dot{{display:inline-block;width:8px;height:8px;background:#3fb950;border-radius:50%;margin-right:5px;animation:blink 1.5s infinite}}
    @keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
    .info-banner{{background:#161b22;border:1px solid #58a6ff;border-radius:6px;padding:.6rem 1rem;margin-bottom:1rem;color:#58a6ff;font-size:.82rem}}
    .btn-close{{background:#f85149;color:#fff;border:none;border-radius:4px;padding:.25rem .6rem;font-size:.7rem;font-family:inherit;cursor:pointer}}
    .btn-close:hover{{background:#da3633}}
    .btn-close-all{{background:#f85149;color:#fff;border:none;border-radius:5px;padding:.4rem .9rem;font-size:.78rem;font-family:inherit;cursor:pointer;font-weight:bold}}
    .btn-close-all:hover{{background:#da3633}}
    .btn-close-all:disabled{{background:#30363d;color:#6e7681;cursor:not-allowed}}
  </style>
</head>
<body>
  <h1>⚡ Futures Executor v4 — Binance USDT Perpetuos [{env}]</h1>

  <div class="info-banner">
    📡 Recibe señales de <b>app.py</b> y ejecuta en Binance Futures.
    Apertura vía <b>Algo Order</b> (STOP_MARKET) → fallback a Market Order.
    Precios en tiempo real vía <b>WebSocket (ws.py)</b> — sin polling.
    🔒 <b>Ninguna posición se cierra automáticamente:</b> sólo por señal explícita
    de app.py o por el botón <b>Cerrar</b> de este dashboard.
    Leverage: <b>{LEVERAGE}x</b> | Verificación de cierre externo (sólo alerta): cada <b>{POSITION_POLL_S}s</b>
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

  <h2>
    <span class="dot"></span>📊 Posiciones Reales Abiertas
    <button class="btn-close-all" id="close_all_btn" onclick="closeAllTrades()" {"disabled" if not em.open_trades else ""}>🛑 Cerrar TODO</button>
  </h2>
  <p id="ws_sym" style="color:#484f58;font-size:.72rem;margin-bottom:.4rem">
    WS activo (ws.py): {", ".join(sorted(em.active_symbols)) or "ninguno"}
  </p>
  <div class="wrap"><table>
    <thead><tr>
      <th>ID</th><th>Par</th><th>Dirección</th><th>Lev</th>
      <th>Entrada</th><th>Precio actual</th>
      <th>PnL (USDT)</th><th>ROI%</th><th>Notional</th><th>Qty</th><th>Abierto</th><th>Acción</th>
    </tr></thead>
    <tbody id="open_body">
      <tr><td colspan="12" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones abiertas</td></tr>
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
    Futures Executor v4 | Leverage: {LEVERAGE}x | Alerta cierre externo: {POSITION_POLL_S}s |
    Iniciado: {es['started_at']} |
    Actualizado: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}
  </p>

  {dashboard_js}
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


# ══════════════════════════════════════════════════════════
#  HTTP SERVER
# ══════════════════════════════════════════════════════════
async def start_http_server():
    app = web.Application()
    app.router.add_post("/signal",           signal_handler)
    app.router.add_post("/manual/close",     manual_close_handler)
    app.router.add_post("/manual/close_all", manual_close_all_handler)
    app.router.add_get("/",          dashboard_handler)
    app.router.add_get("/health",    dashboard_handler)
    app.router.add_get("/api/state", api_state_handler)
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
    log.info("║   Futures Executor v4 — Binance USDT Perpetuos        ║")
    log.info(f"║   Entorno: {env_tag:<44}║")
    log.info(f"║   Leverage: {LEVERAGE}x | Alerta cierre ext.: {POSITION_POLL_S}s              ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    try:
        from binance_api_mejorado import BinanceAPI
    except ImportError:
        log.critical("No se puede importar BinanceAPI desde binance_api_mejorado.py")
        return

    try:
        from WS import SymbolWebSocketPriceCache
    except ImportError:
        log.critical("No se puede importar SymbolWebSocketPriceCache desde ws.py")
        return

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        log.critical("BINANCE_API_KEY y BINANCE_API_SECRET son obligatorias")
        return

    try:
        api = BinanceAPI(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=USE_TESTNET)
        # Arranca sin símbolos: se suscribe dinámicamente sólo a los
        # símbolos con posición abierta (ver ExecutionManager._sync_ws_symbols)
        price_ws = SymbolWebSocketPriceCache([])
        price_ws.start()
        execution_manager = ExecutionManager(api, price_ws)
        log.info(f"BinanceAPI inicializada | Testnet: {USE_TESTNET}")
    except Exception as e:
        log.critical(f"Error inicializando BinanceAPI / WS: {e}")
        return

    await execution_manager.refresh_balance()
    log.info(f"Balance USDT Futures: ${execution_manager.balance:.2f}")

    async with aiohttp.ClientSession() as sess:
        await send_telegram(
            sess,
            f"⚡ <b>Futures Executor v4 — {('TESTNET' if USE_TESTNET else 'REAL')} INICIADO</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Balance USDT:</b> <code>{execution_manager.balance:.2f} USDT</code>\n"
            f"⚡ <b>Leverage:</b> <code>{LEVERAGE}x</code>\n"
            f"📡 <b>Precios:</b> WebSocket (ws.py) — sin polling\n"
            f"📡 <b>Método apertura:</b> Algo Order → Market Order (fallback)\n"
            f"🔒 <b>Cierre:</b> sólo por señal de app.py o botón manual del dashboard\n"
            f"🔒 <b>Secreto compartido:</b> configurado ✅\n"
            f"⚠️ <b>Error -2019:</b> posición se registra como asumida",
        )

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            start_http_server(),
            price_sync_loop(),
            position_monitor_loop(session),
        )


if __name__ == "__main__":
    asyncio.run(main())


import asyncio
import websockets
import json
import threading
import time
import math
from datetime import datetime
from typing import NamedTuple
import os


# ── Helpers de módulo ──────────────────────────────────────────────────────

def _safe_float(d: dict, key: str, default: float = 0.0) -> float:
    try:
        return float(d.get(key, default))
    except (ValueError, TypeError):
        return default


class TickerData(NamedTuple):
    change_pct: float   # % cambio 24h  (campo 'P')
    change_abs: float   # cambio abs 24h (campo 'p')
    last_price: float   # último precio  (campo 'c')
    high_24h:   float   # máximo 24h     (campo 'h')
    low_24h:    float   # mínimo 24h     (campo 'l')
    volume_24h: float   # volumen base   (campo 'v')
    quote_vol:  float   # volumen cotizado(campo 'q')
    ts:         float   # timestamp UNIX


# Límite de Binance: 1024 streams por conexión combinada
_BINANCE_MAX_STREAMS = 1024


class SymbolWebSocketPriceCache:
    """WebSocket para múltiples símbolos usando el máximo de streams por conexión.

    Binance permite 1024 streams por conexión combinada.
    Con 570 símbolos:
      • 1 conexión para todos los @markPrice@1s  (570 streams)
      • 1 conexión para todos los @ticker         (570 streams)
    Total: 2 conexiones independientemente del número de símbolos (hasta 1024).

    Streams activos por símbolo:
      • @markPrice@1s  → precio mark en tiempo real
      • @ticker        → cambio 24h, high/low, volumen

    NOVEDAD: la lista de símbolos puede actualizarse en caliente con
    `update_symbols()` (p.ej. desde futures_executor.py, que sólo necesita
    escuchar los símbolos con posición abierta en cada momento). Al llamar
    a `update_symbols()` se fuerza la reconexión de ambos streams para que
    el cambio se aplique de inmediato — es seguro llamarlo desde otro hilo.
    """

    _WS_MAX_SIZE  = 131_072   # 128 KB — con 570 símbolos los msgs siguen siendo < 2 KB c/u
    _WS_MAX_QUEUE = 64        # cola pequeña; se procesa en tiempo real

    def __init__(self, symbols: list[str], symbols_per_connection: int | None = None):
        # symbols_per_connection se mantiene por compatibilidad con código existente.
        # Ya no se usa: con el límite de 1024 streams de Binance, todos los símbolos
        # caben en 1 conexión por tipo de stream (markPrice + ticker = 2 conexiones total).
        if symbols_per_connection is not None:
            print(
                f"[WS] ⚠️  symbols_per_connection={symbols_per_connection} ignorado — "
                f"todos los símbolos van en 1 conexión por stream (límite Binance: 1024)."
            )

        self.symbols = sorted({s.upper() for s in symbols})

        if len(self.symbols) > _BINANCE_MAX_STREAMS:
            # Si superas 1024 símbolos Binance rechaza la conexión;
            # en ese caso habría que dividir en 2 conexiones por tipo.
            raise ValueError(
                f"Binance permite máximo {_BINANCE_MAX_STREAMS} streams por conexión. "
                f"Tienes {len(self.symbols)} símbolos. "
                f"Separa en dos instancias o implementa chunking."
            )

        # price_cache:  symbol -> (mark_price, timestamp)
        self.price_cache:  dict[str, tuple[float, float]] = {}
        # ticker_cache: symbol -> TickerData (incluye ts)
        self.ticker_cache: dict[str, TickerData] = {}

        self.tasks:   list  = []
        self.lock           = threading.Lock()
        self.running        = False
        self._loop           = None

        # Referencias a las conexiones activas (para poder forzar reconexión
        # desde update_symbols, incluso desde otro hilo)
        self._markprice_ws = None
        self._ticker_ws    = None

        # Estadísticas simples por stream (solo 2 keys: "markprice", "ticker")
        self.connection_stats: dict[str, dict] = {
            "markprice": {"reconnects": 0, "last_error": None},
            "ticker":    {"reconnects": 0, "last_error": None},
        }

    # ──────────────────────────────────────────────────────────────────────
    # Construcción de URLs
    # ──────────────────────────────────────────────────────────────────────

    def _build_url(self, stream_suffix: str) -> str:
        """URL combinada con todos los símbolos actuales para un tipo de stream.
        Se reconstruye en cada intento de conexión, así que siempre refleja
        la lista de símbolos más reciente (ver update_symbols)."""
        with self.lock:
            symbols_snapshot = list(self.symbols)
        streams = "/".join(f"{s.lower()}{stream_suffix}" for s in symbols_snapshot)
        return f"wss://fstream.binance.com/market/stream?streams={streams}"

    # ──────────────────────────────────────────────────────────────────────
    # Conexión WS compartida
    # ──────────────────────────────────────────────────────────────────────

    def _ws_connect(self, url: str):
        return websockets.connect(
            url,
            ping_interval=30,
            ping_timeout=10,
            close_timeout=10,
            max_size=self._WS_MAX_SIZE,
            max_queue=self._WS_MAX_QUEUE,
            compression=None,
        )

    async def _reconnect_loop(self, stream_id: str, coro_factory):
        """Bucle genérico de reconexión con backoff exponencial."""
        reconnect_delay    = 1
        consecutive_errors = 0

        while self.running:
            try:
                await coro_factory()
                reconnect_delay    = 1
                consecutive_errors = 0
            except Exception as e:
                consecutive_errors += 1
                self.connection_stats[stream_id]["reconnects"] += 1
                self.connection_stats[stream_id]["last_error"]  = str(e)
                reconnect_delay = min(reconnect_delay * 1.5, 30)
                if consecutive_errors > 5:
                    reconnect_delay = 60
                print(f"🔴 [{stream_id}] Error: {e}")
                print(f"   Reconectando en {reconnect_delay:.1f}s (intento #{consecutive_errors})")
                await asyncio.sleep(reconnect_delay)

    # ──────────────────────────────────────────────────────────────────────
    # Stream 1 – Mark Price (UNA sola conexión para TODOS los símbolos)
    # ──────────────────────────────────────────────────────────────────────

    async def _ws_markprice(self):

        async def _connect():
            with self.lock:
                symbols_snapshot = list(self.symbols)

            if not symbols_snapshot:
                # Sin símbolos activos (p.ej. executor sin posiciones abiertas):
                # no hay nada que escuchar todavía, esperar a que se asignen.
                await asyncio.sleep(2)
                return

            url = self._build_url("@markPrice@1s")
            async with self._ws_connect(url) as ws:
                self._markprice_ws = ws
                print(f"✅ [markPrice] Conectado — {len(symbols_snapshot)} símbolos en 1 conexión")
                last_ping = time.time()

                try:
                    while self.running:
                        try:
                            msg  = await asyncio.wait_for(ws.recv(), timeout=45)
                            data = json.loads(msg)

                            if "data" in data:
                                pd     = data["data"]
                                symbol = pd.get("s", "").upper()
                                price  = float(pd.get("p", 0.0))

                                if symbol and math.isfinite(price) and price > 0:
                                    with self.lock:
                                        self.price_cache[symbol] = (price, time.time())

                            now = time.time()
                            if now - last_ping > 30:
                                await ws.ping()
                                last_ping = now

                        except asyncio.TimeoutError:
                            print("⏰ [markPrice] Timeout, enviando ping…")
                            await ws.ping()
                            last_ping = time.time()

                        except websockets.ConnectionClosed as e:
                            print(f"🔶 [markPrice] Conexión cerrada: {e}")
                            raise
                finally:
                    self._markprice_ws = None

        await self._reconnect_loop("markprice", _connect)

    # ──────────────────────────────────────────────────────────────────────
    # Stream 2 – Ticker 24h (UNA sola conexión para TODOS los símbolos)
    # ──────────────────────────────────────────────────────────────────────

    async def _ws_ticker(self):

        async def _connect():
            with self.lock:
                symbols_snapshot = list(self.symbols)

            if not symbols_snapshot:
                await asyncio.sleep(2)
                return

            url = self._build_url("@ticker")
            async with self._ws_connect(url) as ws:
                self._ticker_ws = ws
                print(f"✅ [ticker24h] Conectado — {len(symbols_snapshot)} símbolos en 1 conexión")
                last_ping = time.time()

                try:
                    while self.running:
                        try:
                            msg  = await asyncio.wait_for(ws.recv(), timeout=45)
                            data = json.loads(msg)

                            if "data" in data:
                                d      = data["data"]
                                symbol = d.get("s", "").upper()
                                if not symbol:
                                    continue

                                with self.lock:
                                    self.ticker_cache[symbol] = TickerData(
                                        change_pct = _safe_float(d, "P"),
                                        change_abs = _safe_float(d, "p"),
                                        last_price = _safe_float(d, "c"),
                                        high_24h   = _safe_float(d, "h"),
                                        low_24h    = _safe_float(d, "l"),
                                        volume_24h = _safe_float(d, "v"),
                                        quote_vol  = _safe_float(d, "q"),
                                        ts         = time.time(),
                                    )

                            now = time.time()
                            if now - last_ping > 30:
                                await ws.ping()
                                last_ping = now

                        except asyncio.TimeoutError:
                            print("⏰ [ticker24h] Timeout, enviando ping…")
                            await ws.ping()
                            last_ping = time.time()

                        except websockets.ConnectionClosed as e:
                            print(f"🔶 [ticker24h] Conexión cerrada: {e}")
                            raise
                finally:
                    self._ticker_ws = None

        await self._reconnect_loop("ticker", _connect)

    # ──────────────────────────────────────────────────────────────────────
    # Monitor de salud
    # ──────────────────────────────────────────────────────────────────────

    async def _monitor_health(self):
        """Monitorea la salud de las 2 conexiones cada 60 segundos."""
        while self.running:
            await asyncio.sleep(60)
            now          = time.time()
            stale_price  = []
            stale_ticker = []

            with self.lock:
                symbols_snapshot = list(self.symbols)
                for symbol in symbols_snapshot:
                    entry = self.price_cache.get(symbol)
                    if entry is None or now - entry[1] > 120:
                        stale_price.append(symbol)

                    ticker = self.ticker_cache.get(symbol)
                    if ticker is None or now - ticker.ts > 120:
                        stale_ticker.append(symbol)

            if stale_price:
                print(f"⚠️ [markPrice] Sin actualización ({len(stale_price)} símbolos): "
                      f"{stale_price[:5]}{'…' if len(stale_price) > 5 else ''}")
            if stale_ticker:
                print(f"⚠️ [ticker24h] Sin actualización ({len(stale_ticker)} símbolos): "
                      f"{stale_ticker[:5]}{'…' if len(stale_ticker) > 5 else ''}")

    # ──────────────────────────────────────────────────────────────────────
    # Actualización dinámica de símbolos
    # ──────────────────────────────────────────────────────────────────────

    def update_symbols(self, symbols: list[str]):
        """Reemplaza la lista de símbolos suscritos y fuerza la reconexión
        de ambos streams para que el cambio se aplique de inmediato.

        Pensado para consumidores como futures_executor.py, que sólo
        necesitan precios de los símbolos con posición abierta en cada
        momento (en vez de un set fijo de cientos de símbolos).

        Seguro de llamar desde otro hilo/loop distinto al que corre esta
        instancia (usa asyncio.run_coroutine_threadsafe internamente).
        """
        new_symbols = sorted({s.upper() for s in symbols})

        if len(new_symbols) > _BINANCE_MAX_STREAMS:
            raise ValueError(
                f"Binance permite máximo {_BINANCE_MAX_STREAMS} streams por conexión. "
                f"Se solicitaron {len(new_symbols)} símbolos."
            )

        with self.lock:
            if new_symbols == self.symbols:
                return
            self.symbols = new_symbols

        preview = new_symbols[:5]
        print(
            f"🔄 [WS] Símbolos actualizados → {len(new_symbols)} "
            f"({preview}{'…' if len(new_symbols) > 5 else ''})"
        )
        self._force_reconnect()

    def _force_reconnect(self):
        """Cierra las conexiones activas; _reconnect_loop las reabre de
        inmediato usando la lista de símbolos ya actualizada (o, si la
        lista quedó vacía, simplemente las deja en espera)."""
        if not self.running or self._loop is None:
            return
        for attr in ("_markprice_ws", "_ticker_ws"):
            ws_obj = getattr(self, attr, None)
            if ws_obj is not None:
                asyncio.run_coroutine_threadsafe(self._safe_close(ws_obj), self._loop)

    @staticmethod
    async def _safe_close(ws_obj):
        try:
            await ws_obj.close()
        except Exception:
            pass

    # ──────────────────────────────────────────────────────────────────────
    # Ciclo de vida
    # ──────────────────────────────────────────────────────────────────────

    def start(self):
        """Inicia las 2 conexiones WebSocket (markPrice + ticker 24h)."""
        self.running = True

        loop      = asyncio.new_event_loop()
        self._loop = loop

        threading.Thread(target=loop.run_forever, daemon=True).start()

        submit = lambda coro: self.tasks.append(
            asyncio.run_coroutine_threadsafe(coro, loop)
        )

        submit(self._ws_markprice())
        submit(self._ws_ticker())
        submit(self._monitor_health())

        print(f"✅ WebSocket cache iniciado — {len(self.symbols)} símbolos, 2 conexiones")

    def stop(self):
        """Detiene las conexiones."""
        print("🛑 Deteniendo WebSocket cache…")
        self.running = False
        time.sleep(2)

        for task in self.tasks:
            try:
                task.cancel()
            except Exception:
                pass

        if self._loop:
            self._loop.call_soon_threadsafe(self._loop.stop)

        print("✅ WebSocket cache detenido")

    # ──────────────────────────────────────────────────────────────────────
    # Getters – markPrice
    # ──────────────────────────────────────────────────────────────────────

    def get_price(self, symbol: str) -> float | None:
        with self.lock:
            entry = self.price_cache.get(symbol.upper())
            return entry[0] if entry else None

    def get_all_prices(self) -> dict[str, float]:
        with self.lock:
            return {sym: v[0] for sym, v in self.price_cache.items()}

    # ──────────────────────────────────────────────────────────────────────
    # Getters – Ticker 24h
    # ──────────────────────────────────────────────────────────────────────

    def get_change_24h(self, symbol: str) -> float | None:
        with self.lock:
            t = self.ticker_cache.get(symbol.upper())
            return t.change_pct if t else None

    def get_ticker(self, symbol: str) -> dict | None:
        """Ticker completo 24h: change_pct, change_abs, last_price,
        high_24h, low_24h, volume_24h, quote_vol. None si no hay datos."""
        with self.lock:
            t = self.ticker_cache.get(symbol.upper())
            if t is None:
                return None
            return {
                "change_pct": t.change_pct,
                "change_abs": t.change_abs,
                "last_price": t.last_price,
                "high_24h":   t.high_24h,
                "low_24h":    t.low_24h,
                "volume_24h": t.volume_24h,
                "quote_vol":  t.quote_vol,
            }

    def get_all_changes_24h(self) -> dict[str, float]:
        """% cambio 24h de todos los símbolos, ordenado mayor → menor."""
        with self.lock:
            result = {sym: t.change_pct for sym, t in self.ticker_cache.items()}
        return dict(sorted(result.items(), key=lambda x: x[1], reverse=True))

    def get_all_tickers(self) -> dict[str, dict]:
        with self.lock:
            return {
                sym: {
                    "change_pct": t.change_pct,
                    "change_abs": t.change_abs,
                    "last_price": t.last_price,
                    "high_24h":   t.high_24h,
                    "low_24h":    t.low_24h,
                    "volume_24h": t.volume_24h,
                    "quote_vol":  t.quote_vol,
                }
                for sym, t in self.ticker_cache.items()
            }

    # ──────────────────────────────────────────────────────────────────────
    # Utilidades
    # ──────────────────────────────────────────────────────────────────────

    def get_stale_symbols(self, max_age_seconds: int = 60) -> list[str]:
        """Símbolos cuyo markPrice no se ha actualizado en max_age_seconds."""
        now = time.time()
        with self.lock:
            return [
                s for s in self.symbols
                if now - self.price_cache.get(s, (0, 0))[1] > max_age_seconds
            ]

    def get_stats(self) -> dict:
        with self.lock:
            active_prices  = len(self.price_cache)
            active_tickers = len(self.ticker_cache)
            total_symbols  = len(self.symbols)

        return {
            "total_symbols":    total_symbols,
            "active_prices":    active_prices,
            "active_tickers":   active_tickers,
            "stale_symbols":    len(self.get_stale_symbols()),
            "connection_stats": self.connection_stats,
        }


# ══════════════════════════════════════════════════════════════════════════
# Ejemplo de uso
# ══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Ejemplo con 40 símbolos — reemplaza con tus 570
    symbols = [
        "BTCUSDT",  "ETHUSDT",  "BNBUSDT",  "ADAUSDT",  "DOGEUSDT",
        "XRPUSDT",  "DOTUSDT",  "UNIUSDT",  "LINKUSDT", "LTCUSDT",
        "SOLUSDT",  "MATICUSDT","AVAXUSDT", "ATOMUSDT", "FILUSDT",
        "VETUSDT",  "TRXUSDT",  "ETCUSDT",  "XLMUSDT",  "THETAUSDT",
        "AAVEUSDT", "ALGOUSDT", "ICPUSDT",  "SHIBUSDT", "NEARUSDT",
        "LUNAUSDT", "AXSUSDT",  "SANDUSDT", "MANAUSDT", "GALAUSDT",
        "APEUSDT",  "GMTUSDT",  "OPUSDT",   "ARBUSDT",  "APTUSDT",
        "LDOUSDT",  "STXUSDT",  "IMXUSDT",  "INJUSDT",  "SUIUSDT",
    ]

    cache = SymbolWebSocketPriceCache(symbols)
    cache.start()

    print("⏳ Esperando datos iniciales…")
    time.sleep(3)

    try:
        while True:
            time.sleep(1)
            os.system("cls" if os.name == "nt" else "clear")

            now     = datetime.now().strftime("%H:%M:%S")
            changes = cache.get_all_changes_24h()

            col_w = 32
            print("=" * (col_w * 2 + 4))
            print(f"  📊 Cambio 24h – todos los símbolos ({now})")
            print("=" * (col_w * 2 + 4))
            print(f"  {'SÍMBOLO':<12} {'PRECIO':>14} {'24H %':>9}   "
                  f"{'SÍMBOLO':<12} {'PRECIO':>14} {'24H %':>9}")
            print("-" * (col_w * 2 + 4))

            items = list(changes.items())
            half  = (len(items) + 1) // 2

            for i in range(half):
                sym_l, pct_l = items[i]
                price_l      = cache.get_price(sym_l)
                price_str_l  = f"${price_l:.4f}" if price_l else "–"
                arrow_l      = "▲" if pct_l >= 0 else "▼"
                pct_str_l    = f"{arrow_l}{abs(pct_l):.2f}%"
                color_l      = "\033[92m" if pct_l >= 0 else "\033[91m"

                row = (f"  {color_l}{sym_l:<12}{'\033[0m'} "
                       f"{price_str_l:>14} "
                       f"{color_l}{pct_str_l:>9}{'\033[0m'}")

                if i + half < len(items):
                    sym_r, pct_r = items[i + half]
                    price_r      = cache.get_price(sym_r)
                    price_str_r  = f"${price_r:.4f}" if price_r else "–"
                    arrow_r      = "▲" if pct_r >= 0 else "▼"
                    pct_str_r    = f"{arrow_r}{abs(pct_r):.2f}%"
                    color_r      = "\033[92m" if pct_r >= 0 else "\033[91m"

                    row += (f"   {color_r}{sym_r:<12}{'\033[0m'} "
                            f"{price_str_r:>14} "
                            f"{color_r}{pct_str_r:>9}{'\033[0m'}")

                print(row)

            stats = cache.get_stats()
            print("=" * (col_w * 2 + 4))
            print(f"  📈 Precios activos : {stats['active_prices']}/{stats['total_symbols']}  |  "
                  f"Tickers 24h : {stats['active_tickers']}/{stats['total_symbols']}  |  "
                  f"Obsoletos : {stats['stale_symbols']}")

            stale = cache.get_stale_symbols(max_age_seconds=30)
            if stale:
                print(f"  ⚠️  Sin update (>30s): {stale[:6]}")

            print("=" * (col_w * 2 + 4))

    except KeyboardInterrupt:
        print("\n🛑 Deteniendo…")
        cache.stop()
        print("✅ Finalizado")
