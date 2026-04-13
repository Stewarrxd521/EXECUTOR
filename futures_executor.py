"""
futures_executor.py — Bot Ejecutor de Futuros Binance
══════════════════════════════════════════════════════════════════════
Recibe señales HTTP del bot principal (paper trading) y ejecuta
órdenes REALES en Binance Futures USDT Perpetuos.

⚠️  MODO CONTRARIAN ACTIVO:
    Las señales del bot principal se INVIERTEN antes de ejecutarse:
      LONG  → SHORT  (TP y SL también se calculan en la dirección opuesta)
      SHORT → LONG

Flujo:
  Bot Principal → POST /signal  →  Ejecutor (invierte dirección)  →  Binance Futures
                                              ↓
                                     Telegram (confirmación real)
                                              ↓
                             Poll de posiciones / WebSocket
                             para detectar TP, SL, liquidación

Variables de entorno requeridas en Render:
  BINANCE_API_KEY      — clave real de Binance
  BINANCE_API_SECRET   — secreto real de Binance
  SIGNAL_SECRET        — token compartido con el bot principal
  TELEGRAM_BOT_TOKEN   — token del bot de Telegram
  TELEGRAM_CHAT_ID     — chat donde enviar notificaciones
  USE_TESTNET          — "true" para testnet (default: false)
  PORT                 — puerto HTTP (default: 10000)
  TP_PCT               — Take Profit % (default: 1.0)
  SL_PCT               — Stop Loss %   (default: 2.0)
  MAX_LONGS            — máx posiciones LONG simultáneas (default: 10)
  MAX_SHORTS           — máx posiciones SHORT simultáneas (default: 10)
  QTY_MULTIPLIER       — multiplicador sobre la qty mínima (default: 2.0)
  POSITION_POLL_S      — segundos entre polls de posiciones (default: 30)

CAMBIOS v2:
  ✅ FIX 1: force_close_trade usaba run_in_executor mal (llamaba la función
            directamente en vez de pasarla como lambda). Corregido.
  ✅ FIX 2: signal_handler "close" ahora SIEMPRE llama close_all_positions
            en Binance aunque no haya trade registrado localmente.
  ✅ NEW:   verify_and_repair_tp_sl() verifica en cada poll que cada posición
            abierta tenga sus órdenes TP y SL condicionales. Si falta alguna,
            la coloca automáticamente.
"""

import asyncio
import aiohttp
from aiohttp import web
import logging
from datetime import datetime, timezone
import os
import json
from dataclasses import dataclass
from typing import Optional
from decimal import Decimal, ROUND_DOWN

# ══════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════
BINANCE_API_KEY    = os.environ.get("BINANCE_API_KEY",    "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
USE_TESTNET        = os.environ.get("USE_TESTNET", "false").lower() == "true"

# Token compartido con el bot principal para autenticar señales
SIGNAL_SECRET = os.environ.get("SIGNAL_SECRET", "cambiar-por-secreto-seguro")

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8700613197:AAFu7KAP3_9joN8Jq76r3ZcKIZiGcUWzSc4")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID",   "1474510598")

# ── Trading ────────────────────────────────────────────────
TP_PCT         = float(os.environ.get("TP_PCT",         "1.0"))
SL_PCT         = float(os.environ.get("SL_PCT",         "2.0"))
MAX_LONGS      = int(os.environ.get("MAX_LONGS",        "10"))
MAX_SHORTS     = int(os.environ.get("MAX_SHORTS",       "10"))
QTY_MULTIPLIER = float(os.environ.get("QTY_MULTIPLIER", "2.0"))

# ── Bot ────────────────────────────────────────────────────
PORT             = int(os.environ.get("PORT",            "10000"))
POSITION_POLL_S  = int(os.environ.get("POSITION_POLL_S", "30"))

# ══════════════════════════════════════════════════════════
#  MODO CONTRARIAN — invierte la dirección de toda señal
# ══════════════════════════════════════════════════════════
CONTRARIAN_MODE = True   # ← Cambiar a False para volver al modo normal

def invert_direction(direction: str) -> str:
    """Invierte LONG→SHORT y SHORT→LONG."""
    return "SHORT" if direction.upper() == "LONG" else "LONG"

# ── Binance endpoints ──────────────────────────────────────
BINANCE_FAPI_WS = "wss://fstream.binance.com"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("Executor")

# ══════════════════════════════════════════════════════════
#  MODELO DE TRADE REAL
# ══════════════════════════════════════════════════════════
@dataclass
class Trade:
    id                    : int
    symbol                : str
    direction             : str       # "LONG" | "SHORT"  (ya invertida si CONTRARIAN_MODE)
    direction_original    : str       # dirección que envió el bot principal
    entry_price           : float
    quantity              : float
    open_time             : str
    tp_price              : float
    sl_price              : float
    leverage              : int
    paper_trade_id        : int = 0   # ID del trade en el bot principal
    entry_order_id        : str = ""
    current_price         : float = 0.0
    status                : str   = "OPEN"   # OPEN | TP | SL | LIQUIDATED | MANUAL
    close_price           : float = 0.0
    close_time            : str   = ""
    pnl_usdt              : float = 0.0
    roi_pct               : float = 0.0

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
#  POSITION SIZER — leverage máximo y qty mínima válida
# ══════════════════════════════════════════════════════════
def _round_qty_step(quantity: float, step_size: float) -> float:
    if step_size <= 0:
        return quantity
    return float(Decimal(str(quantity)).quantize(
        Decimal(str(step_size)), rounding=ROUND_DOWN
    ))


class PositionSizer:
    """Calcula leverage máximo y quantity mínima válida para un símbolo de Futuros."""

    def __init__(self, api):
        self.api           = api
        self._sym_filters  : dict = {}
        self._lev_brackets : dict = {}

    def _load_symbol_filters(self, symbol: str) -> dict:
        symbol = symbol.upper()
        if symbol in self._sym_filters:
            return self._sym_filters[symbol]
        try:
            info = self.api.client.futures_exchange_info()
            for s in info.get("symbols", []):
                sym = s.get("symbol", "").upper()
                if not sym:
                    continue
                filters = {f["filterType"]: f for f in s.get("filters", [])}
                lot = filters.get("LOT_SIZE", {})
                mn  = filters.get("MIN_NOTIONAL", {})
                pf  = filters.get("PRICE_FILTER", {})
                self._sym_filters[sym] = {
                    "minQty"     : float(lot.get("minQty",   0)),
                    "stepSize"   : float(lot.get("stepSize", 0)),
                    "minNotional": float(mn.get("notional",  0)),
                    "tickSize"   : float(pf.get("tickSize",  0)),
                }
        except Exception as e:
            log.error(f"_load_symbol_filters({symbol}): {e}")
        return self._sym_filters.get(symbol, {})

    def _load_leverage_brackets(self, symbol: str) -> list:
        symbol = symbol.upper()
        if symbol in self._lev_brackets:
            return self._lev_brackets[symbol]
        try:
            data = self.api.client.futures_leverage_bracket(symbol=symbol)
            if isinstance(data, list):
                for d in data:
                    if d.get("symbol", "").upper() == symbol:
                        self._lev_brackets[symbol] = d.get("brackets", [])
                        break
                else:
                    if data:
                        self._lev_brackets[symbol] = data[0].get("brackets", [])
            elif isinstance(data, dict):
                self._lev_brackets[symbol] = data.get("brackets", [])
        except Exception as e:
            log.error(f"_load_leverage_brackets({symbol}): {e}")
        return self._lev_brackets.get(symbol, [])

    def max_leverage(self, symbol: str) -> int:
        brackets = self._load_leverage_brackets(symbol)
        if brackets:
            try:
                return int(brackets[0].get("initialLeverage", 20))
            except Exception:
                pass
        return 20

    def _min_notional_for_leverage(self, symbol: str, leverage: int) -> float:
        brackets = self._load_leverage_brackets(symbol)
        for br in brackets:
            try:
                init_lev = int(br.get("initialLeverage", 0))
            except Exception:
                continue
            if leverage <= init_lev:
                for key in ("notionalFloor", "notionalCap"):
                    val = br.get(key)
                    if val is not None:
                        try:
                            return float(val)
                        except Exception:
                            pass
                break
        return 0.0

    def calculate_quantity(self, symbol: str, price: float, leverage: int) -> float:
        f = self._load_symbol_filters(symbol)
        min_qty      = f.get("minQty",      0.0)
        min_notional = f.get("minNotional", 0.0)
        step_size    = f.get("stepSize",    0.0)

        min_qty_notional = (min_notional * 1.2) / price if price > 0 else 0.0
        min_notional_lev = self._min_notional_for_leverage(symbol, leverage)
        min_qty_lev_brk  = (min_notional_lev * 1.1) / price if min_notional_lev and price > 0 else 0.0

        base_qty  = max(min_qty, min_qty_notional, min_qty_lev_brk)
        final_qty = base_qty * QTY_MULTIPLIER

        if step_size > 0:
            final_qty = _round_qty_step(final_qty, step_size)

        return float(final_qty)

    def round_price(self, symbol: str, price: float) -> float:
        f    = self._load_symbol_filters(symbol)
        tick = f.get("tickSize", 0.0)
        if tick > 0:
            return float(Decimal(str(price)).quantize(Decimal(str(tick)), rounding=ROUND_DOWN))
        return round(price, 8)


# ══════════════════════════════════════════════════════════
#  GESTOR DE TRADES REALES
# ══════════════════════════════════════════════════════════
class ExecutionManager:
    """
    Ejecuta y gestiona posiciones reales en Binance Futures.
    En CONTRARIAN_MODE invierte la dirección de cada señal antes de enviarla.
    """

    def __init__(self, binance_api):
        self.api      = binance_api
        self.sizer    = PositionSizer(binance_api)
        self._trades  : dict[str, Trade] = {}
        self._closed  : list[Trade]      = []
        self._counter : int = 0
        self._lock    = asyncio.Lock()
        self._balance : float = 0.0
        self._paper_id_map : dict[int, str] = {}
        self._conditional_orders : dict[str, dict] = {}

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

    # ── Abrir posición real ───────────────────────────────
    async def open_trade(
        self,
        symbol            : str,
        direction_original: str,   # dirección que envió el bot principal
        price             : float,
        paper_trade_id    : int = 0,
    ) -> Optional[Trade]:
        """
        Recibe la señal del bot principal.
        Si CONTRARIAN_MODE está activo, invierte la dirección antes de operar.
        """
        # ── Inversión de dirección ─────────────────────────
        if CONTRARIAN_MODE:
            direction = invert_direction(direction_original)
            log.info(
                f"[CONTRARIAN] Señal original: {direction_original} → "
                f"Ejecutando: {direction} en {symbol}"
            )
        else:
            direction = direction_original.upper()

        async with self._lock:
            if symbol in self._trades:
                log.warning(f"open_trade: {symbol} ya tiene posición abierta — ignorando")
                return None
            if direction == "LONG" and len(self.open_longs) >= MAX_LONGS:
                log.warning(f"open_trade: máximo LONG alcanzado para {symbol}")
                return None
            if direction == "SHORT" and len(self.open_shorts) >= MAX_SHORTS:
                log.warning(f"open_trade: máximo SHORT alcanzado para {symbol}")
                return None

        loop = asyncio.get_event_loop()

        # ── Leverage y quantity ────────────────────────────
        try:
            leverage = await loop.run_in_executor(None, self.sizer.max_leverage, symbol)
            quantity = await loop.run_in_executor(
                None, self.sizer.calculate_quantity, symbol, price, leverage
            )
        except Exception as e:
            log.error(f"open_trade: cálculo qty/leverage {symbol}: {e}")
            return None

        if quantity <= 0:
            log.warning(f"open_trade: qty inválida {quantity} para {symbol}")
            return None

                # ── Filtro de notional máximo ──────────────────────
        notional = price * quantity
        max_notional = 6.4 * QTY_MULTIPLIER
        if notional > max_notional:
            log.warning(
                f"open_trade: {symbol} BLOQUEADO — notional ${notional:.4f} "
                f"> límite ${max_notional:.4f} (6.4 × {QTY_MULTIPLIER})"
            )
            return None
        # ───────────────────────────────────────────────────

        # ── Precios TP / SL (calculados sobre la dirección REAL ejecutada) ──
        if CONTRARIAN_MODE:
            tp_pct_real = SL_PCT   # 2%
            sl_pct_real = TP_PCT   # 1%
        else:
            tp_pct_real = TP_PCT
            sl_pct_real = SL_PCT

        try:
            if direction == "LONG":
                tp_raw = price * (1 + tp_pct_real / 100)
                sl_raw = price * (1 - sl_pct_real / 100)
                side   = "BUY"
            else:  # SHORT
                tp_raw = price * (1 - tp_pct_real / 100)
                sl_raw = price * (1 + sl_pct_real / 100)
                side   = "SELL"

            tp_price = await loop.run_in_executor(None, self.sizer.round_price, symbol, tp_raw)
            sl_price = await loop.run_in_executor(None, self.sizer.round_price, symbol, sl_raw)
        except Exception as e:
            log.error(f"open_trade: redondeo TP/SL {symbol}: {e}")
            tp_price, sl_price = tp_raw, sl_raw

        # ── Configurar leverage en el exchange ─────────────
        try:
            await loop.run_in_executor(None, self.api.set_leverage, symbol, leverage)
            log.info(f"Leverage {leverage}x configurado → {symbol}")
        except Exception as e:
            log.warning(f"open_trade: no se pudo setear leverage {symbol}: {e}")

        # ── Ejecutar bracket_batch (entrada + TP + SL) ────
        try:
            result = await loop.run_in_executor(
                None,
                lambda: self.api.bracket_batch(
                    symbol      = symbol,
                    side        = side,
                    quantity    = quantity,
                    entry_type  = "MARKET",
                    take_profit = tp_price,
                    stop_loss   = sl_price,
                )
            )
        except Exception as e:
            log.error(f"open_trade: bracket_batch {symbol}: {e}")
            return None

        if not result:
            log.error(f"open_trade: bracket_batch retornó vacío para {symbol}")
            return None

        entry_order_id = ""
        tp_order_id    = ""
        sl_order_id    = ""
        TP_TYPES = {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"}
        SL_TYPES = {"STOP_MARKET", "STOP"}
        try:
            for r in result:
                if not isinstance(r, dict):
                    continue
                otype = r.get("type", "")
                oid   = str(r.get("orderId", ""))
                if otype in TP_TYPES:
                    tp_order_id = oid
                elif otype in SL_TYPES:
                    sl_order_id = oid
                elif otype in ("MARKET", None, "") and not entry_order_id:
                    entry_order_id = oid
            if not entry_order_id and result:
                entry_order_id = str(result[0].get("orderId", ""))
        except Exception:
            pass

        # ── Registrar el trade ────────────────────────────
        async with self._lock:
            if symbol in self._trades:
                log.warning(f"open_trade: race condition en {symbol} — posición ya registrada")
                return None

            self._counter += 1
            trade = Trade(
                id                 = self._counter,
                symbol             = symbol,
                direction          = direction,
                direction_original = direction_original,
                entry_price        = price,
                quantity           = quantity,
                open_time          = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                tp_price           = tp_price,
                sl_price           = sl_price,
                leverage           = leverage,
                paper_trade_id     = paper_trade_id,
                entry_order_id     = entry_order_id,
                current_price      = price,
            )
            self._trades[symbol] = trade
            self._paper_id_map[paper_trade_id] = symbol
            self._conditional_orders[symbol] = {
                "tp_id": tp_order_id,
                "sl_id": sl_order_id,
            }

        log.info(
            f"[REAL #{trade.id}] ABIERTO {direction} {symbol} @ ${price:.8f} "
            f"(señal original: {direction_original}) "
            f"| TP: ${tp_price:.8f} | SL: ${sl_price:.8f} "
            f"| Qty: {quantity} | Lev: {leverage}x | Paper#{paper_trade_id}"
        )
        await self.refresh_balance()
        return trade

    # ── Cerrar posición (sólo estado interno) ─────────────
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

            trade.roi_pct = (trade.pnl_usdt / trade.notional_usdt * 100) if trade.notional_usdt else 0.0

            del self._trades[trade.symbol]
            self._conditional_orders.pop(trade.symbol, None)
            self._paper_id_map.pop(trade.paper_trade_id, None)
            self._closed.append(trade)

        log.info(
            f"[REAL #{trade.id}] CERRADO {reason} {trade.symbol} "
            f"@ ${close_price:.8f} | PnL: {trade.pnl_usdt:+.4f} USDT ({trade.roi_pct:+.2f}%)"
        )
        await self.refresh_balance()
        return True

    # ── Cierre forzado enviando orden al exchange ─────────
    # ✅ FIX 1: run_in_executor recibía el resultado de la función ya ejecutada
    #           en vez de una callable. Ahora se pasa como lambda correctamente.
    async def force_close_trade(self, trade: Trade, reason: str = "MAIN_BOT") -> bool:
        loop = asyncio.get_event_loop()
        close_price = trade.current_price or trade.entry_price

        # Cancelar todas las órdenes abiertas del símbolo (TP, SL, LIMIT, ALGO)
        try:
            await loop.run_in_executor(
                None,
                lambda: self.api.close_all_positions(symbol=trade.symbol)  # ← CORREGIDO: lambda
            )
            log.info(f"force_close: close_all_positions ejecutado para {trade.symbol}")
        except Exception as e:
            log.warning(f"force_close: error en close_all_positions {trade.symbol}: {e}")

        return await self.close_trade(trade, close_price, reason)

    # ── Cierre directo por símbolo (sin trade local registrado) ───
    # ✅ NEW: usado por signal_handler cuando el bot principal pide cerrar
    #        pero el executor no tiene el trade en su estado interno.
    async def force_close_by_symbol(self, symbol: str) -> bool:
        """
        Cierra todas las posiciones de `symbol` directamente en Binance
        sin necesitar un objeto Trade local. Útil cuando el executor se
        reinició y perdió el estado, pero el bot principal envía un cierre.
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

    async def close_all_global(self) -> list[Trade]:
        """
        Cierra una a una todas las posiciones abiertas de todos los simbolos.
        Retorna la lista de trades que fueron cerrados exitosamente.
        """
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

        self._conditional_orders.clear()
        log.info(f"close_all_global: {len(closed_trades)}/{len(trades_snapshot)} posiciones cerradas")
        return closed_trades

    # ── Verificar y reparar TP/SL de una posición ─────────
    # ✅ NEW: comprueba si faltan órdenes condicionales TP o SL y las coloca.
    async def verify_and_repair_tp_sl(self, trade: Trade) -> None:
        symbol = trade.symbol
        loop   = asyncio.get_event_loop()

        stored = self._conditional_orders.get(symbol, {})
        stored_tp_id = stored.get("tp_id", "")
        stored_sl_id = stored.get("sl_id", "")

        try:
            open_orders = await loop.run_in_executor(
                None,
                lambda: self.api.client.futures_get_open_orders(symbol=symbol)
            )
        except Exception as e:
            log.error(f"verify_tp_sl [{symbol}]: error obteniendo ordenes: {e}")
            return

        open_ids = {str(o.get("orderId", "")) for o in open_orders}

        TP_TYPES = {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"}
        SL_TYPES = {"STOP_MARKET", "STOP"}

        has_tp = (stored_tp_id and stored_tp_id in open_ids) or \
                 any(o.get("type", "") in TP_TYPES for o in open_orders)
        has_sl = (stored_sl_id and stored_sl_id in open_ids) or \
                 any(o.get("type", "") in SL_TYPES for o in open_orders)

        if has_tp and has_sl:
            return

        direction = trade.direction
        log.warning(
            f"verify_tp_sl [{symbol}] {direction} — "
            f"TP={'OK' if has_tp else 'FALTA'} (id={stored_tp_id})  "
            f"SL={'OK' if has_sl else 'FALTA'} (id={stored_sl_id}) "
            f"-> reparando ordenes faltantes..."
        )

        if not has_tp:
            try:
                r = await loop.run_in_executor(
                    None,
                    lambda: self.api.set_take_profit(
                        symbol           = symbol,
                        take_profit_price= trade.tp_price,
                        position_side    = direction,
                    )
                )
                new_tp_id = str(r.get("orderId", "")) if isinstance(r, dict) else ""
                self._conditional_orders.setdefault(symbol, {})["tp_id"] = new_tp_id
                log.info(
                    f"verify_tp_sl [{symbol}]: TP colocado @ {trade.tp_price:.8f} "
                    f"({direction}) id={new_tp_id}"
                )
            except Exception as e:
                log.error(f"verify_tp_sl [{symbol}]: error colocando TP: {e}")

        if not has_sl:
            try:
                r = await loop.run_in_executor(
                    None,
                    lambda: self.api.set_stop_loss(
                        symbol      = symbol,
                        stop_price  = trade.sl_price,
                        position_side= direction,
                    )
                )
                new_sl_id = str(r.get("orderId", "")) if isinstance(r, dict) else ""
                self._conditional_orders.setdefault(symbol, {})["sl_id"] = new_sl_id
                log.info(
                    f"verify_tp_sl [{symbol}]: SL colocado @ {trade.sl_price:.8f} "
                    f"({direction}) id={new_sl_id}"
                )
            except Exception as e:
                log.error(f"verify_tp_sl [{symbol}]: error colocando SL: {e}")

    # ── Poll de posiciones reales en Binance ──────────────
    # ✅ MEJORADO: ahora también verifica/repara TP y SL de posiciones abiertas.
    async def poll_positions(self) -> list[tuple]:
        """
        Consulta todas las posiciones abiertas en Binance Futures.
        Para cada posición monitoreada localmente:
          • Si desapareció en Binance → la registra como cerrada (TP o SL).
          • Si sigue abierta          → verifica que tenga TP y SL, y los
                                        coloca si alguno falta.
        Retorna lista de (Trade, close_price, reason) de posiciones cerradas.
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

        repair_tasks = []

        for symbol, trade in open_copy.items():
            if symbol in pos_by_symbol:
                # ── Posición sigue abierta en Binance ─────
                p       = pos_by_symbol[symbol]
                mark_px = float(p.get("markPrice") or p.get("entryPrice") or trade.entry_price)
                trade.update_unrealized(mark_px)

                # Verificar TP/SL en background (no bloqueante para el poll)
                repair_tasks.append(self.verify_and_repair_tp_sl(trade))

            else:
                # ── Posición desapareció → fue cerrada externamente ──
                cp = trade.current_price if trade.current_price > 0 else trade.entry_price
                if trade.direction == "LONG":
                    reason = "TP" if cp >= trade.tp_price * 0.999 else "SL"
                else:
                    reason = "TP" if cp <= trade.tp_price * 1.001 else "SL"
                closed_events.append((trade, cp, reason))

        # Ejecutar verificaciones TP/SL en paralelo
        if repair_tasks:
            await asyncio.gather(*repair_tasks, return_exceptions=True)

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
    "signals_received"  : 0,
    "signals_open"      : 0,
    "signals_close"     : 0,
    "signals_rejected"  : 0,
    "last_signal_time"  : "Esperando señales...",
    "last_signal_detail": "",
    "started_at"        : datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
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
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            if resp.status != 200:
                log.error(f"Telegram error {resp.status}: {await resp.text()}")
    except Exception as e:
        log.error(f"Error Telegram: {e}")


def build_open_message_real(trade: Trade) -> str:
    emoji = "🟢" if trade.direction == "LONG" else "🔴"
    word  = "LONG  ▲" if trade.direction == "LONG" else "SHORT ▼"
    base  = trade.symbol.replace("USDT", "")

    is_contrarian = CONTRARIAN_MODE and trade.direction != trade.direction_original
    tp_pct_shown = SL_PCT if is_contrarian else TP_PCT
    sl_pct_shown = TP_PCT if is_contrarian else SL_PCT
    contrarian_note = (
        f"\n🔀 <b>Contrarian:</b> señal {trade.direction_original} → ejecutado {trade.direction}"
        f"\n    TP={tp_pct_shown}% (era SL) | SL={sl_pct_shown}% (era TP)"
    ) if is_contrarian else ""

    return (
        f"{emoji} <b>🏦 POSICIÓN REAL ABIERTA — {word}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Par:</b>         <code>{trade.symbol}</code>\n"
        f"💰 <b>Entrada:</b>    <code>${trade.entry_price:,.8f}</code>\n"
        f"📦 <b>Cantidad:</b>   <code>{trade.quantity:.6f} {base}</code>\n"
        f"💹 <b>Notional:</b>   <code>{trade.notional_usdt:.2f} USDT</code>\n"
        f"⚡ <b>Leverage:</b>   <code>{trade.leverage}x</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🎯 <b>Take Profit:</b> <code>${trade.tp_price:,.8f}</code>  "
        f"<i>(+{tp_pct_shown}%)</i>\n"
        f"🛑 <b>Stop Loss:</b>   <code>${trade.sl_price:,.8f}</code>  "
        f"<i>(-{sl_pct_shown}%)</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🆔 Real <b>#{trade.id}</b>  |  Paper <b>#{trade.paper_trade_id}</b>"
        f"{contrarian_note}\n"
        f"⏱ {trade.open_time}\n"
        f"💼 Balance: <code>{execution_manager.balance:.2f} USDT</code>"
    )


def build_close_message_real(trade: Trade) -> str:
    reason_map = {
        "TP"        : ("✅", "TAKE PROFIT 🎯"),
        "SL"        : ("❌", "STOP LOSS 🛑"),
        "LIQUIDATED": ("💀", "LIQUIDACIÓN ⚠️"),
        "MAIN_BOT"  : ("🔄", "CIERRE SEÑAL PRINCIPAL"),
        "MANUAL"    : ("🖐", "CIERRE MANUAL"),
    }
    emoji, reason_str = reason_map.get(trade.status, ("⚠️", trade.status))
    dir_str   = "🟢 LONG" if trade.direction == "LONG" else "🔴 SHORT"
    pnl_emoji = "💚" if trade.pnl_usdt >= 0 else "❗"

    closed_all = execution_manager.closed_trades
    wins  = sum(1 for t in closed_all if t.status == "TP")
    total = len(closed_all)
    wr    = f"{wins/total*100:.1f}% ({wins}✅/{total-wins}❌)" if total else "N/A"

    contrarian_note = ""
    if CONTRARIAN_MODE and trade.direction != trade.direction_original:
        contrarian_note = (
            f"\n🔀 <b>Contrarian:</b> señal {trade.direction_original} → ejecutado {trade.direction}"
        )

    return (
        f"{emoji} <b>🏦 POSICIÓN REAL CERRADA — {reason_str}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Par:</b>      <code>{trade.symbol}</code>  {dir_str}\n"
        f"💵 <b>Entrada:</b> <code>${trade.entry_price:,.8f}</code>\n"
        f"💵 <b>Salida:</b>  <code>${trade.close_price:,.8f}</code>\n"
        f"⚡ <b>Lev:</b>     <code>{trade.leverage}x</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{pnl_emoji} <b>PnL:</b>    <code>{trade.pnl_usdt:+.4f} USDT</code>\n"
        f"📊 <b>ROI:</b>    <code>{trade.roi_pct:+.2f}%</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏱ Abierto:  {trade.open_time}\n"
        f"⏱ Cerrado:  {trade.close_time}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 <b>Balance:</b>  <code>{execution_manager.balance:.2f} USDT</code>\n"
        f"💼 <b>Equity:</b>   <code>{execution_manager.equity:.2f} USDT</code>\n"
        f"📈 <b>Win Rate:</b> <code>{wr}</code>"
        f"{contrarian_note}\n"
        f"🆔 Real <b>#{trade.id}</b>  |  Paper <b>#{trade.paper_trade_id}</b>"
    )


# ══════════════════════════════════════════════════════════
#  WEBSOCKET — Precios en tiempo real (Futures miniTicker)
# ══════════════════════════════════════════════════════════
async def ws_price_loop(session: aiohttp.ClientSession):
    log.info("WebSocket Futures Price Manager — iniciado")
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
            # poll_positions ahora también verifica/repara TP y SL internamente
            closed_events = await execution_manager.poll_positions()
            for trade, close_price, reason in closed_events:
                closed = await execution_manager.close_trade(trade, close_price, reason)
                if closed:
                    await send_telegram(session, build_close_message_real(trade))
        except Exception as e:
            log.error(f"position_monitor_loop: {e}")

        await asyncio.sleep(POSITION_POLL_S)


# ══════════════════════════════════════════════════════════
#  ENDPOINT /signal — Recibe señales del bot principal
# ══════════════════════════════════════════════════════════
async def signal_handler(request: web.Request) -> web.Response:
    """
    POST /signal
    Headers: X-Signal-Secret: <SIGNAL_SECRET>
    Body JSON:
      Apertura: {"action":"open",  "trade_id":1, "symbol":"BTCUSDT",
                 "direction":"LONG", "price":50000.0}
      Cierre:   {"action":"close", "trade_id":1, "symbol":"BTCUSDT",
                 "direction":"LONG", "reason":"TP", "close_price":50500.0}
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

    executor_status["signals_received"] += 1
    executor_status["last_signal_time"]   = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    executor_status["last_signal_detail"] = f"{action.upper()} {symbol}"

    log.info(f"Señal recibida: action={action} symbol={symbol} trade_id={trade_id}")

    # ── APERTURA ─────────────────────────────────────────
    if action == "open":
        direction_original = data.get("direction", "").upper()
        price              = float(data.get("price", 0))

        if not symbol or not direction_original or price <= 0:
            executor_status["signals_rejected"] += 1
            return web.json_response({"ok": False, "error": "missing open params"}, status=400)

        async def _do_open():
            trade = await execution_manager.open_trade(
                symbol             = symbol,
                direction_original = direction_original,
                price              = price,
                paper_trade_id     = trade_id,
            )
            if trade:
                executor_status["signals_open"] += 1
                async with aiohttp.ClientSession() as sess:
                    await send_telegram(sess, build_open_message_real(trade))
            else:
                executor_status["signals_rejected"] += 1
                log.warning(f"open_trade rechazado para {symbol} ({direction_original})")

        asyncio.create_task(_do_open())
        return web.json_response({"ok": True, "action": "open", "symbol": symbol})

    # ── CIERRE ────────────────────────────────────────────
    # ✅ FIX 2: ahora SIEMPRE se llama close_all_positions en Binance,
    #           incluso si no hay trade registrado localmente (executor
    #           reiniciado, estado perdido, etc.). El símbolo del mensaje
    #           del bot principal es suficiente para cerrar la posición.
    elif action == "close":
        reason = data.get("reason", "MAIN_BOT").upper()

        # Buscar trade local (por paper_id primero, luego por símbolo)
        trade = execution_manager.find_by_paper_id(trade_id)
        if not trade:
            trade = execution_manager._trades.get(symbol)

        async def _do_close():
            if trade:
                # Caso normal: tenemos el trade registrado localmente
                closed = await execution_manager.force_close_trade(trade, reason="MAIN_BOT")
                if closed:
                    executor_status["signals_close"] += 1
                    async with aiohttp.ClientSession() as sess:
                        await send_telegram(sess, build_close_message_real(trade))
            else:
                # ✅ Caso crítico: no hay trade local (executor reiniciado, etc.)
                # Aun así cerramos en Binance usando sólo el símbolo.
                log.warning(
                    f"signal_handler close: sin trade local para "
                    f"paper#{trade_id} / {symbol} — "
                    f"enviando close_all_positions directo a Binance"
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
                            f"se cerró directamente en Binance</i>\n"
                            f"🆔 Paper#{trade_id} | Razón: {reason}"
                        )

        asyncio.create_task(_do_close())
        return web.json_response({"ok": True, "action": "close", "symbol": symbol})

    # ── CIERRE GLOBAL ─────────────────────────────────────
    elif action == "close_all":
        log.warning("signal_handler: CIERRE GLOBAL recibido — cerrando todas las posiciones")
        total_open = len(execution_manager.open_trades)

        async def _do_close_all():
            closed_trades = await execution_manager.close_all_global()
            async with aiohttp.ClientSession() as sess:
                if not closed_trades:
                    await send_telegram(
                        sess,
                        "CIERRE GLOBAL ejecutado — no habia posiciones abiertas."
                    )
                    return
                for t in closed_trades:
                    await send_telegram(sess, build_close_message_real(t))
                await send_telegram(
                    sess,
                    f"CIERRE GLOBAL completado — {len(closed_trades)} posicion(es) cerrada(s)."
                )

        asyncio.create_task(_do_close_all())
        executor_status["signals_close"] += total_open
        return web.json_response({
            "ok": True,
            "action": "close_all",
            "positions_targeted": total_open,
        })

    else:
        executor_status["signals_rejected"] += 1
        return web.json_response({"ok": False, "error": f"unknown action: {action}"}, status=400)


# ══════════════════════════════════════════════════════════
#  DASHBOARD HTML  (sin cambios funcionales)
# ══════════════════════════════════════════════════════════
DASHBOARD_JS = """
<script>
async function refreshState() {
  try {
    const r = await fetch('/api/state');
    const d = await r.json();
    document.getElementById('bal').textContent  = d.balance.toFixed(2)  + ' USDT';
    document.getElementById('eq').textContent   = d.equity.toFixed(2)   + ' USDT';
    document.getElementById('rpnl').textContent = (d.realized_pnl >= 0 ? '+' : '') + d.realized_pnl.toFixed(4) + ' USDT';
    document.getElementById('upnl').textContent = (d.unrealized_pnl >= 0 ? '+' : '') + d.unrealized_pnl.toFixed(4) + ' USDT';
    document.getElementById('wr').textContent   = d.win_rate != null ? d.win_rate.toFixed(1) + '% (' + d.wins + '✅/' + d.losses + '❌)' : 'N/A';
    document.getElementById('pos').textContent  = d.open_count + ' — ' + d.open_longs + 'L / ' + d.open_shorts + 'S';
    document.getElementById('sig_rx').textContent  = d.executor_status.signals_received;
    document.getElementById('sig_ok').textContent  = d.executor_status.signals_open + ' abiertas / ' + d.executor_status.signals_close + ' cerradas';
    document.getElementById('sig_rej').textContent = d.executor_status.signals_rejected;
    document.getElementById('last_sig').textContent= d.executor_status.last_signal_time + ' — ' + d.executor_status.last_signal_detail;
    document.getElementById('ws_sym').textContent  = 'WS activo: ' + d.ws_symbols;

    // Open trades
    const ob = document.getElementById('open_body');
    if (d.open_trades.length === 0) {
      ob.innerHTML = '<tr><td colspan="14" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones reales abiertas</td></tr>';
    } else {
      ob.innerHTML = d.open_trades.map(t => {
        const dir = t.direction === 'LONG' ? '<span style="color:#3fb950">🟢 LONG</span>' : '<span style="color:#f85149">🔴 SHORT</span>';
        const pnl = t.pnl_usdt >= 0 ? `<span style="color:#3fb950">+${t.pnl_usdt.toFixed(4)}</span>` : `<span style="color:#f85149">${t.pnl_usdt.toFixed(4)}</span>`;
        const roi = t.roi_pct >= 0 ? `<span style="color:#3fb950">+${t.roi_pct.toFixed(2)}%</span>` : `<span style="color:#f85149">${t.roi_pct.toFixed(2)}%</span>`;
        return `<tr><td>#${t.id}</td><td><b>${t.symbol}</b></td><td>${dir}</td><td>${t.leverage}x</td>
          <td>$${t.entry_price.toFixed(6)}</td><td>$${t.current_price.toFixed(6)}</td>
          <td style="color:#d29922">$${t.tp_price.toFixed(6)}</td>
          <td style="color:#f85149">$${t.sl_price.toFixed(6)}</td>
          <td>${pnl}</td><td>${roi}</td>
          <td>${t.notional.toFixed(2)} USDT</td><td>${t.quantity}</td><td>${t.open_time}</td></tr>`;
      }).join('');
    }

    // Closed trades
    const cb = document.getElementById('closed_body');
    const recent = d.closed_trades.slice(-20).reverse();
    if (recent.length === 0) {
      cb.innerHTML = '<tr><td colspan="11" style="color:#8b949e;text-align:center;padding:.8rem">Sin operaciones cerradas</td></tr>';
    } else {
      cb.innerHTML = recent.map(t => {
        const res = t.status === 'TP' ? '<span style="color:#3fb950">✅ TP</span>' : '<span style="color:#f85149">❌ SL</span>';
        const pnl = t.pnl_usdt >= 0 ? `<span style="color:#3fb950">+${t.pnl_usdt.toFixed(4)}</span>` : `<span style="color:#f85149">${t.pnl_usdt.toFixed(4)}</span>`;
        return `<tr><td>#${t.id}</td><td>${t.symbol}</td><td>${t.direction}</td><td>${t.leverage}x</td>
          <td>$${t.entry_price.toFixed(6)}</td><td>$${t.close_price.toFixed(6)}</td>
          <td>${pnl}</td><td>${t.roi_pct.toFixed(2)}%</td>
          <td>${t.notional.toFixed(2)} USDT</td><td>${res}</td><td>${t.close_time}</td></tr>`;
      }).join('');
    }
  } catch(e) { console.error(e); }
}
refreshState();
setInterval(refreshState, 5000);
</script>
"""


async def api_state_handler(request: web.Request) -> web.Response:
    em = execution_manager

    def ser(t: Trade) -> dict:
        return {
            "id": t.id, "paper_trade_id": t.paper_trade_id,
            "symbol": t.symbol, "direction": t.direction,
            "direction_original": t.direction_original,
            "entry_price": t.entry_price, "quantity": t.quantity,
            "notional": t.notional_usdt, "leverage": t.leverage,
            "open_time": t.open_time, "tp_price": t.tp_price,
            "sl_price": t.sl_price, "current_price": t.current_price,
            "status": t.status, "close_price": t.close_price,
            "close_time": t.close_time, "pnl_usdt": t.pnl_usdt,
            "roi_pct": t.roi_pct,
        }

    closed = em.closed_trades
    wins   = sum(1 for t in closed if t.status == "TP")
    total  = len(closed)

    return web.json_response({
        "balance"         : em.balance,
        "equity"          : em.equity,
        "realized_pnl"    : em.total_realized_pnl,
        "unrealized_pnl"  : em.unrealized_pnl,
        "wins"            : wins,
        "losses"          : total - wins,
        "win_rate"        : (wins / total * 100) if total else None,
        "open_count"      : len(em.open_trades),
        "open_longs"      : len(em.open_longs),
        "open_shorts"     : len(em.open_shorts),
        "open_trades"     : [ser(t) for t in em.open_trades],
        "closed_trades"   : [ser(t) for t in closed],
        "executor_status" : executor_status,
        "ws_symbols"      : ", ".join(sorted(em.active_symbols)) or "ninguno",
    })


async def dashboard_handler(request: web.Request) -> web.Response:
    em        = execution_manager
    es        = executor_status
    env_label = "TESTNET 🧪" if USE_TESTNET else "REAL 🔴"
    mode_label= "CONTRARIAN 🔀" if CONTRARIAN_MODE else "NORMAL"
    ws_sym    = ", ".join(sorted(em.active_symbols)) or "ninguno"

    closed = em.closed_trades
    wins   = sum(1 for t in closed if t.status == "TP")
    losses = len(closed) - wins
    wr_str = f"{wins/len(closed)*100:.1f}%" if closed else "N/A"
    eq_col = "#3fb950" if em.equity >= em.balance else "#f85149"
    rp_col = "#3fb950" if em.total_realized_pnl >= 0 else "#f85149"
    up_col = "#3fb950" if em.unrealized_pnl >= 0 else "#f85149"

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Futures Executor Bot</title>
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
    table{{width:100%;border-collapse:collapse;font-size:.77rem;min-width:700px}}
    th{{color:#8b949e;text-align:left;padding:.35rem .45rem;border-bottom:1px solid #30363d;white-space:nowrap;font-size:.71rem}}
    td{{padding:.3rem .45rem;border-bottom:1px solid #1c2128;white-space:nowrap}}
    tr:hover td{{background:#161b22}}
    .dot{{display:inline-block;width:8px;height:8px;background:#3fb950;border-radius:50%;margin-right:5px;animation:blink 1.5s infinite}}
    .badge{{display:inline-block;padding:.1rem .4rem;border-radius:3px;font-size:.7rem;font-weight:bold;background:#161b22;border:1px solid #30363d}}
    .contrarian-banner{{background:#21262d;border:1px solid #f0883e;border-radius:6px;padding:.6rem 1rem;margin-bottom:1rem;color:#f0883e;font-size:.82rem}}
    @keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
  </style>
</head>
<body>
  <h1>⚡ Futures Executor Bot — Binance USDT Perpetuos [{env_label}]</h1>

  <div class="contrarian-banner">
    🔀 <b>Modo {mode_label}</b> activo —
    las señales del bot principal se invierten antes de ejecutar:
    LONG → SHORT &nbsp;|&nbsp; SHORT → LONG
  </div>

  <div class="grid">
    <div class="card"><div class="label">Balance USDT Futures</div>
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
    <div class="card"><div class="label">TP / SL</div>
      <div class="value"><span class="ok">+{TP_PCT}%</span> / <span class="err">-{SL_PCT}%</span></div></div>
    <div class="card"><div class="label">Qty multiplicador</div>
      <div class="value warn">{QTY_MULTIPLIER}× mínimo</div></div>
  </div>

  <h2>📡 Señales Recibidas del Bot Principal</h2>
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
  <p id="ws_sym" style="color:#484f58;font-size:.72rem;margin-bottom:.4rem">WS activo: {ws_sym}</p>
  <div class="wrap"><table>
    <thead><tr>
      <th>ID</th><th>Par</th><th>Dir (ejecutada)</th><th>Lev</th>
      <th>Entrada</th><th>Precio actual</th><th>Take Profit</th><th>Stop Loss</th>
      <th>PnL (USDT)</th><th>ROI%</th><th>Notional</th><th>Qty</th><th>Abierto</th>
    </tr></thead>
    <tbody id="open_body">
      <tr><td colspan="13" style="color:#8b949e;text-align:center;padding:.8rem">Sin posiciones reales abiertas</td></tr>
    </tbody>
  </table></div>

  <h2>📋 Operaciones Cerradas (últimas 20)</h2>
  <div class="wrap"><table>
    <thead><tr>
      <th>#</th><th>Par</th><th>Dir</th><th>Lev</th>
      <th>Entrada</th><th>Salida</th><th>PnL (USDT)</th><th>ROI%</th>
      <th>Notional</th><th>Resultado</th><th>Cerrado</th>
    </tr></thead>
    <tbody id="closed_body">
      <tr><td colspan="11" style="color:#8b949e;text-align:center;padding:.8rem">Sin operaciones cerradas</td></tr>
    </tbody>
  </table></div>

  <p style="color:#484f58;margin-top:.6rem;font-size:.7rem">
    Futures Executor [{mode_label}] | TP: +{TP_PCT}% | SL: -{SL_PCT}% |
    Poll posiciones: {POSITION_POLL_S}s |
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
    app.router.add_post("/signal",   signal_handler)
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

    mode_str = "CONTRARIAN 🔀 (señales invertidas)" if CONTRARIAN_MODE else "NORMAL"
    log.info("╔══════════════════════════════════════════════════════╗")
    log.info("║   Futures Executor Bot — Binance USDT Perpetuos       ║")
    log.info(f"║   Modo: {mode_str:<45}║")
    log.info(f"║   TP:{TP_PCT}% | SL:{SL_PCT}% | Qty:{QTY_MULTIPLIER}×mín | Poll:{POSITION_POLL_S}s       ║")
    log.info(f"║   Testnet: {USE_TESTNET}                                      ║")
    log.info("╚══════════════════════════════════════════════════════╝")

    try:
        from binance_api_mejorado import BinanceAPI
    except ImportError:
        log.critical(
            "No se puede importar BinanceAPI desde binance_api_mejorado.py."
        )
        return

    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        log.critical("BINANCE_API_KEY y BINANCE_API_SECRET son obligatorias.")
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
        await send_telegram(sess,
            f"⚡ <b>Futures Executor Bot — {'TESTNET' if USE_TESTNET else 'REAL'} INICIADO</b>\n"
            f"🔀 <b>Modo:</b> {mode_str}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Balance USDT Futures:</b> <code>{execution_manager.balance:.2f} USDT</code>\n"
            f"🎯 TP: <b>+{TP_PCT}%</b> | 🛑 SL: <b>-{SL_PCT}%</b>\n"
            f"📊 Máx: <b>{MAX_LONGS}L + {MAX_SHORTS}S</b> | "
            f"Qty: <b>{QTY_MULTIPLIER}× mínimo</b>\n"
            f"🔒 Secreto compartido: configurado ✅\n"
            f"⏱ Poll posiciones: cada <b>{POSITION_POLL_S}s</b>\n"
            f"🛡 Verificación TP/SL automática: <b>activa</b> (cada {POSITION_POLL_S}s)"
        )

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(
            start_http_server(),
            ws_price_loop(session),
            position_monitor_loop(session),
        )


if __name__ == "__main__":
    asyncio.run(main())
