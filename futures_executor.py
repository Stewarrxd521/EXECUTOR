"""
futures_executor.py — Ejecutor simple de Binance USDT-M Futures

Este servicio recibe las señales HTTP que envía app.py y las replica en Binance.
No calcula TP, no calcula SL, no reescala cantidades, no invierte dirección y no
aplica límites artificiales: abre y cierra únicamente cuando app.py lo ordena.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Optional

import aiohttp
from aiohttp import web

from binance_api_mejorado import BinanceAPI

BINANCE_API_KEY = os.environ.get("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.environ.get("BINANCE_API_SECRET", "")
USE_TESTNET = os.environ.get("USE_TESTNET", "false").lower() == "true"
SIGNAL_SECRET = os.environ.get("SIGNAL_SECRET", "cambiar-por-secreto-seguro")
PORT = int(os.environ.get("PORT", "10000"))
DEFAULT_LEVERAGE = int(os.environ.get("LEVERAGE", os.environ.get("DEFAULT_LEVERAGE", "0")))
SIGNAL_DEDUPE_TTL_S = int(os.environ.get("SIGNAL_DEDUPE_TTL_S", "10"))
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("futures_executor")


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


@dataclass
class Trade:
    id: int
    paper_trade_id: int
    symbol: str
    direction: str
    quantity: float
    entry_price: float
    open_time: str
    entry_order_id: str = ""
    status: str = "OPEN"
    close_price: float = 0.0
    close_time: str = ""
    close_reason: str = ""
    pnl_usdt: float = 0.0

    @property
    def notional_usdt(self) -> float:
        return self.entry_price * self.quantity


class ExecutionManager:
    """Estado local mínimo y ejecución directa de señales recibidas desde app.py."""

    def __init__(self, api: BinanceAPI):
        self.api = api
        self._lock = asyncio.Lock()
        self._counter = 0
        self._trades_by_symbol: dict[str, Trade] = {}
        self._paper_to_symbol: dict[int, str] = {}
        self._closed: list[Trade] = []
        self._recent: dict[str, float] = {}
        self.signals_received = 0
        self.signals_open = 0
        self.signals_close = 0
        self.signals_rejected = 0
        self.last_signal_time = ""
        self.last_signal_detail = ""

    @property
    def open_trades(self) -> list[Trade]:
        return list(self._trades_by_symbol.values())

    @property
    def closed_trades(self) -> list[Trade]:
        return list(self._closed)

    def _cleanup_recent(self) -> None:
        now = time.time()
        for key, ts in list(self._recent.items()):
            if now - ts > SIGNAL_DEDUPE_TTL_S:
                self._recent.pop(key, None)

    def _dedupe_key(self, action: str, symbol: str, trade_id: int, quantity: float = 0.0) -> str:
        return f"{action}:{trade_id or symbol}:{symbol}:{quantity:.12f}"

    def _position_direction_and_qty(self, position: dict[str, Any]) -> tuple[str, float]:
        amt = to_float(position.get("positionAmt"))
        return ("LONG" if amt > 0 else "SHORT", abs(amt))

    async def _run_blocking(self, fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)

    async def _current_price(self, symbol: str, fallback: float = 0.0) -> float:
        try:
            ticker = await self._run_blocking(self.api.get_ticker_price, symbol)
            return to_float((ticker or {}).get("price"), fallback)
        except Exception:
            return fallback

    async def open_from_signal(self, payload: dict[str, Any]) -> Trade:
        symbol = str(payload.get("symbol", "")).upper().strip()
        direction = str(payload.get("direction", "")).upper().strip()
        trade_id = to_int(payload.get("trade_id"))
        price = to_float(payload.get("price"))
        quantity = to_float(payload.get("quantity", payload.get("qty", payload.get("executed_qty"))))

        if not symbol:
            raise ValueError("open sin symbol")
        if direction not in {"LONG", "SHORT"}:
            raise ValueError("open sin direction LONG/SHORT")
        if quantity <= 0:
            raise ValueError("open sin quantity/qty válida enviada por app.py")

        key = self._dedupe_key("open", symbol, trade_id, quantity)
        async with self._lock:
            self._cleanup_recent()
            if key in self._recent:
                raise ValueError("señal open duplicada")
            self._recent[key] = time.time()

        side = "BUY" if direction == "LONG" else "SELL"
        leverage = to_int(payload.get("leverage"), DEFAULT_LEVERAGE)
        if leverage > 0:
            await self._run_blocking(self.api.set_leverage, symbol, leverage)

        order = await self._run_blocking(
            self.api.create_market_order,
            symbol,
            side,
            quantity,
            None,
            False,
        )
        if not order:
            raise RuntimeError(f"Binance no confirmó la apertura de {symbol}")

        entry_price = price or await self._current_price(symbol)
        executed_qty = to_float(order.get("executedQty"), quantity)
        if executed_qty <= 0:
            executed_qty = quantity

        async with self._lock:
            existing = self._trades_by_symbol.get(symbol)
            if existing and existing.status == "OPEN":
                existing.quantity += executed_qty
                existing.entry_price = entry_price or existing.entry_price
                trade = existing
            else:
                self._counter += 1
                trade = Trade(
                    id=self._counter,
                    paper_trade_id=trade_id,
                    symbol=symbol,
                    direction=direction,
                    quantity=executed_qty,
                    entry_price=entry_price,
                    open_time=utc_now(),
                    entry_order_id=str(order.get("orderId", "")),
                )
                self._trades_by_symbol[symbol] = trade
            if trade_id:
                self._paper_to_symbol[trade_id] = symbol
            self.signals_open += 1
        log.info("OPEN ejecutado: %s", trade)
        return trade

    async def close_from_signal(self, payload: dict[str, Any]) -> list[Trade]:
        symbol = str(payload.get("symbol", "")).upper().strip()
        trade_id = to_int(payload.get("trade_id"))
        reason = str(payload.get("reason", "APP_CLOSE")).upper().strip()
        close_price = to_float(payload.get("close_price", payload.get("price")))
        quantity_requested = to_float(payload.get("quantity", payload.get("qty")))

        if not symbol and trade_id:
            symbol = self._paper_to_symbol.get(trade_id, "")
        if not symbol:
            raise ValueError("close sin symbol")

        key = self._dedupe_key("close", symbol, trade_id, quantity_requested)
        async with self._lock:
            self._cleanup_recent()
            if key in self._recent:
                raise ValueError("señal close duplicada")
            self._recent[key] = time.time()

        closed: list[Trade] = []
        positions = await self._run_blocking(self.api.get_position_info, symbol)
        if isinstance(positions, dict):
            positions = [positions]
        positions = positions or []

        if not positions:
            local = self._trades_by_symbol.get(symbol)
            if local:
                await self._mark_closed(local, close_price or local.entry_price, reason)
                closed.append(local)
            return closed

        remaining = quantity_requested if quantity_requested > 0 else None
        for position in positions:
            pos_direction, pos_qty = self._position_direction_and_qty(position)
            if pos_qty <= 0:
                continue
            qty_to_close = pos_qty if remaining is None else min(pos_qty, remaining)
            if qty_to_close <= 0:
                continue
            side = "SELL" if pos_direction == "LONG" else "BUY"
            position_side = position.get("positionSide")
            order = await self._run_blocking(
                self.api.create_market_order,
                symbol,
                side,
                qty_to_close,
                position_side,
                True,
            )
            if not order:
                raise RuntimeError(f"Binance no confirmó cierre de {symbol}")
            if remaining is not None:
                remaining -= qty_to_close

        trade = self._trades_by_symbol.get(symbol)
        if trade:
            await self._mark_closed(trade, close_price or await self._current_price(symbol, trade.entry_price), reason)
            closed.append(trade)
        else:
            closed.append(Trade(0, trade_id, symbol, "UNKNOWN", quantity_requested, 0.0, "", status="CLOSED", close_price=close_price, close_time=utc_now(), close_reason=reason))
        self.signals_close += 1
        return closed

    async def close_all(self, reason: str = "CLOSE_ALL") -> list[Trade]:
        account_positions = await self._run_blocking(self.api.get_position_info)
        symbols = sorted({p.get("symbol") for p in (account_positions or []) if to_float(p.get("positionAmt")) != 0})
        closed: list[Trade] = []
        for symbol in symbols:
            closed.extend(await self.close_from_signal({"action": "close", "symbol": symbol, "reason": reason}))
        return closed

    async def _mark_closed(self, trade: Trade, close_price: float, reason: str) -> None:
        async with self._lock:
            trade.status = "CLOSED"
            trade.close_reason = reason
            trade.close_price = close_price
            trade.close_time = utc_now()
            if trade.entry_price and close_price:
                if trade.direction == "LONG":
                    trade.pnl_usdt = (close_price - trade.entry_price) * trade.quantity
                elif trade.direction == "SHORT":
                    trade.pnl_usdt = (trade.entry_price - close_price) * trade.quantity
            self._trades_by_symbol.pop(trade.symbol, None)
            self._paper_to_symbol.pop(trade.paper_trade_id, None)
            self._closed.insert(0, trade)
            self._closed = self._closed[:200]
        log.info("CLOSE ejecutado: %s", trade)

    def state(self) -> dict[str, Any]:
        return {
            "ok": True,
            "time": utc_now(),
            "signals_received": self.signals_received,
            "signals_open": self.signals_open,
            "signals_close": self.signals_close,
            "signals_rejected": self.signals_rejected,
            "last_signal_time": self.last_signal_time,
            "last_signal_detail": self.last_signal_detail,
            "open_trades": [asdict(t) | {"notional_usdt": t.notional_usdt} for t in self.open_trades],
            "closed_trades": [asdict(t) | {"notional_usdt": t.notional_usdt} for t in self.closed_trades],
        }


execution_manager: Optional[ExecutionManager] = None


async def send_telegram(message: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=8)
    except Exception as exc:
        log.warning("telegram error: %s", exc)


async def signal_handler(request: web.Request) -> web.Response:
    assert execution_manager is not None
    if request.headers.get("X-Signal-Secret", "") != SIGNAL_SECRET:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        payload = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "invalid json"}, status=400)

    action = str(payload.get("action", "")).lower().strip()
    symbol = str(payload.get("symbol", "")).upper().strip()
    execution_manager.signals_received += 1
    execution_manager.last_signal_time = utc_now()
    execution_manager.last_signal_detail = f"{action.upper()} {symbol}"

    try:
        if action == "open":
            trade = await execution_manager.open_from_signal(payload)
            await send_telegram(f"✅ OPEN {trade.direction} {trade.symbol}\nQty: {trade.quantity}\nPaper#{trade.paper_trade_id}")
            return web.json_response({"ok": True, "action": "open", "trade": asdict(trade)})
        if action == "close":
            trades = await execution_manager.close_from_signal(payload)
            await send_telegram(f"✅ CLOSE {symbol}\nRazón: {payload.get('reason', 'APP_CLOSE')}")
            return web.json_response({"ok": True, "action": "close", "closed": [asdict(t) for t in trades]})
        if action == "close_all":
            trades = await execution_manager.close_all()
            await send_telegram(f"✅ CLOSE_ALL ejecutado\nPosiciones cerradas: {len(trades)}")
            return web.json_response({"ok": True, "action": "close_all", "closed": [asdict(t) for t in trades]})
    except Exception as exc:
        execution_manager.signals_rejected += 1
        log.exception("señal rechazada: %s", payload)
        return web.json_response({"ok": False, "error": str(exc)}, status=400)

    execution_manager.signals_rejected += 1
    return web.json_response({"ok": False, "error": f"unknown action: {action}"}, status=400)


async def manual_close_handler(request: web.Request) -> web.Response:
    """Cierra manualmente una posición abierta desde el dashboard web."""
    assert execution_manager is not None
    try:
        payload = await request.json()
    except Exception:
        payload = {}
    symbol = str(payload.get("symbol") or request.match_info.get("symbol", "")).upper().strip()
    if not symbol:
        return web.json_response({"ok": False, "error": "symbol requerido"}, status=400)
    try:
        trades = await execution_manager.close_from_signal(
            {"action": "close", "symbol": symbol, "reason": "MANUAL_WEB"}
        )
        await send_telegram(f"🖱️ CIERRE MANUAL WEB {symbol}\nPosiciones cerradas: {len(trades)}")
        return web.json_response({"ok": True, "symbol": symbol, "closed": [asdict(t) for t in trades]})
    except Exception as exc:
        execution_manager.signals_rejected += 1
        log.exception("cierre manual rechazado: %s", symbol)
        return web.json_response({"ok": False, "error": str(exc)}, status=400)


async def state_handler(_: web.Request) -> web.Response:
    assert execution_manager is not None
    return web.json_response(execution_manager.state())


async def index_handler(_: web.Request) -> web.Response:
    """Dashboard operativo para ver señales, posiciones y cerrar manualmente."""
    assert execution_manager is not None
    state = execution_manager.state()
    open_rows = "".join(
        f"""
        <tr>
          <td><b>{t['symbol']}</b></td>
          <td><span class="badge {'long' if t['direction'] == 'LONG' else 'short'}">{t['direction']}</span></td>
          <td>{t['quantity']}</td>
          <td>{t['entry_price']:.8f}</td>
          <td>{t['notional_usdt']:.4f} USDT</td>
          <td>{t['open_time']}</td>
          <td><button class="danger" onclick="closeSymbol('{t['symbol']}')">Cerrar manual</button></td>
        </tr>
        """
        for t in state["open_trades"]
    ) or "<tr><td colspan='7' class='muted'>Sin posiciones abiertas registradas localmente.</td></tr>"
    closed_rows = "".join(
        f"""
        <tr>
          <td><b>{t['symbol']}</b></td>
          <td>{t['direction']}</td>
          <td>{t['quantity']}</td>
          <td>{t['entry_price']:.8f}</td>
          <td>{t['close_price']:.8f}</td>
          <td class="{'profit' if t['pnl_usdt'] >= 0 else 'loss'}">{t['pnl_usdt']:+.4f} USDT</td>
          <td>{t['close_reason']}</td>
          <td>{t['close_time']}</td>
        </tr>
        """
        for t in state["closed_trades"][:50]
    ) or "<tr><td colspan='8' class='muted'>Sin cierres registrados.</td></tr>"
    html = f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Futures Executor</title>
  <style>
    :root {{ color-scheme: dark; --bg:#0d1117; --card:#161b22; --line:#30363d; --text:#e6edf3; --muted:#8b949e; --green:#3fb950; --red:#f85149; --blue:#58a6ff; }}
    body {{ margin:0; font-family:Inter,Arial,sans-serif; background:var(--bg); color:var(--text); }}
    header {{ padding:24px 28px; border-bottom:1px solid var(--line); background:#010409; }}
    main {{ padding:24px 28px; }}
    h1 {{ margin:0 0 6px; font-size:28px; }}
    h2 {{ margin-top:28px; }}
    .muted {{ color:var(--muted); }}
    .grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:14px; }}
    .card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:16px; }}
    .label {{ color:var(--muted); font-size:12px; text-transform:uppercase; letter-spacing:.06em; }}
    .value {{ font-size:26px; margin-top:8px; font-weight:700; }}
    table {{ width:100%; border-collapse:collapse; background:var(--card); border:1px solid var(--line); border-radius:12px; overflow:hidden; }}
    th,td {{ padding:10px 12px; border-bottom:1px solid var(--line); text-align:left; }}
    th {{ color:var(--muted); font-size:12px; text-transform:uppercase; }}
    button {{ cursor:pointer; border:0; border-radius:8px; padding:9px 12px; font-weight:700; color:white; background:var(--blue); }}
    button.danger {{ background:var(--red); }}
    button:disabled {{ opacity:.55; cursor:not-allowed; }}
    .badge {{ border-radius:999px; padding:4px 8px; font-weight:700; font-size:12px; }}
    .long,.profit {{ color:var(--green); }}
    .short,.loss {{ color:var(--red); }}
    #toast {{ position:fixed; right:18px; bottom:18px; min-width:260px; display:none; background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px; box-shadow:0 12px 32px #0008; }}
  </style>
</head>
<body>
  <header>
    <h1>⚡ Futures Executor</h1>
    <div class="muted">Ejecutor directo de señales de app.py · Sin TP/SL propios · Sin sizing propio · Última señal: {state['last_signal_detail'] or '—'} {state['last_signal_time']}</div>
  </header>
  <main>
    <section class="grid">
      <div class="card"><div class="label">Señales recibidas</div><div class="value">{state['signals_received']}</div></div>
      <div class="card"><div class="label">Aperturas ejecutadas</div><div class="value long">{state['signals_open']}</div></div>
      <div class="card"><div class="label">Cierres ejecutados</div><div class="value">{state['signals_close']}</div></div>
      <div class="card"><div class="label">Rechazadas</div><div class="value loss">{state['signals_rejected']}</div></div>
      <div class="card"><div class="label">Abiertas locales</div><div class="value">{len(state['open_trades'])}</div></div>
    </section>

    <h2>Posiciones abiertas</h2>
    <table>
      <thead><tr><th>Símbolo</th><th>Dirección</th><th>Qty</th><th>Entrada</th><th>Notional</th><th>Hora apertura</th><th>Acción</th></tr></thead>
      <tbody>{open_rows}</tbody>
    </table>

    <h2>Últimos cierres</h2>
    <table>
      <thead><tr><th>Símbolo</th><th>Dirección</th><th>Qty</th><th>Entrada</th><th>Cierre</th><th>PnL</th><th>Razón</th><th>Hora cierre</th></tr></thead>
      <tbody>{closed_rows}</tbody>
    </table>
  </main>
  <div id="toast"></div>
  <script>
    const toast = (msg) => {{
      const el = document.getElementById('toast');
      el.textContent = msg;
      el.style.display = 'block';
      setTimeout(() => el.style.display = 'none', 4500);
    }};
    async function closeSymbol(symbol) {{
      if (!confirm(`¿Cerrar manualmente ${{symbol}} a mercado?`)) return;
      const buttons = [...document.querySelectorAll('button')];
      buttons.forEach(b => b.disabled = true);
      try {{
        const res = await fetch('/api/close', {{
          method: 'POST',
          headers: {{'Content-Type': 'application/json'}},
          body: JSON.stringify({{symbol}})
        }});
        const data = await res.json();
        if (!res.ok || !data.ok) throw new Error(data.error || `HTTP ${{res.status}}`);
        toast(`Cierre manual enviado para ${{symbol}}`);
        setTimeout(() => location.reload(), 900);
      }} catch (err) {{
        toast(`Error cerrando ${{symbol}}: ${{err.message}}`);
        buttons.forEach(b => b.disabled = false);
      }}
    }}
    setTimeout(() => location.reload(), 30000);
  </script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", index_handler)
    app.router.add_get("/api/state", state_handler)
    app.router.add_post("/api/close", manual_close_handler)
    app.router.add_post("/api/close/{symbol}", manual_close_handler)
    app.router.add_post("/signal", signal_handler)
    return app


def main() -> None:
    global execution_manager
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        raise RuntimeError("BINANCE_API_KEY y BINANCE_API_SECRET son obligatorias")
    api = BinanceAPI(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=USE_TESTNET)
    execution_manager = ExecutionManager(api)
    log.info("Executor iniciado sin TP/SL, sin sizing propio y sin modo contrarian. Testnet=%s", USE_TESTNET)
    web.run_app(create_app(), host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
