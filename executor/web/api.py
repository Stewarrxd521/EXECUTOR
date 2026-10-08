"""API HTTP completa (REST) del executor.

Todo lo que el dashboard hace por WebSocket también se puede hacer con una
petición HTTP simple, al estilo de la API de app_25.py (``/api/close/<symbol>``,
``/api/set-sl/<symbol>``, ``/api/status``, ``/api/live``, ``/api/trades.csv``...).

* Lectura (GET): abierta, salvo que se defina ``DASHBOARD_TOKEN``.
* Escritura (POST/DELETE): requiere el secreto (``X-Signal-Secret`` o
  ``X-Dashboard-Token`` o ``Authorization: Bearer`` o ``?secret=``) con el valor
  de ``SIGNAL_SECRET`` o ``DASHBOARD_TOKEN``. ``API_WRITE_OPEN=true`` lo desactiva.
* Respuesta: ``{"ok": true, "action": <cmd>, "data": <resultado>, ...campos del
  resultado}``; error: ``{"ok": false, "error": "...", "diagnosis"?: {...}}``.
* Rutas antiguas ``/manual/*`` del executor anterior: mismos cuerpos y códigos.
"""

from __future__ import annotations

import asyncio
import csv
import io
import logging
import time
from typing import Any, Optional

from aiohttp import web
from aiohttp.abc import AbstractAccessLogger

from ..service import ExecutorService
from .common import Auth, client_ip, dumps, is_write, json_response, origin_allowed, read_args, run_command

log = logging.getLogger("executor.web")

# (método, ruta, comando, argumentos por defecto)
REST_ROUTES: list[tuple[str, str, str, dict]] = [
    # Lectura
    ("GET", "/api/snapshot", "snapshot", {}),
    ("GET", "/api/status", "snapshot", {}),
    ("GET", "/api/account", "account", {}),
    ("GET", "/api/positions", "positions", {}),
    ("GET", "/api/orders", "orders", {}),
    ("GET", "/api/trades", "trades", {}),
    ("GET", "/api/stats", "stats", {}),
    ("GET", "/api/signals", "signals", {}),
    ("GET", "/api/settings", "settings", {}),
    ("GET", "/api/markets", "markets", {}),
    ("GET", "/api/live", "live", {}),
    ("GET", "/api/symbol/{symbol}", "symbol_info", {}),
    ("GET", "/api/symbol/{symbol}/history", "price_history", {}),
    ("GET", "/api/exchange-info", "exchange_info", {}),
    ("GET", "/api/logs", "logs", {}),
    ("GET", "/api/commands", "commands", {}),
    ("GET", "/api/grids", "grids", {}),
    ("GET", "/api/grids/{id}", "grid_detail", {}),
    ("POST", "/api/grids/preview", "grid_preview", {}),
    ("GET", "/api/errors", "errors", {}),
    ("GET", "/api/errors/catalog", "error_catalog", {}),
    ("GET", "/api/errors/{code}", "explain_error", {}),
    ("GET", "/api/position-mode", "position_mode", {}),
    # Escritura
    ("POST", "/api/signal", "signal", {}),
    ("POST", "/api/open", "open", {}),
    ("POST", "/api/close", "close_position", {"reason": "MANUAL_WEB", "closed_by": "api"}),
    ("POST", "/api/close/{symbol}", "close_position", {"reason": "MANUAL_WEB", "closed_by": "api"}),
    ("POST", "/api/force-close/{symbol}", "close_position", {"reason": "MANUAL_WEB", "closed_by": "api",
                                                             "force": True}),
    ("POST", "/api/close-all", "close_all", {}),
    ("POST", "/api/set-tp/{symbol}", "set_tp", {}),
    ("POST", "/api/set-sl/{symbol}", "set_sl", {}),
    ("POST", "/api/cancel-tp-sl/{symbol}", "cancel_tp_sl", {}),
    ("POST", "/api/limit-order", "limit_order", {}),
    ("POST", "/api/cancel-order", "cancel_order", {}),
    ("POST", "/api/cancel-orders/{symbol}", "cancel_symbol_orders", {}),
    ("POST", "/api/leverage", "set_default_leverage", {}),
    ("POST", "/api/leverage/{symbol}", "set_symbol_leverage", {}),
    ("POST", "/api/margin-type/{symbol}", "set_margin_type", {}),
    ("POST", "/api/margin/{symbol}", "modify_margin", {}),
    ("POST", "/api/position-mode", "set_position_mode", {}),
    ("POST", "/api/trading", "toggle_trading", {}),
    ("POST", "/api/clear-history", "clear_history", {}),
    ("POST", "/api/grids", "grid_create", {}),
    ("POST", "/api/grids/{id}/stop", "grid_stop", {}),
    ("DELETE", "/api/grids/{id}", "grid_delete", {}),
    ("POST", "/api/errors/clear", "clear_errors", {}),
    ("POST", "/api/exchange-info/refresh", "refresh_exchange_info", {}),
    ("POST", "/api/sync", "sync_account", {}),
    ("POST", "/api/sync-orders", "sync_orders", {}),
]

_RESERVED = {"ok", "action", "data", "error"}


def add_route(app: web.Application, method: str, path: str, handler) -> None:
    """Registra la ruta con y sin barra final (urllib no sigue 308 en POST).

    Las rutas GET aceptan también HEAD (monitores tipo UptimeRobot).
    """
    paths = [path] if path == "/" or path.endswith("/") else [path, path + "/"]
    for p in paths:
        if method == "GET":
            app.router.add_get(p, handler)
        else:
            app.router.add_route(method, p, handler)


def _envelope(cmd: str, status: int, body: dict) -> web.Response:
    if body.get("ok"):
        out: dict = {"ok": True, "action": cmd, "data": body["data"]}
        if isinstance(body["data"], dict):
            out.update({k: v for k, v in body["data"].items() if k not in _RESERVED})
        return json_response(out, status)
    return json_response(body, status)


def register_rest(app: web.Application, service: ExecutorService, auth: Auth) -> None:
    def make(cmd: str, defaults: dict):
        async def handler(request: web.Request) -> web.Response:
            args, secret, error = await read_args(request, defaults)
            if args is None:
                return json_response({"ok": False, "error": error}, 400)
            write = is_write(cmd)
            if not auth.can(request, secret, write):
                auth.warn_denied(request, request.path)
                return auth.denied(write)
            status, body = await run_command(service, cmd, args)
            return _envelope(cmd, status, body)
        return handler

    for method, path, cmd, defaults in REST_ROUTES:
        add_route(app, method, path, make(cmd, defaults))

    async def api_command(request: web.Request) -> web.Response:
        args, secret, error = await read_args(request)
        if args is None:
            return json_response({"ok": False, "error": error}, 400)
        cmd = str(args.pop("cmd", "") or "")
        if not cmd:
            return json_response({"ok": False, "error": "falta cmd (ver GET /api/commands)"}, 400)
        write = is_write(cmd)
        if not auth.can(request, secret, write):
            auth.warn_denied(request, f"/api/command {cmd}")
            return auth.denied(write)
        status, body = await run_command(service, cmd, args)
        return json_response(body, status)

    async def trades_csv(request: web.Request) -> web.StreamResponse:
        if not auth.can_read(request, request.headers.get("X-Dashboard-Token", "") or request.query.get("token", "")):
            return auth.denied(False)
        t = service.trades
        cols = ["id", "paper_trade_id", "paper_ids", "symbol", "direction", "status", "source", "closed_by",
                "open_time", "close_time", "entry_price", "close_price", "quantity", "leverage", "pnl_usdt",
                "roi_pct", "roe_pct", "fees_usdt", "bot_pnl", "pnl_diff", "signal_entry_price",
                "signal_close_price", "levels"]
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(cols)
        rows = [x.to_dict() for x in t.closed] + [x.to_dict() for x in t.open_trades]
        for r in rows:
            writer.writerow([r.get(c) if not isinstance(r.get(c), list) else " ".join(map(str, r.get(c)))
                             for c in cols])
        return web.Response(text=buf.getvalue(), content_type="text/csv",
                            headers={"Content-Disposition": 'attachment; filename="executor_trades.csv"',
                                     "Cache-Control": "no-store"})

    add_route(app, "POST", "/api/command", api_command)
    add_route(app, "GET", "/api/trades.csv", trades_csv)


# ── Rutas antiguas /manual/* (mismos cuerpos y códigos que el executor anterior) ──
def register_legacy(app: web.Application, service: ExecutorService, auth: Auth) -> None:
    trades = service.trades

    async def args_or_error(request: web.Request) -> tuple[Optional[dict], Optional[web.Response]]:
        args, secret, error = await read_args(request)
        if args is None:
            return None, json_response({"ok": False, "error": "invalid json"}, 400)
        if not auth.can_write(request, secret):
            auth.warn_denied(request, request.path)
            return None, json_response({"ok": False, "error": "unauthorized"}, 401)
        return args, None

    def no_position(symbol: str) -> web.Response:
        return json_response({"ok": False, "error": f"no hay posición abierta registrada para {symbol} "
                                                    f"(si hay LONG y SHORT simultáneas, especifica 'direction')"}, 404)

    def has_position(symbol: str, direction: Optional[str]) -> bool:
        if trades.get_trade(symbol, direction) is not None:
            return True
        return any(not direction or p.direction == direction for p in service.core.account.positions_for(symbol))

    async def legacy_call(cmd: str, args: dict, shape) -> web.Response:
        status, body = await run_command(service, cmd, args)
        if not body.get("ok"):
            return json_response({"ok": False, "error": body.get("error", "error")}, status)
        return json_response({"ok": True, **shape(body["data"])}, 200)

    def spawn(coro) -> None:
        task = asyncio.create_task(coro)
        trades._tasks.add(task)
        task.add_done_callback(trades._tasks.discard)

    async def close(request):
        args, err = await args_or_error(request)
        if err:
            return err
        symbol = str(args.get("symbol", "")).upper()
        direction = str(args.get("direction", "")).upper() or None
        if not symbol or not has_position(symbol, direction):
            return no_position(symbol)
        spawn(run_command(service, "close_position", {"symbol": symbol, "direction": direction, "reason": "MANUAL",
                                                      "closed_by": "api"}))
        return json_response({"ok": True, "action": "manual_close", "symbol": symbol})

    async def close_all(request):
        args, err = await args_or_error(request)
        if err:
            return err
        total = len(trades.trades)
        spawn(run_command(service, "close_all", {"reason": "MANUAL"}))
        return json_response({"ok": True, "action": "manual_close_all", "positions_targeted": total})

    async def toggle_trading(request):
        args, err = await args_or_error(request)
        if err:
            return err
        return await legacy_call("toggle_trading", args, lambda d: {"trading_enabled": d["trading_enabled"]})

    async def set_leverage(request):
        args, err = await args_or_error(request)
        if err:
            return err
        return await legacy_call("set_default_leverage", args, lambda d: {"leverage": d["leverage"]})

    async def clear_history(request):
        args, err = await args_or_error(request)
        if err:
            return err
        return await legacy_call("clear_history", args, lambda d: {"cleared": d["cleared"]})

    def protect(kind: str):
        async def handler(request):
            args, err = await args_or_error(request)
            if err:
                return err
            symbol = str(args.get("symbol", "")).upper()
            direction = str(args.get("direction", "")).upper() or None
            if not symbol or not has_position(symbol, direction):
                return json_response({"ok": False, "error": f"sin posición abierta para {symbol} (si hay LONG y "
                                                            f"SHORT simultáneas, especifica 'direction')"}, 404)
            key = kind.lower()
            return await legacy_call(f"set_{key}", args,
                                     lambda d: {"symbol": symbol, key: d.get(key), "result": d.get("result")})
        return handler

    async def cancel_tp_sl(request):
        args, err = await args_or_error(request)
        if err:
            return err
        return await legacy_call("cancel_tp_sl", args, lambda d: {"symbol": d["symbol"], "cancelled": d["cancelled"]})

    async def limit_order(request):
        args, err = await args_or_error(request)
        if err:
            return err
        return await legacy_call("limit_order", args, lambda d: {"result": d.get("result", d)})

    async def set_symbol_leverage(request):
        args, err = await args_or_error(request)
        if err:
            return err
        return await legacy_call("set_symbol_leverage", args,
                                 lambda d: {"symbol": d["symbol"], "requested": d["requested"], "applied": d["applied"]})

    async def set_margin_type(request):
        args, err = await args_or_error(request)
        if err:
            return err
        return await legacy_call("set_margin_type", args, lambda d: d)

    async def position_mode(request):
        args, err = await args_or_error(request)
        if err:
            return err
        return await legacy_call("position_mode", args,
                                 lambda d: {k: d[k] for k in ("account_hedge_mode", "local_hedge_mode", "in_sync",
                                                              "open_positions")})

    async def set_position_mode(request):
        args, err = await args_or_error(request)
        if err:
            return err
        return await legacy_call("set_position_mode", args, lambda d: d)

    async def modify_margin(request):
        args, err = await args_or_error(request)
        if err:
            return err
        symbol = str(args.get("symbol", "")).upper()
        if not has_position(symbol, str(args.get("direction", "")).upper() or None):
            return json_response({"ok": False, "error": "posición no encontrada o monto inválido (si hay LONG y "
                                                        "SHORT simultáneas, especifica 'direction')"}, 400)
        return await legacy_call("modify_margin", args, lambda d: d)

    async def orders(request):
        args, err = await args_or_error(request)
        if err:
            return err
        symbol = str(args.get("symbol", "")).upper()
        if not symbol:
            return json_response({"ok": False, "error": "symbol requerido"}, 400)
        acct = service.core.account
        rows: list[dict[str, Any]] = []
        for o in acct.orders_for(symbol):
            rows.append({"orderId": o.id, "clientOrderId": o.client_id, "symbol": o.symbol, "side": o.side,
                         "positionSide": o.position_side, "type": o.type, "price": o.price,
                         "stopPrice": o.trigger_price, "origQty": o.qty, "executedQty": o.filled,
                         "reduceOnly": o.reduce_only, "closePosition": o.close_position, "status": o.status,
                         "time": int(o.time * 1000)})
        for o in acct.algos_for(symbol):
            rows.append({"algoId": o.id, "clientAlgoId": o.client_id, "symbol": o.symbol, "side": o.side,
                         "positionSide": o.position_side, "type": o.type, "orderType": o.type,
                         "triggerPrice": o.trigger_price, "price": o.price, "quantity": o.qty,
                         "reduceOnly": o.reduce_only, "closePosition": o.close_position, "algoStatus": o.status,
                         "_algo": True})
        return json_response({"ok": True, "symbol": symbol, "orders": rows})

    for method, path, handler in [
        ("POST", "/manual/close", close),
        ("POST", "/manual/close_all", close_all),
        ("POST", "/manual/toggle_trading", toggle_trading),
        ("POST", "/manual/set_leverage", set_leverage),
        ("POST", "/manual/clear_history", clear_history),
        ("POST", "/manual/set_tp", protect("TP")),
        ("POST", "/manual/set_sl", protect("SL")),
        ("POST", "/manual/cancel_tp_sl", cancel_tp_sl),
        ("POST", "/manual/limit_order", limit_order),
        ("POST", "/manual/set_symbol_leverage", set_symbol_leverage),
        ("POST", "/manual/set_margin_type", set_margin_type),
        ("GET", "/manual/position_mode", position_mode),
        ("POST", "/manual/set_position_mode", set_position_mode),
        ("POST", "/manual/modify_margin", modify_margin),
        ("GET", "/manual/orders", orders),
    ]:
        add_route(app, method, path, handler)


# ── Middlewares ───────────────────────────────────────────────────────────
def cors_middleware(service: ExecutorService):
    settings = service.settings

    @web.middleware
    async def middleware(request: web.Request, handler):
        origin = request.headers.get("Origin", "")
        preflight = request.method == "OPTIONS" and request.headers.get("Access-Control-Request-Method")
        allowed = bool(origin) and settings.cors_origins and origin_allowed(request, settings.cors_origins)
        if preflight:
            resp: web.StreamResponse = web.Response(status=204 if allowed else 403)
        else:
            resp = await handler(request)
        if allowed and not request.path.startswith("/ws") and not resp.prepared:
            star = "*" in [o.strip() for o in settings.cors_origins.split(",")]
            resp.headers["Access-Control-Allow-Origin"] = "*" if star else origin
            if not star:
                resp.headers["Vary"] = "Origin"
            resp.headers["Access-Control-Allow-Methods"] = "GET, HEAD, POST, DELETE, OPTIONS"
            resp.headers["Access-Control-Allow-Headers"] = ("Content-Type, X-Signal-Secret, X-Dashboard-Token, "
                                                            "Authorization, Idempotency-Key")
            resp.headers["Access-Control-Max-Age"] = "600"
        return resp

    return middleware


@web.middleware
async def json_errors_middleware(request: web.Request, handler):
    """404/405/500 como JSON {ok:false, error} en vez de texto plano."""
    try:
        return await handler(request)
    except web.HTTPException as exc:
        if exc.status < 400 or request.path.startswith("/static"):
            raise
        body = {"ok": False, "error": exc.reason, "status": exc.status, "path": request.path}
        if isinstance(exc, web.HTTPMethodNotAllowed):
            body["allowed"] = sorted(exc.allowed_methods)
        return json_response(body, exc.status)
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Error no controlado en %s %s", request.method, request.path)
        return json_response({"ok": False, "error": "error interno del executor", "status": 500}, 500)


class AccessLogger(AbstractAccessLogger):
    """Registra POST/DELETE y errores; omite el ruido de GET exitosos."""

    trust_proxy = False

    def log(self, request, response, time_taken):  # noqa: A003
        status = response.status
        if request.method in ("GET", "HEAD", "OPTIONS") and status < 400:
            return
        if request.path.startswith(("/ws", "/static")) and status < 400:
            return
        level = logging.WARNING if status >= 400 else logging.INFO
        self.logger.log(level, "%s %s %s %d (%.0f ms)", client_ip(request, self.trust_proxy), request.method,
                        request.path_qs if status >= 400 else request.path, status, time_taken * 1000)


def build_access_logger(trust_proxy: bool) -> type:
    return type("ExecutorAccessLogger", (AccessLogger,), {"trust_proxy": trust_proxy})


__all__ = ["REST_ROUTES", "register_rest", "register_legacy", "cors_middleware", "json_errors_middleware",
           "build_access_logger", "add_route", "dumps", "time"]
