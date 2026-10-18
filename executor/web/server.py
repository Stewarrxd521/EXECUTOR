"""Servidor HTTP/WebSocket del executor.

Compatibilidad con los bots ya desplegados (``ExecutorBridge`` de app.py /
app_25.py), que solo usan dos rutas:

* ``POST /signal``      señal JSON + header ``X-Signal-Secret``; responde al instante.
* ``GET  /api/state``   estado JSON (sin credenciales, salvo ``STATE_REQUIRES_AUTH``).

Además:

* ``GET  /``            dashboard estilo Binance (se abre directamente con el link).
* ``GET  /ws``          WebSocket del dashboard: estado en vivo + comandos.
* ``GET  /ws/signal``   WebSocket para enviar señales sin REST.
* ``GET  /health``      salud del servicio · ``GET /ready`` 200 cuando ya puede operar.
* ``/api/*``            API REST completa (ver ``executor/web/api.py`` y
  ``executor_client.py``) · ``/manual/*`` rutas del executor anterior.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Optional

from aiohttp import WSMsgType, web

from .. import __version__
from ..precision import parse_bool, safe_float
from ..service import ExecutorService
from .api import (add_route, compress_middleware, cors_middleware, json_errors_middleware, register_legacy,
                  register_rest)
from .common import (Auth, any_valid, client_ip, dumps, json_response, origin_allowed, read_body,
                     request_secrets, run_command)

log = logging.getLogger("executor.web")
STATIC_DIR = Path(__file__).resolve().parent / "static"
# Partes del estado que pesan mucho y casi nunca cambian: solo se reenvían si cambian.
HEAVY_STATE_KEYS = ("closed", "signals")
MARKETS_EVERY_S = 5


class Client:
    def __init__(self, ws: web.WebSocketResponse, ip: str):
        self.ws = ws
        self.ip = ip
        self.authed = False
        self.markets = False
        self.symbol = ""
        self.log_seq = 0
        # Dashboard actual: recibe solo lo que cambió y avisa si la pestaña está oculta.
        self.delta = False
        self.visible = True
        self.sent: dict[str, str] = {}
        self.market_sent: dict[str, list] = {}

    def state_text(self, light: str, heavy: dict[str, str]) -> str:
        """Mensaje ``state``: lo ligero siempre; ``closed``/``signals`` solo si cambiaron."""
        extra = []
        for key, text in heavy.items():
            if not self.delta or self.sent.get(key) != text:
                self.sent[key] = text
                extra.append(f',"{key}":{text}')
        return '{"type":"state","data":' + light[:-1] + "".join(extra) + "}}"

    def markets_text(self, rows: list) -> Optional[str]:
        """Mercados completos la primera vez; después solo las filas que cambiaron."""
        full = not self.delta or not self.market_sent
        changed = rows if full else [r for r in rows if self.market_sent.get(r[0]) != r]
        if full:
            self.market_sent = {}
        for r in changed:
            self.market_sent[r[0]] = r
        if not changed and not full:
            return None
        return dumps({"type": "markets", "rows": changed, "full": full})

    async def send(self, text: str) -> None:
        if not self.ws.closed:
            try:
                await self.ws.send_str(text)
            except (ConnectionResetError, RuntimeError):
                pass


class DashboardHub:
    def __init__(self, service: ExecutorService):
        self.service = service
        self.clients: set[Client] = set()
        self._last_failed_auth = 0.0  # global: frena la fuerza bruta aunque cambie la IP
        self._task: Optional[asyncio.Task] = None
        self._sends: set[asyncio.Task] = set()
        service.core.bus.subscribe(self._on_event)
        service.core.journal.subscribe(self._on_error)

    def start(self) -> None:
        self._task = asyncio.create_task(self._broadcast_loop(), name="dashboard-broadcast")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
        for c in list(self.clients):
            await c.ws.close()

    def _authed(self) -> list[Client]:
        return [c for c in self.clients if c.authed and not c.ws.closed]

    def _split_state(self) -> tuple[str, dict[str, str]]:
        snap = self.service.snapshot()
        heavy = {key: dumps(snap.pop(key)) for key in HEAVY_STATE_KEYS}
        return dumps(snap), heavy

    def _push_all(self, payload: dict) -> None:
        targets = self._authed()
        if not targets:
            return
        text = dumps(payload)
        for c in targets:
            task = asyncio.create_task(c.send(text))
            self._sends.add(task)
            task.add_done_callback(self._sends.discard)

    def _on_event(self, event: dict) -> None:
        self._push_all({"type": "event", "event": event})

    def _on_error(self, entry) -> None:
        self._push_all({"type": "error", "entry": entry.to_dict()})

    async def _broadcast_loop(self) -> None:
        tick = 0
        while True:
            await asyncio.sleep(1)
            tick += 1
            clients = self._authed()
            for c in clients:
                if c.symbol:
                    self.service.note_watch(c.symbol)
            # Una pestaña oculta no recibe el estado periódico (sí eventos y errores).
            clients = [c for c in clients if c.visible]
            if not clients:
                continue
            try:
                light, heavy = self._split_state()
                rows = self.service.core.market.market_rows() if tick % MARKETS_EVERY_S == 0 else None
                logs = list(self.service.logs.lines)
                for c in clients:
                    await c.send(c.state_text(light, heavy))
                    if rows is not None and c.markets:
                        markets = c.markets_text(rows)
                        if markets:
                            await c.send(markets)
                    if c.symbol:
                        ticker = dumps({"type": "ticker", "data": self.service.symbol_view(c.symbol)})
                        if not c.delta or c.sent.get("ticker") != ticker:
                            c.sent["ticker"] = ticker
                            await c.send(ticker)
                    fresh = [ln for ln in logs if ln["id"] > c.log_seq]
                    if fresh:
                        c.log_seq = fresh[-1]["id"]
                        await c.send(dumps({"type": "logs", "lines": fresh[-100:]}))
            except Exception:
                log.exception("Dashboard: error enviando estado")

    async def handle(self, request: web.Request) -> web.StreamResponse:
        settings = self.service.settings
        # Solo el propio dashboard (mismo sitio) o los orígenes listados en CORS_ORIGINS
        # (el comodín * no aplica aquí): una página ajena no puede controlar el executor.
        if not origin_allowed(request, settings.cors_origins, strict=True):
            log.warning("WebSocket del dashboard rechazado: Origin %s no permitido (añádelo a CORS_ORIGINS)",
                        str(request.headers.get("Origin"))[:100])
            return json_response({"ok": False, "error": "origin no permitido"}, 403)
        ws = web.WebSocketResponse(heartbeat=25, max_msg_size=1 << 20)
        await ws.prepare(request)
        client = Client(ws, client_ip(request, settings.trust_proxy))
        self.clients.add(client)
        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    data = json.loads(msg.data)
                except ValueError:
                    continue
                if isinstance(data, dict):
                    try:
                        await self._on_message(client, data)
                    except Exception:
                        log.exception("Dashboard: mensaje no procesado")
        finally:
            self.clients.discard(client)
        return ws

    async def _on_message(self, client: Client, data: dict) -> None:
        op = data.get("op")
        if op == "auth":
            settings = self.service.settings
            required = settings.dashboard_auth_required
            if required and time.time() - self._last_failed_auth < 2:
                await asyncio.sleep(2)
            if not required or settings.check_write_token(str(data.get("token", ""))):
                client.authed = True
                await client.send(dumps({"type": "auth", "ok": True, "env": settings.env_label, "required": required}))
                client.sent.clear()
                client.market_sent = {}
                await client.send(client.state_text(*self._split_state()))
                await client.send(client.markets_text(self.service.core.market.market_rows()))
                await client.send(dumps({"type": "errors", "entries": self.service.core.journal.entries(150)}))
                client.log_seq = 0
            else:
                self._last_failed_auth = time.time()
                await client.send(dumps({"type": "auth", "ok": False, "required": True, "error": "token inválido"}))
            return
        if not client.authed:
            await client.send(dumps({"type": "auth", "ok": False, "error": "no autenticado"}))
            return
        if op == "markets":
            client.markets = bool(data.get("on", True))
            if data.get("delta"):
                client.delta = True
        elif op == "visible":
            client.visible = bool(data.get("on", True))
        elif op == "select":
            client.symbol = str(data.get("symbol", "")).upper()[:30]
            if client.symbol:
                self.service.note_watch(client.symbol)
                await client.send(dumps({"type": "ticker", "data": self.service.symbol_view(client.symbol)}))
        elif op == "cmd":
            task = asyncio.create_task(self._run_command(client, data))
            self._sends.add(task)
            task.add_done_callback(self._sends.discard)

    async def _run_command(self, client: Client, data: dict) -> None:
        req_id = data.get("id")
        cmd = str(data.get("cmd", ""))
        args = data.get("args")
        status, reply = await run_command(self.service, cmd, args if isinstance(args, dict) else {})
        reply.update({"type": "reply", "id": req_id, "cmd": cmd, "status": status})
        await client.send(dumps(reply))


def build_app(service: ExecutorService) -> web.Application:
    settings = service.settings
    trades = service.trades
    hub = DashboardHub(service)
    auth = Auth(service)

    async def index(_: web.Request) -> web.StreamResponse:
        return web.FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-cache"})

    # ── POST /signal (ExecutorBridge de app.py / app_25.py) ─────────────────
    def cross_site(request: web.Request) -> bool:
        """Petición de un navegador desde otro sitio (los bots y scripts no envían Origin)."""
        return bool(request.headers.get("Origin")) and not origin_allowed(request, settings.cors_origins, strict=True)

    async def signal(request: web.Request) -> web.Response:
        ip = client_ip(request, settings.trust_proxy)
        if cross_site(request):
            return json_response({"ok": False, "error": "origin no permitido (añádelo a CORS_ORIGINS)"}, 403)
        data, invalid = await read_body(request)
        if invalid or not isinstance(data, dict):
            return json_response({"ok": False, "error": "invalid json"}, 400)
        if not any_valid(request_secrets(request, data), settings.check_signal_secret):
            trades.note_unauthorized(ip, data, "http")
            return json_response({"ok": False, "error": "unauthorized"}, 401)
        wait = parse_bool(request.query.get("wait", data.pop("wait", None)), False)
        signal_id = request.headers.get("Idempotency-Key", "") or request.headers.get("X-Signal-Id", "")
        status, body = await trades.submit_signal(data, source="http", ip=ip, wait=wait, signal_id=signal_id)
        return json_response(body, status)

    # ── WebSocket /ws/signal ──────────────────────────────────────────────
    async def signal_ws(request: web.Request) -> web.StreamResponse:
        ip = client_ip(request, settings.trust_proxy)
        if cross_site(request):
            return json_response({"ok": False, "error": "origin no permitido (añádelo a CORS_ORIGINS)"}, 403)
        ws = web.WebSocketResponse(heartbeat=25, max_msg_size=1 << 20)
        await ws.prepare(request)
        authed = any_valid(request_secrets(request), settings.check_signal_secret)
        failures = 0
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except ValueError:
                await ws.send_str(dumps({"ok": False, "error": "invalid json", "status": 400}))
                continue
            if not isinstance(data, dict):
                await ws.send_str(dumps({"ok": False, "error": "invalid json", "status": 400}))
                continue
            req_id = data.get("id")
            if not authed:
                secret = str(data.get("secret") or data.get("token") or "")
                if not settings.check_signal_secret(secret):
                    trades.note_unauthorized(ip, data, "ws")
                    await ws.send_str(dumps({"ok": False, "error": "unauthorized", "status": 401, "id": req_id}))
                    failures += 1
                    if failures >= 5:
                        await ws.close(message=b"unauthorized")
                        break
                    await asyncio.sleep(min(2.0, 0.25 * failures))
                    continue
                authed = True
                if not data.get("action"):  # mensaje solo de autenticación
                    await ws.send_str(dumps({"ok": True, "auth": True, "id": req_id}))
                    continue
            if str(data.get("action", "")).lower() == "ping":
                await ws.send_str(dumps({"ok": True, "pong": True, "id": req_id, "ready": trades.ready.is_set()}))
                continue
            wait = parse_bool(data.pop("wait", None), False)
            try:
                status, body = await trades.submit_signal(data, source="ws", ip=ip, wait=wait,
                                                          signal_id=str(data.get("signal_id") or ""))
            except Exception:
                log.exception("/ws/signal: señal no procesada")
                status, body = 500, {"ok": False, "error": "error interno del executor"}
            body["status"] = status
            if req_id is not None:
                body["id"] = req_id
            await ws.send_str(dumps(body))
        return ws

    # ── GET /api/state (poll_state_loop de los bots) ─────────────────────────
    async def api_state(request: web.Request) -> web.Response:
        if settings.state_requires_auth and not any_valid(request_secrets(request), settings.check_write_token):
            auth.warn_denied(request, "/api/state")
            return json_response({"ok": False, "error": "unauthorized"}, 401)
        limit = int(safe_float(request.query.get("limit"), settings.state_closed_limit))
        return json_response(service.api_state(max(0, limit)))

    async def health(_: web.Request) -> web.Response:
        h = service.health()
        ok = h["market"]["connected"] and (not h["credentials"] or h["ws_api"]["connected"] or h["user"]["connected"])
        return json_response({"ok": bool(ok), "version": __version__,
                              "uptime_s": int(time.time() - service.started_ts), **h})

    async def ready(_: web.Request) -> web.Response:
        is_ready = trades.ready.is_set() and trades.accepting
        return json_response({"ok": is_ready, "ready": trades.ready.is_set(), "accepting_signals": trades.accepting,
                              "trading_enabled": trades.trading_enabled, "version": __version__},
                             200 if is_ready else 503)

    async def on_startup(_: web.Application) -> None:
        hub.start()

    async def on_shutdown(_: web.Application) -> None:
        await hub.stop()

    app = web.Application(client_max_size=1 << 20,
                          middlewares=[compress_middleware, cors_middleware(service), json_errors_middleware])
    app.router.add_get("/", index)
    app.router.add_get("/ws", hub.handle)
    app.router.add_get("/ws/signal", signal_ws)
    add_route(app, "POST", "/signal", signal)
    add_route(app, "GET", "/api/state", api_state)
    add_route(app, "GET", "/health", health)
    add_route(app, "GET", "/ready", ready)
    register_rest(app, service, auth)
    register_legacy(app, service, auth)
    app.router.add_static("/static", STATIC_DIR, append_version=True)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app
