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
import zlib
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
# Del estado solo se reenvía lo que cambió; estos campos van siempre que se envía algo.
STATE_ALWAYS = ("ts", "uptime_s")
HEALTH_EVERY_S = 5    # latencias y contadores: cambian cada segundo
MARKETS_EVERY_S = 30  # el símbolo seleccionado sigue llegando cada segundo


def _sig(value: float, digits: int = 2) -> float:
    """Redondea a ``digits`` cifras significativas (el volumen solo sirve para ordenar)."""
    return float(f"{value:.{digits}g}") if value else 0.0


def ws_market_rows(rows: list) -> list:
    return [[sym, last, chg, _sig(vol)] for sym, last, chg, vol in rows]


MARKET_SORTS = {"vol": lambda r: -r[3], "chg": lambda r: -r[2], "name": lambda r: r[0]}


def pick_market_rows(rows: list, top: int, sort: str, query: str, keep: set) -> list:
    """Las ``top`` primeras filas según el orden del panel, más las de ``keep``.

    El executor sigue recibiendo todo el mercado; esto solo recorta lo que sale hacia el
    dashboard (lo que Render cuenta como ancho de banda). ``top=0`` = todas.
    """
    if top <= 0:
        return rows
    if query:
        base = [r for r in rows if query in r[0]]
    elif sort == "fav":
        base = [r for r in rows if r[0] in keep]
    else:
        base = rows
    chosen = sorted(base, key=MARKET_SORTS.get(sort, MARKET_SORTS["vol"]))[:top]
    names = {r[0] for r in chosen}
    return chosen + [r for r in rows if r[0] in keep and r[0] not in names]


class Client:
    def __init__(self, ws: web.WebSocketResponse, ip: str):
        self.ws = ws
        self.ip = ip
        self.authed = False
        self.markets = False
        self.symbol = ""
        # Lista reducida (dashboard actual): top N del orden elegido + favoritos en la pestaña ★.
        self.market_top = 0
        self.market_sort = "vol"
        self.market_query = ""
        self.market_pins: set[str] = set()
        self.log_seq = 0
        # Dashboard actual: recibe solo lo que cambió y avisa si la pestaña está oculta.
        self.delta = False
        self.visible = True
        self.sent: dict[str, str] = {}
        self.traffic = None  # Counter del servicio (bytes enviados al dashboard)
        self.market_sent: dict[str, list] = {}
        # Render no negocia permessage-deflate: si el dashboard lo pide, los mensajes van
        # comprimidos en un único flujo deflate (cada uno termina en "\n" y Z_SYNC_FLUSH).
        self.zip: Optional["zlib._Compress"] = None
        self._lock = asyncio.Lock()

    def enable_zip(self) -> None:
        self.zip = zlib.compressobj(6, zlib.DEFLATED, -15)

    def pack(self, text: str) -> bytes:
        return self.zip.compress(text.encode() + b"\n") + self.zip.flush(zlib.Z_SYNC_FLUSH)

    def state_text(self, parts: dict[str, str]) -> Optional[str]:
        """Mensaje ``state`` con los campos que cambiaron (todos si el cliente es antiguo)."""
        changed = [k for k, text in parts.items()
                   if k not in STATE_ALWAYS and (not self.delta or self.sent.get(k) != text)]
        if not changed:
            return None
        for k in changed:
            self.sent[k] = parts[k]
        keys = [k for k in STATE_ALWAYS if k in parts] + changed
        return '{"type":"state","data":{' + ",".join(f'"{k}":{parts[k]}' for k in keys) + "}}"

    def markets_text(self, rows: list) -> Optional[str]:
        """Mercados completos la primera vez; después solo las filas que cambiaron."""
        # Si cambia qué símbolos van en la lista, se reenvía entera.
        full = not self.delta or not self.market_sent or {r[0] for r in rows} != self.market_sent.keys()
        changed = rows if full else [r for r in rows if self.market_sent.get(r[0]) != r]
        if full:
            self.market_sent = {}
        for r in changed:
            self.market_sent[r[0]] = r
        if not changed and not full:
            return None
        return dumps({"type": "markets", "rows": changed, "full": full})

    async def send(self, text: str) -> None:
        if self.ws.closed:
            return
        try:
            if self.zip is None:
                await self.ws.send_str(text)
                if self.traffic is not None:
                    self.traffic["dashboard_ws"] += len(text)
                return
            # El flujo comprimido exige que los mensajes salgan en el orden en que se comprimen.
            async with self._lock:
                packed = self.pack(text)
                await self.ws.send_bytes(packed)
                if self.traffic is not None:
                    self.traffic["dashboard_ws"] += len(packed)
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

    def _state_parts(self, health: bool = True) -> dict[str, str]:
        snap = self.service.snapshot()
        if not health:
            snap.pop("health", None)
        return {k: dumps(v) for k, v in snap.items()}

    def _push_all(self, payload: dict) -> None:
        targets = self._authed()
        if not targets:
            return
        text = dumps(payload)
        for c in targets:
            task = asyncio.create_task(c.send(text))
            self._sends.add(task)
            task.add_done_callback(self._sends.discard)

    def _held_symbols(self) -> set[str]:
        """Símbolos con posición real o trade abierto: siempre van en la lista del panel."""
        held = {pos.symbol for pos in self.service.core.account.positions.values()}
        held.update(t.symbol for t in self.service.trades.open_trades)
        return held

    def _client_rows(self, client: Client, rows: list, held: set) -> list:
        keep = held | {client.symbol} if client.symbol else set(held)
        if client.market_sort == "fav":
            keep |= client.market_pins
        return pick_market_rows(rows, client.market_top, client.market_sort, client.market_query, keep)

    def _want_all_markets(self, client: Client, force: bool = False) -> bool:
        """Modo lite: pide una foto de todos los mercados y se la envía al cliente al llegar.

        Devuelve True si el envío quedó programado (la foto actual es vieja)."""
        market = self.service.core.market
        if market.mode != "lite":
            return False
        stale = time.time() - market.all_tickers_ts > market.SNAPSHOT_EVERY_S
        market.request_all_tickers()
        if stale:
            task = asyncio.create_task(self._send_markets_when_fresh(client, force=force))
            self._sends.add(task)
            task.add_done_callback(self._sends.discard)
        return stale

    async def _send_markets_when_fresh(self, client: Client, timeout: float = 8.0, force: bool = False) -> None:
        market = self.service.core.market
        asked = time.time()
        while time.time() - asked < timeout and not client.ws.closed:
            await asyncio.sleep(0.25)
            if market.all_tickers_ts >= asked:
                break
        if not client.ws.closed and (client.markets or force):
            await self._send_markets(client)

    async def _send_markets(self, client: Client) -> None:
        rows = ws_market_rows(self.service.core.market.market_rows())
        markets = client.markets_text(self._client_rows(client, rows, self._held_symbols()))
        if markets:
            await client.send(markets)

    def _on_event(self, event: dict) -> None:
        self._push_all({"type": "event", "event": event})

    def _on_error(self, entry) -> None:
        self._push_all({"type": "error", "entry": entry.to_dict()})

    async def _broadcast_loop(self) -> None:
        tick = 0
        while True:
            await asyncio.sleep(1)
            tick += 1
            # Una pestaña oculta no recibe el estado periódico (sí eventos y errores) ni
            # mantiene suscrito el precio de su símbolo.
            clients = [c for c in self._authed() if c.visible]
            for c in clients:
                if c.symbol:
                    self.service.note_watch(c.symbol)
            if not clients:
                continue
            if tick % MARKETS_EVERY_S == MARKETS_EVERY_S - 5 and any(c.markets for c in clients):
                # Modo lite: foto de todos los mercados 5 s antes del envío periódico de la lista.
                self.service.core.market.request_all_tickers()
            try:
                parts = self._state_parts(health=tick % HEALTH_EVERY_S == 0)
                rows = ws_market_rows(self.service.core.market.market_rows()) if tick % MARKETS_EVERY_S == 0 else None
                held = self._held_symbols() if rows is not None else set()
                logs = list(self.service.logs.lines)
                for c in clients:
                    state = c.state_text(parts)
                    if state:
                        await c.send(state)
                    if rows is not None and c.markets:
                        markets = c.markets_text(self._client_rows(c, rows, held))
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
        client.traffic = self.service.web_traffic
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
                if data.get("zip") and client.zip is None:
                    client.enable_zip()
                await client.send(dumps({"type": "auth", "ok": True, "env": settings.env_label, "required": required}))
                client.sent.clear()
                client.market_sent = {}
                await client.send(client.state_text(self._state_parts()))
                if not self._want_all_markets(client, force=True):
                    await self._send_markets(client)
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
            if client.markets:
                self._want_all_markets(client)
            if data.get("delta"):
                client.delta = True
            if "top" in data:
                client.market_top = max(0, min(500, int(safe_float(data.get("top"), 0))))
                client.market_sort = str(data.get("sort", "vol"))[:8]
                client.market_query = str(data.get("q", "")).upper().strip()[:30]
                pins = data.get("pins")
                client.market_pins = {str(p).upper()[:30] for p in pins[:100]} if isinstance(pins, list) else set()
                if client.markets:
                    await self._send_markets(client)
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

    # Dashboard en memoria: compress_middleware lo sirve con gzip y ETag (304 si no cambió).
    static_files = {name: (STATIC_DIR / name).read_bytes() for name in ("index.html", "app.js", "app.css")}
    static_types = {"html": "text/html", "js": "application/javascript", "css": "text/css"}

    def static_response(name: str) -> web.Response:
        return web.Response(body=static_files[name], content_type=static_types[name.rsplit(".", 1)[1]],
                            charset="utf-8", headers={"Cache-Control": "no-cache"})

    async def index(_: web.Request) -> web.Response:
        return static_response("index.html")

    async def static_asset(request: web.Request) -> web.Response:
        return static_response(request.match_info["name"])

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

    @web.middleware
    async def traffic_middleware(request: web.Request, handler):
        """Cuenta los bytes de las respuestas HTTP (aprox., antes de gzip)."""
        resp = await handler(request)
        body = getattr(resp, "body", None)
        if isinstance(body, (bytes, bytearray)):
            key = "api_state" if request.path.startswith("/api/state") else "http"
            service.web_traffic[key] += len(body)
        return resp

    app = web.Application(client_max_size=1 << 20,
                          middlewares=[traffic_middleware, compress_middleware, cors_middleware(service),
                                       json_errors_middleware])
    app.router.add_get("/", index)
    app.router.add_get("/ws", hub.handle)
    app.router.add_get("/ws/signal", signal_ws)
    add_route(app, "POST", "/signal", signal)
    add_route(app, "GET", "/api/state", api_state)
    add_route(app, "GET", "/health", health)
    add_route(app, "GET", "/ready", ready)
    register_rest(app, service, auth)
    register_legacy(app, service, auth)
    app.router.add_get("/static/{name:app\\.(?:js|css)}", static_asset)
    app.router.add_static("/static", STATIC_DIR, append_version=True)
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app
