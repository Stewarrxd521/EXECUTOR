"""Servidor HTTP/WebSocket del executor.

* ``GET  /``            dashboard (estático, sin secretos embebidos).
* ``GET  /ws``          WebSocket del dashboard: estado en vivo + comandos.
* ``GET  /ws/signal``   WebSocket para enviar señales sin REST (header X-Signal-Secret).
* ``POST /signal``      señales HTTP (compatibilidad con app.py).
* ``GET  /api/state``   estado JSON (compatibilidad con app.py).
* ``POST /api/command`` comandos del dashboard por HTTP (scripts).
* ``GET  /health``      salud del servicio.
"""

from __future__ import annotations

import asyncio
import hmac
import json
import logging
import time
from pathlib import Path
from typing import Optional

from aiohttp import WSMsgType, web

from .. import __version__
from ..errors import BinanceAPIError, ErrorDoctor
from ..grid import GridError
from ..service import CommandError, ExecutorService

log = logging.getLogger("executor.web")
STATIC_DIR = Path(__file__).resolve().parent / "static"

try:  # orjson es opcional: acelera la serialización si está instalado
    import orjson

    def dumps(data) -> str:
        return orjson.dumps(data, option=orjson.OPT_NON_STR_KEYS, default=str).decode()
except ImportError:  # pragma: no cover
    def dumps(data) -> str:
        return json.dumps(data, separators=(",", ":"), default=str)


def _safe_eq(a: str, b: str) -> bool:
    return bool(a) and hmac.compare_digest(a.encode(), b.encode())


class Client:
    def __init__(self, ws: web.WebSocketResponse, ip: str):
        self.ws = ws
        self.ip = ip
        self.authed = False
        self.markets = False
        self.symbol = ""
        self.log_seq = 0

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
        self._failed_auth: dict[str, float] = {}
        self._task: Optional[asyncio.Task] = None
        service.core.bus.subscribe(self._on_event)
        service.core.journal.subscribe(self._on_error)

    def start(self) -> None:
        self._task = asyncio.create_task(self._broadcast_loop(), name="dashboard-broadcast")

    def _authed(self) -> list[Client]:
        return [c for c in self.clients if c.authed and not c.ws.closed]

    def _push_all(self, payload: dict) -> None:
        targets = self._authed()
        if not targets:
            return
        text = dumps(payload)
        for c in targets:
            asyncio.create_task(c.send(text))

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
            if not clients:
                continue
            try:
                state = dumps({"type": "state", "data": self.service.snapshot()})
                markets = dumps({"type": "markets", "rows": self.service.core.market.market_rows()}) if tick % 2 == 0 else None
                logs = list(self.service.logs.lines)
                for c in clients:
                    await c.send(state)
                    if markets and c.markets:
                        await c.send(markets)
                    if c.symbol:
                        await c.send(dumps({"type": "ticker", "data": self.service.symbol_view(c.symbol)}))
                        self.service.watch[c.symbol] = time.time()
                    fresh = [ln for ln in logs if ln["id"] > c.log_seq]
                    if fresh:
                        c.log_seq = fresh[-1]["id"]
                        await c.send(dumps({"type": "logs", "lines": fresh[-100:]}))
            except Exception:
                log.exception("Dashboard: error enviando estado")

    async def handle(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=25, max_msg_size=1 << 20)
        await ws.prepare(request)
        client = Client(ws, request.remote or "?")
        self.clients.add(client)
        try:
            async for msg in ws:
                if msg.type != WSMsgType.TEXT:
                    continue
                try:
                    data = json.loads(msg.data)
                except ValueError:
                    continue
                await self._on_message(client, data)
        finally:
            self.clients.discard(client)
        return ws

    async def _on_message(self, client: Client, data: dict) -> None:
        op = data.get("op")
        if op == "auth":
            if time.time() - self._failed_auth.get(client.ip, 0) < 2:
                await asyncio.sleep(2)
            if _safe_eq(str(data.get("token", "")), self.service.settings.dashboard_token):
                client.authed = True
                await client.send(dumps({"type": "auth", "ok": True, "env": self.service.settings.env_label}))
                await client.send(dumps({"type": "state", "data": self.service.snapshot()}))
                await client.send(dumps({"type": "markets", "rows": self.service.core.market.market_rows()}))
                await client.send(dumps({"type": "errors", "entries": self.service.core.journal.entries(150)}))
                client.log_seq = 0
            else:
                self._failed_auth[client.ip] = time.time()
                await client.send(dumps({"type": "auth", "ok": False, "error": "token inválido"}))
            return
        if not client.authed:
            await client.send(dumps({"type": "auth", "ok": False, "error": "no autenticado"}))
            return
        if op == "markets":
            client.markets = bool(data.get("on", True))
        elif op == "select":
            client.symbol = str(data.get("symbol", "")).upper()[:30]
            if client.symbol:
                self.service.watch[client.symbol] = time.time()
                await client.send(dumps({"type": "ticker", "data": self.service.symbol_view(client.symbol)}))
        elif op == "cmd":
            asyncio.create_task(self._run_command(client, data))

    async def _run_command(self, client: Client, data: dict) -> None:
        req_id = data.get("id")
        cmd = str(data.get("cmd", ""))
        reply = await run_command(self.service, cmd, data.get("args") or {})
        reply.update({"type": "reply", "id": req_id, "cmd": cmd})
        await client.send(dumps(reply))


async def run_command(service: ExecutorService, cmd: str, args: dict) -> dict:
    try:
        result = await service.execute(cmd, args)
        return {"ok": True, "data": result}
    except BinanceAPIError as err:
        diag = ErrorDoctor.diagnose(err)
        return {"ok": False, "error": f"[{diag.code}] {diag.info.title}", "diagnosis": diag.to_dict()}
    except (CommandError, GridError, ValueError, KeyError) as exc:
        return {"ok": False, "error": str(exc)}
    except Exception as exc:  # pragma: no cover - errores inesperados
        log.exception("Comando %s falló", cmd)
        diag = ErrorDoctor.diagnose(exc)
        return {"ok": False, "error": str(exc), "diagnosis": diag.to_dict()}


def build_app(service: ExecutorService) -> web.Application:
    settings = service.settings
    hub = DashboardHub(service)

    def signal_auth(request: web.Request) -> bool:
        return _safe_eq(request.headers.get("X-Signal-Secret", ""), settings.signal_secret)

    async def index(_: web.Request) -> web.StreamResponse:
        return web.FileResponse(STATIC_DIR / "index.html", headers={"Cache-Control": "no-cache"})

    async def signal(request: web.Request) -> web.Response:
        if not signal_auth(request):
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
        try:
            data = await request.json()
        except ValueError:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        if not isinstance(data, dict):
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        status, body = service.trades.handle_signal(data, source="http")
        return web.json_response(body, status=status)

    async def signal_ws(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=25)
        await ws.prepare(request)
        authed = signal_auth(request)
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except ValueError:
                await ws.send_str(dumps({"ok": False, "error": "invalid json"}))
                continue
            if not authed:
                authed = _safe_eq(str(data.get("secret", "")), settings.signal_secret)
                await ws.send_str(dumps({"ok": authed, "auth": True} if authed else
                                        {"ok": False, "error": "unauthorized"}))
                continue
            status, body = service.trades.handle_signal(data, source="ws")
            body["status"] = status
            if data.get("id") is not None:
                body["id"] = data["id"]
            await ws.send_str(dumps(body))
        return ws

    async def api_state(request: web.Request) -> web.Response:
        if settings.state_requires_auth and not (
                signal_auth(request) or _safe_eq(request.headers.get("X-Dashboard-Token", ""), settings.dashboard_token)):
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
        return web.Response(text=dumps(service.api_state()), content_type="application/json")

    async def api_command(request: web.Request) -> web.Response:
        if not _safe_eq(request.headers.get("X-Dashboard-Token", ""), settings.dashboard_token):
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
        try:
            data = await request.json()
        except ValueError:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)
        reply = await run_command(service, str(data.get("cmd", "")), data.get("args") or {})
        return web.Response(text=dumps(reply), content_type="application/json", status=200 if reply["ok"] else 400)

    async def health(_: web.Request) -> web.Response:
        h = service.health()
        ok = h["market"]["connected"] and (not h["credentials"] or h["ws_api"]["connected"] or h["user"]["connected"])
        return web.Response(text=dumps({"ok": bool(ok), "version": __version__,
                                        "uptime_s": int(time.time() - service.started_ts), **h}),
                            content_type="application/json")

    async def on_startup(_: web.Application) -> None:
        hub.start()

    app = web.Application(client_max_size=1 << 20)
    app.router.add_get("/", index)
    app.router.add_get("/ws", hub.handle)
    app.router.add_get("/ws/signal", signal_ws)
    app.router.add_post("/signal", signal)
    app.router.add_get("/api/state", api_state)
    app.router.add_post("/api/command", api_command)
    app.router.add_get("/health", health)
    app.router.add_static("/static", STATIC_DIR, append_version=True)
    app.on_startup.append(on_startup)
    return app
