"""API HTTP, rutas antiguas, compatibilidad con los bots desplegados y executor_client."""

import asyncio

import pytest
from aiohttp.test_utils import TestClient, TestServer

import executor_client
from executor.config import LEGACY_SIGNAL_SECRETS
from executor.precision import parse_bool
from executor.service import COMMANDS, ExecutorService
from executor.web.api import REST_ROUTES
from executor.web.server import build_app
from tests.compat import bridge_app25, bridge_standalone
from tests.conftest import make_settings, wait_for

SECRET = "clave-secreta-aleatoria"


def legacy_settings(fake, tmp_path, **over):
    """Executor desplegado sin SIGNAL_SECRET ni DASHBOARD_TOKEN (como en Render)."""
    over.setdefault("dashboard_token", "")
    return make_settings(fake, tmp_path, signal_secret=LEGACY_SIGNAL_SECRETS[0],
                         signal_secrets=list(LEGACY_SIGNAL_SECRETS), signal_secret_is_default=True, **over)


@pytest.fixture
async def open_service(fake, tmp_path, snapshot_file):
    svc = ExecutorService(legacy_settings(fake, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.core.market.marks and svc.user.conn.connected and svc.trades.ready.is_set())
    yield svc
    await svc.stop()


@pytest.fixture
async def live_url(open_service):
    """Servidor HTTP real (los bridges usan urllib en hilos)."""
    server = TestServer(build_app(open_service))
    await server.start_server()
    yield str(server.make_url("")).rstrip("/")
    await server.close()


def trade(svc, symbol="ETHUSDT", direction="SHORT"):
    return svc.trades.get_trade(symbol, direction)


# ── Bots desplegados (copias literales) ─────────────────────────────────────
async def test_deployed_standalone_bridge(open_service, live_url):
    logs = []
    bridge = bridge_standalone.ExecutorBridge(executor_url=live_url + "/", signal_secret=SECRET,
                                              poll_secs=1, logger=logs.append)
    # Dos tramos con el mismo trade_id: ambos deben ejecutarse (sin antiduplicado).
    await asyncio.to_thread(bridge.notify_open, 1, "ETHUSDT", "SHORT", 3000, 0.01)
    await asyncio.to_thread(bridge.notify_open, 1, "ETHUSDT", "SHORT", 3000, 0.01)
    await wait_for(lambda: trade(open_service) is not None and trade(open_service).quantity == pytest.approx(0.02))
    assert not logs, logs

    state = await asyncio.to_thread(bridge.fetch_state_sync)
    for key in ("balance", "equity", "realized_pnl", "unrealized_pnl", "wins", "losses", "win_rate",
                "open_trades", "closed_trades", "executor_status", "leverage", "trading_enabled"):
        assert key in state
    assert state["open_trades"][0]["paper_trade_id"] == 1

    await asyncio.to_thread(bridge.notify_close, 1, "ETHUSDT", "SHORT", "TP", 2990)
    await wait_for(lambda: trade(open_service) is None)
    closed = open_service.trades.closed[-1]
    assert closed.status == "TP" and closed.closed_by == "signal"

    seen = []
    flag = {"run": True}

    def on_state(data):
        seen.append(data)
        flag["run"] = False

    await asyncio.wait_for(bridge.poll_state_loop(lambda: flag["run"], on_state), 10)
    assert seen and seen[0]["closed_trades"][-1]["status"] == "TP"


async def test_deployed_app25_bridge(open_service, live_url):
    logs = []
    bridge = bridge_app25.ExecutorBridge(executor_url=live_url, signal_secret=SECRET, logger=logs.append)
    # app_25 notifica sin bloquear desde el event loop.
    bridge.notify_open(trade_id=9, symbol="ETHUSDT", direction="SHORT", price=3000, quantity=0.01,
                       notional=30, level=50)
    await wait_for(lambda: trade(open_service) is not None)
    bridge.notify_open(trade_id=9, symbol="ETHUSDT", direction="SHORT", price=3000, quantity=0.01,
                       notional=30, level=75)
    await wait_for(lambda: trade(open_service).quantity == pytest.approx(0.02))
    assert trade(open_service).to_dict()["levels"] == [50, 75]
    bridge.notify_close(trade_id=9, symbol="ETHUSDT", direction="SHORT", reason="SL", close_price=3010, pnl=-0.2)
    await wait_for(lambda: trade(open_service) is None)
    closed = open_service.trades.closed[-1]
    assert closed.status == "SL" and closed.bot_pnl == pytest.approx(-0.2)
    await wait_for(lambda: len([m for m in logs if "✓ señal enviada" in m]) == 3)
    assert not [m for m in logs if "error" in m], logs


async def test_wrong_secret_is_visible(open_service, live_url):
    bridge = bridge_standalone.ExecutorBridge(executor_url=live_url, signal_secret="otro", logger=lambda m: None)
    await asyncio.to_thread(bridge.notify_open, 2, "ETHUSDT", "SHORT", 3000, 0.01)
    assert open_service.trades.status["signals_unauthorized"] == 1
    last = open_service.trades.signals(1)[0]
    assert not last["ok"] and "secreto" in last["detail"]
    assert trade(open_service) is None


# ── executor_client.ExecutorBridge (reemplazo directo) ─────────────────────
async def test_new_bridge_is_drop_in(open_service, live_url):
    logs = []
    bridge = executor_client.ExecutorBridge(executor_url=live_url, signal_secret=SECRET, logger=logs.append)
    sid = bridge.notify_open(trade_id=3, symbol="ETHUSDT", direction="SHORT", price=3000, quantity=0.01,
                             notional=30, level=50)
    bridge.notify_open(trade_id=3, symbol="ETHUSDT", direction="SHORT", price=3000, quantity=0.01)
    bridge.notify_close(trade_id=3, symbol="ETHUSDT", direction="SHORT", reason="TP", close_price=2990, pnl=0.1)
    assert sid and bridge.pending >= 1
    assert await asyncio.to_thread(bridge.flush, 20)
    await wait_for(lambda: open_service.trades.closed and open_service.trades.closed[-1].paper_trade_id == 3)
    closed = open_service.trades.closed[-1]
    assert closed.quantity == pytest.approx(0.02) and closed.status == "TP"  # orden open→open→close respetado
    assert bridge.sent == 3 and bridge.failed == 0
    assert open_service.trades.signals(signal_id=sid)[0]["ok"]

    # El mismo signal_id dos veces (reintento) no abre dos veces.
    payload = {"action": "open", "trade_id": 4, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
               "quantity": 0.01, "signal_id": "retry-1"}
    first = await asyncio.to_thread(bridge.send_signal_sync, payload)
    again = await asyncio.to_thread(bridge.send_signal_sync, payload)
    assert first["ok"] and again["ok"] and again["duplicate"]
    await wait_for(lambda: trade(open_service) is not None)
    await asyncio.sleep(0.3)
    assert trade(open_service).quantity == pytest.approx(0.01)

    state = await asyncio.to_thread(bridge.fetch_state_sync)
    assert state["open_count"] == 1


async def test_new_bridge_retries_while_restarting(open_service, live_url):
    bridge = executor_client.ExecutorBridge(live_url, SECRET, logger=lambda m: None, retry_backoff=0.2)
    open_service.trades.accepting = False  # executor reiniciándose → 503

    async def reopen():
        await asyncio.sleep(0.5)
        open_service.trades.accepting = True

    task = asyncio.create_task(reopen())
    body = await asyncio.to_thread(bridge.send_signal_sync, {"action": "open", "trade_id": 5, "symbol": "ETHUSDT",
                                                             "direction": "SHORT", "price": 3000, "quantity": 0.01})
    await task
    assert body and body["ok"]
    await wait_for(lambda: trade(open_service) is not None)
    # El 503 no marcó el trade_id como rechazado: su close funciona.
    open_service.trades.handle_signal({"action": "close", "trade_id": 5, "symbol": "ETHUSDT", "direction": "SHORT"})
    await wait_for(lambda: trade(open_service) is None)


async def test_new_bridge_without_url_is_noop():
    bridge = executor_client.ExecutorBridge("", SECRET)
    assert bridge.notify_open(1, "BTCUSDT", "LONG", 1, 1) == ""
    assert bridge.send_signal_sync({"action": "close"}) is None
    assert bridge.fetch_state_sync() is None
    assert executor_client._ExecutorSignalConfig is executor_client.ExecutorSignalConfig


# ── executor_client.ExecutorClient (todos los endpoints) ────────────────────
async def test_executor_client_end_to_end(open_service, live_url, fake):
    c = executor_client.ExecutorClient(live_url, secret=SECRET, timeout=10)

    def flow():
        assert c.wait_ready(5)
        assert c.health()["ok"]
        assert {"balance", "open_trades"} <= set(c.state(limit=5))
        reply = c.open_signal("ETHUSDT", "LONG", trade_id=21, price=3000, quantity=0.01, level=50, sl=2800)
        assert reply["ok"] and reply["result"]["ok"], reply
        pos = c.positions("ETHUSDT")
        assert pos and pos[0]["direction"] == "LONG"
        assert c.set_tp("ETHUSDT", 3300)["tp"] == 3300
        kinds = {o["kind"] for o in c.orders("ETHUSDT")}
        assert "algo" in kinds
        assert c.cancel_tp_sl("ETHUSDT", kind="TP")["cancelled"] >= 1
        assert c.signals(signal_id=reply["signal_id"])[0]["ok"]
        assert c.close("ETHUSDT")["closed"] is True
        closed = c.trades(status="closed")["closed"]
        assert closed[0]["paper_trade_id"] == 21 and closed[0]["closed_by"] == "api"
        assert "ETHUSDT" in c.trades_csv()
        assert c.explain_error(-2019)["code"] == -2019
        assert c.command("explain_error", code=-1111)["code"] == -1111
        assert c.set_leverage(6)["leverage"] == 6
        assert c.set_trading(False)["trading_enabled"] is False
        paused = c.open_signal("ETHUSDT", "LONG", trade_id=22, quantity=0.01, price=3000)
        assert paused["ok"] is False and "pausado" in paused["error"]
        assert c.set_trading()["trading_enabled"] is True
        assert c.manual("toggle_trading") == {"ok": True, "trading_enabled": False}
        assert c.manual("position_mode", method="GET")["ok"]
        preview = c.grid_preview("GRIDUSDT", 90, 110, 4, 200, leverage=2)
        assert preview["levels"]
        assert {r["cmd"] for r in c.commands()} == set(COMMANDS)
        with pytest.raises(executor_client.ExecutorError) as exc:
            c.close("BTCUSDT")
        assert exc.value.status == 404
        with pytest.raises(executor_client.ExecutorError) as exc:
            c.set_leverage(500)
        assert exc.value.status == 400

    await asyncio.to_thread(flow)


async def test_executor_client_cli(open_service, live_url, capsys):
    rc = await asyncio.to_thread(executor_client.main, ["--url", live_url, "--secret", SECRET, "explain", "-1111"])
    assert rc == 0 and '"code": -1111' in capsys.readouterr().out
    rc = await asyncio.to_thread(executor_client.main, ["--url", live_url, "--secret", SECRET, "cmd", "set_default_leverage",
                                                        "leverage=7"])
    assert rc == 0 and open_service.settings.leverage == 7
    rc = await asyncio.to_thread(executor_client.main, ["--url", live_url, "--secret", "malo", "close-all"])
    assert rc == 1 and "401" in capsys.readouterr().err
    rc = await asyncio.to_thread(executor_client.main, ["--url", live_url, "demo"])
    assert rc == 0 and "GET /api/state" in capsys.readouterr().out


async def test_websocket_clients(open_service, live_url):
    async with executor_client.SignalSocket(live_url, SECRET) as ws:
        assert (await ws.ping())["pong"]
        reply = await ws.send({"action": "open", "trade_id": 31, "symbol": "ETHUSDT", "direction": "SHORT",
                               "price": 3000, "quantity": 0.01}, wait=True)
        assert reply["status"] == 200 and reply["result"]["ok"]
    async with executor_client.DashboardSocket(live_url) as dash:
        positions = await dash.command("positions", symbol="ETHUSDT")
        assert positions and positions[0]["direction"] == "SHORT"
        msg = await asyncio.wait_for(dash.__aiter__().__anext__(), 5)
        assert msg["type"] in ("state", "markets", "errors", "event", "logs")
        with pytest.raises(executor_client.ExecutorError) as exc:
            await dash.command("nope")
        assert exc.value.status == 404
    async with executor_client.SignalSocket(live_url, "malo") as ws:
        bad = await ws.send({"action": "close_all"})
        assert bad["status"] == 401


# ── Rutas REST, autenticación, CORS y errores JSON ──────────────────────────
def test_every_command_has_a_route():
    routes = {f"{m} {p}" for m, p, _, _ in REST_ROUTES}
    routes |= {"GET /api/state", "GET /health"}
    for name, meta in COMMANDS.items():
        assert meta["rest"] in routes, f"{name}: {meta['rest']}"
    assert {cmd for _, _, cmd, _ in REST_ROUTES} <= set(COMMANDS)


async def test_rest_auth_cors_and_json_errors(service, fake):
    """Con DASHBOARD_TOKEN y SIGNAL_SECRET definidos (fixture ``service``)."""
    async with TestClient(TestServer(build_app(service))) as client:
        r = await client.get("/api/positions")
        assert r.status == 401 and (await r.json())["ok"] is False
        r = await client.get("/api/positions", headers={"X-Dashboard-Token": "dash"})
        body = await r.json()
        assert r.status == 200 and body["ok"] and body["action"] == "positions" and body["data"] == []
        r = await client.get("/api/positions", headers={"Authorization": "Bearer sig"})
        assert r.status == 200
        # /api/state y /health siguen abiertos (los bots no envían token al consultar).
        assert (await client.get("/api/state")).status == 200
        assert (await client.head("/health")).status == 200
        assert (await client.get("/ready")).status == 200

        # Escritura: requiere el secreto.
        r = await client.post("/api/leverage", json={"leverage": 7})
        assert r.status == 401 and "SIGNAL_SECRET" in (await r.json())["detail"]
        r = await client.post("/api/leverage/", data={"leverage": "7"}, headers={"X-Signal-Secret": "sig"})
        assert r.status == 200 and (await r.json())["leverage"] == 7
        r = await client.post("/api/leverage?secret=sig&leverage=8")
        assert r.status == 200 and service.settings.leverage == 8
        r = await client.post("/api/leverage", json={"leverage": 0}, headers={"X-Signal-Secret": "sig"})
        assert r.status == 400 and "leverage" in (await r.json())["error"]
        r = await client.post("/api/close/BTCUSDT", headers={"X-Signal-Secret": "sig"})
        assert r.status == 404
        r = await client.post("/api/leverage", data=b"{no json", headers={"X-Signal-Secret": "sig",
                                                                          "Content-Type": "application/json"})
        assert r.status == 400 and (await r.json())["error"] == "invalid json"

        # Errores en JSON.
        r = await client.get("/api/no-existe")
        assert r.status == 404 and (await r.json())["ok"] is False
        r = await client.put("/api/positions")
        body = await r.json()
        assert r.status == 405 and "GET" in body["allowed"]
        r = await client.post("/api/command", json={"cmd": "nope"}, headers={"X-Signal-Secret": "sig"})
        assert r.status == 404

        # CORS: preflight y cabeceras (CORS_ORIGINS="*" por defecto).
        r = await client.options("/api/close/ETHUSDT", headers={"Origin": "https://mi-bot.example",
                                                                "Access-Control-Request-Method": "POST"})
        assert r.status == 204 and r.headers["Access-Control-Allow-Origin"] == "*"
        assert "X-Signal-Secret" in r.headers["Access-Control-Allow-Headers"]
        r = await client.get("/api/state", headers={"Origin": "https://mi-bot.example"})
        assert r.headers["Access-Control-Allow-Origin"] == "*"

        # /signal con barra final, secreto en el cuerpo y espera del resultado.
        r = await client.post("/signal/?wait=1", json={"action": "open", "trade_id": 41, "symbol": "ETHUSDT",
                                                      "direction": "LONG", "price": 3000, "quantity": 0.01,
                                                      "secret": "sig"})
        body = await r.json()
        assert r.status == 200 and body["result"]["ok"], body


async def test_legacy_manual_routes(service, fake):
    h = {"X-Signal-Secret": "sig"}
    async with TestClient(TestServer(build_app(service))) as client:
        r = await client.post("/manual/toggle_trading")
        assert r.status == 401
        r = await client.post("/manual/toggle_trading", headers=h)
        assert await r.json() == {"ok": True, "trading_enabled": False}
        r = await client.post("/manual/toggle_trading", headers=h)
        assert (await r.json())["trading_enabled"] is True
        r = await client.post("/manual/set_leverage", json={"leverage": 6}, headers=h)
        assert await r.json() == {"ok": True, "leverage": 6}
        r = await client.post("/manual/close", json={"symbol": "ETHUSDT"}, headers=h)
        assert r.status == 404 and (await r.json())["ok"] is False

        service.trades.handle_signal({"action": "open", "trade_id": 51, "symbol": "ETHUSDT", "direction": "LONG",
                                      "price": 3000, "quantity": 0.01})
        await wait_for(lambda: service.core.account.position("ETHUSDT", "LONG") is not None)
        r = await client.post("/manual/set_sl", json={"symbol": "ETHUSDT", "trigger_price": 2800}, headers=h)
        body = await r.json()
        assert r.status == 200 and body["sl"] == 2800 and body["symbol"] == "ETHUSDT"
        r = await client.get("/manual/orders", params={"symbol": "ETHUSDT"}, headers=h)
        body = await r.json()
        assert body["ok"] and any(o.get("_algo") for o in body["orders"])
        r = await client.post("/manual/cancel_tp_sl", json={"symbol": "ETHUSDT"}, headers=h)
        assert (await r.json())["cancelled"] >= 1
        r = await client.post("/manual/close", json={"symbol": "ETHUSDT"}, headers=h)
        assert await r.json() == {"ok": True, "action": "manual_close", "symbol": "ETHUSDT"}
        await wait_for(lambda: service.trades.get_trade("ETHUSDT", "LONG") is None)
        assert service.trades.closed[-1].status == "MANUAL"
        r = await client.post("/manual/close_all", headers=h)
        assert (await r.json())["action"] == "manual_close_all"
        r = await client.post("/manual/clear_history", headers=h)
        assert (await r.json())["ok"]


# ── Señales: orden, reinicio y arranque ─────────────────────────────────────
async def test_close_before_open_is_applied(service, fake):
    t = service.trades
    status, _ = t.handle_signal({"action": "close", "trade_id": 61, "symbol": "ETHUSDT", "direction": "SHORT",
                                 "reason": "TP"})
    assert status == 200
    await asyncio.sleep(0.2)
    t.handle_signal({"action": "open", "trade_id": 61, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
                     "quantity": 0.01})
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 61)
    assert t.get_trade("ETHUSDT", "SHORT") is None
    assert fake.positions[("ETHUSDT", "SHORT")]["amt"] == 0


async def test_close_waits_for_inflight_open(service, fake):
    t = service.trades
    gate = asyncio.Event()
    original = service.orders.ensure_leverage

    async def slow_leverage(*a, **k):
        await gate.wait()
        return await original(*a, **k)

    service.orders.ensure_leverage = slow_leverage
    t.handle_signal({"action": "open", "trade_id": 62, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
                     "quantity": 0.01})
    t.handle_signal({"action": "close", "trade_id": 62, "symbol": "ETHUSDT", "direction": "SHORT", "reason": "SL"})
    await asyncio.sleep(0.3)
    gate.set()
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 62)
    assert t.closed[-1].status == "SL" and t.get_trade("ETHUSDT", "SHORT") is None


async def test_drain_returns_503_and_ready_gate(fake, tmp_path, snapshot_file):
    svc = ExecutorService(legacy_settings(fake, tmp_path))
    async with TestClient(TestServer(build_app(svc))) as client:
        r = await client.get("/ready")
        assert r.status == 503 and (await r.json())["ready"] is False
        await svc.start()
        await wait_for(lambda: svc.trades.ready.is_set())
        assert (await client.get("/ready")).status == 200
        await svc.trades.drain(1)
        r = await client.post("/signal", json={"action": "close_all"}, headers={"X-Signal-Secret": SECRET})
        body = await r.json()
        assert r.status == 503 and body["retry"] is True
        assert (await client.get("/ready")).status == 503
    await svc.stop()


def test_parse_bool():
    for v in (True, 1, "1", "true", "TRUE", "yes", "si", "sí", "on"):
        assert parse_bool(v) is True
    for v in (False, 0, "0", "false", "no", "off", ""):
        assert parse_bool(v, True) is False
    assert parse_bool("quizás", True) is True and parse_bool(None, True) is True


# ── Regresiones de la revisión adversarial ─────────────────────────────────
async def test_unknown_status_open_is_adopted_and_closable(service, fake):
    """order.place se ejecuta pero responde -1007 y order.status falla: no debe quedar huérfana."""
    t = service.trades
    fake.inject_after.append(("order.place", -1007, "Timeout waiting for response from backend server."))
    for _ in range(3):
        fake.inject.append(("order.status", -1001, "Internal error; unable to process your request."))
    t.handle_signal({"action": "open", "trade_id": 77, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
                     "quantity": 0.01})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None, timeout=12)
    assert (77, "ETHUSDT") not in t._rejected_ids
    assert t.get_trade("ETHUSDT", "SHORT").paper_trade_id == 77
    t.handle_signal({"action": "close", "trade_id": 77, "symbol": "ETHUSDT", "direction": "SHORT", "reason": "TP"})
    await wait_for(lambda: fake.positions[("ETHUSDT", "SHORT")]["amt"] == 0)
    assert t.closed[-1].paper_trade_id == 77


async def test_late_tranche_after_close_does_not_reopen(service, fake):
    t = service.trades
    sig = {"action": "open", "trade_id": 81, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
           "quantity": 0.01}
    t.handle_signal(dict(sig))
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None)
    t.handle_signal({"action": "close", "trade_id": 81, "symbol": "ETHUSDT", "direction": "SHORT", "reason": "SL"})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is None)
    t.handle_signal(dict(sig, level=75))  # tramo que llegó tarde
    await wait_for(lambda: t.signals(1) and "tramo ignorado" in t.signals(1)[0]["detail"])
    assert t.get_trade("ETHUSDT", "SHORT") is None and fake.positions[("ETHUSDT", "SHORT")]["amt"] == 0
    # Otro trade_id sí abre.
    t.handle_signal(dict(sig, trade_id=82))
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None)


async def test_close_without_trade_id_does_not_wait_for_next_open(service, fake):
    t = service.trades
    t.handle_signal({"action": "close", "symbol": "ETHUSDT", "direction": "SHORT"})
    await wait_for(lambda: t.signals(1) and "close sin trade_id" in t.signals(1)[0]["detail"])
    t.handle_signal({"action": "open", "trade_id": 83, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
                     "quantity": 0.01})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None)
    await asyncio.sleep(0.3)
    assert t.get_trade("ETHUSDT", "SHORT") is not None


async def test_close_does_not_swallow_open_accepted_after_it(service, fake):
    t = service.trades
    gate = asyncio.Event()
    original = service.orders.ensure_leverage

    async def slow_leverage(*a, **k):
        await gate.wait()
        return await original(*a, **k)

    service.orders.ensure_leverage = slow_leverage
    base = {"action": "open", "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000, "quantity": 0.01}
    t.handle_signal(dict(base, trade_id=91))
    t.handle_signal({"action": "close", "trade_id": 91, "symbol": "ETHUSDT", "direction": "SHORT", "reason": "TP"})
    t.handle_signal(dict(base, trade_id=92))
    await asyncio.sleep(0.2)
    gate.set()
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 91)
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None)
    assert t.closed[-1].quantity == pytest.approx(0.01)
    assert t.get_trade("ETHUSDT", "SHORT").paper_trade_id == 92
    assert fake.positions[("ETHUSDT", "SHORT")]["amt"] == pytest.approx(-0.01)


async def test_quantity_times_price_defines_size(service, fake):
    t = service.trades
    t.handle_signal({"action": "open", "trade_id": 93, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
                     "quantity": 0.01, "notional": 60})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None)
    assert t.get_trade("ETHUSDT", "SHORT").quantity == pytest.approx(0.01)
    assert "se usa quantity" in t.signals(1)[0]["detail"]


async def test_dashboard_token_rejects_public_default_secret(fake, tmp_path, snapshot_file):
    svc = ExecutorService(legacy_settings(fake, tmp_path, dashboard_token="tok"))
    await svc.start()
    async with TestClient(TestServer(build_app(svc))) as client:
        assert (await client.get("/api/positions", headers={"X-Signal-Secret": SECRET})).status == 401
        assert (await client.post("/api/trading", headers={"X-Signal-Secret": SECRET})).status == 401
        # Varias credenciales: basta con que una sea válida.
        r = await client.get("/api/positions", headers={"X-Signal-Secret": SECRET, "X-Dashboard-Token": "tok"})
        assert r.status == 200
        r = await client.get("/api/trades.csv", headers={"Authorization": "Bearer tok"})
        assert r.status == 200
        # /signal (los bots) sigue aceptando el secreto por defecto.
        r = await client.post("/signal", json={"action": "close_all"}, headers={"X-Signal-Secret": SECRET})
        assert r.status == 200
        ws = await client.ws_connect("/ws")
        await ws.send_json({"op": "auth", "token": SECRET})
        assert (await ws.receive_json(timeout=5))["ok"] is False
        await ws.close()
    await svc.stop()


async def test_cross_site_dashboard_websocket_is_rejected(open_service):
    async with TestClient(TestServer(build_app(open_service))) as client:
        r = await client.get("/ws", headers={"Origin": "https://evil.example", "Connection": "Upgrade",
                                             "Upgrade": "websocket", "Sec-WebSocket-Version": "13",
                                             "Sec-WebSocket-Key": "dGhlIHNhbXBsZSBub25jZQ=="})
        assert r.status == 403
        host = f"http://{client.host}:{client.port}"
        ws = await client.ws_connect("/ws", origin=host)  # el propio dashboard
        await ws.send_json({"op": "auth", "token": ""})
        assert (await ws.receive_json(timeout=5))["ok"] is True
        await ws.close()


async def test_api_signal_reports_rejection_and_idempotency(open_service):
    h = {"X-Signal-Secret": SECRET}
    async with TestClient(TestServer(build_app(open_service))) as client:
        r = await client.post("/api/trading", json={"enabled": False}, headers=h)
        assert (await r.json())["trading_enabled"] is False
        r = await client.post("/api/signal", json={"action": "open", "trade_id": 1, "symbol": "ETHUSDT",
                                                   "direction": "SHORT", "quantity": 0.01, "price": 3000}, headers=h)
        assert r.status == 409 and (await r.json())["ok"] is False
        await client.post("/api/trading", json={"enabled": True}, headers=h)
        body = {"action": "open", "trade_id": 2, "symbol": "ETHUSDT", "direction": "SHORT", "quantity": 0.01,
                "price": 3000}
        r1 = await client.post("/api/signal", json=body, headers={**h, "Idempotency-Key": "k-1"})
        r2 = await client.post("/api/signal", json=body, headers={**h, "Idempotency-Key": "k-1"})
        assert r1.status == 200 and (await r2.json())["data"]["duplicate"] is True
        await asyncio.sleep(0.3)
        assert trade(open_service).quantity == pytest.approx(0.01)


async def test_legacy_close_is_ambiguous_with_long_and_short(open_service, fake):
    t = open_service.trades
    for d in ("LONG", "SHORT"):
        t.handle_signal({"action": "open", "trade_id": 3, "symbol": "ETHUSDT", "direction": d, "price": 3000,
                         "quantity": 0.01})
    await wait_for(lambda: len(t.open_trades) == 2)
    async with TestClient(TestServer(build_app(open_service))) as client:
        h = {"X-Signal-Secret": SECRET}
        r = await client.post("/manual/close", json={"symbol": "ETHUSDT", "direction": None}, headers=h)
        assert r.status == 404
        r = await client.post("/manual/close", json={"symbol": "ETHUSDT", "direction": "short"}, headers=h)
        assert r.status == 200
    await wait_for(lambda: len(t.open_trades) == 1)
    assert t.open_trades[0].direction == "LONG"


async def test_secrets_are_not_logged_and_signal_log_is_sanitized(open_service):
    from executor.web.api import _safe_path
    from aiohttp.test_utils import make_mocked_request
    req = make_mocked_request("POST", "/api/close/BTCUSDT?secret=abc&symbol=BTCUSDT")
    assert _safe_path(req) == "/api/close/BTCUSDT?secret=***&symbol=BTCUSDT"
    t = open_service.trades
    t.handle_signal({"action": "open", "symbol": "X" * 500, "direction": '"><img src=x onerror=alert(1)>'})
    entry = t.signals(1)[0]
    assert entry["direction"] == "" and len(entry["symbol"]) <= 32


def test_signal_secret_with_comma_is_kept(monkeypatch):
    from executor.config import load_settings
    monkeypatch.setenv("SIGNAL_SECRET", "a,b")
    s = load_settings()
    assert s.check_signal_secret("a,b") and not s.check_signal_secret("a") and not s.signal_secret_is_default
    monkeypatch.setenv("SIGNAL_SECRETS", "otro")
    assert load_settings().check_signal_secret("otro")


def test_bridge_never_raises():
    bridge = executor_client.ExecutorBridge("http://[::1", SECRET, logger=lambda m: None, retries=0)
    assert bridge.send_signal_sync({"action": "open", "price": object()}) is None
    assert bridge.failed == 1
