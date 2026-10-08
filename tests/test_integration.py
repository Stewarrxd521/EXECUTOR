"""Pruebas de extremo a extremo contra el Binance simulado."""

import pytest
from aiohttp.test_utils import TestClient, TestServer

from executor.service import ExecutorService
from executor.web.server import build_app
from tests.conftest import make_settings, wait_for


def methods(fake, name):
    return [p for m, p in fake.requests if m == name]


async def test_startup_syncs_account_over_websocket(service, fake):
    acct = service.core.account
    assert acct.hedge_mode is True                      # detectado por account.position (WS)
    assert acct.leverage["BTCUSDT"] == 20               # caché de leverage sin REST
    assert acct.usdt().available == pytest.approx(9000)
    assert len(service.core.exinfo) == 4
    assert service.core.market.mark("GRIDUSDT") == 100.0
    # REST solo para la lectura inicial de órdenes abiertas.
    await wait_for(lambda: "openAlgoOrders" in fake.rest_calls)
    assert set(fake.rest_calls) <= {"openOrders", "openAlgoOrders"}


async def test_signal_open_and_close_roundtrip(service, fake):
    status, body = service.trades.handle_signal(
        {"action": "open", "trade_id": 7, "symbol": "ETHUSDT", "direction": "LONG", "price": 3000, "quantity": 0.01})
    assert status == 200 and body["ok"]
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "LONG") is not None)
    trade = service.trades.get_trade("ETHUSDT", "LONG")
    # 0.01 ETH = 30 USDT ≥ mínimo 20 (+2% colchón) → se envía tal cual
    assert trade.quantity == pytest.approx(0.01) and trade.entry_price == 3000.0
    assert fake.positions[("ETHUSDT", "LONG")]["amt"] == pytest.approx(0.01)
    assert fake.leverage["ETHUSDT"] == 5 and fake.rest_calls.count("leverage") == 1
    await wait_for(lambda: service.core.account.position("ETHUSDT", "LONG") is not None)

    fake.set_mark("ETHUSDT", 3100)
    await wait_for(lambda: service.core.market.mark("ETHUSDT") == 3100)
    service.trades.handle_signal({"action": "close", "trade_id": 7, "symbol": "ETHUSDT", "direction": "LONG", "reason": "TP"})
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "LONG") is None)
    closed = service.trades.closed[-1]
    assert closed.status == "TP" and closed.close_price == 3100
    await wait_for(lambda: closed.realized_exchange != 0)
    assert closed.pnl_usdt == pytest.approx(1.0 - closed.fees_usdt, abs=1e-6)
    assert fake.positions[("ETHUSDT", "LONG")]["amt"] == 0


async def test_doctor_fixes_precision_on_unknown_symbol(service, fake):
    # NEWUSDT no está en el snapshot: la heurística usa paso 0.1, Binance exige enteros.
    fake.true_steps["NEWUSDT"] = __import__("decimal").Decimal("1")
    trade = await service.trades.open_trade("NEWUSDT", "SHORT", notional=10)
    assert trade is not None and trade.quantity == 4  # 10/3 → 3.4 rechazado (-1111) → paso 1 → 4
    errors = service.core.journal.entries()
    assert errors[0]["code"] == -1111 and errors[0]["fixed"]
    assert service.core.exinfo.get("NEWUSDT").step_size == 1  # aprendido y persistido


async def test_doctor_raises_notional_and_flips_position_side(service, fake):
    fake.inject.append(("order.place", -4164, "Order's notional must be no smaller than 25 (unless you choose reduce only)."))
    trade = await service.trades.open_trade("ETHUSDT", "LONG", notional=21)
    assert trade is not None and trade.quantity * 3000 >= 25
    assert service.core.exinfo.get("ETHUSDT").min_notional == 25
    # La cuenta pasa a One-way sin avisar: -4061 → se reenvía con BOTH.
    fake.hedge = False
    fake.positions.clear()
    trade2 = await service.trades.open_trade("BTCUSDT", "SHORT", notional=150)
    assert trade2 is not None and trade2.hedge_mode is False
    assert service.core.account.hedge_mode is False
    assert any(e["code"] == -4061 and e["fixed"] for e in service.core.journal.entries())


async def test_unknown_status_is_checked_before_resending(service, fake):
    before = len(methods(fake, "order.place"))
    fake.inject.append(("order.place", -1007, "Timeout waiting for response from backend server."))
    trade = await service.trades.open_trade("ETHUSDT", "LONG", notional=30)
    assert trade is not None
    assert methods(fake, "order.status"), "debe consultar order.status antes de reenviar"
    assert len(methods(fake, "order.place")) == before + 2


async def test_tp_by_algo_order_closes_trade_externally(service, fake):
    trade = await service.trades.open_trade("ETHUSDT", "LONG", notional=60)
    await wait_for(lambda: service.core.account.position("ETHUSDT", "LONG") is not None)
    await service.trades.set_protection("ETHUSDT", "LONG", "TP", 3300)
    await service.trades.set_protection("ETHUSDT", "LONG", "SL", 2800)
    placed = methods(fake, "algoOrder.place")
    assert placed[-1]["type"] == "STOP_MARKET" and "reduceOnly" not in placed[-1]
    await wait_for(lambda: len(service.core.account.algos) == 2)
    fake.set_mark("ETHUSDT", 3301)
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "LONG") is None)
    assert service.trades.closed[-1].status == "TP"
    assert trade.close_price == pytest.approx(3301)


async def test_neutral_grid_cycles(service, fake):
    cfg = {"symbol": "GRIDUSDT", "lower": 90, "upper": 110, "grids": 4, "investment": 100, "leverage": 5, "mode": "NEUTRAL"}
    preview = service.grids.preview(cfg)
    assert preview["levels"] == ["90", "95", "100", "105", "110"] and not preview["errors"]
    bot = await service.grids.create(cfg)
    await wait_for(lambda: bot.status == "RUNNING" and len(fake.orders) == 4)
    assert float(bot.qty_per_grid) == pytest.approx(1.2)  # 500 USDT / (4 × 100) = 1.25 → paso 0.1
    sides = sorted((o["side"], o["price"]) for o in fake.orders.values())
    assert sides == [("BUY", "90"), ("BUY", "95"), ("SELL", "105"), ("SELL", "110")]

    fake.set_mark("GRIDUSDT", 94.9)   # llena la compra en 95 → coloca venta en 100
    await wait_for(lambda: any(o["side"] == "SELL" and o["price"] == "100" for o in fake.orders.values()))
    fake.set_mark("GRIDUSDT", 100.2)  # llena la venta en 100 → +5 × qty de ganancia
    await wait_for(lambda: bot.matched == 1)
    assert bot.grid_profit == pytest.approx(5 * float(bot.qty_per_grid))
    await wait_for(lambda: len(fake.orders) == 4)

    await service.grids.stop(bot.id, close_position=True)
    assert bot.status == "STOPPED" and not fake.orders
    assert service.grids.list()[0]["matched"] == 1


async def test_long_grid_buys_initial_inventory_and_blocks_signals(service, fake):
    bot = await service.grids.create({"symbol": "GRIDUSDT", "lower": 90, "upper": 110, "grids": 4, "investment": 100,
                                      "leverage": 5, "mode": "LONG"})
    await wait_for(lambda: bot.status == "RUNNING")
    pos = fake.positions[("GRIDUSDT", "LONG")]
    assert pos["amt"] == pytest.approx(2 * float(bot.qty_per_grid))  # 2 celdas por encima del precio
    status, body = service.trades.handle_signal({"action": "open", "symbol": "GRIDUSDT", "direction": "LONG", "quantity": 1})
    assert body["ok"] is False and "Grid" in body["error"]
    await service.grids.stop(bot.id, close_position=True)
    assert fake.positions[("GRIDUSDT", "LONG")]["amt"] == 0


async def test_grid_reconciles_after_restart(fake, tmp_path, snapshot_file):
    svc = ExecutorService(make_settings(fake, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.core.market.marks and svc.user.conn.connected)
    bot = await svc.grids.create({"symbol": "GRIDUSDT", "lower": 90, "upper": 110, "grids": 4, "investment": 100,
                                  "leverage": 5, "mode": "NEUTRAL"})
    await wait_for(lambda: bot.status == "RUNNING" and len(fake.orders) == 4)
    await svc.stop()
    fake.set_mark("GRIDUSDT", 94.9)  # se llena mientras el executor está apagado
    assert len(fake.orders) == 3

    svc2 = ExecutorService(make_settings(fake, tmp_path))
    await svc2.start()
    await wait_for(lambda: svc2.user.conn.connected)
    bot2 = svc2.grids.bots[bot.id]
    await wait_for(lambda: len(fake.orders) == 4)
    assert any(c.state == "HOLD" for c in bot2.cells)
    await svc2.grids.stop(bot.id, close_position=True)
    await svc2.stop()


async def test_http_and_dashboard_websocket(service, fake):
    app = build_app(service)
    async with TestClient(TestServer(app)) as client:
        r = await client.post("/signal", json={"action": "open"}, headers={"X-Signal-Secret": "nope"})
        assert r.status == 401
        r = await client.post("/signal", json={"action": "open", "symbol": "ETHUSDT", "direction": "LONG"},
                              headers={"X-Signal-Secret": "sig"})
        body = await r.json()
        assert r.status == 400 and "tamaño" in body["error"]
        state = await (await client.get("/api/state")).json()
        for key in ("balance", "equity", "realized_pnl", "unrealized_pnl", "open_count", "executor_status"):
            assert key in state
        health = await (await client.get("/health")).json()
        assert health["ok"] is True

        ws = await client.ws_connect("/ws")
        await ws.send_json({"op": "auth", "token": "bad"})
        assert (await ws.receive_json())["ok"] is False
        await ws.send_json({"op": "auth", "token": "dash"})
        assert (await ws.receive_json())["ok"] is True
        kinds = set()
        while {"state", "markets", "errors"} - kinds:
            kinds.add((await ws.receive_json(timeout=5))["type"])
        await ws.send_json({"op": "cmd", "id": 1, "cmd": "explain_error", "args": {"code": "-2019"}})
        while True:
            msg = await ws.receive_json(timeout=5)
            if msg["type"] == "reply":
                break
        assert msg["ok"] and msg["data"]["code"] == -2019
        await ws.send_json({"op": "cmd", "id": 2, "cmd": "open",
                            "args": {"symbol": "ETHUSDT", "direction": "SHORT", "amount": 40, "size_mode": "notional"}})
        while True:
            msg = await ws.receive_json(timeout=5)
            if msg["type"] == "reply" and msg["id"] == 2:
                break
        assert msg["ok"], msg
        await ws.close()

        sig = await client.ws_connect("/ws/signal", headers={"X-Signal-Secret": "sig"})
        await sig.send_json({"id": 9, "action": "close", "symbol": "ETHUSDT", "direction": "SHORT"})
        reply = await sig.receive_json(timeout=5)
        assert reply["ok"] and reply["id"] == 9
        await sig.close()
        await wait_for(lambda: service.trades.get_trade("ETHUSDT", "SHORT") is None)


async def test_exchange_info_bootstrap(fake, tmp_path):
    svc = ExecutorService(make_settings(fake, tmp_path, exchange_info_bootstrap=True))
    await svc.start()
    await wait_for(lambda: len(svc.core.exinfo) == 4)
    assert svc.core.exinfo.get("BTCUSDT").max_leverage == 50
    assert (tmp_path / "data" / "exchange_info.json").exists()
    await svc.stop()


async def test_manual_limit_entry_creates_trade_on_fill(service, fake):
    res = await service.execute("open", {"symbol": "ETHUSDT", "direction": "LONG", "amount": 60, "size_mode": "notional",
                                         "order_type": "LIMIT", "price": 2900, "leverage": 10})
    assert res["price"] == "2900" and len(fake.orders) == 1
    await wait_for(lambda: len(service.core.account.orders) == 1)
    assert service.trades.get_trade("ETHUSDT", "LONG") is None
    fake.set_mark("ETHUSDT", 2899)
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "LONG") is not None)
    trade = service.trades.get_trade("ETHUSDT", "LONG")
    assert trade.source == "manual" and trade.entry_price == 2900 and trade.leverage == 10


async def test_trades_persist_and_reconcile_after_restart(fake, tmp_path, snapshot_file):
    svc = ExecutorService(make_settings(fake, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.core.market.marks and svc.user.conn.connected)
    await svc.trades.open_trade("ETHUSDT", "LONG", notional=40, paper_trade_id=11)
    await svc.trades.open_trade("BTCUSDT", "SHORT", notional=150, paper_trade_id=12)
    await svc.stop()
    # Mientras está apagado, el BTC se cierra en Binance (p. ej. desde la app).
    fake.positions[("BTCUSDT", "SHORT")] = {"amt": 0.0, "entry": 0.0}

    svc2 = ExecutorService(make_settings(fake, tmp_path))
    await svc2.start()
    assert svc2.trades.find_by_paper_id(11) is not None
    assert svc2.trades.find_by_paper_id(12) is None
    assert svc2.trades.closed[-1].symbol == "BTCUSDT" and svc2.trades.closed[-1].status == "EXTERNAL"
    await svc2.stop()


async def test_close_all_spares_grid_positions(service, fake):
    await service.trades.open_trade("ETHUSDT", "SHORT", notional=40)
    bot = await service.grids.create({"symbol": "GRIDUSDT", "lower": 90, "upper": 110, "grids": 4, "investment": 100,
                                      "leverage": 5, "mode": "LONG"})
    await wait_for(lambda: bot.status == "RUNNING")
    closed = await service.trades.close_all("CLOSE_ALL")
    assert len(closed) == 1 and fake.positions[("ETHUSDT", "SHORT")]["amt"] == 0
    assert fake.positions[("GRIDUSDT", "LONG")]["amt"] > 0
    await service.grids.stop(bot.id, close_position=True)


async def test_http_command_api(service, fake):
    app = build_app(service)
    async with TestClient(TestServer(app)) as client:
        r = await client.post("/api/command", json={"cmd": "explain_error", "args": {"code": -1111}})
        assert r.status == 401
        r = await client.post("/api/command", json={"cmd": "grid_preview", "args": {"symbol": "GRIDUSDT", "lower": 90,
                              "upper": 110, "grids": 4, "investment": 1, "leverage": 1}},
                              headers={"X-Dashboard-Token": "dash"})
        body = await r.json()
        assert r.status == 200 and body["data"]["errors"]  # inversión insuficiente explicada
        r = await client.post("/api/command", json={"cmd": "nope"}, headers={"X-Dashboard-Token": "dash"})
        assert r.status == 400 and "desconocido" in (await r.json())["error"]


async def test_dashboard_opens_with_just_the_link(fake, tmp_path, snapshot_file):
    svc = ExecutorService(make_settings(fake, tmp_path, dashboard_token=""))
    await svc.start()
    app = build_app(svc)
    async with TestClient(TestServer(app)) as client:
        ws = await client.ws_connect("/ws")
        await ws.send_json({"op": "auth", "token": ""})
        reply = await ws.receive_json(timeout=5)
        assert reply["ok"] is True and reply["required"] is False
        await ws.close()
        r = await client.post("/api/command", json={"cmd": "explain_error", "args": {"code": -1111}})
        assert r.status == 200
        # /signal sigue protegido por SIGNAL_SECRET (app.py ya lo envía).
        r = await client.post("/signal", json={"action": "close_all"})
        assert r.status == 401
    await svc.stop()
