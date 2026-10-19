"""Ancho de banda: modo de mercado «lite» (solo lo necesario) frente a «full» (todo el mercado)."""

import asyncio

import pytest
from aiohttp.test_utils import TestClient, TestServer

from executor.service import ExecutorService
from executor.streams import MarketData
from executor.web.server import build_app
from tests.conftest import legacy_settings, wait_for


async def _started(fake, tmp_path, **over):
    svc = ExecutorService(legacy_settings(fake, tmp_path, **over))
    await svc.start()
    await wait_for(lambda: svc.core.market.conn.connected and svc.trades.ready.is_set())
    return svc


async def test_lite_subscribes_only_what_is_needed(fake, tmp_path, snapshot_file, monkeypatch):
    monkeypatch.setattr(MarketData, "UNSUBSCRIBE_GRACE_S", 1.0)
    svc = await _started(fake, tmp_path)
    try:
        assert fake.market_subs == {"!contractInfo"}  # sin posiciones: nada del mercado
        await asyncio.sleep(1.2)
        assert not svc.core.market.marks
        # Una posición abierta → mark price de ESE símbolo cada 3 s.
        svc.trades.handle_signal({"action": "open", "trade_id": 1, "symbol": "ETHUSDT", "direction": "SHORT",
                                  "price": 3000, "quantity": 0.01})
        await wait_for(lambda: "ethusdt@markPrice" in fake.market_subs)
        await wait_for(lambda: svc.core.market.mark("ETHUSDT") > 0)
        assert not any(s.startswith("!markPrice") or s == "!miniTicker@arr" for s in fake.market_subs)
        # Lo que se mira en el dashboard: 1 s + estadísticas 24 h.
        svc.note_watch("BTCUSDT")
        await wait_for(lambda: {"btcusdt@markPrice@1s", "btcusdt@miniTicker"} <= fake.market_subs)
        # Al cerrar la posición y pasar el margen de gracia se desuscribe y se olvida el precio.
        svc.trades.handle_signal({"action": "close", "trade_id": 1, "symbol": "ETHUSDT", "direction": "SHORT"})
        await wait_for(lambda: "ethusdt@markPrice" not in fake.market_subs, timeout=8)
        assert svc.core.market.mark("ETHUSDT") == 0.0
        assert fake.market_connections == 1  # todo en caliente, sin reconectar
    finally:
        await svc.stop()


async def test_many_positions_use_one_all_market_stream(fake, tmp_path, snapshot_file, monkeypatch):
    monkeypatch.setattr(MarketData, "MANY_SYMBOLS", 1)
    svc = await _started(fake, tmp_path)
    try:
        for i, (sym, px, qty) in enumerate([("ETHUSDT", 3000, 0.01), ("BTCUSDT", 65000, 0.002)]):
            svc.trades.handle_signal({"action": "open", "trade_id": 10 + i, "symbol": sym, "direction": "SHORT",
                                      "price": px, "quantity": qty})
        await wait_for(lambda: "!markPrice@arr" in fake.market_subs)
        assert not any(s.endswith("@markPrice") for s in fake.market_subs)
    finally:
        await svc.stop()


async def test_grid_symbols_get_one_second_marks(fake, tmp_path, snapshot_file):
    svc = await _started(fake, tmp_path)
    try:
        await svc.execute("grid_create", {"symbol": "GRIDUSDT", "lower": 90, "upper": 110, "grids": 4,
                                          "investment": 200, "leverage": 2, "mode": "NEUTRAL"})
        await wait_for(lambda: "gridusdt@markPrice@1s" in fake.market_subs)
    finally:
        await svc.stop()


async def test_dashboard_markets_list_is_a_one_shot_snapshot(fake, tmp_path, snapshot_file):
    svc = await _started(fake, tmp_path)
    try:
        async with TestClient(TestServer(build_app(svc))) as client:
            ws = await client.ws_connect("/ws")
            await ws.send_json({"op": "auth", "token": ""})
            rows = None
            for _ in range(40):
                msg = await ws.receive_json(timeout=5)
                if msg.get("type") == "markets" and msg.get("rows"):
                    rows = msg["rows"]
                    break
            assert rows and len(rows) == len(fake.marks)
            assert any(r.get("method") == "SUBSCRIBE" and "!miniTicker@arr" in r["params"]
                       for r in fake.market_requests)
            # Recibida la foto, se deja de recibir el mercado entero.
            await wait_for(lambda: "!miniTicker@arr" not in fake.market_subs)
            await ws.close()
    finally:
        await svc.stop()


async def test_rejected_subscribe_falls_back_to_reconnect(fake, tmp_path, snapshot_file):
    fake.reject_subscribe = True
    svc = await _started(fake, tmp_path)
    try:
        svc.note_watch("ETHUSDT")
        await wait_for(lambda: fake.market_connections >= 2 and "ethusdt@markPrice@1s" in fake.market_subs,
                       timeout=10)
        assert svc.core.market.stats()["dynamic_subscribe"] is False
        await wait_for(lambda: svc.core.market.mark("ETHUSDT") > 0)
    finally:
        await svc.stop()


async def test_full_mode_keeps_previous_behaviour(fake, tmp_path, snapshot_file):
    svc = await _started(fake, tmp_path, market_streams="full")
    try:
        assert fake.market_subs == set(MarketData.FULL_STREAMS)
        await wait_for(lambda: len(svc.core.market.marks) == len(fake.marks))
    finally:
        await svc.stop()


async def test_lite_receives_far_less_than_full(fake, tmp_path, snapshot_file):
    # Mercado sintético grande (como Binance: cientos de símbolos).
    for i in range(300):
        fake.marks[f"X{i:03d}USDT"] = 1.0 + i / 100
    measured = {}
    for mode in ("full", "lite"):
        svc = await _started(fake, tmp_path / mode, market_streams=mode)
        try:
            svc.trades.handle_signal({"action": "open", "trade_id": 1, "symbol": "ETHUSDT", "direction": "SHORT",
                                      "price": 3000, "quantity": 0.01})
            await wait_for(lambda: svc.trades.get_trade("ETHUSDT", "SHORT") is not None)
            await asyncio.sleep(1.0)
            start = svc.core.market.conn.bytes_in
            await asyncio.sleep(2.0)
            measured[mode] = svc.core.market.conn.bytes_in - start
            await svc.execute("close_all", {})
        finally:
            await svc.stop()
    assert measured["lite"] * 50 < measured["full"], measured


async def test_network_view_and_light_keepalive(fake, tmp_path, snapshot_file):
    svc = await _started(fake, tmp_path)
    server = TestServer(build_app(svc))
    await server.start_server()
    try:
        svc.settings.keepalive_url = str(server.make_url("")).rstrip("/")
        svc.settings.keepalive_s = 1
        task = asyncio.create_task(svc._keepalive_loop())
        await wait_for(lambda: svc.keepalive["ok"] >= 1, timeout=5)
        task.cancel()
        assert svc.keepalive["last_status"] == 200
        net = svc.health()["network"]
        assert net["market_mode"] == "lite"
        si = net["service_initiated"]
        assert set(si["sources"]) == {"market", "user", "ws_api", "rest"}
        assert si["estimated_billable_mb_h"] >= 0 and "estimated_billable_gb_month" in si
    finally:
        await server.close()
        await svc.stop()


@pytest.mark.parametrize("count", [3, 20])
def test_desired_streams_rules(count):
    m = MarketData("ws://x", clock=None)
    syms = [f"S{i}USDT" for i in range(count)]
    m.want(syms, fast_symbols=["GRIDUSDT", "S0USDT"], ticker_symbols=["VIEWUSDT"])
    desired = m._desired()
    assert "!contractInfo" in desired and "gridusdt@markPrice@1s" in desired
    assert {"viewusdt@markPrice@1s", "viewusdt@miniTicker"} <= desired
    assert "s0usdt@markPrice" not in desired  # ya tiene el de 1 s
    if count > MarketData.MANY_SYMBOLS:
        assert "!markPrice@arr" in desired and not any(d.endswith("@markPrice") for d in desired)
    else:
        assert {f"s{i}usdt@markPrice" for i in range(1, count)} <= desired
