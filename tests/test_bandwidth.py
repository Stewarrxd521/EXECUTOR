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


# ── Regresiones de la revisión adversarial ─────────────────────────────────
async def test_live_with_many_symbols_sends_few_subscribe_frames(fake, tmp_path, snapshot_file):
    for i in range(20):
        fake.marks[f"Q{i:02d}USDT"] = 1.0 + i
    svc = await _started(fake, tmp_path)
    try:
        before = len(fake.market_requests)
        res = await svc.execute("live", {"symbols": ",".join(f"Q{i:02d}USDT" for i in range(20))})
        frames = fake.market_requests[before:]
        assert len(frames) <= 2, frames                     # un único SUBSCRIBE coalescido
        assert all(v["mark"] > 0 for v in res["prices"].values())
    finally:
        await svc.stop()


async def test_junk_symbols_are_never_subscribed(fake, tmp_path, snapshot_file):
    svc = await _started(fake, tmp_path)
    try:
        await svc.execute("live", {"symbols": "ETH#X,A&B,NOPEUSDT"})
        svc.trades.handle_signal({"action": "open", "trade_id": 1, "symbol": "ETHUSDT", "direction": "SHORT",
                                  "price": 3000, "quantity": 0.01})
        await wait_for(lambda: "ethusdt@markPrice" in fake.market_subs)
        assert not any("#" in s or "&" in s or "nopeusdt" in s for s in fake.market_subs)
        await svc.core.market.conn.reconnect()  # la URL de reconexión sigue siendo válida
        await wait_for(lambda: fake.market_connections >= 2 and "ethusdt@markPrice" in fake.market_subs)
    finally:
        await svc.stop()


async def test_sixteen_positions_stay_on_per_symbol_streams(fake, tmp_path, snapshot_file):
    m = MarketData("ws://x", clock=None)
    m.want([f"S{i}USDT" for i in range(16)])
    assert "!markPrice@arr" not in m._desired()


async def test_fallback_mode_snapshot_does_not_reconnect(fake, tmp_path, snapshot_file):
    fake.reject_subscribe = True
    svc = await _started(fake, tmp_path)
    try:
        svc.note_watch("ETHUSDT")
        await wait_for(lambda: svc.core.market.stats()["dynamic_subscribe"] is False
                       and "ethusdt@markPrice@1s" in fake.market_subs, timeout=10)
        conns = fake.market_connections
        rows = await svc.execute("markets", {})
        assert len(rows) == len(fake.marks)
        await asyncio.sleep(1.5)
        assert fake.market_connections == conns + 1  # solo la conexión de un uso para la foto
    finally:
        await svc.stop()


async def test_snapshot_takes_several_arrays_then_unsubscribes(fake, tmp_path, snapshot_file):
    svc = await _started(fake, tmp_path)
    try:
        rows = await svc.execute("markets", {})
        assert len(rows) == len(fake.marks)
        await wait_for(lambda: "!miniTicker@arr" not in fake.market_subs)
    finally:
        await svc.stop()


async def test_order_retries_ignore_stale_snapshot_prices(fake, tmp_path, snapshot_file):
    svc = await _started(fake, tmp_path)
    try:
        await svc.execute("markets", {})               # foto: last de todos los símbolos
        assert svc.core.market.last("TESTUSDT") > 0
        assert svc.orders.mark("TESTUSDT") == 0.0      # sin mark reciente: nunca la foto
        assert await svc.orders.fresh_price("TESTUSDT") == 0.5
    finally:
        await svc.stop()


async def test_watchdog_reconnects_a_silent_stream(fake, tmp_path, snapshot_file, monkeypatch):
    svc = await _started(fake, tmp_path)
    try:
        svc.note_watch("ETHUSDT")
        await wait_for(lambda: "ethusdt@markPrice@1s" in fake.market_subs)
        market = svc.core.market
        monkeypatch.setattr(MarketData, "WATCHDOG_S", 0.0)
        market._last_price_ts = market.conn.connected_since = 0.0
        conns = fake.market_connections
        await market.sync_subscriptions()
        await wait_for(lambda: fake.market_connections > conns)
    finally:
        await svc.stop()


async def test_ensure_price_keeps_polled_symbols_watched(fake, tmp_path, snapshot_file):
    svc = await _started(fake, tmp_path)
    try:
        await svc.ensure_price("ETHUSDT")
        await wait_for(lambda: svc.core.market.mark("ETHUSDT", max_age_s=5) > 0)
        svc.watch["ETHUSDT"] = 0.0                     # a punto de expirar
        await svc.ensure_price("ETHUSDT")              # precio fresco: igual renueva el interés
        assert svc.watch["ETHUSDT"] > 0
    finally:
        await svc.stop()


def test_note_watch_accepts_symbols_reported_by_binance(tmp_path):
    from executor.streams import MiniTicker
    from tests.conftest import make_settings
    from tests.fake_binance import FakeBinance
    svc = ExecutorService(make_settings(FakeBinance(), tmp_path))
    svc.note_watch("NEWLISTUSDT")
    assert "NEWLISTUSDT" not in svc.watch               # ni en exinfo ni reportado: no
    svc.core.market.tickers["NEWLISTUSDT"] = MiniTicker(1, 1, 1, 1, 1, 1, 0)
    svc.note_watch("NEWLISTUSDT")
    assert "NEWLISTUSDT" in svc.watch


# ── Regresiones de la verificación final ───────────────────────────────────
async def test_first_price_stream_after_idle_does_not_trip_watchdog(fake, tmp_path, snapshot_file):
    svc = await _started(fake, tmp_path)
    try:
        market = svc.core.market
        assert market._active == {"!contractInfo"}
        market._last_price_ts = market.conn.connected_since = 0.0   # mucho rato solo con !contractInfo
        reconnects = []

        async def reconnect():
            reconnects.append(1)

        market.conn.reconnect = reconnect
        market.want(["QUIETUSDT"])                                   # su primer precio aún no llega (cada 3 s)
        await market.sync_subscriptions()                            # SUBSCRIBE quietusdt@markPrice
        await market.sync_subscriptions()
        assert "quietusdt@markPrice" in market._active
        assert not reconnects                                        # sin reconexión espuria
    finally:
        await svc.stop()


def test_watched_symbols_are_capped_but_positions_never():
    m = MarketData("ws://x", clock=None)
    now = __import__("time").time()
    m.want([f"P{i}USDT" for i in range(50)], fast_symbols=["GRIDUSDT"],
           ticker_symbols={f"W{i}USDT": now - i for i in range(100)})
    desired = m._desired()
    tickers = {d for d in desired if d.endswith("@miniTicker")}
    assert tickers == {f"w{i}usdt@miniTicker" for i in range(MarketData.MAX_WATCH)}   # los más recientes
    assert {f"p{i}usdt@markPrice" for i in range(50)} <= desired and "gridusdt@markPrice@1s" in desired
    assert len(desired) < 1024


def test_hundreds_of_grids_use_the_all_market_one_second_stream():
    m = MarketData("ws://x", clock=None)
    m.want(["POSUSDT"], fast_symbols=[f"G{i}USDT" for i in range(MarketData.MANY_SYMBOLS + 1)])
    desired = m._desired()
    assert "!markPrice@arr@1s" in desired and not any("@markPrice" in d for d in desired)


async def test_public_live_flood_cannot_grow_the_market_connection(fake, tmp_path, snapshot_file):
    for i in range(200):
        fake.marks[f"F{i:03d}USDT"] = 1.0 + i
    svc = await _started(fake, tmp_path)
    try:
        for n in range(20):
            await svc.execute("live", {"symbols": ",".join(f"F{i:03d}USDT" for i in range(n * 10, n * 10 + 10))})
        cap = 2 * MarketData.MAX_WATCH + 2
        assert len(svc.core.market._desired()) <= cap
        await wait_for(lambda: "f199usdt@miniTicker" in fake.market_subs)  # lo último consultado sí
        await asyncio.sleep(1.5)
        assert len(fake.market_subs) <= cap and "f000usdt@miniTicker" not in fake.market_subs
    finally:
        await svc.stop()


async def test_live_view_ignores_an_old_markets_snapshot(fake, tmp_path, snapshot_file):
    svc = await _started(fake, tmp_path)
    try:
        market = svc.core.market
        await svc.execute("markets", {})
        await wait_for(lambda: "!miniTicker@arr" not in market._active)
        market.seed("ETHUSDT", 2500.0)
        market.tickers["ETHUSDT"].ts -= 10                           # foto de hace 10 s, sin stream propio
        row = svc.live_view(["ETHUSDT"])["prices"]["ETHUSDT"]
        assert row["last"] == 2500.0 and row["change_pct"] is None
        svc.note_watch("ETHUSDT")                                    # mirado: miniTicker en vivo
        await wait_for(lambda: "ethusdt@miniTicker" in market._active)
        await wait_for(lambda: svc.live_view(["ETHUSDT"])["prices"]["ETHUSDT"]["change_pct"] is not None)
    finally:
        await svc.stop()


async def test_unknown_symbols_are_validated_once_and_in_parallel(fake, tmp_path, snapshot_file, monkeypatch):
    svc = await _started(fake, tmp_path)
    try:
        calls = []
        real = svc.core.ws.ticker_price

        async def slow_ticker(sym):
            calls.append(sym)
            await asyncio.sleep(0.1)
            return await real(sym)

        monkeypatch.setattr(svc.core.ws, "ticker_price", slow_ticker)
        t0 = asyncio.get_running_loop().time()
        res = await svc.ensure_prices([f"NOPE{i}USDT" for i in range(30)], wait_s=1.5)
        took = asyncio.get_running_loop().time() - t0
        assert len(calls) == ExecutorService.MAX_UNKNOWN              # una vez cada uno, con tope
        assert took < 1.0, took                                      # sin esperar streams que no existen
        assert set(res) == {f"NOPE{i}USDT" for i in range(30)} and not any(res.values())
        assert not any("nope" in s for s in fake.market_subs)
    finally:
        await svc.stop()
