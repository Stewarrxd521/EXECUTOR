"""Multiplicador de operaciones: niveles automáticos con histéresis, manual, API y señales."""

import asyncio

import pytest
from aiohttp.test_utils import TestClient, TestServer

import executor_client
from executor.multiplier import Multiplier, auto_level, level_band
from executor.service import ExecutorService
from executor.web.server import build_app
from tests.compat import bridge_app25
from tests.conftest import SECRET, legacy_settings, wait_for


def walk(balances, step=100, max_level=10, start=0):
    level, out = start, []
    for b in balances:
        level = auto_level(level, b, step, max_level)
        out.append(level)
    return out


def test_auto_levels_follow_the_requested_rules():
    # 100 → x1 · 200 → x2 · se mantiene x2 de 101 a 299 · 300 → x3 · se mantiene x3 de 201 a 399
    assert walk([100]) == [1]
    assert walk([200]) == [2]
    assert walk([200, 101, 150, 299, 250]) == [2, 2, 2, 2, 2]
    assert walk([200, 100]) == [2, 1]
    assert walk([200, 300]) == [2, 3]
    assert walk([300, 201, 399, 220]) == [3, 3, 3, 3]
    assert walk([300, 200]) == [3, 2]
    assert walk([300, 400]) == [3, 4]
    assert walk([199.99]) == [1] and walk([50]) == [1] and walk([0]) == [1]
    # Saltos grandes: sube/baja varios niveles de una vez, respetando la histéresis.
    assert walk([100, 750]) == [1, 7]
    assert walk([750, 150]) == [7, 2]
    assert walk([5000]) == [10]  # tope MULTIPLIER_MAX
    band = level_band(2, 100, 10)
    assert band["up_at"] == 300 and band["down_at"] == 100


class _FakeSettings:
    def __init__(self, tmp_path, **over):
        self.data_dir = tmp_path
        self.multiplier_mode = "off"
        self.multiplier_factor = 1.0
        self.multiplier_step_usdt = 100.0
        self.multiplier_max = 10.0
        self.multiplier_source = "wallet"
        self.__dict__.update(over)


def test_multiplier_configure_and_persist(tmp_path):
    balance = {"wallet": 250.0}
    events = []
    m = Multiplier(_FakeSettings(tmp_path), lambda: balance, lambda msg, lvl: events.append(msg))
    assert m.effective() == 1.0
    m.configure({"enabled": True, "mode": "manual", "factor": "x2"})
    assert m.effective() == 2.0
    with pytest.raises(ValueError):
        m.configure({"factor": 50})  # supera max_factor (10)
    m.configure({"mode": "auto"})
    assert m.effective() == 2.0 and m.state.level == 2  # 250 → x2
    balance["wallet"] = 320
    assert m.effective() == 3.0 and events and "x2 ▲ x3" in events[-1]
    balance["wallet"] = 205
    assert m.effective() == 3.0  # se mantiene x3 de 201 a 399
    m.store.save_now()
    # Reinicio: se conserva el nivel (la histéresis no se pierde).
    m2 = Multiplier(_FakeSettings(tmp_path), lambda: balance)
    m2.load()
    assert m2.state.mode == "auto" and m2.effective() == 3.0
    # Si cambian las variables MULTIPLIER_* del entorno, mandan ellas.
    m3 = Multiplier(_FakeSettings(tmp_path, multiplier_mode="manual", multiplier_factor=4), lambda: balance)
    m3.load()
    assert m3.state.mode == "manual" and m3.effective() == 4.0
    m.configure({"mode": "off"})
    assert m.effective() == 1.0


async def test_manual_multiplier_scales_bot_signals(open_service, live_url, fake):
    svc = open_service
    svc.multiplier.configure({"enabled": True, "mode": "manual", "factor": 2})
    bridge = bridge_app25.ExecutorBridge(executor_url=live_url, signal_secret=SECRET, logger=lambda m: None)
    bridge.notify_open(trade_id=71, symbol="ETHUSDT", direction="SHORT", price=3000, quantity=0.01,
                       notional=30, level=50)
    await wait_for(lambda: svc.trades.get_trade("ETHUSDT", "SHORT") is not None)
    trade = svc.trades.get_trade("ETHUSDT", "SHORT")
    assert trade.quantity == pytest.approx(0.02) and trade.bot_quantity == pytest.approx(0.01)
    assert trade.multiplier == 2.0 and "multiplicador x2" in svc.trades.signals(1)[0]["detail"]
    # Cierre parcial del bot (0.005) → se cierran 0.01 reales.
    svc.trades.handle_signal({"action": "close", "trade_id": 71, "symbol": "ETHUSDT", "direction": "SHORT",
                              "quantity": 0.005, "reason": "PARTIAL"})
    await wait_for(lambda: fake.positions[("ETHUSDT", "SHORT")]["amt"] == pytest.approx(-0.01))
    bridge.notify_close(trade_id=71, symbol="ETHUSDT", direction="SHORT", reason="TP", close_price=2990, pnl=0.05)
    await wait_for(lambda: svc.trades.get_trade("ETHUSDT", "SHORT") is None)
    closed = svc.trades.closed[-1].to_dict()
    assert closed["bot_pnl_scaled"] == pytest.approx(0.1)
    # Las órdenes manuales del dashboard no se multiplican.
    r = await svc.execute("open", {"symbol": "ETHUSDT", "direction": "LONG", "amount": 30})
    assert r["trade"]["quantity"] == pytest.approx(0.01)


async def test_auto_multiplier_follows_wallet_balance(fake, tmp_path, snapshot_file):
    svc = ExecutorService(legacy_settings(fake, tmp_path))  # wallet simulada: 10 000 USDT
    await svc.start()
    try:
        await wait_for(lambda: svc.trades.ready.is_set() and svc.core.account.usdt().wallet > 0)
        svc.multiplier.configure({"enabled": True, "mode": "auto", "step_usdt": 5000})
        assert svc.multiplier.effective() == 2.0  # 10 000 / 5 000
        svc.trades.handle_signal({"action": "open", "trade_id": 72, "symbol": "ETHUSDT", "direction": "SHORT",
                                  "price": 3000, "quantity": 0.01})
        await wait_for(lambda: svc.trades.get_trade("ETHUSDT", "SHORT") is not None)
        assert svc.trades.get_trade("ETHUSDT", "SHORT").quantity == pytest.approx(0.02)
        events = []
        svc.core.bus.subscribe(lambda e: events.append(e))
        for wallet, expected in ((14_999, 2), (15_000, 3), (10_001, 3), (10_000, 2), (5_000, 1)):
            fake.wallet = wallet
            await svc.refresh_balance(force=True)
            assert svc.multiplier.effective() == expected, wallet
        assert any(e["kind"] == "multiplier" and "x2 ▲ x3" in e["text"] for e in events)
        assert svc.api_state()["multiplier"] == 1.0
    finally:
        await svc.stop()


async def test_multiplier_api_and_client(open_service, live_url):
    async with TestClient(TestServer(build_app(open_service))) as client:
        r = await client.get("/api/multiplier")
        body = await r.json()
        assert r.status == 200 and body["data"]["enabled"] is False and body["data"]["effective"] == 1.0
        assert (await client.post("/api/multiplier", json={"enabled": True})).status == 401
        r = await client.post("/api/multiplier", json={"factor": 0}, headers={"X-Signal-Secret": SECRET})
        assert r.status == 400
        r = await client.post("/api/multiplier", json={"enabled": True, "mode": "manual", "factor": 3},
                              headers={"X-Signal-Secret": SECRET})
        assert (await r.json())["effective"] == 3.0
    c = executor_client.ExecutorClient(live_url, secret=SECRET)
    view = await asyncio.to_thread(c.set_multiplier, True, "auto", None, 100)
    assert view["mode"] == "auto" and view["auto"]["level"] == 10  # 10 000 USDT, tope x10
    assert (await asyncio.to_thread(c.set_multiplier, False))["effective"] == 1.0
    rc = await asyncio.to_thread(executor_client.main, ["--url", live_url, "--secret", SECRET, "multiplier", "on",
                                                        "--mode", "manual", "--factor", "2"])
    assert rc == 0 and open_service.multiplier.effective() == 2.0
    snap = open_service.snapshot()
    assert snap["multiplier"]["effective"] == 2.0


def test_settings_from_env(monkeypatch):
    from executor.config import load_settings
    monkeypatch.setenv("MULTIPLIER_MODE", "auto")
    monkeypatch.setenv("MULTIPLIER_STEP_USDT", "250")
    s = load_settings()
    assert s.multiplier_mode == "auto" and s.multiplier_step_usdt == 250


# ── Regresiones de la revisión adversarial ─────────────────────────────────
async def test_auto_level_updates_without_dashboard(fake, tmp_path, snapshot_file):
    svc = ExecutorService(legacy_settings(fake, tmp_path))
    await svc.start()
    try:
        await wait_for(lambda: svc.trades.ready.is_set() and svc.core.account.usdt().wallet > 0)
        svc.multiplier.configure({"enabled": True, "mode": "auto", "step_usdt": 5000})
        events = []
        svc.core.bus.subscribe(lambda e: events.append(e))
        for wallet in (15_000, 12_000):  # sin consultar el multiplicador entre medias
            fake.wallet = wallet
            await svc.refresh_balance(force=True)
        assert svc.multiplier.state.level == 3
        assert any(e["kind"] == "multiplier" and "x2 ▲ x3" in e["text"] for e in events)
    finally:
        await svc.stop()


async def test_partial_close_with_x3_is_rounded_to_step(open_service, fake):
    svc, t = open_service, open_service.trades
    svc.multiplier.configure({"enabled": True, "mode": "manual", "factor": 3})
    t.handle_signal({"action": "open", "trade_id": 81, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
                     "quantity": 0.01})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None)
    step_before = svc.core.exinfo.get("ETHUSDT").step_size
    t.handle_signal({"action": "close", "trade_id": 81, "symbol": "ETHUSDT", "direction": "SHORT",
                     "quantity": 0.003, "reason": "PARTIAL"})
    await wait_for(lambda: fake.positions[("ETHUSDT", "SHORT")]["amt"] == pytest.approx(-0.021))
    assert svc.core.exinfo.get("ETHUSDT").step_size == step_before  # no «aprende» un step falso
    assert not svc.core.journal.entries()
    trade = t.get_trade("ETHUSDT", "SHORT")
    assert trade.bot_quantity == pytest.approx(0.007) and trade.multiplier == 3.0


async def test_sizeless_open_uses_default_notional_times_factor(fake, tmp_path, snapshot_file):
    svc = ExecutorService(legacy_settings(fake, tmp_path, default_notional_usdt=30))
    await svc.start()
    try:
        await wait_for(lambda: svc.trades.ready.is_set())
        svc.multiplier.configure({"enabled": True, "mode": "manual", "factor": 2})
        svc.trades.handle_signal({"action": "open", "trade_id": 82, "symbol": "ETHUSDT", "direction": "SHORT",
                                  "price": 3000})
        await wait_for(lambda: svc.trades.get_trade("ETHUSDT", "SHORT") is not None)
        assert svc.trades.get_trade("ETHUSDT", "SHORT").quantity == pytest.approx(0.02)
    finally:
        await svc.stop()


async def test_unknown_open_keeps_multiplier(open_service, fake, monkeypatch):
    import executor.trading as trading_mod
    monkeypatch.setattr(trading_mod, "UNKNOWN_OPEN_CHECKS", (0.3, 0.6))
    t = open_service.trades
    open_service.multiplier.configure({"enabled": True, "mode": "manual", "factor": 2})
    fake.inject_after.append(("order.place", -1007, "Timeout waiting for response from backend server."))
    for _ in range(3):
        fake.inject.append(("order.status", -1001, "Internal error; unable to process your request."))
    t.handle_signal({"action": "open", "trade_id": 83, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
                     "quantity": 0.01})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None, timeout=8)
    trade = t.get_trade("ETHUSDT", "SHORT")
    assert trade.quantity == pytest.approx(0.02) and trade.multiplier == 2.0
    assert trade.bot_quantity == pytest.approx(0.01)


def test_old_trades_and_adopted_positions_count_as_x1():
    from executor.trading import Trade
    old = Trade.from_dict({"id": 1, "symbol": "ETHUSDT", "direction": "SHORT", "entry_price": 3000,
                           "quantity": 0.05, "open_time": "", "leverage": 5})
    assert old.bot_quantity == pytest.approx(0.05) and old.multiplier == 1.0
    old.note_tranche_scale(2.0, 0.01, 0.02)  # tramo nuevo con x2
    assert old.multiplier == pytest.approx((0.05 * 1 + 0.01 * 2) / 0.06)


async def test_close_all_does_not_close_opens_received_after_it(open_service, fake):
    t = open_service.trades
    t.handle_signal({"action": "open", "trade_id": 84, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
                     "quantity": 0.01})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None)
    t.handle_signal({"action": "close_all"})
    t.handle_signal({"action": "open", "trade_id": 85, "symbol": "BTCUSDT", "direction": "LONG", "price": 65000,
                     "quantity": 0.002})
    await wait_for(lambda: t.get_trade("BTCUSDT", "LONG") is not None and t.get_trade("ETHUSDT", "SHORT") is None)
    await asyncio.sleep(0.3)
    assert t.get_trade("BTCUSDT", "LONG") is not None


def test_max_factor_caps_auto_level_without_balance(tmp_path):
    balance = {"wallet": 1000.0}
    m = Multiplier(_FakeSettings(tmp_path), lambda: balance)
    m.configure({"enabled": True, "mode": "auto"})
    assert m.effective() == 10.0
    balance["wallet"] = 0.0
    m.configure({"max_factor": 3})
    assert m.effective() == 3.0 and m.view()["effective"] == 3.0


def test_invalid_env_values_fall_back_to_defaults(tmp_path):
    m = Multiplier(_FakeSettings(tmp_path, multiplier_mode="auto", multiplier_step_usdt=0.0,
                                 multiplier_max=float("nan"), multiplier_factor=-2), lambda: {"wallet": 250})
    assert m.state.step_usdt == 100.0 and m.state.max_factor == 10.0 and m.state.factor == 1.0
    assert m.effective() == 2.0


async def test_ws_api_connect_failure_is_retryable_error():
    from executor.binance_api import BinanceWsApi, ServerClock
    from executor.errors import BinanceAPIError
    ws = BinanceWsApi("ws://127.0.0.1:9/ws-fapi/v1", "k", "s", ServerClock(), 5000)
    with pytest.raises(BinanceAPIError) as exc:
        await ws.request("ticker.price", {"symbol": "BTCUSDT"}, timeout=2)
    assert exc.value.code == -1
    await ws.close()


def test_known_zero_balance_drops_to_x1(tmp_path):
    summary = {"available": 800.0, "balance_sync": 0.0}
    m = Multiplier(_FakeSettings(tmp_path), lambda: summary)
    m.configure({"enabled": True, "mode": "auto", "source": "available"})
    assert m.effective() == 8.0
    summary["available"] = 0.0          # aún sin sincronizar: se conserva el nivel
    assert m.effective() == 8.0
    summary["balance_sync"] = 1.0       # saldo confirmado en 0 → x1
    assert m.effective() == 1.0
    summary["available"] = 250.0
    assert m.effective() == 2.0


def test_env_out_of_range_values_are_bounded(tmp_path):
    lo = Multiplier(_FakeSettings(tmp_path, multiplier_mode="auto", multiplier_max=0.5), lambda: {"wallet": 5000})
    assert lo.state.max_factor == 1.0 and lo.effective() == 1.0  # un tope bajo nunca se vuelve x10
    hi = Multiplier(_FakeSettings(tmp_path, multiplier_mode="manual", multiplier_factor=20), lambda: {})
    assert hi.effective() == 10.0


async def test_failed_balance_read_keeps_restored_level(fake, tmp_path, snapshot_file):
    svc = ExecutorService(legacy_settings(fake, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.trades.ready.is_set() and svc.core.account.usdt().wallet > 0)
    svc.multiplier.configure({"enabled": True, "mode": "auto", "step_usdt": 2000})
    assert svc.multiplier.effective() == 5.0
    await svc.stop()
    # Reinicio con la lectura de saldo fallando: se conserva x5 (no es un saldo 0 confirmado).
    for _ in range(9):
        fake.inject.append(("v2/account.balance", -1001, "Internal error; unable to process your request."))
    svc2 = ExecutorService(legacy_settings(fake, tmp_path))
    events = []
    svc2.core.bus.subscribe(lambda e: events.append(e))
    await svc2.start()
    try:
        await wait_for(lambda: svc2.trades.ready.is_set())
        assert svc2.core.account.usdt().wallet == 0.0
        assert svc2.multiplier.effective() == 5.0
        assert not [e for e in events if e["kind"] == "multiplier"]
    finally:
        await svc2.stop()


async def test_near_full_close_quantity_uses_real_position(open_service, fake):
    t = open_service.trades
    open_service.multiplier.configure({"enabled": True, "mode": "manual", "factor": 3})
    t.handle_signal({"action": "open", "trade_id": 86, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
                     "quantity": 0.0035})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None)
    step = open_service.core.exinfo.get("ETHUSDT").step_size
    t.handle_signal({"action": "close", "trade_id": 86, "symbol": "ETHUSDT", "direction": "SHORT",
                     "quantity": 0.0035, "reason": "TP"})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is None)
    assert fake.positions[("ETHUSDT", "SHORT")]["amt"] == 0
    assert open_service.core.exinfo.get("ETHUSDT").step_size == step


async def test_close_retries_through_short_ws_outage(open_service, fake, monkeypatch):
    from executor.errors import BinanceAPIError
    t = open_service.trades
    t.handle_signal({"action": "open", "trade_id": 87, "symbol": "ETHUSDT", "direction": "SHORT", "price": 3000,
                     "quantity": 0.01})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is not None)
    original = open_service.orders.market
    calls = {"n": 0}

    async def flaky_market(*a, **k):
        calls["n"] += 1
        if calls["n"] == 1:
            raise BinanceAPIError(-1, "ws-api: sin conexión: ConnectionRefusedError()", method="order.place")
        return await original(*a, **k)

    monkeypatch.setattr(open_service.orders, "market", flaky_market)
    t.handle_signal({"action": "close", "trade_id": 87, "symbol": "ETHUSDT", "direction": "SHORT", "reason": "SL"})
    await wait_for(lambda: t.get_trade("ETHUSDT", "SHORT") is None, timeout=12)
    assert fake.positions[("ETHUSDT", "SHORT")]["amt"] == 0 and calls["n"] >= 2


def test_external_reduction_shrinks_bot_quantity():
    from executor.trading import Trade
    tr = Trade(id=1, symbol="ETHUSDT", direction="SHORT", entry_price=3000, quantity=0.03, open_time="",
               leverage=5, bot_quantity=0.01, multiplier=3.0)
    tr.sync_quantity(0.015)  # se cerró la mitad desde la app de Binance
    assert tr.quantity == 0.015 and tr.bot_quantity == pytest.approx(0.005)
