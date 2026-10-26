"""Reinicio sin trades.json (disco efímero de Render): los lotes se rehacen con el historial de Binance."""

import asyncio

import pytest

import executor.trading as trading_mod
from executor.service import ExecutorService
from executor.trading import lot_cid_prefix, parse_lot_cid, replay_lots
from tests.conftest import make_settings, wait_for
from tests.test_lots import SYM, amt, close, lots, open_, opened


@pytest.fixture(autouse=True)
def quick_retries(monkeypatch):
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0, 0.5, 1.0))


def outside(fake, side, qty, cid, direction="LONG"):
    fake._place({"symbol": SYM, "side": side, "type": "MARKET", "positionSide": direction, "quantity": str(qty),
                 "newClientOrderId": cid})


async def start(fake, tmp_path):
    svc = ExecutorService(make_settings(fake, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.user.conn.connected and svc.trades.ready.is_set())
    return svc


async def restart_without_state(svc, fake, tmp_path):
    """Deploy en Render sin disco: se pierde trades.json."""
    await svc.stop()
    (tmp_path / "data" / "trades.json").unlink()
    svc = await start(fake, tmp_path)
    await wait_for(lambda: not svc.trades._rebuilding)
    return svc


def test_lot_client_order_ids():
    assert lot_cid_prefix("X", 5) == "X5" and lot_cid_prefix("K", "kline:5") == "K5:kline"
    assert lot_cid_prefix("X", "averyverylongbotname:12345") == "X12345:averyverylon"
    assert len(lot_cid_prefix("X", "a" * 32 + ":123456789012")) <= trading_mod.CID_PREFIX_MAX
    assert parse_lot_cid("X5-18f2c0a1b2Ab3dE4") == ("X", 5)
    assert parse_lot_cid("K5:my-bot-18f2c0a1b2Ab3dE4") == ("K", "my-bot:5")
    assert parse_lot_cid("C12-18f2c0a1b2Ab3dE4") is None and parse_lot_cid("web_manual") is None


def test_replay_starts_at_the_last_flat_point():
    rows = [  # (cid, side, qty) en orden de Binance
        ("X1-a", "BUY", 0.02), ("C3-b", "SELL", 0.02),                      # posición anterior, ya cerrada
        ("X1-c", "BUY", 0.01), ("X2:kline-d", "BUY", 0.02), ("web_e", "BUY", 0.01),
        ("K2:kline-f", "SELL", 0.005), ("web_g", "SELL", 0.005), ("adl_autoclose-h", "SELL", 0.006),
    ]
    orders = [{"orderId": i, "clientOrderId": c, "side": s, "positionSide": "LONG", "executedQty": str(q),
               "avgPrice": "3000", "updateTime": 1000 + i} for i, (c, s, q) in enumerate(rows)]
    book = replay_lots(orders, "LONG", 0.024, 0.0005)
    # sin trade_id 0.01 - 0.005 (cierre de fuera primero lo sin dueño); luego el ADL en proporción
    assert {k: round(v, 6) for k, v in book.lots.items()} == {"1": 0.0080, "kline:2": 0.0120}
    assert round(book.unassigned, 6) == 0.004
    assert [t["trade_id"] for t in book.tranches] == [1, 2]
    assert replay_lots(orders[3:], "LONG", 0.024, 0.0005) is None   # el historial no llega a cero


async def test_restart_without_trades_json_keeps_each_bots_lot(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.01, bot="kline")
        await opened(t, fake, 0.01)
        open_(t, 2, 0.02, bot="app25")
        await opened(t, fake, 0.03)
        open_(t, 3, 0.03)
        await opened(t, fake, 0.06)
        open_(t, 3, 0.01, level=25)                # segundo tramo del trade_id 3
        await opened(t, fake, 0.07)
        outside(fake, "BUY", 0.007, "web_other")   # otro bot fuera del executor
        await wait_for(lambda: amt(fake) == 0.077)
        close(t, 2, bot="app25", quantity=0.01)    # cierre parcial de su lote
        await wait_for(lambda: amt(fake) == 0.067)
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        trade = t.get_trade(SYM, "LONG")
        assert lots(trade) == {"kline:1": 0.01, "app25:2": 0.01, "3": 0.04}
        assert abs(trade.unassigned_qty() - 0.007) < 1e-9 and trade.lot_tracked
        assert len(trade.lot_tranches(3)) == 2 and abs(trade.lot_entry(3) - 3000) < 1e-6
        close(t, 2, bot="app25")
        await wait_for(lambda: amt(fake) == 0.057)
        close(t, 3)
        await wait_for(lambda: amt(fake) == 0.017)
        close(t, 1, bot="kline")                   # último lote: Binance tiene más (lo de fuera), no se toca
        await wait_for(lambda: amt(fake) == 0.007)
        await asyncio.sleep(0.5)
        assert amt(fake) == 0.007
        assert sorted((r.paper_trade_id, round(r.quantity, 6)) for r in t.closed) == [(1, 0.01), (2, 0.01), (3, 0.04)]
    finally:
        await svc.stop()


async def test_close_right_after_the_restart_waits_for_the_rebuild(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.01)
        await opened(t, fake, 0.01)
        open_(t, 2, 0.02)
        await opened(t, fake, 0.03)
        await svc.stop()
        (tmp_path / "data" / "trades.json").unlink()
        svc = ExecutorService(make_settings(fake, tmp_path))
        await svc.start()
        close(svc.trades, 1)                       # llega mientras se rehacen los lotes
        await wait_for(lambda: amt(fake) == 0.02)
        await asyncio.sleep(0.5)
        assert amt(fake) == 0.02 and lots(svc.trades.get_trade(SYM, "LONG")) == {"2": 0.02}
    finally:
        await svc.stop()


async def test_long_bot_name_is_matched_after_the_restart(fake, tmp_path, snapshot_file):
    bot = "averyverylongbotname"
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 12345, 0.01, bot=bot)
        await opened(t, fake, 0.01)
        open_(t, 12345, 0.02, bot="other")
        await opened(t, fake, 0.03)
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        assert lots(t.get_trade(SYM, "LONG")) == {"averyverylon:12345": 0.01, "other:12345": 0.02}
        close(t, 12345, bot=bot)
        await wait_for(lambda: amt(fake) == 0.02)
        assert lots(t.get_trade(SYM, "LONG")) == {"other:12345": 0.02}
        assert t.closed[-1].bot == bot
    finally:
        await svc.stop()


async def test_position_opened_only_outside_is_never_closed_by_a_trade_id(fake, tmp_path, snapshot_file):
    outside(fake, "BUY", 0.02, "web_manual")       # abierta a mano en Binance
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        await wait_for(lambda: not t._rebuilding and t.get_trade(SYM, "LONG") is not None)
        trade = t.get_trade(SYM, "LONG")
        assert trade.lot_tracked and not trade.lots and abs(trade.unassigned_qty() - 0.02) < 1e-9
        close(t, 7)
        await wait_for(lambda: t.signals(1, trade_id=7))
        await asyncio.sleep(0.5)
        assert amt(fake) == 0.02
    finally:
        await svc.stop()


async def test_without_history_the_adopted_position_keeps_the_old_rule(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.01)
        await opened(t, fake, 0.01)
        open_(t, 2, 0.02)
        await opened(t, fake, 0.03)
        fake.history.clear()                       # más antigua que los 7 días del historial
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        trade = t.get_trade(SYM, "LONG")
        assert not trade.lot_tracked and not trade.lots
        assert any("no explica la posición" in e["text"] for e in svc.core.bus.recent)
        close(t, 1)                                # como antes de los lotes: la cierra entera
        await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
    finally:
        await svc.stop()


async def test_failed_history_read_is_retried_by_the_next_signal(fake, tmp_path, snapshot_file, monkeypatch):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.01)
        await opened(t, fake, 0.01)
        open_(t, 2, 0.02)
        await opened(t, fake, 0.03)
        fake.all_orders_error = True
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        assert not t.get_trade(SYM, "LONG").lot_tracked
        fake.all_orders_error = False
        t._rebuild_tried.clear()                   # (pasados 30 s)
        close(t, 2)
        await wait_for(lambda: amt(fake) == 0.01)
        await asyncio.sleep(0.5)
        assert amt(fake) == 0.01 and lots(t.get_trade(SYM, "LONG")) == {"1": 0.01}
    finally:
        await svc.stop()


async def test_partial_close_after_the_restart_uses_x1_for_rebuilt_tranches(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 2, 0.02, bot="app25")
        await opened(t, fake, 0.02)
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        open_(t, 2, 0.03, bot="app25")            # nuevo tramo del mismo trade_id tras el reinicio
        await opened(t, fake, 0.05)
        close(t, 2, bot="app25", quantity=0.01)
        await wait_for(lambda: amt(fake) == 0.04)
        await asyncio.sleep(0.5)
        assert amt(fake) == 0.04 and lots(t.get_trade(SYM, "LONG")) == {"app25:2": 0.04}
    finally:
        await svc.stop()
