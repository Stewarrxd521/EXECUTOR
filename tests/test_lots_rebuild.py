"""Reinicio sin trades.json (disco efímero de Render): los lotes se rehacen con el historial de Binance."""

import asyncio

import pytest

import executor.trading as trading_mod
from executor.service import ExecutorService
from executor.trading import cid_bot, lot_cid_prefix, parse_lot_cid, replay_lots
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
    long = lot_cid_prefix("X", "averyverylongbotname:12345")
    assert long.startswith("X12345:averyve.") and len(long) == trading_mod.CID_PREFIX_MAX
    assert cid_bot("app25_strategy_a", 1729000000000) != cid_bot("app25_strategy_b", 1729000000000)
    assert len(lot_cid_prefix("X", "a" * 32 + ":123456789012")) <= trading_mod.CID_PREFIX_MAX
    assert parse_lot_cid("X5-18f2c0a1b2Ab3dE4") == ("X", 5)
    assert parse_lot_cid("K5:my-bot-18f2c0a1b2Ab3dE4") == ("K", "my-bot:5")
    assert parse_lot_cid("C12-18f2c0a1b2Ab3dE4") is None and parse_lot_cid("web_manual") is None
    assert parse_lot_cid("X5aB3dE4f6g7h8i9j0k1l2") is None   # clientOrderId automático de Binance


def fills(rows, start=0):
    """(clientOrderId, lado, cantidad[, hora]) -> fills como los devuelve userTrades (con su orden)."""
    out = []
    for i, row in enumerate(rows):
        cid, side, qty = row[:3]
        out.append({"id": start + i, "orderId": start + i, "clientOrderId": cid, "side": side, "positionSide": "LONG",
                    "qty": str(qty), "price": "3000", "time": row[3] if len(row) > 3 else 1000 + i})
    return out


def test_replay_starts_at_the_last_flat_point():
    rows = fills([
        ("X1-1a2b3cAbCdEf", "BUY", 0.02), ("C3-1a2b3cAbCdEf", "SELL", 0.02),     # posición anterior, ya cerrada
        ("X1-1a2b3dAbCdEf", "BUY", 0.01), ("X2:kline-1a2b3eAbCdEf", "BUY", 0.02), ("web_e", "BUY", 0.01),
        ("K2:kline-1a2b3fAbCdEf", "SELL", 0.005), ("web_g", "SELL", 0.005), ("adl_autoclose-h", "SELL", 0.006),
    ])
    book = replay_lots(rows, "LONG", 0.024, 0.0005)
    # sin trade_id 0.01 - 0.005 (cierre de fuera primero lo sin dueño); luego el ADL en proporción
    assert {k: round(v, 6) for k, v in book.lots.items()} == {"1": 0.0080, "kline:2": 0.0120}
    assert round(book.unassigned, 6) == 0.004
    assert [t["trade_id"] for t in book.tranches] == [1, 2] and book.history_ms == 1007
    assert replay_lots(rows[3:], "LONG", 0.024, 0.0005) is None   # el historial no llega a cero


def test_replay_follows_each_fill_not_its_order():
    """Una LIMIT de fuera que se llena en parte y se cancela después cuenta a la hora de su fill."""
    rows = fills([("web_limit", "BUY", 0.01, 1000), ("X1-1a2b3cAbCdEf", "BUY", 0.02, 2000),
                  ("web_close", "SELL", 0.01, 3000)])
    book = replay_lots(rows, "LONG", 0.02, 0.0005)
    assert book.lots == {"1": 0.02} and book.unassigned == 0


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
        assert lots(t.get_trade(SYM, "LONG")) == {f"{cid_bot(bot, 12345)}:12345": 0.01, "other:12345": 0.02}
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
        fake.user_fills.clear()                    # más antigua que los 7 días del historial
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


# ── Revisión adversarial: lo que no debe volver a pasar ──────────────────────
async def test_open_after_a_failed_startup_read_keeps_the_old_lots(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.01)
        await opened(t, fake, 0.01)
        open_(t, 2, 0.02)
        await opened(t, fake, 0.03)
        await svc.stop()
        (tmp_path / "data" / "trades.json").unlink()
        fake.inject += [("account.position", -1001, "Internal error")] * 3   # the start-up sync fails once
        svc = ExecutorService(make_settings(fake, tmp_path))
        await svc.start()
        await wait_for(lambda: svc.user.conn.connected and svc.trades.ready.is_set())
        t = svc.trades
        open_(t, 3, 0.01)                             # a third bot opens on the shared symbol
        await opened(t, fake, 0.04)
        trade = t.get_trade(SYM, "LONG")
        close(t, 1)                                   # bot 1 closes its 0.01
        await wait_for(lambda: t.signals(1, trade_id=1))
        await asyncio.sleep(1.0)
        assert amt(fake) == 0.03
    finally:
        await svc.stop()


async def test_open_after_a_failed_history_read_keeps_the_old_lots(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.01)
        await opened(t, fake, 0.01)
        open_(t, 2, 0.02)
        await opened(t, fake, 0.03)
        fake.all_orders_error = True                  # REST (proxy) briefly failing during the deploy
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        fake.all_orders_error = False                 # ... and back a second later
        open_(t, 3, 0.01)                             # a signal queued during start-up
        await opened(t, fake, 0.04)
        trade = t.get_trade(SYM, "LONG")
        t._rebuild_tried.clear()                      # even after the 30 s
        close(t, 1)
        await wait_for(lambda: t.signals(1, trade_id=1))
        await asyncio.sleep(1.0)
        assert amt(fake) == 0.03
    finally:
        await svc.stop()


async def test_long_bot_names_that_start_alike_keep_separate_lots(fake, tmp_path, snapshot_file):
    a, b = "kline_scalper_btc_v2", "kline_scalper_btc_v3"
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 7, 0.01, bot=a)
        await opened(t, fake, 0.01)
        open_(t, 7, 0.02, bot=b)
        await opened(t, fake, 0.03)
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        close(t, 7, bot=a)
        await wait_for(lambda: amt(fake) < 0.03)
        await asyncio.sleep(0.5)
        assert amt(fake) == 0.02, "bot B's 0.02 must stay open"
    finally:
        await svc.stop()


async def test_long_bot_name_is_never_claimed_by_another_bot(fake, tmp_path, snapshot_file):
    a, b = "kline_scalper_btc_v2", "kline_scalper_btc_v3"
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 7, 0.01, bot=a)                      # A: LONG
        await opened(t, fake, 0.01)
        open_(t, 7, 0.02, "SHORT", bot=b)             # B: SHORT, same trade_id
        await opened(t, fake, 0.02, "SHORT")
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        close(t, 7, bot=a)                            # A's close renames B's SHORT lot to A's name too
        await wait_for(lambda: amt(fake) == 0)
        close(t, 7, "SHORT", bot=b)
        await wait_for(lambda: t.signals(1, trade_id=7) and t.signals(1, trade_id=7)[0]["direction"] == "SHORT")
        await asyncio.sleep(1.0)
        assert amt(fake, "SHORT") == 0
    finally:
        await svc.stop()


async def test_lot_close_names_the_lot_it_really_closed(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.01)                             # bot opened without /b/<bot>: lot "1"
        await opened(t, fake, 0.01)
        open_(t, 2, 0.02, bot="app25")
        await opened(t, fake, 0.03)
        close(t, 1, bot="kline", quantity=0.005)      # same bot, now identifying (supported transition)
        await wait_for(lambda: amt(fake) == 0.025)
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        close(t, 1, bot="kline")                      # the rest of its lot: 0.005
        await wait_for(lambda: amt(fake) < 0.025)
        await asyncio.sleep(0.5)
        assert amt(fake) == 0.02, "app25's 0.02 must stay open"
    finally:
        await svc.stop()


async def test_outside_order_ids_never_become_a_lot(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.02)
        await opened(t, fake, 0.02)
        outside(fake, "BUY", 0.01, "Mq2Zr8sLhT0aX1bC9dE4fG")   # API order without newClientOrderId
        await wait_for(lambda: amt(fake) == 0.03)
        outside(fake, "SELL", 0.01, "web_abcdef")               # ... and its owner closes it
        await wait_for(lambda: amt(fake) == 0.02)
        await asyncio.sleep(0.3)
        trade = t.get_trade(SYM, "LONG")
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        close(t, 1)                                   # bot 1 is the only owner
        await wait_for(lambda: amt(fake) < 0.02)
        await asyncio.sleep(0.5)
        assert amt(fake) == 0
    finally:
        await svc.stop()


async def test_partial_fills_are_replayed_at_their_own_time(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.02)
        await opened(t, fake, 0.02)
        # Someone puts a LIMIT take-profit on part of the position in the Binance app; 0.01 of it fills now.
        res = fake._place({"symbol": SYM, "side": "SELL", "type": "LIMIT", "positionSide": "LONG", "quantity": "0.02",
                           "price": "3100", "newClientOrderId": "web_tp_limit"})
        order = fake.orders[res["orderId"]]
        fake._apply_fill(order, 3100.0, 0.01)
        order["status"] = "PARTIALLY_FILLED"
        await wait_for(lambda: amt(fake) == 0.01)
        await asyncio.sleep(0.3)
        open_(t, 2, 0.02)                             # later, bot 2 opens
        await opened(t, fake, 0.03)
        fake.orders.pop(order["orderId"])             # the rest of the LIMIT is cancelled afterwards
        order["status"] = "CANCELED"
        fake.push(fake._otu(order, "CANCELED"))
        await asyncio.sleep(0.3)
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        close(t, 1)
        await wait_for(lambda: amt(fake) < 0.03)
        await asyncio.sleep(0.5)
        assert amt(fake) == 0.02, "bot 2's 0.02 must stay open"
    finally:
        await svc.stop()


async def test_late_event_of_a_replayed_fill_is_counted_once(fake, tmp_path, snapshot_file):
    from tests.test_lots_ledger import lag
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.01)
        await opened(t, fake, 0.01)
        open_(t, 2, 0.02)
        await opened(t, fake, 0.03)
        outside(fake, "BUY", 0.007, "web_other_open")   # another trader's quantity (no trade_id)
        await wait_for(lambda: amt(fake) == 0.037)
        fake.all_orders_error = True                    # start-up rebuild fails (REST hiccup)
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        fake.all_orders_error = False
        t._rebuild_tried.clear()                        # (30 s later)
        lag(fake, 2.5, "ORDER_TRADE_UPDATE")            # user stream ~2.5 s behind
        outside(fake, "SELL", 0.007, "web_other_close") # the other trader closes ITS 0.007
        close(t, 2)                                     # bot 2 closes: the lazy rebuild already sees that close
        await wait_for(lambda: amt(fake) == 0.01)
        await asyncio.sleep(3.0)                        # the late event arrives
        trade = t.get_trade(SYM, "LONG")
        close(t, 1)
        await wait_for(lambda: t.signals(1, trade_id=1))
        await asyncio.sleep(2.5)
        assert amt(fake) == 0
    finally:
        await svc.stop()


async def test_dashboard_limit_order_rebuilds_like_live(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.02)
        await opened(t, fake, 0.02)
        await svc._cmd_limit_order({"symbol": SYM, "side": "BUY", "direction": "LONG", "price": 3100,
                                    "quantity": 0.01})          # dashboard "limit order" (prefix M, no owner)
        await wait_for(lambda: amt(fake) == 0.03)
        outside(fake, "SELL", 0.01, "web_abcdef")               # the operator takes it off by hand
        await wait_for(lambda: amt(fake) == 0.02)
        await asyncio.sleep(0.3)
        trade = t.get_trade(SYM, "LONG")
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        close(t, 1)
        await wait_for(lambda: amt(fake) < 0.02)
        await asyncio.sleep(0.5)
        assert amt(fake) == 0
    finally:
        await svc.stop()


async def test_history_is_read_in_pages(fake, tmp_path, snapshot_file):
    """Más de 1000 fills recientes del símbolo (p. ej. un grid en el otro lado) no impiden rehacer los lotes."""
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.01)
        await opened(t, fake, 0.01)
        open_(t, 2, 0.02)
        await opened(t, fake, 0.03)
        for i in range(1500):                      # fills más nuevos del lado SHORT
            fake.user_fills.append({"symbol": SYM, "id": 10_000 + i, "orderId": 10_000 + i, "side": "SELL",
                                    "positionSide": "SHORT", "price": "3000", "qty": "0.001", "time": fake._ms()})
        svc = await restart_without_state(svc, fake, tmp_path)
        t = svc.trades
        assert lots(t.get_trade(SYM, "LONG")) == {"1": 0.01, "2": 0.02}
        assert fake.rest_calls.count("userTrades") >= 2
        close(t, 1)
        await wait_for(lambda: amt(fake) == 0.02)
    finally:
        await svc.stop()
