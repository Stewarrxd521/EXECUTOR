"""Review 5 — rule violations (repro tests; each one FAILS on the current code)."""

import asyncio

import pytest

import executor.trading as trading_mod
from tests.conftest import wait_for
from tests.test_lots import SYM, amt, close, lots, open_, opened


@pytest.fixture(autouse=True)
def quick_retries(monkeypatch):
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0, 0.5, 1.0))


def last_close(t, tid):
    sig = t.signals(1, trade_id=tid)
    return sig[0] if sig and sig[0]["action"] == "close" else None


# F1: a NAMED bot's close for a trade_id it has no lot for takes the UNNAMED bot's lot with the same number.
async def test_named_bot_close_before_its_open_never_takes_an_unnamed_bots_lot(service, fake):
    t = service.trades
    open_(t, 5, 0.02)                              # app.py (no identity) — its trade_id 5
    trade = await opened(t, fake, 0.02)
    open_(t, 9, 0.01, bot="kline")                 # kline is identified (EXECUTOR_URL=.../b/kline)
    await opened(t, fake, 0.03)
    close(t, 5, bot="kline")                       # kline's close of ITS trade 5 arrives before its open
    await wait_for(lambda: last_close(t, 5))
    await asyncio.sleep(0.5)
    assert amt(fake) == 0.03 and lots(trade) == {"5": 0.02, "kline:9": 0.01}


async def test_named_bot_resent_close_never_takes_an_unnamed_bots_lot(service, fake):
    t = service.trades
    open_(t, 5, 0.02)                              # app.py (no identity), trade_id 5, no TP
    trade = await opened(t, fake, 0.02)
    open_(t, 5, 0.01, bot="kline")                 # kline's own trade_id 5
    await opened(t, fake, 0.03)
    assert lots(trade) == {"5": 0.02, "kline:5": 0.01}
    close(t, 5, bot="kline")                       # kline closes its lot: fine
    await wait_for(lambda: amt(fake) == 0.02)
    t._recent_closed.clear()                       # (30 s later)
    close(t, 5, bot="kline")                       # the same close resent by the bot (retry after a timeout)
    await wait_for(lambda: last_close(t, 5) and len([s for s in t.signals(10, trade_id=5)
                                                       if s["action"] == "close"]) >= 2)
    await asyncio.sleep(0.5)
    assert amt(fake) == 0.02 and lots(trade) == {"5": 0.02}


# F2: a partial close of a trade_id leaves its own TP at the ORIGINAL quantity; the TP then closes another bot's lot.
async def test_partial_close_shrinks_or_drops_its_tp(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3100)                     # bot A alone: its TP covers 0.02
    await opened(t, fake, 0.02)
    await wait_for(lambda: fake.algos)
    open_(t, 2, 0.01)                              # bot B joins (no TP)
    trade = await opened(t, fake, 0.03)
    close(t, 1, quantity=0.01)                     # bot A takes partial profit: lot 1 = 0.01
    await wait_for(lambda: amt(fake) == 0.02)
    assert lots(trade) == {"1": 0.01, "2": 0.01}
    fake.set_mark(SYM, 3100.0)                     # A's TP fires
    await asyncio.sleep(1.0)
    assert amt(fake) == 0.01                       # B's 0.01 must still be open


# F6: a TP set by the first trade_id when the position already holds quantity of another bot (outside the executor)
# covers that quantity too, silently.
async def test_tp_never_covers_quantity_opened_outside_without_warning(service, fake):
    t = service.trades
    fake._place({"symbol": SYM, "side": "BUY", "type": "MARKET", "positionSide": "LONG", "quantity": "0.02",
                 "newClientOrderId": "web_otherbot"})          # another bot, outside the executor
    await wait_for(lambda: service.core.account.position(SYM, "LONG") is not None)
    open_(t, 1, 0.01, tp=3100)
    trade = await opened(t, fake, 0.03)
    await wait_for(lambda: fake.algos)
    await wait_for(lambda: t.signals(1, trade_id=1) and t.signals(1, trade_id=1)[0]["ok"])
    detail = t.signals(1, trade_id=1)[0]["detail"]
    algo_qty = [a["quantity"] for a in fake.algos.values()]
    fake.set_mark(SYM, 3100.0)
    await asyncio.sleep(1.0)
    assert amt(fake) == 0.02 or "cubre también" in detail


async def test_named_bot_close_long_after_its_tp_never_takes_an_unnamed_bots_lot(service, fake):
    t = service.trades
    open_(t, 5, 0.01, bot="kline", tp=3100)        # kline alone: its TP covers its 0.01
    await opened(t, fake, 0.01)
    await wait_for(lambda: fake.algos)
    open_(t, 5, 0.02)                              # app.py (no identity) uses the same number
    trade = await opened(t, fake, 0.03)
    fake.set_mark(SYM, 3100.0)                     # kline's TP fires on Binance: its lot is gone
    await wait_for(lambda: amt(fake) == 0.02)
    await wait_for(lambda: lots(trade) == {"5": 0.02})
    fake.set_mark(SYM, 3000.0)
    t._recent_closed.clear()                       # kline's own logic notices its TP a minute later
    close(t, 5, bot="kline", close_price=3100)
    await wait_for(lambda: last_close(t, 5))
    await asyncio.sleep(0.5)
    assert amt(fake) == 0.02 and lots(trade) == {"5": 0.02}


async def test_tp_of_a_new_tranche_uses_the_real_position(service, fake):
    t = service.trades
    lag_pushes = fake._push

    async def slow(event):                         # ACCOUNT_UPDATE a little behind the WS API response
        if event.get("e") == "ACCOUNT_UPDATE":
            await asyncio.sleep(0.3)
        await lag_pushes(event)
    open_(t, 1, 0.02)
    trade = await opened(t, fake, 0.02)
    fake._push = slow
    open_(t, 1, 0.02, level=50, tp=3100)           # 2nd tranche of the same trade_id with its TP
    await wait_for(lambda: fake.algos)
    assert [a["quantity"] for a in fake.algos.values()] == ["0.04"]


# F9: bot B's SL replaces bot A's SL (one per side); when B closes its trade_id, B's SL is cancelled and A is left
# with no stop at all.
async def test_closing_one_trade_id_never_leaves_another_without_its_sl(service, fake):
    t = service.trades
    open_(t, 1, 0.02, sl=2900)                     # bot A alone: SL 0.02 @ 2900
    await opened(t, fake, 0.02)
    await wait_for(lambda: fake.algos)
    open_(t, 2, 0.01, sl=2950)                     # bot B with its own SL
    trade = await opened(t, fake, 0.03)
    await wait_for(lambda: t.signals(1, trade_id=2) and t.signals(1, trade_id=2)[0]["ok"])
    close(t, 2)                                    # B closes its trade_id
    await wait_for(lambda: amt(fake) == 0.02)
    await asyncio.sleep(0.5)
    fake.set_mark(SYM, 2800.0)                     # the market dumps through A's stop
    await asyncio.sleep(0.8)
    assert amt(fake) == 0                          # A's 0.02 must have been stopped out


# F13: a trade_id that is not a number (e.g. "T-7", a UUID) is silently parsed as 0 = "no trade_id": the bot's close
# then closes the WHOLE position, other bots' lots included.


# F13b: a bot name outside [a-z0-9_.-]{1,32} (e.g. "Kline Bot") is silently dropped: the bot becomes "unnamed" and its
# trade_ids collide with another unnamed bot's.
async def test_invalid_bot_name_is_not_silently_dropped(service, fake):
    t = service.trades
    open_(t, 3, 0.01)                              # unnamed bot (app.py), trade_id 3
    await opened(t, fake, 0.01)
    open_(t, 3, 0.02, bot="Kline Bot")             # another bot, identified with an invalid name
    trade = await opened(t, fake, 0.03)
    assert len(lots(trade)) == 2
