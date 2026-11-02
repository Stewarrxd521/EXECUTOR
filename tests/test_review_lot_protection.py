"""Reproducciones de la revisión adversarial (ronda 6): lo que no debe volver a pasar."""
import asyncio
from tests.conftest import wait_for
from tests.test_lots import SYM, amt, open_, close, opened, lots, algos


def split_algo_fills(fake, parts=2):
    """Real Binance: a triggered MARKET order often fills in several trades (same orderId)."""
    orig = fake._apply_fill

    def apply(order, price, qty):
        if str(order.get("clientOrderId", "")).startswith("algo") and parts > 1:
            piece = round(qty / parts, 10)
            r = 0.0
            for i in range(parts):
                q = piece if i < parts - 1 else round(qty - piece * (parts - 1), 10)
                r += orig(order, price, q)
            return r
        return orig(order, price, qty)
    fake._apply_fill = apply


async def test_tp_fill_in_two_trades_does_not_touch_other_lot(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3100, bot="kline")
    await opened(t, fake, 0.02)
    open_(t, 2, 0.04, tp=3300, bot="app25")
    trade = await opened(t, fake, 0.06)
    await wait_for(lambda: len(fake.algos) == 2)
    split_algo_fills(fake)
    fake.set_mark(SYM, 3100.0)                   # kline TP fires: 0.02 filled as 0.01 + 0.01
    await wait_for(lambda: amt(fake) == 0.04)
    await asyncio.sleep(0.8)
    assert lots(trade) == {"app25:2": 0.04}
    assert algos(fake) == [("TAKE_PROFIT_MARKET", "T2:app25", 0.04)]


async def test_tp_fill_in_two_trades_then_bot_close_hits_other_bot(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3100, bot="kline")
    await opened(t, fake, 0.02)
    open_(t, 2, 0.04, tp=3300, bot="app25")
    trade = await opened(t, fake, 0.06)
    await wait_for(lambda: len(fake.algos) == 2)
    split_algo_fills(fake)
    fake.set_mark(SYM, 3100.0)
    await wait_for(lambda: amt(fake) == 0.04)
    await asyncio.sleep(0.8)
    close(t, 1, bot="kline")                     # kline bot reports its TP close
    await asyncio.sleep(1.0)
    assert amt(fake) == 0.04                     # app25's 0.04 must stay open


async def test_finished_event_after_fill_leaves_stale_owner(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3100, bot="kline")
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02, tp=3300, bot="app25")
    await opened(t, fake, 0.04)
    open_(t, 3, 0.02, bot="other")              # third bot, no TP
    trade = await opened(t, fake, 0.06)
    await wait_for(lambda: len(fake.algos) == 2)
    push = fake._push
    mode = {"late": False}

    async def p(event):
        if event.get("e") == "ALGO_UPDATE" and event["o"].get("X") == "TRIGGERED":
            if mode["late"]:                     # ALGO_UPDATE after the fill (case the code handles)
                await asyncio.sleep(0.3)
                await push(event)
                return
            await push(event)
            await asyncio.sleep(0.2)             # Binance: TRIGGERED -> fill -> FINISHED
            await push({**event, "o": {**event["o"], "X": "FINISHED"}})
            return
        await push(event)
    fake._push = p
    fake.set_mark(SYM, 3100.0)
    await wait_for(lambda: amt(fake) == 0.04)
    await asyncio.sleep(0.6)
    assert lots(trade) == {"app25:2": 0.02, "other:3": 0.02}
    mode["late"] = True
    fake.set_mark(SYM, 3300.0)
    await wait_for(lambda: amt(fake) == 0.02)
    await asyncio.sleep(0.8)
    assert lots(trade) == {"other:3": 0.02}


async def test_finished_event_stale_owner_normal_order(service, fake):
    """TRIGGERED -> fill -> FINISHED for each TP, all in Binance's normal order."""
    t = service.trades
    open_(t, 1, 0.02, tp=3100, bot="kline")
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02, tp=3300, bot="app25")
    await opened(t, fake, 0.04)
    open_(t, 3, 0.02, bot="other")
    trade = await opened(t, fake, 0.06)
    await wait_for(lambda: len(fake.algos) == 2)
    push = fake._push

    async def p(event):
        await push(event)
        if event.get("e") == "ALGO_UPDATE" and event["o"].get("X") == "TRIGGERED":
            await asyncio.sleep(0.2)
            await push({**event, "o": {**event["o"], "X": "FINISHED"}})
    fake._push = p
    fake.set_mark(SYM, 3100.0)
    await wait_for(lambda: amt(fake) == 0.04)
    await asyncio.sleep(0.6)
    fake.set_mark(SYM, 3300.0)
    await wait_for(lambda: amt(fake) == 0.02)
    await asyncio.sleep(0.8)
    assert lots(trade) == {"other:3": 0.02}


async def test_two_outside_fills_place_duplicate_lot_tp(service, fake):
    """Another party reduces the position with a market order that fills in 2 trades."""
    t = service.trades
    open_(t, 1, 0.02, tp=3300, bot="kline")
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02, bot="app25")
    trade = await opened(t, fake, 0.04)
    await wait_for(lambda: len(fake.algos) == 1)
    fake.inject  # noqa
    order = {"symbol": SYM, "side": "SELL", "type": "MARKET", "positionSide": "LONG", "origQty": 0.02,
             "orderId": fake._next(), "clientOrderId": "web_manual1", "status": "NEW"}
    fake._apply_fill(order, 3000.0, 0.01)
    fake._apply_fill(order, 3000.0, 0.01)
    await wait_for(lambda: amt(fake) == 0.02)
    await asyncio.sleep(1.0)
    tps = [a for a in algos(fake) if a[1] == "T1:kline"]
    assert len(tps) == 1 and abs(tps[0][2] - trade.lots["kline:1"]) < 1e-9


async def test_long_bot_name_tp_fill_charged_to_other_lots(service, fake):
    t = service.trades
    open_(t, 123456, 0.02, tp=3100, bot="kline_scalper")      # 13 chars: cut in the clientAlgoId (room 11)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02, tp=3300, bot="app25")
    trade = await opened(t, fake, 0.04)
    await wait_for(lambda: len(fake.algos) == 2)
    fake.set_mark(SYM, 3100.0)
    await wait_for(lambda: amt(fake) == 0.02)
    await asyncio.sleep(0.8)
    assert lots(trade) == {"app25:2": 0.02}


async def test_rejected_lot_close_leaves_lot_without_its_tp_sl(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3300, sl=2900, bot="kline")
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02, bot="app25")
    trade = await opened(t, fake, 0.04)
    await wait_for(lambda: len(fake.algos) == 2)
    for _ in range(3):
        fake.inject.append(("order.place", -2010, "Order would immediately trigger / rejected"))
    close(t, 1, bot="kline")
    await asyncio.sleep(2.0)
    assert amt(fake) == 0.04                     # close rejected: nothing closed
    assert sorted(a[1] for a in algos(fake)) == ["S1:kline", "T1:kline"]   # its TP/SL must still be there


async def test_sixth_lot_over_the_binance_algo_limit_is_alerted(service, fake):
    import tests.fake_binance as fb
    orig = fake._dispatch

    def dispatch(method, params):
        if method == "algoOrder.place" and sum(1 for a in fake.algos.values() if a["symbol"] == params["symbol"]) >= 10:
            raise fb._Err(-4045, "Reach max stop order limit.")
        return orig(method, params)
    fake._dispatch = dispatch
    t = service.trades
    for tid in range(1, 7):
        open_(t, tid, 0.01, tp=3300 + tid, sl=2900 - tid, bot="b%d" % tid)
        await opened(t, fake, round(0.01 * tid, 10))
        await asyncio.sleep(0.3)
    assert len(fake.algos) == 10                 # límite de Binance: no se puede saltar...
    assert any(e["kind"] == "protect_failed" for e in service.core.bus.recent)   # ...pero se avisa


def binance_algo_events(fake):
    """Real ALGO_UPDATE: TRIGGERED carries "ai" (orderId in the matching engine); FINISHED follows the fill."""
    push = fake._push

    async def p(event):
        if event.get("e") == "ALGO_UPDATE" and event["o"].get("X") == "TRIGGERED":
            aid = event["o"]["aid"]
            oid = next((o["orderId"] for o in fake.history.values() if o.get("clientOrderId") == f"algo{aid}"), None)
            o = {**event["o"], "ai": str(oid or "")}
            await push({**event, "o": o})
            await asyncio.sleep(0.2)
            await push({**event, "o": {**o, "X": "FINISHED"}})
            return
        await push(event)
    fake._push = p


async def test_tp_fill_in_three_trades_with_ai(service, fake):
    t = service.trades
    open_(t, 1, 0.03, tp=3100, bot="kline")
    await opened(t, fake, 0.03)
    open_(t, 2, 0.03, tp=3300, bot="app25")
    trade = await opened(t, fake, 0.06)
    await wait_for(lambda: len(fake.algos) == 2)
    binance_algo_events(fake)
    split_algo_fills(fake, parts=3)
    fake.set_mark(SYM, 3100.0)
    await wait_for(lambda: amt(fake) == 0.03)
    await asyncio.sleep(0.8)
    assert lots(trade) == {"app25:2": 0.03}


async def test_single_fill_with_ai_leaves_stale_fired_entry(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3100, bot="kline")
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02, tp=3300, bot="app25")
    await opened(t, fake, 0.04)
    trade = t.get_trade(SYM, "LONG")
    await wait_for(lambda: len(fake.algos) == 2)
    binance_algo_events(fake)
    fake.set_mark(SYM, 3100.0)
    await wait_for(lambda: amt(fake) == 0.02)
    await asyncio.sleep(0.6)
    assert not t._fired.get((SYM, "LONG"))
