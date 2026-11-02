"""Adversarial review: Binance error codes and their automatic corrections.

Every test here FAILS on the current code and describes a real defect."""

import asyncio
import time
from decimal import Decimal

import pytest

import executor.trading as trading_mod
from executor.service import ExecutorService
from tests.conftest import make_settings, wait_for
from tests.fake_binance import FakeBinance, _Err
from tests.test_lots import SYM, amt, close, lots, open_, opened


@pytest.fixture(autouse=True)
def quick(monkeypatch):
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0, 0.3, 0.6))
    monkeypatch.setattr(trading_mod, "UNKNOWN_OPEN_CHECKS", (0.3, 0.6, 1.0))


def settled(t):
    return not t.inflight(SYM) and not t._reconciling and not t._closing


def schedule_place(fake, codes):
    """order.place n-th call raises codes[n] (None = execute normally)."""
    orig = fake._place
    calls = {"n": 0}

    def place(p):
        i = calls["n"]
        calls["n"] += 1
        code = codes[i] if i < len(codes) else None
        if code is not None:
            raise _Err(code, f"injected {code}")
        return orig(p)

    fake._place = place
    return calls


# ── 1. A split / chunked MARKET open whose later chunk fails loses the executed chunk ──────────────────────
@pytest.mark.parametrize("second_code", [-2018, -2019, -1003])
async def test_split_open_with_failing_second_chunk_leaves_no_untracked_position(service, fake, second_code):
    t = service.trades
    # -4005 → split in 2 halves; first half fills, second half is rejected.
    schedule_place(fake, [-4005, None, second_code])
    open_(t, 1, 0.04)
    await asyncio.sleep(0.3)
    await wait_for(lambda: settled(t) and t.signals(1, trade_id=1), timeout=15)
    await asyncio.sleep(1.5)
    real = amt(fake)
    trade = t.get_trade(SYM, "LONG")
    assert real == pytest.approx(0.02)  # the first half really executed on Binance
    assert trade is not None, f"{second_code}: Binance has {real} LONG but the executor tracks nothing"
    assert not trade.order_assumed, f"{second_code}: real {real} recorded as an ASSUMED (non-real) trade"
    assert abs(trade.ledger_qty - real) < 1e-9, f"{second_code}: ledger {trade.ledger_qty} != Binance {real}"


# ── 2. -1000 UNKNOWN is resent blindly (same cid) → executes twice ────────────────────────────────────────
async def test_unknown_error_after_execution_on_lot_close_does_not_close_the_other_lot(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.03)
    # Binance executed the close but answered -1000 "Unknown error, please check your request or try again later."
    fake.inject_after.append(("order.place", -1000, "Unknown error, please check your request or try again later."))
    close(t, 1)
    await asyncio.sleep(0.3)
    await wait_for(lambda: settled(t) and t.signals(1, trade_id=1)
                   and t.signals(1, trade_id=1)[0]["action"] == "close", timeout=20)
    await asyncio.sleep(0.5)
    assert amt(fake) == pytest.approx(0.02), f"lot 2 (0.02) was touched: Binance {amt(fake)}"


async def test_unknown_error_after_execution_on_open_does_not_double_the_order(service, fake):
    t = service.trades
    fake.inject_after.append(("order.place", -1000, "Unknown error, please check your request or try again later."))
    open_(t, 1, 0.01)
    await asyncio.sleep(0.3)
    await wait_for(lambda: settled(t) and t.signals(1, trade_id=1), timeout=15)
    await asyncio.sleep(0.5)
    assert sum(1 for m, p in fake.requests if m == "order.place") == 1 or amt(fake) == pytest.approx(0.01), \
        f"open of 0.01 executed {amt(fake)}"
    assert amt(fake) == pytest.approx(0.01)


async def test_duplicate_client_id_after_unknown_outcome_is_not_resent_with_a_new_id(service, fake):
    """-4116 after a resend means the first order EXISTS: a new clientOrderId duplicates it."""
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.03)
    fake.inject_after.append(("order.place", -1000, "Unknown error, please check your request or try again later."))
    fake.inject.append(("order.place", -4116, "ClientOrderId is duplicated."))
    close(t, 1)
    await asyncio.sleep(0.3)
    await wait_for(lambda: settled(t) and t.signals(1, trade_id=1)
                   and t.signals(1, trade_id=1)[0]["action"] == "close", timeout=20)
    await asyncio.sleep(0.5)
    assert amt(fake) == pytest.approx(0.02), f"lot 2 (0.02) was touched: Binance {amt(fake)}"


# ── 3. TP/SL (algoOrder.place): transient errors are not retried → position without SL ────────────────────
@pytest.mark.parametrize("code,msg", [(-1001, "Internal error; unable to process your request. Please try again."),
                                      (-1008, "Server is currently overloaded with other requests."),
                                      (-4116, "ClientOrderId is duplicated.")])
async def test_sl_on_open_survives_a_transient_algo_error(service, fake, code, msg):
    t = service.trades
    fake.inject.append(("algoOrder.place", code, msg))
    open_(t, 1, 0.01, sl=2900)
    await opened(t, fake, 0.01)
    await wait_for(lambda: settled(t) and t.signals(1, trade_id=1), timeout=10)
    await asyncio.sleep(0.5)
    assert any(a["type"] == "STOP_MARKET" for a in fake.algos.values()), \
        f"{code}: open position left WITHOUT stop loss: {t.signals(1, trade_id=1)[0]['detail']}"


async def test_sl_algo_with_unknown_outcome_is_not_reported_as_not_placed(service, fake):
    t = service.trades
    fake.inject_after.append(("algoOrder.place", -1007, "Timeout waiting for response from backend server."))
    open_(t, 1, 0.01, sl=2900)
    await opened(t, fake, 0.01)
    await wait_for(lambda: settled(t) and t.signals(1, trade_id=1), timeout=10)
    await asyncio.sleep(0.5)
    assert any(a["type"] == "STOP_MARKET" for a in fake.algos.values())
    detail = t.signals(1, trade_id=1)[0]["detail"]
    assert "SL no colocado" not in detail, f"SL exists on Binance but the signal says: {detail}"


# ── 4. One-way: a close that loses reduceOnly can open the opposite side ─────────────────────────────────
@pytest.fixture
async def oneway(tmp_path, snapshot_file):
    f = FakeBinance(hedge=False)
    await f.start()
    svc = ExecutorService(make_settings(f, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.core.market.conn.connected and svc.user.conn.connected and svc.trades.ready.is_set())
    yield svc, f
    await svc.stop()
    await f.stop()


def amt_both(fake):
    return round(fake.positions.get((SYM, "BOTH"), {"amt": 0.0})["amt"], 10)


@pytest.mark.parametrize("code,msg", [(-4062, "Invalid or improper reduceOnly value."),
                                      (-2026, "Order type not supported when reduceOnly."),
                                      (-1106, "Parameter 'reduceonly' sent when not required.")])
async def test_one_way_lot_close_is_never_resent_without_reduce_only(oneway, code, msg):
    svc, fake = oneway
    t = svc.trades
    open_(t, 1, 0.01)
    await wait_for(lambda: amt_both(fake) == 0.01 and t.get_trade(SYM, "LONG") is not None)
    open_(t, 2, 0.02)
    await wait_for(lambda: amt_both(fake) == 0.03 and t.get_trade(SYM, "LONG").quantity == pytest.approx(0.03))
    start = len(fake.requests)
    fake.inject.append(("order.place", code, msg))
    close(t, 1)
    await asyncio.sleep(0.3)
    await wait_for(lambda: settled(t) and t.signals(1, trade_id=1)
                   and t.signals(1, trade_id=1)[0]["action"] == "close", timeout=20)
    sells = [p for m, p in fake.requests[start:] if m == "order.place" and p.get("side") == "SELL"]
    naked = [p for p in sells if p.get("positionSide") == "BOTH" and p.get("reduceOnly") != "true"]
    assert not naked, f"{code}: close sent as a plain SELL in One-way (can open a SHORT): {naked}"


# ── 5. Price-filter error on a MARKET order coarsens the QUANTITY step (persisted) ───────────────────────
@pytest.mark.parametrize("code,msg", [(-1013, "Filter failure: PERCENT_PRICE"),
                                      (-4014, "Price not increased by tick size.")])
async def test_price_error_on_market_order_does_not_change_the_qty_step(service, fake, code, msg):
    t = service.trades
    before = service.core.exinfo.get(SYM).qty_step(True)
    fake.inject.append(("order.place", code, msg))
    open_(t, 1, 0.01)
    await asyncio.sleep(0.3)
    await wait_for(lambda: settled(t) and t.signals(1, trade_id=1), timeout=15)
    after = service.core.exinfo.get(SYM).qty_step(True)
    assert after == before, f"{code} ({msg}) on a MARKET order changed stepSize {before} → {after}"


# ── 6. Rate-limit ban longer than the close wait: the lot close is dropped for good ──────────────────────
async def test_lot_close_during_a_long_ip_ban_is_retried_after_the_ban(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.03)
    until = int((time.time() + 40) * 1000)
    fake.inject.append(("order.place", -1003, f"Way too many requests; IP(1.2.3.4) banned until {until}. "
                                               "Please use the websocket for live updates to avoid bans."))
    close(t, 1)
    # Binance lifts the ban after 40 s; the bot's close must still happen (or stay pending), not be lost.
    await wait_for(lambda: amt(fake) == pytest.approx(0.02), timeout=75)
    assert lots(trade) == {"2": 0.02}


# ── 7. -2027 whose leverage drop fails at Binance: diagnosis says nothing about leverage ─────────────────
@pytest.mark.parametrize("code,msg", [(-2028, "Leverage is smaller than permitted: insufficient margin balance"),
                                      (-1003, "Too many requests"), (-2015, "Invalid API-key, IP, or permissions")])
async def test_2027_when_lowering_leverage_fails_the_rejection_says_why(service, fake, monkeypatch, code, msg):
    from executor.errors import BinanceAPIError
    t = service.trades
    rest = service.core.rest
    orig = rest.set_leverage

    async def set_leverage(symbol, lev):
        if lev < 7:
            raise BinanceAPIError(code, msg, http_status=400, transport="rest")
        return await orig(symbol, lev)

    monkeypatch.setattr(rest, "set_leverage", set_leverage)
    fake.max_position_by_leverage = {7: 10.0, 5: 1000.0}
    t.handle_signal({"action": "open", "trade_id": 1, "symbol": SYM, "direction": "LONG", "price": 3000,
                     "quantity": 0.03, "leverage": 7})
    await wait_for(lambda: t.signals(1, trade_id=1) and not t.inflight(SYM), timeout=10)
    assert amt(fake) == 0
    texts = [str(e.get("text", "")) for e in service.core.bus.recent if e.get("kind") == "open_failed"]
    texts.append(t.signals(1, trade_id=1)[0]["detail"])
    assert any(str(code) in x or "leverage" in x.lower() and "bajar" in x.lower() for x in texts), \
        f"rejection does not say the leverage drop failed ({code}): {texts}"
