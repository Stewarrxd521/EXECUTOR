"""Review 5 — races (repro tests; each one FAILS on the current code)."""

import asyncio
from decimal import Decimal

import pytest

import executor.trading as trading_mod
from tests.conftest import wait_for
from tests.fake_binance import _Err
from tests.test_lots import SYM, amt, close, lots, open_, opened
from tests.test_lots_ledger import TIMEOUT, THIN, lag, status_down

@pytest.fixture(autouse=True)
def quick_retries(monkeypatch):
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0, 0.5, 1.0))
    monkeypatch.setattr(trading_mod, "UNKNOWN_OPEN_CHECKS", (1.0, 2.0, 3.0))


def last_close(t, tid):
    sig = t.signals(1, trade_id=tid)
    return sig[0] if sig and sig[0]["action"] == "close" else None


# F3: the TP fill of lot 1 is still in flight on the user stream (> GAP_WAIT_S) when bot 2 closes its lot.
@pytest.mark.parametrize("stream_lag", [2.0])
async def test_other_bot_close_while_a_tp_fill_lags_past_the_gap_wait(service, fake, stream_lag):
    t = service.trades
    open_(t, 1, 0.02, tp=3100)                    # bot A alone: TP of 0.02 (its lot)
    await opened(t, fake, 0.02)
    await wait_for(lambda: fake.algos)
    open_(t, 2, 0.01)                             # bot B
    trade = await opened(t, fake, 0.03)
    lag(fake, stream_lag)                         # busy market: the user stream is 2 s behind
    fake.set_mark(SYM, 3100.0)                    # A's TP fills on Binance (0.02): only B's 0.01 is left
    await asyncio.sleep(0.05)
    close(t, 2, close_price=3100)                 # B closes its trade_id
    await wait_for(lambda: last_close(t, 2), timeout=10)
    await asyncio.sleep(stream_lag + 1.0)
    assert amt(fake) == 0                         # B's whole 0.01 must be closed


# F5: the gap inferred for a lost lot close is booked on its FIRST clientOrderId; the second order's fill is then
# applied again when it is confirmed, and the other bot's lot shrinks in the ledger.
async def test_inferred_gap_is_booked_on_the_order_that_was_lost(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.04)
    place, state = fake._place, {"n": 0}

    def hooked(p):
        if fake._is_closing(p):
            state["n"] += 1
            if state["n"] == 1 and p["type"] == "MARKET":
                raise _Err(*THIN)                # thin book: MARKET → LIMIT IOC
            if state["n"] == 2:                  # the IOC fills half and expires (response OK)
                q = Decimal(p["quantity"])
                res = place({**p, "quantity": str((q / 2).quantize(Decimal("0.001")))})
                res.update({"origQty": str(q), "status": "EXPIRED"})
                return res
            if state["n"] == 3:                  # the next order fills but its response is lost
                fake.inject_after.append(TIMEOUT)
        return place(p)
    fake._place = hooked
    down = status_down(fake, order_events=True)  # order.status down and its final event lost
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0.02, timeout=10)
    await wait_for(lambda: last_close(t, 1), timeout=15)
    down["on"] = False
    await t.verify_lot_attempts()                # the periodic check confirms both orders
    await asyncio.sleep(0.3)
    close(t, 2)
    await wait_for(lambda: last_close(t, 2) and last_close(t, 2)["ok"], timeout=10)
    await asyncio.sleep(0.5)
    assert amt(fake) == 0                        # bot 2's whole 0.02 must be closed


# F4: an open whose response is lost while the user stream is down; a close of another lot credits the new quantity as
# "unassigned" and the final non-definitive check of the unknown open then sees nothing new: lot 2 is never booked.
async def test_unknown_open_is_booked_even_if_a_close_of_another_lot_saw_it_first(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 3, 0.01)
    trade = await opened(t, fake, 0.02)
    fake.inject_after.append(TIMEOUT)
    down = status_down(fake, order_events=True)  # WS API flaky: response lost, order.status down, order event lost
    open_(t, 2, 0.01)                            # executes on Binance
    await wait_for(lambda: amt(fake) == 0.03 and (SYM, "LONG") in t._reconciling, timeout=8)
    close(t, 1)                                  # bot 1 closes meanwhile (sees +0.01 it cannot explain)
    await wait_for(lambda: last_close(t, 1), timeout=10)
    await wait_for(lambda: (SYM, "LONG") not in t._reconciling, timeout=15)
    down["on"] = False
    close(t, 2)                                  # bot 2 closes its trade_id
    await wait_for(lambda: last_close(t, 2), timeout=10)
    await asyncio.sleep(0.5)
    assert amt(fake) == 0.01                     # only bot 3's 0.01 must remain


# F3 (variant): the TP of lot 1 fires while the executor is restarting (deploy); trades.json is intact.


# F7: first open of a symbol with a lost response; the bot closes it before the reconciliation check: the check then
# registers a phantom trade (Binance is flat) that later shrinks another bot's lot.
async def test_unknown_first_open_closed_before_its_reconciliation_leaves_no_phantom(service, fake):
    t = service.trades
    fake.inject_after.append(TIMEOUT)
    down = status_down(fake)                     # order.status down for the submit's own check
    open_(t, 5, 0.01)                            # executes on Binance; the executor does not know yet
    await wait_for(lambda: amt(fake) == 0.01 and (SYM, "LONG") in t._reconciling, timeout=8)
    down["on"] = False
    close(t, 5)                                  # the bot closes quickly (scalp / reversed signal)
    await wait_for(lambda: last_close(t, 5), timeout=8)
    await wait_for(lambda: amt(fake) == 0, timeout=5)
    await wait_for(lambda: (SYM, "LONG") not in t._reconciling, timeout=10)
    phantom = t.get_trade(SYM, "LONG")
    open_(t, 7, 0.01)                            # another bot opens later
    await wait_for(lambda: amt(fake) == 0.01, timeout=5)
    await asyncio.sleep(0.3)
    close(t, 7)
    await wait_for(lambda: last_close(t, 7), timeout=10)
    await asyncio.sleep(0.5)
    assert phantom is None and amt(fake) == 0


# F8: the close of lot 1 executes but its response is lost; lot 1's TP stays live on Binance until the close is
# confirmed and then fires on bot 2's quantity.
async def test_tp_of_a_lot_whose_close_is_unconfirmed_never_closes_another_lot(service, fake):
    from tests.test_lots_ledger import lost_close
    t = service.trades
    open_(t, 1, 0.02, tp=3100)                    # bot A alone: TP 0.02
    await opened(t, fake, 0.02)
    await wait_for(lambda: fake.algos)
    open_(t, 2, 0.01)                             # bot B
    trade = await opened(t, fake, 0.03)
    down = await lost_close(t, fake, 1, order_events=True)   # A's close executes; response + confirmation lost
    assert amt(fake) == 0.01
    fake.set_mark(SYM, 3100.0)                    # price reaches A's old TP level
    await asyncio.sleep(1.0)
    down["on"] = False
    assert amt(fake) == 0.01                      # B's 0.01 must still be open


async def test_tp_of_a_lot_cannot_fire_between_its_close_and_the_cancel(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3100)                    # bot A alone: TP 0.02 at 3100
    await opened(t, fake, 0.02)
    await wait_for(lambda: fake.algos)
    open_(t, 2, 0.02)                             # bot B
    trade = await opened(t, fake, 0.04)
    place, state = fake._place, {"done": False}

    def hooked(p):
        res = place(p)
        if not state["done"] and str(p.get("newClientOrderId", "")).startswith("K1") and fake._is_closing(p):
            state["done"] = True
            fake.set_mark(SYM, 3100.0)            # the mark reaches A's TP right after A's own close filled
        return res
    fake._place = hooked
    close(t, 1, close_price=3100)                 # bot A closes at its TP level (its own logic)
    await wait_for(lambda: last_close(t, 1), timeout=10)
    await asyncio.sleep(1.0)
    assert amt(fake) == 0.02                      # B's 0.02 must still be open


# F10: a PARTIAL close (quantity) whose order executes but loses its response; the automatic retry first infers the
# gap to that order and then still sends the full partial quantity again: the trade_id is reduced twice.
async def test_retried_partial_close_after_a_lost_response_is_not_sent_twice(service, fake):
    t = service.trades
    open_(t, 1, 0.03)
    await opened(t, fake, 0.03)
    open_(t, 2, 0.01)
    trade = await opened(t, fake, 0.04)
    fake.inject_after.append(TIMEOUT)
    down = status_down(fake, order_events=True)
    close(t, 1, quantity=0.01)                    # bot 1 takes a third of its trade
    await wait_for(lambda: last_close(t, 1), timeout=15)
    await asyncio.sleep(0.5)
    down["on"] = False
    await t.verify_lot_attempts()
    await asyncio.sleep(0.3)
    assert amt(fake) == 0.03 and lots(trade) == {"1": 0.02, "2": 0.01}


# F11: a tranche whose open response is lost is reconciled AFTER the bot already closed its trade_id: the reconciled
# quantity is credited back to the closed lot and nobody ever closes it.
async def test_unknown_tranche_reconciled_after_its_close_is_closed_too(service, fake):
    t = service.trades
    open_(t, 9, 0.01)                             # another bot
    await opened(t, fake, 0.01)
    open_(t, 5, 0.01, level=0)
    trade = await opened(t, fake, 0.02)
    fake.inject_after.append(TIMEOUT)
    down = status_down(fake, order_events=True)
    open_(t, 5, 0.01, level=25)                   # 2nd tranche executes; response, status and event lost
    await wait_for(lambda: amt(fake) == 0.03 and (SYM, "LONG") in t._reconciling, timeout=8)
    close(t, 5)                                   # the bot closes trade 5 (both tranches) right away
    await wait_for(lambda: last_close(t, 5), timeout=10)
    down["on"] = False                            # order.status is back before the last check
    await wait_for(lambda: (SYM, "LONG") not in t._reconciling, timeout=10)
    await asyncio.sleep(0.5)
    assert amt(fake) == 0.01 and lots(trade) == {"9": 0.01}
