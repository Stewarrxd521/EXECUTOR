"""Cierre por trade_id con varios bots: carreras, eventos atrasados, respuestas perdidas y TP/SL."""

import asyncio
from decimal import Decimal

import pytest

import executor.trading as trading_mod
from tests.conftest import wait_for
from tests.fake_binance import _Err
from tests.test_lots import SYM, amt, close, lots, open_, opened

ERR1001 = ("order.place", -1001, "Internal error; unable to process your request.")
STATUS_DOWN = ("order.status", -1001, "Internal error; unable to process your request.")


@pytest.fixture(autouse=True)
def quick_retries(monkeypatch):
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0, 0.5, 1.0))


def lag_account_updates(fake, delay):
    """El user stream llega más tarde que las respuestas de la WS API (conexiones distintas)."""
    orig = fake._push

    async def slow(event):
        if event.get("e") == "ACCOUNT_UPDATE":
            await asyncio.sleep(delay)
        await orig(event)
    fake._push = slow


def outside_order(fake, side, qty, cid):
    fake._place({"symbol": SYM, "side": side, "type": "MARKET", "positionSide": "LONG", "quantity": str(qty),
                 "newClientOrderId": cid})


def after_open_of(fake, tid, action):
    """Ejecuta ``action`` justo al colocar la orden de apertura del trade_id ``tid``."""
    orig = fake._place
    state = {"done": False}

    def place(p):
        if not state["done"] and str(p.get("newClientOrderId", "")).startswith(f"X{tid}") \
                and not fake._is_closing(p):
            state["done"] = True
            action()
        return orig(p)
    fake._place = place


# ── Eventos del user stream atrasados o desordenados ─────────────────────────
@pytest.mark.parametrize("lag", [0.15, 0.4])
async def test_lagging_stream_never_shrinks_a_lot(service, fake, lag):
    t = service.trades
    lag_account_updates(fake, lag)
    for level in (0, 25, 50):                    # app_25: 3 tramos del mismo trade_id en el mismo segundo
        open_(t, 7, 0.01, level=level)
    await wait_for(lambda: amt(fake) == 0.03)
    await asyncio.sleep(lag * 3 + 0.3)
    close(t, 7)
    await wait_for(lambda: amt(fake) == 0)


async def test_lagging_stream_with_two_bots_closes_both_fully(service, fake):
    t = service.trades
    lag_account_updates(fake, 0.4)
    open_(t, 1, 0.01)
    open_(t, 2, 0.02)
    await wait_for(lambda: amt(fake) == 0.03)
    await asyncio.sleep(1.5)
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0.02)
    close(t, 2)
    await wait_for(lambda: amt(fake) == 0)


async def test_outside_close_just_before_an_open_is_charged_to_the_older_lot(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    trade = await opened(t, fake, 0.02)
    after_open_of(fake, 2, lambda: outside_order(fake, "SELL", 0.01, "adl_autoclose-1"))
    open_(t, 2, 0.03)
    await wait_for(lambda: amt(fake) == 0.04)
    await wait_for(lambda: lots(trade) == {"1": 0.01, "2": 0.03})
    close(t, 1)
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 1)
    assert amt(fake) == 0.03


# ── Respuestas perdidas y reintentos ──────────────────────────────────────────
async def test_deferred_close_with_a_transient_error_is_retried(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    close(t, 2)                                  # su apertura llega después
    await wait_for(lambda: any(k[0] == 2 for k in t._pending_close))
    after_open_of(fake, 2, lambda: fake.inject.extend([ERR1001] * 7))
    open_(t, 2, 0.02)
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 2, timeout=10)
    assert amt(fake) == 0.01


async def test_lost_response_of_one_lot_is_settled_before_another_lot_closes(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.03)
    fake.inject_after.append(("order.place", -1007, "Timeout waiting for response from backend server."))
    fake.inject.extend([STATUS_DOWN] * 3)
    close(t, 2)                                  # se ejecuta; respuesta y confirmación perdidas
    await wait_for(lambda: amt(fake) == 0.01)
    await wait_for(lambda: (trade.key, "2") in t._lot_attempts)
    close(t, 1)                                  # otro bot cierra antes de que se confirme
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None, timeout=10)
    assert sorted((x.paper_trade_id, round(x.quantity, 6)) for x in t.closed) == [(1, 0.01), (2, 0.02)]


async def test_unconfirmed_close_is_settled_by_the_periodic_check(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.04)
    fake.inject_after.append(("order.place", -1007, "Timeout waiting for response from backend server."))
    down = {"on": True}                          # order.status caído durante todos los reintentos
    orig = fake._dispatch

    def dispatch(method, p):
        if method == "order.status" and down["on"]:
            raise _Err(-1001, "Internal error; unable to process your request.")
        return orig(method, p)

    fake._dispatch = dispatch
    close(t, 1)
    await wait_for(lambda: t.signals(1, trade_id=1) and not t.signals(1, trade_id=1)[0]["ok"], timeout=10)
    assert amt(fake) == 0.02 and (trade.key, "1") in t._lot_attempts
    down["on"] = False
    await t.verify_lot_attempts()                # lo que hace el servicio cada 15 s
    assert lots(trade) == {"2": 0.02} and [(x.paper_trade_id, x.quantity) for x in t.closed] == [(1, 0.02)]
    close(t, 2)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_position_without_local_trade_is_retried_after_a_transient_error(service, fake):
    t = service.trades
    outside_order(fake, "BUY", 0.02, "web_outside")   # p. ej. reinicio sin trades.json
    await wait_for(lambda: service.core.account.position(SYM, "LONG") is not None)
    fake.inject.extend([ERR1001] * 7)
    close(t)
    await wait_for(lambda: amt(fake) == 0, timeout=10)


def thin_book(fake, partials=10):
    """Cada MARKET de cierre da -4131 y cada LIMIT IOC se llena solo a medias."""
    orig = fake._place
    state = {"n": 0}

    def place(p):
        if fake._is_closing(p) and state["n"] < partials:
            if p["type"] == "MARKET":
                raise _Err(-4131, "The counterparty's best price does not meet the PERCENT_PRICE filter limit.")
            if p["type"] == "LIMIT" and p.get("timeInForce") == "IOC":
                state["n"] += 1
                q = Decimal(p["quantity"])
                half = max(Decimal("0.001"), (q / 2).quantize(Decimal("0.001")))
                res = orig({**p, "quantity": str(half)})
                res.update({"origQty": str(q), "status": "EXPIRED"})
                return res
        return orig(p)
    fake._place = place


async def test_repeated_partial_fills_complete_the_lot(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.01)
    trade = await opened(t, fake, 0.03)
    thin_book(fake)
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0.01, timeout=10)
    assert lots(trade) == {"2": 0.01}
    assert abs(sum(x.quantity for x in t.closed if x.paper_trade_id == 1) - 0.02) < 1e-9


# ── Identidad de bot ─────────────────────────────────────────────────────────
async def test_bot_close_without_direction_finds_its_own_position(service, fake):
    t = service.trades
    open_(t, 1, 0.01, "LONG", bot="kline")
    await opened(t, fake, 0.01, "LONG")
    open_(t, 1, 0.02, "SHORT", bot="app25")      # mismo número, otro bot, otra dirección
    await opened(t, fake, 0.02, "SHORT")
    t.handle_signal({"action": "close", "trade_id": 1, "symbol": SYM, "bot": "kline", "reason": "TP"})
    await wait_for(lambda: amt(fake, "LONG") == 0)
    assert amt(fake, "SHORT") == 0.02
    t.handle_signal({"action": "close", "trade_id": 1, "bot": "app25", "reason": "TP"})   # ni symbol ni direction
    await wait_for(lambda: amt(fake, "SHORT") == 0)


# ── Lo que queda sin lote, aperturas asumidas y conciliadas ──────────────────
async def test_quantity_left_by_another_bot_is_never_closed_by_a_trade_id(service, fake):
    t = service.trades
    open_(t, 21, 0.01)
    trade = await opened(t, fake, 0.01)
    outside_order(fake, "BUY", 0.02, "web_otherbot")  # otro bot abre por fuera del executor
    await wait_for(lambda: amt(fake) == 0.03)
    close(t, 21)
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 21)
    assert amt(fake) == 0.02 and t.get_trade(SYM, "LONG") is trade
    close(t, 21)                                 # el mismo close repetido
    close(t, 99)                                 # un trade_id desconocido
    await wait_for(lambda: len([s for s in t.signals(5) if s["action"] == "close"]) >= 3)
    await asyncio.sleep(0.3)
    assert amt(fake) == 0.02 and len(t.closed) == 1


async def test_assumed_lot_survives_the_last_real_lot(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    trade = await opened(t, fake, 0.01)
    fake.inject.append(("order.place", -2019, "Margin is insufficient."))
    open_(t, 2, 0.01)
    await wait_for(lambda: "2" in trade.assumed_lots)
    close(t, 1, pnl=0.3)
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 1)
    assert amt(fake) == 0 and trade.order_assumed and t.get_trade(SYM, "LONG") is trade
    close(t, 2, pnl=-0.2)
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 2)
    assert t.signals(1, trade_id=2)[0]["ok"] and t.get_trade(SYM, "LONG") is None


async def test_reconciled_lot_keeps_its_own_multiplier_and_entry(service, fake, monkeypatch):
    monkeypatch.setattr(trading_mod, "UNKNOWN_OPEN_CHECKS", (0.5, 1.0, 2.0))
    t = service.trades
    open_(t, 2, 0.01)                            # x1 a 3000
    trade = await opened(t, fake, 0.01)
    fake.set_mark(SYM, 3200.0)
    await asyncio.sleep(0.3)
    service.multiplier.configure({"enabled": True, "mode": "manual", "factor": 2})
    fake.inject_after.append(("order.place", -1007, "Timeout waiting for response from backend server."))
    fake.inject.extend([STATUS_DOWN] * 3)
    open_(t, 1, 0.01)                            # x2 a 3200; la respuesta se pierde y se concilia
    await wait_for(lambda: "1" in trade.lots and lots(trade)["2"] == 0.01, timeout=8)
    real1 = trade.lots["1"]                      # ≈ 0.02 (×2), redondeada al step y al notional
    assert abs(trade.lot_multiplier("1") - real1 / 0.01) < 1e-9 and abs(trade.lot_entry("1") - 3200) < 1
    close(t, 1, quantity=0.01)                   # toda la cantidad del bot 1, en unidades del bot
    await wait_for(lambda: amt(fake) == 0.01)
    assert lots(trade) == {"2": 0.01}


# ── TP/SL de un trade_id ─────────────────────────────────────────────────────
async def test_tp_fill_is_charged_to_the_trade_id_that_placed_it(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3100)                   # solo: su TP cubre 0.02
    await opened(t, fake, 0.02)
    await wait_for(lambda: fake.algos)
    open_(t, 2, 0.01)                            # otro bot se suma después
    trade = await opened(t, fake, 0.03)
    fake.set_mark(SYM, 3100.0)                   # salta el TP del bot 1
    await wait_for(lambda: amt(fake) == 0.01)
    await wait_for(lambda: lots(trade) == {"2": 0.01})
    close(t, 1, close_price=3100)                # el bot 1 envía su close después
    await wait_for(lambda: t.signals(1, trade_id=1) and t.signals(1, trade_id=1)[0]["action"] == "close")
    assert "ya estaba cerrado" in t.signals(1, trade_id=1)[0]["detail"] and amt(fake) == 0.01


async def test_closing_a_trade_id_cancels_its_own_tp(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3300)
    await opened(t, fake, 0.02)
    await wait_for(lambda: fake.algos)
    open_(t, 2, 0.01)
    await opened(t, fake, 0.03)
    close(t, 1)                                  # su TP de 0.02 ya no debe quedar vivo
    await wait_for(lambda: amt(fake) == 0.01)
    await wait_for(lambda: not fake.algos)
    fake.set_mark(SYM, 3300.0)
    await asyncio.sleep(0.5)
    assert amt(fake) == 0.01
