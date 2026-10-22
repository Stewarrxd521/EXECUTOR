"""Libro de fills por trade_id: reinicios, sincronizaciones, eventos desordenados, cierres de fuera y TP/SL."""

import asyncio
import json
from decimal import Decimal

import pytest

import executor.trading as trading_mod
from executor.service import ExecutorService
from tests.conftest import make_settings, wait_for
from tests.fake_binance import _Err
from tests.test_lots import SYM, amt, close, lots, open_, opened

TIMEOUT = ("order.place", -1007, "Timeout waiting for response from backend server.")
ERR1001 = ("order.place", -1001, "Internal error; unable to process your request.")
THIN = (-4131, "The counterparty's best price does not meet the PERCENT_PRICE filter limit.")


@pytest.fixture(autouse=True)
def quick_retries(monkeypatch):
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0, 0.5, 1.0))
    monkeypatch.setattr(trading_mod, "UNKNOWN_OPEN_CHECKS", (1.0, 2.0, 3.0))


def status_down(fake, order_events=False):
    """order.status no responde (y, con ``order_events``, el evento final de la orden tampoco llega)."""
    down = {"on": True}
    dispatch, push = fake._dispatch, fake._push

    def failing(method, p):
        if method == "order.status" and down["on"]:
            raise _Err(-1001, "Internal error; unable to process your request.")
        return dispatch(method, p)

    async def filtered(event):
        if not (order_events and down["on"] and down.get("events", True) and event.get("e") == "ORDER_TRADE_UPDATE"):
            await push(event)
    fake._dispatch, fake._push = failing, filtered
    return down


def lag(fake, delay, kind=None):
    """Retrasa los eventos del user stream (todos en orden, o solo los de tipo ``kind``)."""
    push = fake._push

    async def slow(event):
        if kind is None or event.get("e") == kind:
            await asyncio.sleep(delay)
        await push(event)
    fake._push = slow


def outside(fake, side, qty, cid, direction="LONG"):
    fake._place({"symbol": SYM, "side": side, "type": "MARKET", "positionSide": direction, "quantity": str(qty),
                 "newClientOrderId": cid})


def when_open_of(fake, tid, action):
    """Ejecuta ``action`` justo antes de que Binance llene la apertura del trade_id ``tid``."""
    place, state = fake._place, {"done": False}

    def hooked(p):
        if not state["done"] and str(p.get("newClientOrderId", "")).startswith(f"X{tid}") \
                and not fake._is_closing(p):
            state["done"] = True
            action()
        return place(p)
    fake._place = hooked


def thin_book(fake, partials):
    """Cada MARKET de cierre da -4131 y cada LIMIT IOC se llena solo a medias."""
    place, state = fake._place, {"n": 0}

    def hooked(p):
        if fake._is_closing(p) and state["n"] < partials:
            if p["type"] == "MARKET":
                raise _Err(*THIN)
            if p["type"] == "LIMIT" and p.get("timeInForce") == "IOC":
                state["n"] += 1
                q = Decimal(p["quantity"])
                res = place({**p, "quantity": str(max(Decimal("0.001"), (q / 2).quantize(Decimal("0.001"))))})
                res.update({"origQty": str(q), "status": "EXPIRED"})
                return res
        return place(p)
    fake._place = hooked


def last_close(t, tid):
    sig = t.signals(1, trade_id=tid)
    return sig[0] if sig and sig[0]["action"] == "close" else None


async def lost_close(t, fake, tid, order_events=False):
    """Cierre del trade_id que se ejecuta en Binance pero cuya respuesta y confirmación se pierden."""
    fake.inject_after.append(TIMEOUT)
    down = status_down(fake, order_events)
    close(t, tid)
    await wait_for(lambda: last_close(t, tid) and not last_close(t, tid)["ok"], timeout=10)
    return down


async def start(fake, tmp_path, trades_json=None):
    if trades_json is not None:
        data = tmp_path / "data"
        data.mkdir(parents=True, exist_ok=True)
        (data / "trades.json").write_text(json.dumps(trades_json))
    svc = ExecutorService(make_settings(fake, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.user.conn.connected and svc.trades.ready.is_set())
    return svc


# ── Cierres sin confirmar: reinicio, sincronización y cierre total ───────────
async def test_restart_keeps_an_unconfirmed_lot_close(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.02)
        await opened(t, fake, 0.02)
        open_(t, 2, 0.02)
        trade = await opened(t, fake, 0.04)
        down = await lost_close(t, fake, 1, order_events=True)
        assert amt(fake) == 0.02 and (trade.key, "1") in t._lot_attempts
    finally:
        await svc.stop()                          # deploy antes de la verificación de cada 15 s
    down["on"] = False
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        trade = t.get_trade(SYM, "LONG")
        assert (trade.key, "1") in t._lot_attempts
        await wait_for(lambda: lots(trade) == {"2": 0.02})
        close(t, 2)
        await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
        await wait_for(lambda: len(t.closed) == 2)
        assert sorted((r.paper_trade_id, round(r.quantity, 6)) for r in t.closed) == [(1, 0.02), (2, 0.02)]
        assert not t._lot_attempts
    finally:
        await svc.stop()


async def test_account_sync_before_the_lost_close_is_settled(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.04)
    down = await lost_close(t, fake, 1, order_events=True)
    await service.sync_account("user stream conectado")
    await asyncio.sleep(2.5)                      # conciliación tras la sincronización
    down["on"] = False
    await t.verify_lot_attempts()
    assert lots(trade) == {"2": 0.02}
    close(t, 2)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_account_sync_before_the_bots_own_retry(service, fake, monkeypatch):
    monkeypatch.setattr(trading_mod, "CLOSE_RETRY_DELAYS", (0.0, 2.0, 4.0))
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.04)
    fake.inject_after.append(TIMEOUT)
    down = status_down(fake, order_events=True)
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0.02 and (trade.key, "1") in t._lot_attempts
                   and not t._lot_attempts[(trade.key, "1")]["active"], timeout=5)
    await service.sync_account("user stream conectado")
    down["on"] = False                            # vuelve antes del reintento automático
    await wait_for(lambda: (trade.key, "1") not in t._lot_attempts, timeout=10)
    assert lots(trade) == {"2": 0.02}
    close(t, 2)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_lost_lot_close_then_whole_close_books_what_binance_realized(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.04)
    fake.set_mark(SYM, 3100.0)                    # cada cierre realiza PnL
    await asyncio.sleep(0.3)
    down = await lost_close(t, fake, 1, order_events=True)
    down["events"] = False                        # los eventos del cierre total sí llegan
    close(t)                                      # sin trade_id: toda la posición
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
    down["on"] = False
    await t.verify_lot_attempts()
    await asyncio.sleep(0.3)
    assert not t._lot_attempts and not t._detached
    assert sorted((x.paper_trade_id, round(x.quantity, 6)) for x in t.closed) == [(1, 0.02), (2, 0.02)]
    lost_fee = 0.02 * 3100 * 0.0004               # su comisión solo llega en el evento que se perdió
    assert abs(t.realized_total - lost_fee - (fake.wallet - 10_000.0)) < 1e-6
    assert abs(t.realized_total - sum(x.pnl_usdt for x in t.closed)) < 1e-6


async def test_unconfirmed_close_of_a_closed_trade_survives_a_restart(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        open_(t, 1, 0.02)
        await opened(t, fake, 0.02)
        open_(t, 2, 0.02)
        await opened(t, fake, 0.04)
        down = await lost_close(t, fake, 1, order_events=True)
        down["events"] = False
        close(t)
        await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
        assert len(t._detached) == 1 and [x.paper_trade_id for x in t.closed] == [2]
    finally:
        await svc.stop()
    down["on"] = False
    svc = await start(fake, tmp_path)
    try:
        t = svc.trades
        assert len(t._detached) == 1
        await t.verify_lot_attempts()
        assert not t._detached
        assert sorted((x.paper_trade_id, round(x.quantity, 6)) for x in t.closed) == [(1, 0.02), (2, 0.02)]
        assert abs(t.realized_total - sum(x.pnl_usdt for x in t.closed)) < 1e-6
    finally:
        await svc.stop()


async def test_stale_attempt_is_not_applied_to_a_new_trade(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.04)
    down = await lost_close(t, fake, 1, order_events=True)
    close(t)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
    open_(t, 3, 0.03)                             # otro bot vuelve a abrir el mismo símbolo y dirección
    new = await opened(t, fake, 0.03)
    before = (new.bot_quantity, new.fees_usdt, new.pnl_adjust, lots(new))
    down["on"] = False
    await t.verify_lot_attempts()
    assert (new.bot_quantity, new.fees_usdt, new.pnl_adjust, lots(new)) == before and lots(new) == {"3": 0.03}


async def test_close_of_another_bot_waits_for_an_unconfirmed_close(service, fake):
    t = service.trades
    open_(t, 1, 0.02, bot="kline")
    await opened(t, fake, 0.02)
    open_(t, 2, 0.01, bot="app25")
    await opened(t, fake, 0.03)
    fake.inject_after.append(TIMEOUT)
    down = status_down(fake, order_events=True)
    close(t, 1, bot="kline")
    await asyncio.sleep(0.2)
    close(t, 2, bot="app25")                      # otro bot cierra su trade_id mientras tanto
    await wait_for(lambda: last_close(t, 2) and last_close(t, 1), timeout=15)
    down["on"] = False
    await t.verify_lot_attempts()
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None, timeout=20)


async def test_lost_response_of_a_partially_filled_ioc_close(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.04)
    place, state = fake._place, {"market": 0, "ioc": 0}

    def hooked(p):
        if fake._is_closing(p) and p["type"] == "MARKET" and not state["market"]:
            state["market"] = 1
            raise _Err(*THIN)
        if fake._is_closing(p) and p["type"] == "LIMIT" and p.get("timeInForce") == "IOC" and not state["ioc"]:
            state["ioc"] = 1                      # la IOC se llena a medias, expira y su respuesta se pierde
            q = Decimal(p["quantity"])
            res = place({**p, "quantity": str((q / 2).quantize(Decimal("0.001")))})
            fake.done_orders[res["orderId"]].update({"origQty": float(q), "status": "EXPIRED"})
            fake.inject_after.append(TIMEOUT)
            return {**res, "origQty": str(q), "status": "EXPIRED"}
        return place(p)
    fake._place = hooked
    close(t, 1)
    await wait_for(lambda: last_close(t, 1), timeout=10)
    await asyncio.sleep(0.5)
    assert amt(fake) == 0.02 and lots(trade) == {"2": 0.02}


# ── Aperturas con estado desconocido, posiciones asumidas e identidad del bot ─
async def test_close_right_after_an_unknown_tranche_closes_it_too(service, fake):
    t = service.trades
    open_(t, 5, 0.01, level=0)
    await opened(t, fake, 0.01)
    fake.inject_after.append(TIMEOUT)
    down = status_down(fake)
    open_(t, 5, 0.01, level=25)                   # se ejecuta en Binance; el executor no lo sabe aún
    await wait_for(lambda: amt(fake) == 0.02 and any("desconocido" in s["detail"] for s in t.signals(5)), timeout=8)
    down["on"] = False
    close(t, 5)                                   # el bot cierra antes de la conciliación (1 s)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None, timeout=10)


async def test_deferred_closes_of_the_same_trade_id_on_two_symbols(service, fake):
    t = service.trades

    def pos(sym):
        return round(abs(fake.positions.get((sym, "LONG"), {"amt": 0.0})["amt"]), 10)

    def sig(action, sym, qty=None):
        d = {"action": action, "trade_id": 5, "symbol": sym, "direction": "LONG", "reason": "TP"}
        if qty:
            d.update(quantity=qty, price={"ETHUSDT": 3000.0, "GRIDUSDT": 100.0}[sym])
        t.handle_signal(d)

    down = status_down(fake)
    down["on"] = False
    sig("close", "GRIDUSDT")                      # el cierre llega antes que su apertura: queda pendiente
    await asyncio.sleep(0.3)
    down["on"] = True
    fake.inject_after.append(TIMEOUT)
    sig("open", "GRIDUSDT", 0.2)
    await wait_for(lambda: pos("GRIDUSDT") == 0.2 and ("GRIDUSDT", "LONG") in t._reconciling, timeout=8)
    fake.inject_after.append(TIMEOUT)             # el cierre diferido se ejecuta y pierde su respuesta
    await wait_for(lambda: pos("GRIDUSDT") == 0, timeout=8)
    await asyncio.sleep(2.5)
    down["on"] = False
    await t.verify_lot_attempts()
    sig("close", "ETHUSDT")                       # otro bot, su propio trade_id 5 en ETHUSDT
    await asyncio.sleep(0.3)
    down["on"] = True
    fake.inject_after.append(TIMEOUT)
    sig("open", "ETHUSDT", 0.02)
    await wait_for(lambda: pos("ETHUSDT") > 0 and ("ETHUSDT", "LONG") in t._reconciling, timeout=8)
    down["on"] = False
    await wait_for(lambda: pos("ETHUSDT") == 0, timeout=10)


LEGACY_ASSUMED = {"id": 5, "symbol": SYM, "direction": "LONG", "entry_price": 3000.0, "quantity": 0.02,
                  "open_time": "2026-10-19 10:00:00 UTC", "leverage": 5, "paper_trade_id": 8, "paper_ids": [8],
                  "order_assumed": True, "entry_order_id": "MARGIN_INSUFFICIENT", "status": "OPEN",
                  "tranches": [{"level": 0, "trade_id": 8, "qty": 0.02, "fill_price": 3000.0, "assumed": True}],
                  "bot_quantity": 0.02, "multiplier": 1.0}


async def test_legacy_assumed_trade_is_closed_by_its_bot(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path, {"counter": 5, "open": [dict(LEGACY_ASSUMED)], "closed": []})
    try:
        t = svc.trades
        trade = t.get_trade(SYM, "LONG")
        assert trade.assumed_lots == {"8": 0.02} and not trade.lots
        close(t, 8)
        await wait_for(lambda: t.get_trade(SYM, "LONG") is None)
    finally:
        await svc.stop()


async def test_legacy_assumed_lot_never_shrinks_a_new_real_lot(fake, tmp_path, snapshot_file):
    svc = await start(fake, tmp_path, {"counter": 5, "open": [dict(LEGACY_ASSUMED)], "closed": []})
    try:
        t = svc.trades
        open_(t, 9, 0.02)                         # vuelve el margen: el bot abre de verdad
        trade = await opened(t, fake, 0.02)
        assert lots(trade) == {"9": 0.02}
        close(t, 9)
        await wait_for(lambda: amt(fake) == 0)
    finally:
        await svc.stop()


async def test_assumed_trade_ids_sl_is_cancelled_by_its_close(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    trade = await opened(t, fake, 0.02)
    fake.inject.append(("order.place", -2019, "Margin is insufficient."))
    open_(t, 2, 0.01, sl=2900)                    # asumida (-2019): no coloca SL sobre la posición compartida
    await wait_for(lambda: "2" in trade.assumed_lots)
    await asyncio.sleep(0.5)
    assert not fake.algos
    close(t, 2)
    await wait_for(lambda: "2" not in trade.assumed_lots)
    fake.set_mark(SYM, 2890.0)
    await asyncio.sleep(0.8)
    assert amt(fake) == 0.02


async def test_close_after_switching_to_the_bot_url(service, fake):
    t = service.trades
    open_(t, 5, 0.02)                             # abierta con la URL sin /b/<bot>: lote "5"
    await opened(t, fake, 0.02)
    close(t, 5, bot="kline")                      # el mismo bot ya con EXECUTOR_URL=.../b/kline
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


# ── Reducciones de fuera (otros bots, manuales, ADL) ─────────────────────────
@pytest.mark.parametrize("account_first", [False, True])
async def test_outside_add_then_outside_close_leaves_the_lot_whole(service, fake, account_first):
    t = service.trades
    open_(t, 1, 0.02)
    trade = await opened(t, fake, 0.02)
    outside(fake, "BUY", 0.02, "web_manual_add")  # alguien suma 0.02 a mano (sin trade_id)
    await wait_for(lambda: abs(trade.quantity - 0.04) < 1e-9 and trade.unassigned_qty() > 0.019)
    if account_first:
        lag(fake, 0.15, "ORDER_TRADE_UPDATE")
    outside(fake, "SELL", 0.02, "web_manual_close")   # ... y luego cierra 0.02 a mano
    await wait_for(lambda: abs(trade.quantity - 0.02) < 1e-9)
    await asyncio.sleep(0.5)
    assert lots(trade) == {"1": 0.02} and trade.unassigned_qty() < 1e-9
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_other_bot_closing_its_own_outside_quantity(service, fake):
    t = service.trades
    open_(t, 1, 0.01, bot="kline")
    trade = await opened(t, fake, 0.01)
    outside(fake, "BUY", 0.01, "web_other1")
    await wait_for(lambda: abs(trade.quantity - 0.02) < 1e-9)
    outside(fake, "SELL", 0.01, "web_other2")
    await wait_for(lambda: abs(trade.quantity - 0.01) < 1e-9)
    close(t, 1, bot="kline")
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_account_update_before_order_update_of_an_outside_close(service, fake):
    t = service.trades
    open_(t, 1, 0.01, bot="kline")
    await opened(t, fake, 0.01)
    open_(t, 2, 0.01, bot="app25")
    trade = await opened(t, fake, 0.02)
    outside(fake, "BUY", 0.02, "web_other1")
    await wait_for(lambda: abs(trade.quantity - 0.04) < 1e-9)
    lag(fake, 0.3, "ORDER_TRADE_UPDATE")
    outside(fake, "SELL", 0.02, "web_other2")     # el otro bot cierra sus 0.02
    await asyncio.sleep(1.0)
    assert lots(trade) == {"kline:1": 0.01, "app25:2": 0.01}
    close(t, 1, bot="kline")
    close(t, 2, bot="app25")
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_outside_fill_while_another_open_holds_the_lock(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    trade = await opened(t, fake, 0.01)
    when_open_of(fake, 2, lambda: outside(fake, "BUY", 0.01, "web_other1"))
    open_(t, 2, 0.01)
    await wait_for(lambda: amt(fake) == 0.03)
    await wait_for(lambda: abs(trade.quantity - 0.03) < 1e-9)
    assert lots(trade) == {"1": 0.01, "2": 0.01} and abs(trade.unassigned_qty() - 0.01) < 1e-9


async def test_lagged_outside_fill_after_a_lot_close_is_charged_once(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.01)
    trade = await opened(t, fake, 0.02)
    lag(fake, 0.5)
    outside(fake, "SELL", 0.01, "adl_autoclose-1")   # ADL: reparto proporcional
    close(t, 1)
    await wait_for(lambda: last_close(t, 1), timeout=10)
    await asyncio.sleep(1.5)
    assert amt(fake) == 0.005 and lots(trade) == {"2": 0.005}
    close(t, 2)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_position_closed_outside_while_the_symbol_is_locked(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    fake.inject.extend([ERR1001] * 4)             # la siguiente apertura se reintenta ~3 s con el lock tomado
    open_(t, 2, 0.01, direction="SHORT")
    await asyncio.sleep(0.2)
    outside(fake, "SELL", 0.01, "web_manual1")    # el LONG se cierra a mano en la app de Binance
    await wait_for(lambda: amt(fake, "SHORT") == 0.01, timeout=10)
    await wait_for(lambda: t.get_trade(SYM, "LONG") is None, timeout=10)
    assert any(r.paper_trade_id == 1 and r.status == "EXTERNAL" for r in t.closed)


async def test_close_without_trade_id_on_a_thin_book(service, fake):
    t = service.trades
    open_(t, 1, 0.04)
    await opened(t, fake, 0.04)
    thin_book(fake, 2)
    close(t)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
    assert abs(sum(r.quantity for r in t.closed) - 0.04) < 1e-9


# ── TP/SL de cada trade_id ───────────────────────────────────────────────────
async def test_tp_of_a_lot_emptied_outside_is_cancelled(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3100)
    trade = await opened(t, fake, 0.02)
    await wait_for(lambda: fake.algos)
    when_open_of(fake, 2, lambda: outside(fake, "SELL", 0.02, "adl_autoclose-1"))
    open_(t, 2, 0.03)
    await wait_for(lambda: amt(fake) == 0.03)
    await wait_for(lambda: lots(trade) == {"2": 0.03})
    close(t, 1)
    await wait_for(lambda: last_close(t, 1))
    await asyncio.sleep(0.5)
    fake.set_mark(SYM, 3100.0)
    await asyncio.sleep(1.0)
    assert amt(fake) == 0.03


@pytest.mark.parametrize("delay", [0.0, 0.05, 0.3])
async def test_close_racing_its_own_tp_fill(service, fake, delay):
    t = service.trades
    open_(t, 1, 0.02, tp=3100)
    await opened(t, fake, 0.02)
    await wait_for(lambda: fake.algos)
    open_(t, 2, 0.01)
    await opened(t, fake, 0.03)
    if delay:
        lag(fake, delay)
    fake.set_mark(SYM, 3100.0)                    # el TP del trade_id 1 se dispara (0.02)
    close(t, 1, close_price=3100)                 # y el bot 1 cierra a la vez
    await asyncio.sleep(2.0)
    assert amt(fake) == 0.01


async def test_other_bot_close_racing_a_tp_fill(service, fake):
    t = service.trades
    open_(t, 1, 0.02, tp=3100)
    await opened(t, fake, 0.02)
    await wait_for(lambda: fake.algos)
    open_(t, 2, 0.01)
    await opened(t, fake, 0.03)
    lag(fake, 0.1)
    fake.set_mark(SYM, 3100.0)
    close(t, 2, close_price=3100)
    await wait_for(lambda: amt(fake) == 0, timeout=10)


# ── Registros ────────────────────────────────────────────────────────────────
async def test_absorbed_parent_late_fill_keeps_realized_total(service, fake):
    t = service.trades
    lag(fake, 1.0)
    open_(t, 5, 0.01)
    await wait_for(lambda: t.get_trade(SYM, "LONG") is not None)
    close(t, 5)
    await wait_for(lambda: t.closed and t.get_trade(SYM, "LONG") is None, timeout=10)
    await asyncio.sleep(2.5)
    assert amt(fake) == 0
    assert abs(t.realized_total - sum(x.pnl_usdt for x in t.closed)) < 1e-6


async def test_last_lot_record_keeps_its_own_open_time(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    await asyncio.sleep(2.2)
    open_(t, 2, 0.01)
    trade = await opened(t, fake, 0.02)
    own_ts = min(x["ts"] for x in trade.tranches if x.get("trade_id") == 2)
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0.01)
    close(t, 2)
    await wait_for(lambda: amt(fake) == 0 and len(t.closed) == 2)
    assert abs(t.closed[-1].opened_ts - own_ts) < 1.0
