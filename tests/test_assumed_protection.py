"""TP/SL de las operaciones asumidas (-2019, margen insuficiente) y apertura a mano en Binance.

- Trade solo asumido: su TP/SL condicional se pone en Binance con la cantidad asumida (clientAlgoId de su
  trade_id). Sin posición, al dispararse no ejecuta nada; si se abre a mano, ya está protegido.
- Lote asumido dentro de una posición real de otros trade_id: el TP/SL se guarda pero no se pone (cerraría la
  cantidad de los otros); se pone cuando ese lote tenga cantidad real.
- Lo que se abre a mano en Binance sobre una posición con lotes asumidos pasa a ser de esos lotes (reales).
"""

import asyncio

from tests.conftest import wait_for
from tests.test_lots import SYM, amt, close, lots, open_, opened

MARGIN = ("order.place", -2019, "Margin is insufficient.")


def algos(fake, prefix=""):
    return [a for a in fake.algos.values() if a["clientAlgoId"].startswith(prefix)]


def tp(t, tid, trigger, direction="LONG"):
    t.handle_signal({"action": "open_tp", "trade_id": tid, "symbol": SYM, "direction": direction,
                     "trigger_price": trigger})


def sl(t, tid, trigger, direction="LONG"):
    t.handle_signal({"action": "open_sl", "trade_id": tid, "symbol": SYM, "direction": direction,
                     "trigger_price": trigger})


def manual_open(fake, qty, direction="LONG"):
    """Orden de mercado puesta a mano en Binance (sin clientOrderId del executor)."""
    fake._place({"symbol": SYM, "side": "BUY" if direction == "LONG" else "SELL", "type": "MARKET",
                 "positionSide": direction, "quantity": str(qty), "newClientOrderId": "web_manual"})


async def assumed_trade(t, fake, tid, qty, direction="LONG"):
    fake.inject.append(MARGIN)
    open_(t, tid, qty, direction=direction)
    await wait_for(lambda: (tr := t.get_trade(SYM, direction)) is not None
                   and tr.assumed_lots.get(str(tid), 0) > 0 and not t.inflight(SYM))
    return t.get_trade(SYM, direction)


async def test_assumed_trade_gets_its_tp_and_sl_on_binance(service, fake):
    t = service.trades
    trade = await assumed_trade(t, fake, 1, 0.01)
    tp(t, 1, 3100)
    sl(t, 1, 2900)
    await wait_for(lambda: len(algos(fake, "T1")) == 1 and len(algos(fake, "S1")) == 1)
    assert {a["quantity"] for a in algos(fake)} == {"0.01"}
    assert {a["positionSide"] for a in algos(fake)} == {"LONG"}
    assert trade.lot_prot == {"1": {"TP": 3100.0, "SL": 2900.0}}
    await wait_for(lambda: t.signals(5, trade_id=1)[0]["ok"] and t.signals(5, trade_id=1)[0]["action"] == "open_sl")


async def test_assumed_tp_triggering_without_a_position_does_nothing(service, fake):
    t = service.trades
    trade = await assumed_trade(t, fake, 1, 0.01)
    tp(t, 1, 3100)
    await wait_for(lambda: algos(fake, "T1"))
    fake.set_mark(SYM, 3100.0)
    await wait_for(lambda: not algos(fake, "T1"))
    await asyncio.sleep(0.3)
    assert amt(fake) == 0 and t.get_trade(SYM, "LONG") is trade and trade.order_assumed


async def test_manual_open_makes_the_assumed_trade_real_and_keeps_it_protected(service, fake):
    t = service.trades
    trade = await assumed_trade(t, fake, 1, 0.01)
    tp(t, 1, 3100)
    await wait_for(lambda: algos(fake, "T1"))
    manual_open(fake, 0.01)
    await wait_for(lambda: not trade.order_assumed)
    assert lots(trade) == {"1": 0.01} and trade.assumed_lots == {} and trade.unassigned == 0
    assert trade.quantity == 0.01 and trade.entry_price == 3000.0
    await asyncio.sleep(0.3)
    assert [a["quantity"] for a in algos(fake, "T1")] == ["0.01"]     # un solo TP, ahora sobre lo real
    fake.set_mark(SYM, 3100.0)                                         # el TP cierra lo abierto a mano
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None and t.closed, timeout=10)
    assert t.closed[-1].status == "TP" and not t.closed[-1].order_assumed


async def test_bot_close_after_a_manual_open_closes_it_for_real(service, fake):
    t = service.trades
    trade = await assumed_trade(t, fake, 1, 0.01)
    tp(t, 1, 3100)
    await wait_for(lambda: algos(fake, "T1"))
    manual_open(fake, 0.03)                                            # más de lo asumido
    await wait_for(lambda: not trade.order_assumed)
    assert lots(trade) == {"1": 0.01} and abs(trade.unassigned - 0.02) < 1e-12
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0.02 and not t.inflight(SYM) and t.closed)
    assert not algos(fake, "T1")                                       # su TP se cancela con su lote
    rec = t.closed[-1]
    assert rec.paper_trade_id == 1 and not rec.order_assumed and rec.quantity == 0.01


async def test_manual_open_not_seen_by_the_stream_is_found_at_close(service, fake):
    t = service.trades
    await assumed_trade(t, fake, 1, 0.01)
    fake.positions[(SYM, "LONG")] = {"amt": 0.01, "entry": 3000.0}     # sin evento (executor caído, corte…)
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None and t.closed)
    assert not t.closed[-1].order_assumed


async def test_assumed_lot_inside_a_real_position_waits_for_real_quantity(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    trade = await opened(t, fake, 0.01)
    tp(t, 1, 3200)
    await wait_for(lambda: algos(fake, "T1"))
    fake.inject.append(MARGIN)
    open_(t, 2, 0.02)
    await wait_for(lambda: trade.assumed_lots.get("2") == 0.02 and not t.inflight(SYM))
    tp(t, 2, 3100)
    await wait_for(lambda: trade.lot_prot.get("2") == {"TP": 3100.0})
    await asyncio.sleep(0.3)
    assert not algos(fake, "T2")                       # cerraría la cantidad del trade_id 1 al dispararse
    reply = t.signals(5, trade_id=2)[0]
    assert reply["ok"] and "asumido" in reply["detail"]
    manual_open(fake, 0.02)
    await wait_for(lambda: lots(trade) == {"1": 0.01, "2": 0.02} and algos(fake, "T2"))
    assert [a["quantity"] for a in algos(fake, "T2")] == ["0.02"]
    assert [a["quantity"] for a in algos(fake, "T1")] == ["0.01"] and trade.assumed_lots == {}


async def test_closing_an_assumed_trade_cancels_its_tp_sl(service, fake):
    t = service.trades
    await assumed_trade(t, fake, 1, 0.01)
    await assumed_trade(t, fake, 2, 0.02)
    tp(t, 1, 3100)
    tp(t, 2, 3150)
    await wait_for(lambda: algos(fake, "T1") and algos(fake, "T2"))
    close(t, 1)
    await wait_for(lambda: not algos(fake, "T1") and t.closed)
    assert algos(fake, "T2")
    close(t)                                           # sin trade_id: todo
    await wait_for(lambda: not fake.algos and t.get_trade(SYM, "LONG") is None)


async def test_assumed_trade_entry_follows_its_open_lots(service, fake):
    t = service.trades
    fake.slippage_pct = 0.5
    trade = await assumed_trade(t, fake, 1, 0.01)        # ask 3015
    fake.set_mark(SYM, 3100.0)
    await assumed_trade(t, fake, 2, 0.01)                # ask 3115.5
    assert abs(trade.entry_price - 3065.25) < 1e-9
    close(t, 1)
    await wait_for(lambda: t.closed)
    assert t.closed[-1].entry_price == 3015.0 and trade.entry_price == 3115.5
    close(t, 2)
    await wait_for(lambda: len(t.closed) == 2)
    assert t.closed[-1].entry_price == 3115.5


async def test_real_open_of_another_trade_id_removes_the_assumed_tp(service, fake):
    """Trade solo asumido con TP en Binance; luego otro trade_id abre real: ese TP (con la cantidad asumida)
    cerraría la del otro, así que se quita de Binance y queda guardado."""
    t = service.trades
    trade = await assumed_trade(t, fake, 1, 0.01)
    tp(t, 1, 3100)
    await wait_for(lambda: algos(fake, "T1"))
    open_(t, 2, 0.02)
    await wait_for(lambda: amt(fake) == 0.02 and lots(trade) == {"2": 0.02})
    await wait_for(lambda: not algos(fake, "T1"))
    assert trade.lot_prot["1"] == {"TP": 3100.0} and trade.assumed_lots == {"1": 0.01}
    fake.set_mark(SYM, 3100.0)
    await asyncio.sleep(0.5)
    assert amt(fake) == 0.02                           # el trade_id 2 no se tocó


async def test_partial_manual_open_realizes_the_oldest_assumed_lot_only(service, fake):
    t = service.trades
    trade = await assumed_trade(t, fake, 1, 0.01)
    await assumed_trade(t, fake, 2, 0.02)
    tp(t, 1, 3100)
    tp(t, 2, 3150)
    await wait_for(lambda: algos(fake, "T1") and algos(fake, "T2"))
    manual_open(fake, 0.01)
    await wait_for(lambda: lots(trade) == {"1": 0.01} and trade.assumed_lots == {"2": 0.02})
    await wait_for(lambda: not algos(fake, "T2"))      # lote 2 sigue asumido: su TP sale de Binance
    assert [a["quantity"] for a in algos(fake, "T1")] == ["0.01"]
    manual_open(fake, 0.02)
    await wait_for(lambda: lots(trade) == {"1": 0.01, "2": 0.02} and algos(fake, "T2"))
    assert [a["quantity"] for a in algos(fake, "T2")] == ["0.02"]
