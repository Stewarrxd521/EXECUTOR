"""Cierre por trade_id: cada trade_id cierra solo su lote (varios bots en el mismo símbolo)."""

import json

from executor.service import ExecutorService
from executor.trading import Trade, TradeManager
from tests.conftest import make_settings, wait_for

SYM = "ETHUSDT"


def amt(fake, direction="LONG") -> float:
    return round(abs(fake.positions.get((SYM, direction), {"amt": 0.0})["amt"]), 10)


def open_(t, tid, qty, direction="LONG", **extra):
    t.handle_signal({"action": "open", "trade_id": tid, "symbol": SYM, "direction": direction, "price": 3000,
                     "quantity": qty, **extra})


def close(t, tid=None, direction="LONG", **extra):
    sig = {"action": "close", "symbol": SYM, "direction": direction, "reason": "TP", **extra}
    if tid is not None:
        sig["trade_id"] = tid
    t.handle_signal(sig)


async def opened(t, fake, total, direction="LONG"):
    def ok():
        trade = t.get_trade(SYM, direction)
        return abs(amt(fake, direction) - total) < 1e-9 and trade is not None and abs(trade.quantity - total) < 1e-9
    await wait_for(ok)
    return t.get_trade(SYM, direction)


def lots(trade) -> dict:
    return {k: round(v, 10) for k, v in trade.effective_lots().items()}


async def test_closing_one_trade_id_keeps_the_others_open(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.03)
    assert lots(trade) == {"1": 0.01, "2": 0.02}
    close(t, 1, close_price=3010, pnl=0.1)
    await wait_for(lambda: amt(fake) == 0.02)
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 1)
    rec = t.closed[-1]
    assert rec.quantity == 0.01 and rec.status == "TP" and rec.bot_pnl == 0.1 and rec.id != trade.id
    assert t.get_trade(SYM, "LONG") is trade and abs(trade.quantity - 0.02) < 1e-9 and lots(trade) == {"2": 0.02}
    assert (1, SYM) not in t.paper_map and t.paper_map[(2, SYM)] == (SYM, "LONG")
    assert "sigue abierta" in t.signals(1)[0]["detail"]
    close(t, 2)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
    assert t.closed[-1].paper_trade_id == 2 and abs(t.closed[-1].quantity - 0.02) < 1e-9


async def test_three_trade_ids_closed_in_any_order(service, fake):
    t = service.trades
    for tid, qty, total in ((11, 0.01, 0.01), (12, 0.02, 0.03), (13, 0.03, 0.06)):
        open_(t, tid, qty)
        await opened(t, fake, total)
    for tid, left in ((12, 0.04), (13, 0.01), (11, 0.0)):
        close(t, tid)
        await wait_for(lambda: amt(fake) == left)
        await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == tid)
    assert t.get_trade(SYM, "LONG") is None
    assert [round(x.quantity, 10) for x in t.closed[-3:]] == [0.02, 0.03, 0.01]


async def test_last_trade_id_only_closes_its_quantity_when_the_position_has_more(service, fake):
    t = service.trades
    open_(t, 21, 0.01)
    trade = await opened(t, fake, 0.01)
    fake.positions[(SYM, "LONG")]["amt"] = 0.03   # otro bot abrió 0.02 en el mismo símbolo por fuera
    close(t, 21)
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 21)
    assert amt(fake) == 0.02 and abs(t.closed[-1].quantity - 0.01) < 1e-9
    assert t.get_trade(SYM, "LONG") is trade and abs(trade.quantity - 0.02) < 1e-9 and not trade.lots


async def test_close_without_trade_id_closes_the_whole_position(service, fake):
    t = service.trades
    open_(t, 31, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 32, 0.02)
    await opened(t, fake, 0.03)
    close(t)                                     # sin trade_id: cerrar todo
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_trade_id_without_quantity_in_the_position_does_not_touch_it(service, fake):
    t = service.trades
    open_(t, 41, 0.01)
    await opened(t, fake, 0.01)
    close(t, 42)                                 # su apertura aún no llegó
    await wait_for(lambda: t.signals(1) and "no tiene cantidad del trade_id 42" in t.signals(1)[0]["detail"])
    assert amt(fake) == 0.01 and (42, SYM, "LONG") in t._pending_close
    open_(t, 42, 0.02)                           # llega tarde: se cierra solo su lote
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 42)
    await wait_for(lambda: amt(fake) == 0.01)
    assert lots(t.get_trade(SYM, "LONG")) == {"41": 0.01}


async def test_rejected_trade_id_close_is_ignored_on_another_bots_position(service, fake):
    t = service.trades
    open_(t, 45, 0.01)
    await opened(t, fake, 0.01)
    t._rejected_ids[(46, SYM)] = 0.0
    close(t, 46)
    await wait_for(lambda: t.signals(1) and "nunca se abrió" in t.signals(1)[0]["detail"])
    assert amt(fake) == 0.01 and (46, SYM, "LONG") not in t._pending_close


async def test_unknown_trade_id_closes_only_the_quantity_of_nobody(service, fake):
    t = service.trades
    open_(t, 51, 0.01)
    await opened(t, fake, 0.01)
    fake.positions[(SYM, "LONG")]["amt"] = 0.03   # 0.02 sin lote (abierto fuera / adoptado)
    close(t, 52)
    await wait_for(lambda: amt(fake) == 0.01)
    assert t.closed[-1].paper_trade_id == 52 and lots(t.get_trade(SYM, "LONG")) == {"51": 0.01}


async def test_adopted_position_is_closed_by_any_trade_id(service, fake):
    t = service.trades
    trade = await t.open_trade(SYM, "LONG", notional=30)
    trade.lots = {}                              # como una posición adoptada tras perder trades.json
    close(t, 99)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_simultaneous_closes_of_several_trade_ids_all_execute(service, fake):
    t = service.trades
    for tid, total in ((61, 0.01), (62, 0.02), (63, 0.03)):
        open_(t, tid, 0.01)
        await opened(t, fake, total)
    for tid in (61, 62, 63):                     # los tres TP en el mismo instante
        close(t, tid)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
    assert sorted(x.paper_trade_id for x in t.closed[-3:]) == [61, 62, 63]
    assert all(abs(x.quantity - 0.01) < 1e-9 for x in t.closed[-3:])


async def test_tranches_of_one_trade_id_close_together(service, fake):
    t = service.trades
    open_(t, 81, 0.01, level=50)
    await opened(t, fake, 0.01)
    open_(t, 81, 0.01, level=75)                 # promedio del mismo trade_id
    await opened(t, fake, 0.02)
    open_(t, 82, 0.01)
    trade = await opened(t, fake, 0.03)
    assert lots(trade) == {"81": 0.02, "82": 0.01}
    close(t, 81)
    await wait_for(lambda: amt(fake) == 0.01)
    rec = t.closed[-1]
    assert rec.paper_trade_id == 81 and abs(rec.quantity - 0.02) < 1e-9
    assert sorted(x["level"] for x in rec.tranches) == [50, 75]
    assert [x["trade_id"] for x in trade.tranches] == [82]


async def test_partial_close_with_quantity_only_reduces_that_trade_id(service, fake):
    t = service.trades
    open_(t, 101, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 102, 0.01)
    trade = await opened(t, fake, 0.03)
    close(t, 101, quantity=0.01)
    await wait_for(lambda: amt(fake) == 0.02)
    assert lots(trade) == {"101": 0.01, "102": 0.01}
    close(t, 101, quantity=0.05)                 # más de lo que tiene: solo su lote
    await wait_for(lambda: amt(fake) == 0.01)
    assert lots(trade) == {"102": 0.01}


async def test_reduction_outside_the_executor_scales_each_trade_id(service, fake):
    t = service.trades
    open_(t, 111, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 112, 0.02)
    await opened(t, fake, 0.04)
    fake.positions[(SYM, "LONG")]["amt"] = 0.02  # la mitad se cerró fuera (p. ej. a mano en Binance)
    close(t, 111)
    await wait_for(lambda: amt(fake) == 0.01)


async def test_long_and_short_on_the_same_symbol_are_independent(service, fake):
    t = service.trades
    open_(t, 121, 0.01, "LONG")
    await opened(t, fake, 0.01, "LONG")
    open_(t, 122, 0.02, "SHORT")
    await opened(t, fake, 0.02, "SHORT")
    close(t, 121, "LONG")
    await wait_for(lambda: amt(fake, "LONG") == 0)
    assert amt(fake, "SHORT") == 0.02 and t.get_trade(SYM, "SHORT") is not None


async def test_multiplied_quantity_belongs_to_its_trade_id(service, fake):
    t = service.trades
    service.multiplier.configure({"enabled": True, "mode": "manual", "factor": 2})
    open_(t, 131, 0.01)
    await opened(t, fake, 0.02)
    open_(t, 132, 0.01)
    await opened(t, fake, 0.04)
    close(t, 131)
    await wait_for(lambda: amt(fake) == 0.02)


async def test_lots_survive_a_restart(fake, tmp_path, snapshot_file):
    svc = ExecutorService(make_settings(fake, tmp_path))
    await svc.start()
    await wait_for(lambda: svc.user.conn.connected and svc.trades.ready.is_set())
    open_(svc.trades, 141, 0.01)
    await opened(svc.trades, fake, 0.01)
    open_(svc.trades, 142, 0.02)
    await opened(svc.trades, fake, 0.03)
    await svc.stop()
    svc2 = ExecutorService(make_settings(fake, tmp_path))
    await svc2.start()
    try:
        await wait_for(lambda: svc2.user.conn.connected and svc2.trades.ready.is_set())
        assert lots(svc2.trades.get_trade(SYM, "LONG")) == {"141": 0.01, "142": 0.02}
        close(svc2.trades, 142)
        await wait_for(lambda: amt(fake) == 0.01)
    finally:
        await svc2.stop()


def test_positions_saved_before_lots_are_split_equally(tmp_path):
    trade = Trade(id=1, symbol=SYM, direction="LONG", entry_price=3000, quantity=0.03, open_time="",
                  leverage=5, paper_trade_id=3, paper_ids=[1, 2, 3])
    TradeManager._migrate_lots(trade)
    assert {k: round(v, 10) for k, v in trade.lots.items()} == {"1": 0.01, "2": 0.01, "3": 0.01}
    lonely = Trade(id=2, symbol=SYM, direction="LONG", entry_price=3000, quantity=0.03, open_time="", leverage=5)
    TradeManager._migrate_lots(lonely)
    assert lonely.lots == {} and lonely.unassigned_qty() == 0.03


async def test_legacy_trades_file_is_migrated_on_load(fake, tmp_path, snapshot_file):
    data_dir = tmp_path / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    legacy = {"counter": 5, "open": [{"id": 5, "symbol": SYM, "direction": "LONG", "entry_price": 3000,
                                      "quantity": 0.02, "open_time": "", "leverage": 5, "paper_trade_id": 8,
                                      "paper_ids": [7, 8]}], "closed": []}
    (data_dir / "trades.json").write_text(json.dumps(legacy))
    fake.positions[(SYM, "LONG")] = {"amt": 0.02, "entry": 3000.0}
    svc = ExecutorService(make_settings(fake, tmp_path))
    await svc.start()
    try:
        await wait_for(lambda: svc.user.conn.connected and svc.trades.ready.is_set())
        assert lots(svc.trades.get_trade(SYM, "LONG")) == {"7": 0.01, "8": 0.01}
        close(svc.trades, 7)
        await wait_for(lambda: amt(fake) == 0.01)
    finally:
        await svc.stop()


async def test_dashboard_shows_each_trade_id_quantity(service, fake):
    t = service.trades
    open_(t, 151, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 152, 0.02)
    await opened(t, fake, 0.03)
    row = next(p for p in service.positions_view() if p["symbol"] == SYM)
    assert [[i, round(q, 10)] for i, q in row["lots"]] == [[151, 0.01], [152, 0.02]]


async def test_one_way_account_is_warned_once(tmp_path, snapshot_file):
    from tests.fake_binance import FakeBinance
    fake = FakeBinance(hedge=False)
    await fake.start()
    svc = ExecutorService(make_settings(fake, tmp_path))
    try:
        await svc.start()
        await wait_for(lambda: svc.trades.ready.is_set())
        await svc.sync_account("manual")                  # una segunda sincronización no repite el aviso
        events = [e for e in svc.core.bus.recent if e.get("kind") == "position_mode"]
        assert len(events) == 1 and "One-way" in events[0]["text"]
    finally:
        await svc.stop()
        await fake.stop()


async def test_lot_close_message_says_the_position_stays_open(service, fake, monkeypatch):
    t = service.trades
    sent = []
    monkeypatch.setattr(service.core.telegram, "send", lambda text, *a, **k: sent.append(text))
    open_(t, 171, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 172, 0.02)
    await opened(t, fake, 0.03)
    close(t, 171)
    await wait_for(lambda: any("TRADE_ID #171 CERRADO" in m for m in sent))
    msg = next(m for m in sent if "TRADE_ID #171 CERRADO" in m)
    assert "Sigue abierta" in msg and "0.02" in msg
    events = [e["text"] for e in service.core.bus.recent if e["kind"] == "close"]
    assert any("trade_id 171 de ETHUSDT cerrado" in e for e in events)


async def test_trade_id_close_without_direction_only_touches_its_own_lot(service, fake):
    t = service.trades
    open_(t, 161, 0.01, "LONG")
    await opened(t, fake, 0.01, "LONG")
    open_(t, 162, 0.02, "SHORT")
    await opened(t, fake, 0.02, "SHORT")
    t.handle_signal({"action": "close", "trade_id": 163, "symbol": SYM, "reason": "TP"})   # trade_id ajeno
    await wait_for(lambda: t.signals(1) and t.signals(1)[0]["action"] == "close")
    assert amt(fake, "LONG") == 0.01 and amt(fake, "SHORT") == 0.02 and (163, SYM) not in t.paper_map
    t.handle_signal({"action": "close", "trade_id": 162, "symbol": SYM, "reason": "TP"})   # sin direction
    await wait_for(lambda: amt(fake, "SHORT") == 0)
    assert amt(fake, "LONG") == 0.01
