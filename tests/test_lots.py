"""Cierre por trade_id: cada trade_id cierra solo su lote (varios bots en el mismo símbolo)."""

import asyncio
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


async def test_unknown_trade_id_never_closes_quantity_opened_outside(service, fake):
    t = service.trades
    open_(t, 51, 0.01)
    await opened(t, fake, 0.01)
    fake.positions[(SYM, "LONG")]["amt"] = 0.03   # otro bot abrió 0.02 por fuera del executor
    close(t, 52)                                 # trade_id sin cantidad en la posición
    await wait_for(lambda: t.signals(1) and "no tiene cantidad del trade_id 52" in t.signals(1)[0]["detail"])
    assert amt(fake) == 0.03 and (52, SYM, "LONG") in t._pending_close


async def test_adopted_position_is_closed_by_any_trade_id(service, fake):
    t = service.trades
    trade = await t.open_trade(SYM, "LONG", notional=30)
    t._unmap_trade(trade)
    del t.trades[trade.key]                      # se perdió trades.json: la posición se adopta sin lotes
    await wait_for(lambda: service.core.account.position(SYM, "LONG") is not None)
    assert t.adopt_untracked() == 1 and not t.get_trade(SYM, "LONG").lot_tracked
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
    assert [[i, round(q, 10)] for i, q in row["lots"]] == [["151", 0.01], ["152", 0.02]]


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


# ── Segunda revisión: reintentos, carreras, llenados parciales, asumidas, PnL y bots ─────────
def external_reduce(fake, qty, direction="LONG"):
    """Cierre hecho fuera del executor (a mano en Binance, ADL): llega por el user stream."""
    side = "SELL" if direction == "LONG" else "BUY"
    fake._place({"symbol": SYM, "side": side, "type": "MARKET", "positionSide": direction,
                 "quantity": str(qty), "newClientOrderId": "adl_autoclose-1"})


async def test_lot_close_with_a_transient_error_is_retried(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.03)
    fake.inject.extend([("order.place", -1001, "Internal error; unable to process your request.")] * 7)
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0.02, timeout=15)
    assert [x.paper_trade_id for x in t.closed] == [1]


async def test_lost_response_is_checked_before_closing_again(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    trade = await opened(t, fake, 0.03)
    fake.inject_after.append(("order.place", -1007, "Timeout waiting for response from backend server."))
    fake.inject.extend([("order.status", -1001, "Internal error; unable to process your request.")] * 3)
    close(t, 1)                                  # se ejecuta pero no hay respuesta ni confirmación
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 1, timeout=20)
    assert amt(fake) == 0.02 and lots(trade) == {"2": 0.02} and len(t.closed) == 1   # nunca dos veces
    close(t, 2)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_close_all_waits_for_a_lot_close_in_progress(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.03)
    close(t, 1)
    t.handle_signal({"action": "close_all"})     # en el mismo instante
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_close_without_trade_id_waits_for_a_lot_close_in_progress(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    await opened(t, fake, 0.01)
    open_(t, 2, 0.02)
    await opened(t, fake, 0.03)
    open_(t, 3, 0.01, "SHORT")                   # otro bot abre SHORT (tiene el lock del símbolo)
    close(t, 1)
    close(t)                                     # cerrar todo el LONG
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None, timeout=10)


async def test_deferred_close_without_direction_applies_to_its_late_open(service, fake):
    t = service.trades
    open_(t, 1, 0.01, "LONG")                    # bot A
    await opened(t, fake, 0.01, "LONG")
    t.handle_signal({"action": "close", "trade_id": 5, "symbol": SYM, "reason": "TP"})   # bot B, sin direction
    await wait_for(lambda: (5, SYM, "") in t._pending_close)
    open_(t, 5, 0.02, "SHORT")                   # su apertura llega tarde
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 5)
    await wait_for(lambda: amt(fake, "SHORT") == 0)
    assert amt(fake, "LONG") == 0.01


async def test_old_external_reduction_is_not_charged_to_a_new_trade_id(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    trade = await opened(t, fake, 0.02)
    external_reduce(fake, 0.01)                  # solo existía el trade_id 1
    await wait_for(lambda: abs(trade.quantity - 0.01) < 1e-9)
    open_(t, 2, 0.03)
    await opened(t, fake, 0.04)
    assert lots(trade) == {"1": 0.01, "2": 0.03}
    close(t, 1)
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 1)
    assert amt(fake) == 0.03


def thin_book(fake):
    """La 1.ª orden MARKET de cierre da -4131; la LIMIT IOC siguiente se llena a medias."""
    from decimal import Decimal
    from tests.fake_binance import _Err
    orig = fake._place
    state = {"rejected": False}

    def place(p):
        closing = fake._is_closing(p)
        if closing and p["type"] == "MARKET" and not state["rejected"]:
            state["rejected"] = True
            raise _Err(-4131, "The counterparty's best price does not meet the PERCENT_PRICE filter limit.")
        if closing and p["type"] == "LIMIT" and p.get("timeInForce") == "IOC" and not state.get("half"):
            state["half"] = True
            q = Decimal(p["quantity"])
            res = orig({**p, "quantity": str((q / 2).quantize(Decimal("0.001")))})
            res.update({"origQty": str(q), "status": "EXPIRED"})
            return res
        return orig(p)

    fake._place = place


async def test_partial_fill_of_the_only_lot_is_completed(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    thin_book(fake)
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)
    assert abs(sum(x.quantity for x in t.closed if x.paper_trade_id == 1) - 0.02) < 1e-9


async def test_partial_fill_of_a_lot_never_touches_the_others(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.01)
    trade = await opened(t, fake, 0.03)
    thin_book(fake)
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0.01)
    assert lots(trade) == {"2": 0.01} and trade.unassigned_qty() < 1e-9


async def test_assumed_lot_never_sends_orders_or_shrinks_real_lots(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    trade = await opened(t, fake, 0.01)
    fake.inject.append(("order.place", -2019, "Margin is insufficient."))
    open_(t, 2, 0.01)                            # sin margen: queda ASUMIDA (sin orden real)
    await wait_for(lambda: "2" in trade.assumed_lots)
    assert lots(trade) == {"1": 0.01} and not trade.order_assumed and abs(trade.quantity - 0.01) < 1e-9
    close(t, 2)                                  # nunca se abrió en Binance: no se envía nada
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 2)
    assert amt(fake) == 0.01 and t.closed[-1].order_assumed and "2" not in trade.assumed_lots
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_unknown_status_tranche_is_added_to_its_lot_even_after_a_sync(service, fake, monkeypatch):
    import executor.trading as trading_mod
    monkeypatch.setattr(trading_mod, "UNKNOWN_OPEN_CHECKS", (1.0, 2.0, 3.0))
    t = service.trades
    open_(t, 1, 0.01)
    trade = await opened(t, fake, 0.01)
    fake.inject_after.append(("order.place", -1007, "Timeout waiting for response from backend server."))
    fake.inject.extend([("order.status", -1001, "Internal error; unable to process your request.")] * 3)
    open_(t, 1, 0.01, level=75)                  # 2.º tramo: se ejecuta, la respuesta se pierde
    await wait_for(lambda: abs(amt(fake) - 0.02) < 1e-9)
    await wait_for(lambda: (SYM, "LONG") in t._reconciling)
    await service.sync_account("user stream conectado")  # la posición se relee antes de reconciliar
    await wait_for(lambda: (SYM, "LONG") not in t._reconciling, timeout=8)
    assert lots(trade) == {"1": 0.02}
    close(t, 1)
    await wait_for(lambda: amt(fake) == 0)


async def test_partial_close_uses_the_trade_ids_own_multiplier(service, fake):
    t = service.trades
    service.multiplier.configure({"enabled": True, "mode": "manual", "factor": 2})
    open_(t, 1, 0.01)                            # 0.01 del bot × 2 = 0.02 reales
    await opened(t, fake, 0.02)
    service.multiplier.configure({"enabled": False})
    open_(t, 2, 0.01)                            # × 1
    trade = await opened(t, fake, 0.03)
    close(t, 1, quantity=0.01)                   # toda la cantidad del bot 1
    await wait_for(lambda: amt(fake) == 0.01)
    assert lots(trade) == {"2": 0.01}


async def test_each_lot_record_uses_its_own_entry_price(service, fake):
    t = service.trades
    open_(t, 1, 0.01)                            # entra a 3000
    await opened(t, fake, 0.01)
    fake.set_mark(SYM, 3200.0)
    await asyncio.sleep(0.4)
    open_(t, 2, 0.01)                            # entra a 3200
    await opened(t, fake, 0.02)
    fake.set_mark(SYM, 3100.0)
    await asyncio.sleep(0.4)
    close(t, 1, close_price=3100, pnl=1.0)
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 1)
    close(t, 2, close_price=3100, pnl=-1.0)
    await wait_for(lambda: t.get_trade(SYM, "LONG") is None)
    await asyncio.sleep(0.5)                     # llegan los fills (realizedPnl) por el user stream
    r1, r2 = t.closed[-2], t.closed[-1]
    fees = r1.fees_usdt
    assert r1.entry_price == 3000 and abs(r1.pnl_usdt - (1.0 - fees)) < 0.01
    assert r2.entry_price == 3200 and abs(r2.pnl_usdt - (-1.0 - r2.fees_usdt)) < 0.01
    assert abs(t.realized_total - (r1.pnl_usdt + r2.pnl_usdt)) < 1e-9


async def test_outside_fill_during_the_last_lot_close_is_kept_and_recorded(service, fake):
    t = service.trades
    open_(t, 1, 0.01)
    trade = await opened(t, fake, 0.01)
    orig = fake._place

    def place(p):
        res = orig(p)
        if p.get("side") == "SELL" and p["type"] == "MARKET" and not p["newClientOrderId"].startswith("adl"):
            fake._place({"symbol": SYM, "side": "BUY", "type": "MARKET", "positionSide": "LONG",
                         "quantity": "0.01", "newClientOrderId": "web_other"})
        return res

    fake._place = place
    close(t, 1, close_price=3000, pnl=0.5)
    await wait_for(lambda: t.closed and t.closed[-1].paper_trade_id == 1)
    assert amt(fake) == 0.01 and t.closed[-1].bot_pnl == 0.5
    assert t.get_trade(SYM, "LONG") is trade and not trade.lots and abs(trade.quantity - 0.01) < 1e-9


async def test_two_bots_with_the_same_trade_id_keep_separate_lots(service, fake):
    t = service.trades
    open_(t, 1, 0.01, bot="kline")
    await opened(t, fake, 0.01)
    open_(t, 1, 0.02, bot="app25")               # mismo número, otro bot
    trade = await opened(t, fake, 0.03)
    assert lots(trade) == {"kline:1": 0.01, "app25:1": 0.02}
    close(t, 1, bot="kline")
    await wait_for(lambda: amt(fake) == 0.02)
    assert t.closed[-1].bot == "kline" and lots(trade) == {"app25:1": 0.02}
    open_(t, 1, 0.01, bot="app25", level=75)     # un tramo del otro bot no se toma como «ya cerrado»
    await opened(t, fake, 0.03)
    close(t, 1, bot="app25")
    await wait_for(lambda: amt(fake) == 0 and t.get_trade(SYM, "LONG") is None)


async def test_bot_identity_from_the_executor_url(service, fake):
    from aiohttp.test_utils import TestClient, TestServer
    from executor.web.server import build_app
    t = service.trades
    async with TestClient(TestServer(build_app(service))) as client:
        h = {"X-Signal-Secret": "sig"}
        for bot, qty in (("kline", 0.01), ("app25", 0.02)):
            r = await client.post(f"/b/{bot}/signal", json={"action": "open", "trade_id": 7, "symbol": SYM,
                                                           "direction": "LONG", "price": 3000, "quantity": qty},
                                  headers=h)
            assert r.status == 200
        trade = await opened(t, fake, 0.03)
        assert lots(trade) == {"kline:7": 0.01, "app25:7": 0.02}
        r = await client.post("/b/kline/signal", json={"action": "close", "trade_id": 7, "symbol": SYM,
                                                       "direction": "LONG"}, headers=h)
        assert r.status == 200
        await wait_for(lambda: amt(fake) == 0.02)
        assert (await client.get("/b/kline/api/state")).status in (200, 401)


async def test_tp_of_one_trade_id_over_other_lots_is_warned(service, fake):
    t = service.trades
    open_(t, 1, 0.02)
    await opened(t, fake, 0.02)
    open_(t, 2, 0.01, tp=3300)                   # su TP en Binance protege la posición entera
    await wait_for(lambda: t.signals(1, trade_id=2) and t.signals(1, trade_id=2)[0]["ok"])
    assert "cubre también otros trade_id" in t.signals(1, trade_id=2)[0]["detail"]
    assert any(e["kind"] == "protect_shared" for e in service.core.bus.recent)


def test_client_takes_the_bot_from_its_executor_url(monkeypatch):
    import executor_client
    monkeypatch.delenv("EXECUTOR_BOT", raising=False)
    c = executor_client.ExecutorClient("https://executor.example.com/b/KLINE")
    assert c.base_url == "https://executor.example.com" and c.bot == "kline"
    c2 = executor_client.ExecutorClient("https://executor.example.com", bot="app25")
    assert c2.base_url == "https://executor.example.com" and c2.bot == "app25"
    assert executor_client.ExecutorClient("https://executor.example.com").bot == ""
