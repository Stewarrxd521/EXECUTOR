"""Entrada STOP_MARKET pendiente por señal (open_stop / cancel_stop): la venganza ya puesta en Binance."""

import pytest

from tests.conftest import wait_for


def methods(fake, name):
    return [p for m, p in fake.requests if m == name]


def _open_stop(service, **over):
    sig = {"action": "open_stop", "trade_id": 42, "bot": "ratio_rev", "symbol": "ETHUSDT", "direction": "SHORT",
           "trigger_price": 2900, "quantity": 0.02, "tp": 2850, "sl": 2950}
    sig.update(over)
    return service.trades.handle_signal(sig)


async def test_open_stop_queda_pendiente_y_al_dispararse_abre_con_su_tp_y_sl(service, fake):
    status, body = _open_stop(service)
    assert status == 200 and body["ok"]
    await wait_for(lambda: len(service.trades.pending_stops()) == 1)
    placed = methods(fake, "algoOrder.place")[-1]
    assert placed["type"] == "STOP_MARKET" and placed["side"] == "SELL" and placed["positionSide"] == "SHORT"
    assert placed["workingType"] == "CONTRACT_PRICE" and float(placed["quantity"]) == pytest.approx(0.02)
    assert "reduceOnly" not in placed and "closePosition" not in placed
    assert placed["clientAlgoId"].startswith("E42:ratio_rev-")
    assert service.trades.get_trade("ETHUSDT", "SHORT") is None           # aún no abre nada

    fake.set_mark("ETHUSDT", 2899)                                         # toca el nivel: Binance la dispara
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "SHORT") is not None)
    trade = service.trades.get_trade("ETHUSDT", "SHORT")
    await wait_for(lambda: not service.trades.pending_stops())
    assert trade.quantity == pytest.approx(0.02) and trade.lots.get("ratio_rev:42") == pytest.approx(0.02)
    assert fake.positions[("ETHUSDT", "SHORT")]["amt"] == pytest.approx(-0.02)
    # Su TP y SL, con la cantidad de su lote.
    await wait_for(lambda: len(service.core.account.algos_for("ETHUSDT", "SHORT")) == 2)
    kinds = {o.type: o.trigger_price for o in service.core.account.algos_for("ETHUSDT", "SHORT")}
    assert kinds == {"TAKE_PROFIT_MARKET": 2850, "STOP_MARKET": 2950}

    # El bot la cierra luego por su trade_id como cualquier otra.
    service.trades.handle_signal({"action": "close", "trade_id": 42, "bot": "ratio_rev", "symbol": "ETHUSDT",
                                  "direction": "SHORT", "reason": "TP"})
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "SHORT") is None)
    assert fake.positions[("ETHUSDT", "SHORT")]["amt"] == 0


async def test_cancel_stop_la_quita_de_binance(service, fake):
    _open_stop(service)
    await wait_for(lambda: len(service.core.account.algos_for("ETHUSDT")) == 1)
    status, body = service.trades.handle_signal({"action": "cancel_stop", "trade_id": 42, "bot": "ratio_rev",
                                                 "symbol": "ETHUSDT", "direction": "SHORT"})
    assert status == 200 and body["found"]
    await wait_for(lambda: not fake.algos and not service.trades.pending_stops())
    fake.set_mark("ETHUSDT", 2800)
    assert service.trades.get_trade("ETHUSDT", "SHORT") is None


async def test_un_close_de_una_entrada_stop_sin_disparar_la_cancela(service, fake):
    _open_stop(service)
    await wait_for(lambda: len(fake.algos) == 1)
    status, body = service.trades.handle_signal({"action": "close", "trade_id": 42, "bot": "ratio_rev",
                                                 "symbol": "ETHUSDT", "direction": "SHORT", "reason": "MANUAL"})
    assert status == 200 and body["stop_cancelled"]
    await wait_for(lambda: not fake.algos and not service.trades.pending_stops())


async def test_open_stop_no_toca_la_posicion_contraria_del_mismo_par(service, fake):
    service.trades.handle_signal({"action": "open", "trade_id": 41, "bot": "ratio_rev", "symbol": "ETHUSDT",
                                  "direction": "LONG", "price": 3000, "quantity": 0.01, "sl": 2900})
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "LONG") is not None)
    _open_stop(service, quantity=0.1)                                      # la venganza ×10, contraria
    await wait_for(lambda: len(service.trades.pending_stops()) == 1)
    fake.set_mark("ETHUSDT", 2899)                                         # SL del LONG y entrada del SHORT
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "SHORT") is not None)
    short = service.trades.get_trade("ETHUSDT", "SHORT")
    assert short.quantity == pytest.approx(0.1) and short.lots.get("ratio_rev:42") == pytest.approx(0.1)
    assert fake.positions[("ETHUSDT", "SHORT")]["amt"] == pytest.approx(-0.1)
