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
    assert kinds == {"TAKE_PROFIT": 2850, "STOP_MARKET": 2950}

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


async def test_un_sl_de_la_posicion_del_mismo_lado_no_quita_la_entrada_stop(service, fake):
    service.trades.handle_signal({"action": "open", "trade_id": 50, "symbol": "ETHUSDT", "direction": "SHORT",
                                  "price": 3000, "quantity": 0.01})
    await wait_for(lambda: service.core.account.position("ETHUSDT", "SHORT") is not None)
    _open_stop(service, trigger_price=2800)
    await wait_for(lambda: len(service.trades.pending_stops()) == 1 and len(fake.algos) == 1)
    service.trades.handle_signal({"action": "open_sl", "symbol": "ETHUSDT", "direction": "SHORT",
                                  "trigger_price": 3100})                   # SL de la posición entera
    await wait_for(lambda: len(fake.algos) == 2)
    assert {a["type"] for a in fake.algos.values()} == {"STOP_MARKET"}
    assert any(a["clientAlgoId"].startswith("E42") for a in fake.algos.values())   # la entrada sigue


async def test_si_binance_no_confirma_la_cancelacion_la_entrada_sigue_registrada(service, fake):
    _open_stop(service)
    await wait_for(lambda: len(service.trades.pending_stops()) == 1)

    async def falla(*a, **k):
        return False
    real = service.trades.orders.cancel_algo
    service.trades.orders.cancel_algo = falla
    _, body = await service.trades.submit_signal({"action": "cancel_stop", "trade_id": 42, "bot": "ratio_rev",
                                                  "symbol": "ETHUSDT"}, wait=True)
    assert body["ok"] is False and len(service.trades.pending_stops()) == 1 and len(fake.algos) == 1
    service.trades.orders.cancel_algo = real
    fake.set_mark("ETHUSDT", 2899)                                         # si se dispara, se atribuye igual
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "SHORT") is not None)
    assert service.trades.get_trade("ETHUSDT", "SHORT").lots.get("ratio_rev:42") == pytest.approx(0.02)


async def test_close_all_justo_despues_de_open_stop_no_deja_entradas(service, fake):
    _open_stop(service)
    _, body = await service.trades.submit_signal({"action": "close_all"}, wait=True)
    assert body["ok"]
    await wait_for(lambda: not fake.algos and not service.trades.pending_stops())


async def test_close_con_lote_abierto_y_entrada_pendiente_cierra_las_dos(service, fake):
    service.trades.handle_signal({"action": "open", "trade_id": 42, "bot": "ratio_rev", "symbol": "ETHUSDT",
                                  "direction": "SHORT", "price": 3000, "quantity": 0.01})
    await wait_for(lambda: service.core.account.position("ETHUSDT", "SHORT") is not None)
    _open_stop(service, trigger_price=2800)
    await wait_for(lambda: len(service.trades.pending_stops()) == 1)
    service.trades.handle_signal({"action": "close", "trade_id": 42, "bot": "ratio_rev", "symbol": "ETHUSDT",
                                  "direction": "SHORT", "reason": "MANUAL"})
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "SHORT") is None)
    await wait_for(lambda: not fake.algos and not service.trades.pending_stops())
    assert fake.positions[("ETHUSDT", "SHORT")]["amt"] == 0


# ── Menos deslizamiento: entrada stop límite y TP límite ─────────────────────

async def test_open_stop_con_limit_price_entra_como_stop_limite_y_sale_al_tope(service, fake):
    _open_stop(service, limit_price=2897.1)
    await wait_for(lambda: len(service.trades.pending_stops()) == 1)
    placed = methods(fake, "algoOrder.place")[-1]
    assert placed["type"] == "STOP" and float(placed["price"]) == pytest.approx(2897.1)
    assert placed["timeInForce"] == "GTC"
    fake.set_mark("ETHUSDT", 2899)                                         # dentro del tope: entra al límite
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "SHORT") is not None)
    trade = service.trades.get_trade("ETHUSDT", "SHORT")
    assert trade.entry_price == pytest.approx(2897.1) and trade.lots.get("ratio_rev:42") == pytest.approx(0.02)
    await wait_for(lambda: len(service.core.account.algos_for("ETHUSDT", "SHORT")) == 2)
    await wait_for(lambda: not service.trades.pending_stops())


async def test_stop_limite_que_el_precio_salta_se_cancela_sin_abrir(service, fake):
    service.settings.stop_limit_timeout_s = 0.3
    _open_stop(service, limit_price=2897.1)
    await wait_for(lambda: len(service.trades.pending_stops()) == 1)
    fake.set_mark("ETHUSDT", 2880)                                         # hueco: pasa el tope sin llenarse
    await wait_for(lambda: len(fake.orders) == 1)                          # su LIMIT queda en el libro
    await wait_for(lambda: not fake.orders and not service.trades.pending_stops(), timeout=5)
    assert service.trades.get_trade("ETHUSDT", "SHORT") is None
    assert fake.positions.get(("ETHUSDT", "SHORT"), {"amt": 0})["amt"] == 0


async def test_cancel_stop_de_una_stop_limite_disparada_quita_su_limit(service, fake):
    service.settings.stop_limit_timeout_s = 60
    _open_stop(service, limit_price=2897.1)
    await wait_for(lambda: len(service.trades.pending_stops()) == 1)
    fake.set_mark("ETHUSDT", 2880)
    await wait_for(lambda: len(service.core.account.orders_for("ETHUSDT")) == 1)
    _, body = await service.trades.submit_signal({"action": "cancel_stop", "trade_id": 42, "bot": "ratio_rev",
                                                  "symbol": "ETHUSDT", "direction": "SHORT"}, wait=True)
    assert body["ok"]
    await wait_for(lambda: not fake.orders and not service.trades.pending_stops())


async def test_open_stop_rechaza_un_tope_mejor_que_el_disparo(service, fake):
    status, body = _open_stop(service, limit_price=2905)                  # SHORT: el tope no puede ser mayor
    assert status == 400 and not body["ok"]


async def test_tp_es_limite_por_ultimo_precio(service, fake):
    service.trades.handle_signal({"action": "open", "trade_id": 7, "bot": "b", "symbol": "ETHUSDT",
                                  "direction": "LONG", "price": 3000, "quantity": 0.01, "tp": 3100})
    await wait_for(lambda: len(service.core.account.algos_for("ETHUSDT", "LONG")) == 1)
    tp = methods(fake, "algoOrder.place")[-1]
    assert tp["type"] == "TAKE_PROFIT" and float(tp["price"]) == float(tp["triggerPrice"]) == 3100
    assert tp["workingType"] == "CONTRACT_PRICE" and tp["timeInForce"] == "GTC"


async def test_close_de_respaldo_tras_el_tp_no_toca_el_lote_de_otro(service, fake):
    for tid, tp in ((1, 3100), (2, 0)):
        sig = {"action": "open", "trade_id": tid, "bot": "b", "symbol": "ETHUSDT", "direction": "LONG",
               "price": 3000, "quantity": 0.01}
        if tp:
            sig["tp"] = tp
        await service.trades.submit_signal(sig, wait=True)
    await wait_for(lambda: len(service.core.account.algos_for("ETHUSDT", "LONG")) == 1)
    fake.set_mark("ETHUSDT", 3101)                                         # Binance cierra el lote 1 con su TP
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "LONG").lots.get("b:1", 0) == 0)
    await service.trades.submit_signal({"action": "close", "trade_id": 1, "bot": "b", "symbol": "ETHUSDT",
                                        "direction": "LONG", "quantity": 0.01, "reason": "TP"}, wait=True)
    import asyncio
    await asyncio.sleep(0.3)
    assert fake.positions[("ETHUSDT", "LONG")]["amt"] == pytest.approx(0.01)     # el lote 2 sigue entero
    assert service.trades.get_trade("ETHUSDT", "LONG").lots.get("b:2") == pytest.approx(0.01)


async def test_cancel_stop_de_una_stop_limite_llenada_en_parte_quita_el_resto_y_protege_lo_llenado(service, fake):
    service.settings.stop_limit_timeout_s = 60
    _open_stop(service, limit_price=2897.1)
    await wait_for(lambda: len(service.trades.pending_stops()) == 1)
    fake.set_mark("ETHUSDT", 2880)                                         # hueco: la LIMIT queda en el libro
    await wait_for(lambda: len(fake.orders) == 1)
    o = next(iter(fake.orders.values()))
    fake._apply_fill(o, 2897.1, 0.005)                                     # se llena en parte
    await wait_for(lambda: service.trades.get_trade("ETHUSDT", "SHORT") is not None)
    _, body = await service.trades.submit_signal({"action": "cancel_stop", "trade_id": 42, "bot": "ratio_rev",
                                                  "symbol": "ETHUSDT", "direction": "SHORT"}, wait=True)
    assert body["ok"]
    await wait_for(lambda: not fake.orders and not service.trades.pending_stops())
    trade = service.trades.get_trade("ETHUSDT", "SHORT")
    assert trade.lots.get("ratio_rev:42") == pytest.approx(0.005)
    await wait_for(lambda: len(service.core.account.algos_for("ETHUSDT", "SHORT")) == 2)   # su TP y SL


async def test_cancelar_el_tp_disparado_de_un_lote_no_toca_el_de_otro_bot_con_el_mismo_numero(service, fake):
    t = service.trades
    t._fired_orders.update({901: ("alpha:42", 1), 902: ("beta:42", 2)})
    assert t._same_lot("alpha:42", "alpha:42") and not t._same_lot("beta:42", "alpha:42")
