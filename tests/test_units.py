"""Pruebas unitarias: precisión, errores, exchangeInfo y matemática del grid."""

from decimal import Decimal

import pytest

from executor.errors import Action, BinanceAPIError, ErrorDoctor, ErrorJournal, parse_error
from executor.exchange_info import ExchangeInfo, SymbolRules, heuristic_rules, parse_exchange_info
from executor.grid import GridConfig, GridError, GridManager, compute_levels
from executor.precision import ceil_step, coarser_step, decimals_of, floor_step, fmt, round_step
from tests.fake_binance import exchange_info_payload


# ── Precisión ─────────────────────────────────────────────────────────────
def test_steps_are_exact():
    assert floor_step(0.30000000000000004, "0.1") == Decimal("0.3")
    assert ceil_step(1.01, "0.1") == Decimal("1.1")
    assert round_step(26.999999999, "1") == Decimal("27")
    assert fmt(Decimal("1.2300")) == "1.23"
    assert fmt(Decimal("100")) == "100"
    assert fmt(Decimal("1E+1")) == "10"
    assert decimals_of("0.001") == 3 and decimals_of("1") == 0
    assert coarser_step("0.01") == Decimal("0.1") and fmt(coarser_step("1")) == "10"


# ── Errores ───────────────────────────────────────────────────────────────
@pytest.mark.parametrize("raw,code", [
    ('Binance WS error 400: {"code": -2019, "msg": "Margin is insufficient."}', -2019),
    ('REST POST /fapi/v1/leverage error 400: {"code":-4028,"msg":"Leverage 20 is not valid"}', -4028),
    ("APIError(code=-1111): Precision is over the maximum defined for this asset.", -1111),
    ({"status": 400, "error": {"code": -4061, "msg": "Order's position side does not match user's setting."}}, -4061),
])
def test_parse_error_reads_codes(raw, code):
    assert parse_error(raw)[0] == code


def test_doctor_actions():
    assert ErrorDoctor.diagnose(BinanceAPIError(-1111, "Precision")).action == Action.FIX_PRECISION
    assert ErrorDoctor.diagnose(BinanceAPIError(-4164, "notional")).action == Action.RAISE_NOTIONAL
    assert ErrorDoctor.diagnose(BinanceAPIError(-4061, "side")).action == Action.FLIP_POSITION_SIDE
    assert ErrorDoctor.diagnose(BinanceAPIError(-1106, "Parameter 'reduceonly' sent when not required.")).action == Action.DROP_REDUCE_ONLY
    assert ErrorDoctor.diagnose(BinanceAPIError(-1106, "Parameter 'foo' sent when not required.")).action == Action.NONE
    assert ErrorDoctor.diagnose(BinanceAPIError(-1007, "timeout")).action == Action.CHECK_STATUS
    assert ErrorDoctor.diagnose(BinanceAPIError(-4046, "No need")).action == Action.ALREADY_DONE
    d = ErrorDoctor.diagnose(BinanceAPIError(-1003, "Way too many requests; IP banned until 99999999999999."))
    assert d.action == Action.WAIT and d.retry_after_s > 0
    assert ErrorDoctor.diagnose(BinanceAPIError(0, "Forbidden", http_status=451)).action == Action.USE_PROXY
    assert ErrorDoctor.diagnose(ConnectionError("boom")).info.name == "NETWORK"


def test_explain_and_catalog():
    known = ErrorDoctor.explain(-2019)
    assert known["known"] and "Margen" in known["title"]
    unknown = ErrorDoctor.explain(-9999)
    assert unknown["known"] is False
    assert ErrorDoctor.explain(418)["name"] == "IP_AUTO_BANNED"
    catalog = ErrorDoctor.catalog()
    assert len(catalog) > 100 and all("solution" in c for c in catalog)


def test_journal_tracks_fixes():
    j = ErrorJournal()
    seen = []
    j.subscribe(seen.append)
    entry = j.record(ErrorDoctor.diagnose(BinanceAPIError(-1111, "x")), "test", "AUSDT")
    j.mark_fixed(entry, "paso ajustado")
    assert j.entries()[0]["fixed"] and j.counts() == {"-1111": 1} and len(seen) == 2


# ── exchangeInfo ──────────────────────────────────────────────────────────
def test_parse_exchange_info_filters():
    rules = parse_exchange_info(exchange_info_payload())
    btc = rules["BTCUSDT"]
    assert btc.tick_size == Decimal("0.1") and btc.step_size == Decimal("0.001")
    assert btc.min_notional == Decimal("100") and btc.market_max_qty == Decimal("50000")
    assert btc.multiplier_up == Decimal("1.05")
    qty = btc.qty_for_notional(150, 65000)
    assert qty * 65000 >= 150 and qty % Decimal("0.001") == 0
    # El mínimo de 100 USDT obliga a subir la cantidad.
    assert btc.qty_for_notional(10, 65000) * 65000 >= 100
    assert btc.validate(Decimal("0.0015"), 65000.05) and not btc.validate(Decimal("0.002"), Decimal("65000.1"))
    assert btc.clamp_to_band(80000, 65000) == Decimal("68250.0")


def test_rules_roundtrip_and_learning(tmp_path):
    ex = ExchangeInfo(tmp_path, tmp_path / "missing.json")
    ex.load()
    ex.replace_all(parse_exchange_info(exchange_info_payload()), "test")
    assert ex.known("ETHUSDT") and len(ex) == 4
    unknown = ex.get("NEWUSDT", 3.0)
    assert unknown.source == "heuristic" and unknown.step_size == Decimal("0.1")
    learned = ex.learn_coarser_qty("NEWUSDT", unknown)
    assert learned.step_size == Decimal("1") and learned.source == "learned"
    ex2 = ExchangeInfo(tmp_path, tmp_path / "missing.json")
    ex2.load()
    assert ex2.get("NEWUSDT", 3.0).step_size == Decimal("1")
    assert SymbolRules.from_json(ex2.get("ETHUSDT").to_json()).tick_size == Decimal("0.01")
    ex2.apply_contract_info({"e": "contractInfo", "s": "ETHUSDT", "cs": "SETTLING", "bks": [{"ma": 75}, {"ma": 50}]})
    assert ex2.tradable("ETHUSDT")[0] is False and ex2.get("ETHUSDT").max_leverage == 75


def test_heuristic_rules_follow_price_tiers():
    assert heuristic_rules("A", 1.5, 5).step_size == Decimal("1")
    assert heuristic_rules("A", 50, 5).step_size == Decimal("0.1")
    assert heuristic_rules("A", 5000, 5).step_size == Decimal("0.001")


# ── Grid ──────────────────────────────────────────────────────────────────
def test_grid_levels():
    cfg = GridConfig.from_dict({"symbol": "GRIDUSDT", "lower": 90, "upper": 110, "grids": 4, "investment": 100})
    assert [fmt(x) for x in compute_levels(cfg, Decimal("0.01"))] == ["90", "95", "100", "105", "110"]
    geo = GridConfig.from_dict({"symbol": "GRIDUSDT", "lower": 100, "upper": 400, "grids": 2, "investment": 100,
                                "spacing": "GEOMETRIC"})
    assert [fmt(x) for x in compute_levels(geo, Decimal("0.01"))] == ["100", "200", "400"]
    with pytest.raises(GridError):
        compute_levels(GridConfig.from_dict({"symbol": "X", "lower": 1, "upper": 1.01, "grids": 50, "investment": 10}),
                       Decimal("0.01"))
    with pytest.raises(GridError):
        GridConfig.from_dict({"symbol": "X", "lower": 10, "upper": 5, "grids": 5, "investment": 10})


@pytest.mark.parametrize("mode,expected", [
    ("LONG", [("LONG", "WAIT_OPEN"), ("LONG", "WAIT_OPEN"), ("LONG", "HOLD"), ("LONG", "HOLD")]),
    ("SHORT", [("SHORT", "HOLD"), ("SHORT", "HOLD"), ("SHORT", "WAIT_OPEN"), ("SHORT", "WAIT_OPEN")]),
    ("NEUTRAL", [("LONG", "WAIT_OPEN"), ("LONG", "WAIT_OPEN"), ("SHORT", "WAIT_OPEN"), ("SHORT", "WAIT_OPEN")]),
])
def test_grid_cells_by_mode(mode, expected):
    cfg = GridConfig.from_dict({"symbol": "GRIDUSDT", "lower": 90, "upper": 110, "grids": 4, "investment": 100, "mode": mode})
    levels = compute_levels(cfg, Decimal("0.01"))
    cells = GridManager._build_cells(None, cfg, levels, Decimal("1"), 100.0)
    assert [(c.kind, c.state) for c in cells] == expected
    assert GridManager._initial_hold_count(cfg, levels, 100.0) == sum(1 for _, s in expected if s == "HOLD")
