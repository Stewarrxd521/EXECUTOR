"""Binance USDⓈ-M simulado para pruebas de extremo a extremo.

Implementa lo que usa el executor: WS API (órdenes, algo orders, cuenta,
listenKey), stream de mercado (``/market/stream``), User Data Stream
(``/private/stream``) y el REST mínimo (leverage, órdenes abiertas,
exchangeInfo). Las órdenes MARKET se llenan al mark; las LIMIT quedan en el
libro hasta que el mark las cruza; los TP/SL se disparan por mark price.
"""

from __future__ import annotations

import asyncio
import json
import time
from decimal import Decimal
from typing import Optional

from aiohttp import WSMsgType, web

RULES = {
    "BTCUSDT": {"tick": "0.10", "step": "0.001", "min_notional": "100"},
    "ETHUSDT": {"tick": "0.01", "step": "0.001", "min_notional": "20"},
    "TESTUSDT": {"tick": "0.001", "step": "1", "min_notional": "5"},
    "GRIDUSDT": {"tick": "0.01", "step": "0.1", "min_notional": "5"},
}


def exchange_info_payload() -> dict:
    symbols = []
    for sym, r in RULES.items():
        symbols.append({
            "symbol": sym, "pair": sym, "contractType": "PERPETUAL", "status": "TRADING",
            "baseAsset": sym[:-4], "quoteAsset": "USDT", "marginAsset": "USDT",
            "pricePrecision": 4, "quantityPrecision": 3, "onboardDate": 1569398400000,
            "filters": [
                {"filterType": "PRICE_FILTER", "minPrice": "0.001", "maxPrice": "1000000", "tickSize": r["tick"]},
                {"filterType": "LOT_SIZE", "minQty": r["step"], "maxQty": "100000", "stepSize": r["step"]},
                {"filterType": "MARKET_LOT_SIZE", "minQty": r["step"], "maxQty": "50000", "stepSize": r["step"]},
                {"filterType": "MAX_NUM_ORDERS", "limit": 200},
                {"filterType": "MAX_NUM_ALGO_ORDERS", "limit": 10},
                {"filterType": "MIN_NOTIONAL", "notional": r["min_notional"]},
                {"filterType": "PERCENT_PRICE", "multiplierUp": "1.0500", "multiplierDown": "0.9500", "multiplierDecimal": "4"},
            ],
        })
    return {"timezone": "UTC", "serverTime": int(time.time() * 1000), "symbols": symbols}


class FakeBinance:
    def __init__(self, hedge: bool = True, true_steps: Optional[dict] = None):
        self.hedge = hedge
        self.marks: dict[str, float] = {"BTCUSDT": 65000.0, "ETHUSDT": 3000.0, "TESTUSDT": 0.5,
                                        "GRIDUSDT": 100.0, "NEWUSDT": 3.0}
        # Paso real por símbolo (para simular símbolos fuera del snapshot).
        self.true_steps = {s: Decimal(r["step"]) for s, r in RULES.items()}
        self.true_steps.update({k: Decimal(v) for k, v in (true_steps or {}).items()})
        self.positions: dict[tuple[str, str], dict] = {}
        self.orders: dict[int, dict] = {}
        self.algos: dict[int, dict] = {}
        self.leverage: dict[str, int] = {s: 20 for s in self.marks}
        self.wallet = 10_000.0
        self.seq = 1000
        self.user_ws: set[web.WebSocketResponse] = set()
        self.inject: list[tuple[str, int, str]] = []
        self.requests: list[tuple[str, dict]] = []
        self.rest_calls: list[str] = []
        self._tasks: list[asyncio.Task] = []
        self.runner: Optional[web.AppRunner] = None
        self.port = 0

    # ── Servidor ──────────────────────────────────────────────────────────
    async def start(self) -> None:
        app = web.Application()
        app.router.add_get("/ws-fapi/v1", self._ws_api)
        app.router.add_get("/market/stream", self._market)
        app.router.add_get("/private/stream", self._private)
        app.router.add_post("/fapi/v1/leverage", self._rest_leverage)
        app.router.add_get("/fapi/v1/openOrders", self._rest_open_orders)
        app.router.add_get("/fapi/v1/openAlgoOrders", self._rest_open_algos)
        app.router.add_get("/fapi/v1/exchangeInfo", self._rest_exinfo)
        app.router.add_get("/fapi/v1/leverageBracket", self._rest_brackets)
        app.router.add_get("/fapi/v1/positionSide/dual", self._rest_position_mode)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        for ws in list(self.user_ws):
            await ws.close()
        if self.runner:
            await self.runner.cleanup()

    @property
    def http(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    @property
    def ws(self) -> str:
        return f"ws://127.0.0.1:{self.port}"

    # ── Utilidades ────────────────────────────────────────────────────────
    def _next(self) -> int:
        self.seq += 1
        return self.seq

    def set_mark(self, symbol: str, price: float) -> None:
        self.marks[symbol] = price
        self._match(symbol)

    async def _push(self, event: dict) -> None:
        text = json.dumps({"stream": "lk", "data": event})
        for ws in list(self.user_ws):
            try:
                await ws.send_str(text)
            except ConnectionError:
                pass

    def push(self, event: dict) -> None:
        self._tasks.append(asyncio.create_task(self._push(event)))

    def position_rows(self) -> list[dict]:
        rows = []
        for sym in self.marks:
            sides = ("LONG", "SHORT") if self.hedge else ("BOTH",)
            for ps in sides:
                p = self.positions.get((sym, ps), {"amt": 0.0, "entry": 0.0})
                rows.append({"symbol": sym, "positionSide": ps, "positionAmt": str(p["amt"]),
                             "entryPrice": str(p["entry"]), "breakEvenPrice": str(p["entry"]),
                             "unRealizedProfit": "0", "liquidationPrice": "0", "leverage": str(self.leverage.get(sym, 20)),
                             "marginType": "cross", "isolatedWallet": "0", "markPrice": str(self.marks[sym])})
        return rows

    # ── Motor de órdenes ──────────────────────────────────────────────────
    def _apply_fill(self, order: dict, price: float, qty: float) -> float:
        sym, ps, side = order["symbol"], order["positionSide"], order["side"]
        key = (sym, ps)
        pos = self.positions.setdefault(key, {"amt": 0.0, "entry": 0.0})
        delta = qty if side == "BUY" else -qty
        realized = 0.0
        if pos["amt"] == 0 or (pos["amt"] > 0) == (delta > 0):
            new = pos["amt"] + delta
            pos["entry"] = (abs(pos["amt"]) * pos["entry"] + qty * price) / abs(new)
            pos["amt"] = new
        else:
            closing = min(abs(delta), abs(pos["amt"]))
            sign = 1 if pos["amt"] > 0 else -1
            realized = (price - pos["entry"]) * closing * sign
            pos["amt"] = round(pos["amt"] + delta, 10)
            if abs(pos["amt"]) < 1e-12:
                pos["amt"], pos["entry"] = 0.0, 0.0
            elif (pos["amt"] > 0) != (sign > 0):
                pos["entry"] = price
        self.wallet += realized - price * qty * 0.0004
        order["executedQty"] = float(order.get("executedQty", 0)) + qty
        order["avgPrice"] = price
        order["status"] = "FILLED"
        self.push(self._otu(order, "TRADE", last_qty=qty, last_price=price, realized=realized))
        self.push({"e": "ACCOUNT_UPDATE", "E": int(time.time() * 1000), "a": {
            "m": "ORDER", "B": [{"a": "USDT", "wb": str(self.wallet), "cw": str(self.wallet), "bc": "0"}],
            "P": [{"s": sym, "pa": str(pos["amt"]), "ep": str(pos["entry"]), "bep": str(pos["entry"]), "cr": "0",
                   "up": "0", "mt": "cross", "iw": "0", "ps": ps}]}})
        return realized

    def _otu(self, o: dict, exec_type: str, last_qty: float = 0.0, last_price: float = 0.0, realized: float = 0.0) -> dict:
        return {"e": "ORDER_TRADE_UPDATE", "E": int(time.time() * 1000), "T": int(time.time() * 1000), "o": {
            "s": o["symbol"], "c": o["clientOrderId"], "S": o["side"], "o": o["type"], "ot": o.get("origType", o["type"]),
            "f": o.get("timeInForce", "GTC"), "q": str(o["origQty"]), "p": str(o.get("price", 0)),
            "ap": str(o.get("avgPrice", 0)), "sp": "0", "x": exec_type, "X": o["status"], "i": o["orderId"],
            "l": str(last_qty), "z": str(o.get("executedQty", 0)), "L": str(last_price), "N": "USDT",
            "n": str(last_price * last_qty * 0.0004), "T": int(time.time() * 1000), "t": self._next(),
            "m": o["type"] == "LIMIT", "R": o.get("reduceOnly", False), "ps": o["positionSide"], "cp": False,
            "rp": str(realized)}}

    def _match(self, symbol: str) -> None:
        mark = self.marks[symbol]
        for oid, o in list(self.orders.items()):
            if o["symbol"] != symbol or o["status"] != "NEW":
                continue
            p = float(o["price"])
            if (o["side"] == "BUY" and mark <= p) or (o["side"] == "SELL" and mark >= p):
                del self.orders[oid]
                self._apply_fill(o, p, float(o["origQty"]))
        for aid, a in list(self.algos.items()):
            if a["symbol"] != symbol:
                continue
            trig = float(a["triggerPrice"])
            long_close = a["side"] == "SELL"
            hit = (mark >= trig) if (a["type"] == "TAKE_PROFIT_MARKET") == long_close else (mark <= trig)
            if hit:
                del self.algos[aid]
                self.push({"e": "ALGO_UPDATE", "E": int(time.time() * 1000), "o": {"aid": aid, "caid": a["clientAlgoId"],
                           "s": symbol, "X": "TRIGGERED", "o": a["type"], "S": a["side"], "ps": a["positionSide"],
                           "tp": a["triggerPrice"], "q": a.get("quantity", "0")}})
                pos = self.positions.get((symbol, a["positionSide"]), {"amt": 0.0})
                qty = abs(pos["amt"]) if not a.get("quantity") else min(abs(pos["amt"]), float(a["quantity"]))
                if qty > 0:
                    order = {"symbol": symbol, "side": a["side"], "type": "MARKET", "origType": a["type"],
                             "positionSide": a["positionSide"], "origQty": qty, "orderId": self._next(),
                             "clientOrderId": f"algo{aid}", "status": "NEW", "reduceOnly": True}
                    self._apply_fill(order, mark, qty)

    # ── WS API ────────────────────────────────────────────────────────────
    async def _ws_api(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            req = json.loads(msg.data)
            method, params = req["method"], req.get("params", {})
            self.requests.append((method, params))
            try:
                for i, (m, code, text) in enumerate(self.inject):
                    if m == method:
                        self.inject.pop(i)
                        raise _Err(code, text)
                result = self._dispatch(method, params)
                await ws.send_str(json.dumps({"id": req["id"], "status": 200, "result": result,
                                              "rateLimits": [{"rateLimitType": "REQUEST_WEIGHT", "interval": "MINUTE",
                                                              "intervalNum": 1, "limit": 2400, "count": len(self.requests)}]}))
            except _Err as e:
                await ws.send_str(json.dumps({"id": req["id"], "status": 400, "error": {"code": e.code, "msg": e.msg}}))
        return ws

    def _dispatch(self, method: str, p: dict):
        if method == "account.position":
            return self.position_rows()
        if method == "v2/account.balance":
            return [{"asset": "USDT", "balance": str(self.wallet), "crossWalletBalance": str(self.wallet),
                     "availableBalance": str(self.wallet * 0.9), "crossUnPnl": "0"}]
        if method in ("userDataStream.start", "userDataStream.ping"):
            return {"listenKey": "lk"}
        if method == "ticker.price":
            return {"symbol": p["symbol"], "price": str(self.marks.get(p["symbol"], 0))}
        if method == "order.place":
            return self._place(p)
        if method == "order.cancel":
            oid = int(p.get("orderId") or 0)
            o = self.orders.pop(oid, None)
            if o is None:
                o = next((x for x in self.orders.values() if x["clientOrderId"] == p.get("origClientOrderId")), None)
                if o:
                    self.orders.pop(o["orderId"])
            if o is None:
                raise _Err(-2011, "Unknown order sent.")
            o["status"] = "CANCELED"
            self.push(self._otu(o, "CANCELED"))
            return {"orderId": o["orderId"], "status": "CANCELED"}
        if method == "order.status":
            oid = int(p.get("orderId") or 0)
            o = self.orders.get(oid) or next((x for x in self.orders.values()
                                              if x["clientOrderId"] == p.get("origClientOrderId")), None)
            if o is None:
                raise _Err(-2013, "Order does not exist.")
            return dict(o)
        if method == "algoOrder.place":
            self._check_side(p)
            aid = self._next()
            algo = {"algoId": aid, "clientAlgoId": p.get("clientAlgoId", f"a{aid}"), "symbol": p["symbol"],
                    "side": p["side"], "positionSide": p.get("positionSide", "BOTH"), "type": p["type"],
                    "triggerPrice": p["triggerPrice"], "quantity": p.get("quantity"), "algoStatus": "NEW"}
            self.algos[aid] = algo
            self.push({"e": "ALGO_UPDATE", "E": int(time.time() * 1000), "o": {"aid": aid, "caid": algo["clientAlgoId"],
                       "s": algo["symbol"], "X": "NEW", "o": algo["type"], "S": algo["side"], "ps": algo["positionSide"],
                       "tp": algo["triggerPrice"], "q": algo["quantity"] or "0"}})
            return {**algo, "orderType": algo["type"]}
        if method == "algoOrder.cancel":
            aid = int(p.get("algoId") or 0)
            if aid not in self.algos:
                raise _Err(-2011, "Unknown order sent.")
            a = self.algos.pop(aid)
            self.push({"e": "ALGO_UPDATE", "E": int(time.time() * 1000), "o": {"aid": aid, "s": a["symbol"], "X": "CANCELED",
                       "o": a["type"], "S": a["side"], "ps": a["positionSide"], "tp": a["triggerPrice"]}})
            return {"algoId": aid, "code": "200"}
        raise _Err(-1020, f"método no soportado por el simulador: {method}")

    def _check_side(self, p: dict) -> None:
        ps = p.get("positionSide", "BOTH")
        if self.hedge and ps == "BOTH" or (not self.hedge and ps in ("LONG", "SHORT")):
            raise _Err(-4061, "Order's position side does not match user's setting.")
        if ps in ("LONG", "SHORT") and p.get("reduceOnly"):
            raise _Err(-1106, "Parameter 'reduceonly' sent when not required.")

    def _place(self, p: dict) -> dict:
        sym = p["symbol"]
        if sym not in self.marks:
            raise _Err(-1121, "Invalid symbol.")
        self._check_side(p)
        qty = Decimal(p["quantity"])
        step = self.true_steps.get(sym, Decimal("1"))
        if qty % step != 0:
            raise _Err(-1111, "Precision is over the maximum defined for this asset.")
        price = float(p.get("price", 0) or 0)
        ref = price or self.marks[sym]
        min_notional = float(RULES.get(sym, {}).get("min_notional", 5))
        if float(qty) * ref < min_notional and not p.get("reduceOnly") and not self._is_closing(p):
            raise _Err(-4164, f"Order's notional must be no smaller than {min_notional:g} (unless you choose reduce only).")
        oid = self._next()
        order = {"orderId": oid, "clientOrderId": p.get("newClientOrderId", f"c{oid}"), "symbol": sym,
                 "side": p["side"], "type": p["type"], "positionSide": p.get("positionSide", "BOTH"),
                 "origQty": float(qty), "price": p.get("price", "0"), "status": "NEW", "executedQty": 0.0,
                 "avgPrice": 0.0, "timeInForce": p.get("timeInForce", "GTC"), "reduceOnly": p.get("reduceOnly") == "true"}
        if p["type"] == "MARKET":
            self.push(self._otu(order, "NEW"))
            self._apply_fill(order, self.marks[sym], float(qty))
            return {**order, "executedQty": str(order["executedQty"]), "avgPrice": str(order["avgPrice"])}
        crosses = (p["side"] == "BUY" and price >= self.marks[sym]) or (p["side"] == "SELL" and price <= self.marks[sym])
        if crosses:
            self._apply_fill(order, self.marks[sym], float(qty))
            return {**order, "executedQty": str(order["executedQty"]), "avgPrice": str(order["avgPrice"])}
        self.orders[oid] = order
        self.push(self._otu(order, "NEW"))
        return dict(order)

    def _is_closing(self, p: dict) -> bool:
        ps = p.get("positionSide", "BOTH")
        return (ps == "LONG" and p["side"] == "SELL") or (ps == "SHORT" and p["side"] == "BUY")

    # ── Streams ───────────────────────────────────────────────────────────
    async def _market(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        try:
            while not ws.closed:
                now = int(time.time() * 1000)
                marks = [{"e": "markPriceUpdate", "E": now, "s": s, "p": str(p), "i": str(p), "r": "0.0001",
                          "T": now + 3600_000} for s, p in self.marks.items()]
                tick = [{"e": "24hrMiniTicker", "E": now, "s": s, "c": str(p), "o": str(p * 0.97), "h": str(p * 1.05),
                         "l": str(p * 0.95), "v": "1000", "q": str(p * 1000)} for s, p in self.marks.items()]
                await ws.send_str(json.dumps({"stream": "!markPrice@arr@1s", "data": marks}))
                await ws.send_str(json.dumps({"stream": "!miniTicker@arr", "data": tick}))
                await asyncio.sleep(0.2)
        except (ConnectionError, RuntimeError):
            pass
        return ws

    async def _private(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.user_ws.add(ws)
        try:
            async for _ in ws:
                pass
        finally:
            self.user_ws.discard(ws)
        return ws

    # ── REST ──────────────────────────────────────────────────────────────
    async def _rest_leverage(self, request: web.Request) -> web.Response:
        self.rest_calls.append("leverage")
        sym, lev = request.query["symbol"], int(request.query["leverage"])
        if lev > 50:
            return web.json_response({"code": -4028, "msg": f"Leverage {lev} is not valid"}, status=400)
        self.leverage[sym] = lev
        self.push({"e": "ACCOUNT_CONFIG_UPDATE", "E": int(time.time() * 1000), "ac": {"s": sym, "l": lev}})
        return web.json_response({"symbol": sym, "leverage": lev, "maxNotionalValue": "1000000"})

    async def _rest_position_mode(self, request: web.Request) -> web.Response:
        self.rest_calls.append("positionSide")
        return web.json_response({"dualSidePosition": self.hedge})

    async def _rest_open_orders(self, request: web.Request) -> web.Response:
        self.rest_calls.append("openOrders")
        return web.json_response([dict(o, time=int(time.time() * 1000)) for o in self.orders.values()])

    async def _rest_open_algos(self, request: web.Request) -> web.Response:
        self.rest_calls.append("openAlgoOrders")
        return web.json_response([dict(a, orderType=a["type"]) for a in self.algos.values()])

    async def _rest_exinfo(self, request: web.Request) -> web.Response:
        self.rest_calls.append("exchangeInfo")
        return web.json_response(exchange_info_payload())

    async def _rest_brackets(self, request: web.Request) -> web.Response:
        self.rest_calls.append("leverageBracket")
        return web.json_response([{"symbol": s, "brackets": [{"initialLeverage": 50}]} for s in RULES])


class _Err(Exception):
    def __init__(self, code: int, msg: str):
        super().__init__(msg)
        self.code, self.msg = code, msg
