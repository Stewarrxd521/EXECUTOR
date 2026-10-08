#!/usr/bin/env python3
"""Genera el exchangeInfo predefinido de todos los símbolos USDⓈ-M.

Uso (en una máquina con acceso a Binance):

    python tools/update_exchange_info.py
    python tools/update_exchange_info.py --proxy http://user:pass@host:80
    python tools/update_exchange_info.py --brackets      # + leverage máximo (requiere API key)
    python tools/update_exchange_info.py --out data/exchange_info.json

Por defecto escribe ``executor/data/exchange_info.json`` (el snapshot
empaquetado con el código). Hace UNA petición pública a
``/fapi/v1/exchangeInfo`` (peso 1) y, con ``--brackets``, una firmada a
``/fapi/v1/leverageBracket``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from executor.binance_api import RestClient, ServerClock  # noqa: E402
from executor.config import BUNDLED_EXCHANGE_INFO  # noqa: E402
from executor.exchange_info import apply_brackets, build_snapshot, parse_exchange_info  # noqa: E402


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", default=str(BUNDLED_EXCHANGE_INFO), help="ruta de salida")
    parser.add_argument("--proxy", action="append", default=[], help="proxy HTTP (repetible)")
    parser.add_argument("--testnet", action="store_true", help="usar testnet")
    parser.add_argument("--brackets", action="store_true", help="incluir leverage máximo (requiere API key)")
    args = parser.parse_args()

    base = "https://testnet.binancefuture.com" if args.testnet else "https://fapi.binance.com"
    proxies = args.proxy or [p.strip() for p in os.environ.get("PROXY_URLS", "").split(",") if p.strip()]
    rest = RestClient(base, os.environ.get("BINANCE_API_KEY", ""), os.environ.get("BINANCE_API_SECRET", ""),
                      ServerClock(), proxies)
    try:
        payload = await rest.exchange_info()
        rules = parse_exchange_info(payload)
        if not rules:
            print("La respuesta no contiene símbolos USDT/USDC", file=sys.stderr)
            return 1
        if args.brackets:
            try:
                n = apply_brackets(rules, await rest.leverage_brackets())
                print(f"Leverage máximo aplicado a {n} símbolos")
            except Exception as exc:
                print(f"No se pudieron leer los brackets ({exc}); se continúa sin ellos", file=sys.stderr)
        snapshot = build_snapshot(rules, "fapi/v1/exchangeInfo" + (" + leverageBracket" if args.brackets else ""))
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(snapshot, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        trading = sum(1 for r in rules.values() if r.status == "TRADING")
        print(f"OK: {len(rules)} símbolos ({trading} en TRADING) → {out}")
        return 0
    finally:
        await rest.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
