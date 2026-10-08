#!/usr/bin/env python3
"""Actualiza ``exchangeInfo.txt``, la base predefinida de reglas de todos los símbolos.

Uso (en una máquina con acceso a Binance):

    python tools/update_exchange_info.py
    python tools/update_exchange_info.py --proxy http://user:pass@host:80
    python tools/update_exchange_info.py --brackets      # + leverage máximo (requiere API key)

Guarda la respuesta cruda de ``/fapi/v1/exchangeInfo`` (el mismo formato que
devuelve Binance) en ``exchangeInfo.txt`` en la raíz del repositorio. Hace UNA
petición pública (peso 1) y, con ``--brackets``, una firmada a
``/fapi/v1/leverageBracket`` que se añade bajo la clave ``_leverageBrackets``.
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
from executor.exchange_info import parse_exchange_info  # noqa: E402


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
                payload["_leverageBrackets"] = await rest.leverage_brackets()
                print(f"Leverage máximo incluido para {len(payload['_leverageBrackets'])} símbolos")
            except Exception as exc:
                print(f"No se pudieron leer los brackets ({exc}); se continúa sin ellos", file=sys.stderr)
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        trading = sum(1 for r in rules.values() if r.status == "TRADING")
        print(f"OK: {len(rules)} símbolos ({trading} en TRADING) → {out}")
        return 0
    finally:
        await rest.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
