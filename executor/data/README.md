Aquí va `exchange_info.json`, el snapshot predefinido de reglas de todos los símbolos USDⓈ-M.

Generarlo (en una máquina con acceso a Binance) y commitearlo:

    python tools/update_exchange_info.py --brackets

Si no existe, el executor lo crea automáticamente en `DATA_DIR/exchange_info.json` al primer arranque.
