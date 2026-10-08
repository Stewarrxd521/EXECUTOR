# Futures Executor v3 — Binance USDⓈ-M

Servicio que recibe las señales de `app.py` y las ejecuta en Binance Futuros USDⓈ-M, con **bots Grid**, un
**lector y solucionador de códigos de error** y un **dashboard en tiempo real estilo Binance**.

```bash
pip install -r requirements.txt
python futures_executor.py          # dashboard en http://localhost:10000
```

## Arquitectura: WebSocket primero

| Necesidad | Canal | Notas |
| --- | --- | --- |
| Abrir / cerrar / cancelar órdenes | **WebSocket API** (`order.place`, `order.cancel`, `order.status`) | Una conexión persistente, peticiones concurrentes |
| TP / SL | **WebSocket API** (`algoOrder.place`, `algoOrder.cancel`) | `STOP_MARKET` / `TAKE_PROFIT_MARKET` por mark price |
| Posiciones, balance, fills, TP/SL disparados | **User Data Stream** (push) | `ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE`, `ALGO_UPDATE`… sin polling |
| listenKey | **WebSocket API** (`userDataStream.start/ping`) | Sin REST |
| Leverage actual de cada símbolo y modo Hedge/One-way | **WebSocket API** (`account.position`) | Al arrancar; luego `ACCOUNT_CONFIG_UPDATE` |
| Precios de **todos** los símbolos | **1 stream** `!markPrice@arr@1s` + `!miniTicker@arr` + `!contractInfo` | Precio siempre fresco: ya no hay que suscribir/reconectar por símbolo |
| Reglas de cada símbolo (tick, step, mínimos) | **exchangeInfo local** | Cero consultas al operar |
| Cambiar leverage, tipo de margen, modo de posición, margen aislado | REST | Binance no ofrece estos métodos por WebSocket |
| Lectura inicial de órdenes abiertas | REST (1 vez al arrancar) | Desactivable con `SEED_OPEN_ORDERS_REST=false` |

El cambio de leverage solo se envía si el símbolo no tiene ya el leverage deseado (la caché sale de
`account.position`), así que en la práctica una apertura **no hace ninguna llamada REST**.
La pestaña *Sistema* del dashboard muestra cuántas llamadas REST se han hecho y a qué rutas.

Código en el paquete `executor/`:

| Módulo | Responsabilidad |
| --- | --- |
| `binance_api.py` | Cliente WebSocket API (firma, reconexión, latencia, rate limits) y REST mínimo con rotación de proxies |
| `streams.py` | Stream de mercado global y User Data Stream |
| `account.py` | Estado de cuenta alimentado por eventos |
| `exchange_info.py` | Reglas por símbolo, redondeo exacto y aprendizaje |
| `errors.py` | Catálogo de códigos de error, diagnóstico y registro |
| `orders.py` | Envío de órdenes con autocorrección |
| `trading.py` | Señales de `app.py`, operaciones manuales, TP/SL, detección de cierres externos |
| `grid.py` | Bots Grid |
| `service.py` | Orquestación y comandos del dashboard |
| `web/` | Servidor HTTP/WebSocket y dashboard |

## exchangeInfo predefinido

Las reglas de todos los símbolos (tickSize, stepSize, minQty, maxQty, notional mínimo, banda PERCENT_PRICE, máximo de
órdenes y, si se incluye, leverage máximo) se leen de archivos locales, sin consultar Binance:

1. **`exchangeInfo.txt`** (raíz del repositorio): la respuesta cruda de `/fapi/v1/exchangeInfo`. Es la base que funciona
   aunque Binance bloquee la IP. Otra ruta: `EXCHANGE_INFO_FILE`.
2. `DATA_DIR/exchange_info.json`: si el executor consigue descargar una versión más nueva, la guarda aquí. Siempre se
   usa **la más reciente** de las dos (por la fecha `serverTime` / `generated_at`).
3. Solo si no hay ninguna, se descarga una vez al arrancar (`EXCHANGE_INFO_BOOTSTRAP=true`), reintentando con espera.
4. Para renovar la base: `python tools/update_exchange_info.py --brackets` (acepta `--proxy`) reescribe
   `exchangeInfo.txt` en el mismo formato de Binance; luego haz commit.

Mantenimiento automático sin REST:

- El stream `!contractInfo` actualiza el estado de cada contrato (TRADING, SETTLING, CLOSE…) y su leverage máximo.
- Si un símbolo nuevo no está en el snapshot se usan reglas aproximadas por precio; si Binance rechaza la precisión
  (-1111 / -4014) el executor **aprende** el paso correcto y lo guarda en `exchange_info_learned.json`.
- `EXCHANGE_INFO_REFRESH_H` > 0 refresca el snapshot cada N horas (desactivado por defecto) y el dashboard tiene un
  botón *Actualizar exchangeInfo*.

## Lector y solucionador de códigos de error

`executor/errors.py` cataloga más de 120 códigos de Binance (incluidos HTTP 403/418/429/451/503) con su significado
en español, la causa típica, la solución y la **corrección automática** que aplica el executor:

| Error | Corrección automática |
| --- | --- |
| -1111, -4023, -4030 precisión | Redondea al step real y aprende el paso si el símbolo no estaba en el snapshot |
| -4014, -4029 tick | Redondea el precio al tick |
| -4164 / -1013 notional mínimo | Sube la cantidad al mínimo exigido (y aprende el mínimo que indica Binance) |
| -4061, -4060 positionSide | Detecta el modo real (Hedge/One-way) y reenvía |
| -1106 reduceOnly en Hedge | Quita `reduceOnly` y reenvía |
| -1006, -1007, 503 estado desconocido | Consulta `order.status` antes de reenviar: nunca duplica la orden |
| -1021 / -5028 reloj | Recalcula el desfase con los eventos del stream y reintenta |
| -1003, 418, 429 rate limit | Pausa ese canal hasta que expire el ban; REST rota a otro proxy |
| -4028, -4202, -4203 leverage | Baja por la escalera de leverage hasta uno aceptado |
| -4131 PERCENT_PRICE en MARKET | Convierte a LIMIT IOC dentro de la banda |
| -2022, -2024 reduceOnly rechazado | Relee la posición; si ya está en 0 la da por cerrada |
| -2019 margen insuficiente | Registra la posición como *asumida* (comportamiento anterior) o, con `MARGIN_AUTO_REDUCE=true`, reintenta con menos cantidad |
| -2011, -2013, -4046, -4059 | Ya estaba hecho: no es un error real |

La pestaña **Errores** del dashboard muestra cada error con su explicación, si se autocorrigió y cómo, e incluye un
**lector de códigos** (escribe `-2019`, `-1111`, `418`…) y el catálogo completo filtrable.

## Bots Grid

Grids de futuros **Long, Short y Neutral**, aritméticos o geométricos:

- Una orden límite viva por celda; al llenarse se coloca la contraria un grid más allá (ganancia = 1 grid).
- Long: las celdas por encima del precio arrancan con inventario comprado a mercado. Short: al revés.
  Neutral: compras bajo el precio y ventas encima, sin posición inicial.
- Vista previa antes de crear: beneficio por grid (bruto y neto de comisiones), cantidad por grid, margen requerido,
  inversión mínima y avisos.
- Stop loss / take profit del bot, precio de activación, opción post-only y cierre de posición al detener.
- Fills en tiempo real por el User Data Stream. El estado se guarda en `DATA_DIR/grids.json` y al reiniciar se
  reconcilia con `order.status` (las órdenes llenadas mientras estaba apagado se procesan).
- Un grid y las operaciones por señal no comparten símbolo/dirección (se protege en ambos sentidos).

## Dashboard

`GET /` — diseño inspirado en Binance Futures (tema oscuro/claro, responsive):

- Lista de mercados en vivo (búsqueda, favoritos, orden por volumen/cambio), barra del símbolo (mark, índice, funding,
  máximos/mínimos, reglas) y gráfico de mark price con entrada, TP, SL, liquidación y niveles del grid.
- Panel para abrir posiciones (mercado/límite, tamaño en USDT/margen/cantidad, leverage, TP/SL por ROE) y crear bots Grid.
- Pestañas: posiciones (cierre, TP/SL, margen, cierre límite), órdenes abiertas, bots Grid, historial, señales,
  errores y sistema (conexiones, latencia, rate limits, REST usado, reloj, exchangeInfo y log en vivo).
- Todo por **un WebSocket** (`/ws`): el estado llega empujado cada segundo y los comandos viajan por el mismo socket.
- **Se entra directamente con el link**, sin usuario ni token. Si algún día quieres protegerlo, define
  `DASHBOARD_TOKEN` y el dashboard pedirá ese token.

## Señales (compatibles con `app.py`)

`POST /signal` con el header `X-Signal-Secret` (el mismo que ya envía `app.py`), o por WebSocket en `/ws/signal` (header `X-Signal-Secret`, o un primer
mensaje `{"secret": "..."}`; cada mensaje recibe respuesta y puede llevar `id`).

```json
{"action": "open", "trade_id": 123, "symbol": "SOLUSDT", "direction": "LONG", "price": 150.2, "quantity": 0.5}
{"action": "close", "trade_id": 123, "symbol": "SOLUSDT", "direction": "LONG", "reason": "TP"}
{"action": "close_all"}
{"action": "open_tp", "symbol": "SOLUSDT", "direction": "LONG", "trigger_price": 160}
{"action": "open_sl", "symbol": "SOLUSDT", "direction": "LONG", "trigger_price": 140}
{"action": "close_tp", "symbol": "SOLUSDT"}
{"action": "close_sl", "symbol": "SOLUSDT"}
```

- Tamaño de apertura: `quantity`/`qty` (con `price` define el notional deseado, como antes), o `notional`/`usdt`, o
  `margin`. Sin ninguno se rechaza, salvo que `DEFAULT_NOTIONAL_USDT` > 0.
- La cantidad final se calcula con el precio real y las reglas del símbolo (redondeo exacto con `Decimal`).
- Se mantiene la regla de leverage por precio: `HIGH_PRICE_LEVERAGE` si el precio supera `HIGH_PRICE_THRESHOLD`.
- Los cierres por TP/SL de Binance, liquidaciones o cierres manuales en la app se **detectan en tiempo real** y la
  operación se cierra localmente con su motivo y el PnL real reportado por Binance.

Otros endpoints: `GET /api/state` (mismo formato que antes, lo consume `app.py`), `POST /api/command`
(comandos del dashboard; pide `X-Dashboard-Token` solo si definiste `DASHBOARD_TOKEN`) y `GET /health` (JSON con el estado de las conexiones).

## Variables de entorno

| Variable | Defecto | Uso |
| --- | --- | --- |
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | — | Credenciales (Futuros habilitado) |
| `USE_TESTNET` | `false` | Testnet de Binance Futures |
| `SIGNAL_SECRET` | `cambiar-por-secreto-seguro` | Secreto que `app.py` envía en `X-Signal-Secret` (igual que antes). No afecta al dashboard |
| `DASHBOARD_TOKEN` | — | Opcional. Vacío = dashboard abierto con el link; con valor, pide ese token |
| `PORT` | `10000` | Puerto HTTP |
| `PROXY_URLS` | — | Proxies HTTP separados por coma para REST (rotación y baneo por IP). `FIXIE_URL` sigue aceptándose |
| `REST_PROXY_ALL` | `false` | Usar los proxies para todo el REST. Por defecto: leverage y exchangeInfo por proxy; el resto directo, pasando a un proxy si Binance bloquea la IP (403/451/-2015) |
| `LEVERAGE` / `DEFAULT_LEVERAGE` | `4` | Leverage de las señales (editable desde el dashboard) |
| `HIGH_PRICE_THRESHOLD` / `HIGH_PRICE_LEVERAGE` | `2.0` / `20` | Leverage para símbolos con precio alto |
| `HEDGE_MODE` | `false` | Solo pista inicial: el modo real se detecta por WebSocket |
| `BLOCKED_SYMBOLS` | `BTCUSDT,ETHUSDT,BTCUSDC,ETHUSDC` | Símbolos que no se abren por señal |
| `MIN_NOTIONAL_USDT` / `NOTIONAL_SAFETY_BUFFER_PCT` | `5.1` / `2.0` | Notional mínimo y colchón |
| `MAX_NOTIONAL_OVERSHOOT_X` | `4.0` | Tope al corregir precisión (no abrir algo desproporcionado) |
| `DEFAULT_NOTIONAL_USDT` | `0` | Tamaño por defecto si la señal no trae cantidad (0 = rechazar) |
| `ASSUME_ON_MARGIN_ERROR` | `true` | Con -2019 registrar la posición como asumida |
| `MARGIN_AUTO_REDUCE` | `false` | Con -2019 reintentar con la mitad de cantidad |
| `SIGNAL_DEDUPE_TTL_S` | `10` | Ventana anti-duplicados de señales |
| `MAX_PRICE_AGE_S` | `5` | Antigüedad máxima del precio de referencia |
| `DATA_DIR` | `data` | Estado persistente (trades, grids, exchangeInfo descargado) |
| `EXCHANGE_INFO_FILE` | `exchangeInfo.txt` | Base predefinida de reglas (respuesta cruda de Binance) |
| `EXCHANGE_INFO_BOOTSTRAP` | `true` | Generar el exchangeInfo si no hay snapshot |
| `EXCHANGE_INFO_REFRESH_H` | `0` | Refresco periódico del exchangeInfo (horas, 0 = nunca) |
| `SEED_OPEN_ORDERS_REST` | `true` | Leer órdenes abiertas al arrancar |
| `STATE_REQUIRES_AUTH` | `false` | Exigir token en `/api/state` |
| `GRID_MAX_LEVELS` / `GRID_ORDER_CONCURRENCY` | `150` / `4` | Límites de los bots Grid |
| `BALANCE_POLL_S` | `60` | Refresco de seguridad del balance (WS API) |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | — | Avisos por Telegram |
| `BINANCE_WS_FAPI_URL` / `BINANCE_STREAM_URL` / `BINANCE_REST_FAPI_URL` | producción o testnet | Sobrescribir endpoints |
| `LOG_LEVEL` | `INFO` | Nivel de log |

## Pruebas

```bash
pip install -r requirements-dev.txt
pytest
```

Las pruebas de integración levantan un Binance simulado (`tests/fake_binance.py`) con WS API, streams y REST mínimo,
y cubren: apertura/cierre por señal, autocorrección de errores (-1111, -4164, -4061, estado desconocido), TP/SL por
algo orders, grids Neutral y Long, reconciliación tras reinicio, persistencia, dashboard por WebSocket y `/ws/signal`.

## Cambios respecto a la versión anterior

- Las credenciales del proxy Fixie ya no están en el código: configura `PROXY_URLS` (o `FIXIE_URL`). Conviene rotar
  esa contraseña, porque estuvo publicada en el historial del repositorio.
- El dashboard se abre con el link, como antes, pero el secreto de señales ya no viaja dentro del HTML.
- `GET /health` devuelve JSON con el estado de las conexiones (sigue respondiendo 200).
- Si el cierre de una posición falla en Binance, la operación sigue abierta y se avisa (antes se marcaba cerrada).
- Los cierres por TP/SL/liquidación hechos por Binance se detectan y registran automáticamente.
- El win rate cuenta como ganadas las operaciones con PnL > 0.
