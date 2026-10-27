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
| Precios | **1 stream** con suscripción en caliente (`SUBSCRIBE`/`UNSUBSCRIBE`) | Modo `lite` (por defecto): solo los símbolos que hacen falta. Ver «Ancho de banda» |
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
| `streams.py` | Stream de mercado (lite/full) y User Data Stream |
| `account.py` | Estado de cuenta alimentado por eventos |
| `exchange_info.py` | Reglas por símbolo, redondeo exacto y aprendizaje |
| `errors.py` | Catálogo de códigos de error, diagnóstico y registro |
| `orders.py` | Envío de órdenes con autocorrección |
| `trading.py` | Señales de `app.py`, operaciones manuales, TP/SL, detección de cierres externos |
| `grid.py` | Bots Grid |
| `service.py` | Orquestación y comandos del dashboard |
| `web/` | Servidor HTTP/WebSocket y dashboard |

## Ancho de banda (Render)

Render factura el tráfico **de salida**. En las conexiones que abre el executor (Binance, proxy, Telegram,
auto-ping) se factura como **Service-Initiated** todo lo que el executor envía, incluidos los ACK TCP/TLS que
genera cada paquete recibido. Antes el executor recibía el mercado entero cada segundo (`!markPrice@arr@1s` +
`!miniTicker@arr`, ≈ 750 MB/h de entrada para ~785 símbolos), y solo confirmar esos paquetes producía ≈ 5 MB/h
de salida facturable aunque se usaran 2 o 3 símbolos.

Con `MARKET_STREAMS=lite` (por defecto) la conexión de mercado recibe solo:

| Qué | Stream | Frecuencia |
| --- | --- | --- |
| Símbolos con posición o trade abierto | `<símbolo>@markPrice` | 3 s (con más de 300 se usa `!markPrice@arr`, uno solo) |
| Símbolos con bot Grid activo | `<símbolo>@markPrice@1s` | 1 s (con más de 300 se usa `!markPrice@arr@1s`) |
| Símbolo que se mira en el dashboard / API | `<símbolo>@markPrice@1s` + `<símbolo>@miniTicker` | 1 s, hasta 2 min después de la última consulta; siempre los 40 más recientes y, si ya estaban suscritos, hasta 60 |
| Listados y estados de contratos | `!contractInfo` | solo cuando cambian |
| Lista de mercados del dashboard | `!miniTicker@arr` | solo con el dashboard visible: una foto completa (3 envíos) cada 5 min y un refresco de 1 envío cada 30 s |

Las suscripciones cambian en caliente (`SUBSCRIBE`/`UNSUBSCRIBE`, sin reconectar y como mucho 4 mensajes por
segundo; Binance admite 10). Un símbolo que deja de hacer falta se mantiene 60 s y luego se descarta su precio.
Para un símbolo nuevo (señal, grid, consulta) se pide el precio por la WebSocket API mientras llega su stream; un
símbolo que no está en el exchangeInfo solo se suscribe si Binance devuelve precio. Las consultas (`/api/live`,
dashboard) nunca suben la conexión de ~650 streams (Binance admite 1024); las posiciones y grids no tienen tope.
`/api/live` solo da `last`/`change_pct` si el ticker del símbolo llega en vivo (no de la foto de la lista de
mercados). Si los precios dejan de llegar 20 s con streams activos, la conexión se reinicia; si reconectar no lo
arregla (p. ej. un símbolo deslistado que sigue en el exchangeInfo local), la espera se dobla cada vez hasta 15 min.
Un símbolo que Binance responde como inexistente (`-1121`) deja de suscribirse durante 10 min. Si Binance rechazara `SUBSCRIBE`, se reconecta con la lista
nueva y la foto de mercados va por una conexión aparte de un solo uso. Con 1 posición abierta el executor recibe ≈ 0.25 MB/h
(unas 1 000 veces menos) y la salida facturable baja a ≈ 0.1-0.2 MB/h.

- `MARKET_STREAMS=full` vuelve al comportamiento anterior (todo el mercado cada segundo).
- Las conexiones piden compresión `permessage-deflate` y el auto-ping (`KEEPALIVE_URL`) hace `HEAD /ready`
  (sin cuerpo).
- La pestaña *Sistema* del dashboard y `GET /health` → `network` muestran el tráfico por conexión (MB/h
  recibidos y enviados) y una estimación de lo que factura Render (MB/h y GB/mes).

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
  `margin`. Si llegan `quantity` y `notional`, manda `quantity × price` (se avisa si difieren). Sin ninguno se
  rechaza, salvo que `DEFAULT_NOTIONAL_USDT` > 0.
- La cantidad final se calcula con el precio real y las reglas del símbolo (redondeo exacto con `Decimal`).
- Se mantiene la regla de leverage por precio: `HIGH_PRICE_LEVERAGE` si el precio supera `HIGH_PRICE_THRESHOLD`.
- Los cierres por TP/SL de Binance, liquidaciones o cierres manuales en la app se **detectan en tiempo real** y la
  operación se cierra localmente con su motivo y el PnL real reportado por Binance.

- Varios `open` con el mismo `trade_id` y símbolo son **tramos** de la misma posición (app_25 promedia así): todos
  se ejecutan y quedan en `levels`/`tranches` del trade, aunque lleguen varios en el mismo segundo con el mismo
  contenido. Ningún antiduplicado por contenido se aplica a `open` (las versiones anteriores a la 3.0 descartaban
  como «señal duplicada» los tramos del mismo `trade_id` recibidos en menos de 10 s).
- Si una apertura del bot no se ejecuta (rechazada o fallida en Binance) se avisa en el dashboard y por Telegram.
- Una señal con `signal_id` (o header `Idempotency-Key`) nunca se ejecuta dos veces: así los reintentos son seguros.
- **Cierre por `trade_id` (varios bots en el mismo símbolo).** Binance junta en una sola posición todo lo abierto en
  el mismo símbolo y dirección, pero el executor guarda cuánto abrió cada `trade_id` (su **lote**: la cantidad real
  llenada, con multiplicador incluido y sumando sus tramos).
  - `close` **con `trade_id`** cierra solo el lote de ese `trade_id` (orden reduceOnly). La posición se cierra
    entera solo cuando lo que queda en Binance es justo ese lote. Si la posición tiene más (por ejemplo, otro bot
    abrió en el mismo símbolo por fuera), se cierra solo la cantidad de ese `trade_id`, aunque sea el último.
  - `close` **sin `trade_id`** cierra la posición entera del símbolo (y de la dirección, si se indica).
  - Un `trade_id` que no tiene cantidad en la posición nunca toca los lotes de otros ni lo abierto por fuera: el
    cierre queda en espera 30 s por si su apertura llega tarde. Solo una posición **sin ningún lote** (adoptada
    tras perder `trades.json`) la cierra el primer `trade_id` que lo pida, como antes.
  - Los lotes solo cambian con fills concretos, una vez por orden (`clientOrderId`), lleguen antes por la respuesta
    de la WS API o por el user stream. Lo que se cierra fuera del executor se descuenta así:
    - el TP/SL que puso un `trade_id`: primero de su lote;
    - liquidación o ADL: de todos en proporción (también de lo abierto por fuera);
    - otro bot o un cierre a mano en Binance: primero de lo que no es de ningún `trade_id` (lo abierto por fuera,
      también lo que ya había en Binance cuando el primer `trade_id` abrió en ese símbolo) y el resto en proporción.

    Un lote abierto después de esa reducción no carga con ella (hora de Binance). Antes de cerrar un lote el
    executor relee la posición y espera hasta 1.5 s los eventos atrasados del user stream; lo que siga sin cuadrar
    se reparte con la misma regla y no se vuelve a contar cuando llegue su evento.
  - Cada cierre de un `trade_id` queda en el historial con su propio registro: su cantidad, su precio de entrada,
    su PnL (con su entrada, no la media de la posición), sus comisiones y el PnL del bot. En el dashboard, la
    posición muestra cuánto es de cada `trade_id`.
  - Los cierres simultáneos del mismo símbolo (TP de varios `trade_id`, `close_all`, el cierre del dashboard) se
    ejecutan uno tras otro, sin perder ninguno.
  - Si Binance no responde a la orden de cierre de un lote, se reintenta como siempre (también un cierre que llegó
    antes que su apertura), pero antes se consulta la orden anterior por su `clientOrderId`: si ya se ejecutó, se
    registra y solo se envía lo que falte (nunca se cierra dos veces). La confirmación llega por el estado final de
    la orden en el user stream o por `order.status`; el executor lo vuelve a consultar cada 15 s aunque el bot ya no
    reintente, y el cierre pendiente se guarda en `trades.json` (sobrevive a un reinicio). Si la posición se cierra
    entera antes de confirmarlo, ese cierre igual queda en el historial con su propio registro. Si la orden se
    llena a medias, se envía otra por el resto del mismo `trade_id`.
  - Una apertura **asumida** (`-2019` con `ASSUME_ON_MARGIN_ERROR`) no cuenta como cantidad real: su `close` no
    envía órdenes ni reduce los lotes reales de otros `trade_id`.
  - Las posiciones abiertas antes de esta versión no tienen lotes: al arrancar, su cantidad se reparte a partes
    iguales entre sus `trade_id`.
  - **Reinicio sin `trades.json`** (cada deploy en Render sin disco persistente): cada orden lleva su lote en el
    `clientOrderId` (`X<trade_id>[:<bot>]` al abrir, `K<trade_id>[:<bot>]` al cerrar un lote). La primera vez que
    el executor lee la cuenta, cada posición sin trade se adopta y se rehace con el historial de Binance de los
    últimos 7 días (cada fill de `GET /fapi/v1/userTrades` con el `clientOrderId` de su orden en `allOrders`),
    desde la última vez que estuvo en cero; las señales de ese símbolo esperan a que termine. Un evento atrasado
    de un fill ya incluido no se cuenta otra vez. Lo que no se puede recuperar así: a quién pertenecía cada TP/SL
    (se reparte como un cierre de fuera), el multiplicador de cada `trade_id` (cuenta como ×1 para los cierres
    parciales con `quantity`) y las aperturas asumidas (`-2019`).
    - Si Binance no responde, el `close` con `trade_id` se reintenta; si en el último intento sigue sin historial,
      se aplica la regla de antes.
    - Si el historial no explica la posición (abierta hace más de 7 días), lo que no es de ningún lote lo cierra el
      primer `close` con `trade_id` que no tenga lote (como antes de los lotes); los lotes que sí se conocen no se
      tocan. Se avisa en el dashboard y por Telegram.
    - Un nombre de bot que no cabe en el `clientOrderId` va recortado con 4 caracteres de su hash (dos bots que
      empiezan igual no se confunden) y recupera su nombre con su primera señal.
    - Aun así, lo más seguro es un Render Disk (`DATA_DIR=/var/data`).
  - La cuenta debe estar en **Hedge mode** para que LONG y SHORT del mismo símbolo sean posiciones distintas. En
    One-way, el executor avisa al arrancar (log, dashboard y Telegram).
  - **TP/SL:** el TP/SL que envía un `trade_id` (`tp`/`sl` en la apertura, u `open_tp`/`open_sl`):
    - Si en la posición solo está su lote, cubre la cantidad de ese lote y al dispararse se descuenta de él. Antes
      de cerrar ese `trade_id` por señal se cancela (para que no se dispare sobre otros bots justo después); si el
      lote sigue abierto (cierre parcial o fallido), se vuelve a poner con lo que le queda.
    - Si ya hay lotes de otros `trade_id` o cantidad abierta por fuera, cubre la posición entera, no es de ningún
      `trade_id` (ningún cierre por `trade_id` lo cancela) y al dispararse se reparte entre todos. El executor lo
      avisa en el detalle de la señal y en el dashboard. Binance admite un TP y un SL por lado: uno nuevo reemplaza
      al anterior.
    - Para que cada bot cierre solo lo suyo, lo más seguro es cerrar con `close` + `trade_id` (como app_25).
- **Identidad de cada bot.** Si dos bots usan los mismos números de `trade_id` en el mismo símbolo (app.py numera
  1, 2, 3… en cada proceso), sus lotes se mezclarían. Para separarlos, cada bot se identifica de una de estas formas:
  - Sin tocar su código: su `EXECUTOR_URL` termina en `/b/<nombre>` (por ejemplo
    `https://executor-xxxx.onrender.com/b/kline`). El executor sirve `/b/<nombre>/signal`, `/b/<nombre>/api/state`,
    `/b/<nombre>/ws/signal`, `/b/<nombre>/health` y `/b/<nombre>/ready`.
  - O enviando `"bot": "<nombre>"` en cada señal. `ExecutorClient(bot="kline")` (o `EXECUTOR_BOT`) lo añade solo,
    y también lo toma de una `EXECUTOR_URL` con `/b/<nombre>`.
  - El nombre: letras, números, `_`, `.` o `-` (máx. 32; mayúsculas y otros caracteres se convierten: «Kline Bot» →
    `kline_bot`). Sin nombre, todo funciona como antes (solo `trade_id`).
  - Un bot que empieza a identificarse con trades abiertos de antes cierra esos lotes sin nombre solo si no abrió
    nada con nombre en esa posición y el lote es anterior a su primera señal con nombre; así nunca toma el lote de
    otro bot sin nombre que use los mismos números.
- `trade_id` debe ser un entero ≥ 0: un valor como `"T-7"` se rechaza (400) en vez de tomarse como «sin trade_id»,
  que cerraría la posición entera.
- `close` acepta `close_price`, `pnl` (PnL del bot, se compara con el real), `quantity` (cierre parcial; con
  `trade_id`, nunca más que su lote). Si llega antes que su `open` (o mientras se abre) se aplica en cuanto la
  posición existe; un tramo de ese `trade_id` que llegue hasta 30 s después del cierre se ignora (no reabre). El orden
  se respeta por símbolo: un `close` espera a los `open` recibidos antes que él, nunca a los posteriores.
- Si Binance no confirma una apertura (timeout, estado desconocido) el executor relee la posición real y la registra
  si se abrió, así el `close` del bot siempre la cierra.
- `POST /signal?wait=1` espera el resultado en Binance (hasta `SIGNAL_WAIT_TIMEOUT_S`) y lo devuelve en `result`.
- Un secreto incorrecto responde 401 y queda visible en la pestaña Señales y en `GET /api/signals`.
- Las peticiones de un navegador desde otro sitio (`Origin` ajeno) se rechazan en `/signal`, `/ws/signal` y `/ws`
  salvo que el origen esté en `CORS_ORIGINS`; los bots y scripts no envían `Origin` y no se ven afectados.

## Multiplicador de operaciones

Botón **×1** de la barra superior (o `POST /api/multiplier`): multiplica el tamaño de cada señal `open` de los bots
(`quantity`, `notional` y `margin`). Con ×2, una señal de 0.01 abre 0.02; los cierres parciales del bot se escalan con
el mismo factor que recibió la operación y los cierres totales cierran la posición real completa. Las órdenes manuales
del dashboard y los bots Grid no se multiplican.

- **Manual**: factor fijo (×1.5, ×2, ×3…), activable y desactivable.
- **Automático**: el factor sale del saldo de la billetera, en niveles de `MULTIPLIER_STEP_USDT` (100) con histéresis:

| Nivel | Se activa con | Se mantiene mientras el saldo esté entre |
| --- | --- | --- |
| ×1 | < 200 | 0 – 199.99 |
| ×2 | ≥ 200 | 100.01 – 299.99 (101-299) |
| ×3 | ≥ 300 | 200.01 – 399.99 (201-399) |
| ×N | ≥ N·100 | (N−1)·100 – (N+1)·100 |

  Sube un nivel al llegar al siguiente múltiplo y baja uno al caer al múltiplo anterior o menos; cada cambio de nivel
  se avisa en el dashboard y por Telegram. `MULTIPLIER_MAX` (10) pone un tope.

La configuración elegida en el dashboard se guarda en `DATA_DIR/multiplier.json`; en Render (disco efímero) conviene
fijarla también con las variables `MULTIPLIER_*` para que sobreviva a un redeploy.

## API HTTP y cliente (`executor_client.py`)

Todo lo que hace el dashboard se puede hacer con peticiones HTTP simples. La referencia completa (rutas, parámetros,
ejemplos con curl y Python) está en el docstring de [`executor_client.py`](executor_client.py):

```bash
python -c "import executor_client; help(executor_client)"   # referencia
python executor_client.py --url https://tu-executor.onrender.com demo
```

| Ruta | Uso |
| --- | --- |
| `POST /signal` · `GET /api/state` | Las dos rutas que usan los bots desplegados (sin cambios de formato) |
| `GET /health` · `GET /ready` | Salud de conexiones · 200 cuando ya sincronizó la cuenta (503 mientras arranca) |
| `GET /api/snapshot` `/api/status` `/api/account` `/api/positions` `/api/orders` `/api/trades` `/api/trades.csv` `/api/stats` `/api/signals` `/api/live` `/api/markets` `/api/symbol/{s}` `/api/logs` `/api/commands` `/api/errors/{code}` `/api/grids` | Lectura |
| `POST /api/open` `/api/close/{s}` `/api/force-close/{s}` `/api/close-all` `/api/set-tp/{s}` `/api/set-sl/{s}` `/api/cancel-tp-sl/{s}` `/api/limit-order` `/api/cancel-order` `/api/leverage[/{s}]` `/api/margin-type/{s}` `/api/margin/{s}` `/api/position-mode` `/api/trading` `/api/grids` … | Operación |
| `POST /api/command` `{"cmd": "...", "args": {...}}` | Cualquier comando del catálogo (`GET /api/commands`) |
| `POST /manual/*` | Rutas del executor anterior (`close`, `close_all`, `toggle_trading`, `set_leverage`, `set_tp`, `set_sl`, …) con las mismas respuestas |
| `GET /ws` · `GET /ws/signal` | WebSocket del dashboard · señales por WebSocket |

- **Autenticación**: lectura abierta (o `X-Dashboard-Token` si definiste `DASHBOARD_TOKEN`); escritura con el secreto
  de señales en `X-Signal-Secret`, `X-Dashboard-Token`, `Authorization: Bearer …` o `?secret=`. Con `DASHBOARD_TOKEN`
  definido y sin `SIGNAL_SECRET`, la API solo acepta el token (los secretos por defecto de los bots siguen valiendo
  únicamente para `/signal`).
- Respuestas JSON `{"ok": true, "action", "data"}`; errores `{"ok": false, "error", "diagnosis"?}` con 400/401/404/409/502/503.
  Si Binance rechaza la orden, `diagnosis` explica el código y la solución.
- CORS habilitado (`CORS_ORIGINS`), parámetros en JSON, formulario o query string, rutas con o sin `/` final.

`executor_client.py` (un archivo, solo biblioteca estándar) trae:

- `ExecutorBridge`: reemplazo directo del de `app.py`/`app_25.py` (misma firma). No bloquea, mantiene el orden de
  las señales, reintenta si el executor está despertando en Render y no duplica (`signal_id`).
- `ExecutorClient`: un método por endpoint (`positions()`, `open_signal()`, `close()`, `set_sl()`, `grid_create()`…).
- `SignalSocket` / `DashboardSocket`: WebSockets `/ws/signal` y `/ws` (con `aiohttp` o `websockets`).
- CLI: `python executor_client.py positions`, `open BTCUSDT SHORT --quantity 0.001`, `cmd set_sl symbol=… trigger_price=…`.

Los bots que siguen con su `ExecutorBridge` original funcionan igual; `tests/compat/` contiene copias literales de
ambos y las pruebas los ejecutan contra el executor.

## Variables de entorno

| Variable | Defecto | Uso |
| --- | --- | --- |
| `BINANCE_API_KEY` / `BINANCE_API_SECRET` | — | Credenciales (Futuros habilitado) |
| `USE_TESTNET` | `false` | Testnet de Binance Futures |
| `SIGNAL_SECRET` | — | Secreto de `/signal` y de la escritura por HTTP, usado tal cual (como antes; puede contener comas). Sin definir acepta los valores por defecto de los bots: `clave-secreta-aleatoria` y `cambiar-por-secreto-seguro`. Debe coincidir con `EXECUTOR_SECRET` de app_25 |
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
| `SIGNAL_DEDUPE_TTL_S` | `0` | Antiduplicado por contenido para `close`/TP/SL (nunca para `open`: los tramos se ejecutan todos). Las señales con `signal_id` nunca se repiten |
| `MULTIPLIER_MODE` | `off` | Multiplicador al arrancar: `off`, `manual` o `auto` (se cambia desde el dashboard) |
| `MULTIPLIER` | `1` | Factor del modo manual (2 = el doble) |
| `MULTIPLIER_STEP_USDT` | `100` | USDT por nivel del modo automático |
| `MULTIPLIER_MAX` | `10` | Factor máximo permitido |
| `MULTIPLIER_BALANCE` | `wallet` | Saldo del modo automático: `wallet` (billetera), `margin` (con PnL abierto) o `available` |
| `SIGNAL_WAIT_TIMEOUT_S` | `7` | Espera máxima de `?wait=1` |
| `LEVERAGE_REQUIRED` | `false` | Rechazar la apertura si Binance no confirma el leverage pedido |
| `ADOPT_POSITIONS` | `true` | Al arrancar, registrar como trades las posiciones abiertas que no conocía |
| `RESTORE_TRADING_PAUSE` | `true` | Mantener la pausa de trading tras reiniciar |
| `API_WRITE_OPEN` | `false` | Permitir escritura HTTP sin secreto (no recomendado) |
| `SIGNAL_SECRETS` | — | Secretos adicionales aceptados, separados por comas (p. ej. al rotar el secreto) |
| `CORS_ORIGINS` | `*` | Orígenes permitidos para la API (CORS). El WebSocket del dashboard solo acepta el mismo sitio o los orígenes listados explícitamente |
| `TRUST_PROXY` | `true` en Render | Tomar la IP del cliente de `X-Forwarded-For` |
| `KEEPALIVE_URL` / `KEEPALIVE_S` | `RENDER_EXTERNAL_URL` / `600` | Auto-ping (`HEAD /ready`) para que Render no duerma el servicio (con UptimeRobot u otro monitor externo puedes poner `KEEPALIVE_S=0`) |
| `MARKET_STREAMS` | `lite` | `lite`: solo los símbolos necesarios (ahorra ancho de banda) · `full`: todo el mercado cada segundo |
| `MAX_PRICE_AGE_S` | `5` | Antigüedad máxima del precio de referencia |
| `DATA_DIR` | `data` | Estado persistente (trades, grids, exchangeInfo descargado) |
| `EXCHANGE_INFO_FILE` | `exchangeInfo.txt` | Base predefinida de reglas (respuesta cruda de Binance) |
| `EXCHANGE_INFO_BOOTSTRAP` | `true` | Generar el exchangeInfo si no hay snapshot |
| `EXCHANGE_INFO_REFRESH_H` | `0` | Refresco periódico del exchangeInfo (horas, 0 = nunca) |
| `SEED_OPEN_ORDERS_REST` | `true` | Leer órdenes abiertas al arrancar |
| `STATE_CLOSED_LIMIT` | `15` | Operaciones cerradas que se envían al dashboard y a `/api/state` sin `?limit=` (`0` = todas). El historial completo se sigue guardando para las estadísticas y el CSV |
| `STATE_REQUIRES_AUTH` | `false` | Exigir el secreto en `/api/state` (los bots desplegados no lo envían: déjalo en `false`) |
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
`tests/test_api.py` ejecuta los `ExecutorBridge` desplegados (copias literales en `tests/compat/`), el cliente
`executor_client.py`, todas las rutas REST y `/manual/*`, autenticación, CORS, errores JSON, reintentos y apagado.

## Cambios respecto a la versión anterior

- Las credenciales del proxy Fixie ya no están en el código: configura `PROXY_URLS` (o `FIXIE_URL`). Conviene rotar
  esa contraseña, porque estuvo publicada en el historial del repositorio.
- El dashboard se abre con el link, como antes, pero el secreto de señales ya no viaja dentro del HTML.
- `GET /health` devuelve JSON con el estado de las conexiones (sigue respondiendo 200).
- Si el cierre de una posición falla en Binance, la operación sigue abierta y se avisa (antes se marcaba cerrada).
- Los cierres por TP/SL/liquidación hechos por Binance se detectan y registran automáticamente.
- En `/api/state`, `wins`/`losses`/`win_rate` mantienen la definición anterior (cierres por TP); la definición por
  PnL > 0 está en `wins_pnl`/`losses_pnl`/`win_rate_pnl`.
- Vuelven las rutas `/manual/*` y se añade la API REST completa (`/api/*`) y `executor_client.py`.
