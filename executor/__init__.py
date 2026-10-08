"""
Futures Executor — motor de ejecución para Binance USDⓈ-M Futures.

Arquitectura WebSocket-first:

* Órdenes, cancelaciones, consultas de cuenta y algo orders (TP/SL) por la
  WebSocket API de Binance (``ws-fapi``).
* Posiciones, balances, fills y TP/SL en tiempo real por el User Data Stream
  (push, sin polling).
* Precios de TODOS los símbolos por un único stream de mercado
  (``!markPrice@arr@1s`` + ``!miniTicker@arr`` + ``!contractInfo``).
* Reglas de cada símbolo (tickSize, stepSize, minNotional...) desde un
  exchangeInfo local predefinido: cero consultas REST al operar.
* REST solo donde Binance no ofrece alternativa WebSocket (cambiar leverage,
  tipo de margen, modo de posición y margen aislado).
"""

__version__ = "3.0.0"
