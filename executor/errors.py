"""Lector y solucionador de códigos de error de Binance USDⓈ-M Futures.

* ``CATALOG``: cada código conocido con su nombre oficial, una explicación en
  español, la causa típica, la solución y la *acción automática* que el
  executor aplica para corregirlo.
* ``BinanceAPIError``: excepción única para errores de WS API y REST, con
  código, mensaje, estado HTTP y tiempo de espera sugerido.
* ``ErrorDoctor``: diagnostica cualquier error (excepción, dict o texto) y
  devuelve un ``Diagnosis`` con la acción recomendada.
* ``ErrorJournal``: historial en memoria de los últimos errores con su
  diagnóstico y si se autocorrigieron; alimenta la pestaña *Errores* del
  dashboard.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
from collections import Counter, deque
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Optional


class Action(str, Enum):
    RETRY = "retry"
    WAIT = "wait"
    CHECK_STATUS = "check_status"
    RESYNC_TIME = "resync_time"
    FIX_PRECISION = "fix_precision"
    FIX_PRICE_TICK = "fix_price_tick"
    RAISE_QTY = "raise_qty"
    RAISE_NOTIONAL = "raise_notional"
    SPLIT_QTY = "split_qty"
    CLAMP_PRICE = "clamp_price"
    REDUCE_SIZE = "reduce_size"
    LOWER_LEVERAGE = "lower_leverage"
    FLIP_POSITION_SIDE = "flip_position_side"
    DROP_REDUCE_ONLY = "drop_reduce_only"
    REFRESH_POSITION = "refresh_position"
    USE_ALGO_API = "use_algo_api"
    USE_QUANTITY = "use_quantity"
    TRIGGER_CROSSED = "trigger_crossed"
    REPRICE = "reprice"
    NEW_CLIENT_ID = "new_client_id"
    ALREADY_DONE = "already_done"
    RESTART_USER_STREAM = "restart_user_stream"
    BLOCK_SYMBOL = "block_symbol"
    CANCEL_FIRST = "cancel_first"
    USE_PROXY = "use_proxy"
    FIX_CONFIG = "fix_config"
    NONE = "none"


ACTION_LABELS = {
    Action.RETRY: "Reintentar automáticamente con espera progresiva",
    Action.WAIT: "Pausar las peticiones hasta que Binance levante el límite",
    Action.CHECK_STATUS: "Consultar el estado real de la orden antes de reenviarla",
    Action.RESYNC_TIME: "Resincronizar el reloj con el servidor y reintentar",
    Action.FIX_PRECISION: "Redondear cantidad/precio a stepSize/tickSize y aprender el paso real",
    Action.FIX_PRICE_TICK: "Redondear el precio al tickSize del símbolo",
    Action.RAISE_QTY: "Subir la cantidad al mínimo permitido (minQty)",
    Action.RAISE_NOTIONAL: "Subir la cantidad hasta cubrir el notional mínimo",
    Action.SPLIT_QTY: "Dividir la orden en partes dentro del máximo permitido",
    Action.CLAMP_PRICE: "Ajustar el precio dentro de la banda permitida (PERCENT_PRICE)",
    Action.REDUCE_SIZE: "Reducir el tamaño de la orden o el leverage",
    Action.LOWER_LEVERAGE: "Bajar el leverage al máximo permitido por el símbolo",
    Action.FLIP_POSITION_SIDE: "Cambiar positionSide al modo real de la cuenta (Hedge/One-way)",
    Action.DROP_REDUCE_ONLY: "Quitar reduceOnly (en Hedge Mode no se permite)",
    Action.REFRESH_POSITION: "Releer la posición real y ajustar la cantidad a cerrar",
    Action.USE_ALGO_API: "Enviar la orden por la Algo Order API (algoOrder.place)",
    Action.USE_QUANTITY: "Usar quantity + reduceOnly en vez de closePosition",
    Action.TRIGGER_CROSSED: "El precio ya cruzó el disparo: ejecutar a mercado o mover el trigger",
    Action.REPRICE: "Recolocar el precio a un tick de distancia",
    Action.NEW_CLIENT_ID: "Generar un nuevo clientOrderId",
    Action.ALREADY_DONE: "No requiere acción: el estado deseado ya se cumple",
    Action.RESTART_USER_STREAM: "Renovar el listenKey y reconectar el User Data Stream",
    Action.BLOCK_SYMBOL: "Bloquear el símbolo para nuevas aperturas",
    Action.CANCEL_FIRST: "Cancelar órdenes / cerrar posiciones antes de repetir",
    Action.USE_PROXY: "Reintentar por un proxy de PROXY_URLS",
    Action.FIX_CONFIG: "Requiere intervención: revisar API key, permisos, IP o región",
    Action.NONE: "Sin corrección automática: revisar parámetros",
}


@dataclass(frozen=True)
class ErrorInfo:
    code: int
    name: str
    category: str
    action: Action
    title: str
    cause: str
    solution: str
    severity: str = "error"
    retryable: bool = False

    def to_dict(self) -> dict:
        data = asdict(self)
        data["action"] = self.action.value
        data["action_label"] = ACTION_LABELS.get(self.action, "")
        return data


CATALOG: dict[int, ErrorInfo] = {}


def _e(code, name, category, action, title, cause, solution, severity="error", retryable=False):
    CATALOG[code] = ErrorInfo(code, name, category, action, title, cause, solution, severity, retryable)


# ── 10xx: servidor / red / autenticación ─────────────────────────────────
_e(-1000, "UNKNOWN", "red", Action.RETRY, "Error desconocido de Binance",
   "Fallo interno no clasificado del servidor.",
   "Se reintenta con espera. Si se repite, revisa el estado de Binance.", "warning", True)
_e(-1001, "DISCONNECTED", "red", Action.RETRY, "Error interno, petición no procesada",
   "Binance no pudo procesar la petición (sobrecarga o reinicio interno).",
   "Se reintenta automáticamente con espera progresiva.", "warning", True)
_e(-1002, "UNAUTHORIZED", "autenticación", Action.FIX_CONFIG, "No autorizado",
   "La API key no tiene permiso para esta operación.",
   "Habilita 'Enable Futures' en la API key y revisa la lista de IPs permitidas.", "critical")
_e(-1003, "TOO_MANY_REQUESTS", "límite", Action.WAIT, "Demasiadas peticiones (rate limit / IP baneada)",
   "Se superó el peso de peticiones por minuto; Binance puede banear la IP temporalmente.",
   "El executor pausa ese canal hasta la hora indicada por Binance y usa WebSocket/otro proxy mientras tanto.",
   "critical", True)
_e(-1004, "DUPLICATE_IP", "configuración", Action.ALREADY_DONE, "IP ya registrada",
   "La IP ya está en la lista blanca.", "No requiere acción.", "info")
_e(-1006, "UNEXPECTED_RESP", "red", Action.CHECK_STATUS, "Respuesta inesperada: estado de ejecución desconocido",
   "Binance recibió la orden pero no confirmó si se ejecutó.",
   "Se consulta order.status con el clientOrderId antes de reenviar, para no duplicar la orden.", "warning", True)
_e(-1007, "TIMEOUT", "red", Action.CHECK_STATUS, "Timeout del backend: estado de ejecución desconocido",
   "El motor de Binance tardó demasiado en responder.",
   "Se consulta order.status con el clientOrderId antes de reenviar, para no duplicar la orden.", "warning", True)
_e(-1008, "SERVER_BUSY", "red", Action.RETRY, "Servidor sobrecargado",
   "Binance está saturado (típico en alta volatilidad).",
   "Se reintenta con espera progresiva; las órdenes reduceOnly tienen prioridad en Binance.", "warning", True)
_e(-1010, "ERROR_MSG_RECEIVED", "red", Action.RETRY, "Mensaje de error del motor",
   "El motor interno devolvió un error transitorio.", "Se reintenta con espera.", "warning", True)
_e(-1011, "NON_WHITE_LIST", "autenticación", Action.USE_PROXY, "IP no autorizada para esta ruta",
   "La IP de salida no está en la lista blanca de la API key.",
   "Agrega la IP del servidor/proxy a la API key o define PROXY_URLS con IPs autorizadas.", "critical")
_e(-1013, "INVALID_MESSAGE", "parámetros", Action.FIX_PRECISION, "Filtro de la orden no superado",
   "La orden incumple un filtro del símbolo (LOT_SIZE, PRICE_FILTER, MIN_NOTIONAL...).",
   "Se recalcula la orden con las reglas del exchangeInfo local.")
_e(-1014, "UNKNOWN_ORDER_COMPOSITION", "parámetros", Action.NONE, "Combinación de orden no soportada",
   "La combinación de tipo/TIF/flags no es válida.", "Revisa type, timeInForce y reduceOnly/closePosition.")
_e(-1015, "TOO_MANY_ORDERS", "límite", Action.WAIT, "Demasiadas órdenes nuevas",
   "Se superó el límite de órdenes por 10s o por minuto.",
   "Se espera y se reintenta; los grids colocan órdenes con concurrencia limitada.", "warning", True)
_e(-1016, "SERVICE_SHUTTING_DOWN", "red", Action.RETRY, "Servicio en mantenimiento",
   "El servicio se está apagando o reiniciando.", "Se reintenta más tarde.", "warning", True)
_e(-1020, "UNSUPPORTED_OPERATION", "parámetros", Action.NONE, "Operación no soportada",
   "La operación no está disponible para esta cuenta o símbolo.", "Revisa el tipo de orden o el endpoint.")
_e(-1021, "INVALID_TIMESTAMP", "reloj", Action.RESYNC_TIME, "Timestamp fuera de recvWindow",
   "El reloj local está desfasado respecto al servidor de Binance.",
   "El executor estima el desfase con los eventos del stream y reintenta. Sincroniza NTP si persiste.",
   "warning", True)
_e(-1022, "INVALID_SIGNATURE", "autenticación", Action.FIX_CONFIG, "Firma inválida",
   "BINANCE_API_SECRET no corresponde a la API key o la firma se calculó mal.",
   "Verifica BINANCE_API_SECRET (sin espacios ni saltos de línea).", "critical")
_e(-1099, "NOT_FOUND_OR_UNAUTHENTICATED", "autenticación", Action.FIX_CONFIG, "No encontrado / no autenticado",
   "La petición no se pudo autenticar.", "Revisa la API key y sus permisos.", "critical")

# ── 11xx: parámetros ──────────────────────────────────────────────────────
_e(-1100, "ILLEGAL_CHARS", "parámetros", Action.NONE, "Caracteres ilegales en un parámetro",
   "Algún parámetro tiene caracteres no permitidos.", "Revisa símbolo y clientOrderId.")
_e(-1101, "TOO_MANY_PARAMETERS", "parámetros", Action.NONE, "Demasiados parámetros",
   "Se enviaron parámetros duplicados o de más.", "Revisa la petición.")
_e(-1102, "MANDATORY_PARAM_EMPTY_OR_MALFORMED", "parámetros", Action.NONE, "Falta un parámetro obligatorio",
   "Un parámetro requerido no se envió o tiene formato inválido.", "Revisa quantity, price, timeInForce y positionSide.")
_e(-1103, "UNKNOWN_PARAM", "parámetros", Action.NONE, "Parámetro desconocido",
   "Se envió un parámetro que el endpoint no reconoce.", "Quita el parámetro sobrante.")
_e(-1104, "UNREAD_PARAMETERS", "parámetros", Action.NONE, "Parámetros no leídos",
   "No todos los parámetros fueron usados por el endpoint.", "Quita los parámetros sobrantes.")
_e(-1105, "PARAM_EMPTY", "parámetros", Action.NONE, "Parámetro vacío", "Un parámetro llegó vacío.", "Completa el parámetro.")
_e(-1106, "PARAM_NOT_REQUIRED", "parámetros", Action.DROP_REDUCE_ONLY, "Parámetro no requerido",
   "Se envió un parámetro que no aplica (típico: reduceOnly en Hedge Mode).",
   "Se quita reduceOnly cuando hay positionSide LONG/SHORT y se reenvía.", "warning", True)
_e(-1108, "BAD_ASSET", "parámetros", Action.NONE, "Activo inválido", "El activo no es válido.", "Revisa el activo.")
_e(-1109, "BAD_ACCOUNT", "configuración", Action.FIX_CONFIG, "Cuenta inválida",
   "La cuenta de futuros no existe o no está habilitada.", "Activa la cuenta de Futuros USDⓈ-M.", "critical")
_e(-1110, "BAD_INSTRUMENT_TYPE", "parámetros", Action.NONE, "Tipo de instrumento inválido",
   "El instrumento no es válido para este endpoint.", "Revisa el símbolo.")
_e(-1111, "BAD_PRECISION", "precisión", Action.FIX_PRECISION, "Precisión mayor a la permitida",
   "La cantidad o el precio tiene más decimales que stepSize/tickSize.",
   "Se redondea al paso real del exchangeInfo; si el símbolo no estaba en el snapshot se aprende un paso más grueso.",
   "warning", True)
_e(-1112, "NO_DEPTH", "mercado", Action.RETRY, "Sin liquidez en el libro",
   "No hay órdenes en el libro para el símbolo.", "Se reintenta más tarde.", "warning", True)
_e(-1114, "TIF_NOT_REQUIRED", "parámetros", Action.NONE, "timeInForce no requerido",
   "Se envió timeInForce en una orden que no lo usa (p.ej. MARKET).", "Quita timeInForce.")
_e(-1115, "INVALID_TIF", "parámetros", Action.NONE, "timeInForce inválido", "Valor de TIF no soportado.", "Usa GTC, IOC, FOK o GTX.")
_e(-1116, "INVALID_ORDER_TYPE", "parámetros", Action.NONE, "Tipo de orden inválido", "El tipo de orden no existe.", "Revisa type.")
_e(-1117, "INVALID_SIDE", "parámetros", Action.NONE, "Side inválido", "side debe ser BUY o SELL.", "Corrige side.")
_e(-1118, "EMPTY_NEW_CL_ORD_ID", "parámetros", Action.NEW_CLIENT_ID, "clientOrderId vacío",
   "Se envió un newClientOrderId vacío.", "Se genera uno nuevo.", "warning", True)
_e(-1120, "BAD_INTERVAL", "parámetros", Action.NONE, "Intervalo inválido", "Intervalo no soportado.", "Revisa el intervalo.")
_e(-1121, "BAD_SYMBOL", "símbolo", Action.BLOCK_SYMBOL, "Símbolo inválido",
   "El símbolo no existe en Futuros USDⓈ-M (o fue deslistado).",
   "Se bloquea el símbolo. Actualiza el exchangeInfo si es un listado nuevo.")
_e(-1122, "INVALID_SYMBOL_STATUS", "símbolo", Action.BLOCK_SYMBOL, "Estado del símbolo no permite operar",
   "El símbolo no está en estado TRADING.", "Se bloquea hasta que vuelva a TRADING.")
_e(-1125, "INVALID_LISTEN_KEY", "stream", Action.RESTART_USER_STREAM, "listenKey inválido o expirado",
   "El listenKey del User Data Stream expiró.", "Se renueva el listenKey y se reconecta.", "warning", True)
_e(-1127, "MORE_THAN_XX_HOURS", "parámetros", Action.NONE, "Rango de tiempo demasiado largo",
   "El intervalo de tiempo consultado excede el máximo.", "Reduce el rango.")
_e(-1128, "OPTIONAL_PARAMS_BAD_COMBO", "parámetros", Action.NONE, "Combinación de parámetros inválida",
   "Parámetros opcionales incompatibles.", "Revisa la combinación de parámetros.")
_e(-1130, "INVALID_PARAMETER", "parámetros", Action.NONE, "Parámetro con valor inválido",
   "Algún valor no es aceptado.", "Revisa los valores enviados.")
_e(-1136, "INVALID_NEW_ORDER_RESP_TYPE", "parámetros", Action.NONE, "newOrderRespType inválido",
   "Valor no soportado.", "Usa ACK o RESULT.")

# ── 20xx: órdenes / cuenta ────────────────────────────────────────────────
_e(-2010, "NEW_ORDER_REJECTED", "orden", Action.NONE, "Orden rechazada",
   "El motor rechazó la orden.", "Revisa el mensaje detallado de Binance.")
_e(-2011, "CANCEL_REJECTED", "orden", Action.ALREADY_DONE, "Cancelación rechazada (orden desconocida)",
   "La orden ya no existe: se ejecutó, expiró o ya se canceló.", "Se considera cancelada y se refresca el estado.", "info")
_e(-2013, "NO_SUCH_ORDER", "orden", Action.ALREADY_DONE, "La orden no existe",
   "La orden ya no está activa.", "Se refresca el estado local.", "info")
_e(-2014, "BAD_API_KEY_FMT", "autenticación", Action.FIX_CONFIG, "Formato de API key inválido",
   "BINANCE_API_KEY tiene un formato inválido.", "Copia de nuevo la API key sin espacios.", "critical")
_e(-2015, "REJECTED_MBX_KEY", "autenticación", Action.FIX_CONFIG, "API key, IP o permisos inválidos",
   "La API key no existe, no tiene Futuros habilitados o la IP no está en la lista blanca.",
   "Habilita 'Enable Futures', revisa la IP autorizada o usa PROXY_URLS con la IP permitida.", "critical")
_e(-2016, "NO_TRADING_WINDOW", "mercado", Action.RETRY, "Sin ventana de trading",
   "No hay ventana de trading para el símbolo.", "Se reintenta más tarde.", "warning", True)
_e(-2017, "API_KEYS_LOCKED", "autenticación", Action.FIX_CONFIG, "API key bloqueada",
   "Binance bloqueó la API key.", "Revisa la cuenta en Binance.", "critical")
_e(-2018, "BALANCE_NOT_SUFFICIENT", "margen", Action.REDUCE_SIZE, "Balance insuficiente",
   "No hay balance suficiente en la billetera de futuros.", "Reduce el tamaño o transfiere USDT a Futuros.")
_e(-2019, "MARGIN_NOT_SUFFICIENT", "margen", Action.REDUCE_SIZE, "Margen insuficiente",
   "El margen disponible no cubre la orden con el leverage actual.",
   "Reduce el tamaño, sube el leverage o agrega USDT. Con MARGIN_AUTO_REDUCE=true se reintenta con menos cantidad.")
_e(-2020, "UNABLE_TO_FILL", "orden", Action.REPRICE, "La orden no se pudo llenar (FOK/IOC)",
   "No hay liquidez suficiente al precio indicado.", "Recolocar la orden con otro precio o tipo.", "warning")
_e(-2021, "ORDER_WOULD_IMMEDIATELY_TRIGGER", "orden", Action.TRIGGER_CROSSED, "El TP/SL se dispararía de inmediato",
   "El precio de disparo ya fue superado por el precio actual.",
   "Mueve el trigger o cierra a mercado; el executor lo indica sin reintentar a ciegas.", "warning")
_e(-2022, "REDUCE_ONLY_REJECT", "orden", Action.REFRESH_POSITION, "Orden reduceOnly rechazada",
   "La posición ya está cerrada o es menor que la cantidad a reducir.",
   "Se relee la posición real; si ya está en 0 se considera cerrada.", "warning", True)
_e(-2023, "USER_IN_LIQUIDATION", "margen", Action.NONE, "Cuenta en liquidación",
   "La cuenta está siendo liquidada.", "Espera a que termine la liquidación.", "critical")
_e(-2024, "POSITION_NOT_SUFFICIENT", "orden", Action.REFRESH_POSITION, "Posición insuficiente",
   "La cantidad a cerrar supera la posición abierta.", "Se ajusta a la posición real.", "warning", True)
_e(-2025, "MAX_OPEN_ORDER_EXCEEDED", "límite", Action.CANCEL_FIRST, "Límite de órdenes abiertas alcanzado",
   "El símbolo superó el máximo de órdenes abiertas (MAX_NUM_ORDERS).",
   "Cancela órdenes viejas o reduce la cantidad de niveles del grid.")
_e(-2026, "REDUCE_ONLY_ORDER_TYPE_NOT_SUPPORTED", "orden", Action.DROP_REDUCE_ONLY, "Tipo de orden no admite reduceOnly",
   "Este tipo de orden no acepta reduceOnly.", "Se quita reduceOnly.", "warning", True)
_e(-2027, "MAX_LEVERAGE_RATIO", "margen", Action.LOWER_LEVERAGE, "Posición máxima excedida para el leverage actual",
   "El notional supera el máximo del bracket para este leverage.",
   "Baja el leverage o reduce el tamaño de la posición.")
_e(-2028, "MIN_LEVERAGE_RATIO", "margen", Action.REDUCE_SIZE, "Leverage demasiado bajo para el margen",
   "El margen no alcanza para mantener la posición con este leverage.", "Agrega margen o reduce la posición.")

# ── 40xx: filtros y configuración de posiciones ───────────────────────────
_e(-4000, "INVALID_ORDER_STATUS", "orden", Action.ALREADY_DONE, "Estado de orden inválido",
   "La orden no está en un estado que permita la operación.", "Se refresca el estado.", "info")
_e(-4001, "PRICE_LESS_THAN_ZERO", "precio", Action.CLAMP_PRICE, "Precio menor que cero", "El precio es <= 0.", "Corrige el precio.")
_e(-4002, "PRICE_GREATER_THAN_MAX_PRICE", "precio", Action.CLAMP_PRICE, "Precio mayor al máximo",
   "El precio supera maxPrice del símbolo.", "Se ajusta al rango permitido.")
_e(-4003, "QTY_LESS_THAN_ZERO", "cantidad", Action.RAISE_QTY, "Cantidad menor o igual a cero",
   "La cantidad quedó en 0 tras redondear al stepSize.", "Se sube al mínimo permitido.", "warning", True)
_e(-4004, "QTY_LESS_THAN_MIN_QTY", "cantidad", Action.RAISE_QTY, "Cantidad menor al mínimo",
   "La cantidad es menor que minQty.", "Se sube a minQty.", "warning", True)
_e(-4005, "QTY_GREATER_THAN_MAX_QTY", "cantidad", Action.SPLIT_QTY, "Cantidad mayor al máximo",
   "La cantidad supera maxQty (o marketMaxQty en MARKET).", "Se divide en varias órdenes.", "warning", True)
_e(-4006, "STOP_PRICE_LESS_THAN_ZERO", "precio", Action.CLAMP_PRICE, "Stop price <= 0", "El trigger es inválido.", "Corrige el trigger.")
_e(-4007, "STOP_PRICE_GREATER_THAN_MAX_PRICE", "precio", Action.CLAMP_PRICE, "Stop price mayor al máximo",
   "El trigger supera maxPrice.", "Ajusta el trigger.")
_e(-4013, "PRICE_LESS_THAN_MIN_PRICE", "precio", Action.CLAMP_PRICE, "Precio menor al mínimo",
   "El precio es menor que minPrice.", "Se ajusta al mínimo permitido.")
_e(-4014, "PRICE_NOT_INCREASED_BY_TICK_SIZE", "precisión", Action.FIX_PRICE_TICK, "Precio no múltiplo del tickSize",
   "El precio no respeta el tickSize del símbolo.", "Se redondea al tickSize.", "warning", True)
_e(-4015, "INVALID_CL_ORD_ID_LEN", "parámetros", Action.NEW_CLIENT_ID, "clientOrderId demasiado largo",
   "El clientOrderId supera 36 caracteres.", "Se genera uno nuevo más corto.", "warning", True)
_e(-4016, "PRICE_HIGHTER_THAN_MULTIPLIER_UP", "precio", Action.CLAMP_PRICE, "Precio por encima de la banda permitida",
   "El precio supera markPrice × multiplierUp (PERCENT_PRICE).", "Se ajusta dentro de la banda.", "warning", True)
_e(-4023, "QTY_NOT_INCREASED_BY_STEP_SIZE", "precisión", Action.FIX_PRECISION, "Cantidad no múltiplo del stepSize",
   "La cantidad no respeta el stepSize.", "Se redondea al stepSize.", "warning", True)
_e(-4024, "PRICE_LOWER_THAN_MULTIPLIER_DOWN", "precio", Action.CLAMP_PRICE, "Precio por debajo de la banda permitida",
   "El precio es menor que markPrice × multiplierDown (PERCENT_PRICE).", "Se ajusta dentro de la banda.", "warning", True)
_e(-4028, "INVALID_LEVERAGE", "leverage", Action.LOWER_LEVERAGE, "Leverage no válido para el símbolo",
   "El símbolo no admite ese leverage (su máximo es menor).",
   "Se baja por la escalera de leverage hasta uno aceptado.", "warning", True)
_e(-4029, "INVALID_TICK_SIZE_PRECISION", "precisión", Action.FIX_PRICE_TICK, "Precisión de tickSize inválida",
   "El precio tiene más decimales de los permitidos.", "Se redondea al tickSize.", "warning", True)
_e(-4030, "INVALID_STEP_SIZE_PRECISION", "precisión", Action.FIX_PRECISION, "Precisión de stepSize inválida",
   "La cantidad tiene más decimales de los permitidos.", "Se redondea al stepSize.", "warning", True)
_e(-4031, "INVALID_WORKING_TYPE", "parámetros", Action.NONE, "workingType inválido", "Usa MARK_PRICE o CONTRACT_PRICE.", "Corrige workingType.")
_e(-4044, "INVALID_BALANCE_TYPE", "parámetros", Action.NONE, "Tipo de balance inválido", "Tipo no soportado.", "Revisa el parámetro.")
_e(-4045, "MAX_STOP_ORDER_EXCEEDED", "límite", Action.CANCEL_FIRST, "Límite de órdenes stop alcanzado",
   "Demasiadas órdenes condicionales abiertas.", "Cancela TP/SL antiguos antes de crear nuevos.")
_e(-4046, "NO_NEED_TO_CHANGE_MARGIN_TYPE", "configuración", Action.ALREADY_DONE, "El tipo de margen ya era ese",
   "El símbolo ya estaba en ese tipo de margen.", "No requiere acción.", "info")
_e(-4047, "THERE_EXISTS_OPEN_ORDERS", "configuración", Action.CANCEL_FIRST, "Hay órdenes abiertas",
   "No se puede cambiar el tipo de margen con órdenes abiertas.", "Cancela las órdenes del símbolo y repite.")
_e(-4048, "THERE_EXISTS_QUANTITY", "configuración", Action.CANCEL_FIRST, "Hay una posición abierta",
   "No se puede cambiar el tipo de margen con posición abierta.", "Cierra la posición y repite.")
_e(-4049, "ADD_ISOLATED_MARGIN_REJECT", "margen", Action.NONE, "No se puede agregar margen aislado",
   "El símbolo no está en margen aislado o no hay posición.", "Cambia a ISOLATED y abre posición primero.")
_e(-4050, "CROSS_BALANCE_INSUFFICIENT", "margen", Action.REDUCE_SIZE, "Balance cruzado insuficiente",
   "No hay balance cruzado suficiente.", "Reduce el monto.")
_e(-4051, "ISOLATED_BALANCE_INSUFFICIENT", "margen", Action.REDUCE_SIZE, "Balance aislado insuficiente",
   "No hay margen aislado suficiente para retirar.", "Reduce el monto a retirar.")
_e(-4052, "NO_NEED_TO_CHANGE_AUTO_ADD_MARGIN", "configuración", Action.ALREADY_DONE, "Auto-add margin ya estaba así",
   "Sin cambios.", "No requiere acción.", "info")
_e(-4054, "ADD_ISOLATED_MARGIN_NO_POSITION_REJECT", "margen", Action.NONE, "No hay posición para agregar margen",
   "No existe posición aislada.", "Abre la posición primero.")
_e(-4055, "AMOUNT_MUST_BE_POSITIVE", "parámetros", Action.NONE, "El monto debe ser positivo", "Monto <= 0.", "Corrige el monto.")
_e(-4059, "NO_NEED_TO_CHANGE_POSITION_SIDE", "configuración", Action.ALREADY_DONE, "El modo de posición ya era ese",
   "La cuenta ya estaba en ese modo (Hedge/One-way).", "No requiere acción.", "info")
_e(-4060, "INVALID_POSITION_SIDE", "posición", Action.FLIP_POSITION_SIDE, "positionSide inválido",
   "El positionSide no corresponde al modo de la cuenta.", "Se usa el positionSide del modo real.", "warning", True)
_e(-4061, "POSITION_SIDE_NOT_MATCH", "posición", Action.FLIP_POSITION_SIDE, "positionSide no coincide con el modo de la cuenta",
   "Se envió LONG/SHORT con la cuenta en One-way, o BOTH con la cuenta en Hedge.",
   "Se detecta el modo real y se reenvía con el positionSide correcto.", "warning", True)
_e(-4062, "REDUCE_ONLY_CONFLICT", "orden", Action.DROP_REDUCE_ONLY, "reduceOnly inválido",
   "reduceOnly no aplica en este contexto.", "Se reenvía sin reduceOnly.", "warning", True)
_e(-4067, "POSITION_SIDE_CHANGE_EXISTS_OPEN_ORDERS", "configuración", Action.CANCEL_FIRST, "Hay órdenes abiertas",
   "No se puede cambiar el modo de posición con órdenes abiertas.", "Cancela todas las órdenes y repite.")
_e(-4068, "POSITION_SIDE_CHANGE_EXISTS_QUANTITY", "configuración", Action.CANCEL_FIRST, "Hay posiciones abiertas",
   "No se puede cambiar el modo de posición con posiciones abiertas.", "Cierra todas las posiciones y repite.")
_e(-4082, "INVALID_BATCH_PLACE_ORDER_SIZE", "parámetros", Action.NONE, "Lote de órdenes inválido",
   "Cantidad de órdenes por lote fuera de rango.", "Envía entre 1 y 5 órdenes por lote.")
_e(-4083, "PLACE_BATCH_ORDERS_FAIL", "orden", Action.RETRY, "Falló el lote de órdenes",
   "Binance no pudo procesar el lote.", "Se reintenta individualmente.", "warning", True)
_e(-4087, "REDUCE_ONLY_ORDER_PERMISSION", "cuenta", Action.FIX_CONFIG, "Solo se permiten órdenes reduceOnly",
   "La cuenta está restringida a reducir posiciones.", "Revisa restricciones de la cuenta en Binance.", "critical")
_e(-4088, "NO_PLACE_ORDER_PERMISSION", "cuenta", Action.FIX_CONFIG, "Sin permiso para colocar órdenes",
   "La cuenta no tiene permiso de trading.", "Revisa restricciones de la cuenta.", "critical")
_e(-4104, "INVALID_CONTRACT_TYPE", "símbolo", Action.BLOCK_SYMBOL, "Tipo de contrato inválido",
   "El contrato no es perpetuo o no es válido.", "Usa un símbolo perpetuo USDT.")
_e(-4108, "SYMBOL_NOT_TRADING", "símbolo", Action.BLOCK_SYMBOL, "Símbolo en entrega, liquidación o cerrado",
   "El contrato no está operativo (delivering/settling/closed/pre-trading).", "Se bloquea el símbolo.")
_e(-4109, "ACCOUNT_INACTIVE", "cuenta", Action.FIX_CONFIG, "Cuenta inactiva",
   "La cuenta de futuros está inactiva.", "Activa la cuenta transfiriendo fondos a Futuros.", "critical")
_e(-4116, "DUPLICATED_CLIENT_ORDER_ID", "orden", Action.NEW_CLIENT_ID, "clientOrderId duplicado",
   "Ya existe una orden abierta con ese clientOrderId.", "Se genera uno nuevo.", "warning", True)
_e(-4117, "STOP_ORDER_TRIGGERING", "orden", Action.RETRY, "La orden stop se está disparando",
   "No se puede modificar/cancelar mientras se dispara.", "Se reintenta en un momento.", "warning", True)
_e(-4118, "REDUCE_ONLY_MARGIN_CHECK_FAILED", "orden", Action.REFRESH_POSITION, "Falló la validación reduceOnly",
   "La orden reduceOnly no cuadra con la posición y órdenes abiertas.", "Se relee la posición y se ajusta.", "warning", True)
_e(-4120, "STOP_ORDER_SWITCH_ALGO", "orden", Action.USE_ALGO_API, "Tipo de orden solo por Algo Order API",
   "STOP/TAKE_PROFIT/TRAILING se migraron al servicio de Algo Orders.",
   "Se envía por algoOrder.place (WebSocket).", "warning", True)
_e(-4131, "MARKET_ORDER_REJECT", "precio", Action.CLAMP_PRICE, "Orden de mercado fuera de la banda PERCENT_PRICE",
   "El mejor precio de la contraparte está fuera del límite permitido (mercado muy volátil o ilíquido).",
   "Se reintenta como LIMIT IOC dentro de la banda, o espera a que el libro se normalice.", "warning", True)
_e(-4135, "INVALID_ACTIVATION_PRICE", "precio", Action.CLAMP_PRICE, "Precio de activación inválido",
   "activationPrice no es válido para el trailing.", "Corrige el precio de activación.")
_e(-4137, "QUANTITY_EXISTS_WITH_CLOSE_POSITION", "parámetros", Action.USE_QUANTITY, "quantity con closePosition",
   "No se puede enviar quantity junto a closePosition=true.", "Se usa solo quantity + reduceOnly.", "warning", True)
_e(-4138, "REDUCE_ONLY_MUST_BE_TRUE", "parámetros", Action.NONE, "reduceOnly debe ser true",
   "Este tipo de orden exige reduceOnly=true.", "Activa reduceOnly.")
_e(-4141, "SYMBOL_ALREADY_CLOSED", "símbolo", Action.BLOCK_SYMBOL, "Símbolo cerrado / deslistado",
   "El contrato fue cerrado.", "Se bloquea el símbolo.")
_e(-4144, "INVALID_PAIR", "símbolo", Action.BLOCK_SYMBOL, "Par inválido", "El par no existe.", "Revisa el símbolo.")
_e(-4161, "ISOLATED_LEVERAGE_REJECT_WITH_POSITION", "leverage", Action.NONE, "No se puede bajar el leverage con posición aislada",
   "En margen aislado con posición abierta no se puede reducir el leverage.", "Cierra la posición o usa CROSSED.")
_e(-4164, "MIN_NOTIONAL", "cantidad", Action.RAISE_NOTIONAL, "Notional menor al mínimo",
   "precio × cantidad es menor que el notional mínimo del símbolo (5 USDT en la mayoría).",
   "Se sube la cantidad al siguiente múltiplo de stepSize que cumple el mínimo.", "warning", True)
_e(-4183, "PRICE_HIGHTER_THAN_STOP_MULTIPLIER_UP", "precio", Action.CLAMP_PRICE, "Precio límite por encima de la banda del stop",
   "El precio de la orden stop-limit está fuera del límite superior.", "Ajusta el precio dentro de la banda.")
_e(-4184, "PRICE_LOWER_THAN_STOP_MULTIPLIER_DOWN", "precio", Action.CLAMP_PRICE, "Precio límite por debajo de la banda del stop",
   "El precio de la orden stop-limit está fuera del límite inferior.", "Ajusta el precio dentro de la banda.")
_e(-4192, "COOLING_OFF_PERIOD", "cuenta", Action.FIX_CONFIG, "Periodo de enfriamiento activo",
   "La cuenta está en cooling-off y no puede abrir posiciones.", "Espera a que termine el periodo.", "critical")
_e(-4202, "ADJUST_LEVERAGE_KYC_FAILED", "leverage", Action.LOWER_LEVERAGE, "Leverage alto requiere verificación",
   "Para usar más de 20x se requiere verificación intermedia (KYC).", "Se usa un leverage menor o completa la verificación.")
_e(-4203, "ADJUST_LEVERAGE_ONE_MONTH_FAILED", "leverage", Action.LOWER_LEVERAGE, "Leverage alto no disponible aún",
   "Más de 20x solo se habilita tras 30 días desde el registro.", "Se usa un leverage menor.")
_e(-4400, "TRADING_QUANTITATIVE_RULE", "cuenta", Action.FIX_CONFIG, "Reglas cuantitativas: solo reduceOnly",
   "Binance restringió temporalmente la cuenta por sus reglas cuantitativas.", "Solo se pueden cerrar posiciones hasta que expire.", "critical")
_e(-4401, "LARGE_POSITION_SYM_RULE", "cuenta", Action.REDUCE_SIZE, "Posición demasiado grande en el símbolo",
   "Se superó el límite de posición grande para el símbolo.", "Reduce el tamaño.")
_e(-4402, "COMPLIANCE_RESTRICTION", "cuenta", Action.FIX_CONFIG, "Restricción regional / de cumplimiento",
   "La función no está disponible en tu región.", "Revisa la región de la cuenta o usa un proxy en una región permitida.", "critical")
_e(-4403, "ADJUST_LEVERAGE_COMPLIANCE_FAILED", "leverage", Action.LOWER_LEVERAGE, "Leverage restringido por cumplimiento",
   "El leverage máximo está limitado por regulación local.", "Se usa un leverage menor.")
_e(-4509, "TIF_GTE_NEEDS_POSITION", "orden", Action.USE_QUANTITY, "GTE solo con posición abierta",
   "closePosition=true usa TIF GTE, que Binance solo acepta si ya existe posición u orden.",
   "Se envía el TP/SL con quantity + reduceOnly, que no depende del timing.", "warning", True)

# ── 50xx: órdenes avanzadas ───────────────────────────────────────────────
_e(-5021, "FOK_ORDER_REJECT", "orden", Action.REPRICE, "Orden FOK rechazada",
   "La orden Fill-or-Kill no se pudo llenar completa.", "Usa GTC/IOC o ajusta el precio.", "warning")
_e(-5022, "GTX_ORDER_REJECT", "orden", Action.REPRICE, "Post-only rechazada (cruzaría el libro)",
   "La orden post-only se habría ejecutado como taker.", "Se recoloca a un tick de distancia.", "warning", True)
_e(-5024, "MOVE_ORDER_NOT_ALLOWED", "orden", Action.NONE, "No se puede modificar la orden",
   "El símbolo no está en trading.", "Espera a que vuelva a TRADING.")
_e(-5025, "LIMIT_ORDER_ONLY", "orden", Action.NONE, "Solo órdenes LIMIT se pueden modificar", "order.modify solo aplica a LIMIT.", "Cancela y recrea.")
_e(-5027, "SAME_ORDER", "orden", Action.ALREADY_DONE, "La orden ya tiene esos valores", "No hay cambios que aplicar.", "No requiere acción.", "info")
_e(-5028, "ME_RECVWINDOW_REJECT", "reloj", Action.RESYNC_TIME, "Fuera del recvWindow del motor",
   "La orden llegó al motor demasiado tarde.", "Se resincroniza el reloj y se reintenta.", "warning", True)
_e(-5041, "BBO_ORDER_REJECT", "orden", Action.REPRICE, "Orden BBO sin profundidad", "No hay profundidad para priceMatch.", "Usa precio explícito.")


# Errores HTTP sin código Binance (o con el código en el cuerpo).
HTTP_CATALOG: dict[int, ErrorInfo] = {
    403: ErrorInfo(403, "WAF_FORBIDDEN", "red", Action.USE_PROXY, "Bloqueado por el firewall (WAF)",
                   "Binance bloqueó la petición (WAF o IP no permitida).",
                   "Se reintenta por un proxy de PROXY_URLS.", "critical", True),
    408: ErrorInfo(408, "REQUEST_TIMEOUT", "red", Action.RETRY, "Timeout de la petición",
                   "La petición tardó demasiado.", "Se reintenta.", "warning", True),
    418: ErrorInfo(418, "IP_AUTO_BANNED", "límite", Action.WAIT, "IP baneada automáticamente",
                   "Se siguió enviando peticiones tras un 429.", "Se pausa el canal hasta que expire el ban.", "critical", True),
    429: ErrorInfo(429, "RATE_LIMITED", "límite", Action.WAIT, "Rate limit excedido",
                   "Se superó el límite de peso o de órdenes.", "Se espera el tiempo indicado (Retry-After).", "warning", True),
    451: ErrorInfo(451, "RESTRICTED_LOCATION", "red", Action.USE_PROXY, "Ubicación restringida",
                   "Binance no presta servicio desde la región de esta IP.",
                   "Configura PROXY_URLS con una IP de una región permitida.", "critical", True),
    503: ErrorInfo(503, "SERVICE_UNAVAILABLE", "red", Action.CHECK_STATUS, "Servicio no disponible: estado desconocido",
                   "Binance aceptó la petición pero no confirmó el resultado.",
                   "Se verifica el estado antes de reintentar.", "warning", True),
}

GENERIC_ERROR = ErrorInfo(0, "UNCLASSIFIED", "desconocido", Action.NONE, "Error no catalogado",
                          "Binance devolvió un error sin código conocido.",
                          "Revisa el mensaje original; si se repite, regístralo en el catálogo.")
NETWORK_ERROR = ErrorInfo(-1, "NETWORK", "red", Action.RETRY, "Fallo de red / conexión",
                          "Se perdió la conexión con Binance o expiró el tiempo de espera.",
                          "Se reconecta y se reintenta automáticamente.", "warning", True)


_CODE_RE = re.compile(r'["\']?code["\']?\s*[:=]\s*(-?\d{3,5})')
_MSG_RE = re.compile(r'["\']?msg["\']?\s*[:=]\s*["\'](.*?)["\']\s*[,}]', re.S)
_BAN_RE = re.compile(r"banned until (\d{13})")
_BARE_CODE_RE = re.compile(r"(?<![\d.])(-[1-5]\d{3})(?!\d)")


class BinanceAPIError(Exception):
    """Error devuelto por Binance (WS API o REST) con su código y contexto."""

    def __init__(
        self,
        code: int,
        msg: str,
        *,
        http_status: int = 0,
        method: str = "",
        transport: str = "ws",
        retry_after_s: float = 0.0,
        params: Optional[dict] = None,
    ):
        self.code = int(code or 0)
        self.msg = str(msg or "")
        self.http_status = int(http_status or 0)
        self.method = method
        self.transport = transport
        self.params = _sanitize(params or {})
        self.retry_after_s = retry_after_s or _ban_seconds(self.msg)
        super().__init__(f"[{self.code}] {self.msg}" + (f" (HTTP {self.http_status})" if self.http_status else ""))

    @property
    def info(self) -> ErrorInfo:
        return lookup(self.code, self.http_status)


def _sanitize(params: dict) -> dict:
    hidden = {"signature", "apiKey", "listenKey"}
    return {k: ("***" if k in hidden else v) for k, v in params.items()}


def _ban_seconds(msg: str) -> float:
    m = _BAN_RE.search(msg or "")
    if not m:
        return 0.0
    return max(0.0, int(m.group(1)) / 1000 - time.time())


def lookup(code: int, http_status: int = 0) -> ErrorInfo:
    if code and code in CATALOG:
        return CATALOG[code]
    if http_status in HTTP_CATALOG:
        return HTTP_CATALOG[http_status]
    if code == -1:
        return NETWORK_ERROR
    return GENERIC_ERROR


def parse_error(error: Any) -> tuple[int, str, int]:
    """Extrae (code, msg, http_status) de una excepción, dict o texto."""
    if isinstance(error, BinanceAPIError):
        return error.code, error.msg, error.http_status
    if isinstance(error, (asyncio.TimeoutError, ConnectionError, OSError)):
        return -1, str(error) or error.__class__.__name__, 0
    if isinstance(error, dict):
        payload = error.get("error", error)
        return int(payload.get("code") or 0), str(payload.get("msg") or ""), int(error.get("status") or 0)
    text = str(error)
    try:
        data = json.loads(text)
        if isinstance(data, dict) and "code" in data:
            return int(data["code"]), str(data.get("msg", "")), 0
    except (ValueError, TypeError):
        pass
    code_m = _CODE_RE.search(text) or _BARE_CODE_RE.search(text)
    msg_m = _MSG_RE.search(text)
    code = int(code_m.group(1)) if code_m else 0
    return code, (msg_m.group(1) if msg_m else text), 0


@dataclass
class Diagnosis:
    code: int
    msg: str
    http_status: int
    info: ErrorInfo
    action: Action
    retry_after_s: float = 0.0
    context: dict = field(default_factory=dict)

    @property
    def retryable(self) -> bool:
        return self.info.retryable

    def summary(self) -> str:
        code = f"{self.code}" if self.code else (f"HTTP {self.http_status}" if self.http_status else "red")
        return f"[{code}] {self.info.title} — {self.info.solution}"

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "msg": self.msg,
            "http_status": self.http_status,
            "name": self.info.name,
            "title": self.info.title,
            "cause": self.info.cause,
            "solution": self.info.solution,
            "category": self.info.category,
            "severity": self.info.severity,
            "action": self.action.value,
            "action_label": ACTION_LABELS.get(self.action, ""),
            "retry_after_s": round(self.retry_after_s, 1),
            "context": self.context,
        }


class ErrorDoctor:
    """Diagnostica errores y decide la corrección automática."""

    @staticmethod
    def diagnose(error: Any, **context) -> Diagnosis:
        code, msg, http_status = parse_error(error)
        info = lookup(code, http_status)
        action = info.action
        low = msg.lower()

        # Refinamientos según el mensaje concreto.
        if code == -1106 and "reduceonly" not in low:
            action = Action.NONE
        elif code == -1013:
            if "notional" in low:
                action = Action.RAISE_NOTIONAL
            elif "price" in low and "lot" not in low:
                action = Action.FIX_PRICE_TICK
        elif code == 0 and http_status == 0:
            if "notional" in low:
                action = Action.RAISE_NOTIONAL
            elif "precision" in low or "lot_size" in low:
                action = Action.FIX_PRECISION
            elif "margin is insufficient" in low:
                info = CATALOG[-2019]
                action = info.action

        retry_after = 0.0
        if isinstance(error, BinanceAPIError):
            retry_after = error.retry_after_s
        if not retry_after:
            retry_after = _ban_seconds(msg)
        if action == Action.WAIT and not retry_after:
            retry_after = 30.0 if code == -1003 or http_status in (418, 429) else 5.0

        return Diagnosis(code, msg, http_status, info, action, retry_after, context)

    @staticmethod
    def explain(code: int) -> dict:
        info = lookup(int(code), int(code) if int(code) > 0 else 0)
        data = info.to_dict()
        data["known"] = info is not GENERIC_ERROR
        return data

    @staticmethod
    def catalog() -> list[dict]:
        items = [info.to_dict() for info in CATALOG.values()]
        items += [info.to_dict() for info in HTTP_CATALOG.values()]
        return sorted(items, key=lambda d: (d["code"] > 0, -d["code"] if d["code"] < 0 else d["code"]))


@dataclass
class JournalEntry:
    id: int
    ts: float
    where: str
    symbol: str
    code: int
    http_status: int
    name: str
    title: str
    msg: str
    solution: str
    action: str
    severity: str
    fixed: bool = False
    fix_note: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


class ErrorJournal:
    """Historial de errores (en memoria) con su diagnóstico."""

    def __init__(self, maxlen: int = 300):
        self._entries: deque[JournalEntry] = deque(maxlen=maxlen)
        self._counts: Counter = Counter()
        self._seq = 0
        self._listeners: list[Callable[[JournalEntry], None]] = []

    def subscribe(self, callback: Callable[[JournalEntry], None]) -> None:
        self._listeners.append(callback)

    def record(self, diagnosis: Diagnosis, where: str, symbol: str = "", fixed: bool = False, fix_note: str = "") -> JournalEntry:
        self._seq += 1
        entry = JournalEntry(
            id=self._seq,
            ts=time.time(),
            where=where,
            symbol=symbol,
            code=diagnosis.code,
            http_status=diagnosis.http_status,
            name=diagnosis.info.name,
            title=diagnosis.info.title,
            msg=diagnosis.msg[:400],
            solution=diagnosis.info.solution,
            action=diagnosis.action.value,
            severity=diagnosis.info.severity,
            fixed=fixed,
            fix_note=fix_note,
        )
        self._entries.append(entry)
        self._counts[diagnosis.code or diagnosis.http_status] += 1
        for cb in list(self._listeners):
            try:
                cb(entry)
            except Exception:  # pragma: no cover - un listener roto no debe tumbar el executor
                pass
        return entry

    def mark_fixed(self, entry: Optional[JournalEntry], note: str) -> None:
        if entry is None:
            return
        entry.fixed = True
        entry.fix_note = note
        for cb in list(self._listeners):
            try:
                cb(entry)
            except Exception:  # pragma: no cover
                pass

    def entries(self, limit: int = 100) -> list[dict]:
        return [e.to_dict() for e in list(self._entries)[-limit:]][::-1]

    def counts(self) -> dict:
        return {str(k): v for k, v in self._counts.most_common()}

    def clear(self) -> None:
        self._entries.clear()
        self._counts.clear()

    def __len__(self) -> int:
        return len(self._entries)
