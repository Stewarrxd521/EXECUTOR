"""Aritmética decimal exacta para precios y cantidades.

Binance valida ``price`` contra ``tickSize`` y ``quantity`` contra
``stepSize`` con aritmética decimal. Trabajar con ``float`` produce
errores como ``0.30000000000000004`` que terminan en ``-1111``/``-4014``,
así que todo redondeo pasa por ``Decimal``.
"""

from __future__ import annotations

import math
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal, InvalidOperation
from typing import Union

Number = Union[int, float, str, Decimal]

ZERO = Decimal("0")
ONE = Decimal("1")


def D(value: Number) -> Decimal:
    """Convierte a Decimal sin heredar el ruido binario de los float."""
    if isinstance(value, Decimal):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"número no finito: {value}")
        return Decimal(repr(value))
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"número inválido: {value!r}") from exc


_TRUE = {"1", "true", "yes", "y", "si", "sí", "s", "on", "t"}
_FALSE = {"0", "false", "no", "n", "off", "f", "", "none", "null"}


def parse_bool(value, default: bool = False) -> bool:
    """Booleano tolerante: acepta bool, números y textos ('true', 'false', '0', 'sí'...)."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    return default


def safe_float(value, default: float = 0.0) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return default
    return out if math.isfinite(out) else default


def _quantize(value: Number, step: Number, rounding: str) -> Decimal:
    v, s = D(value), D(step)
    if s <= 0:
        return v
    units = (v / s).to_integral_value(rounding=rounding)
    return (units * s).quantize(s) if s.as_tuple().exponent < 0 else units * s


def floor_step(value: Number, step: Number) -> Decimal:
    return _quantize(value, step, ROUND_FLOOR)


def ceil_step(value: Number, step: Number) -> Decimal:
    return _quantize(value, step, ROUND_CEILING)


def round_step(value: Number, step: Number) -> Decimal:
    return _quantize(value, step, ROUND_HALF_UP)


def is_multiple(value: Number, step: Number) -> bool:
    s = D(step)
    if s <= 0:
        return True
    return (D(value) % s) == 0


def decimals_of(step: Number) -> int:
    """Decimales implicados por un step (``0.001`` → 3, ``1`` → 0)."""
    exp = D(step).normalize().as_tuple().exponent
    return max(0, -int(exp))


def fmt(value: Number) -> str:
    """Formatea sin notación científica ni ceros sobrantes (formato Binance)."""
    d = D(value)
    if d == d.to_integral_value():
        return str(d.quantize(ONE))
    text = format(d.normalize(), "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def coarser_step(step: Number) -> Decimal:
    """Siguiente paso más grueso (0.001 → 0.01 → 0.1 → 1 → 10)."""
    s = D(step)
    if s <= 0:
        return ONE
    return (s * 10).normalize()
