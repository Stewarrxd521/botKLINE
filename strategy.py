"""
strategy.py — Reglas puras de la estrategia (sin red ni estado), fáciles de probar.

  Señal   cuerpo de la última vela 1m cerrada = (cierre - apertura) / apertura.
          ≥ +UMBRAL% → LONG  |  ≤ -UMBRAL% → SHORT (se sigue la tendencia).
  Salida  el precio avanza TP% a favor desde la entrada. Sin stop loss.
  Tamaño  notional fijo (5 USDT), redondeando la cantidad HACIA ARRIBA al
          stepSize para no quedar por debajo del mínimo de Binance.
"""

from __future__ import annotations

from math import ceil
from typing import Optional, Tuple

LONG = "LONG"
SHORT = "SHORT"


def body_change_pct(open_: float, close: float) -> float:
    """% de cambio del cuerpo de una vela (cierre contra apertura)."""
    if open_ <= 0:
        return 0.0
    return (close - open_) / open_ * 100.0


def signal_direction(open_: float, close: float, threshold_pct: float) -> Optional[str]:
    """LONG si la vela subió ≥ threshold_pct, SHORT si bajó ≥ threshold_pct, si no None."""
    body = body_change_pct(open_, close)
    if body >= threshold_pct:
        return LONG
    if body <= -threshold_pct:
        return SHORT
    return None


def favorable_move_pct(price: float, entry: float, direction: str) -> float:
    """Cuánto se movió el precio a favor de la posición, en % de la entrada."""
    if entry <= 0 or price <= 0:
        return 0.0
    move = (price - entry) / entry * 100.0
    return move if direction == LONG else -move


def take_profit_price(entry: float, direction: str, tp_pct: float) -> float:
    factor = 1.0 + tp_pct / 100.0 if direction == LONG else 1.0 - tp_pct / 100.0
    return entry * factor


def take_profit_hit(price: float, entry: float, direction: str, tp_pct: float) -> bool:
    if price <= 0 or entry <= 0:
        return False
    target = take_profit_price(entry, direction, tp_pct)
    return price >= target if direction == LONG else price <= target


def pnl_usd(price: float, entry: float, qty: float, direction: str) -> float:
    if price <= 0:
        return 0.0
    diff = price - entry
    return diff * qty if direction == LONG else -diff * qty


def _decimals(step: float) -> int:
    text = f"{step:.12f}".rstrip("0")
    return len(text.split(".")[1]) if "." in text else 0


def size_order(notional: float, price: float, step: float = 0.001, min_qty: float = 0.0,
               min_notional: float = 5.0, max_over: float = 1.5) -> Tuple[float, float, str]:
    """(qty, notional_efectivo, "") o (0, 0, motivo) si el par no admite ese tamaño.

    La cantidad se redondea HACIA ARRIBA al stepSize: 5 USDT nunca queda en
    4,99 (Binance la rechazaría). Si el mínimo del par es mayor que el
    notional pedido, o el redondeo lo infla por encima de notional × max_over
    (pares caros con stepSize grueso), se descarta."""
    if price <= 0:
        return 0.0, 0.0, "sin precio"
    if min_notional > notional + 1e-9:
        return 0.0, 0.0, f"mínimo del par {min_notional:g} USDT > {notional:g} USDT"
    step = step if step > 0 else 0.001
    decs = _decimals(step)
    steps = ceil(notional / price / step - 1e-9)
    qty = round(max(steps * step, min_qty), decs)
    if qty * price < min_notional - 1e-9:
        qty = round(qty + step, decs)
    eff = qty * price
    if eff > notional * max_over:
        return 0.0, 0.0, f"stepSize {step:g} obliga a {eff:.2f} USDT (> {notional * max_over:g})"
    return qty, eff, ""
