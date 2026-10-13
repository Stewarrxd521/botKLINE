"""
app.py — Bot de velas 1m (Binance USDⓈ-M Futures) que sigue la tendencia.

Estrategia
  • Velas 1m de TODOS los perpetuos USDT por WebSocket (kline_ws.py, sin REST).
    Se guardan las dos últimas velas cerradas de cada símbolo.
  • Al cerrar cada vela se mide su cuerpo: (cierre - apertura) / apertura.
      ≥ +SIGNAL_THRESHOLD_PCT (3 %)  → abre LONG
      ≤ -SIGNAL_THRESHOLD_PCT (-3 %) → abre SHORT
  • Cada entrada usa un notional fijo de NOTIONAL_USDT (5 USDT). Una sola
    posición por símbolo: mientras esté abierta se ignoran nuevas señales.
  • Cierre cuando el precio avanza TAKE_PROFIT_PCT (2 %) o más a favor desde
    la entrada. SIN stop loss (solo cierre manual desde la web).
  • Las órdenes NO salen de aquí: se mandan como señales al Executor externo
    (POST <executor>/signal), igual que antes. El link del Executor se puede
    cambiar en caliente desde la web o con POST /api/executor.

Precios para el take profit: WS.py (!markPrice@arr de todos los símbolos +
bookTicker en tiempo real de los que tienen posición abierta).
Recuperación tras reinicio: state_store.py (Upstash Redis + copia local).
"""

from __future__ import annotations

import asyncio
import atexit
import concurrent.futures
import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from typing import Deque, Dict, List, Optional, Tuple

from flask import Flask, jsonify, make_response, request

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from WS import SymbolWebSocketPriceCache                       # noqa: E402
from kline_ws import Candle, KlineWebSocketStream              # noqa: E402
from state_store import StateStore, backend_from_env, make_owner_id  # noqa: E402
import strategy                                               # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────

BASE_URL    = os.getenv("BASE_URL", "https://fapi.binance.com")
QUOTE_ASSET = os.getenv("QUOTE_ASSET", "USDT")
# Proxy HTTP opcional SOLO para la única consulta REST del arranque (exchangeInfo).
REST_PROXY_URL = os.getenv("REST_PROXY_URL", "http://fixie:6U5qGtczYcZJRHN@ventoux.usefixie.com:80").strip()
INITIAL_SYMBOLS = [s.strip().upper() for s in os.getenv("INITIAL_SYMBOLS", "").split(",") if s.strip()]
SYMBOLS_CACHE_FILE = os.getenv(
    "SYMBOLS_CACHE_FILE", os.path.join(tempfile.gettempdir(), "futures_symbols_cache.json"))
SYMBOL_REFRESH_HOURS = float(os.getenv("SYMBOL_REFRESH_HOURS", "12"))

# ── Estrategia ────────────────────────────────────────────────────────────────
SIGNAL_THRESHOLD_PCT = float(os.getenv("SIGNAL_THRESHOLD_PCT", "3"))    # cuerpo mínimo de la vela
TAKE_PROFIT_PCT      = float(os.getenv("TAKE_PROFIT_PCT", "2"))         # avance a favor para cerrar
NOTIONAL_USDT        = float(os.getenv("NOTIONAL_USDT", "5"))           # tamaño de cada entrada
# Si el stepSize obliga a una cantidad mayor que NOTIONAL × esto, el par se salta.
MAX_NOTIONAL_OVER    = float(os.getenv("MAX_NOTIONAL_OVER", "1.5"))
# Una vela cuyo cierre llega más tarde que esto (reconexión) no genera señal.
SIGNAL_MAX_DELAY_S   = float(os.getenv("SIGNAL_MAX_DELAY_S", "10"))
# Pausa (s) antes de volver a operar un símbolo tras cerrarlo. 0 = sin pausa.
COOLDOWN_SECONDS     = float(os.getenv("COOLDOWN_SECONDS", "0"))
PRICE_MAX_AGE_S      = float(os.getenv("PRICE_MAX_AGE_S", "5"))         # precio más viejo no se usa
SAFETY_TICK_SECS     = float(os.getenv("SAFETY_TICK_SECS", "1"))        # revisión periódica del TP

# ── Velas ─────────────────────────────────────────────────────────────────────
KLINE_INTERVAL = "1m"
KLINE_HISTORY  = max(2, int(os.getenv("KLINE_HISTORY", "2")))           # última y penúltima vela
KLINE_DTYPE    = os.getenv("KLINE_DTYPE", "float64")

# ── Executor externo ──────────────────────────────────────────────────────────
EXECUTOR_URL    = os.getenv("EXECUTOR_URL", "https://executor-5lu0.onrender.com")
EXECUTOR_SECRET = os.getenv("EXECUTOR_SECRET", "clave-secreta-aleatoria")
# Si se define, los POST de la web (cambiar executor, cerrar) exigen este token.
ADMIN_TOKEN     = os.getenv("ADMIN_TOKEN", "").strip()

# ── Persistencia ──────────────────────────────────────────────────────────────
SETTINGS_FILE     = os.getenv("SETTINGS_FILE", os.path.join(_HERE, "bot_settings.json"))
RECOVERY_FILE     = os.getenv("RECOVERY_FILE", os.path.join(tempfile.gettempdir(), "botkline_recovery.json"))
STATE_KEY_PREFIX  = os.getenv("STATE_KEY_PREFIX", "botkline")
STATE_TRADES_MAX  = int(os.getenv("STATE_TRADES_MAX", "5000"))
STATE_BOOT_WAIT_S = float(os.getenv("STATE_BOOT_WAIT_S", "20"))
STATE_PERSIST_SECS = float(os.getenv("STATE_PERSIST_SECS", "30"))
SHUTDOWN_DRAIN_S  = 5.0

STATUS_POLL_MS = int(os.getenv("STATUS_POLL_MS", "1500"))


def _utc(ts: float, pattern: str = "%Y-%m-%d %H:%M:%S") -> str:
    try:
        return datetime.fromtimestamp(float(ts), timezone.utc).strftime(pattern)
    except (TypeError, ValueError, OverflowError, OSError):
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# EXECUTOR (señales de apertura/cierre)
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ExecutorTarget:
    url:    str = ""
    secret: str = ""


class ExecutorBridge:
    """Envía señales al Executor. El destino se puede cambiar en caliente:
    cada envío toma el destino vigente en ese momento (lectura atómica)."""

    def __init__(self, url: str, secret: str, logger=print, timeout: float = 8.0) -> None:
        self.target = ExecutorTarget(self.clean_url(url), secret)
        self.logger = logger
        self.timeout = timeout
        self.sent = 0
        self.errors = 0
        self.last_error = ""
        self.last_ok_ts = 0.0

    @staticmethod
    def clean_url(url: str) -> str:
        return (url or "").strip().rstrip("/")

    def set_target(self, url: str, secret: Optional[str] = None) -> ExecutorTarget:
        old = self.target
        self.target = ExecutorTarget(self.clean_url(url), old.secret if secret is None else secret)
        return self.target

    def _log(self, msg: str) -> None:
        try:
            self.logger(msg)
        except Exception:
            pass

    def send_sync(self, payload: dict, target: ExecutorTarget) -> bool:
        if not target.url:
            return False
        req = urllib.request.Request(
            f"{target.url}/signal",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "X-Signal-Secret": target.secret},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                resp.read()
            self.sent += 1
            self.last_ok_ts = time.time()
            self._log(f"[executor] ✓ {payload.get('action')} {payload.get('symbol')} "
                      f"{payload.get('direction')} → {target.url}")
            return True
        except Exception as exc:
            self.errors += 1
            self.last_error = f"{payload.get('action')} {payload.get('symbol')}: {exc}"
            self._log(f"[executor] ✗ error enviando {self.last_error} → {target.url}")
            return False

    def send(self, payload: dict) -> None:
        """No bloquea: el envío va en un hilo con el destino vigente AHORA."""
        target = self.target
        if not target.url:
            self._log(f"[executor] sin URL: no se envía {payload.get('action')} {payload.get('symbol')}")
            return
        threading.Thread(target=self.send_sync, args=(payload, target), daemon=True).start()

    def probe(self, url: str) -> Tuple[bool, str]:
        """GET <url>/api/state para comprobar que el Executor responde."""
        url = self.clean_url(url)
        try:
            with urllib.request.urlopen(f"{url}/api/state", timeout=self.timeout) as resp:
                return True, f"HTTP {resp.status}"
        except urllib.error.HTTPError as exc:
            return True, f"HTTP {exc.code}"      # responde, aunque no tenga /api/state
        except Exception as exc:
            return False, str(exc)


# ─────────────────────────────────────────────────────────────────────────────
# MODELO
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Position:
    trade_id:    int
    symbol:      str
    direction:   str            # LONG | SHORT
    entry_price: float
    qty:         float
    notional:    float
    tp_price:    float
    signal_pct:  float          # cuerpo de la vela que disparó la entrada
    candle_open: float
    candle_close: float
    candle_time: int            # open_time (ms) de esa vela
    opened_ts:   float = field(default_factory=time.time)
    last_price:  float = 0.0
    mfe_pct:     float = 0.0    # mejor avance a favor visto
    mae_pct:     float = 0.0    # peor retroceso en contra visto

    def update(self, price: float) -> float:
        self.last_price = price
        move = strategy.favorable_move_pct(price, self.entry_price, self.direction)
        self.mfe_pct = max(self.mfe_pct, move)
        self.mae_pct = min(self.mae_pct, move)
        return move


# ─────────────────────────────────────────────────────────────────────────────
# BOT
# ─────────────────────────────────────────────────────────────────────────────

class KlineBot:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.positions: Dict[str, Position] = {}
        self.closed_trades: List[dict] = []
        self.signals: Deque[dict] = deque(maxlen=200)
        self.events: Deque[str] = deque(maxlen=200)
        self.cooldown: Dict[str, float] = {}
        self.total_realized_pnl = 0.0
        self.trade_id_seq = 0
        self.signal_count = 0
        self.opened_count = 0
        self.last_error = ""

        self.filters: Dict[str, Dict[str, float]] = {}
        self.symbols: List[str] = []

        self.price_cache: Optional[SymbolWebSocketPriceCache] = None
        self.kline_stream: Optional[KlineWebSocketStream] = None

        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self.thread: Optional[threading.Thread] = None
        self.running = False
        self.started_at = time.time()

        self._pos_syms: frozenset = frozenset()     # copia para el hilo del WS (sin lock)
        self._last_candle: Dict[str, int] = {}      # dedupe de cierres por símbolo
        self._pending: set = set()                  # símbolos con tick de precio pendiente
        self._pending_lock = threading.Lock()
        self._wake_scheduled = False
        self._tick_event: Optional[asyncio.Event] = None
        self._closing: set = set()
        self._inflight = 0

        settings = self._load_settings()
        self.executor = ExecutorBridge(settings.get("executor_url", EXECUTOR_URL),
                                       settings.get("executor_secret", EXECUTOR_SECRET),
                                       logger=self.log)
        self.executor_changed_ts = float(settings.get("executor_changed_ts") or 0.0)

        backend, self._store_desc = backend_from_env(self.log)
        self.store = StateStore(
            backend, RECOVERY_FILE, make_owner_id(),
            build_doc=self._recovery_doc, log=self.log, on_fenced=self._on_fenced,
            key_prefix=STATE_KEY_PREFIX, max_trades=STATE_TRADES_MAX,
        )

    # ── Utilidades ────────────────────────────────────────────────────────────

    def log(self, msg: str) -> None:
        line = f"{_utc(time.time())} | {msg}"
        print(line, flush=True)
        with self.lock:
            self.events.appendleft(line)

    def _next_trade_id(self) -> int:
        with self.lock:
            # Arranca en la hora Unix para no repetir ids que el Executor ya vio
            # de versiones anteriores del bot.
            self.trade_id_seq = max(self.trade_id_seq + 1, int(time.time()))
            return self.trade_id_seq

    def _refresh_pos_syms(self) -> None:
        with self.lock:
            self._pos_syms = frozenset(self.positions)

    # ── Ajustes locales (link del executor) ───────────────────────────────────

    @staticmethod
    def _load_settings() -> dict:
        try:
            with open(SETTINGS_FILE, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save_settings(self) -> None:
        t = self.executor.target
        data = {"executor_url": t.url, "executor_secret": t.secret,
                "executor_changed_ts": self.executor_changed_ts}
        tmp = f"{SETTINGS_FILE}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh)
            os.replace(tmp, SETTINGS_FILE)
        except Exception as exc:
            self.log(f"No pude guardar ajustes locales: {exc}")

    def set_executor(self, url: str, secret: Optional[str] = None) -> ExecutorTarget:
        """Cambia el destino de las señales en caliente (sin reiniciar)."""
        old = self.executor.target.url
        target = self.executor.set_target(url, secret)
        self.executor_changed_ts = time.time()
        self._save_settings()
        self.store.mark_dirty(True)
        self.log(f"[executor] link cambiado: {old or '(vacío)'} → {target.url or '(vacío)'}"
                 + (" (secreto actualizado)" if secret is not None else ""))
        return target

    # ── Arranque / parada ─────────────────────────────────────────────────────

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self._run_loop, daemon=True, name="BotLoop")
        self.thread.start()

    def _run_loop(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self._tick_event = asyncio.Event()
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:
            self.running = False
            self.last_error = str(exc)
            self.log(f"Bot detenido por error no controlado: {exc!r}")

    async def _main(self) -> None:
        self.log(f"Bot iniciado | señal: cuerpo vela 1m ≥ {SIGNAL_THRESHOLD_PCT:g}% | "
                 f"TP {TAKE_PROFIT_PCT:g}% | {NOTIONAL_USDT:g} USDT | sin stop loss | "
                 f"executor → {self.executor.target.url or '(vacío)'}")
        self.log(f"[estado] Persistencia: {self._store_desc}")
        self.store.start()
        await self._init_symbols()

        # El estado (posiciones abiertas) se restaura ANTES de abrir los WebSockets.
        if await asyncio.to_thread(self.store.acquired.wait, STATE_BOOT_WAIT_S):
            self._restore_from_store()
        else:
            self.log("[estado] ⚠️ Aún no controlo el estado (otra instancia activa o Upstash "
                     "no responde): sin entradas ni cierres hasta recuperarlo")

        self.price_cache = SymbolWebSocketPriceCache(list(self.positions))
        self.price_cache.set_update_callback(self._on_price_event)
        self.price_cache.start()

        self.kline_stream = KlineWebSocketStream(
            self.symbols, intervals=(KLINE_INTERVAL,), history=KLINE_HISTORY,
            dtype=KLINE_DTYPE, on_close=self._on_kline_close,
        )
        self.kline_stream.start()
        self.log(f"Velas 1m por WebSocket: {len(self.symbols)} símbolos, "
                 f"{KLINE_HISTORY} velas cerradas por símbolo")

        await asyncio.gather(
            self._supervised(self._engine_loop, "engine"),
            self._supervised(self._maintenance_loop, "maintenance"),
        )

    async def _supervised(self, factory, name: str) -> None:
        while self.running:
            try:
                await factory()
            except asyncio.CancelledError:
                if not self.running:
                    break
            except Exception as exc:
                self.last_error = f"{name}: {exc!r}"
                self.log(f"[supervisor] {name} falló: {exc!r} — reinicio en 2 s")
            await asyncio.sleep(2.0)

    def shutdown(self) -> None:
        """Apagado ordenado (atexit al recibir SIGTERM): deja de operar, espera
        las órdenes en vuelo, guarda por última vez y libera el control."""
        self.store.begin_stop()
        deadline = time.time() + SHUTDOWN_DRAIN_S
        while time.time() < deadline and (self._inflight or self._closing):
            time.sleep(0.1)
        if self.store.close():
            print("[estado] guardado final enviado", flush=True)

    # ── Símbolos y filtros (UNA petición REST como mucho) ─────────────────────

    def _fetch_exchange_info(self) -> Dict[str, Dict[str, float]]:
        routes = [("proxy", REST_PROXY_URL), ("IP directa", "")] if REST_PROXY_URL else [("IP directa", "")]
        last_exc: Optional[Exception] = None
        for label, proxy in routes:
            try:
                handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})] if proxy else []
                opener = urllib.request.build_opener(*handlers)
                req = urllib.request.Request(f"{BASE_URL}/fapi/v1/exchangeInfo",
                                             headers={"User-Agent": "BOTKLINE/1.0"})
                with opener.open(req, timeout=15) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                self.log(f"REST exchangeInfo por {label}")
                break
            except Exception as exc:
                last_exc = exc
                self.log(f"[REST] exchangeInfo por {label} falló: {exc}")
        else:
            raise RuntimeError(f"exchangeInfo falló: {last_exc}")
        out: Dict[str, Dict[str, float]] = {}
        for s in data.get("symbols", []):
            if (s.get("quoteAsset") != QUOTE_ASSET or s.get("contractType") != "PERPETUAL"
                    or s.get("status") != "TRADING"):
                continue
            row = {"stepSize": 0.001, "minQty": 0.0, "minNotional": 5.0}
            for f in s.get("filters", []):
                if f.get("filterType") == "LOT_SIZE":
                    row["stepSize"] = float(f.get("stepSize", row["stepSize"]))
                    row["minQty"] = float(f.get("minQty", row["minQty"]))
                elif f.get("filterType") == "MIN_NOTIONAL":
                    row["minNotional"] = float(f.get("notional", row["minNotional"]))
            out[s["symbol"]] = row
        return out

    async def _init_symbols(self) -> None:
        cached = None
        try:
            with open(SYMBOLS_CACHE_FILE, "r", encoding="utf-8") as fh:
                cached = json.load(fh)
        except Exception:
            pass
        age_h = (time.time() - float(cached.get("saved_at", 0))) / 3600 if cached else None
        if cached and cached.get("filters") and age_h is not None and age_h < SYMBOL_REFRESH_HOURS:
            self.filters = dict(cached["filters"])
            self.log(f"Símbolos desde caché en disco: {len(self.filters)} (hace {age_h:.1f} h)")
        else:
            try:
                self.filters = await asyncio.to_thread(self._fetch_exchange_info)
                tmp = f"{SYMBOLS_CACHE_FILE}.tmp"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump({"symbols": sorted(self.filters), "filters": self.filters,
                               "saved_at": time.time()}, fh)
                os.replace(tmp, SYMBOLS_CACHE_FILE)
            except Exception as exc:
                self.last_error = str(exc)
                if cached and cached.get("filters"):
                    self.filters = dict(cached["filters"])
                    self.log(f"Usando caché viejo de símbolos ({len(self.filters)})")
                elif INITIAL_SYMBOLS:
                    self.filters = {s: {"stepSize": 0.001, "minQty": 0.0, "minNotional": 5.0}
                                    for s in INITIAL_SYMBOLS}
                    self.log(f"Usando INITIAL_SYMBOLS ({len(self.filters)}) sin filtros reales")
                else:
                    self.log("⚠️ Sin lista de símbolos: el bot no puede suscribir velas")
        self.symbols = sorted(self.filters)
        self.log(f"{len(self.symbols)} perpetuos {QUOTE_ASSET} operables")

    # ── Entradas: cierre de vela (hilo del WS de velas) ───────────────────────

    def _on_kline_close(self, symbol: str, interval: str, candle: Candle) -> None:
        """Debe ser instantáneo: solo filtra y pasa la señal al loop del bot."""
        direction = strategy.signal_direction(candle.open, candle.close, SIGNAL_THRESHOLD_PCT)
        if direction is None:
            return
        loop = self.loop
        if loop is not None:
            loop.call_soon_threadsafe(self._handle_signal, symbol, direction, candle, time.time())

    def _record_signal(self, symbol: str, direction: str, candle: Candle, action: str) -> None:
        with self.lock:
            self._append_signal(symbol, direction, candle, action)

    def _append_signal(self, symbol: str, direction: str, candle: Candle, action: str) -> None:
        self.signals.appendleft({
            "ts":        time.time(),
            "time":      _utc(time.time(), "%H:%M:%S"),
            "symbol":    symbol,
            "direction": direction,
            "body_pct":  strategy.body_change_pct(candle.open, candle.close),
            "open":      candle.open,
            "close":     candle.close,
            "candle":    _utc(candle.open_time / 1000, "%H:%M"),
            "action":    action,
        })

    def _handle_signal(self, symbol: str, direction: str, candle: Candle, arrived: float) -> None:
        """Corre en el loop del bot. Decide si abrir y lanza la apertura."""
        if self._last_candle.get(symbol, -1) >= candle.open_time:
            return                                          # duplicado tras reconexión
        self._last_candle[symbol] = candle.open_time
        self.signal_count += 1
        body = strategy.body_change_pct(candle.open, candle.close)

        delay = arrived - candle.close_time / 1000
        if delay > SIGNAL_MAX_DELAY_S:
            self._record_signal(symbol, direction, candle, f"ignorada: llegó {delay:.0f}s tarde")
            return
        if not self.store.can_open():
            self._record_signal(symbol, direction, candle, "ignorada: estado sin confirmar")
            return
        with self.lock:
            if symbol in self.positions:
                held = self.positions[symbol].direction
                self._record_signal(symbol, direction, candle, f"ignorada: ya hay {held} abierto")
                return
            until = self.cooldown.get(symbol, 0.0)
        if until > time.time():
            self._record_signal(symbol, direction, candle, "ignorada: en pausa tras cierre")
            return

        price = self._price(symbol) or candle.close
        f = self.filters.get(symbol, {})
        qty, eff, why = strategy.size_order(
            NOTIONAL_USDT, price, f.get("stepSize", 0.001), f.get("minQty", 0.0),
            f.get("minNotional", 5.0), MAX_NOTIONAL_OVER)
        if qty <= 0:
            self._record_signal(symbol, direction, candle, f"saltada: {why}")
            self.log(f"Señal {direction} {symbol} ({body:+.2f}%) saltada: {why}")
            return

        pos = Position(
            trade_id=self._next_trade_id(), symbol=symbol, direction=direction,
            entry_price=price, qty=qty, notional=eff,
            tp_price=strategy.take_profit_price(price, direction, TAKE_PROFIT_PCT),
            signal_pct=body, candle_open=candle.open, candle_close=candle.close,
            candle_time=candle.open_time, last_price=price,
        )
        with self.lock:
            self.positions[symbol] = pos
            self.opened_count += 1
        self._refresh_pos_syms()
        if self.price_cache is not None:
            self.price_cache.ensure_symbols([symbol])       # bookTicker en tiempo real
        self.store.mark_dirty(True)
        self._record_signal(symbol, direction, candle, f"abierta #{pos.trade_id}")
        self.log(f"ABRE {direction} {symbol} | vela {candle.open:g}→{candle.close:g} ({body:+.2f}%) | "
                 f"px={price:g} qty={qty:g} ({eff:.2f} USDT) | TP {pos.tp_price:g} | trade #{pos.trade_id}")
        self.executor.send({
            "action":    "open",
            "trade_id":  pos.trade_id,
            "symbol":    symbol,
            "direction": direction,
            "price":     price,
            "quantity":  qty,
            "notional":  round(eff, 4),
            "level":     round(body, 4),
        })

    # ── Salidas: ticks de precio (hilo del WS de precios) ─────────────────────

    def _on_price_event(self, symbol: str, kind: str) -> None:
        if symbol not in self._pos_syms:
            return
        with self._pending_lock:
            self._pending.add(symbol)
            if self._wake_scheduled:
                return
            self._wake_scheduled = True
        loop, ev = self.loop, self._tick_event
        if loop is None or ev is None:
            with self._pending_lock:
                self._wake_scheduled = False
            return
        loop.call_soon_threadsafe(ev.set)

    async def _engine_loop(self) -> None:
        ev = self._tick_event
        while self.running:
            await ev.wait()
            ev.clear()
            with self._pending_lock:
                batch, self._pending = self._pending, set()
                self._wake_scheduled = False
            for sym in batch:
                self._check_take_profit(sym)
            await asyncio.sleep(0)

    def _price(self, symbol: str) -> Optional[float]:
        pc = self.price_cache
        if pc is None:
            return None
        return pc.get_price(symbol, max_age_s=PRICE_MAX_AGE_S)

    def _check_take_profit(self, symbol: str) -> None:
        price = self._price(symbol)
        if not price:
            return
        with self.lock:
            pos = self.positions.get(symbol)
            if pos is None:
                return
            pos.update(price)
            hit = strategy.take_profit_hit(price, pos.entry_price, pos.direction, TAKE_PROFIT_PCT)
        if hit and self.store.can_act():
            self._close(symbol, price, "TP")

    def _close(self, symbol: str, price: float, reason: str) -> bool:
        with self.lock:
            pos = self.positions.get(symbol)
            if pos is None or symbol in self._closing:
                return False
            self._closing.add(symbol)
        try:
            with self.lock:
                pos = self.positions.pop(symbol, None)
                if pos is None:
                    return False
                pos.update(price)
                pnl = strategy.pnl_usd(price, pos.entry_price, pos.qty, pos.direction)
                now = time.time()
                rec = {
                    **asdict(pos),
                    "close_price": price,
                    "closed_at_ts": now,
                    "duration_s": now - pos.opened_ts,
                    "move_pct": strategy.favorable_move_pct(price, pos.entry_price, pos.direction),
                    "pnl": pnl,
                    "reason": reason,
                }
                self.total_realized_pnl += pnl
                self.closed_trades.insert(0, rec)
                del self.closed_trades[500:]
                if COOLDOWN_SECONDS > 0:
                    self.cooldown[symbol] = now + COOLDOWN_SECONDS
                # Misma sección crítica que saca la posición: el guardado lleva ambas cosas.
                self.store.append_trade(rec)
            self._refresh_pos_syms()
            self.store.mark_dirty(True)
            self.log(f"CIERRA {pos.direction} {symbol} ({reason}) | entrada {pos.entry_price:g} → "
                     f"{price:g} ({rec['move_pct']:+.2f}%) | PnL {pnl:+.4f} USDT | trade #{pos.trade_id}")
            self.executor.send({
                "action":      "close",
                "trade_id":    pos.trade_id,
                "symbol":      symbol,
                "direction":   pos.direction,
                "reason":      reason,
                "close_price": price,
                "pnl":         round(pnl, 6),
            })
            return True
        finally:
            with self.lock:
                self._closing.discard(symbol)

    def close_manual(self, symbol: str) -> Tuple[bool, str]:
        """Cierre manual desde la web (corre en el loop del bot)."""
        with self.lock:
            pos = self.positions.get(symbol)
        if pos is None:
            return False, "No hay posición abierta en ese símbolo"
        price = self._price(symbol) or (self.price_cache.get_mark_price(symbol) if self.price_cache else None) \
            or pos.last_price or pos.entry_price
        ok = self._close(symbol, price, "MANUAL")
        return ok, "Posición cerrada" if ok else "No se pudo cerrar (ya se estaba cerrando)"

    # ── Mantenimiento ─────────────────────────────────────────────────────────

    async def _maintenance_loop(self) -> None:
        last_persist = time.time()
        while self.running:
            await asyncio.sleep(max(0.2, SAFETY_TICK_SECS))
            now = time.time()
            for sym in list(self._pos_syms):              # red de seguridad del TP
                self._check_take_profit(sym)
            if self.cooldown:
                with self.lock:
                    self.cooldown = {s: t for s, t in self.cooldown.items() if t > now}
            if now - last_persist >= STATE_PERSIST_SECS:
                last_persist = now
                self.store.mark_dirty(False)              # MFE/MAE con el latido
            if self.store.restore_pending():
                self._restore_from_store(late=True)

    # ── Recuperación tras reinicio ────────────────────────────────────────────

    def _recovery_doc(self) -> Tuple[dict, List[dict]]:
        now = time.time()
        with self.lock:
            doc = {
                "v": 2,
                "trade_id_seq": self.trade_id_seq,
                "total_realized_pnl": self.total_realized_pnl,
                "positions": {s: asdict(p) for s, p in self.positions.items()},
                "cooldowns": {s: t for s, t in self.cooldown.items() if t > now},
            }
            trades = self.store.peek_trades()
        t = self.executor.target
        if self.executor_changed_ts:
            # Solo si se cambió desde la web: así sobrevive a reinicios en Render.
            doc["executor"] = {"url": t.url, "secret": t.secret, "changed_ts": self.executor_changed_ts}
        return doc, trades

    def _restore_from_store(self, late: bool = False) -> None:
        r = self.store.take_restore()
        if r is None:
            return
        doc, trades, source = r
        doc = doc or {}
        names = {f.name for f in fields(Position)}
        positions: Dict[str, Position] = {}
        for sym, d in (doc.get("positions") or {}).items():
            try:
                pos = Position(**{k: v for k, v in d.items() if k in names})
                positions[pos.symbol] = pos
            except Exception as exc:
                self.log(f"[estado] posición {sym} ilegible: {exc}")
        ex = doc.get("executor") or {}
        try:
            changed = float(ex.get("changed_ts") or 0.0)
        except (TypeError, ValueError):
            changed = 0.0
        if ex.get("url") is not None and changed > self.executor_changed_ts:
            self.executor.set_target(ex.get("url", ""), ex.get("secret", self.executor.target.secret))
            self.executor_changed_ts = changed
            self._save_settings()
        rows = [t for t in trades if isinstance(t, dict)]
        rows.sort(key=lambda t: float(t.get("closed_at_ts") or 0), reverse=True)
        with self.lock:
            self.positions = positions
            self.cooldown = {s: float(t) for s, t in (doc.get("cooldowns") or {}).items()}
            self.total_realized_pnl = float(doc.get("total_realized_pnl") or 0.0)
            self.trade_id_seq = max(int(doc.get("trade_id_seq") or 0),
                                    *[p.trade_id for p in positions.values()], 0)
            self.closed_trades = rows[:500]
        self._refresh_pos_syms()
        if not self.store.enable_writes():
            self.log("[estado] Perdí el control mientras aplicaba el estado; sigo esperando")
            return
        if late and self.price_cache is not None:
            self.price_cache.ensure_symbols(list(positions))
        detail = ", ".join(f"{p.direction} {p.symbol} #{p.trade_id}" for p in positions.values()) or "ninguna"
        self.log(f"[estado] Recuperado desde {source}: abiertas: {detail} | "
                 f"historial: {len(rows)} cierres | executor → {self.executor.target.url or '(vacío)'}")

    def _on_fenced(self) -> None:
        self.last_error = "Otra instancia tomó el control del estado: esta queda en standby"

    # ── Vista para la web ─────────────────────────────────────────────────────

    def snapshot(self) -> dict:
        now = time.time()
        with self.lock:
            positions = []
            for p in sorted(self.positions.values(), key=lambda p: p.opened_ts, reverse=True):
                price = self._price(p.symbol) or p.last_price
                positions.append({
                    **asdict(p),
                    "price":    price,
                    "move_pct": strategy.favorable_move_pct(price, p.entry_price, p.direction),
                    "pnl":      strategy.pnl_usd(price, p.entry_price, p.qty, p.direction),
                    "age_s":    now - p.opened_ts,
                    "opened":   _utc(p.opened_ts, "%m-%d %H:%M:%S"),
                })
            closed = [{**t, "closed": _utc(t.get("closed_at_ts", 0), "%m-%d %H:%M:%S")}
                      for t in self.closed_trades[:100]]
            signals = list(self.signals)[:60]
            events = list(self.events)[:80]
        ks = self.kline_stream.get_stats() if self.kline_stream else {}
        k1 = (ks.get("intervals") or {}).get(KLINE_INTERVAL, {})
        pc = self.price_cache
        wins = sum(1 for t in self.closed_trades if t.get("pnl", 0) > 0)
        return {
            "mode":            "señales al executor",
            "running":         self.running,
            "uptime_s":        now - self.started_at,
            "config": {
                "threshold_pct": SIGNAL_THRESHOLD_PCT, "tp_pct": TAKE_PROFIT_PCT,
                "notional": NOTIONAL_USDT, "stop_loss": None, "cooldown_s": COOLDOWN_SECONDS,
                "admin_token": bool(ADMIN_TOKEN),
            },
            "executor": {
                "url": self.executor.target.url, "sent": self.executor.sent,
                "errors": self.executor.errors, "last_error": self.executor.last_error,
                "last_ok": _utc(self.executor.last_ok_ts, "%H:%M:%S") if self.executor.last_ok_ts else "",
            },
            "klines": {
                "symbols": k1.get("symbols", 0), "with_data": k1.get("with_data", 0),
                "closed_candles": k1.get("closed_candles", 0),
                "connections": ks.get("connections", 0), "active": ks.get("active_connections", 0),
                "reconnects": ks.get("reconnects", 0), "last_error": ks.get("last_error", ""),
            },
            "prices_connected": bool(pc and pc.is_connected()),
            "signal_count":    self.signal_count,
            "opened_count":    self.opened_count,
            "closed_count":    len(self.closed_trades),
            "wins":            wins,
            "realized_pnl":    self.total_realized_pnl,
            "unrealized_pnl":  sum(p["pnl"] for p in positions),
            "positions":       positions,
            "closed_trades":   closed,
            "signals":         signals,
            "events":          events,
            "last_error":      self.last_error,
            "persistence":     self.store.status(),
        }


# ─────────────────────────────────────────────────────────────────────────────
# FLASK
# ─────────────────────────────────────────────────────────────────────────────

bot = KlineBot()
bot.start()
atexit.register(bot.shutdown)

app = Flask(__name__)


def _no_store(resp):
    resp.headers["Cache-Control"] = "no-store, max-age=0"
    return resp


def _guard():
    """401 si hay ADMIN_TOKEN y no viene; 409 si esta instancia no controla el estado."""
    if ADMIN_TOKEN:
        given = request.headers.get("X-Admin-Token") or (request.get_json(silent=True) or {}).get("token")
        if given != ADMIN_TOKEN:
            return jsonify({"ok": False, "error": "Token de administrador inválido"}), 401
    if not bot.store.can_act():
        mode = bot.store.status()["mode"]
        return jsonify({"ok": False, "error": f"Esta instancia no controla el estado ({mode}). "
                                             "Reintenta en unos segundos."}), 409
    return None


@app.get("/")
def index():
    return _no_store(make_response(HTML.replace("__POLL_MS__", str(STATUS_POLL_MS))))


@app.get("/api/status")
def api_status():
    return _no_store(jsonify(bot.snapshot()))


@app.get("/api/executor")
def api_executor_get():
    t = bot.executor.target
    return _no_store(jsonify({"ok": True, "url": t.url, "has_secret": bool(t.secret)}))


@app.post("/api/executor")
def api_executor_set():
    """Cambia el link del Executor en caliente. Body JSON:
    {"url": "https://...", "secret": "opcional", "token": "si hay ADMIN_TOKEN"}.
    url vacío = dejar de mandar señales."""
    busy = _guard()
    if busy:
        return busy
    data = request.get_json(silent=True) or {}
    url = ExecutorBridge.clean_url(str(data.get("url", "")))
    if url and not url.startswith(("http://", "https://")):
        return jsonify({"ok": False, "error": "El link debe empezar por http:// o https://"}), 400
    secret = data.get("secret")
    secret = str(secret) if secret not in (None, "") else None
    target = bot.set_executor(url, secret)
    reachable, detail = bot.executor.probe(url) if url else (False, "sin URL: señales desactivadas")
    return jsonify({"ok": True, "url": target.url, "reachable": reachable, "detail": detail})


@app.post("/api/close/<symbol>")
def api_close(symbol: str):
    busy = _guard()
    if busy:
        return busy
    if not bot.loop or not bot.loop.is_running():
        return jsonify({"ok": False, "error": "El loop del bot no está activo"}), 503
    symbol = symbol.upper().strip()

    async def _run():
        return bot.close_manual(symbol)

    try:
        ok, msg = asyncio.run_coroutine_threadsafe(_run(), bot.loop).result(timeout=10)
    except concurrent.futures.TimeoutError:
        return jsonify({"ok": False, "error": "Tiempo de espera agotado"}), 504
    return jsonify({"ok": ok, "symbol": symbol, "msg": msg}), (200 if ok else 404)


@app.get("/api/trades")
def api_trades():
    with bot.lock:
        rows = list(bot.closed_trades)
    return _no_store(jsonify(rows))


@app.get("/health")
def health():
    snap = bot.snapshot()
    return jsonify({
        "ok": True, "running": bot.running, "positions": len(snap["positions"]),
        "klines": snap["klines"], "prices_connected": snap["prices_connected"],
        "executor": snap["executor"]["url"], "persistence": snap["persistence"],
        "last_error": snap["last_error"],
    })


@app.get("/api/recovery")
def api_recovery():
    doc = bot.store.last_doc or bot.store.read_local()
    if isinstance(doc, dict) and isinstance(doc.get("executor"), dict):
        doc = {**doc, "executor": {**doc["executor"], "secret": "***"}}
    return _no_store(jsonify({"status": bot.store.status(), "document": doc}))


# ─────────────────────────────────────────────────────────────────────────────
# WEB
# ─────────────────────────────────────────────────────────────────────────────

HTML = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>BOTKLINE · velas 1m</title>
<style>
  :root { --bg:#0a0f1e; --card:#121a30; --line:#223052; --txt:#e8edf8; --muted:#8b9bbd;
          --green:#34d399; --red:#f87171; --amber:#fbbf24; --blue:#60a5fa; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--txt); font:14px system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
         font-variant-numeric:tabular-nums; }
  main { max-width:1200px; margin:0 auto; padding:16px; }
  h1 { font-size:20px; margin:0 0 4px; } h2 { font-size:15px; margin:0 0 10px; }
  .muted { color:var(--muted); } .pos { color:var(--green); } .neg { color:var(--red); } .warn { color:var(--amber); }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:10px; margin:14px 0; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:12px; padding:12px; margin-bottom:14px; }
  .stat .label { color:var(--muted); font-size:12px; } .stat .v { font-size:18px; font-weight:700; margin-top:4px; word-break:break-all; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { padding:6px 8px; border-bottom:1px solid var(--line); text-align:right; white-space:nowrap; }
  th:first-child, td:first-child { text-align:left; }
  th { color:var(--muted); font-weight:600; }
  .scroll { overflow-x:auto; }
  .tag { padding:2px 8px; border-radius:999px; font-weight:700; font-size:11px; }
  .LONG { background:rgba(52,211,153,.15); color:var(--green); } .SHORT { background:rgba(248,113,113,.15); color:var(--red); }
  form { display:flex; gap:8px; flex-wrap:wrap; }
  input { flex:1 1 220px; background:#0e1630; color:var(--txt); border:1px solid var(--line); border-radius:8px; padding:8px 10px; font:inherit; }
  button { background:var(--blue); color:#06101f; border:0; border-radius:8px; padding:8px 14px; font-weight:700; cursor:pointer; }
  button.small { padding:4px 10px; font-size:12px; background:#2a3658; color:var(--txt); }
  #exMsg { margin-top:8px; min-height:18px; }
  pre { margin:0; max-height:300px; overflow:auto; font-size:12px; color:var(--muted); white-space:pre-wrap; }
</style>
</head>
<body>
<main>
  <h1>BOTKLINE <span class="muted" style="font-size:13px;font-weight:400">velas 1m · sigue la tendencia</span></h1>
  <div class="muted" id="rules"></div>

  <div class="grid">
    <div class="card stat"><div class="label">Velas 1m</div><div class="v" id="sKl">—</div></div>
    <div class="card stat"><div class="label">Señales / abiertas</div><div class="v" id="sSig">—</div></div>
    <div class="card stat"><div class="label">Posiciones abiertas</div><div class="v" id="sPos">—</div></div>
    <div class="card stat"><div class="label">PnL abierto</div><div class="v" id="sUpnl">—</div></div>
    <div class="card stat"><div class="label">PnL realizado</div><div class="v" id="sRpnl">—</div></div>
    <div class="card stat"><div class="label">Estado</div><div class="v" id="sState">—</div></div>
  </div>

  <div class="card">
    <h2>Executor</h2>
    <div class="muted">Actual: <b id="exUrl">—</b> · enviadas <span id="exSent">0</span> · errores <span id="exErr">0</span></div>
    <form id="exForm" style="margin-top:10px">
      <input id="exNew" type="url" placeholder="https://mi-executor.onrender.com" autocomplete="off">
      <input id="exSecret" type="password" placeholder="Secreto (opcional, vacío = mantener)" autocomplete="off">
      <input id="exToken" type="password" placeholder="Token admin" autocomplete="off" style="display:none">
      <button type="submit">Cambiar link</button>
    </form>
    <div id="exMsg" class="muted"></div>
  </div>

  <div class="card"><h2>Posiciones abiertas</h2><div class="scroll"><table>
    <thead><tr><th>Símbolo</th><th>Dir.</th><th>Vela</th><th>Entrada</th><th>Precio</th><th>Avance</th><th>TP</th><th>Notional</th><th>PnL</th><th>Abierta</th><th></th></tr></thead>
    <tbody id="tPos"></tbody></table></div></div>

  <div class="card"><h2>Señales recientes</h2><div class="scroll"><table>
    <thead><tr><th>Hora</th><th>Símbolo</th><th>Dir.</th><th>Vela</th><th>Cuerpo</th><th>Acción</th></tr></thead>
    <tbody id="tSig"></tbody></table></div></div>

  <div class="card"><h2>Operaciones cerradas</h2><div class="scroll"><table>
    <thead><tr><th>Cierre</th><th>Símbolo</th><th>Dir.</th><th>Entrada</th><th>Salida</th><th>Avance</th><th>PnL</th><th>Duración</th><th>Motivo</th></tr></thead>
    <tbody id="tClosed"></tbody></table></div></div>

  <div class="card"><h2>Eventos</h2><pre id="events"></pre></div>
</main>
<script>
const q = id => document.getElementById(id);
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const num = (v, d=4) => (v === null || v === undefined || isNaN(v)) ? '—' : (+v).toLocaleString('es', {maximumFractionDigits:d});
const pct = v => `<span class="${v >= 0 ? 'pos' : 'neg'}">${v >= 0 ? '+' : ''}${(+v).toFixed(2)}%</span>`;
const usd = v => `<span class="${v >= 0 ? 'pos' : 'neg'}">${v >= 0 ? '+' : ''}${(+v).toFixed(4)}</span>`;
const dur = s => { s = Math.floor(s); const h = Math.floor(s/3600), m = Math.floor(s%3600/60);
                   return h ? `${h}h ${m}m` : m ? `${m}m ${s%60}s` : `${s}s`; };
const tag = d => `<span class="tag ${esc(d)}">${esc(d)}</span>`;
let token = '';
try { token = localStorage.getItem('botkline_token') || ''; } catch (e) {}
q('exToken').value = token;

function headers() {
  const h = {'Content-Type': 'application/json'};
  const t = q('exToken').value.trim();
  if (t) h['X-Admin-Token'] = t;
  return h;
}

async function refresh() {
  let d;
  try { d = await (await fetch('/api/status', {cache: 'no-store'})).json(); } catch (e) { return; }
  const c = d.config;
  q('rules').textContent = `Cuerpo de la última vela ≥ ${c.threshold_pct}% → sigue la tendencia · TP ${c.tp_pct}% · ${c.notional} USDT · sin stop loss`;
  q('exToken').style.display = c.admin_token ? '' : 'none';
  const k = d.klines;
  q('sKl').innerHTML = `${k.with_data}/${k.symbols} <span class="muted" style="font-size:12px">${k.active}/${k.connections} conex.</span>`;
  q('sSig').textContent = `${d.signal_count} / ${d.opened_count}`;
  q('sPos').textContent = d.positions.length;
  q('sUpnl').innerHTML = usd(d.unrealized_pnl);
  q('sRpnl').innerHTML = `${usd(d.realized_pnl)} <span class="muted" style="font-size:12px">${d.wins}/${d.closed_count} gan.</span>`;
  const p = d.persistence || {};
  q('sState').innerHTML = `<span class="${p.mode === 'ok' || p.mode === 'local' ? 'pos' : 'warn'}">${esc(p.mode)}</span>`;
  q('exUrl').textContent = d.executor.url || '(vacío: sin señales)';
  q('exSent').textContent = d.executor.sent;
  q('exErr').textContent = d.executor.errors;

  q('tPos').innerHTML = d.positions.map(r => `<tr>
    <td><b>${esc(r.symbol)}</b> <span class="muted">#${r.trade_id}</span></td><td>${tag(r.direction)}</td>
    <td>${pct(r.signal_pct)}</td><td>${num(r.entry_price, 8)}</td><td>${num(r.price, 8)}</td>
    <td>${pct(r.move_pct)}</td><td>${num(r.tp_price, 8)}</td><td>${num(r.notional, 2)}</td><td>${usd(r.pnl)}</td>
    <td>${esc(r.opened)} <span class="muted">(${dur(r.age_s)})</span></td>
    <td><button class="small" data-close="${esc(r.symbol)}">Cerrar</button></td></tr>`).join('')
    || '<tr><td colspan="11" class="muted">Sin posiciones abiertas</td></tr>';

  q('tSig').innerHTML = d.signals.map(s => `<tr><td>${esc(s.time)}</td><td>${esc(s.symbol)}</td><td>${tag(s.direction)}</td>
    <td>${esc(s.candle)} <span class="muted">${num(s.open, 8)}→${num(s.close, 8)}</span></td><td>${pct(s.body_pct)}</td>
    <td class="${s.action.startsWith('abierta') ? 'pos' : 'muted'}">${esc(s.action)}</td></tr>`).join('')
    || '<tr><td colspan="6" class="muted">Esperando velas con cuerpo ≥ umbral</td></tr>';

  q('tClosed').innerHTML = d.closed_trades.map(t => `<tr><td>${esc(t.closed)}</td><td>${esc(t.symbol)}</td><td>${tag(t.direction)}</td>
    <td>${num(t.entry_price, 8)}</td><td>${num(t.close_price, 8)}</td><td>${pct(t.move_pct)}</td><td>${usd(t.pnl)}</td>
    <td>${dur(t.duration_s || 0)}</td><td>${esc(t.reason)}</td></tr>`).join('')
    || '<tr><td colspan="9" class="muted">Aún no hay cierres</td></tr>';

  q('events').textContent = d.events.join('\n');
}

q('exForm').addEventListener('submit', async ev => {
  ev.preventDefault();
  const body = {url: q('exNew').value.trim()};
  const secret = q('exSecret').value;
  if (secret) body.secret = secret;
  try { localStorage.setItem('botkline_token', q('exToken').value.trim()); } catch (e) {}
  if (!body.url && !confirm('Link vacío: el bot dejará de mandar señales. ¿Seguir?')) return;
  q('exMsg').textContent = 'Guardando…';
  try {
    const r = await (await fetch('/api/executor', {method: 'POST', headers: headers(), body: JSON.stringify(body)})).json();
    q('exMsg').innerHTML = r.ok
      ? `<span class="pos">Link cambiado.</span> ${r.url ? (r.reachable ? 'El executor responde (' + esc(r.detail) + ').' : '<span class="warn">No responde: ' + esc(r.detail) + '</span>') : esc(r.detail)}`
      : `<span class="neg">${esc(r.error)}</span>`;
    if (r.ok) { q('exNew').value = ''; q('exSecret').value = ''; }
  } catch (e) { q('exMsg').innerHTML = `<span class="neg">${esc(e)}</span>`; }
  refresh();
});

document.addEventListener('click', async ev => {
  const sym = ev.target.dataset && ev.target.dataset.close;
  if (!sym || !confirm(`¿Cerrar ${sym} a mercado?`)) return;
  try {
    const r = await (await fetch('/api/close/' + encodeURIComponent(sym), {method: 'POST', headers: headers(), body: '{}'})).json();
    alert(r.ok ? r.msg : (r.error || r.msg));
  } catch (e) { alert(e); }
  refresh();
});

refresh();
setInterval(refresh, __POLL_MS__);
</script>
</body>
</html>"""


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8000")), threaded=True)
