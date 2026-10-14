# BOTKLINE · velas 1m que siguen la tendencia (Binance USDⓈ-M Futures)

Aplicación web para Render que vigila las velas de 1 minuto de **todos** los perpetuos USDT por WebSocket y manda señales de apertura y cierre al **Executor** externo, que es quien abre las órdenes en Binance.

## Estrategia

1. **Velas por WebSocket** (`kline_ws.py`): una conexión con `<símbolo>@kline_1m` de todos los perpetuos. Se guardan las dos últimas velas cerradas de cada símbolo. Sin REST.
2. **Señal**: al cerrar cada vela se mide su cuerpo, `(cierre − apertura) / apertura`.
   - Sube **≥ 3 %** → abre **LONG**.
   - Baja **≥ 3 %** → abre **SHORT**.
3. **Tamaño**: notional fijo de **5 USDT**. La cantidad se redondea hacia arriba al `stepSize` del par para no quedar por debajo del mínimo de Binance. Los pares que exigen más de 5 USDT, o cuyo `stepSize` obliga a más de 1,5× el notional, se saltan y quedan anotados en la tabla de señales.
4. **Una posición por símbolo**: mientras esté abierta, las señales nuevas de ese símbolo se ignoran.
5. **Salida**: se cierra cuando el precio avanza **2 % o más** a favor desde la entrada (precio en tiempo real de `WS.py`: bookTicker del símbolo + `!markPrice@arr`). **No hay stop loss**; solo cierre manual desde la web.

Las cifras se configuran con variables de entorno (tabla abajo). El **% de la señal** y el **% del TP** también se cambian en caliente desde el bloque *Parámetros* de la web o con `POST /api/config` (`{"threshold_pct": 3, "tp_pct": 2}`). El umbral aplica desde la siguiente vela; el TP nuevo también se aplica a las posiciones abiertas. Los valores se guardan en `SETTINGS_FILE`.

### Bloquear longs o shorts

En el bloque *Parámetros* hay un botón por dirección (o `POST /api/block` con `{"direction": "LONG", "blocked": true}`). Una dirección bloqueada no abre entradas nuevas; sus señales aparecen como *bloqueada* en la tabla de señales. Las posiciones ya abiertas siguen hasta su TP o cierre manual. El estado se guarda en `SETTINGS_FILE`.

### MFE / MAE

Cada posición registra su máximo a favor (**MFE**) y su máximo en contra (**MAE**), en % y en USDT, con el momento en que se marcaron y el precio máximo y mínimo vistos. La web los muestra en las posiciones abiertas y en las cerradas, más un resumen (medias, mejor MFE, peor MAE, tiempo hasta el MAE) total y por dirección. `GET /api/trades.csv` descarga el historial completo con estas columnas.

### Evaluaciones

La web muestra todas las velas evaluadas, no solo las que generan señal:
- **Mayores cuerpos** de la última vela 1m cerrada (los `EVAL_TOP_N` primeros, por defecto 15), con la señal que daría con el umbral actual.
- **Resumen por minuto** (últimos `EVAL_MINUTES_KEPT`, por defecto 30): velas evaluadas, cuántas pasaron el umbral en cada dirección y la mayor subida y caída.

## Executor

Cada apertura y cierre se manda con `POST <EXECUTOR_URL>/signal` y la cabecera `X-Signal-Secret`, con el mismo formato de antes:

```json
{"action": "open",  "trade_id": 1791908572, "symbol": "FOOUSDT", "direction": "LONG",
 "price": 1.04, "quantity": 5, "notional": 5.2, "level": 4.0}
{"action": "close", "trade_id": 1791908572, "symbol": "FOOUSDT", "direction": "LONG",
 "reason": "TP", "close_price": 1.0609, "pnl": 0.1045}
```

`direction` ahora puede ser `LONG` o `SHORT`, y `level` lleva el % del cuerpo de la vela que disparó la señal.

### Latencia

Las señales salen por conexiones HTTP persistentes (keep-alive): `EXECUTOR_WORKERS` hilos (4 por defecto) mantienen abierta su conexión con el executor y la refrescan con un `GET /api/state` cada `EXECUTOR_WARM_S` segundos (20) si no hay señales. Así cada señal paga solo el viaje de red, sin abrir TCP+TLS de nuevo. La entrada usa el cierre de la vela como precio. La web muestra, para la última señal, los ms de decisión, los ms del HTTP al executor y el total desde que llegó el cierre de Binance.

### Cambiar el link en caliente

- **Web**: bloque *Executor* → pegar el link nuevo (y el secreto si cambia) → *Cambiar link*. La web avisa si el executor nuevo responde.
- **API**:

  ```bash
  curl -X POST https://<bot>/api/executor -H 'Content-Type: application/json' \
       -d '{"url": "https://nuevo-executor.onrender.com", "secret": "opcional"}'
  ```

  `url` vacío deja de mandar señales. Si `ADMIN_TOKEN` está definido, añade `-H 'X-Admin-Token: <token>'`.

El cambio aplica a la siguiente señal, sin reiniciar, y se guarda en disco local (`SETTINGS_FILE`). En Render free ese disco se borra al reiniciar, así que tras un redeploy vuelve a `EXECUTOR_URL`. Los cierres de posiciones ya abiertas van al link vigente en el momento del cierre.

## Variables de entorno

| Variable | Default | Descripción |
| --- | --- | --- |
| `SIGNAL_THRESHOLD_PCT` | `3` | Cuerpo mínimo de la vela 1m para abrir. |
| `TAKE_PROFIT_PCT` | `2` | Avance a favor para cerrar. |
| `NOTIONAL_USDT` | `5` | USDT por entrada. |
| `MAX_NOTIONAL_OVER` | `1.5` | Si el redondeo al `stepSize` supera notional × esto, se salta el par. |
| `COOLDOWN_SECONDS` | `0` | Pausa tras un cierre antes de volver a operar el símbolo. |
| `SIGNAL_MAX_DELAY_S` | `10` | Un cierre de vela que llega más tarde que esto no genera señal. |
| `PRICE_MAX_AGE_S` | `5` | Antigüedad máxima del precio usado para el TP. |
| `EXECUTOR_URL` | `https://executor-5lu0.onrender.com` | Link inicial del Executor (se puede cambiar desde la web). |
| `EXECUTOR_SECRET` | `clave-secreta-aleatoria` | Valor de `X-Signal-Secret`. |
| `ADMIN_TOKEN` | vacío | Si se define, cambiar el executor y cerrar desde la web lo exige. |
| `KLINE_HISTORY` | `2` | Velas cerradas guardadas por símbolo. |
| `REST_PROXY_URL` | — | Proxy para la única petición REST (`exchangeInfo`, al arrancar). |
| `SYMBOL_REFRESH_HOURS` | `12` | Antigüedad máxima del caché de `exchangeInfo` en disco. |
| `RECOVERY_FILE` | `/tmp/botkline_recovery.json` | Copia local de posiciones abiertas e historial. Upstash está desactivado por ahora: en Render free las posiciones abiertas se pierden al reiniciar. |

## Endpoints

| Ruta | Uso |
| --- | --- |
| `GET /` | Panel: posiciones, señales recientes, cierres, eventos y link del executor. |
| `GET /api/status` | Estado completo en JSON. |
| `GET/POST /api/executor` | Ver o cambiar el link del Executor. |
| `GET/POST /api/config` | Ver o cambiar el % de la señal y del TP. |
| `POST /api/close/<SÍMBOLO>` | Cierre manual. |
| `GET /api/trades` | Historial de cierres. |
| `GET /api/trades.csv` | Historial con MFE/MAE en CSV. |
| `POST /api/block` | Bloquear o permitir LONG o SHORT. |
| `GET /health`, `GET /api/recovery` | Diagnóstico. |

## Ejecutar local

```bash
pip install -r requirements.txt
python app.py          # http://localhost:8000
python -m pytest tests
```

## Deploy en Render

`render.yaml` define el servicio y las variables. El código de la estrategia está en `strategy.py` (funciones puras, con pruebas en `tests/test_strategy.py`) y el bot en `app.py`.
