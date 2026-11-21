---
title: "Lab 7 — Retry и обработка ошибок"
aliases:
  - exercises/lab-7
---

> Из материалов [[materials/day-3|Day 3 — Production, Security, and Performance]]. Стенд и загрузка эталонов — на странице [[labs/index|Labs]].

**Цель.** Построить flow, который переживает временные сбои приёмника и не блокируется «плохими» данными.

**Архитектура.**

```text
GenerateRecord ► ValidateRecord ─ invalid ─► [DLQ group]
                     │ valid
                     ▼
              InvokeHTTP (endpoint с имитацией сбоев)
                 ├ Original (2xx) ► PutFile #{ok.dir}
                 ├ Response ► LogAttribute
                 ├ Retry ──► RetryFlowFile ─ retry ► (penalize) ► InvokeHTTP
                 │                 └ retries_exceeded ► [DLQ group]
                 ├ No Retry ───────────────────────────────► [DLQ group]
                 └ Failure ► RetryFlowFile (тот же)
[DLQ group]: UpdateAttribute (error.*) ► PutFile #{dlq.dir}/${error.stage} (с большим порогом back pressure)
```

`Original` — исходный FlowFile запроса после ответа 2xx (именно его логично складывать в `ok`); `Response` — тело ответа (здесь `{}`).

Parameter Context `lab7-params` (стенд): `flaky.url=http://flaky:9099/events`, `ok.dir=/opt/nifi/data/ok`, `dlq.dir=/opt/nifi/data/dlq` (на хосте — `nifi/data/ok`, `nifi/data/dlq`).

**Источник данных с невалидными записями.** `GenerateRecord` выдаёт записи строго по своей схеме, поэтому без подготовки ветка `invalid` пуста. Объявите в схеме генератора поля как `["null", …]` и задайте `Nullable Fields = true`, `Null Value Percentage = 5`, а в `ValidateRecord` (Schema Access Strategy `Use 'Schema Text' Property`) проверяйте по строгой схеме, где все поля обязательны: записи с `null` уйдут в `invalid`.

**Имитация нестабильного API.** На стенде сервис уже запущен: контейнер `flaky` (`make lab-services`, код — `lab-services/flaky.py`), адрес из NiFi `http://flaky:9099/events`. Без стенда:

```bash
python3 - <<'EOF'
import random
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers.get('Content-Length', 0)))
        r = random.random()
        code = 200 if r < 0.6 else (503 if r < 0.9 else 400)   # 60% ok, 30% 503, 10% 400
        self.send_response(code); self.end_headers(); self.wfile.write(b'{}')
HTTPServer(('0.0.0.0', 9099), H).serve_forever()
EOF
```

**Конфигурация.**

| Компонент | Настройка |
|---|---|
| `GenerateRecord` | Schema Text — схема с полями `["null", …]`; Number of Records `5`; Nullable Fields `true`; Null Value Percentage `5`; Run Schedule `2 sec` |
| `ValidateRecord` | Schema Access Strategy `Use 'Schema Text' Property` — строгая схема (все поля обязательны); Strict Type Checking `true`; Validation Details Attribute Name `validation.details` |
| `InvokeHTTP` | URL `#{flaky.url}`; POST; Connection/Read Timeout — по `5 sec`; Concurrent Tasks `2` |
| `RetryFlowFile` | Maximum Retries `3`; Penalize Retries `true`; Retry Attribute — по умолчанию; динамическое свойство `error.stage` = `retries_exceeded`. Значение штрафа — *Penalty Duration* на вкладке Settings (например, `10 sec`) |
| Connection `...► DLQ` | Back Pressure Object Threshold `100000`, Size `10 GB` |
| `UpdateAttribute` (перед DLQ) | `error.stage` = `validation` / `http-${invokehttp.status.code}` / `retries-exceeded` (по пути ошибки); `filename` = `${uuid}.json` |
| `PutFile` (DLQ) | Directory `#{dlq.dir}/${error.stage}`; Conflict Resolution `replace` (имена по `${uuid}`) |

**Проверка** (с хоста стенда):

- Через несколько минут большая часть FlowFile в `nifi/data/ok` (`ls nifi/data/ok | wc -l`); `400`-ответы — в `nifi/data/dlq/http-400`, невалидные записи — в `nifi/data/dlq/validation`; ответы `503` проходят через `RetryFlowFile` (в очереди `InvokeHTTP → RetryFlowFile` видны FlowFile со штрафом).
- `retries_exceeded` при работающем `flaky` почти не встречается: нужно четыре ответа `503` подряд (~1 %). Остановите сервис — `docker stop flaky`: запросы уходят в `Failure`, после 3 повторов со штрафом появляется `nifi/data/dlq/retries-exceeded`, очередь перед `InvokeHTTP` растёт, затем срабатывает back pressure; после `docker start flaky` очередь рассасывается *без вмешательства*.
- Проверьте провенанс: FlowFile с несколькими попытками имеет цепочку событий `ROUTE`/`SEND`; атрибут `flowfile.retries` показывает число попыток.

**Ошибки и диагностика.** Петля без счётчика (retry → тот же процессор, без `RetryFlowFile`) — бесконечные повторы; очередь `Failure` не пуста и блокирует источник — её нужно вывести в сток; DLQ-директория заполняет диск — добавить мониторинг (`MonitorDiskUsage`) и очистку; ветка `invalid` всегда пуста — генератор выдаёт только валидные по своей схеме записи (см. «Источник данных с невалидными записями»).

**Эталонное решение:** [lab7-retry-errors.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab7-retry-errors.json).
