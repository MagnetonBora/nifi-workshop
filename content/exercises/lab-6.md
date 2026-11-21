---
title: "Lab 6 — Доставка событий в Splunk"
---

> Из материалов [[materials/day-2|Day 2 — Data Processing and Integrations]]. Стенд и загрузка эталонов — на странице [[exercises/index|Exercises]].

**Цель.** Отправить NDJSON-события из Lab 5 в Splunk либо в эмулированный endpoint и измерить throughput.

**Архитектура.**

```text
...► MergeRecord (NDJSON) ► PutSplunk (TCP) ► [Splunk TCP input / netcat-эмулятор]
                       └─► (альтернатива) InvokeHTTP ► [HEC / http-эмулятор]
```

**Эмуляция.** На стенде эмулятор Splunk уже запущен (`make lab-services`, контейнер `splunk-sim`, код — `lab-services/splunk_sim.py`): TCP input на `splunk-sim:5514` пишет события в `/data/splunk_in.log`, HEC на `http://splunk-sim:8088/services/collector/event` — в `/data/hec_in.log` и отвечает как Splunk (`{"text":"Success","code":0}`). Без стенда тот же эффект даёт:

```bash
# Эмуляция TCP-input Splunk
nc -lk 5514 > /tmp/splunk_in.log
# Либо на Python: простой приёмник HTTP для HEC
python3 - <<'EOF'
from http.server import BaseHTTPRequestHandler, HTTPServer
class H(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get('Content-Length', 0)); body = self.rfile.read(n)
        open('/tmp/hec_in.log','ab').write(body + b'\n')
        self.send_response(200); self.end_headers(); self.wfile.write(b'{"text":"Success","code":0}')
HTTPServer(('0.0.0.0', 8088), H).serve_forever()
EOF
```

**Источник событий.** `GenerateRecord` (схема лог-события, 100 записей на FlowFile, `Run Schedule 1 sec`) ► `MergeRecord` (Min/Max Records `10000`, Max Bin Age `10 sec`). Удобно собрать две группы: A — `PutSplunk` (TCP), B — `InvokeHTTP` (HEC) — и запускать их по очереди.

**Конфигурация `PutSplunk`:** Protocol `TCP`; Hostname `#{splunk.host}` (`splunk-sim`); Port `#{splunk.port}` (`5514`); Message Delimiter `\n`; Concurrent Tasks `1` (затем эксперимент с `2`, `4`). Свойства `Connection Per FlowFile` в NiFi 2.12 нет (см. 2.3.2).

**Конфигурация `InvokeHTTP` (HEC):** HTTP Method `POST`; HTTP URL `#{hec.url}`; Request Content-Type `application/json`; динамическое свойство `Authorization` = `Splunk #{hec.token}` — динамические свойства `InvokeHTTP` становятся заголовками запроса.

> **Формат тела HEC.** Эндпоинт `/services/collector/event` настоящего Splunk принимает события только в конверте `{"time": …, "sourcetype": …, "event": {…}}` (см. 2.3.4), а «голый» NDJSON отклоняет. Эмулятор стенда принимает любое тело, поэтому лабораторная работает и без конверта. Для реального HEC оберните записи перед `InvokeHTTP`, например `JoltTransformRecord` со спецификацией `[{"operation":"shift","spec":{"*":"event.&"}},{"operation":"default","spec":{"sourcetype":"app:log","index":"main"}}]` (**[проверить]** на вашей версии) — либо отправляйте на `/services/collector/raw`.

**Эксперимент.**

1. Подготовьте 1 млн событий (многократный `GenerateRecord`/`GenerateFlowFile`). На ноутбуке начните с ~100 000 и увеличивайте, следя за heap.
2. Вариант 1: мелкие FlowFile по 100 событий. Вариант 2: пачки по 10 000 через `MergeRecord`. Вариант 3: `Concurrent Tasks=4`.
3. Измеряйте события/с по счётчикам очереди и времени (на стенде: `make lab-splunk-count`).

**Проверка.** `make lab-splunk-count` (или `wc -l /tmp/splunk_in.log` без стенда) равно числу отправленных событий — например, 21 FlowFile по 100 событий дают ровно 2100 строк и по TCP, и по HEC; в provenance события `SEND` показывают адрес приёмника. Перед новым замером обнулите файлы эмулятора: `docker exec splunk-sim sh -c ': > /data/splunk_in.log; : > /data/hec_in.log'`.

**Типичные ошибки.** `PutSplunk` в `failure` — приёмник недоступен/закрыл соединение; всё в одну строку — нет разделителя между событиями; Splunk не разбирает время — формат/порядок полей не соответствует настройкам sourcetype; HEC отвечает `400 Invalid data format` — событие не завёрнуто в конверт `{"event": …}`.

**Эталонное решение:** [lab6-splunk-delivery.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab6-splunk-delivery.json) (без конверта HEC — рассчитано на эмулятор).
