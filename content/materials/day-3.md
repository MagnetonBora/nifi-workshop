---
title: "Day 3 — Production, Security, and Performance"
---

> Документ 3 из 3. Связанные документы: **Day 1** (`day-1.md`), **Day 2** (`day-2.md`). Обозначения ([2.x], [1.x], [dep], [rem], [проверить]) и принципы достоверности — в начале Day 1.

## Содержание

- [Block 3.1: Fault tolerance and error handling](#block-31-fault-tolerance-and-error-handling)
  - [3.1.4 Эффективный перезапуск flow и минимизация ручного вмешательства](#314-эффективный-перезапуск-flow-и-минимизация-ручного-вмешательства) (расширено)
- [Block 3.2: Monitoring, provenance and troubleshooting](#block-32-monitoring-provenance-and-troubleshooting)
  - [Site-to-Site Reporting Tasks: подробно](#site-to-site-reporting-tasks-подробно) (расширено)
  - [3.2.6 NiFi Settings: назначение вкладок](#326-nifi-settings-назначение-вкладок-general-management-controller-services-reporting-tasks-flow-analysis-rules-registry-clients-parameter-providers)
- [Block 3.3: Security and governance](#block-33-security-and-governance)
- [Block 3.4: Performance tuning, hardware and storage](#block-34-performance-tuning-hardware-and-storage)
- [Block 3.5: Final hands-on lab: end-to-end pipeline](#block-35-final-hands-on-lab-end-to-end-pipeline)
- [Block 3.6: Course recap and next steps](#block-36-course-recap-and-next-steps)
- [Приложение A: Лабораторный стенд (Docker Compose)](#приложение-a-лабораторный-стенд-docker-compose)
- [Приложение B: Список источников и статус верификации](#приложение-b-список-источников-и-статус-верификации)

---

## Block 3.1: Fault tolerance and error handling

### 3.1.1 Проектирование устойчивых flow с минимальными узкими местами

**Принципы.**

1. **Каждая relationship обработана.** Для каждого процессора решите заранее: `success` — дальше; `failure`/`invalid`/`retry` — в отдельный путь; auto-terminate только осознанно (для `LogAttribute.success` и т. п.).
2. **Разделяйте типы ошибок.** Временные (сеть, перегрузка приёмника: *retry*) и постоянные (данные некорректны: *dead-letter*). Повтор постоянной ошибки бессмысленен и создаёт петли.
3. **Изолируйте точки сбоя.** Один медленный/нестабильный приёмник не должен останавливать остальные потоки: разделяйте приёмники по Process Group и очередям, используйте независимые очереди и back pressure для каждого.
4. **Идемпотентность.** NiFi даёт at-least-once внутри и на входе/выходе; приёмники должны выдерживать повторы (UPSERT, ключ события, дедупликация).
5. **Короткие транзакции.** Крупные FlowFile лучше разбивать на разумные пачки (десятки–сотни МБ), чтобы retry не перерабатывал гигабайты.
6. **Наблюдаемость на ошибках.** Добавляйте в атрибуты причину ошибки и контекст (например, `error.stage`, `error.message`, `retry.count`), чтобы разбор DLQ был возможен без повторного прогона.

### 3.1.2 Failure и retry: relationships, framework-level retry, RetryFlowFile

**Framework-level retry (с 1.16.0).** Любую relationship процессора можно отметить как *Retry*: настройка в диалоге процессора, вкладка *Relationships*. Параметры: число попыток (*Number of Retry Attempts*), **Backoff Policy** (`Penalize` — штраф к FlowFile; `Yield` — пауза процессора), **Maximum Backoff Period**. Пока попытки не исчерпаны, FlowFile повторно обрабатывается тем же процессором, а после — уходит по relationship как обычно. Преимущество: нет петель в flow; недостаток — локальное решение на одном процессоре, без общего контекста.

**`RetryFlowFile`.** Процессор для явного управления повторами в flow ([описание компонента `nifi-standard-nar` в зеркале документации Snowflake Openflow](https://docs.snowflake.com/en/user-guide/data-integration/openflow/processors/retryflowfile); поведение счётчика и relationships подтверждено также реализацией в [MiNiFi C++](https://apache.googlesource.com/nifi-minifi-cpp/+/656fca6f2331fe43915ebf4c83a56265fcb4790d/extensions/standard-processors/processors/RetryFlowFile.cpp); на страницу компонента на nifi.apache.org для вашей версии сверьтесь отдельно). Он проверяет значение **счётчика попыток** (хранится в атрибуте FlowFile — *Retry Attribute*) относительно **Maximum Retries**: пока лимит не превышен, увеличивает счётчик и отправляет FlowFile в `retry`; когда превышен — в `retries_exceeded`.

```text
 PutDatabaseRecord ─ retry ──► RetryFlowFile ─ retry ──► (penalize ► обратно на PutDatabaseRecord)
        │                            │
        │                            └ retries_exceeded ► [Dead-letter]   (НЕ возвращать на вход!)
        └ failure ──────────────────────────────────────► [Dead-letter]
```

| Свойство | Назначение |
|---|---|
| **Retry Attribute** | Имя атрибута-счётчика (по умолчанию `flowfile.retries` — **[проверить]**). Обновляется текущим числом попыток |
| **Maximum Retries** | Сколько раз FlowFile можно повторить до `retries_exceeded` (по умолчанию `3` — **[проверить]**); значение может содержать Expression Language |
| **Penalize Retries** | Если `true`, FlowFile штрафуется перед отправкой в `retry` (не относится к `retries_exceeded`) |
| **Fail on Non-numerical Overwrite** | Если в *Retry Attribute* уже лежит не число: `false` — значение сбрасывается в `1`; `true` — FlowFile уходит в `failure` |
| Динамические свойства | Любые атрибуты, которые нужно **добавить к FlowFile в `retries_exceeded`** (поддерживают EL), например `error.stage = retries_exceeded` |

**Relationships.**

| Relationship | Что значит | Куда соединять |
|---|---|---|
| `retry` | Лимит не исчерпан | **Обратно на вход** процессора, который дал сбой: так создаётся ограниченная петля |
| `retries_exceeded` | Лимит исчерпан | **В DLQ/обработку ошибок**, не возвращать на вход, иначе петля станет бесконечной |
| `failure` | Счётчик не числовой и включён *Fail on Non-numerical Overwrite* (либо *Maximum Retries* не вычисляется в целое) | В обработку ошибок |

**Дополнительные атрибуты:** кроме счётчика, процессор пишет `<Retry Attribute>.uuid` — идентификатор процессора, который последним повторял FlowFile.

**Подводные камни.**

1. **Счётчик не обнуляется сам.** Если FlowFile успешно прошёл повтор и дальше попал в другой `RetryFlowFile` с тем же *Retry Attribute*, старое значение продолжит накапливаться. Удаляйте атрибут после успеха (`UpdateAttribute` → *Delete Attributes Expression*) или задавайте разные *Retry Attribute* для разных точек повтора.
2. **Penalty** (по умолчанию `30 sec`) защищает приёмник от «шторма» повторов: пока FlowFile оштрафован, процессор его не берёт. Если **все** FlowFile в очереди оштрафованы, UI показывает это, и процессор не крутится вхолостую (улучшение 1.16.0).
3. Не используйте `RetryFlowFile` для **ошибок данных** (`invalid`, `4xx`): повтор ничего не изменит.

**Что выбрать: framework retry или `RetryFlowFile`.**

| | Framework-level retry | `RetryFlowFile` |
|---|---|---|
| Где настраивается | В диалоге процессора, вкладка *Relationships* | Отдельный процессор в flow |
| Лишние компоненты и петли | Нет | Да (петля `retry`) |
| Backoff | `Penalize`/`Yield`, **Maximum Backoff Period** (нарастающая пауза) | Штраф фиксирован (Penalty Duration) |
| Действия при исчерпании | FlowFile уходит по relationship как обычно | Отдельная relationship `retries_exceeded` с **дополнительными атрибутами** (динамические свойства) |
| Контекст | Локальный, один процессор | Единый счётчик, видимый в атрибутах; можно использовать общий для нескольких шагов |

Практическое правило: **framework retry — по умолчанию**; `RetryFlowFile` — когда нужен счётчик в атрибутах, общий лимит на цепочку шагов или явное обогащение FlowFile при исчерпании попыток.

**Экспоненциальная пауза.** Для нарастающих задержек используйте framework retry с Backoff Policy и `Maximum Backoff Period`, либо `RetryFlowFile` + `UpdateAttribute` с вычислением штрафа.

**Когда не использовать retry.** Ошибки данных (нарушение схемы, `4xx` от API, `invalid`) — прямо в DLQ. Ошибки аутентификации — после исчерпания попыток сигнализировать оператору (bulletin/alert).

### 3.1.3 Сохранение работоспособности основного потока при заполнении failure-очередей

**Проблема.** Как указано в Block 1.2, процессор перестаёт планироваться, если **любая** его исходящая связь заполнена до порога back pressure. Поэтому очередь `failure`, в которую попадает «мусор» (например, из-за несоответствия схеме), со временем заполнится и **остановит весь основной поток**, хотя `success` работает.

**Решения (комбинируйте).**

| Приём | Суть | Риск/компромисс |
|---|---|---|
| **Сток с большой ёмкостью** | Соединить `failure` с процессором, который немедленно вытесняет данные на диск/в хранилище (`PutFile`, `PutS3Object`, `PublishKafka` в DLQ-топик) — очередь не копится | Нужно мониторить место на диске/в DLQ |
| **Dead-letter топик/хранилище** | Централизованный DLQ с метаданными ошибки; повторная обработка по требованию | Дополнительная инфраструктура |
| **Увеличенные пороги** на failure-связи | Выше порог back pressure (например, 100 000 / 10 GB) для запаса времени | Это отсрочка, а не решение |
| **FlowFile Expiration** на failure-связи | Автоудаление через N часов | **Потеря данных**: допустимо только для некритичных/воспроизводимых данных |
| **Раннее разделение** | `ValidateRecord`/`QueryRecord` отделяют невалидные записи *до* основной цепочки; плохие данные не мешают хорошим | Более сложная схема, но самая устойчивая |
| **Алерты по размеру failure-очереди** | Мониторинг queued count/size | Требует настроенной системы мониторинга |
| **Разные Process Group для ветвей** | Failure-путь изолирован и имеет собственные ресурсы | |

**Паттерн «Dead Letter» (типовая реализация).**

```text
...► ValidateRecord ─ valid ─► основной поток
          │ invalid / failure
          ▼
   UpdateAttribute:
      error.stage   = validate
      error.time    = ${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")}
      error.reason  = ${validation.details:replaceNull('see bulletin')}   // атрибут заполняет ValidateRecord, если в нём задано Validation Details Attribute Name = validation.details
      original.name = ${filename}
          ▼
   PublishKafka (topic: dlq.<flow>)   или   PutFile /data/dlq/${error.stage}/${now():format('yyyyMMdd')}/${uuid}
```

Хорошие практики DLQ: сохранять **исходный контент без изменений** + атрибуты ошибки (в заголовках Kafka или в сопроводительном JSON); ограничить срок хранения; завести процедуру *replay* (отдельный flow, читающий DLQ и подающий данные обратно после исправления причины); завести счётчики (`UpdateCounter`) и алерты на рост DLQ.

### 3.1.4 Эффективный перезапуск flow и минимизация ручного вмешательства

**Цель подраздела.** Научиться (1) останавливать и запускать NiFi и отдельные flow предсказуемо и без потери данных, (2) сократить время простоя при перезапуске, (3) автоматизировать типовые действия оператора и (4) строить flow так, чтобы большинство сбоев исчезало само, без вызова человека.

Связанные блоки: back pressure и очереди — Block 1.2; кластерные роли и offload — Block 1.4; backup/restore — 3.1.5; мониторинг и алерты — Block 3.2; sensitive key и безопасность — Block 3.3.

#### 3.1.4.1 Что происходит при остановке и запуске NiFi

**Остановка (`bin/nifi.sh stop`).**

```text
nifi.sh stop
  └─► Bootstrap-процесс отправляет JVM команду остановки
        └─► Flow Controller: прекращает планирование процессоров (новые onTrigger не стартуют)
              └─► ожидание завершения активных потоков до nifi.flowcontroller.graceful.shutdown.period
                    ├─ потоки завершились → commit сессий → checkpoint FlowFile Repository → выход JVM
                    └─ не завершились → принудительное прерывание (termination), незавершённые сессии откатываются
        └─► Bootstrap ждёт выхода JVM до graceful.shutdown.seconds (bootstrap.conf); затем завершает процесс принудительно
```

Ключевые параметры:

| Параметр | Файл | Значение по умолчанию | Смысл |
|---|---|---|---|
| `nifi.flowcontroller.graceful.shutdown.period` | `nifi.properties` | `10 secs` | Сколько ждать завершения компонентов при остановке ([Administration Guide, исходник документации](https://github.com/apache/nifi/pull/11488/files)). В ветке main (сентябрь 2026, NIFI-16290) описание уточнено: это же значение ограничивает ожидание остановки компонентов перед принудительным прерыванием при **offload узла** ([коммит](https://mail-archive.com/commits@nifi.apache.org/msg46395.html)); **[проверить]**, в каком релизе это поведение доступно у вас |
| `graceful.shutdown.seconds` | `bootstrap.conf` | `20` в документации NiFi Registry (для NiFi значение сверьте в своём `bootstrap.conf`) | Сколько секунд Bootstrap ждёт чистого завершения, после чего принудительно завершает процесс. Изменения `bootstrap.conf` вступают в силу только после остановки и запуска NiFi ([описание](https://docs.cloudera.com/HDPDocuments/HDF3/HDF-3.0.1/bk_administration/content/bootstrap_properties.html)) |
| `nifi.flowcontroller.autoResumeState` | `nifi.properties` | `true` | Возвращать ли компоненты в прежнее состояние (Running/Stopped, Controller Services enabled/disabled) после рестарта ([Administration Guide](https://github.com/apache/nifi/pull/11488/files)). В кластере значение должно быть одинаковым на всех узлах |

**Что это значит на практике.**

- 10 секунд — это **мало** для процессоров, выполняющих длинные операции (большой `PutDatabaseRecord`, `MergeRecord`, запись в сеть). Если за время ожидания поток не завершился, он прерывается; сессия откатывается, FlowFile возвращается в очередь, и операция будет **повторена** после старта. Для идемпотентных приёмников это безопасно, для неидемпотентных — источник дублей.
- Поэтому для плановой остановки используйте **«мягкую» последовательность** (раздел 3.1.4.3): сначала остановить источники и дать конвейеру опустеть, и только потом останавливать JVM.
- Если `graceful.shutdown.seconds` в Bootstrap меньше фактического времени остановки (большой heap, длительный checkpoint), Bootstrap завершит процесс принудительно. Увеличьте значение и согласуйте с таймаутом systemd/Kubernetes (см. 3.1.4.5).

**Запуск.** Последовательность: загрузка NAR и расширений → чтение `flow.json.gz` → восстановление **FlowFile Repository** (чтение WAL и snapshot, восстановление очередей, swap-файлов) → индексация/восстановление **Provenance Repository** → инициализация **Content Repository** (проверка claims) → (в кластере) подключение к ZooKeeper, flow election, присоединение к кластеру → при `autoResumeState=true` включение Controller Services и запуск процессоров.

#### 3.1.4.2 Управление состоянием после перезапуска: `autoResumeState`

`autoResumeState=true` (по умолчанию) — идеальный режим для **автономного восстановления**: после падения или планового рестарта flow сам возвращается в рабочее состояние, оператор не нужен. Именно это свойство реализует «самоподнятие» после сбоя питания, OOM-перезапуска или перезапуска контейнера.

Но есть ситуации, когда автозапуск **вреден**:

| Ситуация | Риск при autoResume = true | Что делать |
|---|---|---|
| Миграция/апгрейд (Block 2.4) | Недопереведённые компоненты стартуют, источники начинают потреблять, пока flow не проверен | Первый старт после апгрейда — с `autoResumeState=false`; проверить invalid/ghost-компоненты; затем запускать вручную/скриптом |
| Восстановление из backup на «чужом» окружении | Процессоры начинают писать в **боевые** приёмники | Старт с `false`, заменить Parameter Context значения, затем запуск |
| Расследование инцидента | Бесконтрольное повторное получение «ядовитых» данных | Старт с `false`, изучить очереди, затем запуск по группам |
| Кластер: узлы с разными значениями | Часть узлов запускается, часть нет → путаница | Одинаковое значение на всех узлах (управлять через IaC) |

Рекомендуемая схема: **в штатной эксплуатации `true`; на время работ и восстановления — временно `false`** через конфигурацию (при управлении конфигурацией через Ansible/Helm значение подставляется параметром окружения). После рестарта с `false` запускайте компоненты **скриптом** (раздел 3.1.4.4), в правильном порядке.

#### 3.1.4.3 Порядок остановки и запуска: от источников к стокам и обратно

Принцип: **при остановке — сначала источники, затем остальные по ходу данных; при запуске — сначала стоки, потом источники.** Тогда в системе нет данных, которым «некуда идти», и нет всплеска повторов.

**Плановая остановка конвейера (runbook).**

1. Перейти в режим обслуживания: оповестить потребителей, при необходимости заморозить изменения flow (организационно).
2. **Остановить источники**: `ConsumeKafka`, `Listen*`, `List*`, `GenerateFlowFile`, `QueryDatabaseTable*`. Для `ConsumeKafka` это приводит к фиксации offsets обработанных данных и к ребалансировке группы — запланируйте это заранее.
3. **Дождаться дренирования очередей** по ходу данных: смотрите счётчики очередей и *Active Threads*; критерий — очереди пусты (или останутся только в failure/DLQ — они переживают рестарт). Не ждите вечно: задайте предел времени (например, 5–15 минут), после которого очередь допускается оставить на диске.
4. **Остановить процессоры** (по группам, в порядке данных), затем **отключить Controller Services** (Disable) — это снимает соединения с БД и Kafka.
5. Остановить NiFi (`bin/nifi.sh stop`), дождаться завершения процесса; убедиться по `nifi-bootstrap.log`/`nifi-app.log`, что остановка была корректной.
6. Выполнить работы.

**Плановый запуск.**

1. Запустить NiFi; дождаться доступности UI/API и завершения загрузки flow (на больших репозиториях запуск занимает минуты).
2. Проверить: нет invalid/ghost-компонентов, Controller Services включены, bulletins отсутствуют (раздел «Чек-лист после запуска» ниже).
3. Запустить **стоки и промежуточные звенья** (`Put*`, `InvokeHTTP`, `PublishKafka`), затем преобразования, затем **источники**.
4. Наблюдать нагрузку 10–15 минут: очереди рассасываются, нет роста back pressure, нет всплеска ошибок.

**Особый случай — очереди на диске.** Данные в очередях переживают рестарт. После запуска стоков накопленный backlog начнёт обрабатываться на полной скорости; приёмник должен быть готов к «пиковой» нагрузке. Для этого используйте `ControlRate` или временно уменьшите `Concurrent Tasks` на стоках.

#### 3.1.4.4 Автоматизация: REST API и скрипты вместо ручных кликов

Любое действие в UI — это вызов REST API (`/nifi-api`), поэтому типовые процедуры автоматизируются. Ниже — иллюстративные примеры; пути `POST /access/token`, `PUT /flow/process-groups/{id}` (запуск и остановка), `PUT /flow/process-groups/{id}/controller-services` (включение и выключение сервисов) и `GET /flow/process-groups/{id}/status` подтверждены описаниями REST API и клиентскими библиотеками; тела запросов и структуру ответов сверяйте с REST API вашей версии, особенно при использовании в production. Для безопасного окружения нужна аутентификация (токен, mTLS или OIDC client credentials — см. Block 3.3); **не храните пароли в скриптах**, используйте секрет-хранилище CI/оркестратора.

**Получить токен (Single User/LDAP) — пример для лаборатории.**

```bash
NIFI=https://nifi-1.example.com:8443/nifi-api
TOKEN=$(curl -s --cacert ca.pem -X POST "$NIFI/access/token" \
        -d "username=$NIFI_USER&password=$NIFI_PASS")
```

**Остановить/запустить все компоненты Process Group.**

```bash
PG_ID=<id группы>
# STOP: отправить состояние STOPPED всем запланированным компонентам группы
curl -s --cacert ca.pem -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -X PUT "$NIFI/flow/process-groups/$PG_ID" \
     -d "{\"id\":\"$PG_ID\",\"state\":\"STOPPED\"}"

# START
curl -s --cacert ca.pem -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
     -X PUT "$NIFI/flow/process-groups/$PG_ID" \
     -d "{\"id\":\"$PG_ID\",\"state\":\"RUNNING\"}"
```

**Включить Controller Services группы** — аналогичный `PUT` на ресурс `/flow/process-groups/{id}/controller-services` с `"state":"ENABLED"` (тело `{"id":"<id группы>","state":"ENABLED"}`).

**Ожидание остановки (чтобы не запустить следующий шаг раньше времени).**

```bash
# Ждём, пока в группе не останется активных потоков процессоров
until [ "$(curl -s --cacert ca.pem -H "Authorization: Bearer $TOKEN" \
      "$NIFI/flow/process-groups/$PG_ID/status" \
      | jq '[.. | .activeThreadCount? // empty] | add')" = "0" ]; do
  sleep 5
done
```

(Структура JSON статуса зависит от версии; идея — суммировать `activeThreadCount`.)

**NiFi CLI (из NiFi Toolkit).** Содержит команды для управления Process Group и версиями flow (например, запуск/остановка группы, включение сервисов, смена версии, работа с Flow Registry Clients; в 2.1.0 добавлены команды управления Flow Registry Clients — [Release Notes 2.1.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)). Перечень команд смотрите в [Toolkit Guide](https://nifi.apache.org/docs/nifi-docs/html/toolkit-guide.html).

Автоматизация приносит три результата: воспроизводимость (одна и та же последовательность каждый раз), скорость (минуты вместо часов кликов) и возможность запускать процедуры по расписанию/из CI без человека.

#### 3.1.4.5 Супервизор процесса: systemd и Kubernetes

В NiFi 1.x Bootstrap умел отправлять уведомления о старте, остановке и **неожиданном падении** NiFi через Notification Services. В 2.x **Bootstrap Notification Services удалены** (2.0.0-M2, [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)), а Bootstrap переработан (мониторинг по HTTP, 2.0.0). Следовательно, **оповещение о падении и гарантированный перезапуск должен обеспечивать внешний супервизор и мониторинг**.

**systemd (Linux, одиночный узел/VM).**

```ini
# /etc/systemd/system/nifi.service  (иллюстрация; адаптируйте под вашу установку)
[Unit]
Description=Apache NiFi
After=network-online.target
Wants=network-online.target

[Service]
Type=forking
User=nifi
Group=nifi
Environment=JAVA_HOME=/usr/lib/jvm/java-21
ExecStart=/opt/nifi/bin/nifi.sh start
ExecStop=/opt/nifi/bin/nifi.sh stop
ExecReload=/opt/nifi/bin/nifi.sh restart
LimitNOFILE=50000
LimitNPROC=10000
Restart=on-failure
RestartSec=30
TimeoutStartSec=900
TimeoutStopSec=180

[Install]
WantedBy=multi-user.target
```

Пояснения: `TimeoutStopSec` должен быть **больше**, чем время корректной остановки (graceful period + `graceful.shutdown.seconds`); `TimeoutStartSec` — достаточно большим для восстановления репозиториев; `LimitNOFILE` — большое число файловых дескрипторов (множество claim-файлов и соединений). Тип `forking`/`Restart` проверяйте на вашем дистрибутиве: `nifi.sh start` запускает демон, и поведение зависит от формы запуска (**[проверить]**; альтернатива — `nifi.sh run` с `Type=simple`).

**Kubernetes.**

- Используйте **StatefulSet** со стабильными идентичностями и persistent volumes для репозиториев; `terminationGracePeriodSeconds` — больше времени корректной остановки; `preStop` может запускать скрипт «мягкой» остановки (остановка источников → ожидание).
- **Probes:** `startupProbe` с большим `failureThreshold` (запуск NiFi с крупными репозиториями — минуты), иначе kubelet будет убивать «ещё загружающийся» под; `livenessProbe` — не слишком агрессивный (GC-пауза не должна приводить к перезапуску); `readinessProbe` — по готовности API (**[проверить]** какие endpoints доступны без аутентификации в вашей конфигурации; часто используют TCP-проверку порта).
- Рекомендуется **PodDisruptionBudget** (не более одного узла недоступно) и **rolling update по одному узлу** с ожиданием готовности.
- В 2.x доступна **Kubernetes-based leader election** без ZooKeeper (Block 1.4).

#### 3.1.4.6 Rolling restart кластера без простоя

Цель — обновить/перезапустить узлы по одному, чтобы кластер всё время обрабатывал данные.

1. **Предусловия:** кластер здоров (все узлы CONNECTED, кворум ZooKeeper), нет невалидных компонентов; принято решение, что остаётся достаточная ёмкость при выводе одного узла.
2. **Узел N → Disconnect** (UI: Cluster → Disconnect). Узел перестаёт получать запросы кластера, но продолжает работать.
3. **Offload** (Cluster → Offload): узел останавливает приём новых данных у источников и **передаёт свои FlowFile** другим узлам через load balancing. Дождаться состояния OFFLOADED. Для успешного offload на связях должна быть возможна балансировка; FlowFile в связях с отключённой балансировкой могут «застрять» — перед offload проверьте. Offload ограничен по времени ожидания остановки компонентов (см. graceful period выше).
4. **Остановить NiFi на узле**, выполнить работы (обновление, перезапуск ОС, смена конфигурации).
5. **Запустить**, дождаться загрузки; узел **присоединяется (Reconnect)** и синхронизирует flow с кластером (с 1.16.0 — даже при существенно разном локальном flow, с сохранением backup своего). Если узел был offloaded — выполните *Connect*.
6. **Проверка здоровья** перед следующим узлом: узел CONNECTED; нет invalid-компонентов; очередь сбалансирована; (для Kafka) нет всплеска ребалансировок и lag; метрики в норме.
7. Повторить для остальных узлов. **Primary Node** и **Cluster Coordinator** при перезапуске перевыбираются — процессоры *Primary Node Only* (`List*`, `QueryDatabaseTable*`) переедут на новый узел, state берётся из cluster state.

**Что ускоряет одновременный рестарт кластера.** При одновременном старте всех узлов происходит **flow election**: узлы ждут до `nifi.cluster.flow.election.max.wait.time` либо пока не наберётся `nifi.cluster.flow.election.max.candidates` кандидатов ([Migration Guidance 1.0→1.1](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)). Если `max.candidates` равно числу узлов, выборы завершаются сразу после подключения всех узлов, а не по таймауту.

**Ограничения 2.x для TLS-кластера.** С 2.12.0 `nifi.cluster.node.address` и `nifi.cluster.load.balance.host` должны быть DNS-именами; после смены образов/адресов проверяйте это в первую очередь ([Migration Guidance 2.12.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).

#### 3.1.4.7 Что замедляет перезапуск и как это уменьшить

| Фактор | Почему замедляет | Мера |
|---|---|---|
| Огромные очереди (миллионы FlowFile) | Чтение WAL/snapshot, восстановление swap-файлов | Держать очереди небольшими (back pressure, оперативный дренаж); решать причины накопления |
| Большой FlowFile Repository / редкие checkpoints | Долгое воспроизведение журнала | Настроить `nifi.flowfile.repository.checkpoint.interval`; медленные диски → быстрые (NVMe для WAL) |
| Крупный Provenance Repository | Восстановление индексов | Уменьшить срок/размер хранения; изолированный быстрый том (Block 3.4) |
| Много Controller Services и «внешних» проверок | Включение сервисов ждёт подключения к БД/Kafka | Короткие таймауты подключения; доступность зависимостей перед стартом |
| Flow election по таймауту | Ожидание `max.wait.time` | Установить `max.candidates` = числу узлов |
| Медленная остановка (GC-паузы, большой heap) | Bootstrap убивает процесс → долгое восстановление после «грязного» выхода | Правильные timeout; корректная остановка источников до рестарта |
| Грязный выход (`kill -9`, потеря питания) | Восстановление после неполных записей | Избегать; использовать `nifi.sh stop` |

#### 3.1.4.8 Самовосстановление на уровне flow: чтобы человек не понадобился

Перезапуск процесса — крайняя мера. Большинство сбоев должно решаться **без перезапуска**, внутри flow:

| Сбой | Автоматический механизм | Настройка |
|---|---|---|
| Приёмник временно недоступен | Framework retry с backoff; `Yield Duration`; очередь + back pressure | Retry на relationships; адекватные таймауты |
| Сбой отдельных данных | Маршрутизация в DLQ; основной поток не блокируется | Block 3.1.3 |
| Потеря соединения с БД/Kafka | Контроллер-сервис/клиент переподключается; процессор повторяет `onTrigger` | Validation Query у DBCP, таймауты; Kafka client reconnect |
| Процессор «завис» на внешнем вызове | Таймауты в процессоре (Connection/Read Timeout) | Всегда задавать явные таймауты |
| Контроллер-сервис не включился (зависимость была недоступна) | Фреймворк периодически пробует включить сервис повторно (поведение зависит от версии — **[проверить]**); известны исправления сценариев «сервисы не стартуют после рестарта» ([Release Notes 1.28.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)) | Мониторинг состояния сервисов + алерт |
| Падение JVM | Супервизор (systemd/Kubernetes) + `autoResumeState=true` + WAL | 3.1.4.5 |
| Узел выпал из кластера | Автоматическое возвращение узла и синхронизация flow | Корректные heartbeat/таймауты, стабильная сеть |
| Простой источника (нет данных) | `MonitorActivity` → алерт | Block 3.2 |

**Watchdog-flow (опционально).** Простой «страж» внутри NiFi или (предпочтительно) во внешнем оркестраторе: периодически читает статус через REST/Site-to-Site Reporting Tasks (см. подраздел «Site-to-Site Reporting Tasks» в Block 3.2), обнаруживает признаки зависания (*processor invalid*, очередь не уменьшается при активном приёмнике, `activeThreadCount > 0` слишком долго) и вызывает API (Stop → дождаться → Start) либо поднимает алерт. Рекомендации: (а) автоматические действия ограничивайте **числом попыток** и «охлаждением» (иначе будет бесконечный цикл рестартов); (б) вызовы API делайте из внешней системы с минимальными правами; (в) для критичных действий оставляйте подтверждение человеком.

**Принципы дизайна flow, снижающие ручное вмешательство:**

1. Параметры окружения — в Parameter Contexts (смена значения не требует правки flow).
2. Нет «петель без счётчика» (retry всегда ограничен).
3. У каждой failure-ветки есть сток и алерт.
4. Все внешние вызовы имеют таймауты.
5. Приёмники идемпотентны → рестарт и повторы безопасны.
6. Состояние хранится в state/в Kafka/в приёмнике, а не «в голове у оператора» (offset, `List*` state).
7. Все операции (stop/start/clear/replay) описаны runbook-ом и автоматизированы.

#### 3.1.4.9 Чек-листы

**Перед плановым рестартом:** источники остановлены; очереди опустошены (или осознанно оставлены); Controller Services отключены; бэкап `conf/`, `state/local`, ключей сделан (3.1.5); есть план отката; окно согласовано.

**После запуска (валидация, ≈10–15 минут):**

- [ ] Все узлы CONNECTED, роли Coordinator/Primary выбраны.
- [ ] Нет invalid/ghost-компонентов (UI: Summary; поиск `invalid`).
- [ ] Все нужные Controller Services *Enabled*.
- [ ] Bulletins ERROR отсутствуют (или объяснены).
- [ ] Очереди рассасываются, нет заполненных back pressure.
- [ ] Kafka: consumer group стабилизирована, lag не растёт.
- [ ] Нет неожиданного роста DLQ.
- [ ] Метрики heap/GC/диски в норме.

#### 3.1.4.10 Типичные проблемы после рестарта

| Симптом | Вероятная причина | Решение |
|---|---|---|
| После старта все процессоры остановлены | `autoResumeState=false` или flow загружен «без состояния» | Запустить скриптом/вручную; вернуть `true` после работ |
| Узел не присоединяется к кластеру | Разные `nifi.sensitive.props.key`; DNS/TLS (SAN, 2.12+); недоступен ZooKeeper | Сверить ключ, сертификаты, `nifi.cluster.*`; логи `nifi-app.log` |
| Controller Service не включается | Недоступна БД/Kafka; неверные параметры | Проверить зависимость; Disable → Enable |
| Процессор `invalid` после рестарта | Внешняя проверка (например, Kafka topic) не проходит | Проверить доступность и права; Verify в UI |
| Долгий старт | Крупные очереди/репозитории | См. 3.1.4.7 |
| Дубли в приёмнике после рестарта | At-least-once: прерванные операции повторены | Идемпотентность приёмника |
| Повторный листинг файлов/таблиц | Потерян/очищен state | Восстановить `state/local` или принять повторную загрузку |
| Всплеск ребалансировок Kafka | Перезапуск потребителей | Staggered restart (по одному узлу), нужная версия ≥ 2.8.0 |
| Узел 2.x не стартует после отката с 2.14 | Новый формат local state | Восстановить `state/local` из backup (Block 2.4) |

#### Mini-lab 3.1.4: учебный рестарт под нагрузкой

**Цель.** Убедиться, что конвейер переживает плановый и аварийный рестарт без потерь, а повторы допустимы.

**Шаги.**

1. Запустите конвейер из Lab 7 или Block 3.5 с нагрузкой.
2. **Плановый рестарт:** выполните runbook 3.1.4.3 (остановить источники → дренаж → остановка → запуск → стоки → источники). Замерьте время остановки/запуска.
3. **Аварийный рестарт:** отправьте `kill -9` процессу JVM NiFi (**только в лаборатории**). Наблюдайте: супервизор поднимает NiFi, при `autoResumeState=true` flow стартует сам.
4. Сравните число записей на входе и в приёмнике; посчитайте дубли (ключ события).
5. Повторите с `autoResumeState=false` и запустите flow скриптом из 3.1.4.4.
6. Сформулируйте выводы: сколько дублей допустимо; сколько времени занял восстановление; что автоматизировать.

**Проверка.** Нет потери данных; дубли обнаружены и обработаны приёмником (UPSERT/ключ); после рестарта нет invalid-компонентов; время восстановления зафиксировано.

### 3.1.5 Backup и restore: конфигурация, определения flow, ключи

| Что | Где | Зачем | Периодичность |
|---|---|---|---|
| `conf/flow.json.gz` | `NIFI_HOME/conf/` | Текущий flow | Регулярно + перед изменениями; NiFi хранит версии в `conf/archive/` |
| Версии flow в Registry (Git) | Git-репозиторий | Основной механизм отката логики | Каждое изменение (commit) |
| `nifi.properties`, `bootstrap.conf`, `logback.xml`, `state-management.xml` | `conf/` | Конфигурация узла | При изменении (под управлением CM/IaC) |
| `authorizers.xml`, `users.xml`, `authorizations.xml`, `login-identity-providers.xml` | `conf/` | Пользователи, группы, политики | Регулярно |
| **`nifi.sensitive.props.key`** | `nifi.properties` / секрет-хранилище | Без него невозможно расшифровать sensitive-значения в flow | Хранить отдельно и надёжно |
| Сертификаты и ключи (keystore/truststore/PEM) | Файловая система/секрет-хранилище | TLS | По процедуре PKI |
| `state/local` и cluster state (ZooKeeper) | `state/`, ZooKeeper | Позиции `List*`, инкрементальные выгрузки | Перед апгрейдами и регулярно |
| Репозитории (FlowFile/Content/Provenance) | Тома данных | Данные «в полёте»; provenance | Снимки томов при плановых работах (на горячую согласованный backup нетривиален) |
| `database_repository` | Каталог | История изменений flow | По необходимости |

**Замечание.** Backup репозиториев «на горячую» файловым копированием может быть несогласованным; для согласованного снимка остановите NiFi или используйте снимки на уровне блочных устройств с учётом связанных репозиториев. В большинстве сценариев **данные в полёте считаются повторно получаемыми** (из Kafka, файлов), а backup концентрируется на flow, конфигурации, ключах и state.

**Restore (последовательность).** 1) Установить ту же (или совместимую) версию NiFi; 2) вернуть `conf/` и ключи; 3) убедиться, что `nifi.sensitive.props.key` совпадает; 4) вернуть state (при необходимости); 5) запустить; 6) проверить валидность компонентов; 7) включить источники.

В 2.12.0 добавлена фреймворковая абстракция **PropertyEncryptionProvider** для шифрования свойств ([Release Notes 2.12.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)); при её использовании ключи провайдера также входят в состав backup.

### Lab 7: flow с retry и обработкой ошибок

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

**Эталонное решение:** `nifi/flows/lab7-retry-errors.json`.

---

## Block 3.2: Monitoring, provenance and troubleshooting

### 3.2.1 NiFi UI, метрики, Provenance, bulletins

**UI.**

| Инструмент | Что показывает |
|---|---|
| Canvas: счётчики на процессорах и связях | In/Out, Read/Write, Tasks/Time (CPU-время), размеры очередей, индикатор back pressure |
| **Status History** | Графики метрик компонента во времени (в т. ч. на уровне кластера по узлам) |
| **Summary** (глобальное меню) | Сводные таблицы по процессорам, связям, портам, Process Group; сортировка по Tasks/Time и т. п. |
| **Bulletin Board** | Все bulletins (с уровнем, источником, узлом); в 2.7.0 добавлены stack trace и очистка |
| **Counters** | Пользовательские счётчики (`UpdateCounter`) |
| **System Diagnostics** | Heap, GC, потоки, размеры репозиториев, нагрузка |
| **Cluster** | Узлы, роли, heartbeat, нагрузка по узлам |
| **Flow Configuration History** | Аудит изменений (Block 3.3) |
| **Data Provenance** | Поиск и визуализация событий |

**Provenance Visualizer (Lineage graph).** Для выбранного FlowFile показывает граф событий: `RECEIVE`/`CREATE` → `ROUTE`, `CONTENT_MODIFIED`, `CLONE`/`FORK`/`JOIN`, `SEND` → `DROP`. Из каждого события доступны атрибуты до/после, **контент до/после** (если не истёк срок хранения в Content Repository/archive и есть права на данные), возможность **Replay** (повторная отправка FlowFile в точку события). В 2.8.0 в UI lineage рядом с узлом показывается тип компонента ([Release Notes 2.8.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).

Основные типы событий: `RECEIVE`, `FETCH`, `CREATE`, `SEND`, `ROUTE`, `CLONE`, `FORK`, `JOIN`, `CONTENT_MODIFIED`, `ATTRIBUTES_MODIFIED`, `DROP`, `EXPIRE`, `REPLAY`, `DOWNLOAD`.

**Метрики для внешнего мониторинга.**

- **Prometheus:** REST-эндпоинт метрик (с 1.12.0, [Release Notes 1.12.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes); `/nifi-api/flow/metrics/prometheus`) и `PrometheusReportingTask`. В 2.13.0 добавлены метрики `cluster_is_primary_node` и `cluster_is_cluster_coordinator`; в 2.12.0 из `cluster_connected_node_count` удалена метка `connected_nodes` ([Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).
- **Site-to-Site Reporting Tasks:** отправка статусов, bulletins и метрик в NiFi-получатель (`SiteToSiteStatusReportingTask`, `SiteToSiteBulletinReportingTask`, `SiteToSiteMetricsReportingTask`) — подробно в подразделе «Site-to-Site Reporting Tasks: подробно» ниже.
- **Ключевые метрики для алертов:** размер/число FlowFile в очередях и признак back pressure; возраст старейшего FlowFile в очереди; процент использования heap и время GC; заполнение репозиториев (`MonitorDiskUsage`); число активных потоков; число ошибок (bulletins ERROR); узлы CONNECTED; lag consumer group Kafka (во внешней системе); время с последней активности (`MonitorActivity`).

**Логи.**

| Файл | Содержимое |
|---|---|
| `nifi-app.log` | Основной лог приложения и процессоров |
| `nifi-bootstrap.log` | Запуск/остановка/перезапуск JVM |
| `nifi-user.log` | Действия пользователей, запросы авторизации |
| `nifi-request.log` | HTTP-запросы (с 1.16.0) |
| `nifi-deprecation.log` | Предупреждения об использовании deprecated-компонентов (с 1.18.0) |

#### Site-to-Site Reporting Tasks: подробно

Подраздел раскрывает пункт «отправка статусов и bulletins самому NiFi/другим системам» (`SiteToSiteStatusReportingTask`, `SiteToSiteBulletinReportingTask`, `SiteToSiteMetricsReportingTask`). Все три задачи входят в бандл `nifi-site-to-site-reporting-nar` и присутствуют в документации 2.x (например, 2.10.0 — [справочник](https://nifi.incubator.apache.org/components/org.apache.nifi.reporting.SiteToSiteMetricsReportingTask)). Описание свойств ниже основано на справочниках компонентов (версии 1.27–1.28 и 2.0.0-M2/M3, [Status](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-site-to-site-reporting-nar/1.28.0/org.apache.nifi.reporting.SiteToSiteStatusReportingTask), [Metrics](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-site-to-site-reporting-nar/1.28.0/org.apache.nifi.reporting.SiteToSiteMetricsReportingTask)); точные имена и значения по умолчанию в вашей версии проверьте во встроенной документации (**[проверить]**).

##### Зачем это нужно

UI NiFi показывает статус «здесь и сейчас» и короткую историю (Status History). Этого недостаточно для эксплуатации: нужны **долгосрочные тренды, алерты и корреляция с другими системами**. Есть два подхода:

| Подход | Суть | Когда выбирать |
|---|---|---|
| **Pull** (Prometheus, REST API) | Внешняя система сама опрашивает NiFi | Уже есть Prometheus/Grafana; нужны метрики узла и JVM |
| **Push через Site-to-Site Reporting Tasks** | NiFi сам формирует записи о статусе/bulletins/метриках и отправляет **в Input Port другого (или того же) NiFi**, где они обрабатываются обычным flow | Нужно обработать данные мониторинга средствами NiFi (фильтрация, обогащение, маршрутизация в Splunk, Kafka, БД, e-mail, чат); нет возможности ставить Prometheus; нужны события bulletins как поток |

Сильная сторона push-подхода: данные мониторинга — это обычные **записи (records)**, а значит, к ним применимы всё, что вы уже знаете из Day 1–2: `QueryRecord`, `PartitionRecord`, `PutSplunk`, `PublishKafka`, DLQ, back pressure.

##### Что такое Site-to-Site (S2S)

**Site-to-Site** — встроенный протокол передачи FlowFile между экземплярами NiFi (и из MiNiFi/клиентских библиотек). Принимающая сторона публикует **Remote Input Port** (Input Port корневой группы); отправляющая — подключается по URL NiFi и пишет в порт по имени. Два транспорта (свойство *Transport Protocol*): `RAW` (сокет; значение по умолчанию) и `HTTP` (удобен через прокси/балансировщики). S2S поддерживает сжатие, батчи и распределение между узлами кластера-получателя: **адрес в Destination URL используется только для установления соединения, а сами данные балансируются по всем узлам целевого кластера** ([справочник Status](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-site-to-site-reporting-nar/2.0.0-M3/org.apache.nifi.reporting.SiteToSiteStatusReportingTask/index.html)).

##### Архитектура

```text
 Узел NiFi (источник мониторинга)                                       Узел/кластер NiFi (приёмник мониторинга)
┌──────────────────────────────────────┐   S2S (RAW|HTTP, TLS, compress)  ┌─────────────────────────────────────────────┐
│ Reporting Tasks (запуск по расписанию)│ ───────────────────────────────► │ Input Port "monitoring-in" (root group)     │
│  • SiteToSiteStatusReportingTask      │  записи JSON/Avro (Record Writer) │   │                                         │
│  • SiteToSiteBulletinReportingTask    │                                   │   ▼                                         │
│  • SiteToSiteMetricsReportingTask     │                                   │ Flow обработки мониторинга:                 │
└──────────────────────────────────────┘                                   │  RouteOnAttribute / QueryRecord ─► алерты   │
                                                                           │  ─► PutSplunk / PublishKafka / PutDatabase… │
                                                                           └─────────────────────────────────────────────┘
```

Варианты размещения приёмника:

1. **Тот же NiFi/кластер** (Destination URL указывает на себя). Просто в настройке, но **мониторинг умирает вместе с системой**: если NiFi остановлен, алерта не будет.
2. **Отдельный «monitoring» NiFi** (малый кластер/инстанс). Рекомендуемый вариант для production: сбои основного кластера не мешают алертам. Ограничение — нужно эксплуатировать ещё один экземпляр.
3. **Несколько приёмников** — отдельные задачи для разных получателей (например, безопасность и эксплуатация).

##### Общие свойства всех трёх задач

| Свойство | Назначение | Значение по умолчанию / примечания |
|---|---|---|
| **Destination URL** | URL NiFi-получателя (для кластера — список адресов через запятую, формат `http(s)://host:port/nifi`) | Используется только для первоначального соединения; данные балансируются по узлам получателя |
| **Input Port Name** | Имя Input Port получателя | Должно совпадать точно |
| **SSL Context Service** | TLS-настройки при отправке | Если не задан, соединение **не защищено**; в production задавайте всегда |
| **Instance URL** | URL этого экземпляра, помещаемый в Content URI события | По умолчанию `http://${hostname(true)}:8080/nifi` — **для HTTPS-развёртывания (порт 8443) обязательно поправьте** |
| **Compress Events** | Сжатие данных при передаче | `true` |
| **Communications Timeout** | Сколько ждать ответа получателя | `30 secs` |
| **Batch Size** | Максимум записей в одной пачке (у Status/Bulletin) | `1000` |
| **Transport Protocol** | `RAW` или `HTTP` | `RAW` |
| HTTP Proxy hostname/port/username/password | Прокси для режима `HTTP` | Пароль — sensitive |
| **Record Writer** | Controller Service для сериализации записей | Например, `JsonRecordSetWriter` (для Status/Bulletin обязателен; для Metrics — при формате `Record Format`) |
| **Include Null Values** | Выводить ли поля со значением `null` | `false` |
| **Platform** | Значение поля `platform` в записях | `nifi`; удобно задавать «окружение/кластер» (`prod-eu`, `test`) для разделения потоков в приёмнике |

Свойства с Expression Language вычисляются по **переменным окружения/системным свойствам** (scope *Environment*; в 1.x — Variable Registry, удалён в 2.0). Отдельное замечание: reporting tasks создаются на уровне контроллера (Global menu → Controller Settings → Reporting Tasks), а не внутри Process Group — **Parameter Contexts к ним неприменимы** (**[проверить]**), поэтому значения окружения подставляйте через конфигурацию задачи или через EL/переменные окружения.

##### SiteToSiteStatusReportingTask

**Что отправляет:** периодические «снимки» статуса компонентов: процессоров, Process Group (в т. ч. корневой), Remote Process Group, связей (Connection), входных/выходных портов. Каждая запись — один компонент в один момент времени.

**Фильтры (чтобы не слать лишнее):**

| Свойство | По умолчанию | Назначение |
|---|---|---|
| Component Type Filter Regex | `(Processor|ProcessGroup|RemoteProcessGroup|RootProcessGroup|Connection|InputPort|OutputPort)` | Какие типы компонентов отправлять. Например, `(Processor|Connection)` — только процессоры и очереди |
| Component Name Filter Regex | `.*` | Фильтр по имени компонента |

Важно: фильтры образуют пересечение (запись должна подходить под оба); при этом все Process Group обходятся рекурсивно независимо от того, подходит ли сама группа под фильтр ([справочник](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-site-to-site-reporting-nar/1.27.0/org.apache.nifi.reporting.SiteToSiteStatusReportingTask/index.html)).

**Поля записи (выдержка из схемы, [additionalDetails](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-site-to-site-reporting-nar/1.27.0/org.apache.nifi.reporting.SiteToSiteStatusReportingTask/additionalDetails.html)):**

| Группа | Поля |
|---|---|
| Идентификация | `statusId`, `timestamp`, `timestampMillis`, `actorHostname`, `componentType`, `componentName`, `componentId`, `parentId`, `parentName`, `parentPath`, `platform`, `application` |
| Объёмы | `bytesRead`, `bytesWritten`, `inputCount`, `inputBytes`, `outputCount`, `outputBytes`, `bytesTransferred`, `flowFilesTransferred` |
| Очереди/back pressure | `queuedCount`, `queuedBytes`, `maxQueuedCount`, `maxQueuedBytes`, `backPressureObjectThreshold`, `backPressureBytesThreshold`, `backPressureDataSizeThreshold`, `isBackPressureEnabled` |
| Процессоры | `processorType`, `runStatus`, `averageLineageDurationMS`, `terminatedThreadCount`, `counters` (map) |
| Remote Process Group | `activeRemotePortCount`, `inactiveRemotePortCount`, `transmissionStatus`, `targetURI` |

**Практическое применение:**

- *Долгосрочные тренды* пропускной способности каждого процессора и размера очередей (в UI хранится ограниченная история).
- *Раннее обнаружение back pressure*: условие `isBackPressureEnabled = 'true'` (в схеме поле строкового типа).
- *Обнаружение остановленных/невалидных компонентов*: `runStatus` отличен от ожидаемого (`Running`).
- *Отчёты о загрузке по Process Group/командам*.

##### SiteToSiteBulletinReportingTask

**Что отправляет:** bulletins (ERROR/WARN/INFO) со всех компонентов — как поток событий. Это даёт то, чего нет у метрик: **текст причины ошибки**.

Свойства — общие (см. таблицу выше): Destination URL, Input Port Name, SSL Context Service, Instance URL, Compress Events, Communications Timeout, Batch Size, Transport Protocol, Record Writer, Include Null Values, Platform.

**Поля записи** (схема из [справочника задачи](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-site-to-site-reporting-nar/1.7.1/org.apache.nifi.reporting.SiteToSiteBulletinReportingTask/additionalDetails.html)): `objectId`, `platform`, `bulletinId`, `bulletinCategory`, `bulletinGroupId`, `bulletinGroupName`, `bulletinGroupPath`, `bulletinLevel`, `bulletinMessage`, `bulletinNodeAddress`, `bulletinNodeId`, `bulletinSourceId`, `bulletinSourceName`, `bulletinSourceType`, `bulletinTimestamp` (формат `yyyy-MM-dd'T'HH:mm:ss.SSS'Z'`), `bulletinFlowFileUuid`.

**Практическое применение:**

- Алерты на `ERROR` с текстом причины и именем компонента.
- Корреляция сбоев по времени с инцидентами во внешних системах.
- Аудит «шума»: какие компоненты чаще всего пишут ошибки.

**Особенность (подтверждено справочником задачи).** Хранится не более **5 bulletins на компонент и 10 на уровне контроллера, не дольше 5 минут**; если задача запускается недостаточно часто, часть bulletins не будет отправлена. Поэтому **интервал запуска должен быть заметно меньше 5 минут** (в примере ниже — 30 секунд), а при большом числе ошибок на одном компоненте часть сообщений теряется в любом случае.

##### SiteToSiteMetricsReportingTask

**Что отправляет:** метрики **узла NiFi и JVM** — те же, что собирал Ambari Reporting Task: загрузка CPU/load average, число ядер, число FlowFile и байт в очередях, объёмы за последние 5 минут, число активных потоков, суммарное время задач, метрики JVM (uptime, heap, GC, состояния потоков, файловые дескрипторы и пр.).

Дополнительные свойства:

| Свойство | По умолчанию | Назначение |
|---|---|---|
| **Output Format** (`s2s-metrics-format`) | `Ambari Format` | `Ambari Format` — JSON в формате Ambari Metrics API с динамическими ключами (для обработки рекомендуется Jolt-преобразование, пример есть в документации); `Record Format` — записи через Record Writer с фиксированной схемой |
| **Hostname** (`s2s-metrics-hostname`) | `${hostname(true)}` | Имя хоста, включаемое в метрики |
| **Application ID** (`s2s-metrics-application-id`) | `nifi` | Идентификатор приложения в метриках |

В `Record Format` схема содержит поля вида: `appid`, `instanceid`, `hostname`, `timestamp`, `loadAverage1min`, `availableCores`, `FlowFilesReceivedLast5Minutes`, `BytesReceivedLast5Minutes`, `FlowFilesSentLast5Minutes`, `BytesSentLast5Minutes`, `FlowFilesQueued`, `BytesQueued`, `BytesReadLast5Minutes`, `BytesWrittenLast5Minutes`, `ActiveThreads`, `TotalTaskDurationSeconds`, `jvmuptime`, `jvmheap_used`, `jvmheap_usage`, `jvmnon_heap_usage`, `jvmthread_states*` и др. ([additionalDetails](https://nifi.incubator.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-site-to-site-reporting-nar/1.28.0/org.apache.nifi.reporting.SiteToSiteMetricsReportingTask/additionalDetails.html)).

**Рекомендация:** для новых проектов используйте `Record Format` (проще обрабатывать `QueryRecord`-ом и отправлять в хранилища), а `Ambari Format` оставляйте только при необходимости совместимости.

##### Сравнение трёх задач

| Аспект | Status | Bulletin | Metrics |
|---|---|---|---|
| Единица записи | Один компонент (процессор/очередь/группа/порт) | Одно сообщение bulletin | Один набор метрик узла |
| Уровень детализации | По компонентам flow | По событиям ошибок | По узлу и JVM |
| Типовой вопрос | Где растёт очередь? Что остановлено? | Что сломалось и почему? | Хватает ли heap/CPU? Нет ли утечки потоков? |
| Объём данных | Большой (пропорционален числу компонентов × частота) | Зависит от числа ошибок | Небольшой |
| Алерт-кейс | Back pressure, невалидные/остановленные компоненты | Рост ERROR | Heap > порога, GC, потоки |
| Дополнительно | Фильтры по типу/имени | — | Формат Ambari/Record |

(В бандле есть также `SiteToSiteProvenanceReportingTask` — отправка событий provenance; в программе курса не рассматривается.)

##### Настройка получателя

1. **Input Port в корневой Process Group** получателя (S2S принимает только порты корневой группы): имя совпадает с *Input Port Name* в задаче (например, `monitoring-in`). Порт нужно **запустить**.
2. **Конфигурация S2S в `nifi.properties`** получателя (имена подтверждены примером реальной конфигурации 2.x и документацией портов): `nifi.remote.input.host`, `nifi.remote.input.secure=true`, `nifi.remote.input.socket.port` (для RAW), `nifi.remote.input.http.enabled=true` (для HTTP). Порт RAW должен быть доступен по сети.
3. **Права доступа (в защищённом режиме):** идентичности отправляющих узлов (DN сертификатов) должны иметь глобальную политику **retrieve site-to-site details** и политику **receive data via site-to-site** на Input Port. Без них задача получит отказ в авторизации (bulletin на задаче).
4. **TLS:** отправитель использует *SSL Context Service* с сертификатом, которому доверяет получатель; получатель — с сертификатом, которому доверяет отправитель. Для кластера-получателя адреса узлов должны соответствовать SAN сертификатов (Block 1.4).
5. В 2.x: `StandardRestrictedSSLContextService` в таких задачах задаёт ограничения протоколов; примечание — с 2.7.0 он **[dep]** в пользу базового `SSLContextService` (см. [Migration Guidance 2.7.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)); проверьте, какой тип предлагает UI вашей версии.

##### Пошаговая настройка (на примере Status + Bulletin → Splunk)

**1) Получатель (monitoring NiFi).**

```text
Input Port "monitoring-in"  (root group, Running)
   │
   ▼
UpdateAttribute:  schema/тип потока  (опционально; можно различать по полям записи)
   │
   ▼
PartitionRecord ── по полю componentType/bulletinLevel (опционально)
   │
   ├─► QueryRecord "alerts"  : фильтр критичных записей ─► alert-цепочка
   └─► MergeRecord (NDJSON) ─► PutSplunk / InvokeHTTP→HEC  (долгосрочное хранение/тренды)
```

**2) Отправитель (боевой NiFi): Global menu → Controller Settings → Reporting Tasks → «+».**

| Задача | Ключевые настройки |
|---|---|
| `SiteToSiteStatusReportingTask` | Destination URL `https://monitor.example.com:8443/nifi`; Input Port Name `monitoring-in`; SSL Context Service — сервис с клиентским сертификатом; Instance URL `https://${hostname(true)}:8443/nifi`; Record Writer `JsonRecordSetWriter` (NDJSON, без Pretty Print); Platform `prod-eu`; Component Type Filter Regex `(Processor|Connection)`; Run Schedule `1 min` |
| `SiteToSiteBulletinReportingTask` | Те же Destination/Input Port/SSL; Record Writer `JsonRecordSetWriter`; Platform `prod-eu`; Run Schedule `30 sec` |
| `SiteToSiteMetricsReportingTask` | Output Format `Record Format`; Record Writer `JsonRecordSetWriter`; Hostname `${hostname(true)}`; Application ID `nifi-prod`; Run Schedule `1 min` |

Запустите задачи (кнопка *Start*). Для всех трёх можно использовать один Input Port и различать типы по полям (`componentType` / `bulletinLevel` / `appid`). Если приёмник один, а потоков много, заведите **разные порты** (`monitoring-status-in`, `monitoring-bulletin-in`, `monitoring-metrics-in`) — проще настраивать Reader'ы и права.

**3) Примеры SQL для `QueryRecord` на приёмнике.**

Bulletins уровня ERROR/WARN (динамическое свойство `alerts`):

```sql
SELECT bulletinNodeAddress, bulletinGroupName, bulletinSourceName, bulletinLevel, bulletinMessage, bulletinTimestamp
FROM FLOWFILE
WHERE bulletinLevel IN ('ERROR', 'WARN')
```

Очереди с включённым back pressure (из Status):

```sql
SELECT actorHostname, componentName, parentName, backPressureObjectThreshold, isBackPressureEnabled
FROM FLOWFILE
WHERE componentType = 'Connection' AND isBackPressureEnabled = 'true'
```

Остановленные/невалидные процессоры (значения `runStatus` проверьте на вашей версии):

```sql
SELECT actorHostname, componentName, parentName, runStatus
FROM FLOWFILE
WHERE componentType = 'Processor' AND runStatus <> 'Running'
```

Heap выше порога (Metrics, Record Format):

```sql
SELECT hostname, timestamp, jvmheap_usage, FlowFilesQueued, ActiveThreads
FROM FLOWFILE
WHERE jvmheap_usage > 0.85
```

(Для SQL нужен Reader со схемой, соответствующей входным данным. Для записей S2S проще всего использовать `JsonTreeReader` с **Schema Access Strategy = Infer Schema** на лабораторном стенде и **явную схему** в production — по Block 1.3.)

**4) Подавление «шторма алертов».** Один и тот же сбой будет приходить в каждой пачке. Фильтруйте повторы: `PartitionRecord` по ключу (например, `bulletinSourceId`) → `DetectDuplicate` (Cache Entry Identifier = `${bulletinSourceId}`; *Age Off Duration* `15 min`) → только `non-duplicate` ведёт в уведомление. Для кластерного приёмника используйте распределённый cache-сервис (в 2.x сервисы называются `MapCacheServer`/`MapCacheClientService`, см. Block 2.4).

**5) Доставка алертов.** `PutEmail`, `InvokeHTTP` в Slack/Teams/PagerDuty/Opsgenie, `PublishKafka` в топик `alerts`, `PutSplunk`/HEC. Всегда добавляйте контекст: `platform`, `actorHostname`/`bulletinNodeAddress`, имя группы, ссылка на компонент (`Instance URL` + `?processGroupId=...&componentIds=...`).

##### Производительность и ёмкость

- **Частота.** Status — раз в 1–5 минут (а не в секунды); Bulletin — 15–60 секунд; Metrics — 1 минута. Чем чаще, тем больше нагрузка на отправителя, сеть и приёмник.
- **Объём Status** пропорционален числу компонентов: 1000 компонентов × 1 раз/мин × 1 КБ ≈ 1 МБ/мин на узел — для кластера умножьте на число узлов. Ограничивайте `Component Type Filter Regex` и `Component Name Filter Regex`.
- **Сжатие** (`Compress Events=true`) снижает сеть, но добавляет CPU.
- **Batch Size** влияет на размер пачек и число обращений.
- **Приёмник** должен справляться с пиками (после простоя связи отправитель может послать накопленное).

##### Надёжность и подводные камни

| Проблема | Причина | Решение |
|---|---|---|
| Данные мониторинга не приходят | Порт не запущен/имя не совпадает; нет политик; сеть/TLS | Проверить Input Port, policies *retrieve site-to-site details* / *receive data via site-to-site*, bulletins на самой Reporting Task |
| Ошибка TLS/handshake | Недоверенный сертификат; SAN не соответствует хосту | Truststore/SAN; для кластера-получателя — DNS-имена (с 2.12.0) |
| Ссылки в событиях ведут на `:8080` | Не изменён *Instance URL* | Указать актуальный HTTPS URL |
| Пропуски в данных | Нет очереди на стороне reporting task: если получатель недоступен, данные за период могут быть потеряны (**[проверить]** поведение вашей версии) | Не использовать S2S-мониторинг как единственный источник алерта; дублировать критичные алерты через Prometheus/внешний health-check |
| **Петля обратной связи** | Мониторинг-flow сам попадает в Status (генерирует записи о собственных компонентах) на том же NiFi | Отдельный monitoring-NiFi; либо исключить группу фильтром по имени |
| Всплеск объёма | Слишком частый запуск/слишком много компонентов | Расписание, фильтры |
| Дубли с нескольких узлов | Reporting Tasks работают на каждом узле, каждый отправляет свой статус | Различайте по `actorHostname`/`bulletinNodeAddress`; для задач, которым нужен один отчёт на кластер, проверьте возможность ограничить выполнение Primary Node (**[проверить]**) |
| Алерт-шторм | Повторяющиеся bulletins | Дедупликация (п. 4) |
| Утечка секретов в сообщениях | Bulletin message может содержать фрагменты данных/URL с токенами | Маскирование (`UpdateRecord`/`ReplaceText`) до отправки во внешние системы |

##### Альтернативы и сочетание

| Инструмент | Когда лучше |
|---|---|
| Prometheus REST endpoint / `PrometheusReportingTask` | Стандартный стек метрик (Prometheus + Grafana + Alertmanager); минимальная нагрузка; метрики узла/кластера/JVM/процессоров |
| Site-to-Site Reporting Tasks | Нужна обработка данных мониторинга **внутри NiFi**, доставка в Splunk/Kafka/БД, bulletins с текстом причин |
| REST API (`/nifi-api/flow/...`) | Одноразовые проверки, скрипты оркестрации |
| Логи (`nifi-app.log`) в SIEM | Детали исключений, форензика |

Практический совет: **сочетайте** — Prometheus для метрик и алертов по ресурсам, Site-to-Site Bulletin Reporting для текстов ошибок и маршрутизации уведомлений, Status — для долгосрочной статистики по компонентам.

##### Mini-lab: мониторинг NiFi средствами NiFi

**Цель.** Отправить статус и bulletins в Input Port, построить два алерта и сохранить записи в файл/Splunk-эмулятор.

**Шаги.**

1. На том же NiFi создайте Input Port `monitoring-in` (корневая группа) и flow: `monitoring-in ► QueryRecord (alerts) ► LogAttribute`, параллельно `monitoring-in ► MergeRecord (NDJSON) ► PutFile /opt/nifi/data/monitoring` (на хосте стенда — `nifi/data/monitoring`).
2. Создайте `JsonTreeReader` (Infer Schema — только для лаборатории) и `JsonRecordSetWriter` (One Line Per Object).
3. Создайте `SiteToSiteBulletinReportingTask` и `SiteToSiteStatusReportingTask` с Destination URL на этот же экземпляр; выдайте политики (*retrieve site-to-site details*, *receive data via site-to-site*) идентичности NiFi.
4. Намеренно вызовите ошибку: поместите в `InvokeHTTP` неверный URL — появится bulletin ERROR; создайте переполнение очереди — back pressure.
5. Проверьте: `QueryRecord` пропускает bulletin ERROR и записи с `isBackPressureEnabled = 'true'`; в `nifi/data/monitoring` накапливается NDJSON.

> На стенде (Приложение A) Destination URL — `https://nifi:8443/nifi` (single) или `https://nifi1:8443/nifi` (cluster); узлы работают по TLS с общим CA (`nifi/certs`). Идентичность узла для политик S2S — DN сертификата (`CN=nifi, OU=NIFI` / `CN=nifiN, OU=NIFI`). Мини-лабораторная на стенде не проверялась (**[проверить]**).
6. Исключите петлю: ограничьте фильтры Status так, чтобы flow `monitoring` не попадал в отчёты (по имени), и сравните объём до/после.

**Проверка.** Алерты формируются в течение 1–2 интервалов запуска; нет роста очереди на `monitoring-in`; при остановке получателя отправитель логирует ошибку (bulletin на задаче), после возобновления потоки восстанавливаются.

**Типичные ошибки.** Забыли запустить Input Port; не выданы политики; `Instance URL` оставлен по умолчанию; Reader не соответствует схеме записей Status/Bulletin (пустой результат `QueryRecord`).


### 3.2.2 Мониторинг и автономное восстановление: уменьшение ручного контроля

1. **Alerting вместо наблюдения за холстом.** Prometheus/Alertmanager (или аналог) по метрикам выше; `MonitorActivity` (alert о «тишине» источника); bulletins → Site-to-Site → отдельный flow, рассылающий уведомления (`PutEmail`, `InvokeHTTP` в чат/PagerDuty).
2. **Самовосстановление:** retry на уровне фреймворка/`RetryFlowFile`; автоматический повторный запуск JVM; DLQ+replay; kubernetes liveness/readiness.
3. **Ограничение ущерба:** back pressure на всех связях; лимиты по месту на диске (archive Content Repository ограничивается по возрасту и проценту использования); `FlowFile Expiration` там, где потеря допустима.
4. **Runbook-и.** Для типичных сигналов — формализованные действия (что смотреть, какие решения принимать).
5. **Flow Analysis Rules** (API введён в 2.0): предотвращают плохие практики на стадии проектирования (например, запрет автотерминации `failure`), уменьшая число инцидентов эксплуатации (набор встроенных правил — в разделе 3.2.6).

### 3.2.3 Типичные проблемы и расследование первопричины

#### Память

| Симптом | Вероятные причины | Диагностика | Действия |
|---|---|---|---|
| `OutOfMemoryError: Java heap space` | Миллионы мелких FlowFile (метаданные в heap); крупные атрибуты (контент в атрибутах); `MergeContent`/`MergeRecord` с огромными bin; Reader/Writer, накапливающий данные; `EvaluateJsonPath`/`ReplaceText` на огромном FlowFile целиком; утечки в кастомных компонентах | System Diagnostics; `bin/nifi.sh dump`/`diagnostics`; heap dump (`-XX:+HeapDumpOnOutOfMemoryError`); Summary по числу FlowFile | Record-подход; меньшие bin/пачки; увеличить heap с проверкой GC; ограничить размер атрибутов; swap/пороги очередей |
| Долгие паузы GC | Слишком большой/малый heap, G1 при больших live-set | GC-логи; графики GC в Status History | Настроить размеры; уменьшить число FlowFile; поэкспериментировать с параметрами G1 |
| `unable to create native thread` | Слишком много потоков/лимиты ОС | `ulimit -u`, число потоков | Уменьшить concurrent tasks; увеличить лимиты |

#### Очереди

| Симптом | Причина | Решение |
|---|---|---|
| Очередь растёт перед процессором | Недостаточно concurrent tasks/ресурсов; медленный приёмник | Масштабировать (потоки, узлы); искать узкое место по Tasks/Time; ограничить источник |
| Процессор «не запускается» при свободных потоках | Заполнена одна из исходящих связей; процессор Primary Node Only на неактивном узле; ресурсы; invalid | Проверить связи (красный индикатор), validation errors |
| Все FlowFile «застряли» | Оштрафованы (penalty) | Проверить индикатор penalized; подождать/исправить причину |
| Рост swap-файлов | Очереди > `nifi.queue.swap.threshold` | Усилить приёмник/агрегацию |
| Данные «пропали» | `FlowFile Expiration`; auto-terminate; `DROP` в provenance | Provenance по `uuid` |

#### Ошибки процессоров

1. Откройте bulletin (иконка на процессоре); посмотрите stack trace (с 2.7.0 есть в UI).
2. Найдите в `nifi-app.log` по времени/`uuid`/имени процессора; обратите внимание на `Caused by`.
3. Проверьте провенанс FlowFile, у которого произошла ошибка: какие атрибуты/контент были на входе.
4. Воспроизведите на копии: сохраните проблемный FlowFile (через provenance → Download), прогоните в тестовой цепочке (или используйте `Run Once`).
5. Классифицируйте: данные / конфигурация / внешняя система / ресурсы NiFi.

**Поток расследования (RCA).**

```text
Симптом (алерт) → Где? (Summary: какой процессор/очередь) → Что? (bulletin, log) →
Почему? (provenance + атрибуты, метрики ресурсов, внешняя система) → Исправление →
Предотвращение (алерт, правило анализа, тест, runbook)
```

### 3.2.4 Стратегии восстановления: restart и failover

- **Restart процессора/группы:** Stop → проверить, что нет активных потоков → Start; для Controller Service — Disable → Enable.
- **Restart узла:** корректная остановка; после старта проверка валидности и очередей. Данные в репозиториях восстановятся из WAL.
- **Failover.** Кластер продолжает работать при отказе узла (при сохранении кворума ZooKeeper). Перевыбираются Coordinator и Primary Node; процессоры *Primary Node Only* стартуют на новом primary (state — в cluster state). Данные, находившиеся на упавшем узле, ждут его возврата; для быстрого failover «с данными» — хранилище, которое можно подключить к другому узлу (сетевой диск/PV в Kubernetes) и **идентичность узла**, либо допускается повторная загрузка из источников (Kafka offset не сдвинулся, т. к. commit произошёл после обработки).
- **Disconnected node.** С 1.16.0 узел может вернуться в кластер, синхронизировав flow (с резервной копией своего); кластер допускает изменения flow при отключённых узлах.
- **Восстановление из backup** — Block 3.1.5.

### 3.2.5 Flow rollback: возврат flow к предыдущей версии

1. **Через Registry (Git).** Process Group → **Version → Change version** → выбрать предыдущую; NiFi остановит затронутые компоненты и применит. Для проверки — **Show local changes** до/после.
2. **Откат локальных правок** — **Revert local changes**.
3. **Архив конфигурации.** `conf/archive/flow.json.gz.<timestamp>` — восстановление всего flow на узле/кластере при остановленном NiFi (учитывайте параметры/sensitive key).
4. **Что откат не вернёт:** данные в очередях (останутся и будут обработаны уже откаченным flow), state компонентов (если схема state изменилась), внешние побочные эффекты.
5. **Правило безопасности:** перед откатом остановите источники и дайте очередям опустеть, если новая и старая версия по-разному обрабатывают «промежуточные» FlowFile.

### 3.2.6 NiFi Settings: назначение вкладок General, Management Controller Services, Reporting Tasks, Flow Analysis Rules, Registry Clients, Parameter Providers

Раздел **NiFi Settings** (в 1.x — *Controller Settings*; меню в правом верхнем углу → *Settings*) содержит настройки **уровня всего экземпляра/кластера**, а не конкретной Process Group. Здесь живут компоненты, которые не принадлежат ни одному flow, но обслуживают весь NiFi: мониторинг, контроль качества дизайна, версионирование и источники параметров. Изменения реплицируются на все узлы кластера, а сами компоненты сохраняются в конфигурации flow (**[проверить]** для вашей версии). Для доступа нужны глобальные права (политика *access the controller*; Block 3.3).

```text
NiFi Settings
├── General                          ← глобальные параметры работы (пулы потоков и т. п.)
├── Management Controller Services   ← общие сервисы для компонентов этого раздела
├── Reporting Tasks                  ← периодические задачи: метрики, мониторинг, отправка статусов
├── Flow Analysis Rules              ← правила проверки качества дизайна flow
├── Registry Clients                 ← подключения к хранилищам версий flow (Git, NiFi Registry)
└── Parameter Providers              ← загрузка параметров из внешних источников (секреты, конфигурация)
```

#### General

**Назначение.** Глобальные настройки поведения узла.

| Параметр | Смысл |
|---|---|
| **Maximum Timer Driven Thread Count** | Размер общего пула потоков, из которого выполняются все процессоры и службы узла (Block 3.4). Это самый важный параметр раздела: слишком малое значение — простаивающие ядра и растущие очереди, слишком большое — конкуренция за CPU и диск |
| Maximum Event Driven Thread Count | Только в 1.x; в 2.x event-driven планирование и этот пул удалены |

**Чего здесь нет.** Значения по умолчанию для новых соединений (back pressure: 10 000 FlowFile и 1 GB) задаются **не на этой вкладке**: глобально — свойствами `nifi.queue.backpressure.count` и `nifi.queue.backpressure.size` в `nifi.properties` ([Administration Guide, Core Properties](https://docs.cloudera.com/cfm/2.1.2/nifi-admin-guide/topics/nifi-core-properties-br.html) — зеркало документации Apache), а на уровне группы — в настройках Process Group (*Default FlowFile Expiration*, *Default Back Pressure Object/Data Size Threshold*). Других обязательных полей, кроме пула потоков, источники не подтверждают; если ваш UI показывает дополнительные поля, ориентируйтесь на справку в самом интерфейсе.

**Особенности.** Параметр применяется **на каждом узле** кластера отдельно (общее число потоков в кластере = значение × число узлов). Event-driven пул в 2.x удалён, поэтому остаётся один пул. Изменение вступает в силу сразу, без перезапуска.

#### Management Controller Services

**Назначение.** Controller Services **уровня контроллера**: общие сервисы, которые используют компоненты этого же раздела (Reporting Tasks, Flow Analysis Rules, Registry Clients, Parameter Providers). Это отличает их от сервисов, создаваемых в Process Group: **процессоры в flow эти сервисы не видят**, а сервисы Process Group недоступны reporting tasks.

**Примеры.**

| Сервис | Для чего здесь |
|---|---|
| `StandardSSLContextService` (или PEM-вариант) | TLS для `SiteToSite*ReportingTask`, клиентов реестра и провайдеров параметров |
| `JsonRecordSetWriter` / другой Record Writer | Формат записей, которые отправляют Site-to-Site reporting tasks (Block 3.2.1) |
| `StandardProxyConfigurationService` | Доступ к внешним сервисам через прокси |
| Сервисы учётных данных (AWS Credentials, GCP Credentials, Kerberos) | Аутентификация провайдеров параметров и reporting tasks во внешних системах |

**Колонки таблицы на скриншоте:** *Name* (имя), *Type* (класс сервиса), *Bundle* (NAR, из которого он загружен), *State* (Enabled/Disabled/Invalid и т. п.), *Scope* (область действия). Кнопка **«+»** создаёт сервис. Порядок работы тот же, что в Process Group: создать → настроить → **Enable**. Пустая таблица означает, что управляющие сервисы ещё не создавались.

**Рекомендации.** Заводите отдельные сервисы для мониторинга (не переиспользуйте сервисы потоков данных); давайте им понятные имена; следите, чтобы сервис был включён раньше, чем запускается зависимая задача.

#### Reporting Tasks

**Назначение.** Задачи, которые NiFi запускает **периодически на уровне контроллера**, вне потока данных: сбор и экспорт метрик, проверка ресурсов, отправка статусов и bulletins. Подробно Site-to-Site задачи разобраны в подразделе «Site-to-Site Reporting Tasks: подробно» выше.

| Задача | Что делает |
|---|---|
| `PrometheusReportingTask` | Публикует метрики в формате Prometheus (альтернатива REST-эндпоинту метрик, Block 3.2.1) |
| `SiteToSiteStatusReportingTask`, `SiteToSiteBulletinReportingTask`, `SiteToSiteMetricsReportingTask` | Отправляют статусы компонентов, bulletins и метрики в Input Port другого (или того же) NiFi |
| `MonitorDiskUsage` | Контролирует заполнение каталогов (репозитории) и пишет предупреждения при превышении порога |
| `MonitorMemory` | Следит за использованием пулов памяти JVM и сигнализирует о превышении порога |
| `ControllerStatusReportingTask` | Периодически записывает сводку статуса контроллера и компонентов в журнал |

**Настройка.** У каждой задачи есть *Scheduling Strategy* (Timer driven или CRON), *Run Schedule* (периодичность), состояние (Running/Stopped) и свойства; периодичность по умолчанию для reporting tasks — порядка 5 минут (так указано в [NIFI-3878](https://issues.apache.org/jira/browse/NIFI-3878), где её меняли на 1 минуту для одной конкретной задачи); у процессоров есть настройка *Execution* (*All Nodes* / *Primary Node*, [NIFI-401](https://www.mail-archive.com/commits@nifi.apache.org/msg19852.html)), но для reporting tasks наличие такого переключателя в вашей версии подтвердить не удалось — по умолчанию считайте, что задача работает **на каждом узле** (в записях видно, с какого узла они пришли) (**[проверить]** возможность выполнения только на Primary Node). Результат сообщается через bulletins самой задачи, поэтому смотрите их при проблемах.

**Практика.** Минимальный набор для production: `MonitorDiskUsage` для томов репозиториев, `MonitorMemory`, экспорт метрик (Prometheus или S2S), отправка bulletins; пороги и расписание подбирайте так, чтобы не создавать шум.

#### Flow Analysis Rules

**Назначение.** **Правила автоматической проверки дизайна flow** (governance): они анализируют конфигурацию компонентов и Process Group и находят нарушения стандартов (например, запрещённые типы процессоров или небезопасные настройки). API введён в 2.0.0-M1 ([Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).

**Как работают.**

1. Администратор создаёт правило на вкладке *Flow Analysis Rules*, настраивает его свойства и **включает**.
2. При изменении flow NiFi анализирует затронутые компоненты и группы; найденные **нарушения** показываются в UI у соответствующих компонентов.
3. У правила есть **политика применения** (*Enforcement Policy*): *Warn* — компонент с нарушением **можно запускать**, нарушение только попадает в отчёт о нарушениях (Rules Violations report, доступен с холста); *Enforce* — компонент с нарушением помечается **Invalid** и **не может быть запущен**, даже если настроен правильно; нарушение также видно в отчёте ([пост по Flow Analysis Rules](https://community.cloudera.com/t5/Developer-Blogs/Flow-Analysis-Rules-enforcing-Best-Practices-in-Flow-Design/m-p/413017) от разработчика NiFi).

**Встроенные правила.** В Apache NiFi 2.6.0 доступны четыре ([источник](https://community.cloudera.com:443/users/641/mburgess.html)):

| Правило | Что проверяет |
|---|---|
| `DisallowComponentType` | Запрещает использовать указанный тип компонента (например, `ExecuteScript`, если скрипты запрещены политикой) |
| `RestrictBackpressureSettings` | Ограничивает значения back pressure на соединениях |
| `RequireServerSSLContextService` | Требует SSL Context Service у серверных компонентов |
| `RestrictFlowFileExpiration` | Ограничивает значения FlowFile Expiration |

В более новых сборках (по данным Cloudera, основанным на Apache NiFi 2.x) набор шире, например `DisallowDeadEnd`, `RecommendRecordProcessor`, `RestrictComponentNaming`, `RestrictProcessorConcurrency`, `RestrictThreadPoolSize`, `RequireHandleHttpResponseAfterHandleHttpRequest` ([список](https://docs.cloudera.com/cfm/latest/release-notes/topics/cfm-supported-nifi-flow-analysis-rules.html)); какие из них входят в сам Apache NiFi вашей версии, смотрите в списке при создании правила (**[проверить]**). Правило создаётся и настраивается из меню NiFi (раздел Settings) на вкладке *Flow Analysis Rules*.

**Зачем это в эксплуатации.** Правила переносят требования «как проектировать flow» из документа в автоматическую проверку: меньше ошибок в production (например, автотерминация `failure`, небезопасные настройки), единые стандарты для разных команд. Можно писать собственные правила как расширения.

#### Registry Clients

**Назначение.** Подключения NiFi к **хранилищам версий flow** — основа версионирования Process Group (Block 1.4.6). Через клиент вы выполняете *Version → Start version control / Commit / Change version*.

| Клиент | Хранилище | Статус |
|---|---|---|
| NiFi Registry | Отдельный сервис NiFi Registry | **[dep]** с 2.8.0 |
| GitHub | Репозиторий GitHub (ветки и подкаталоги) | 2.x |
| GitLab | Репозиторий GitLab | 2.x |
| Bitbucket | Репозиторий Bitbucket | 2.x |
| Azure DevOps | Git-репозиторий Azure DevOps | 2.x |

**Настройка GitHub-клиента** (по исходному коду реализации, [PR #8765](https://www.mail-archive.com/issues@nifi.apache.org/msg173690.html); в более поздних версиях набор может быть шире):

| Свойство | Смысл |
|---|---|
| GitHub API URL | Адрес API (по умолчанию `https://api.github.com/`; для GitHub Enterprise — свой) |
| Repository Owner / Repository Name | Владелец и имя репозитория |
| Default Branch | Ветка по умолчанию (`main`) |
| Repository Path | Подкаталог для хранения flow; пустое значение — корень репозитория; **без `/` в начале и в конце** (иначе ошибка валидации) |
| Authentication Type | `NONE` (по умолчанию), `PERSONAL_ACCESS_TOKEN` или `APP_INSTALLATION_TOKEN` |
| Personal Access Token / App Installation Token | Секрет (sensitive); поле зависит от выбранного типа аутентификации |

Для GitLab, Bitbucket и Azure DevOps имена свойств другие (**[проверить]** в документации клиента). Если клиенту нужен TLS или прокси, используйте сервисы из *Management Controller Services*.

**Практика.** Отдельный репозиторий/ветка на окружение или единый репозиторий с promotion между ветками; токен с минимальными правами; не коммитить секреты (параметры sensitive не должны попадать в Git); проверять, что у пользователей есть права на создание и фиксацию версий.

#### Parameter Providers

**Назначение.** Загрузка значений для **Parameter Contexts из внешних источников** (хранилища секретов, файлы, переменные окружения, БД). Введены в 1.18.0. Позволяют не хранить пароли и токены в flow.

**Как работает.**

1. Создайте провайдера на вкладке *Parameter Providers* и настройте источник (адрес хранилища, аутентификация — часто через сервис из *Management Controller Services*).
2. **Fetch Parameters**: NiFi запрашивает параметры у источника. Они сгруппированы в **Parameter Groups**; для каждой группы нужно указать, какие параметры **sensitive**.
3. **Apply**: группа превращается в Parameter Context (или обновляет существующий), помеченный как «управляемый провайдером». Значения таких параметров редактируются только в источнике.
4. Дальше параметры используются как обычно: `#{db.password}` в свойствах процессоров и сервисов.

**Доступные провайдеры** (по спискам поставляемых в составе Apache NiFi провайдеров из [документации Cloudera](https://docs.cloudera.com/cfm/latest/release-notes/topics/cfm-supported-parameter-providers.html)):

| Провайдер | Источник |
|---|---|
| `EnvironmentVariableParameterProvider` | Переменные окружения |
| `FileParameterProvider`, `PropertiesFileParameterProvider` | Файлы (каждый файл — параметр) и `.properties`-файлы |
| `DatabaseParameterProvider` | Таблица в БД через JDBC |
| `HashiCorpVaultParameterProvider` | HashiCorp Vault |
| `AwsSecretsManagerParameterProvider` | AWS Secrets Manager |
| `GcpSecretManagerParameterProvider` | GCP Secret Manager |
| `AzureKeyVaultSecretsParameterProvider` | Azure Key Vault |
| `CyberArkConjurParameterProvider`, `OnePasswordParameterProvider` | CyberArk Conjur, 1Password |
| `KubernetesSecretParameterProvider` | Kubernetes Secrets (в новых сборках 2.x) |

Набор зависит от версии (в 1.18 их было шесть, в более новых 2.x — больше); окончательный список смотрите в диалоге создания провайдера. Провайдеры создаются из меню NiFi в разделе Settings, как и остальные глобальные компоненты ([описание](https://community.cloudera.com:443/users/641/mburgess.html)).

**Важные нюансы.** Загрузка выполняется **по запросу** (Fetch/Apply), а не непрерывно: после ротации секрета нужно обновить контекст (вручную или автоматизируя через REST API). Значения из внешних источников не должны попадать в Git при версионировании flow. Права на Parameter Providers — только у администраторов.

#### Как компоненты работают вместе (сценарии)

| Сценарий | Какие вкладки задействованы |
|---|---|
| **Мониторинг в Splunk** | *Management Controller Services* (SSL Context + Record Writer) → *Reporting Tasks* (`SiteToSite*`) → flow-приёмник доставляет в Splunk |
| **Безопасные секреты** | *Management Controller Services* (учётные данные облака) → *Parameter Providers* (Vault/Secrets Manager) → Parameter Context в flow |
| **Контроль качества и релизы** | *Flow Analysis Rules* проверяют дизайн → *Registry Clients* фиксируют версию в Git → promotion по окружениям |
| **Тюнинг производительности** | *General* (Timer Driven Thread Count) + метрики из *Reporting Tasks* для проверки эффекта |

#### Резервное копирование и аудит

Состав раздела — часть конфигурации NiFi: входит в `flow.json.gz` и в backup (Block 3.1.5); изменения фиксируются в *Flow Configuration History* (Block 3.3.3). При восстановлении из backup на другом окружении проверьте токены реестров, адреса приёмников reporting tasks и источники провайдеров параметров: они часто окружение-специфичны.

---

## Block 3.3: Security and governance

### 3.3.1 TLS и аутентификация

**TLS по умолчанию.** С 1.14.0 NiFi стартует в защищённом режиме: HTTPS (порт 8443), автосгенерированный самоподписанный сертификат и случайные учётные данные Single User, выводимые в лог ([Release Notes 1.14.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes), [Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)). Для production замените его сертификатом корпоративного PKI.

**Конфигурация TLS (`nifi.properties`).**

```properties
nifi.web.https.host=nifi-1.example.com
nifi.web.https.port=8443
nifi.security.keystore=./conf/keystore.p12
nifi.security.keystoreType=PKCS12
nifi.security.keystorePasswd=<секрет>
nifi.security.keyPasswd=<секрет>
nifi.security.truststore=./conf/truststore.p12
nifi.security.truststoreType=PKCS12
nifi.security.truststorePasswd=<секрет>
```

Особенности 2.x:

- Поддерживается конфигурация **PEM-кодированными** ключами и сертификатами для фреймворка (с 2.1.0) и для расширений (`PEMEncodedSSLContextProvider`) ([Release Notes 2.1.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).
- Keystore/truststore Jetty могут **автоматически перезагружаться** при изменении файлов (с 1.14.0; в 2.0.0-M4 улучшена динамическая перезагрузка для кластерного взаимодействия).
- **PKCS8** обязателен для PEM-ключей `StandardPrivateKeyService` (2.13.0).
- Сертификаты узлов должны содержать SAN, соответствующий имени хоста; wildcard-сертификаты официально не поддерживаются (из истории 1.7.x); с 2.12.0 в TLS-кластере нужны DNS-имена в свойствах кластера.
- Удалено: OCSP-валидация клиентских сертификатов (2.7.0).

**Способы аутентификации пользователей.**

| Способ | Примечания |
|---|---|
| **Client certificate (mTLS)** | Идентичность из DN сертификата; удобно для сервисных клиентов; обязательно между узлами кластера |
| **Single User** (`SingleUserLoginIdentityProvider`) | Только для разработки/лаборатории |
| **LDAP / Active Directory** | `LdapProvider` в `login-identity-providers.xml`; с 2.x доступны также провайдеры групп (в т. ч. Azure AD, с 1.14.0) |
| **OpenID Connect (OIDC)** | SSO; поддерживает Authorization Code (с PKCE, 2.0.0-M3) и Client Credentials (для программного доступа) |
| **SAML** | SSO |
| **Kerberos Login Identity Provider** | **[dep]** помечен к удалению начиная с 2.7.0 |
| ~~Knox SSO~~ / ~~Kerberos SPNEGO (REST)~~ | **[rem 2.0.0-M4]** |

(Источник: [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes).)

**Single User в Docker/лаборатории:** `bin/nifi.sh set-single-user-credentials <user> <password>`.

### 3.3.2 Контроль доступа: пользователи, группы, политики, ролевая модель

**Модель авторизации (multi-tenant authorization).** Права задаются **политиками (Access Policies)** — парой *(ресурс, действие)* и списком пользователей/групп. Роли как таковых нет: «роль» реализуется **группой** (например, `nifi-operators`, `nifi-developers`) с набором политик. С 1.0.0 заменила прежнюю роль-ориентированную модель ([Migration Guidance 0.x→1.0.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).

Конфигурация — `authorizers.xml`, состоит из трёх уровней:

1. **User Group Provider** — откуда берутся пользователи/группы (`FileUserGroupProvider`, `LdapUserGroupProvider`, Azure, Composite…).
2. **Access Policy Provider** — где хранятся политики (`FileAccessPolicyProvider`) и кто начальный администратор (*Initial Admin Identity*).
3. **Authorizer** — `StandardManagedAuthorizer`, использующий провайдеры выше.

В 2.x ранее поддерживавшиеся внешние механизмы **Apache Ranger plugin удалён** (2.0.0-M3), **Shell User Group Provider удалён** (2.0.0).

**Типы политик.**

| Уровень | Примеры | Действия |
|---|---|---|
| Глобальные (controller) | access the controller, access parameter contexts, query provenance, access all policies, access system diagnostics, proxy user requests, retrieve site-to-site details, view counters | View/Modify |
| Компонентные | view/modify the component; **view/modify the data** (доступ к атрибутам и контенту FlowFile через очередь/Provenance); **view provenance** (с 1.7.0 — отдельная политика просмотра событий без доступа к контенту); operate the component (start/stop); view/modify the policies | Применяются к Process Group и наследуются вложенными компонентами |

**Принципы проектирования доступа.**

1. **Минимум привилегий.** Операторам — *operate* + *view*; разработчикам — *modify* в своих Process Group; администраторам — управление политиками.
2. **Разделение данных и метаданных.** *view provenance* без *view the data* позволяет понимать flow без доступа к содержимому (важно для PII).
3. **Группы вместо отдельных пользователей;** политика на уровне Process Group (наследование) вместо политик на каждый компонент.
4. **Parameter Contexts:** отдельные права на *access parameter contexts*; sensitive-параметры не отображаются.
5. **Прокси-пользователи:** при reverse proxy/балансировщике — политика `proxy user requests` для идентичности прокси, корректная передача `X-ProxiedEntitiesChain` (формат с обрамляющими `<>`, [Migration Guidance 1.13](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).

**Изменения в 2.x по авторизации (важно для долгосрочного проектирования).**

- **2.10.0 [rem]:** удалена фреймворковая авторизация «restricted components / required permissions» (NIFI-24) и аннотации Restricted у компонентов; ограничения теперь реализуются иными средствами (политики на Process Group, процессные ограничения) ([Migration Guidance 2.10.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).
- **2.12.0 [dep]:** **fine-grained авторизация на уровне Process Group и компонентов объявлена deprecated для удаления** (NIP-39), как и `AbstractPolicyBasedAuthorizer` ([Release Notes 2.12.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes), [Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)). Это означает, что при проектировании новой модели доступа не стоит строить сложную иерархию политик на уровне отдельных компонентов; **следите за NIP-39** и планируйте более грубую модель (на уровне окружений/потоков) и изоляцию через отдельные инстансы/кластеры/namespaces.
- **Проверьте конкретику** (сроки удаления, что именно остаётся) в NIP-39 перед архитектурными решениями.

### 3.3.3 Audit trails: отслеживание изменений конфигурации и доступа

| Механизм | Что фиксирует | Где |
|---|---|---|
| **Flow Configuration History** | Кто, когда и что изменил (добавление/удаление/настройка компонентов, запуск/остановка) | UI: Global menu → *Flow Configuration History*; хранится в `database_repository` (JetBrains Xodus с 1.24.0; ранее H2) |
| `nifi-user.log` | Аутентификация, запросы пользователей, отказ в авторизации | `logs/` |
| `nifi-request.log` | HTTP-запросы (с 1.16.0) | `logs/` |
| **Data Provenance** | Происхождение данных (какие компоненты обработали FlowFile) | UI и API |
| Git history (Registry) | Версии flow и авторы коммитов | Git |
| **Flow Action Reporter** | Расширяемая интеграция для отправки действий по изменению flow во внешние системы (добавлен в 2.4.0) | [Release Notes 2.4.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes) |

Рекомендации: централизуйте логи (SIEM); задайте срок хранения истории изменений в соответствии с политикой аудита; сопоставляйте идентичности из mTLS/OIDC с человекочитаемыми именами; отдельно аудируйте административные операции (изменения политик).

### 3.3.4 Шифрование: sensitive properties и data at rest

**Sensitive properties.** Значения свойств, помеченных как *sensitive* (пароли, токены), шифруются в `flow.json.gz` ключом, производным от `nifi.sensitive.props.key`. Настройки: `nifi.sensitive.props.key`, `nifi.sensitive.props.algorithm` (по умолчанию в поставляемой конфигурации 2.x — `NIFI_PBKDF2_AES_GCM_256`). Правила:

- Ключ **обязателен** и должен быть **одинаковым на всех узлах кластера**; смена — через `bin/nifi.sh set-sensitive-properties-key <new key>` (который перешифрует конфигурацию) ([Migration Guidance 1.13→1.14](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).
- Потеря ключа = невозможность расшифровать sensitive-значения; их придётся ввести заново.
- Предпочтительно **не хранить секреты в flow вообще:** **Parameter Providers** (HashiCorp Vault, AWS/GCP Secrets Manager, Kubernetes Secrets — с 1.18.0/2.0) подгружают параметры во время работы. В 2.12.0 добавлена расширяемая абстракция шифрования свойств (**PropertyEncryptionProvider**).
- В 2.x удалены: «application property protection» и `encrypt-config` (2.0.0-M4) — то есть шифрование значений в `nifi.properties` средствами `encrypt-config` недоступно; используйте внешние секрет-хранилища/переменные окружения и права файловой системы ([Release Notes 2.0.0-M4](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)). **[проверить]** актуальные штатные альтернативы для защиты `nifi.properties` в вашей версии.
- Динамические свойства могут быть помечены пользователем как sensitive (с 1.17.0).

**Data at rest.**

- **[rem 2.0.0]** Встроенное шифрование репозиториев (FlowFile/Content/Provenance) **удалено**. Следовательно, защита данных «на диске» в 2.x обеспечивается на уровне инфраструктуры: **шифрование файловой системы/тома** (LUKS/dm-crypt, шифрование облачных дисков, KMS), контроль доступа к ОС и бэкапам.
- Шифрование отдельных данных внутри flow: процессоры `EncryptContentAge`/`DecryptContentAge` (age), `EncryptContentPGP`/`DecryptContentPGP`; обратите внимание: `DecryptContent` и `DecryptContentCompatibility` удалены в 2.0.0.
- **Data in motion:** TLS на всех границах (UI/API, кластер, Site-to-Site, Kafka, JDBC, HTTP).
- **PII:** маскирование/удаление до логирования; ограничение прав *view the data*; осторожность с `LogAttribute`/`LogMessage`.

### Governance: практический чек-лист

1. TLS везде; нет Single User в production.
2. Группы и политики документированы; регулярный пересмотр доступа.
3. Секреты — в Parameter Providers/Vault; `sensitive.props.key` защищён и бэкапнут отдельно.
4. Шифрование дисков на уровне ОС; закрытые каталоги репозиториев.
5. Аудит: история изменений + логи + Git; экспорт в SIEM.
6. Процесс изменений: PR/review в Git, тестовое окружение, запрет прямых правок на prod.
7. Политика обновлений: следить за [Security Advisories](https://nifi.apache.org/security.html) и Release Notes.

---

## Block 3.4: Performance tuning, hardware and storage

### 3.4.1 JVM, heap, thread pools, concurrent tasks, параметры кластера

**JVM (`conf/bootstrap.conf`).**

```text
java.arg.2=-Xms8g
java.arg.3=-Xmx8g
# Сборщик по умолчанию - G1; параметры менять только по результатам анализа GC-логов
java.arg.XX...=-XX:+HeapDumpOnOutOfMemoryError
```

- Дефолтный heap в поставке мал (порядка 512 МБ) и предназначен для знакомства. Для production начинайте с ~4–16 ГБ и корректируйте по GC и объёму метаданных FlowFile. Устанавливайте `Xms = Xmx` (без динамического изменения размера).
- Основной потребитель heap — **метаданные FlowFile (атрибуты)** и записи в очередях, а не контент (он на диске). Поэтому heap растёт с **числом** FlowFile, а не с объёмом данных.
- Не выделяйте всю RAM под heap: Content Repository и Provenance активно используют **page cache ОС**; оставьте RAM на кэш.
- Включите GC-логирование, анализируйте паузы; рост heap «ради паузы» часто маскирует проблему слишком большого числа FlowFile.

**Пулы потоков.**

- **Maximum Timer Driven Thread Count** (Global menu → *Controller Settings*): общий пул для всех компонентов узла (по умолчанию `10` на каждый узел; для кластера из трёх узлов это 30 потоков в сумме). Общая рекомендация разработчиков NiFi — начинать с **2–4 потоков на ядро** и корректировать по загрузке CPU: если load average приближается к числу ядер или превышает его, пул нужно уменьшать ([разбор на community.cloudera.com](https://community.cloudera.com/t5/Community-Articles/Understanding-NiFi-max-thread-pools-and-processor-concurrent/m-p/248920)); это общепринятый ориентир из сообщества, а не нормативное значение Administration Guide. Слишком малое значение — простаивающие ядра и очереди; слишком большое — переключение контекста и конкуренция за диск.
- Пул *event-driven* удалён в 2.0 — настраивается только Timer-driven.
- В кластере настройка применяется **к каждому узлу**.

**Concurrent Tasks (на процессоре).**

- Определяет, сколько потоков одновременно может выполнять *один* процессор (по умолчанию 1). Увеличивайте для процессоров, ограниченных ожиданием I/O или CPU, при наличии нескольких FlowFile в очереди.
- Общая нагрузка ограничена пулом: сумма активных задач ≤ Maximum Timer Driven Thread Count.
- Для источников (`ConsumeKafka`, `ListenX`) — число задач ≈ число партиций/каналов.
- Не увеличивайте для процессоров, зависящих от порядка/единственного ресурса.

**Run Duration.** Параметр (0 мс … 2 с) позволяет процессору в одном «запуске» обработать несколько FlowFile в одной сессии → меньше накладных расходов на commit в репозиторий; выше throughput, но выше latency и больше «в полёте» при сбое. Подходит для процессоров, помеченных как поддерживающие batching (в UI слайдер *Run Duration*).

**Scheduling.** *Run Schedule* (`0 sec` — как можно чаще; для источников установите разумный интервал, чтобы не молотить внешние системы), *Penalty Duration* (`30 sec` по умолчанию), *Yield Duration* (`1 sec` по умолчанию).

**Параметры кластера.**

- `nifi.cluster.node.protocol.threads` (начальный размер) и `nifi.cluster.node.protocol.max.threads` (макс. размер пула репликации запросов, по умолчанию 50) — для кластеров с большим числом узлов/компонентов ([Migration Guidance 1.3.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).
- Балансировка между узлами ([Cluster Node Properties](https://docs.cloudera.com/cfm/2.1.2/nifi-admin-guide/topics/nifi-cluster_node_properties.html)): `nifi.cluster.load.balance.connections.per.node` (по документации `4`; сколько сокетов открывать к каждому узлу) и `nifi.cluster.load.balance.max.thread.count` (по документации `8`; сколько потоков передают данные другим узлам). Увеличение даёт эффект, пока значение не превысит число узлов × число соединений на узел; дополнительные потоки нагружают запись в Content и FlowFile Repository. Таймаут обмена — `nifi.cluster.load.balance.comms.timeout` (`30 sec`).
- Размер ансамбля ZooKeeper (3/5), латентность до ZK; частые изменения state нагружают ZK.
- Время heartbeat/таймауты: слишком агрессивные значения приводят к ложным disconnect при GC-паузах.

**Репозитории.**

- `nifi.content.claim.max.appendable.size` — размер claim, в который дописываются мелкие FlowFile (меньше файлов в ФС, лучше для мелких данных).
- `nifi.flowfile.repository.checkpoint.interval` — реже checkpoint = меньше нагрузка, больше время восстановления.
- `always.sync=true` — надёжнее, но существенно медленнее.
- `nifi.queue.swap.threshold` — порог swap.
- Provenance: `nifi.provenance.repository.max.storage.time/size`, `index.threads`, `rollover.time/size`, перечень `indexed.fields`/`indexed.attributes` — **индексируйте минимум**; чем больше индексируемых полей, тем выше нагрузка (CPU и I/O).
- В 2.10.0 добавлена частичная дефрагментация Content Repository (truncation tail-claim) — снижает потребление диска при долгоживущих claim ([Release Notes 2.10.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).

### 3.4.2 Масштабирование и выбор оборудования

**Больше ядер vs меньше, но быстрее.**

| Нагрузка | Предпочтение | Обоснование |
|---|---|---|
| Много независимых потоков/FlowFile, высокая параллельность (Kafka с множеством партиций, пакетная обработка) | **Больше ядер** | Throughput масштабируется числом одновременных задач (Timer-driven пул) |
| Узкое последовательное звено (один порядок/одно соединение, `Concurrent Tasks=1`, сложный разбор одного огромного FlowFile, TLS-шифрование одного потока) | **Более быстрые ядра** | Скорость одного потока ограничена тактовой частотой/IPC |
| Provenance indexing, сжатие, шифрование, парсинг JSON | Большее число ядер **плюс** достаточная частота | Это CPU-bound операции, но параллелятся |
| Смешанная | Баланс: 16–32 физических ядра на узел — типичная отправная точка, затем **масштабируйте горизонтально** | Узел с чрезмерным числом ядер упирается в диск/GC/heap раньше, чем используются все ядра |

Рекомендации: горизонтальное масштабирование (больше узлов) надёжнее, чем одна «огромная» машина (GC-паузы, диски, единая точка отказа). Но помните про цену: балансировка между узлами = сеть + запись в репозиторий принимающего узла.

**RAM.** Heap (см. выше) + page cache; для интенсивного Provenance Lucene-индексов нужен кэш ОС.

**Сеть.** 10 GbE для кластера с интенсивным load balancing; проверьте MTU, отсутствие пересчёта на NAT; TLS-накладные.

### 3.4.3 Структура хранилища и нагрузка Provenance Repository на I/O

**Паттерны нагрузки.**

| Репозиторий | Профиль I/O |
|---|---|
| FlowFile Repository | Малые синхронные записи WAL (latency-sensitive); при `always.sync=true` — fsync на каждый commit |
| Content Repository | Последовательные записи/чтения больших объёмов (throughput-sensitive); большое число файлов claim |
| Provenance Repository | Записи журналов + **индексация (Lucene)**: много мелких случайных операций, merge сегментов, поисковые чтения при запросах UI |
| `database_repository` | Мало |

**Вариант A: раздельные массивы/диски.**

```text
Диск/RAID1-10 №1: FlowFile Repository (низкая латентность, надёжность)
Диски №2..N     : Content Repository (несколько директорий `contentX` на разных устройствах)
Диск №M         : Provenance Repository (изолирует индексирование и поиск от основного потока)
ОС/логи         : отдельно
```

Плюсы: изоляция I/O (нагрузка provenance не влияет на WAL/контент), предсказуемость, возможность подобрать тип устройства под профиль. Минусы: сложнее, больше устройств, неравномерная загрузка.

**Вариант B: единый массив NVMe.**

Плюсы: очень высокие IOPS/throughput общего пула, простота, быстрые Lucene-merge. Минусы: **шумные соседи** — всплеск индексации/merge Provenance конкурирует с fsync WAL и записью контента; при заполнении тома одним репозиторием страдают все; сложнее наблюдать причину деградации.

**Как выбирать.**

1. Если можно — **отдельный том для Provenance**, даже при NVMe (логическое разделение, `quota`, отдельные метрики).
2. Если NVMe-массив имеет большой запас IOPS и вы замеряете отсутствие деградации WAL-latency — единый массив допустим.
3. Снижайте нагрузку Provenance: меньше срок хранения/размер, меньше индексируемых полей, `rollover` настроить, исключить из индексации крупные/бесполезные атрибуты; **не отключайте provenance целиком** без понимания последствий (потеря аудита и отладки) — в крайнем случае используйте `VolatileProvenanceRepository` (в памяти; события теряются при рестарте).
4. **Замеряйте** (`iostat -x`, `await`, `%util`, `fio`; метрики repo в System Diagnostics) до и после.
5. **Content Repository** распределяйте по нескольким директориям/устройствам; следите за `archive` (`max.retention.period`, `max.usage.percentage`): архив сохраняет удалённый контент для просмотра/Replay, но занимает место.

**Файловая система/ОС:** ext4/xfs; отключите `atime` (`noatime`); увеличьте лимиты файловых дескрипторов (`ulimit -n`) и процессов; заранее предусмотрите достаточное количество inode (множество мелких claim).

### 3.4.4 Снижение узких мест в дизайне flow, приоритизация, управление throughput

**Дизайн.**

1. **Record-подход** вместо Split (Block 1.3) — самое большое улучшение для CPU/heap/репозиториев.
2. **Меньше шагов, переписывающих контент:** объединяйте преобразования (один `QueryRecord`/`JoltTransformRecord` вместо цепочки `ReplaceText`→`ReplaceText`).
3. **Избегайте контента в атрибутах** (`EvaluateJsonPath` больших объектов, `ExtractText` больших значений).
4. **Крупные пачки на выходе** (MergeRecord) для приёмников, платящих за каждое соединение/запрос (БД, Splunk, HTTP).
5. **Параллелизм там, где узко:** Concurrent Tasks/load balancing по месту медленного звена, а не везде.
6. **Не используйте `Connection Per FlowFile`, `Wait/Notify` и т. п. на «горячих» путях без необходимости.**
7. **Минимум сложных регулярных выражений** на горячем пути; предпочитайте строгие парсеры.
8. **Выносите «холодные» пути** (DLQ, аудит) в отдельные группы.

**Приоритизация и throughput control.**

- **Prioritizers** на очередях (Block 1.2): `NewestFlowFileFirst` для приоритета свежих данных; `PriorityAttributePrioritizer` для классов трафика.
- **ControlRate:** ограничение скорости по числу FlowFile/размеру/значению атрибута — защита приёмников.
- **Back pressure** как основной регулятор; планируйте пороги по ёмкости: порог по FlowFile защищает heap, по размеру — диск.
- **Load balancing** — выравнивание, а не лекарство от всех бед.
- **Run Duration / Run Schedule** — баланс latency и throughput.
- **Приоритеты между потоками:** NiFi не имеет жёстких QoS между Process Group в одном пуле потоков; для гарантий выделяйте отдельный кластер/экземпляр или ограничивайте concurrent tasks менее критичных потоков.

**Типичные ошибки производительности.**

| Ошибка | Следствие |
|---|---|
| Heap по умолчанию | Частые GC/OOM |
| Миллионы мелких FlowFile | Нагрузка на heap/WAL/provenance |
| Provenance и Content на одном медленном диске | Низкий throughput |
| Слишком много Concurrent Tasks | Конкуренция, GC, медленнее общий результат |
| `Infer Schema` на больших FlowFile | Лишние проходы и нестабильность |
| Один огромный FlowFile | Нет параллелизма, трудный retry |

### Lab 8: тест производительности и тюнинг конвейера

**Цель.** Измерить throughput конвейера из Lab 5, найти узкое место и улучшить показатели.

**Методика (по шагам).**

1. **Базовая линия.** Зафиксируйте версию, конфигурацию (heap, thread pool, concurrent tasks), железо. Подготовьте генератор нагрузки (`GenerateRecord`, Kafka producer perf-test).
2. **Метрики.** События/с на выходе; Tasks/Time по процессорам; очереди (где растут); heap/GC; CPU; `iostat`; размер/время событий provenance.
3. **Сценарии.**
   - A: базовая конфигурация.
   - B: явные схемы (убрать Infer).
   - C: увеличить `Concurrent Tasks` узкого звена.
   - D: увеличить размер пачек (`MergeRecord`).
   - E: перенести Provenance на отдельный том/уменьшить индексацию.
   - F: увеличить heap и thread pool (по росту числа ядер).
4. **Один параметр за раз;** по 3 прогона; медиана.
5. **Отчёт:** таблица «изменение → эффект → вывод», обновлённые настройки, рекомендации.

**На стенде (Приложение A).**

- **Генератор:** `GenerateRecord` (схема лог-события с `user.pwd`, Number of Records `10000`, Run Schedule `1 sec`) перед конвейером Lab 5 — около 10 000 событий/с; нагрузку увеличивают через *Number of Records* и *Run Schedule*. Пока узким местом остаётся генератор, это и есть базовая линия.
- **Счётчик выхода:** `UpdateCounter` после `MergeRecord` с Delta `${record.count}` — events/s = прирост счётчика (☰ → Counters) за интервал.
- **Сценарий A** — Reader с `Infer Schema`; **B** — переключите Record Reader у `ValidateRecord` на `JsonTreeReader` с явной схемой.
- **Сценарий D** — *Minimum/Maximum Number of Records* у `MergeRecord`.
- **Сценарий E** — на Docker Desktop все volumes лежат на одном виртуальном диске, поэтому эффект отдельного тома не воспроизвести; уменьшите индексацию provenance в `conf/nifi.properties` (volume `nifi_conf`): `nifi.provenance.repository.indexed.fields`, `nifi.provenance.repository.indexed.attributes`, `nifi.provenance.repository.max.storage.time`, затем перезапуск `make nifi` (**[проверить]** влияние на вашей версии).
- **Сценарий F** — heap: `NIFI_JVM_HEAP_INIT` / `NIFI_JVM_HEAP_MAX` в `nifi/single/docker-compose-nifi-single.yml` и `make nifi` (учитывайте лимит памяти Docker Desktop); thread pool: ☰ → Controller Settings → *Maximum Timer Driven Thread Count*.
- **Метрики:** Prometheus уже собирает метрики NiFi (`http://localhost:9090`, job `nifi`), графики — Grafana `http://localhost:3000`.
- **Выход** пишется в `nifi/data/lab8-out` с именами по секундам (`lab8-${now():format('ss')}.ndjson`, Conflict Resolution `replace`) — на диске не больше 60 файлов при любой нагрузке.

**Шаблон таблицы результатов.**

```markdown
| Сценарий | Изменение | events/s | CPU % | GC pause (p95) | I/O await (мс) | Вывод |
|---|---|---|---|---|---|---|
| A | базовая | | | | | |
| B | явные схемы | | | | | |
```

**Полезные команды.**

```bash
bin/nifi.sh dump /tmp/nifi-dump.txt           # thread dump
bin/nifi.sh diagnostics /tmp/diag.txt         # диагностика
iostat -x 5                                   # нагрузка на диски
jstat -gcutil <pid> 5000                      # GC
```

На стенде те же команды выполняются внутри контейнера, а нагрузка видна через `docker stats`:

```bash
docker exec nifi /opt/nifi/nifi-current/bin/nifi.sh dump /tmp/nifi-dump.txt
docker exec nifi /opt/nifi/nifi-current/bin/nifi.sh diagnostics /tmp/diag.txt
docker exec nifi sh -c 'jstat -gcutil $(pgrep -f org.apache.nifi.NiFi | head -1) 5000'   # [проверить]
docker stats nifi                                                                     # CPU / память контейнера
```

**Типичные ошибки.** Измерение без стабильного входа; одновременное изменение нескольких параметров; игнорирование прогрева JVM (первые минуты не показательны); сравнение без одинаковых данных; выход на диск без ограничения объёма (заполняет диск за минуты).

**Эталонное решение:** `nifi/flows/lab8-performance.json`.

---

## Block 3.5: Final hands-on lab: end-to-end pipeline

### Цель и результат

Собрать производственно-ориентированный конвейер: **Kafka → валидация и преобразование JSON/NDJSON (Record) → доставка (Splunk или файл) → обработка ошибок → проверка производительности и тюнинг**.

### Архитектура

```text
                         ┌───────────────────────── PG: ingest ─────────────────────────┐
 Kafka topic logs.raw ──►│ ConsumeKafka (Processing=RECORD, Group=nifi-logs, Output=Value)│
                         │   └ parse failure ───────────────────────────► PG: errors     │
                         └───────────────┬───────────────────────────────────────────────┘
                                         ▼
                         ┌───────────────── PG: validate-transform ───────────────────┐
                         │ ValidateRecord (schema LogEvent.in)                         │
                         │   ├ invalid ─────────────────────────────► PG: errors        │
                         │ UpdateRecord (ingest_ts; writer-schema без секретов,         │
                         │               порядок полей: ingest_ts первым)               │
                         │ QueryRecord (WHERE level <> 'DEBUG')                         │
                         │ MergeRecord (NDJSON, до 10 000 записей, Max Bin Age 30 sec)  │
                         └───────────────┬───────────────────────────────────────────────┘
                                         ▼
                         ┌───────────────────── PG: deliver ─────────────────────────┐
                         │ PutSplunk (TCP)  или  InvokeHTTP→HEC  или PutFile           │
                         │   ├ failure/retry ► RetryFlowFile ► (исчерпано) ► PG: errors │
                         └───────────────┬───────────────────────────────────────────────┘
                                         ▼
                         ┌───────────────────── PG: errors ───────────────────────────┐
                         │ UpdateAttribute(error.*) ► PublishKafka (logs.dlq) / PutFile │
                         └───────────────────────────────────────────────────────────────┘
```

### Controller Services и параметры

| Сервис/параметр | Значение |
|---|---|
| `Kafka3ConnectionService` | `#{kafka.bootstrap}` |
| `AvroSchemaRegistry` | `LogEvent.in`, `LogEvent.out` |
| `JsonTreeReader` | Schema Name Property, registry; без Infer |
| `JsonRecordSetWriter` | `LogEvent.out`, `One Line Per Object`, без Pretty Print |
| Parameter Context `logs-prod` | `kafka.bootstrap`, `splunk.host`, `splunk.port`, `dlq.topic`, `out.dir` |

Значения для стенда (Приложение A.5): `kafka.bootstrap=kafka1:19092`, `splunk.host=splunk-sim`, `splunk.port=5514`, `dlq.topic=logs.dlq`, `out.dir=/opt/nifi/data/final` (резервный сток DLQ; на хосте — `nifi/data/final`).

В режиме `RECORD` у `ConsumeKafka` есть relationship **`parse failure`** (сообщения, которые Reader не разобрал) — направьте его в `errors`, иначе процессор невалиден. Для сверки удобно поставить `UpdateCounter` с Delta `${record.count}` в точках «принято» (после `ConsumeKafka`), «доставлено» (перед `PutSplunk`), «отфильтровано» (выход `debug` второго запроса `QueryRecord`: `SELECT * FROM FLOWFILE WHERE level = 'DEBUG'`) и «в DLQ» (в группе `errors`).

### Последовательность

1. Подготовьте стенд (Приложение A); создайте топики `logs.raw` (например, 6 партиций) и `logs.dlq` — на стенде это делает `make lab-setup`.
2. Создайте Parameter Context, Controller Services (схемы!).
3. Постройте группы в порядке: ingest → validate-transform → deliver → errors; соедините порты.
4. Настройте back pressure на межгрупповых связях; на связях в `errors` — высокие пороги.
5. Включите framework retry на `PutSplunk`/`InvokeHTTP`; `RetryFlowFile` для исчерпавших.
6. Запустите генератор нагрузки: смесь валидных и невалидных событий (5–10 % невалидных), события DEBUG, события с секретом. На стенде: `make lab-final-load n=100000` (код — `lab-services/produce_logs.py`). Вручную:

```bash
python3 - <<'EOF' | docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 --topic logs.raw
import json, random, time
for i in range(100000):
    r = random.random()
    if r < 0.05:
        print('{"ts":"bad"}')                     # невалидная запись
    else:
        print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ"), "level": random.choice(["INFO","ERROR","DEBUG"]),
                          "svc": "api", "msg": "event %d" % i, "user": {"id": i % 100, "pwd": "s3cret"}}))
EOF
```

### Проверка корректности

- В приёмнике: события **без** `pwd`, без `DEBUG`, с `ingest_ts` первым полем.
- В `logs.dlq` — невалидные записи (примерно 5 %) с атрибутами/заголовками ошибки.
- Сумма: `принято = доставлено + отфильтровано (DEBUG) + в DLQ` (сверка через `record.count`/счётчики `UpdateCounter`).
- Отсутствие роста очередей в `errors` свыше порогов; основная очередь не блокируется при росте DLQ.
- Provenance: для случайного события виден путь Kafka → … → SEND.

На стенде:

```bash
make lab-splunk-count                                    # строк в эмуляторе = final.delivered
docker exec splunk-sim sh -c 'grep -c pwd /data/splunk_in.log; grep -c DEBUG /data/splunk_in.log'   # ожидаем 0 и 0
docker exec kafka1 kafka-console-consumer --bootstrap-server kafka1:19092 --topic logs.dlq \
   --from-beginning --timeout-ms 10000 --property print.headers=true | head
```

Пример сверки на 2000 событий: `final.accepted` 2000 = `final.delivered` 1256 + `final.filtered.debug` 636 + `final.dlq` 108 (☰ → Counters).

**Повторный прогон.** Consumer group `nifi-logs` хранит offsets; перед новым прогоном с начала топика остановите `ConsumeKafka` и выполните `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-logs`; обнулите эмулятор: `docker exec splunk-sim sh -c ': > /data/splunk_in.log'`.

### Тест отказов

1. Остановите приёмник (Splunk-эмулятор; на стенде `docker stop splunk-sim`, затем `docker start splunk-sim`): очередь растёт, back pressure останавливает ingest; Kafka lag растёт (потери нет; `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --describe --group nifi-logs`); после возврата — рассасывание.
2. Перезапустите NiFi во время нагрузки (на стенде `docker restart nifi`): очереди восстанавливаются, consumer продолжает с зафиксированных offsets; возможны повторы (at-least-once) — проверить идемпотентность/приемлемость.
3. Подайте «пачку мусора» (100 % невалидных): основной поток не останавливается; DLQ принимает.

### Performance check и tuning

Выполните Lab 8 на этом конвейере: цель — стабильная пропускная способность без роста lag, GC-паузы в пределах SLO; зафиксируйте итоговые настройки (heap, thread pool, concurrent tasks, размер пачек, репозитории).

### Типичные ошибки и диагностика

| Симптом | Диагностика |
|---|---|
| Данные не появляются | Статус `ConsumeKafka` (Group/Offset Reset), bulletins; `kafka-consumer-groups.sh --describe` (на стенде `docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --describe --group nifi-logs`); группа уже прочитала топик — см. «Повторный прогон» |
| `ConsumeKafka` невалиден | Не подключён relationship `parse failure` |
| Всё в DLQ | Схема `LogEvent.in` не соответствует данным (типы/вложенные структуры) — смотрите атрибуты ошибки |
| Низкий throughput | Tasks/Time, очереди; не хватает партиций; мелкие FlowFile; один поток на `PutSplunk` |
| Дубли в приёмнике | Перезапуски/ребалансы; нужна идемпотентность |

**Эталонное решение:** `nifi/flows/lab9-final-pipeline.json`. Тесты отказов (остановка приёмника, перезапуск NiFi под нагрузкой) на эталоне не прогонялись.

---

## Block 3.6: Course recap and next steps

### Ключевые выводы за три дня

1. **Фундамент:** архитектура NiFi (Flow Controller, три репозитория, immutable content), FlowFile (атрибуты + контент), back pressure, prioritization, модульный дизайн (Process Group + Parameter Context + Flow Registry).
2. **Record-подход** — основной способ эффективной обработки: Reader/Writer/Schema, явные схемы, NDJSON, `QueryRecord`/`UpdateRecord`/`JoltTransformRecord`/`MergeRecord`.
3. **Интеграции:** JDBC (`ExecuteSQLRecord`, `QueryDatabaseTableRecord`, `PutDatabaseRecord`), REST (`InvokeHTTP`), Kafka 2.x (`ConsumeKafka`/`PublishKafka` + `Kafka3ConnectionService`), Splunk (`PutSplunk`/HEC).
4. **Миграция 1.x → 2.x:** Java 21; предварительно 1.27.0; удалённые компоненты (в т. ч. Kafka 2.6, Variables, Templates, Event-driven); blue/green и backup `state/local`; тестирование и план отката.
5. **Надёжность:** retry (framework-level и `RetryFlowFile`), DLQ, разделение путей ошибок, защита основного потока от заполнения failure-очередей, backup `conf/` + sensitive key.
6. **Эксплуатация:** мониторинг (UI, Prometheus, bulletins, provenance), RCA, rollback, восстановление.
7. **Безопасность:** TLS/mTLS, OIDC/LDAP, политики (с учётом deprecated fine-grained авторизации с 2.12), аудит, секреты и шифрование (repo encryption удалён в 2.0).
8. **Производительность:** heap/GC, thread pool, concurrent tasks, схема хранилища (изоляция Provenance), дизайн flow без узких мест, измерения.

### Рекомендуемые следующие шаги для команды

| Область | Действия |
|---|---|
| **Миграция** | Инвентаризация компонентов; апгрейд до последней 1.x ≥ 1.27.0; deprecation-аудит; staging 2.x (Java 21); план Kafka-переключения через Group ID; blue/green; backup `conf/`, ключей и `state/local` |
| **Flow tuning** | Замена Split на Record; явные схемы; пересмотр concurrent tasks/heap/thread pool; изоляция Provenance; метрики и baseline |
| **Error handling** | Стандартный шаблон DLQ+replay; framework retry на сетевых процессорах; алерты на размер failure-очередей; идемпотентные приёмники |
| **Безопасность** | mTLS/OIDC; модель групп; план перехода от fine-grained политик (NIP-39); Parameter Providers; шифрование дисков |
| **Процесс** | Git-based Registry; review и promotion dev→test→prod; Flow Analysis Rules; runbooks |

### Final Q&A и feedback

Обсуждение открытых вопросов; сбор обратной связи по курсу; план внедрения на 30/60/90 дней.

---

## Приложение A: Лабораторный стенд (Docker Compose)

Учебный стенд для всех лабораторных (не для production) — репозиторий стенда (compose-проект `nifi-playground`). Все сервисы запускаются через `make`, работают в одной Docker-сети `nifi-net` и хранят данные в именованных volumes. Проверено на NiFi 2.12.0.

### A.1 Состав стенда

| Сервис | Образ | Контейнер | Адрес в сети `nifi-net` | Порт на хосте | Compose-файл |
|---|---|---|---|---|---|
| NiFi (single) | `apache/nifi:2.12.0` | `nifi` | `https://nifi:8443` | `8443`, `8081` (ListenHTTP, Lab 2) | `nifi/single/docker-compose-nifi-single.yml` |
| NiFi (cluster, 3 узла) | `apache/nifi:2.12.0` | `nifi1`–`nifi3` | `https://nifi1:8443` … | `9443`–`9445` | `nifi/cluster/docker-compose-nifi-cluster.yml` |
| ZooKeeper (для кластера) | `zookeeper:3.9` | `nifi-cluster-zookeeper` | `zookeeper:2181` | — | там же |
| NiFi Registry (HTTPS) + LDAP (`lldap`) | `apache/nifi-registry:2.12.0` | `nifi-registry` | `https://nifi-registry:18443` | `18443`; UI LDAP `17170` | `nifi/registry/docker-compose-nifi-registry.yml` |
| PostgreSQL 18 | `postgres:18-alpine` | `nifi-postgres` | `postgres:5432` | `5432` | `postgres/docker-compose-postgres.yml` |
| pgAdmin (UI для PostgreSQL) | `dpage/pgadmin4` | `pgadmin` | — | `127.0.0.1:5050` | там же |
| Kafka (KRaft, 1 брокер) | `confluentinc/cp-kafka:8.0.0` | `kafka1` | `kafka1:19092` | `9092` | `kafka/docker-compose-kafka-single-node.yml` |
| Conduktor Console (UI Kafka) | `conduktor/conduktor-console` | — | — | `8080` | там же |
| Prometheus + kafka-exporter | `prom/prometheus` | `prometheus`, `kafka-exporter` | `prometheus:9090` | `9090`, `9308` | `monitoring/prometheus/docker-compose-prometheus.yml` |
| Grafana | `grafana/grafana` | `grafana` | `grafana:3000` | `3000` | `monitoring/grafana/docker-compose-grafana.yml` |
| Заглушка REST API клиентов (Lab 4) | `python:3.12-alpine` | `customers-api` | `http://customers-api:8000` | — | `lab-services/docker-compose-lab-services.yml` |
| Эмулятор Splunk (Lab 6, финальная) | `python:3.12-alpine` | `splunk-sim` | TCP `splunk-sim:5514`, HEC `http://splunk-sim:8088` | — | там же |
| Нестабильный API (Lab 7) | `python:3.12-alpine` | `flaky` | `http://flaky:9099/events` | — | там же |
| Splunk Enterprise (trial, по желанию) | `splunk/splunk` | `splunk` | TCP `splunk:5514`, HEC `http://splunk:8088` | `8000` (UI), `8088` | `splunk/docker-compose-splunk.yml` |

Splunk не входит в `make up` и запускается отдельно (`make splunk`, первый старт — несколько минут; на Apple Silicon образ amd64 работает через эмуляцию). Для лабораторных достаточно эмулятора `splunk-sim`; настоящий Splunk нужен, чтобы увидеть события в поиске (`index=nifi`). Порты те же, что у эмулятора, — в Parameter Context меняется только хост: `splunk-sim` → `splunk`.

### A.2 Структура репозитория

```text
Makefile                      все команды стенда (make help)
nifi/
  single/  cluster/           compose-файлы NiFi, authorizers.xml
  registry/                   NiFi Registry + LDAP, authorizers.xml
  common/                     генерация сертификатов, пустой flow, скрипты политик и импорта
  flows/                      эталонные решения лабораторных (lab*.json) + README
  certs/                      общий CA и сертификаты узлов (генерируются, не коммитятся)
  data/                       смонтирован в NiFi как /opt/nifi/data (входы и выходы лабораторных)
  drivers/                    JDBC-драйверы, смонтированы как /opt/nifi/drivers
kafka/                        Kafka + Conduktor
postgres/                     PostgreSQL + pgAdmin; init/ — таблицы лабораторных, pgadmin/ — сервер стенда
monitoring/prometheus/        Prometheus, kafka-exporter, prometheus.yml
monitoring/grafana/           Grafana и provisioning источника данных
splunk/                       Splunk Enterprise; apps/ — индекс nifi, TCP/HEC inputs, sourcetype
lab-services/                 заглушки: customers_api.py, splunk_sim.py, flaky.py; генератор produce_logs.py
```

### A.3 Запуск

Требования: Docker (Docker Desktop) с ~8 ГБ памяти, `make`, `python3` на хосте (только для `make lab-final-load`).

```bash
make up              # single NiFi + Registry, Postgres, Kafka, мониторинг, заглушки
make up-cluster      # то же с кластером NiFi из 3 узлов (≈4–5 ГБ памяти только на кластер)
make lab-setup       # таблица orders_stage, топики Kafka, каталоги nifi/data/* (идемпотентно)
make ps              # состояние контейнеров
make logs s=nifi     # логи контейнера
make urls            # адреса и логины
make down            # остановить всё, данные в volumes сохраняются
make destroy         # удалить всё вместе с данными (спрашивает подтверждение)
```

Отдельные стеки: `make nifi`, `make cluster`, `make registry`, `make postgres`, `make kafka`, `make monitoring`, `make lab-services`, `make splunk` и соответствующие `*-down`. NiFi стартует 1–3 минуты; `make nifi` / `make cluster` дожидаются готовности и выдают нужные политики доступа.

### A.4 Доступ

| Что | Адрес | Логин |
|---|---|---|
| NiFi (single) | `https://localhost:8443/nifi` | `admin` / пароль из `make urls` |
| NiFi (cluster) | `https://localhost:9443/nifi` (узлы 2–3: `:9444`, `:9445`) | тот же |
| NiFi Registry | `https://localhost:18443/nifi-registry` | `admin` / пароль из `make urls` (пользователи — в LDAP) |
| Пользователи Registry (lldap) | `http://localhost:17170` | тот же `admin` |
| Splunk (если запущен) | `http://localhost:8000`, поиск `index=nifi` | `admin` / пароль из `make urls` |
| PostgreSQL | `localhost:5432`, БД `nifi` | `nifi` / `nifi` |
| pgAdmin | `http://localhost:5050`; сервер `nifi-playground` уже в списке (пароль БД `nifi` спросит при первом подключении) | `admin@example.com` / пароль из `make urls` |
| Kafka | `localhost:9092` | — |
| Conduktor | `http://localhost:8080` | при первом входе |
| Prometheus | `http://localhost:9090` | — |
| Grafana | `http://localhost:3000` | `admin` / `admin` |

NiFi работает по HTTPS с сертификатом от собственного CA стенда; чтобы браузер не предупреждал, импортируйте `nifi/certs/ca.crt` как доверенный корневой сертификат. В кластере работайте через один узел: токен входа выдаётся узлом, и при переключении между портами придётся логиниться заново.

### A.5 Адреса для лабораторных

Что указывать в Parameter Context и командах вместо абстрактных адресов:

| Назначение | Значение на стенде |
|---|---|
| Kafka из NiFi (`bootstrap.servers`) | `kafka1:19092`, Security Protocol `PLAINTEXT` |
| Kafka CLI | `docker exec -i kafka1 kafka-console-producer --bootstrap-server kafka1:19092 ...` (аналогично `kafka-console-consumer`, `kafka-topics`, `kafka-consumer-groups`) |
| JDBC URL | `jdbc:postgresql://postgres:5432/nifi` (пользователь `nifi` / `nifi`) |
| JDBC-драйвер | `org.postgresql.Driver`, `/opt/nifi/drivers/postgresql-42.7.14.jar` |
| SQL-консоль | `docker exec nifi-postgres psql -U nifi -d nifi -c "..."` |
| Каталоги для `ListFile` / `PutFile` | `/opt/nifi/data/<имя>` в NiFi = `nifi/data/<имя>` на хосте |
| REST API клиентов | `http://customers-api:8000/customers/<id>` |
| Splunk | эмулятор: TCP `splunk-sim:5514`, HEC `http://splunk-sim:8088/services/collector/event`; настоящий Splunk: TCP `splunk:5514`, HEC `http://splunk:8088/services/collector/raw` (NDJSON как есть) или `/services/collector/event` (конверт `{"event": …}`), заголовок `Authorization: Splunk 00000000-0000-0000-0000-000000000000` |
| Нестабильный API | `http://flaky:9099/events` |
| NiFi Registry (Registry Client) | `https://nifi-registry:18443` — клиент создаётся автоматически (`make nifi` / `make cluster`), SSL Context Service не нужен |

### A.6 Особенности стенда

- **Single и cluster.** Порт `8081` для `ListenHTTP` (Lab 2) проброшен только у single NiFi. В кластере источники-генераторы и `ListFile` стоит запускать на Primary Node (*Execution: Primary node*), иначе каждый узел генерирует свои данные.
- **Безопасность.** Все узлы NiFi используют общий CA (`nifi/certs`), авторизация — `managed-authorizer`, вход — single-user `admin`. Права администратора и узлов выдаются автоматически при `make nifi` / `make cluster`.
- **Метрики NiFi.** Prometheus забирает `/nifi-api/flow/metrics/prometheus` по mTLS с клиентским сертификатом `CN=prometheus` (только чтение `/flow`).
- **Драйверы и данные.** `nifi/drivers` и `nifi/data` смонтированы во все узлы по одинаковым путям; новый JAR подхватывается при включении `DBCPConnectionPool` без перезапуска NiFi.
- **Один compose-проект.** Стеки запускаются через `make` с `docker compose -p nifi-playground`; запуская файлы напрямую, указывайте тот же `-p`, иначе Compose создаст новые пустые volumes.

### A.7 Тестовые данные и сброс

```bash
make lab4-data                    # 4 тестовых заказа в orders.raw (Lab 4)
make lab-final-load n=100000      # нагрузка для финальной лабораторной в logs.raw
make lab-splunk-count             # сколько строк получил эмулятор Splunk
make import f=nifi/flows/<файл>.json            # загрузить поток в single NiFi
make import-cluster f=nifi/flows/<файл>.json    # то же для кластера
```

Перед повторным прогоном лабораторных с Kafka сбросьте consumer group (при остановленном `ConsumeKafka`), иначе поток не прочитает уже прочитанные сообщения:

```bash
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-lab4   # Lab 4
docker exec kafka1 kafka-consumer-groups --bootstrap-server kafka1:19092 --delete --group nifi-logs   # финальная
docker exec nifi-postgres psql -U nifi -d nifi -c "TRUNCATE orders_stage"                             # Lab 4
docker exec splunk-sim sh -c ': > /data/splunk_in.log; : > /data/hec_in.log'                          # эмулятор Splunk
```

### A.8 Эталонные решения

В каталоге `nifi/flows` лежат проверенные на стенде Flow Definitions всех лабораторных (`lab1-first-flow.json` … `lab8-performance.json`, `lab9-final-pipeline.json` — финальная). Импорт: перетащите на холст значок **Process Group** и нажмите значок загрузки файла, либо `make import f=...`. После импорта включите Controller Services группы (*Enable all controller services*) и запустите её. Значения окружения вынесены в Parameter Context каждой лабораторной. Чтобы изменить эталон, поправьте поток в NiFi, экспортируйте его (ПКМ на группе → *Download flow definition*) и замените файл в `nifi/flows`.

---

## Приложение B: Список источников и статус верификации

### Источники, использованные для подтверждения фактов в этой сессии

| Источник | Что подтверждено |
|---|---|
| [Release Notes (Apache NiFi wiki)](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes) | История релизов 1.x/2.x; удалённые компоненты; deprecations; новые возможности (Kafka, Registry-клиенты, Prometheus, PEM, Flow Action Reporter и др.) |
| [Migration Guidance (Apache NiFi wiki)](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance) | Путь через 1.27.0; изменения по версиям 2.x; breaking changes (2.8, 2.10, 2.12, 2.13, 2.14); `flow.json.gz`; scope `ENVIRONMENT`; Distributed→Map/Set Cache; Windows `nifi.cmd` |
| [NiFi 2.0 Release Goals](https://cwiki.apache.org/confluence/display/NIFI/NiFi+2.0+Release+Goals) | Цели 2.0: удаление Templates/Variable Registry/`PersistentProvenanceRepository`, `flow.xml.gz`; переход на `java.time`; ghosted components |
| [Migrating Deprecated Components and Features for 2.0.0](https://cwiki.apache.org/confluence/spaces/NIFI/pages/240883792/Migrating+Deprecated+Components+and+Features+for+2.0.0) | Замены удалённых компонентов (частично) |
| [ConsumeKafka (справочник)](https://nifi.apache.org/components/org.apache.nifi.kafka.processors.ConsumeKafka/) и [PR о новых Kafka-компонентах](https://www.mail-archive.com/issues@nifi.apache.org/msg169807.html) | Свойства `ConsumeKafka`; Processing/Output Strategy; `Kafka3ConnectionService` |
| [П. Виллар: Getting ready for Apache NiFi 2.0](https://medium.com/cloudera-inc/getting-ready-for-apache-nifi-2-0-5a5e6a67f450) (автор — PMC NiFi) и документы Cloudera ([Behavioral changes](https://docs.cloudera.com/cfm/latest/release-notes/topics/cfm-behavioral-changes.html), [Breaking changes](https://docs.cloudera.com/cfm/4.10.0/release-notes/topics/cfm-nifi2-breaking-changes.html)) | Java 21; event-driven scheduling; замены компонентов (вторичные источники) |
| Справочники компонентов Site-to-Site Reporting Tasks ([Status](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-site-to-site-reporting-nar/1.28.0/org.apache.nifi.reporting.SiteToSiteStatusReportingTask), [Metrics](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-site-to-site-reporting-nar/1.28.0/org.apache.nifi.reporting.SiteToSiteMetricsReportingTask)) | Свойства и схемы записей Status/Metrics (версии 1.27–1.28, 2.0.0-M2/M3); наличие в 2.10.0 |
| Описание Flow Analysis Rules ([пост разработчика NiFi](https://community.cloudera.com/t5/Developer-Blogs/Flow-Analysis-Rules-enforcing-Best-Practices-in-Flow-Design/m-p/413017)), [PR #8765 (GitHub-клиент реестра)](https://www.mail-archive.com/issues@nifi.apache.org/msg173690.html), списки [Parameter Providers](https://docs.cloudera.com/cfm/latest/release-notes/topics/cfm-supported-parameter-providers.html) | Политики Warn/Enforce; встроенные правила 2.6.0; свойства GitHub-клиента; список провайдеров параметров |
| Справочники компонентов `SplitText`, `SplitRecord`, `SplitJson`, `QueryRecord`, `UpdateRecord`, `ValidateRecord`, `EvaluateJsonPath`, `MergeRecord` (nifi.apache.org, версии 1.2–2.0.0-M3 и latest; ссылки — в Day 1 §1.3.7 и Day 2 §2.2.6) и [RetryFlowFile](https://docs.snowflake.com/en/user-guide/data-integration/openflow/processors/retryflowfile) | Свойства, relationships и атрибуты этих процессоров |
| Справочники [Kafka3ConnectionService](https://docs.snowflake.com/en/user-guide/data-integration/openflow/controllers/kafka3connectionservice), [PublishKafka](https://docs.snowflake.com/en/en/user-guide/data-integration/openflow/processors/publishkafka), [DBCPConnectionPool](https://docs.snowflake.com/en/user-guide/data-integration/openflow/controllers/dbcpconnectionpool), [HikariCPConnectionPool](https://docs.snowflake.com/en/user-guide/data-integration/openflow/controllers/hikaricpconnectionpool), [JsonTreeReader](https://docs.snowflake.com/en/en/user-guide/data-integration/openflow/controllers/jsontreereader) (зеркала документации компонентов Apache NiFi), схемы Site-to-Site Bulletin/Status, README образа apache/nifi, описание REST API, [release notes Jolt 0.0.22](https://github.com/bazaarvoice/jolt/releases/tag/jolt-0.0.22) | Свойства и значения по умолчанию компонентов; поля S2S-записей; переменные образа; пути REST API; семантика `modify-*` в Jolt |
| Исходник Administration Guide ([PR](https://github.com/apache/nifi/pull/11488/files)) и [коммит NIFI-16290](https://mail-archive.com/commits@nifi.apache.org/msg46395.html) | `nifi.flowcontroller.autoResumeState` (по умолчанию `true`), `graceful.shutdown.period` (по умолчанию `10 secs`) и его использование при offload |
| Лабораторный стенд (Приложение A, NiFi 2.12.0): манифесты расширений из NAR образа (`META-INF/docs/extension-manifest.xml`) и прогон эталонных решений `nifi/flows` | Имена свойств, значения по умолчанию, допустимые значения и relationships всех процессоров и сервисов лабораторных; поведение Lab 1–8 и финальной лабораторной на реальных данных |

### Статус верификации (обновлено)

**Подтверждено по справочникам и документации в ходе работы над конспектом:** свойства, relationships и атрибуты `SplitText`, `SplitRecord`, `SplitJson`, `QueryRecord`, `UpdateRecord`, `ValidateRecord`, `EvaluateJsonPath`, `MergeRecord`, `RetryFlowFile`, `JsonTreeReader`; свойства `Kafka3ConnectionService`, `PublishKafka`, `DBCPConnectionPool`, `HikariCPConnectionPool`; схемы записей Site-to-Site Bulletin и Status; пути REST API (`/access/token`, `/flow/process-groups/{id}`, `.../controller-services`, `.../status`); переменные Docker-образа; `autoResumeState`, `graceful.shutdown.period`, `graceful.shutdown.seconds` (семантика); параметры балансировки кластера; `always.sync`; пул потоков (10 на узел) и ориентир 2–4 потока на ядро (сообщество); Warn/Enforce и встроенные правила Flow Analysis (2.6.0); свойства GitHub-клиента реестра; список провайдеров параметров; семантика `modify-*` в Jolt; отсутствие официального Helm chart на ноябрь 2024.

**Проверено на лабораторном стенде (NiFi 2.12.0):** значения по умолчанию `RetryFlowFile` (`Retry Attribute = flowfile.retries`, `Maximum Retries = 3`, `Penalize Retries = true`); свойства `Kafka3ConnectionService`, `ConsumeKafka`, `PublishKafka` (по манифестам NAR) и relationship `parse failure` у `ConsumeKafka` в режиме `RECORD`; `UpdateRecord` добавляет поле, отсутствующее в схеме Reader, при Writer с `Inherit Record Schema`; загрузка JDBC-драйвера через *Database Driver Locations* при включении сервиса, без перезапуска NiFi; Java 21 в образе `apache/nifi:2.12.0`; конфигурация KRaft одиночного брокера (`confluentinc/cp-kafka:8.0.0`); формат даты в Expression Language: `format("…'T'…")` работает, `format('…\'T\'…')` — нет; `ListenHTTP` не выставляет `mime.type`; Calcite в `QueryRecord` не сравнивает Avro enum в `IN`/`NOT IN`; у `PutSplunk` в 2.12 нет свойства `Connection Per FlowFile`.

**Остаётся непроверенным (отмечено в тексте как [проверить]):**

- Страница компонента `RetryFlowFile` на nifi.apache.org (значения по умолчанию подтверждены на стенде).
- Страница документации по сборке `Kafka3ConnectionService` и `PublishKafka` в самом Apache (использованы зеркала документации; свойства подтверждены манифестами NAR на стенде).
- Поведение `QueryRecord` (отдельный проход на каждый запрос).
- Загрузка JDBC-драйвера: отдельный class loader на сервис, передача драйвера через Assets Parameter Context, классы и URL конкретных драйверов (кроме PostgreSQL), режим курсора pgjdbc, ограничения `DBCPConnectionPoolLookup` по процессорам.
- Версии удаления в Apache для `ConvertJSONToAvro`, `ConvertCSVToAvro`, `ConvertAvroSchema`, `InferAvroSchema`, `ConvertAvroToORC`, старых Kafka-процессоров, Ignite/Flume/Kite, точные имена классов Hive 3; замены `InvokeAWSGatewayApi`.
- Reporting Tasks: выполнение только на Primary Node, неприменимость Parameter Contexts; поведение при недоступном получателе.
- systemd-юнит, probes Kubernetes, повторное включение Controller Services после недоступности зависимости.
- Расположение изменения `graceful.shutdown.period` при offload (NIFI-16290) по релизам; значение `graceful.shutdown.seconds` именно для NiFi.
- Stateless: in-memory content (2.13.0); `logback.xml` (автоподхват), `nifi-env.sh`; официальной роли Ansible; Java 21 в образах тегов, кроме проверенного `2.12.0`; альтернативы защиты `nifi.properties` после удаления `encrypt-config`.

### Ссылки на руководства (для самостоятельной сверки)

- [Overview](https://nifi.apache.org/docs/nifi-docs/html/overview.html) · [Getting Started](https://nifi.apache.org/docs/nifi-docs/html/getting-started.html) · [User Guide](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html) · [Administration Guide](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html)
- [Expression Language Guide](https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html) · [RecordPath Guide](https://nifi.apache.org/docs/nifi-docs/html/record-path-guide.html) · [NiFi In Depth](https://nifi.apache.org/docs/nifi-docs/html/nifi-in-depth.html)
- [Python Developer Guide](https://nifi.apache.org/docs/nifi-docs/html/python-developer-guide.html) · [Toolkit Guide](https://nifi.apache.org/docs/nifi-docs/html/toolkit-guide.html)
- [Справочник компонентов](https://nifi.apache.org/components/) · [Security Advisories](https://nifi.apache.org/security.html) · [Deprecated Components and Features](https://cwiki.apache.org/confluence/display/NIFI/Deprecated+Components+and+Features)
