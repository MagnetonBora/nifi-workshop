---
title: "Day 1 — Apache NiFi Fundamentals"
---

> Документ 1 из 3. Основа структуры — программа курса «Apache NiFi Training: Agenda» (21 час, 3 дня). Целевая версия: **Apache NiFi 2.x**; версия 1.x упоминается там, где это помогает понять архитектуру, эволюцию или миграцию.
> Связанные документы: **Day 2** (`day-2.md`), **Day 3** (`day-3.md`). Перекрёстные ссылки вида «Block X.Y» указывают на соответствующий блок в одном из трёх документов.

## Содержание

- [О материале: версии, источники, обозначения](#о-материале-версии-источники-обозначения)
- [Block 1.1: NiFi fundamentals](#block-11-nifi-fundamentals)
- [Block 1.2: Building data flows](#block-12-building-data-flows)
- [Block 1.3: Record-based processing](#block-13-record-based-processing)
- [Block 1.4: Installation, cluster configuration and setup](#block-14-installation-cluster-configuration-and-setup)
- [Block 1.5: Day 1 recap and Q&A](#block-15-day-1-recap-and-qa)

---

## О материале: версии, источники, обозначения

### Версионная база

На момент подготовки конспекта (ноябрь 2026) ветка 2.x развивается минорными релизами: [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes) перечисляют основной релиз 2.12.0 (13 сентября 2026), а страница [Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance) уже описывает переход на 2.14.0. Страницы вики могут отставать друг от друга, поэтому перед работой сверяйте точную версию, которую вы ставите.

Обозначения в тексте:

| Метка | Значение |
|---|---|
| **[2.x]** | Поведение актуально для NiFi 2.x |
| **[1.x]** | Поведение 1.x; в 2.x изменено или удалено |
| **[dep]** | Deprecated: пока работает, но помечено к удалению |
| **[rem]** | Removed: удалено в указанной версии |
| **[проверить]** | Утверждение не подтверждено в рамках подготовки конспекта; сверить с документацией вашей версии |

### Принципы достоверности

1. Факты о релизах, удалённых компонентах и миграции подтверждены по [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes), [Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance) и [NiFi 2.0 Release Goals](https://cwiki.apache.org/confluence/display/NIFI/NiFi+2.0+Release+Goals).
2. Остальные описания (параметры процессоров, свойства `nifi.properties`) основаны на официальных руководствах: [User Guide](https://nifi.apache.org/docs/nifi-docs/html/user-guide.html), [Administration Guide](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html), [Expression Language Guide](https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html), [RecordPath Guide](https://nifi.apache.org/docs/nifi-docs/html/record-path-guide.html), [NiFi In Depth](https://nifi.apache.org/docs/nifi-docs/html/nifi-in-depth.html), а также [справочнике компонентов](https://nifi.apache.org/components/). Имена и значения по умолчанию некоторых свойств могут отличаться в вашей минорной версии. Точные значения смотрите во встроенной документации (**Help → Documentation** в UI) своей версии.
3. Числовые рекомендации (heap, потоки) — это стартовые точки для замеров, а не нормативы.
4. Перечень источников и список пунктов, требующих сверки, находится в Приложении B (в конце документа Day 3).

---

## Block 1.1: NiFi fundamentals

### 1.1.1 Data in motion vs data at rest

**Data at rest** — данные, лежащие в хранилище (БД, data lake, файловая система). Типичные инструменты: SQL-движки, Spark в batch-режиме. Единица работы — запрос или job над ограниченным набором.

**Data in motion** — данные, которые перемещаются между системами: события, логи, сообщения, строки CDC. Единица работы — событие/пакет; важны latency, порядок, гарантии доставки, управление перегрузкой (backpressure), происхождение (lineage).

NiFi решает задачи **динамической маршрутизации и доставки** данных между гетерогенными системами: принять, проверить, при необходимости преобразовать, обогатить, доставить и сохранить историю происхождения. Это инструмент **data movement**, а не вычислительный движок аналитики. Тяжёлые агрегации и joins по большим окнам лучше оставлять Spark/Flink/SQL-движку, а в NiFi делать лёгкую обработку (фильтрация, нормализация, маршрутизация, форматные преобразования).

| Критерий | Data at rest | Data in motion (NiFi) |
|---|---|---|
| Единица обработки | таблица / файл / набор | FlowFile (событие или пакет событий) |
| Ключевая метрика | время выполнения job | throughput, latency, backlog |
| Отказы | перезапуск job | retry, back pressure, повтор доставки |
| Прозрачность | логи job | Data Provenance для каждого FlowFile |

### 1.1.2 Архитектура NiFi

Apache NiFi работает как Java-приложение в JVM на каждом узле. Основные элементы (подробнее — [NiFi In Depth](https://nifi.apache.org/docs/nifi-docs/html/nifi-in-depth.html) и [Overview](https://nifi.apache.org/docs/nifi-docs/html/overview.html)):

```text
┌───────────────────────────── JVM (NiFi node) ─────────────────────────────┐
│  Web Server (Jetty, REST API + UI)                                        │
│  ┌──────────────────────── Flow Controller ────────────────────────────┐  │
│  │ Scheduler ── Timer-driven thread pool ── Processors / Services       │  │
│  │ Process Sessions ─ Connections (queues) ─ Bulletin repository        │  │
│  └──────────────────────────────────────────────────────────────────────┘  │
│  FlowFile Repository   Content Repository     Provenance Repository        │
│  (WAL: attributes +    (claims на диске,      (события; индексируются      │
│   pointer на content)   immutable-контент)      для поиска)                │
│  Extensions: Processors, Controller Services, Reporting Tasks, Parameter   │
│  Providers, Flow Registry Clients (NAR-пакеты; Python extensions в 2.x)    │
└───────────────────────────────────────────────────────────────────────────┘
```

**Flow Controller** — «мозг» узла. Он планирует выполнение процессоров, выдаёт им потоки (threads) из пула, управляет очередями и репозиториями. Процессор не владеет потоком: Flow Controller запускает его метод `onTrigger` по расписанию, а процессор через **ProcessSession** берёт FlowFile из входящих очередей и передаёт в исходящие.

**Thread pool.** В NiFi 2.x остался единственный пул: *time-driven*. Event-driven scheduling был экспериментальным, не дал заметного выигрыша и удалён в 2.0 ([источник — статья П. Виллара](https://medium.com/cloudera-inc/getting-ready-for-apache-nifi-2-0-5a5e6a67f450), [Cloudera: Behavioral changes](https://docs.cloudera.com/cfm/latest/release-notes/topics/cfm-behavioral-changes.html); удаление event-driven пула и необходимость переключить такие компоненты на *Timer Driven* подтверждены документацией дистрибутива на базе NiFi 2.0; в Apache Migration Guidance отдельной строки об этом нет). Практический вывод: до миграции переключите компоненты со стратегией *Event Driven* на *Timer Driven*.

**Bulletins.** Это сообщения о проблемах, которые компоненты публикуют (ошибка записи, недоступность БД). Они видны как красный значок на компоненте, на **Bulletin Board** и через API; хранятся в памяти (Bulletin Repository) и вытесняются по времени. В 2.7.0 добавлены очистка bulletins и stack trace в bulletin board ([Release Notes 2.7.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).

### 1.1.3 Репозитории и жизненный цикл FlowFile

Три репозитория — основа гарантий NiFi.

| Репозиторий | Что хранит | Реализация по умолчанию | Ключевые свойства `nifi.properties` |
|---|---|---|---|
| **FlowFile Repository** | Состояние каждого FlowFile: атрибуты, ссылка на content claim, в какой очереди находится | `WriteAheadFlowFileRepository` (write-ahead log + периодические checkpoints) | `nifi.flowfile.repository.directory`, `nifi.flowfile.repository.checkpoint.interval`, `nifi.flowfile.repository.always.sync` |
| **Content Repository** | Байты контента в виде *content claims* (неизменяемые, append-only файлы-контейнеры) | `FileSystemRepository` | `nifi.content.repository.directory.default` (можно несколько директорий), `nifi.content.repository.archive.*`, `nifi.content.claim.max.appendable.size` |
| **Provenance Repository** | События происхождения (RECEIVE, SEND, ROUTE, CONTENT_MODIFIED, DROP и т. д.) с индексом для поиска | `WriteAheadProvenanceRepository` | `nifi.provenance.repository.directory.default`, `nifi.provenance.repository.max.storage.time`, `nifi.provenance.repository.max.storage.size`, `nifi.provenance.repository.index.*` |

Замечания по версиям:

- `PersistentProvenanceRepository` и legacy-классы удалены в 2.0 ([Release Goals](https://cwiki.apache.org/confluence/display/NIFI/NiFi+2.0+Release+Goals)). `WriteAheadProvenanceRepository` доступна с 1.2.0 ([Release Notes 1.2.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).
- **[rem 2.0.0]** Поддержка шифрования репозиториев удалена ([Release Notes 2.0.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes), раздел Breaking Changes). Это важно для миграции (Block 2.4) и для раздела Encryption (Block 3.3).

#### Принцип copy-on-write и неизменяемость контента

Контент записанного claim **не модифицируется**. Когда процессор «меняет» контент (например, `ConvertRecord`), он читает старый claim и пишет **новый**; FlowFile получает ссылку на новый claim. Следствия:

1. Клонирование FlowFile (например, при ветвлении на две связи) не копирует байты, а лишь добавляет ссылку на тот же claim (счётчик ссылок).
2. Операции, меняющие только атрибуты (`UpdateAttribute`, `RouteOnAttribute`), дёшевы: контент не трогается.
3. Каждая трансформация контента — это I/O чтения и записи. Цепочки из многих процессоров, переписывающих контент, увеличивают нагрузку на диск. Это мотивирует использовать Record-based processing с минимальным числом «перезаписывающих» шагов.
4. Место на диске освобождается, когда на claim не остаётся ссылок; удалённый контент может некоторое время лежать в **archive** (если включён), что нужно учитывать при расчёте ёмкости.

#### Жизненный цикл FlowFile (упрощённо)

```text
Источник (например, ListenHTTP / ConsumeKafka / GetFile)
  │ 1. session.create() → новый FlowFile; событие provenance RECEIVE / CREATE
  │ 2. session.write(...) → контент в новый claim
  │ 3. session.transfer(ff, REL_SUCCESS)
  │ 4. session.commit():
  │       a) обновление FlowFile Repository (WAL)  ← точка durability
  │       b) запись provenance-событий
  │       c) FlowFile попадает в очередь Connection
  ▼
Connection (queue): back pressure, prioritizers, expiration, load balancing
  ▼
Следующий процессор: session.get() → обработка → commit/rollback
  ▼
Терминальный процессор / автозавершение связи → событие DROP; claim освобождается
```

**Семантика гарантий.** Внутри NiFi обработка организована транзакциями сессии. При `rollback` (исключение в процессоре) FlowFile возвращается во входящую очередь (с penalization в ряде случаев), поэтому внутри узла реализуется модель **at-least-once**: данные не теряются при сбое между шагами, но могут быть обработаны повторно, если сбой произошёл после побочного эффекта во внешней системе, но до commit. Это ключевой момент для интеграций (Kafka, БД, REST): идемпотентность записи на приёмнике — задача проектировщика flow.

Durability зависит от настроек синхронизации: по умолчанию `nifi.flowfile.repository.always.sync=false` и `nifi.content.repository.always.sync=false`. Данные сбрасываются на диск операционной системой асинхронно; при крахе ОС/питания возможна потеря последних изменений. `true` повышает надёжность ценой throughput (имена и значения по умолчанию подтверждены документацией FlowFile Repository, [Administration Guide](https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html), и примером реальной конфигурации: `always.sync=false` у FlowFile и Content Repository, `nifi.queue.swap.threshold=20000`, архив Content Repository — 7 дней или 50 % тома). Интервал checkpoint FlowFile Repository в документации и в поставляемых конфигурациях встречается разный (`2 mins` в описании, `20 secs` в примере конфигурации), поэтому смотрите значение в своём `nifi.properties`.

#### Swapping и FlowFile в памяти

Метаданные FlowFile (атрибуты) живут в heap. Чтобы большие очереди не вызывали OutOfMemory, NiFi **выгружает (swap)** FlowFile из очередей на диск при превышении порога `nifi.queue.swap.threshold` (по умолчанию 20 000 на очередь). Практический вывод: миллионы мелких FlowFile опасны для heap и для скорости; гораздо лучше сотни крупных FlowFile с пачками записей (Record-based processing, MergeRecord).

### 1.1.4 Ключевые понятия

| Понятие | Описание | Практические детали |
|---|---|---|
| **FlowFile** | Единица данных: *атрибуты* (key-value в памяти/WAL) + *content* (поток байт в Content Repository) | Системные атрибуты: `uuid`, `filename`, `path`, `fileSize`, `mime.type` (если установлен) |
| **Processor** | Единица работы: ingest, transform, route, deliver | Имеет свойства, relationships, scheduling, concurrent tasks, run duration, penalty/yield |
| **Relationship** | Именованный выход процессора (`success`, `failure`, `retry`, `matched`…) | Каждую relationship нужно либо соединить, либо **auto-terminate**, иначе процессор невалиден |
| **Connection** | Очередь между процессорами/портами | Back pressure, prioritizers, FlowFile expiration, load balancing |
| **Process Group** | Контейнер компонентов; вложенность; Input/Output Ports | Единица модульности, версионирования, параметризации и (в 2.x) исполнения в Stateless режиме |
| **Controller Service** | Общий конфигурируемый сервис (DBCP pool, SSL Context, Record Reader/Writer, Schema Registry) | Область видимости — группа, в которой создан (и вложенные); требует *Enable* |
| **Parameter Context** | Именованный набор параметров (в т. ч. sensitive), привязываемый к Process Group | Ссылка `#{param}` в свойствах; поддерживает наследование (с 1.15.0) |
| **Reporting Task** | Периодическая задача уровня контроллера (метрики, мониторинг) | Например, `PrometheusReportingTask`, `MonitorDiskUsage` |
| **Parameter Provider** | Загрузка параметров из внешнего хранилища (Vault, AWS/GCP Secrets) | Введены в 1.18.0 |
| **Flow Registry Client** | Интеграция с реестром версий flow (NiFi Registry, GitHub, GitLab, Bitbucket, Azure DevOps) | Подробнее в Block 1.4 |

Про Variable Registry: **[rem 2.0]** переменные (Variables) и Variable Registry удалены в пользу Parameter Contexts ([Release Goals](https://cwiki.apache.org/confluence/display/NIFI/NiFi+2.0+Release+Goals)); в Expression Language вместо `VARIABLE_REGISTRY` доступен scope `ENVIRONMENT` (системные свойства JVM и переменные окружения) ([Migration Guidance, 2.0.0-M1](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).

### 1.1.5 Типовые сценарии

1. **Stream ingestion.** Приём событий по HTTP (`ListenHTTP`/`HandleHttpRequest`), из Kafka (`ConsumeKafka`), MQTT; нормализация и маршрутизация в хранилища.
2. **Log shipping.** Приём syslog (`ListenSyslog`), tail файлов (`TailFile`), разбор, фильтрация (отбрасывание шума), доставка в Splunk/Elastic/Kafka/S3. Splunk рассматривается в Block 2.3.
3. **Event pipelines.** Валидация против схемы, обогащение (`LookupRecord`), раздельная доставка в несколько приёмников, DLQ для некорректных событий.
4. **Batch-интеграции.** List/Fetch-паттерн (`ListFile`+`FetchFile`, `ListSFTP`+`FetchSFTP`, `ListS3`+`FetchS3Object`) с состоянием (state) для инкрементальной загрузки.

### Lab 1: интерфейс NiFi и первый flow

**Цель.** Освоить UI NiFi, создать, запустить и отследить простейший flow, увидеть provenance.

**Ожидаемый результат.** Flow `GenerateFlowFile → UpdateAttribute → LogAttribute`, статистика очередей, события provenance и содержимое FlowFile в Content Viewer.

**Архитектура решения.**

```text
GenerateFlowFile ──success──► UpdateAttribute ──success──► LogAttribute (auto-terminate success)
```

**Процессоры и конфигурация.**

| Процессор | Свойство | Значение |
|---|---|---|
| `GenerateFlowFile` | Scheduling → Run Schedule | `5 sec` |
| | File Size | `0 B` (если задан Custom Text) |
| | Custom Text | `{"id": ${random():mod(1000)}, "ts": "${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")}"}` |
| | Mime Type | `application/json` |
| `UpdateAttribute` | Dynamic property `lab.stage` | `ingested` |
| | Dynamic property `event.id` | `${UUID()}` |
| `LogAttribute` | Log Level | `info`; Relationship `success` → Auto-terminate |

**Последовательность.**

1. Добавьте процессоры перетаскиванием иконки Processor на холст (поиск по имени).
2. Соедините связью `success`; в диалоге Connection отметьте только `success`.
3. Для `LogAttribute` в *Settings → Automatically Terminate Relationships* включите `success`.
4. Запустите (Start). Наблюдайте счётчики *In/Out*, *Tasks/Time*, очередь на связи.
5. ПКМ на связи → **List queue** → просмотр атрибутов и контента (при остановленном приёмнике — остановите `UpdateAttribute`, очередь заполнится).
6. ПКМ на процессоре → **View data provenance** → откройте событие и **Lineage graph**.

**Что происходит с FlowFile.** `GenerateFlowFile` создаёт FlowFile (событие `CREATE`); `UpdateAttribute` фиксирует `ATTRIBUTES_MODIFIED`; `LogAttribute` пишет в `nifi-app.log` и FlowFile завершается (`DROP`).

**Проверка.** В `logs/nifi-app.log` (на стенде: `make logs s=nifi`) появляются записи `LogAttribute` с атрибутами `lab.stage=ingested`; в provenance видны три события на один `uuid`; в Content Viewer — JSON вида `{"id": 598, "ts": "2026-11-19T16:00:05.141Z"}`.

**Типичные ошибки.** Процессор остаётся с жёлтым треугольником (невалиден): нет подключённых/auto-terminated relationships или не заданы обязательные свойства. Очередь «не двигается»: приёмник остановлен или достигнут back pressure. В контенте вместо даты сырой текст `${now():format(...)}` — Expression Language не разобрал выражение: строку формата с литералом `'T'` пишите в **двойных** кавычках (`format("yyyy-MM-dd'T'...")`); вариант `format('yyyy-MM-dd\'T\'...')` в NiFi 2.12 не вычисляется. Процессор при этом остаётся валидным — ошибка видна только по содержимому FlowFile.

**Эталонное решение:** `nifi/flows/lab1-first-flow.json` (Приложение A.8).

---

## Block 1.2: Building data flows

### 1.2.1 Ingestion разных типов данных

| Источник | Основные компоненты | Состояние / инкремент | Примечания |
|---|---|---|---|
| **Файлы (локальные/SFTP/SMB/S3)** | `ListFile` + `FetchFile`, `GetFile`, `ListSFTP` + `FetchSFTP`, `ListS3` + `FetchS3Object` | `List*` хранят state (последние метки времени/ключи) | List/Fetch предпочтительнее `Get*`: List можно запускать на Primary Node Only, а Fetch распределять по кластеру через load balancing |
| **HTTP/REST (входящие)** | `ListenHTTP`, `HandleHttpRequest` + `HandleHttpResponse` | — | Для приёма событий; настройте TLS и аутентификацию |
| **HTTP/REST (исходящие)** | `InvokeHTTP` | — | Подробно в Block 2.1 |
| **Базы данных** | `ExecuteSQLRecord`, `QueryDatabaseTableRecord`, `GenerateTableFetch` | Maximum-value columns в state | Подробно в Block 2.1 |
| **Messaging** | `ConsumeKafka`, `ConsumeJMS`, `ConsumeMQTT`, `ConsumeAMQP` | Offsets/acks на стороне брокера | Kafka — Block 2.1 |
| **Логи/сети** | `TailFile`, `ListenSyslog`, `ListenTCP`, `ListenUDP` | Позиция чтения в state | |

**Паттерн List/Fetch.** `List*` формирует FlowFile без контента (только атрибуты, описывающие объект), затем `Fetch*` читает содержимое. Благодаря этому листинг выполняется на одном узле (Primary Node Only), а дорогая загрузка — на всех узлах кластера после балансировки очереди. Из NiFi 1.14 процессоры, запланированные на *Primary Node Only* и имеющие входящие соединения, становятся невалидными ([Migration Guidance, 1.14.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).

### 1.2.2 Маршрутизация и фильтрация

#### Expression Language (EL) — основы

EL вычисляется для значений свойств, помеченных как *Supports Expression Language*; в документации свойства указан scope: `NONE`, `ENVIRONMENT`, `FLOWFILE_ATTRIBUTES`. Полное описание — [Expression Language Guide](https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html).

```text
${filename}                                   // значение атрибута
${filename:toUpper()}                         // цепочка функций
${fileSize:gt(1048576)}                       // булево: размер > 1 МБ
${mime.type:equals('application/json')}
${kafka.topic:startsWith('orders.')}
${now():format('yyyy-MM-dd')}                 // текущая дата
${http.query.param.limit:replaceNull('100')}  // значение по умолчанию
${severity:equals('ERROR'):or(${severity:equals('FATAL')})}
${literal(5):plus(3)}
#{db.url}                                     // ссылка на параметр (Parameter Context), не EL
```

#### RouteOnAttribute

`RouteOnAttribute` направляет FlowFile по именованным relationships на основании EL-предикатов. Контент не читается, поэтому процессор очень дешёв.

| Свойство | Назначение |
|---|---|
| **Routing Strategy** | `Route to Property name` (каждое динамическое свойство = relationship); `Route to 'match' if all match`; `Route to 'match' if any matches` |
| Dynamic property (имя = relationship) | EL-выражение, возвращающее boolean |
| Relationship `unmatched` | Всё, что не подошло ни под одно условие |

Пример конфигурации (`Route to Property name`):

```text
big_json      = ${mime.type:equals('application/json'):and(${fileSize:gt(10485760)})}
small_json    = ${mime.type:equals('application/json'):and(${fileSize:le(10485760)})}
csv           = ${filename:endsWith('.csv')}
```

**Ограничение.** `RouteOnAttribute` видит только атрибуты. Для маршрутизации по содержимому записей используйте `RouteOnContent` (регулярные выражения, сканирует контент), `QueryRecord`/`PartitionRecord` (разбиение по значениям полей без Split) или предварительно извлеките значение в атрибут (`EvaluateJsonPath`, `ExtractText`) — но извлечение в атрибут имеет смысл только для небольших значений (см. раздел про производительность).

### 1.2.3 Слияние потоков (merging), back pressure, prioritization

**Слияние.** Несколько входящих связей на один процессор допустимы (fan-in); процессор берёт FlowFile из любой. Для объединения самих *данных* используются `MergeContent` (байтовая склейка, с форматами binary concatenation/TAR/ZIP/Avro) и `MergeRecord` (запись-ориентированная склейка с учётом схемы; подробный справочник по его свойствам и правилам готовности bin'а — Day 2, раздел 2.2.6). Ключевые параметры слияния: *Minimum/Maximum Number of Entries (Records)*, *Minimum/Maximum Group Size*, **Max Bin Age** (принудительный сброс по времени — важен, чтобы хвост данных не застревал), *Correlation Attribute Name*, *Merge Strategy* (`Bin-Packing Algorithm` или `Defragment`).

> Историческая особенность: в NiFi 1.8 `MergeRecord` начал честно соблюдать *Minimum Number of Records*; если задать minimum меньше maximum, выпуск может содержать больше мелких FlowFile. Для крупных выходных файлов задавайте minimum ближе к maximum ([Migration Guidance, 1.8.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).

#### Back pressure

Back pressure — механизм защиты от переполнения: у каждой Connection есть **Object Threshold** и **Size Threshold**. Новые подключения получают значения по умолчанию **10 000 FlowFile и 1 GB** (введено в 1.0.0, [Migration Guidance](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)). Когда любой порог достигнут, процессор-источник этой связи **перестаёт планироваться**, пока очередь не освободится.

Важное следствие: достаточно, чтобы **одна из исходящих связей** процессора была заполнена, и он остановится целиком, даже если остальные связи свободны. Это критично для failure-очередей (Block 3.1).

| Параметр Connection | Назначение |
|---|---|
| Back Pressure Object Threshold | Максимум FlowFile в очереди (по умолчанию 10 000) |
| Back Pressure Data Size Threshold | Максимум суммарного размера (по умолчанию 1 GB) |
| FlowFile Expiration | Через сколько удалять неотработанные FlowFile (по умолчанию `0 sec` = не удалять) |
| Prioritizers | Порядок выборки из очереди |
| Load Balance Strategy | Распределение по узлам кластера (Block 1.4) |

#### Prioritization

Prioritizers применяются на уровне очереди:

| Prioritizer | Поведение |
|---|---|
| `FirstInFirstOutPrioritizer` | По времени попадания в очередь (по порядку) |
| `NewestFlowFileFirstPrioritizer` | Сначала новейшие (снижает latency свежих событий при backlog) |
| `OldestFlowFileFirstPrioritizer` | Сначала старейшие по возрасту lineage |
| `PriorityAttributePrioritizer` | По атрибуту `priority` (меньшее значение — выше приоритет) |

Prioritizers не гарантируют строгий глобальный порядок при нескольких concurrent tasks и в кластере: порядок обработки определяется ещё и параллелизмом. Если порядок критичен (например, CDC для одного ключа), используйте партиционирование по ключу, один поток на ключ (partition by attribute + 1 concurrent task) или `EnforceOrder`.

### 1.2.4 Практики проектирования flow

**Именование.** Единый стандарт: `<Действие> <Объект> [<Источник/Приёмник>]`, например `Consume orders from Kafka`, `Validate order schema`, `Route invalid orders to DLQ`. Имена должны быть понятны без открытия конфигурации. Для связей давайте имена только когда это добавляет смысла (`valid`, `invalid`).

**Документирование.** Используйте поле **Comments** у компонентов и **Labels** на холсте; описывайте контракт группы (входы/выходы, форматы, SLA). Включайте описание Process Group.

**Модульность.**

- **Process Group** как функциональный модуль: *ingest*, *validate*, *transform*, *deliver*, *error handling*. Общение между группами — через Input/Output Ports.
- **Parameter Context** для всех значений, зависящих от окружения (URL, пути, топики, имена таблиц, credentials как sensitive parameters). Наследование контекстов (`base` → `prod`) позволяет хранить общие значения отдельно.
- Один Controller Service — один смысл; создавайте сервисы на уровне Process Group, а не root, если они не нужны глобально.
- Выделяйте error handling в переиспользуемую группу (подробно в Block 3.1).

**Антипаттерны.** «Спагетти» из сотен процессоров на одном холсте; жёстко зашитые URL и пароли; извлечение больших фрагментов контента в атрибуты; бесконечные связи `failure → тот же процессор` без ограничения числа повторов; процессоры без имён.

### Lab 2: flow из нескольких источников с маршрутизацией

**Цель.** Принять данные из нескольких источников (файлы, HTTP, генератор) и смаршрутизировать их по атрибутам.

**Архитектура.**

```text
[PG: ingest]                                   [PG: route]
 ListFile → FetchFile ───────┐
 ListenHTTP (port 8081) ─────┼─► Output Port ─► RouteOnAttribute ─┬─ json  ─► (PG: process_json)
 GenerateFlowFile ───────────┘   "all-sources"                    ├─ csv   ─► (PG: process_csv)
                                                                  └─ unmatched ─► LogAttribute / PutFile (quarantine)
```

**Конфигурация.**

| Компонент | Ключевые свойства |
|---|---|
| `ListFile` | Input Directory `#{ingest.dir}`; File Filter `.*\.(json|csv)`; Listing Strategy по умолчанию (tracking timestamps) |
| `FetchFile` | File to Fetch `${absolute.path}/${filename}`; Completion Strategy `None` (лабораторный режим) |
| `ListenHTTP` | Listening Port `8081`; Base Path `events`; **HTTP Headers for Attributes `Content-Type`**; Для лабораторной без TLS (в production — SSL Context Service) |
| `UpdateAttribute` (на каждом входе) | `source` = `file` / `http` / `generator`; на входе HTTP дополнительно `mime.type` = `${'Content-Type':substringBefore(';'):trim()}` — `ListenHTTP` сам `mime.type` не выставляет |
| `GenerateFlowFile` | Custom Text — JSON; Mime Type `application/json` |
| `RouteOnAttribute` | Routing Strategy `Route to Property name`; `json`=`${mime.type:equals('application/json'):or(${filename:endsWith('.json')})}`, `csv`=`${filename:endsWith('.csv')}` |
| `PutFile` (quarantine) | Directory `#{quarantine.dir}` |
| Parameter Context `lab-params` | `ingest.dir=/opt/nifi/data/in`, `quarantine.dir=/opt/nifi/data/quarantine` (на хосте стенда — `nifi/data/in`, `nifi/data/quarantine`, см. Приложение A.5) |

**Порядок построения.**

1. Создайте Parameter Context `lab-params` и привяжите к группам (*Configure Process Group → Parameter Context*).
2. Постройте группу `ingest`, внутри — три источника и выходной порт.
3. Постройте группу `route`; соедините порт группы `ingest` с входным портом `route`.
4. Настройте приоритет `NewestFlowFileFirstPrioritizer` на связи после объединения (для демонстрации).
5. Отправьте тестовое событие и положите файлы (команды с хоста стенда, из корня репозитория):

```bash
curl -X POST -H 'Content-Type: application/json' \
     -d '{"id":1,"type":"click"}' http://localhost:8081/events
echo '{"id":10,"type":"file"}' > nifi/data/in/sample.json
printf 'id,type\n2,view\n' > nifi/data/in/sample.csv
```

**Проверка.** В очередях `json`/`csv` появляются FlowFile; атрибут `source` соответствует источнику; HTTP-событие уходит в `json` с `mime.type=application/json`; события provenance показывают событие `ROUTE`.

**Ошибки.** `ListenHTTP` не отвечает — порт занят или не проброшен в контейнер (на стенде порт 8081 проброшен только у single NiFi); `ListFile` не видит файлы — права пользователя NiFi на директорию; `unmatched` растёт — не заданы mime.type/filename (проверьте атрибуты через List queue). Частый случай: HTTP-события уходят в `unmatched`, потому что `ListenHTTP` не переносит заголовок `Content-Type` в `mime.type` — нужны `HTTP Headers for Attributes` и `UpdateAttribute` из таблицы выше.

**Эталонное решение:** `nifi/flows/lab2-multi-source-routing.json`.

---

## Block 1.3: Record-based processing

### 1.3.1 Концепция Record

В классической модели «один FlowFile = один объект»: для JSON-массива из 10 000 событий приходилось делать `SplitJson` (10 000 FlowFile), затем обрабатывать каждый. Это порождает 10 000 записей в FlowFile Repository, столько же provenance-событий, 10 000 вызовов `onTrigger` на каждом шаге и серьёзную нагрузку на heap.

**Record-oriented processing** рассматривает FlowFile как **набор записей (records)** одного формата. Формат (JSON, CSV, Avro, Parquet, XML…) абстрагирован двумя Controller Services:

- **Record Reader** — десериализует байты в поток записей (`JsonTreeReader`, `CSVReader`, `AvroReader`, `ParquetReader`, `XMLReader`, `GrokReader`…).
- **Record Writer** — сериализует записи в байты (`JsonRecordSetWriter`, `CSVRecordSetWriter`, `AvroRecordSetWriter`, `ParquetRecordSetWriter`…).

Схема (**Schema**) описывает поля и типы. Процессор работает с записями потоково (streaming): одна запись в памяти за раз, поэтому большие FlowFile обрабатываются без загрузки целиком (если конкретный процессор не накапливает данные: `QueryRecord` и `MergeRecord` имеют свои нюансы).

```text
FlowFile (NDJSON, 100k записей)
   │ JsonTreeReader ──► поток Record
   │          └── процессор: ConvertRecord / UpdateRecord / QueryRecord / ValidateRecord …
   │ JsonRecordSetWriter / AvroRecordSetWriter ◄──
   ▼
FlowFile (другой формат или схема, 1 FlowFile, 1 запись в репозитории)
```

### 1.3.2 Reader, Writer, Schema и Schema Registry

**Schema Access Strategy (у Reader)** определяет, откуда берётся схема входных данных:

| Стратегия | Источник схемы | Когда использовать |
|---|---|---|
| `Infer Schema` | Выводится из самих данных | Прототипирование; нестрогие данные; не production-ориентир (см. 1.3.3) |
| `Use 'Schema Name' Property` | Имя схемы из атрибута (`schema.name`) разрешается через Schema Registry | Production: много типов данных с общим реестром |
| `Use 'Schema Text' Property` | Схема (Avro JSON) прямо в свойстве | Единичные стабильные схемы |
| `Schema Reference Reader` и др. | Ссылка на схему в данных/заголовке (например, Confluent wire format) | Интеграция с Confluent Schema Registry |

**Schema Write Strategy (у Writer)** определяет, как схема сопровождает выход: `Do Not Write Schema`, `Set 'schema.name' Attribute`, `Embed Avro Schema`, `Inherit Record Schema` и др. Writer также может использовать собственную схему, отличную от входной (поэтому `ConvertRecord` — это преобразование формата **и** схемы).

**Schema Registry** — Controller Service, хранящий схемы:

| Сервис | Особенность |
|---|---|
| `AvroSchemaRegistry` | Схемы хранятся прямо в конфигурации сервиса (динамические свойства `имя = Avro schema JSON`); просто, версионируется вместе с flow |
| `ConfluentSchemaRegistry` | Внешний реестр Confluent |
| `AmazonGlueSchemaRegistry` и др. | Облачные реестры (в 1.23+/2.x) |

Пример Avro-схемы, зарегистрированной в `AvroSchemaRegistry` как `order.v1`:

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.example.orders",
  "fields": [
    {"name": "order_id",  "type": "string"},
    {"name": "customer_id", "type": ["null", "string"], "default": null},
    {"name": "amount",    "type": {"type": "bytes", "logicalType": "decimal", "precision": 12, "scale": 2}},
    {"name": "currency",  "type": "string", "default": "EUR"},
    {"name": "created_at","type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
```

Атрибут `schema.name=order.v1` устанавливается `UpdateAttribute` после ingest.

> Примечание по 2.x. Парсинг и форматирование дат/времени в Record-компонентах переведён с `SimpleDateFormat` на `DateTimeFormatter` (**2.0.0-M2**, [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)); шаблоны формата должны соответствовать синтаксису `java.time`.

### 1.3.3 Record-процессоры против Split-процессоров

| Аспект | Split-подход (`SplitJson`, `SplitText`, …) | Record-подход (`ConvertRecord`, `QueryRecord`, `PartitionRecord`, …) |
|---|---|---|
| Число FlowFile | N (по числу записей) | 1 (или несколько партиций) |
| Нагрузка на FlowFile Repository / provenance | Высокая | Низкая |
| Расход heap на метаданные | Растёт пропорционально N | Константа |
| Контент перезаписывается | На каждом шаге для каждой записи | Один раз на шаг для всего FlowFile |
| Схема и типы | Нет (JSONPath по строкам) | Есть; валидация и приведение типов |
| Фильтрация/маршрутизация | `EvaluateJsonPath` + `RouteOnAttribute` | SQL-запросом в `QueryRecord`, `PartitionRecord` |
| Агрегация обратно | `MergeContent` (Defragment) | Не требуется или `MergeRecord` |

Типовая замена:

```text
БЫЛО:  SplitJson ► EvaluateJsonPath ► RouteOnAttribute ► ReplaceText ► MergeContent ► PutX
СТАЛО: QueryRecord (фильтр + проекция + маршрутизация) ► UpdateRecord (при необходимости) ► PutXRecord
```

Когда Split всё же оправдан: нужна обработка записи как независимой единицы с отдельным результатом (например, по одной записи вызывается REST API с независимым retry); либо необходимо ограничить размер пачки (`SplitRecord` с *Records Per Split* — это уже record-вариант разбиения).

### 1.3.4 JSON Tree Reader: Infer Schema, ограничения, явная схема

`JsonTreeReader` читает JSON-объекты как записи; поддерживает одиночный объект, массив объектов и **NDJSON** (несколько объектов подряд, по одному в строке) — один и тот же Reader принимает все три раскладки.

**Infer Schema.** Reader анализирует данные FlowFile и строит схему из значений. Это удобно при прототипировании, но имеет ограничения:

1. **Дополнительный проход по данным.** Для вывода схемы Reader должен просмотреть данные до начала собственно обработки; на больших FlowFile это заметная накладная (CPU/I/O).
2. **Нестабильность схемы между FlowFile.** Тип поля выводится по значениям *этого* FlowFile: поле, содержащее `1`, `2`, `3`, выведется как целое; если в другом FlowFile встретится `1.5`, тип будет другим. Поле, которое во всех записях `null`, выводится без полезного типа. Результат: разные схемы у соседних FlowFile, ошибки при записи в БД/Avro, разные выходные структуры.
3. **Необратимость решений.** Writer с `Inherit Record Schema` повторит «случайную» схему: ошибки обнаружатся только в приёмнике.
4. **Нет контракта.** Нельзя гарантировать обязательность полей, форматы дат, десятичную точность (decimal), значения по умолчанию.
5. **Особенности с датами и временем.** Строка вида `2026-11-15T10:00:00Z` может остаться строкой или быть распознана как timestamp в зависимости от настроек форматов Reader.

**Что подтверждено справочником `JsonTreeReader`** ([справочник](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-record-serialization-services-nar/1.5.0/org.apache.nifi.json.JsonTreeReader/index.html), [описание в 2.x](https://docs.snowflake.com/en/en/user-guide/data-integration/openflow/controllers/jsontreereader)):

- **Schema Access Strategy по умолчанию — `Infer Schema`.** То есть, если вы ничего не меняли, Reader выводит схему из данных каждого FlowFile.
- Reader принимает «JSON-per-line» (NDJSON), одиночные объекты и массивы; элементы массива становятся отдельными записями.
- При заданной схеме **поля JSON, которых нет в схеме, пропускаются**, а поля схемы, которых нет в JSON, получают `null`. Следствие: при явной схеме всё, что нужно дальше (в том числе для Jolt и `QueryRecord`), должно быть описано в схеме Reader'а.
- Для `Infer Schema` можно подключить **кэш схем**: без него вывод схемы повторяется для каждого FlowFile; с ним найденная схема переиспользуется. Для потоков с однородными данными это дешёвая оптимизация, но она не заменяет явную схему как контракт.

**Когда задавать схему явно:** любые production-потоки; запись в БД и колоночные форматы (Avro/Parquet); валидация (`ValidateRecord`); совместный доступ нескольких команд к данным; необходимость эволюции схемы под контролем.

Основные свойства `JsonTreeReader`:

| Свойство | Назначение |
|---|---|
| Schema Access Strategy | См. таблицу выше |
| Schema Registry / Schema Name / Schema Text | Параметры выбранной стратегии |
| Starting Field Strategy (`Root Node` / `Nested Field`) + Starting Field Name | Чтение записей из вложенного массива (например, `$.data.items`) |
| Schema Application Strategy | К каким узлам применить схему при вложенном старте |
| Date / Time / Timestamp Format | Шаблоны парсинга строк |
| Allow Comments | Допуск комментариев в JSON |
| Max String Length | Максимальная длина строкового значения при разборе (по умолчанию `20 MB`); защита от чрезмерно больших значений |
| Infer Schema Cache | Необязательный кэш схем для Schema Access Strategy `Infer Schema`: если задан, сначала ищется подходящая схема в кэше, иначе схема выводится заново **для каждого FlowFile** |

В 2.10.0 «улучшена снисходительность JSON-читателей и гибкость JSON Writer» ([Release Notes 2.10.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)); при обновлении проверяйте регрессионно поведение на «грязных» данных.

### 1.3.5 Настройка Reader и Writer для производительности

1. **Явная схема** вместо Infer (избегаем дополнительного прохода и нестабильных типов).
2. **Минимум преобразований.** Если читаете и пишете JSON с той же схемой, не добавляйте лишний `ConvertRecord`.
3. **Writer.** У `JsonRecordSetWriter`: `Pretty Print JSON = false`; `Output Grouping`: `Array` или `One Line Per Object` (NDJSON). Для потоковой доставки в Splunk/Kafka/логи NDJSON удобнее массива: его можно резать по строкам, конкатенировать и обрабатывать потоково. `Suppress Null Values`: `Never Suppress` / `Always Suppress` / `Suppress Missing Values` — влияет на размер выхода и на совместимость с приёмником.
4. **Схема Writer.** Явная схема с нужным порядком полей определяет порядок вывода (см. Block 2.2).
5. **Формат.** Бинарные форматы (Avro, Parquet) дешевле текстового JSON по размеру и CPU разбора; внутри цепочки можно конвертировать в Avro и обратно в JSON на границе.
6. **Размер FlowFile.** Много записей в одном FlowFile — хорошо, но гигантские FlowFile (гигабайты) затрудняют параллелизм и retry. Обычно целятся в десятки-сотни МБ на FlowFile; подбирайте под приёмники.
7. **Concurrent Tasks.** Record-процессоры безопасно масштабируются по `Concurrent Tasks`; ограничение — число FlowFile в очереди (при одном огромном FlowFile параллелизм бесполезен). Разбейте вход на партиции (`PartitionRecord`/`SplitRecord` с большим *Records Per Split*).
8. **Кэширование схем.** Reader/Writer как Controller Service создаются один раз и переиспользуются; не создавайте отдельный сервис на каждый процессор без необходимости.

### 1.3.6 Основные Record-процессоры (обзор)

| Процессор | Назначение |
|---|---|
| `ConvertRecord` | Формат/схема → формат/схема |
| `QueryRecord` | SQL (Apache Calcite) над записями FlowFile (`FROM FLOWFILE`); каждый динамический property = relationship с результатом запроса |
| `UpdateRecord` | Изменение значений полей по RecordPath |
| `JoltTransformRecord` | Структурные преобразования через Jolt (размещён в `nifi-jolt-nar` с 2.0.0-M2) |
| `ValidateRecord` | Проверка записей на схему; relationships `valid`, `invalid`, `failure` |
| `PartitionRecord` | Разбиение по значениям полей в отдельные FlowFile с атрибутами |
| `LookupRecord` | Обогащение через Lookup Service |
| `MergeRecord` | Склейка мелких FlowFile в крупные |
| `SplitRecord` | Разбиение на пачки записей (когда это необходимо) |
| `RemoveRecordField` | Удаление полей (добавлен в 1.23.0) |
| `ForkRecord` | Разворачивание вложенных массивов |

### 1.3.7 Справочник: Split-процессоры, введение в QueryRecord и RecordPath

Этот раздел нужен для Lab 3: в ней вы сравниваете Split-подход с Record-подходом, поэтому важно понимать, **что именно делает каждый Split-процессор** и чем его можно заменить. Описание свойств основано на справочниках компонентов NiFi ([SplitText](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.9.2/org.apache.nifi.processors.standard.SplitText/index.html), [SplitRecord](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.17.0/org.apache.nifi.processors.standard.SplitRecord/), [SplitJson](https://nifi.apache.org/docs/nifi-docs/components/org.apache.nifi/nifi-standard-nar/1.2.0/org.apache.nifi.processors.standard.SplitJson/index.html)); в вашей версии имена и значения по умолчанию сверяйте во встроенной документации.

#### Три Split-процессора: сравнение

| | `SplitText` | `SplitRecord` | `SplitJson` |
|---|---|---|---|
| Что делит | Текст **по строкам** | Данные в любом формате, который читает Record Reader | **Массив** внутри одного JSON-документа, указанный выражением JsonPath |
| «Понимает» структуру данных | Нет, только строки и размеры | Да (схема, Reader/Writer) | Да, но только JSON |
| Типовой вход | Логи, CSV, NDJSON «как текст» | JSON, NDJSON, CSV, Avro, Parquet и др. | Один JSON с массивом (`{"items":[…]}`) |
| Размер части | Число строк и/или байт | Число записей (`Records Per Split`) | Всегда один элемент массива на FlowFile |
| Формат результата | Тот же текст | Формат Record Writer (можно сразу сменить формат) | JSON-фрагмент или скаляр |
| Relationships | `splits`, `original`, `failure` | `splits`, `original`, `failure` | `split`, `original`, `failure` |

Все три процессора пишут атрибуты **фрагментов**: `fragment.identifier` (одинаков для всех частей одного родителя), `fragment.index` (порядковый номер), `fragment.count` (сколько частей) и `segment.original.filename` (имя родительского FlowFile). По ним `MergeContent`/`MergeRecord` в режиме **Defragment** собирают части обратно (для Defragment обязательны `fragment.identifier` и `fragment.count`).

#### SplitText

Делит текстовый файл по границам строк. Подходит для NDJSON и логов, когда нужно **нарезать пачки по N строк**, не разбирая содержимое.

| Свойство | Назначение |
|---|---|
| **Line Split Count** | Сколько строк в каждой части (без строк заголовка). Значение `0` допустимо только вместе с *Maximum Fragment Size* |
| **Maximum Fragment Size** | Максимальный размер части в байтах, включая заголовок. Если заданы оба ограничения, деление происходит по тому, которое достигнуто первым. Строка, превышающая лимит сама по себе, уходит в отдельную часть, превышающую лимит |
| **Header Line Count** | Сколько первых строк считать заголовком; они **дублируются в каждую часть** (удобно для CSV) |
| **Header Line Marker Characters** | Альтернатива: строки, начинающиеся с указанных символов, считаются заголовком (игнорируется, если *Header Line Count* не нулевой) |
| **Remove Trailing Newlines** | Убирать ли перевод строки в конце части (по умолчанию `true`). Если части потом будут склеиваться, ставьте `false` |

Атрибут результата: `text.line.count` (число строк в части) и перечисленные выше `fragment.*`.

**Рекомендации.**

- Делите **крупными пачками** (`Line Split Count` = тысячи строк), а не по одной строке: это уже полурешение проблемы мелких FlowFile.
- Справочник предупреждает: если разбиение даёт очень много частей, возможна повышенная нагрузка на память (в памяти хранятся FlowFile с атрибутами, а не контент), поэтому при необходимости применяют **двухэтапное разбиение**: сначала крупные куски, затем мельче.
- `SplitText` **не понимает JSON**: он разрежет «красиво отформатированный» многострочный JSON посередине объекта. Для таких данных нужен `SplitRecord` или `SplitJson`.

#### SplitRecord

Делит FlowFile в record-ориентированном формате на несколько FlowFile по **числу записей**. Reader читает вход, Writer пишет результат — поэтому одним действием можно и разбить, и сменить формат.

| Свойство | Назначение |
|---|---|
| **Record Reader** | Как читать вход |
| **Record Writer** | Как писать каждую часть |
| **Records Per Split** | Сколько записей в каждой части; поддерживает Expression Language по атрибутам FlowFile |

Relationships: `splits` (части), `original` (исходный FlowFile после успешного деления), `failure` (если FlowFile не удалось прочитать/преобразовать, он возвращается без изменений). Атрибуты: `mime.type`, `record.count` (записей в части), `fragment.*`, `segment.original.filename`.

**Когда нужен.** Не для «одной записи на FlowFile» (это антипаттерн), а для **управления размером пакетов**: огромный FlowFile надо разбить на части по 10 000–100 000 записей, чтобы они обрабатывались параллельно несколькими потоками или узлами (Block 1.3.5, п. 7).

#### SplitJson

Делит JSON-документ по **массиву**, найденному выражением JsonPath. Каждый элемент массива становится отдельным FlowFile.

| Свойство | Назначение |
|---|---|
| **JsonPath Expression** | Путь к массиву, например `$.items` |
| **Null Value Representation** | Как представлять `null`: пустая строка (по умолчанию) или строка `null` |
| **Max String Length** | Максимальная длина строкового значения при разборе (по умолчанию 20 MB) |

Relationships: `split` (элементы), `original`, `failure` — в `failure` уходит FlowFile, если это не валидный JSON или путь не найден (либо не указывает на массив).

**Особенности.** Процессор рассчитан на **один JSON-документ** и разбирает его целиком; на NDJSON (много документов подряд) он не предназначен. Он создаёт **по FlowFile на элемент**, то есть ведёт к проблеме мелких FlowFile из Block 1.1.3, если массив большой.

#### Как выбрать

| Задача | Что использовать |
|---|---|
| Нарезать логи/NDJSON/CSV на пачки по N строк без разбора содержимого | `SplitText` с большим `Line Split Count` |
| Нарезать данные любого формата на пачки по N **записей** или сразу сменить формат | `SplitRecord` |
| Из одного JSON `{"items":[…]}` получить отдельные объекты для независимой обработки | `SplitJson` — или, лучше, `ForkRecord`/`QueryRecord`, если независимость не нужна |
| Отфильтровать, спроецировать, маршрутизировать записи | **Не Split**: `QueryRecord`, `PartitionRecord`, `UpdateRecord` |

**Когда Split всё же оправдан:** нужен независимый результат/retry/вызов внешнего API на каждый элемент; нужно ограничить размер пакета перед приёмником; формат не поддерживается Reader'ами. Во всех остальных случаях выигрыш даёт Record-подход.

#### Введение в QueryRecord

`QueryRecord` — главный «заменитель» цепочки `Split → EvaluateJsonPath → RouteOnAttribute`. Идея: вы пишете **SQL-запрос** над записями FlowFile (таблица называется `FLOWFILE`), и результат запроса становится содержимым выходного FlowFile.

- Каждое **динамическое свойство** процессора — это одна **relationship**: имя свойства = имя relationship, значение = SQL `SELECT`. Должно быть хотя бы одно свойство.
- SQL — ANSI SQL на базе Apache Calcite.
- Нужны Record Reader и Record Writer.

Три типичных запроса (каждый — отдельное свойство/relationship):

```sql
-- relationship "errors": только ошибки, только нужные поля
SELECT ts, svc, msg FROM FLOWFILE WHERE level = 'ERROR'

-- relationship "normal": всё, кроме DEBUG
SELECT * FROM FLOWFILE WHERE level <> 'DEBUG'

-- relationship "stats": агрегат по сервисам
SELECT svc, COUNT(*) AS cnt FROM FLOWFILE GROUP BY svc
```

Один FlowFile с 100 000 записей обрабатывается без единого Split; в результате три relationship получают данные нужной формы. Полный справочник (свойства, поведение при пустых результатах, схема результата, стоимость) — в Day 2, раздел 2.2.6.

#### Мини-врезка: RecordPath

Многие Record-процессоры (`UpdateRecord`, `PartitionRecord`, `LookupRecord`, `RemoveRecordField`) указывают поля с помощью **RecordPath** — языка путей по полям записи ([RecordPath Guide](https://nifi.apache.org/docs/nifi-docs/html/record-path-guide.html)). Основы:

| Конструкция | Смысл | Пример |
|---|---|---|
| `/field` | Поле верхнего уровня | `/level` |
| `/parent/child` | Вложенное поле | `/user/id` |
| `//field` | Поле с таким именем на любом уровне вложенности | `//id` |
| `/array[*]` | Все элементы массива | `/items[*]/sku` |
| `..` | Родитель текущего узла | `../id` |

Подробный разбор RecordPath и функций (`concat`, `format` и др.) — в Day 2, раздел 2.2.6 (вместе с `UpdateRecord`).


### Lab 3: замена Split-flow на Record-flow и сравнение throughput

**Цель.** Количественно сравнить два варианта обработки одного набора данных.

**Исходные данные.** `GenerateRecord` (процессор появился в 1.20.0) создаёт FlowFile с N случайных записей по заданной схеме. Либо `GenerateFlowFile` с NDJSON-текстом.

Нужны **две схемы**. Схема генератора (`Schema Text` у `GenerateRecord`, Writer — `JsonRecordSetWriter` с `Output Grouping = One Line Per Object`): `currency` объявлен enum, иначе генератор выдаёт случайные строки и фильтр по валюте не сработает:

```json
{"type": "record", "name": "GeneratedOrder", "fields": [
  {"name": "order_id", "type": "string"},
  {"name": "customer", "type": "string"},
  {"name": "amount",   "type": "double"},
  {"name": "currency", "type": {"type": "enum", "name": "Currency", "symbols": ["EUR", "USD", "GBP"]}}
]}
```

Схема Reader (`JsonTreeReader`, `Use 'Schema Text' Property`) — та же, но `currency` объявлен строкой: SQL-движок `QueryRecord` (Apache Calcite) не сравнивает enum со строками в `IN` / `NOT IN`:

```json
{"type": "record", "name": "Order", "fields": [
  {"name": "order_id", "type": "string"},
  {"name": "customer", "type": "string"},
  {"name": "amount",   "type": "double"},
  {"name": "currency", "type": "string"}
]}
```

**Вариант A (Split-based).**

```text
GenerateRecord ► SplitRecord(Records Per Split=1) ► EvaluateJsonPath ► RouteOnAttribute ► UpdateAttribute ► MergeContent ► LogAttribute
```

**Вариант B (Record-based).**

```text
GenerateRecord ► QueryRecord ─ valid ─► UpdateRecord ► LogAttribute
                              └ rejected ─► LogAttribute
```

QueryRecord, динамическое свойство `valid`:

```sql
SELECT order_id, customer, amount, currency
FROM FLOWFILE
WHERE amount > 0 AND currency IN ('EUR', 'USD')
```

и `rejected`:

```sql
SELECT * FROM FLOWFILE WHERE amount <= 0 OR currency NOT IN ('EUR', 'USD')
```

**Методика замера.**

1. Одинаковый вход: например, 100 FlowFile по 100 000 записей (настраивается в `GenerateRecord`: *Number of Records*). Учтите масштаб: вариант A превращает такой вход в ~10 млн FlowFile, что не помещается в учебный heap (на стенде 1–2 ГБ). На ноутбуке начинайте с 1 FlowFile по 10 000 записей (*Run Once* у генератора) и увеличивайте объём, следя за heap и back pressure.
2. Заморозьте начальные условия: чистые очереди, одинаковые `Concurrent Tasks`.
3. Запустите вариант A, дождитесь обработки всех данных; запишите: время обработки, значения *Tasks/Time*, *Bytes Read/Written*, число FlowFile в очередях, heap и GC (System Diagnostics), число provenance-событий.
4. Остановите, очистите очереди, запустите вариант B с теми же входными данными; повторите замеры.
5. Повторите 3 раза и берите медиану.

**Ожидаемое поведение (качественно).** Вариант B обрабатывает данные заметно быстрее и с меньшей нагрузкой на heap/GC; число provenance-событий и записей в FlowFile Repository на порядки меньше. Абсолютные значения зависят от железа — **цифры нужно получить в вашем окружении**, а не брать из учебника.

**Проверка корректности.** Сумма записей в `valid` и `rejected` = входному числу; атрибуты `record.count` (`QueryRecord` ставит его на выходные FlowFile) совпадают. Например, при 10 000 записей на входе: `valid` 4910 + `rejected` 5090 (событие `ROUTE` у `QueryRecord` в provenance).

**Типичные ошибки.** `QueryRecord` падает с ошибкой SQL — имя таблицы должно быть `FLOWFILE`; тип поля не совпал (decimal vs double) — задайте схему явно; ошибка `Values passed to NOT IN operator must have compatible types` — поле в схеме Reader объявлено enum, объявите его строкой; в варианте A `MergeContent` ждёт нужного числа элементов — задайте *Max Bin Age*. Одинаковые имена процессоров в вариантах A и B (например, два `LogAttribute (valid)`) путают при поиске в provenance — давайте им разные имена.

**Эталонное решение:** `nifi/flows/lab3-split-vs-record.json`.

---

## Block 1.4: Installation, cluster configuration and setup

### Конфигурационные файлы NiFi: обзор

Основная конфигурация лежит в каталоге `NIFI_HOME/conf/`. Ниже — краткий справочник; расположение репозиториев и полное дерево каталогов — в разделе 1.4.5.

| Файл | Назначение | Что внутри | Заметки для кластера |
|---|---|---|---|
| `nifi.properties` | **Главный файл настроек узла** | Порты и хост веб-сервера, TLS (keystore/truststore), пути и параметры трёх репозиториев, кластер (`nifi.cluster.*`), ZooKeeper (`nifi.zookeeper.*`), Site-to-Site (`nifi.remote.*`), `nifi.sensitive.props.key`, пути к другим конфигам (`authorizers.xml`, `login-identity-providers.xml`, `state-management.xml`), архивирование flow | У каждого узла свой файл: часть значений одинакова (sensitive key, настройки кластера), часть индивидуальна (адрес узла, пути, сертификаты). Изменения вступают в силу после перезапуска узла |
| `bootstrap.conf` | Параметры **запуска JVM** и процесса-супервизора (Bootstrap) | `java.arg.N` (heap `-Xms`/`-Xmx`, параметры GC, `-XX:+HeapDumpOnOutOfMemoryError`), путь к Java, таймаут корректной остановки | Правится при тюнинге памяти (Block 3.4); перезапуск обязателен |
| `flow.json.gz` | **Сохранённый flow**: процессоры, связи, Process Group, Controller Services, Parameter Contexts | JSON в сжатом виде; sensitive-значения зашифрованы ключом из `nifi.sensitive.props.key` | Одинаков на всех узлах кластера; вручную не редактируется. В 2.x единственный формат (`flow.xml.gz` не используется) |
| `authorizers.xml` | **Авторизация**: откуда берутся пользователи и группы, где хранятся политики | User Group Provider, Access Policy Provider, Authorizer; начальный администратор (*Initial Admin Identity*) и идентичности узлов кластера | Должен быть согласован между узлами |
| `login-identity-providers.xml` | **Аутентификация** по логину/паролю | Настройки провайдера: LDAP/AD, Single User и др. Используется, когда выбран соответствующий провайдер в `nifi.properties` | Одинаков на узлах кластера |
| `users.xml`, `authorizations.xml` | Пользователи/группы и политики доступа, если используются **файловые** провайдеры | Создаются и обновляются NiFi (имена файлов задаются в `authorizers.xml`) | В кластере должны совпадать; **входят в backup** (Block 3.1.5) |
| `state-management.xml` | **Хранение состояния компонентов** (state) | Local state provider (на диске узла, `state/local`) и cluster state provider (ZooKeeper или Kubernetes) | Параметры подключения кластерного провайдера — на всех узлах одинаково |
| `logback.xml` | **Логирование** | Файлы журналов (`nifi-app.log`, `nifi-user.log`, `nifi-bootstrap.log`, `nifi-request.log`, `nifi-deprecation.log`), уровни, ротация | Уровни логов часто меняют при отладке; logback обычно подхватывает изменения без перезапуска (**[проверить]** у вас) |
| `zookeeper.properties` | Настройки **встроенного ZooKeeper** | Порты, каталог данных, список серверов | Только для лабораторных; в production используется внешний ансамбль ZooKeeper. В 2.x интеграция с ZooKeeper вынесена в отдельный NAR (2.6.0) |
| `archive/` (каталог) | **Архив предыдущих версий flow** | Копии `flow.json.gz` с метками времени (`nifi.flow.configuration.archive.*`) | Первая линия отката конфигурации (Block 3.1/3.2) |
| Keystore/truststore (например, `keystore.p12`, `truststore.p12`) или PEM-файлы | **TLS-материал** узла | Сертификат и закрытый ключ узла, доверенные сертификаты | Индивидуальны для каждого узла (SAN = имя узла); хранить с ограниченными правами |
| `bin/nifi-env.sh` (вне `conf/`) | Переменные окружения для скриптов запуска (например, `JAVA_HOME`) | Экспорт переменных | Нужен, если Java не определяется автоматически (**[проверить]** в вашей версии) |

Устаревшее и удалённое в 2.x: `flow.xml.gz` (заменён `flow.json.gz`), `bootstrap-notification-services.xml` (Bootstrap Notification Services удалены в 2.0.0-M2), шифрование значений в `nifi.properties` через `encrypt-config` (удалено в 2.0.0-M4) — подробности в Block 2.4 и Block 3.3.

**Основные группы свойств `nifi.properties`.**

| Группа (префикс) | За что отвечает |
|---|---|
| `nifi.web.*` | Адрес и порты HTTPS (по умолчанию 8443), прокси-хосты, настройки Jetty |
| `nifi.security.*` | Keystore/truststore, режим аутентификации и авторизации |
| `nifi.flow.configuration.*` | Путь к `flow.json.gz`, архивирование flow |
| `nifi.flowfile.repository.*`, `nifi.content.repository.*`, `nifi.provenance.repository.*` | Расположение и параметры репозиториев |
| `nifi.queue.*` | Пороги swap очередей |
| `nifi.cluster.*` | Роль узла в кластере, адреса, load balancing, flow election |
| `nifi.zookeeper.*` | Подключение к ZooKeeper |
| `nifi.remote.*` | Site-to-Site |
| `nifi.sensitive.props.*` | Ключ и алгоритм шифрования sensitive-значений |
| `nifi.state.management.*` | Путь к `state-management.xml` и провайдеры состояния |

**Практические правила.**

1. Изменения в конфигурации выполняйте через систему управления конфигурациями (Ansible, Helm values, образы), а не руками на узлах: так узлы остаются согласованными.
2. Не меняйте файлы «на ходу» без перезапуска там, где он нужен (`nifi.properties`, `bootstrap.conf`); после правки проверяйте `nifi-app.log` на ошибки старта.
3. Секреты (пароли keystore, `nifi.sensitive.props.key`) не храните в репозитории с конфигурацией и не публикуйте в логах; ограничьте права на файлы (`chmod 600/640`, владелец — пользователь NiFi).
4. Перед правкой делайте копию файла; перед миграцией — полный backup `conf/` (Block 2.4 и 3.1.5).

### 1.4.1 Single node vs cluster

| Аспект | Single node | Cluster |
|---|---|---|
| Масштабирование | Вертикальное | Горизонтальное (добавление узлов) |
| Доступность | Единая точка отказа | Отказ узла не останавливает кластер; данные застрявшего узла обрабатываются после его возврата |
| Сложность | Низкая | ZooKeeper (или Kubernetes leader election), TLS между узлами, балансировка |
| Единое управление flow | — | Изменение flow применяется ко всем узлам |
| Когда выбирать | Разработка, малые нагрузки, простые источники | Throughput > одного узла, HA, rolling-обслуживание |

Важно: кластер NiFi — это **набор независимых узлов с одинаковым flow**, а не единая очередь. Каждый узел обрабатывает *свои* FlowFile; данные между узлами перемещаются только через настроенный load balancing или Site-to-Site. Отказ узла означает, что данные в его репозиториях недоступны, пока узел не вернётся (или пока его диски не будут подключены к замене) — отдельное планирование надёжности хранилища (RAID, репликация томов).

### 1.4.2 Роли в кластере и координация

NiFi использует **zero-master clustering** (с 1.0.0): выделенного менеджера нет, роли избираются.

| Роль | Функция |
|---|---|
| **Cluster Coordinator** | Принимает heartbeats, управляет членством (connect/disconnect), реплицирует изменения запросов UI на все узлы, объединяет ответы |
| **Primary Node** | Единственный узел, на котором работают процессоры с планированием *Primary Node Only* (например, `List*`, `QueryDatabaseTableRecord`) |
| Остальные узлы | Выполняют flow, участвуют в load balancing |

Выбор Coordinator и Primary Node реализуется через **leader election**: по умолчанию через Apache ZooKeeper; в 2.x доступна также реализация на **Kubernetes Leases/ConfigMaps** (Kubernetes-based leader election и state management введены в 2.0.0-M1, [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)). В 2.6.0 интеграция с ZooKeeper вынесена в отдельный NAR-модуль ([Release Notes 2.6.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).

ZooKeeper используется также как **cluster state provider**: он хранит *cluster-scoped* state компонентов (например, указатели `List*`). *Local* state хранится на диске узла (`state/local`).

**Flow election.** При старте кластера узлы голосуют за «правильный» flow: ждут `nifi.cluster.flow.election.max.wait.time` или `nifi.cluster.flow.election.max.candidates` узлов; побеждает flow с наибольшим числом голосов. Узел, чей flow отличается, при подключении **синхронизируется** с кластерным flow, предварительно сохранив резервную копию своего (с 1.16.0 допускается подключение даже при существенных различиях, [Release Notes 1.16.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).

Ключевые свойства `nifi.properties` для кластера:

```properties
nifi.cluster.is.node=true
nifi.cluster.node.address=nifi-1.example.com
nifi.cluster.node.protocol.port=11443
nifi.cluster.flow.election.max.wait.time=5 mins
nifi.cluster.flow.election.max.candidates=3
nifi.cluster.load.balance.host=nifi-1.example.com
nifi.cluster.load.balance.port=6342
nifi.zookeeper.connect.string=zk1:2181,zk2:2181,zk3:2181
nifi.zookeeper.root.node=/nifi
nifi.state.management.configuration.file=./conf/state-management.xml
```

Особенности 2.x:

- Начиная с **2.12.0**, при включённом TLS свойства `nifi.cluster.load.balance.host` и `nifi.cluster.node.address` **должны** быть DNS-именами (для TLS Endpoint Identification) ([Migration Guidance, 2.12.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).
- Все узлы кластера должны иметь корректные сертификаты (со SAN, соответствующим имени хоста); взаимная аутентификация (mTLS) между узлами обязательна (с 1.8.0 защищённая связь требует двустороннего SSL).
- Встроенный ZooKeeper подходит только для лабораторных; в production используйте внешний ансамбль из 3 или 5 узлов.
- Для проверки: UI → **Cluster** (меню) показывает узлы, роли, heartbeat, статусы (CONNECTED, DISCONNECTED, OFFLOADED).

### 1.4.3 Load balancing между узлами

Load balancing настраивается **на уровне Connection** (с 1.8.0): вкладка *Settings → Load Balance Strategy*.

| Стратегия | Поведение | Когда использовать |
|---|---|---|
| `Do not load balance` | FlowFile остаётся на узле, где создан | Источники, дающие равномерную нагрузку на каждом узле |
| `Round robin` | Равномерно по узлам | Выравнивание после источника на одном узле (`List*`, `ConsumeKafka` с малым числом партиций) |
| `Partition by attribute` | Значение атрибута определяет узел (хеш) | Нужна «привязка» связанных данных к узлу (агрегация, `MergeRecord` по ключу, порядок по ключу) |
| `Single node` | Все FlowFile на один узел | Сериализация доступа к ресурсу, глобальная агрегация |

Дополнительно: сжатие (`Load Balance Compression`: не сжимать / только атрибуты / контент и атрибуты). Балансировка идёт через порт `nifi.cluster.load.balance.port` (6342 по умолчанию), его нужно открыть между узлами.

Практические замечания: load balancing — это дополнительный сетевой I/O и запись в репозитории принимающего узла; не включайте его «на всякий случай» на каждой связи. Типичное решение — один раз распределить данные после источника, дальше обрабатывать локально. Начиная с 2.3.0 исправлено распределение FlowFile при `partition by attribute` ([Release Notes 2.3.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)); после 2.10.0 исправлены редкие проблемы при offload/балансировке ([Release Notes 2.10.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).

**Offload.** Перед обслуживанием узел можно **Offload**: он прекращает приём новых данных и передаёт свои FlowFile другим узлам; после этого узел безопасно останавливать (Cluster dialog → Disconnect → Offload).

### 1.4.4 Варианты развёртывания: Docker, Helm, Ansible

| Вариант | Что есть официально | Комментарий |
|---|---|---|
| **Docker** | Официальные образы [`apache/nifi`](https://hub.docker.com/r/apache/nifi) | Образ настраивается переменными окружения (в том числе `SINGLE_USER_CREDENTIALS_USERNAME`/`PASSWORD`, `NIFI_JVM_HEAP_INIT`/`NIFI_JVM_HEAP_MAX`, `NIFI_WEB_HTTPS_PORT`, `NIFI_SENSITIVE_PROPS_KEY`, `NIFI_ELECTION_MAX_WAIT`, `NIFI_ELECTION_MAX_CANDIDATES`); каталог установки — `/opt/nifi/nifi-current`. Версию Java смотрите в описании выбранного тега образа (для 2.x нужна Java 21) |
| **Kubernetes / Helm** | Насколько известно, Apache не поставляет официальный Helm chart | Используются сообщественные решения: [cetic/helm-nifi](https://github.com/cetic/helm-nifi) — **архивирован и не поддерживается**, последняя версия chart ориентирована на NiFi 1.23.2; операторы [Stackable](https://stackable.tech/en/apache-nifi2-key-updates-stackable/) и [NiFiKop](https://github.com/konpyutaika/nifikop). Отсутствие официального chart подтверждено обсуждением в [рассылке пользователей NiFi (ноябрь 2024)](https://www.mail-archive.com/users@nifi.apache.org/msg16771.html); на текущий момент проверьте повторно. Для кластера в Kubernetes в 2.x есть Kubernetes leader election (не нужен ZooKeeper) |
| **Ansible** | Нет официальной роли в проекте (**[проверить]**) | Используйте собственные роли/сообщественные: установка Java 21, распаковка дистрибутива, шаблонизация `nifi.properties`, сертификаты |

Docker, минимальная одноузловая лаборатория (учебный режим; для production — свои сертификаты и секреты):

```bash
docker run --name nifi -p 8443:8443 -d \
  -e SINGLE_USER_CREDENTIALS_USERNAME=admin \
  -e SINGLE_USER_CREDENTIALS_PASSWORD=ChangeMe-Min12Chars! \
  apache/nifi:latest
```

Пароль Single User должен быть длиной не менее 12 символов, иначе NiFi сгенерирует случайные имя и пароль (поведение описано в README образа; сгенерированные данные пишутся в лог: `docker logs nifi | grep Generated`). UI доступен по `https://localhost:8443/nifi`; сертификат самоподписанный.

Рекомендации по контейнерам:

- Репозитории и `conf/` вынесите на **volumes**; контейнер должен быть заменяемым, данные — нет.
- `nifi.sensitive.props.key` должен быть **одинаковым** на всех узлах кластера и храниться в секрет-хранилище.
- Закрепляйте точную версию образа (не `latest`) для воспроизводимости.
- Для Kubernetes обеспечьте **stable identity** (StatefulSet) и persistent volumes для репозиториев каждого узла.

### 1.4.5 Конфигурационные файлы, репозитории и их расположение

Структура установки (`NIFI_HOME`):

```text
NIFI_HOME/
├── bin/                         nifi.sh (start|stop|restart|status|dump|diagnostics ...), nifi.cmd (Windows)
├── conf/
│   ├── nifi.properties          основная конфигурация (порты, TLS, репозитории, кластер)
│   ├── bootstrap.conf           JVM-аргументы, Java, параметры bootstrap
│   ├── logback.xml              логирование
│   ├── state-management.xml     провайдеры state (local / cluster)
│   ├── authorizers.xml          authorizer, user group provider, access policy provider
│   ├── login-identity-providers.xml   LDAP / single-user и т. д.
│   ├── flow.json.gz             текущий flow (в 2.x — единственный формат)
│   ├── users.xml, authorizations.xml  внутренние пользователи и политики (File providers)
│   └── archive/                 архив предыдущих версий flow
├── lib/                         библиотеки и NAR фреймворка
├── extensions/                  пользовательские NAR (горячая загрузка)
├── python/                      Python-расширения (2.x)
├── flowfile_repository/
├── content_repository/
├── provenance_repository/
├── database_repository/         история изменений flow (Flow Configuration History)
├── state/                       local state
├── work/                        распакованные NAR, документация
└── logs/                        nifi-app.log, nifi-bootstrap.log, nifi-user.log, nifi-request.log, nifi-deprecation.log
```

Факты о конфигурации:

- В 2.x `flow.xml.gz` не используется; в `nifi.properties` свойство `nifi.flow.configuration.file` должно ссылаться на `flow.json.gz` ([Migration Guidance, 2.0.0-M1](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).
- NiFi **архивирует** предыдущие версии flow в `conf/archive` (`nifi.flow.configuration.archive.enabled`, `.max.time`, `.max.storage`) — это ваша первая линия отката конфигурации (Block 3.1/3.2).
- `nifi.sensitive.props.key` — ключ шифрования чувствительных свойств в `flow.json.gz`. **Потеря ключа = невозможность расшифровать sensitive-значения** (потребуется ввести их заново). Он обязателен с 1.14.0 ([Migration Guidance, 1.13→1.14](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)).
- JVM-параметры (heap и т. д.) — `bootstrap.conf` (`java.arg.N`). В поставке по умолчанию heap небольшой (порядка 512 МБ); для реальных нагрузок необходимо увеличить (Block 3.4).
- Порты (по умолчанию): web HTTPS `8443`; load balancing `6342`; cluster node protocol — задаётся `nifi.cluster.node.protocol.port`; Site-to-Site — `nifi.remote.input.socket.port`. Полный перечень — Administration Guide.
- Репозитории размещайте на **отдельных томах** (минимум: FlowFile Repository отдельно от Content; Provenance отдельно) — подробнее в Block 3.4.

### 1.4.6 Версионирование flow и управление изменениями

**Flow Registry Client** — подключаемый компонент для хранения версий Process Group. Поддерживаемые реализации (с историей):

| Клиент | Статус / версия |
|---|---|
| **NiFi Registry** | Исторически стандартный реестр; **[dep]** объявлен deprecated в **2.8.0** с рекомендацией использовать git-based клиентов ([Release Notes 2.8.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes), [Migration Guidance 2.8.0](https://cwiki.apache.org/confluence/display/NIFI/Migration+Guidance)) |
| **GitHub** | Доступен с 2.0.0-M3 (GitHub-репозиторий как «реестр»; ветки поддерживаются) |
| **GitLab** | Добавлен в 2.0.0 |
| **Bitbucket** | Добавлен в 2.4.0 (Data Center — с 2.7.0) |
| **Azure DevOps (Git)** | Добавлен в 2.6.0 |

(Источники: [Release Notes](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes).) Начиная с 2.10.0: поддержка создания веток и *Author+Committer* для git-клиентов; в 2.11.0 — начальная поддержка rebase.

**Рабочий процесс.**

1. Process Group → **Version → Start version control**: выбираете клиент, bucket/ветку, имя flow.
2. После изменений группа показывает значок *locally modified*; **Show local changes** отображает diff; **Commit local changes** фиксирует новую версию; **Revert local changes** откатывает.
3. На другом окружении: **Import from Registry** или **Change version** на нужную версию. NiFi сам останавливает затронутые компоненты и применяет изменения.
4. Значения параметров: с 2.5.0 есть опции обработки значений параметров при версионировании в git-клиентах (чтобы не коммитить окружение-специфичные значения) и нормализация JSON flow definitions.

**Практики change management.**

- Окружения dev → test → prod; одна и та же версия flow, различие — только в Parameter Context.
- Секреты — только sensitive parameters / Parameter Providers; не в Git.
- Review изменений в git (pull requests) вместо правок в UI на prod; на prod ограничить права на изменение (Block 3.3).
- **Flow Analysis Rules** (API введён в 2.0.0-M1) позволяют автоматически проверять соблюдение стандартов проектирования (в Apache NiFi 2.6.0 доступны `DisallowComponentType`, `RestrictBackpressureSettings`, `RequireServerSSLContextService`, `RestrictFlowFileExpiration` — подробно в Day 3, раздел 3.2.6).
- XML Templates удалены в 2.0; вместо них — JSON flow definitions и версионирование (**[rem 2.0]**).
- Копирование flow между независимыми развёртываниями: с 2.1.0 UI поддерживает copy/paste flow definitions ([Release Notes 2.1.0](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)).

#### Stateless execution (2.x)

С 2.0 любой Process Group можно настроить на **Stateless** выполнение: группа выполняется как транзакция «от источника до стока» без записи промежуточных FlowFile в репозитории; при сбое транзакция откатывается целиком. Это даёт at-least-once/exactly-once-подобную семантику для Kafka→…→Kafka и подобных цепочек, но ограничивает параллелизм/размеры (данные в памяти) ([Release Notes 2.0.0-M1](https://cwiki.apache.org/confluence/display/NIFI/Release+Notes)). Отметим, что в 2.13.0 API добавлена поддержка in-memory content для Stateless-движка (**[exp]**/новая функциональность — **[проверить]** перед использованием в production).

---

## Block 1.5: Day 1 recap and Q&A

### Ключевые выводы

1. **Архитектура.** Flow Controller + три репозитория; контент неизменяем (copy-on-write); at-least-once внутри узла; durability настраивается.
2. **FlowFile.** Атрибуты в heap/WAL, контент на диске; мелкие FlowFile — враг производительности.
3. **Построение flow.** List/Fetch, `RouteOnAttribute`, EL; back pressure защищает систему, но один заполненный выход останавливает процессор.
4. **Record-based.** Один FlowFile = много записей; Reader/Writer/Schema — основа; Infer Schema допустим для прототипов, в production — явные схемы.
5. **Установка и кластер.** Узлы независимы; координатор и primary избираются; load balancing — на уровне связи; TLS и корректные DNS-имена обязательны.
6. **Версионирование.** Parameter Contexts + Flow Registry Clients (git); NiFi Registry deprecated с 2.8.0.

### Вопросы для самопроверки

1. Почему `UpdateAttribute` дешевле `ReplaceText`?
2. Что произойдёт с процессором, если очередь по связи `failure` достигла порога back pressure, а `success` свободна?
3. В каких случаях `Infer Schema` приводит к разным схемам у соседних FlowFile?
4. Чем `Partition by attribute` отличается от `Round robin` и когда нужен каждый?
5. Какие данные теряются безвозвратно при потере `nifi.sensitive.props.key`?

### Предпросмотр Day 2

Подключения к БД, REST и Kafka (на Record-процессорах), преобразования JSON/NDJSON, доставка в Splunk, а также миграция с NiFi 1.x на 2.x.
