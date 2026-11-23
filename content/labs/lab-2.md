---
title: "Lab 2 — Several sources and routing"
aliases:
  - exercises/lab-2
---

> From the materials of [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-2|Lab 2 — solution]].

**Goal.** Collect data from three sources — files, HTTP and a generator — into one stream, mark each FlowFile with its source, and route by type: JSON, CSV or "unknown".

**Where the data comes from.**

| Source | On the stand | Attribute |
|---|---|---|
| Files | `ListFile` + `FetchFile` from `/opt/nifi/data/in` (on the host: `nifi/data/in`), only `*.json` and `*.csv` | `source=file` |
| HTTP | `ListenHTTP` on port `8081`, path `/events` (from the host: `http://localhost:8081/events`) | `source=http` |
| Generator | `GenerateFlowFile`, a JSON every 30 seconds | `source=generator` |

**Where it goes.**

| Route | Condition | Destination |
|---|---|---|
| `json` | `mime.type` is `application/json` or the file name ends with `.json` | group `process_json` (logs the content) |
| `csv` | the file name ends with `.csv` | group `process_csv` (logs the content) |
| `unmatched` | everything else | files in `/opt/nifi/data/quarantine` (on the host: `nifi/data/quarantine`) |

```text
ingest:  ListFile ► FetchFile ► source=file      ─┐
         ListenHTTP ► source=http                ─┼─► output port all-sources
         GenerateFlowFile ► source=generator     ─┘
                                                        │
route:   input port from-ingest ► RouteOnAttribute ─ json ─────► process_json
                                                   ├ csv ──────► process_csv
                                                   └ unmatched ► PutFile (quarantine)
```

## Before you start

```bash
make lab-setup        # creates nifi/data/in and nifi/data/quarantine
```

Parameter Context `lab2-params`: `ingest.dir=/opt/nifi/data/in`, `quarantine.dir=/opt/nifi/data/quarantine`.

## Steps

1. **Create the Parameter Context** `lab2-params` and bind it to the group *Lab 2* (*Configure Process Group → Parameter Context*); bind it to the child groups as well.
2. **Build the `ingest` group**: the three sources, after each an `UpdateAttribute` that sets `source`, all of them into one output port `all-sources`. For HTTP, copy the request header into an attribute: `ListenHTTP` → *HTTP Headers for Attributes* `Content-Type`, then in its `UpdateAttribute` `mime.type` = `${'Content-Type':substringBefore(';'):trim()}`.
3. **Build the `route` group**: input port `from-ingest` → `RouteOnAttribute` (*Route to Property name*) with the properties `json` and `csv` from the table above → `process_json`, `process_csv`, and `PutFile` into `#{quarantine.dir}` for `unmatched`. On the parent canvas connect `all-sources` to `from-ingest`.
4. **Set a prioritizer** on the connection into `RouteOnAttribute`: `NewestFlowFileFirstPrioritizer` — the newest FlowFiles are processed first (for demonstration).
5. **Start the whole group** and send data from each source (from the root of the stand repository):

   ```bash
   curl -X POST -H 'Content-Type: application/json' -d '{"id":1,"type":"click"}' http://localhost:8081/events
   curl -X POST -H 'Content-Type: text/plain' -d 'hello' http://localhost:8081/events
   echo '{"id":10,"type":"file"}' > nifi/data/in/sample.json
   printf 'id,type\n2,view\n' > nifi/data/in/sample.csv
   ```

   The generator works on its own, one JSON every 30 seconds. `ListFile` checks the directory every 10 seconds.

6. **Watch** the queues into `process_json` / `process_csv`, open a FlowFile and check its `source` and `mime.type` attributes; in provenance find the `ROUTE` event of `RouteOnAttribute`.

## Expected behavior

| What you sent | Route | Where you see it |
|---|---|---|
| HTTP with `Content-Type: application/json` | `json` | `process_json`, `source=http`, `mime.type=application/json` |
| HTTP with `Content-Type: text/plain` | `unmatched` | a file in `nifi/data/quarantine` |
| `sample.json` | `json` | `process_json`, `source=file` |
| `sample.csv` | `csv` | `process_csv`, `source=file` |
| the generator | `json` | `process_json`, `source=generator`, every 30 s |

## Verification

```bash
make logs s=nifi | grep "source"      # LogAttribute in process_json / process_csv logs attributes and content; Ctrl+C to stop
ls nifi/data/quarantine               # the text/plain request
```

## Re-running

`ListFile` remembers which files it has already listed: a file with the same name and an unchanged timestamp is not picked up again — write a new file or change it. Clear the quarantine: `rm nifi/data/quarantine/*`.

Port `8081` is available only on the single NiFi (`make up`), not in the cluster.

Reference flow: `make import f=nifi/flows/lab2-multi-source-routing.json`.
