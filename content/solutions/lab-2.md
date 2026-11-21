---
title: "Lab 2 — Several sources and routing"
aliases:
  - exercises/lab-2-solution
  - labs/lab-2-solution
---

> From the materials of [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-2|Lab 2 — Several sources and routing]].

**Architecture.**

```text
[PG: ingest]                                   [PG: route]
 ListFile → FetchFile ───────┐
 ListenHTTP (port 8081) ─────┼─► Output Port ─► RouteOnAttribute ─┬─ json  ─► (PG: process_json)
 GenerateFlowFile ───────────┘   "all-sources"                    ├─ csv   ─► (PG: process_csv)
                                                                  └─ unmatched ─► LogAttribute / PutFile (quarantine)
```

**Configuration.**

| Component | Key properties |
|---|---|
| `ListFile` | Input Directory `#{ingest.dir}`; File Filter `.*\.(json|csv)`; the default Listing Strategy (tracking timestamps) |
| `FetchFile` | File to Fetch `${absolute.path}/${filename}`; Completion Strategy `None` (lab mode) |
| `ListenHTTP` | Listening Port `8081`; Base Path `events`; **HTTP Headers for Attributes `Content-Type`**; for the lab without TLS (in production — an SSL Context Service) |
| `UpdateAttribute` (on every input) | `source` = `file` / `http` / `generator`; on the HTTP input additionally `mime.type` = `${'Content-Type':substringBefore(';'):trim()}` — `ListenHTTP` does not set `mime.type` itself |
| `GenerateFlowFile` | Custom Text — JSON; Mime Type `application/json` |
| `RouteOnAttribute` | Routing Strategy `Route to Property name`; `json`=`${mime.type:equals('application/json'):or(${filename:endsWith('.json')})}`, `csv`=`${filename:endsWith('.csv')}` |
| `PutFile` (quarantine) | Directory `#{quarantine.dir}` |
| Parameter Context `lab-params` | `ingest.dir=/opt/nifi/data/in`, `quarantine.dir=/opt/nifi/data/quarantine` (on the stand host — `nifi/data/in`, `nifi/data/quarantine`, see [[materials/Appendix-A#a5-addresses-for-the-labs|Appendix A.5]]) |

**Mistakes.** `ListenHTTP` does not respond — the port is taken or not forwarded into the container (on the stand, port 8081 is forwarded only for the single NiFi); `ListFile` does not see the files — the NiFi user's permissions on the directory; `unmatched` keeps growing — mime.type/filename are not set (check the attributes through List queue). A frequent case: HTTP events go to `unmatched` because `ListenHTTP` does not carry the `Content-Type` header into `mime.type` — you need `HTTP Headers for Attributes` and the `UpdateAttribute` from the table above.

**Reference solution:** [lab2-multi-source-routing.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab2-multi-source-routing.json).
