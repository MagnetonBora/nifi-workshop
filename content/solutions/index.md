---
title: Solutions
---

Solutions for the workshop labs. Each page explains the architecture, the configuration of the components, what
happens to the data, and common mistakes. Try to build the flow yourself first, then compare it with the solution.

## Solutions

| Solution | Lab | Day | Reference Flow Definition |
|---|---|---|---|
| [[solutions/lab-1\|Lab 1 — The NiFi UI and a first flow]] | [[labs/lab-1\|Task]] | Day 1 | [lab1-first-flow.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab1-first-flow.json) |
| [[solutions/lab-2\|Lab 2 — Several sources and routing]] | [[labs/lab-2\|Task]] | Day 1 | [lab2-multi-source-routing.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab2-multi-source-routing.json) |
| [[solutions/lab-3\|Lab 3 — A Split flow versus a Record flow]] | [[labs/lab-3\|Task]] | Day 1 | [lab3-split-vs-record.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab3-split-vs-record.json) |
| [[solutions/lab-4\|Lab 4 — Connecting NiFi to a database]] | [[labs/lab-4\|Task]] | Day 2 | [lab4-db-basics.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab4-db-basics.json) |
| [[solutions/lab-5\|Lab 5 — A database + REST + Kafka with record processing]] | [[labs/lab-5\|Task]] | Day 2 | [lab5-db-rest-kafka.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab5-db-rest-kafka.json) |
| [[solutions/lab-6\|Lab 6 — An NDJSON transformation pipeline]] | [[labs/lab-6\|Task]] | Day 2 | [lab6-ndjson-pipeline.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab6-ndjson-pipeline.json) |
| [[solutions/lab-7\|Lab 7 — Delivering events to Splunk]] | [[labs/lab-7\|Task]] | Day 2 | [lab7-splunk-delivery.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab7-splunk-delivery.json) |
| [[solutions/lab-8\|Lab 8 — Retry and error handling]] | [[labs/lab-8\|Task]] | Day 3 | [lab8-retry-errors.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab8-retry-errors.json) |
| [[solutions/lab-9\|Lab 9 — A performance test and tuning]] | [[labs/lab-9\|Task]] | Day 3 | [lab9-performance.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab9-performance.json) |
| [[solutions/final-lab\|Final lab — an end-to-end pipeline]] | [[labs/final-lab\|Task]] | Day 3 | [lab10-final-pipeline.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab10-final-pipeline.json) |

## Loading a reference solution

Every lab has a verified Flow Definition: a ready-made Process Group with a Parameter Context that already contains
the addresses of the lab stand (see [[labs/index#lab-stand|Lab stand]]).

- **In the UI:** download the file from the table, drag the **Process Group** icon onto the canvas, and click the
  upload file icon.
- **From the terminal:** `make import f=nifi/flows/<file>.json` (for a cluster: `make import-cluster`).

After the import, enable the group's Controller Services (*Enable all controller services*) and start it.

The list of reference files on the stand is in [[materials/Appendix-A#a8-reference-solutions|Appendix A.8]].
