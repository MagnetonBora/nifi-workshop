---
title: "Lab 3 — A Split flow versus a Record flow"
aliases:
  - exercises/lab-3-solution
  - labs/lab-3-solution
---

> From the materials of [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-3|Lab 3 — A Split flow versus a Record flow]].

**Two schemas** are needed. The generator schema (`Schema Text` of `GenerateRecord`, the Writer is `JsonRecordSetWriter` with `Output Grouping = One Line Per Object`): `currency` is declared as an enum, otherwise the generator produces random strings and the currency filter will not work:

```json
{"type": "record", "name": "GeneratedOrder", "fields": [
  {"name": "order_id", "type": "string"},
  {"name": "customer", "type": "string"},
  {"name": "amount",   "type": "double"},
  {"name": "currency", "type": {"type": "enum", "name": "Currency", "symbols": ["EUR", "USD", "GBP"]}}
]}
```

The Reader schema (`JsonTreeReader`, `Use 'Schema Text' Property`) is the same, but `currency` is declared as a string: the SQL engine of `QueryRecord` (Apache Calcite) does not compare an enum with strings in `IN` / `NOT IN`:

```json
{"type": "record", "name": "Order", "fields": [
  {"name": "order_id", "type": "string"},
  {"name": "customer", "type": "string"},
  {"name": "amount",   "type": "double"},
  {"name": "currency", "type": "string"}
]}
```

**Variant A (Split-based).**

```text
GenerateRecord ► SplitRecord(Records Per Split=1) ► EvaluateJsonPath ► RouteOnAttribute ► UpdateAttribute ► MergeContent ► LogAttribute
```

**Variant B (Record-based).**

```text
GenerateRecord ► QueryRecord ─ valid ─► UpdateRecord ► LogAttribute
                              └ rejected ─► LogAttribute
```

QueryRecord, the dynamic property `valid`:

```sql
SELECT order_id, customer, amount, currency
FROM FLOWFILE
WHERE amount > 0 AND currency IN ('EUR', 'USD')
```

and `rejected`:

```sql
SELECT * FROM FLOWFILE WHERE amount <= 0 OR currency NOT IN ('EUR', 'USD')
```

**Measurements.** On the stand, after `make import f=nifi/flows/lab3-split-vs-record.json` and *Enable all controller services* on the group *Lab 3 - Split vs Record*:

```bash
make lab3-measure        # runs A, then B; report in the terminal and in nifi/data/results/
```

The measurement is the script `nifi/common/lab3-measure.py` in a one-off Python container. For each variant it starts every processor except the generator, does *Run Once* on `GenerateRecord`, waits until the queues are empty and every `LogAttribute` has received data, then stops the group. It takes the processor statistics before and after the run (the difference gives *Tasks/Time* and *In/Out* of this run), samples the queues and the heap every 0.25 s, and counts provenance events per processor. The Grafana dashboard **Lab 3: Split vs Record** shows the same picture over time.

Results template (repeat 3 times per volume and take the median):

| Metric | A (Split) | B (Record) | A / B |
|---|---|---|---|
| Wall time, s | | | |
| Tasks | | | |
| Task time, ms | | | |
| Provenance events | | | |
| Peak queued | | | |
| Heap growth, MB | | | |

**What to expect.** An example from a laptop with 100,000 records per run (your numbers will differ, the ratio should not):

- Variant A: `SplitRecord` sends 100,000 FlowFiles, `EvaluateJsonPath` and `RouteOnAttribute` receive 100,000 each, and the rest of the chain receives the valid or rejected part. In total the processors of A handle about 350,000 FlowFiles per run; the processors of B handle 4.
- The queues of A held up to ~90,000 FlowFiles at once; B shows 0.
- Task time of A was about 15 times that of B; provenance events differ by orders of magnitude (one or more per FlowFile per processor in A, a handful in B).
- The wall time of A includes up to 10 s of `MergeContent` waiting for *Max Bin Age*.

**Answers to the questions.**

1. One FlowFile per record (*Records Per Split* = 1): `out` of `SplitRecord` and `in` of `EvaluateJsonPath` equal the input record count.
2. The processors that see every FlowFile: usually `RouteOnAttribute`, `SplitRecord`, `UpdateAttribute` and `LogAttribute (rejected, A)`, which writes a log line per FlowFile. The work per record is trivial; the cost is the per-FlowFile overhead: a session commit, a FlowFile Repository entry, attributes in the heap, provenance events.
3. Variant B has one FlowFile at each step and processes it in milliseconds: nothing waits in a queue long enough to be seen by a sample.
4. In variant A tasks, provenance events, queue peak and heap grow linearly with the record count. In variant B the number of FlowFiles, tasks and provenance events stays the same; only the processing time grows with the amount of data.
5. Split is justified when each record really has to be handled separately (a per-record call to an external system, per-record routing to different destinations) or the format has no Record Reader. Otherwise use Record processors: `QueryRecord`, `UpdateRecord`, `PartitionRecord`, `LookupRecord`, `ForkRecord`, and `MergeRecord` instead of `MergeContent` if batches have to be built.

**Correctness check.** valid + rejected = the input count: in A, `in` of `MergeContent` plus `in` of `LogAttribute (rejected, A)`; in B, `record.count` of the two FlowFiles after `QueryRecord` (provenance or `make logs s=nifi | grep record.count`). The split is about half and half, for example 4910 valid + 5090 rejected out of 10,000.

**Common mistakes.** `make lab3-measure` waits forever — a controller service is disabled or a processor is invalid, so the data never reaches a `LogAttribute`: stop it with Ctrl+C, fix the group, empty the queues; `tasks` / `time` of a repeated run are too small or even negative — the previous run of the same variant was less than 5 minutes ago and dropped out of the statistics window during the measurement: wait 5 minutes between runs; `QueryRecord` fails with an SQL error — the table name must be `FLOWFILE`; the field type did not match (decimal vs double) — set the schema explicitly; the error `Values passed to NOT IN operator must have compatible types` — the field is declared as an enum in the Reader schema, declare it as a string; in variant A, `MergeContent` waits for the required number of elements — set *Max Bin Age*. Identical processor names in variants A and B (for example, two `LogAttribute (valid)`) cause confusion when searching in provenance — give them different names.

**Reference solution:** [lab3-split-vs-record.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab3-split-vs-record.json).
