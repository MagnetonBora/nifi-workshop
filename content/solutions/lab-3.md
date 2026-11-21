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

**Common mistakes.** `QueryRecord` fails with an SQL error — the table name must be `FLOWFILE`; the field type did not match (decimal vs double) — set the schema explicitly; the error `Values passed to NOT IN operator must have compatible types` — the field is declared as an enum in the Reader schema, declare it as a string; in variant A, `MergeContent` waits for the required number of elements — set *Max Bin Age*. Identical processor names in variants A and B (for example, two `LogAttribute (valid)`) cause confusion when searching in provenance — give them different names.

**Reference solution:** [lab3-split-vs-record.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab3-split-vs-record.json).
