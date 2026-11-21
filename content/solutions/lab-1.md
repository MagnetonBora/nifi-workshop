---
title: "Lab 1 — The NiFi UI and a first flow"
aliases:
  - exercises/lab-1-solution
  - labs/lab-1-solution
---

> From the materials of [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-1|Lab 1 — The NiFi UI and a first flow]].

**Solution architecture.**

```text
GenerateFlowFile ──success──► UpdateAttribute ──success──► LogAttribute (auto-terminate success)
```

**Processors and configuration.**

| Processor | Property | Value |
|---|---|---|
| `GenerateFlowFile` | Scheduling → Run Schedule | `5 sec` |
| | File Size | `0 B` (if Custom Text is set) |
| | Custom Text | `{"id": ${random():mod(1000)}, "ts": "${now():format("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")}"}` |
| | Mime Type | `application/json` |
| `UpdateAttribute` | Dynamic property `lab.stage` | `ingested` |
| | Dynamic property `event.id` | `${UUID()}` |
| `LogAttribute` | Log Level | `info`; Relationship `success` → Auto-terminate |

**What happens to the FlowFile.** `GenerateFlowFile` creates the FlowFile (a `CREATE` event); `UpdateAttribute` records `ATTRIBUTES_MODIFIED`; `LogAttribute` writes to `nifi-app.log` and the FlowFile terminates (`DROP`).

**Common mistakes.** The processor keeps a yellow triangle (it is invalid): there are no connected/auto-terminated relationships, or required properties are not set. The queue "does not move": the destination is stopped or back pressure has been reached. Instead of a date, the content contains the raw text `${now():format(...)}` — Expression Language did not parse the expression: write the format string with the literal `'T'` in **double** quotes (`format("yyyy-MM-dd'T'...")`); the variant `format('yyyy-MM-dd\'T\'...')` is not evaluated in NiFi 2.12. The processor remains valid in this case — the error is visible only in the FlowFile content.

**Reference solution:** [lab1-first-flow.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab1-first-flow.json) ([[materials/Appendix-A#a8-reference-solutions|Appendix A.8]]).
