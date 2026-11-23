---
title: "Lab 3 — A Split flow versus a Record flow"
aliases:
  - exercises/lab-3
---

> From the materials of [[materials/day-1|Day 1 — Apache NiFi Fundamentals]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Solution: [[solutions/lab-3|Lab 3 — solution]].

**Goal.** Run two flows that do the same job in different ways, measure them, and explain the difference.

**The two variants.** Both read the same input: `GenerateRecord` creates one FlowFile with 10,000 random orders (`order_id`, `customer`, `amount`, `currency`). Both keep the orders with `amount > 0` and `currency` EUR or USD, and mark them `stage = processed`.

- **A — Split-based:** `SplitRecord` turns the FlowFile into one FlowFile per record, each record goes through `EvaluateJsonPath` → `RouteOnAttribute` → `UpdateAttribute`, and `MergeContent` collects the valid records back.
- **B — Record-based:** `QueryRecord` splits the whole set into `valid` and `rejected` with SQL, `UpdateRecord` sets the field in every record.

## Steps

1. **Import the flow.** It contains both variants as the groups `A - Split-based` and `B - Record-based`:

   ```bash
   make import f=nifi/flows/lab3-split-vs-record.json
   ```

   Open the group *Lab 3 - Split vs Record* → right-click → **Enable all controller services**. Do not start the processors: the measurement starts them itself. Look at both variants and find where they differ.

2. **Run the measurement.**

   ```bash
   make lab03-measure
   ```

   The command runs A, then B. For each variant it starts every processor except the generator, triggers the generator with *Run Once*, waits until the data has reached every `LogAttribute`, and stops the group. The report is printed to the terminal and saved to `nifi/data/results/lab3-measure-<date-time>.txt`. Before running, both groups must be stopped and their queues empty.

3. **Read the report.** For each variant there is a table per processor and a summary at the end:

   | Column / row | Meaning |
   |---|---|
   | `tasks`, `time, ms` | how many times NiFi ran the processor and how much thread time it spent |
   | `in`, `out` | FlowFiles the processor received and sent |
   | `provenance` | provenance events the processor produced |
   | `wall time, s` | from *Run Once* until the last FlowFile was processed |
   | `peak queued` | the largest number of FlowFiles waiting in the group's queues |
   | `heap growth, MB` | peak heap during the run minus the heap before it |

4. **Look at the Grafana dashboard.** `http://localhost:3000` → folder *NiFi playground* → **Lab 3: Split vs Record**: queues, task time, heap and GC, FlowFiles and CPU time per processor for both variants. Prometheus scrapes NiFi every 15 seconds, so short spikes may be missed: the dashboard shows the trend, the report gives the exact numbers.

5. **Increase the volume.** Set *Number of Records* to 100,000 in both generators and run the measurement again. Watch the heap and the queues of variant A in Grafana. Do not go much higher: variant A turns every record into a FlowFile, and millions of FlowFiles do not fit into the stand's heap (2 GB).

6. **Repeat** each volume 3 times and take the median.

**Keep in mind.**

- NiFi counts the processor statistics over a rolling 5-minute window. Wait 5 minutes before running the measurement again, otherwise the previous run drops out of the window during the measurement and skews the numbers.
- The wall time of variant A includes the `MergeContent` wait: valid records are fewer than *Minimum Number of Entries*, so the bin is closed by *Max Bin Age* (10 s).
- The heap belongs to the whole NiFi, not to one group, and depends on GC: compare the trend, not the exact megabytes.

## Conclusions

Write down the results in a table (A, B, how many times A is bigger) for both volumes, then answer:

1. How many FlowFiles does variant A create from one input FlowFile? Where in the report do you see it?
2. Which processor of variant A spends the most time, and why?
3. Why does variant B show `peak queued` 0?
4. How do the numbers change when the volume grows 10 times, for A and for B?
5. Where does variant A still make sense, and what replaces Split + Merge in NiFi 2.x?

**Correctness check.** Both variants must split the records the same way: valid + rejected = the input count. In variant A: `in` of `MergeContent` (valid) plus `in` of `LogAttribute (rejected, A)`. In variant B: the `record.count` attribute of the two FlowFiles after `QueryRecord` (☰ → **Data Provenance**, or `make logs s=nifi | grep record.count`).

**The numbers must be obtained in your environment.** The absolute values depend on the hardware, only the ratio between A and B matters.

## Without the stand

Build both variants yourself (the settings are in the [[solutions/lab-3|solution]]) and record the same metrics by hand after each *Run Once*: *Tasks/Time* and *In/Out* on the processor cards, the queue peak from **View Status History** of the connection after the split, *Used Heap* from ☰ → **Node Status History**, the number of events from ☰ → **Data Provenance** limited to the run's time window.
