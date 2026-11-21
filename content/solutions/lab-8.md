---
title: "Lab 8 — A performance test and tuning"
aliases:
  - exercises/lab-8-solution
  - labs/lab-8-solution
---

> From the materials of [[materials/day-3|Day 3 — Production, Security, and Performance]]. The lab stand is described on the [[labs/index|Labs]] page; loading the reference solutions — on the [[solutions/index|Solutions]] page.
> Task: [[labs/lab-8|Lab 8 — A performance test and tuning]].

**Common mistakes.** Measuring without a stable input; changing several parameters at once; ignoring JVM warm-up (the first minutes are not representative); comparing without identical data; output to disk without a volume limit (it fills the disk within minutes).

**Reference solution:** [lab8-performance.json](https://github.com/MagnetonBora/nifi-playground/blob/master/nifi/flows/lab8-performance.json).
