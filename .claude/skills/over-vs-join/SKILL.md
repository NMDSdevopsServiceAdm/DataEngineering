---
name: over-vs-join
description: Use when choosing or reviewing between a Polars .over() window function and a join-based rewrite (join_asof, group_by+join, semi/anti join) for streaming support or memory reasons. Triggers on ".over()", "window function", "join_asof", "streaming-friendly rewrite", or an OOM investigation touching either.
---

# `.over()` vs join-based rewrites — memory tradeoff

**"Not on the streaming-engine list" is not a proxy for "will OOM"** — they're different questions and can point in opposite directions. Confirmed by direct measurement during a 2026-07-16 OOM investigation on the imputation pipeline (see `merge_ascwds_and_pir_filled_post_submissions` and `split_dataset_for_imputation` in this repo for the worked examples):

- **`.over()`'s in-memory fallback** computes and writes its result in place — measured at ~0 extra peak memory beyond holding the frame, whether it's a simple aggregate or an order-dependent shift/cumsum.
- **A join that broadcasts a computed value back onto every row** (a left-join, or `join_asof` — the usual "streaming-friendly" `.over()` replacement) needs a hash/sorted structure *and* a new merged output frame. Measured several GB more peak memory than the equivalent `.over()` on a ~4M-row wide frame — a real, fan-out-free, 1:1 join. This is what caused the production OOM: converting `.over()` to `join_asof`/`group_by`+join to "fix" memory made it worse, not better.
- **Row-filtering joins (`how="semi"`/`"anti"`) avoid the merged-output cost**, since they don't attach new columns — this is why converting `split_dataset_for_imputation`'s `.over()` to a semi/anti join *did* fix an OOM there. But recombining split frames afterwards (`pl.concat`) has its own real memory cost — don't assume a join is free just because it's semi/anti.

## Before converting a `.over()` to a join to fix a memory issue: compare, don't assume

1. **Check which engine actually runs each version:**
   ```bash
   POLARS_VERBOSE=1 python -c "your_lf.collect(engine='streaming')"
   ```
   Look for a genuine streaming node (e.g. `sorted-group-by`) vs. a fallback (`in-memory-join`/`in-memory-map`). Node names alone don't tell you the memory cost, so:
2. **Measure actual peak memory for both versions**, on a synthetic frame shaped like the real data (row count *and* column count — sort/join cost scales with frame width, not just row count):
   ```python
   import psutil
   proc = psutil.Process()
   before = proc.memory_info().peak_wset  # rss on Linux/Mac
   result = candidate_lf.collect(engine="streaming")
   print((proc.memory_info().peak_wset - before) / 1e6, "MB")
   ```
   Run each candidate in its **own fresh process** — peak memory only ever goes up within a process, so reusing one process across comparisons contaminates later results with earlier peaks.
3. **State which option you picked and why** (streaming support, measured memory, row-filter vs column-broadcast) when you write or review this kind of change.

## Retire this skill around 2026-10

This guidance is temporary — CLAUDE.md flags it as "remove after ~2026-10" once the team has built enough real examples to trust default judgement without measuring each time. When that note is removed from CLAUDE.md, delete this skill too (or fold the still-useful parts into `polars-streaming-check`).
