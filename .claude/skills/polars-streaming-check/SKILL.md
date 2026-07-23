---
name: polars-streaming-check
description: Use before relying on .collect(engine="streaming") for a large or OOM-prone Polars pipeline in this repo, or when asked "does this stream", "is this streaming-safe", "check streaming support". Refreshes the known non-streaming operation list against the upstream Polars tracking issue.
---

# Checking Polars streaming-engine coverage

`.collect(engine="streaming")` doesn't cover everything yet. Some operations silently fall back to the in-memory engine, which defeats the point on large data.

## Known non-streaming operations (last checked 2026-07-14 against [pola-rs/polars#20947](https://github.com/pola-rs/polars/issues/20947))

- **`.over()` (window functions):** only `.over(mapping_strategy="group_to_rows")` streams. Plain `.over()` (→ group-by+join) and `.over(keys)` with sorted keys do not yet — see the `over-vs-join` skill before rewriting one of these to chase streaming support, since the rewrite is not automatically a memory win.
- **Out-of-core (streams but must still fit in memory):** `group_by`, equi-joins, `sort`.
- **Aggregates:** `implode`, median/quantile, `str.join`.
- **Other expressions:** `.replace()`, `is_last_distinct`/`is_unique`/`is_duplicated`, `reshape`, `qcut`, `sample`, `pct_change`, `interpolate_by`, `ewm_*_by`, `fill_null(strategy="min"/"max"/"mean")`, `search_sorted`, `random`, `rank`, `arg_sort`, `hist`, rolling functions (`rolling_sum`/`std`/`var`/etc.), `group_by_dynamic` with a sorted key.
- **Sources/sinks:** `AnonymousScan`, anonymous sinks.

Assume anything not listed above streams natively.

## When to refresh this list

Refresh if either is true:
- It's been more than ~3 months since the date above.
- You're about to rely on streaming mode for an OOM-prone pipeline that uses one of the operations listed above, and want to confirm it's still accurate right now.

To refresh:

```bash
curl -s https://api.github.com/repos/pola-rs/polars/issues/20947 | python3 -c "import json,sys; print(json.load(sys.stdin)['body'])"
```

(no auth needed — it's a public issue). Diff the checkboxes against the list above. If anything moved, update **both** this file and the equivalent list in the repo's `CLAUDE.md`, and bump the "last checked" date in both places.
