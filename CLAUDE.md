# DataEngineering — CLAUDE.md

Skills for Care Workforce Intelligence Team. Builds reproducible AWS data pipelines from adult social care workforce data (ASC-WDS). See [README.md](README.md) for full background.

## Stack

- Python 3.11.
- Two engines coexist in this repo:
  - `jobs/` folders — legacy **PySpark**.
  - `fargate/` folders — **Polars**, the target for all new work.
- We are mid-migration from PySpark to Polars. Do not introduce new PySpark code unless explicitly asked or working in a file already containing PySpark code. When migrating a function, add a comment on the old PySpark function pointing at its replacement: `# converted to polars -> filepath.py` (see `.github/PULL_REQUEST_TEMPLATE/polars_migration_template.md`).
- Tests are mid-migration from `unittest` to `pytest`. Write new tests in pytest style (see Testing below). Don't mass-rewrite passing unittest tests — migrate opportunistically when you're already touching a file.

## Scale is the top constraint

Datasets here are large in both rows and columns, and OOM is a recurring real problem. When writing or reviewing Polars code, correctness and "idiomatic" style are necessary but not sufficient — always also check for unnecessary materialisation. Concretely:

- Keep the pipeline lazy end-to-end; use `scan_parquet` / `scan_csv`, not `read_*`; `.collect()` once, at the end, not mid-pipeline.
- `.select()` / `.drop()` unneeded columns as early as possible — with wide datasets, column count matters as much as row count.
- Be deliberate around operations that can force materialisation or blow up memory: `.unique()` on high-cardinality columns, `.pivot()`, `.explode()`, joins with row fan-out, `.collect().to_pandas()` round-trips. Even a fan-out-free 1:1 join (e.g. broadcasting one computed value back onto every row) has real memory overhead of its own — see `.over()` vs join-based rewrites below.
- Prefer `Categorical` / `Enum` over `String` for low-cardinality repeated columns.
- Prefer `float32` over `float64` where precision requirements allow — call out explicitly when a trade-off matters (e.g. long aggregations/sums where error can accumulate).
- If a lazy chain is unintentionally broken (an early `.collect()`, a `.to_pandas()`, list-comprehension row-wise logic), flag it — that's a correctness-adjacent issue here, not a style nitpick. Some `.collect()` are ok as they directly follow an aggregation which results in a very small LazyFrame.

### Polars streaming engine — what isn't covered yet

`.collect(engine="streaming")` doesn't yet cover everything — some operations silently fall back to the in-memory engine, which defeats the point for large data. Below is what's **not yet natively streaming**, last checked 2026-07-14 against [pola-rs/polars#20947](https://github.com/pola-rs/polars/issues/20947) (the project's own tracking issue — check it, don't rely on general knowledge, since this moves fast). Assume anything not listed here streams natively.

- **`.over()` (window functions — relevant to our PySpark→Polars window-function migration):** only `.over(mapping_strategy="group_to_rows")` translates to streaming. Plain `.over()` → group-by+join, and `.over(keys)` with sorted keys, do not yet. Before rewriting a plain `.over()` this way to chase streaming support, read the memory caveat just below — the group-by+join rewrite is not automatically a memory win.
- **Out-of-core (i.e. runs in streaming mode but still needs to fit in memory):** `group_by`, equi-joins, `sort`.
- **Aggregates:** `implode`, median/quantile, `str.join`.
- **Other expressions:** `.replace()`, `is_last_distinct`/`is_unique`/`is_duplicated`, `reshape`, `qcut`, `sample`, `pct_change`, `interpolate_by`, `ewm_*_by`, `fill_null(strategy="min"/"max"/"mean")`, `search_sorted`, `random`, `rank`, `arg_sort`, `hist`, rolling functions (`rolling_sum`/`std`/`var`/etc.), `group_by_dynamic` with a sorted key.
- **Sources/sinks:** `AnonymousScan`, anonymous sinks.

**Refreshing this list:** re-check the issue every ~3 months, or immediately before relying on streaming mode for a pipeline that's OOM-prone and uses one of the operations above. Fetch the current checklist with:
```
curl -s https://api.github.com/repos/pola-rs/polars/issues/20947 | python3 -c "import json,sys; print(json.load(sys.stdin)['body'])"
```
(no auth needed — it's a public issue). Diff the checkboxes against this list and update it if anything's moved.

### `.over()` vs join-based rewrites — "not streaming" ≠ "uses more memory"

"Not on the streaming list above" is not a proxy for "will OOM" — they're different questions, and can point in opposite directions. Confirmed by direct measurement during a 2026-07-16 OOM investigation on the imputation pipeline:

- `.over()`'s in-memory fallback computes and writes its result in place — measured at ~0 extra peak memory beyond holding the frame, whether it's a simple aggregate or an order-dependent shift/cumsum.
- A join that **broadcasts a computed value back onto every row** (a left-join, or `join_asof` — the usual "streaming-friendly" `.over()` replacement) needs a hash/sorted structure *and* a new merged output frame. Measured several GB more peak memory than the equivalent `.over()` on a ~4M-row wide frame — real fan-out-free, 1:1 joins, not the row-fan-out case already flagged above. This is what caused a real production OOM in `merge_ascwds_and_pir_filled_post_submissions`. It was converted from `.over()` to "streaming-friendly" `join_asof`/`group_by`+join specifically to fix a memory issue, and that conversion made memory usage worse, not better.
- Row-filtering joins (`how="semi"`/`"anti"`) avoid the merged-output cost since they don't attach new columns — this is why converting `split_dataset_for_imputation`'s `.over()` to a semi/anti join *did* fix an OOM there (see that function for a worked example). But recombining split frames afterwards (`pl.concat`) has its own real memory cost, so don't assume a join is free just because it's a semi/anti join.

**Before converting a `.over()` to a join to fix a memory issue, compare — don't assume:**
1. Check which engine actually runs each version: `POLARS_VERBOSE=1 python -c "your_lf.collect(engine='streaming')"` — a genuine streaming node (e.g. `sorted-group-by`) vs. a fallback (`in-memory-join`/`in-memory-map`). Node names alone don't tell you the memory cost, so:
2. Measure actual peak memory for both versions, on a synthetic frame shaped like the real data (row count *and* column count — sort/join cost scales with frame width, not just row count):
   ```python
   import psutil
   proc = psutil.Process()
   before = proc.memory_info().peak_wset  # rss on Linux/Mac
   result = candidate_lf.collect(engine="streaming")
   print((proc.memory_info().peak_wset - before) / 1e6, "MB")
   ```
   Run each candidate in its own fresh process — peak memory only ever goes up within a process, so reusing one process across comparisons will contaminate later results with earlier peaks.

**Temporary, remove after ~2026-10:** when you write or review code that chooses between `.over()` and a join-based rewrite, state which one you picked and why (streaming support, measured memory, row-filter vs column-broadcast). The team is still building intuition here — once we have enough real examples to trust default judgement, drop this instruction.

## Polars style

- Expression-based and declarative; avoid `.apply()` unless there's no vectorised alternative.
- Avoid `.pipe()` — we find it harder to visually trace than a plain linear chain, and it makes it harder to drop in an intermediate `.collect().head()` or print for debugging. Prefer explicit intermediate LazyFrame variables instead (see LazyFrame naming below).
- Keep basic, simple transformations inline. Only extract into a function when it's genuinely complex, encodes non-obvious business logic, or is worth its own unit tests — not by default. This is a deliberate correction from old PySpark habits in this repo, where almost everything was wrapped in a function regardless of complexity, often making the function longer and harder to follow than the plain code inside it.

## LazyFrame naming

Keep the same `<entity>_lf` name for as long as it's still fundamentally the same entity — including through `.join()` and adding/amending columns; overwriting the same name (`workplace_lf = workplace_lf.join(...)`) is expected and fine, since it's still "the workplace data, with more columns." Only rename when an operation changes what the frame conceptually represents — e.g. a `.group_by().agg()` that collapses it into a summary is no longer "the workplace data," so give it a new name (`workplace_summary_lf`).

## Project layout

```
projects/
├── <project>/
│   ├── _01_ingest/          # numbered pipeline stage
│   │   ├── jobs/            # PySpark
│   │   ├── fargate/         # Polars
│   │   ├── utils/           # helpers used only within this stage
│   │   └── tests/           # mirrors jobs/fargate/utils
│   ├── utils/               # helpers shared across stages within this project
│   └── unittest_data/       # test fixtures shared across stages within this project
└── utils/                   # helpers shared across projects
```

Place a helper at the narrowest scope that covers its actual usage (stage → project → repo-wide) — check whether something similar already exists at that scope before adding a new one.

## Column names

Never hardcode column-name strings. Column-name classes belong in `utils/column_names` (shared across the repo), not per-project. Import under a short alias:

```python
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
```

## Testing

- New tests: pytest, following the pattern already in use —
  - a `class Test<Thing>:` wrapper (not `unittest.TestCase`)
  - cases built via a small `@dataclass` holding an `id` plus test data, converted with `.as_pytest_param()`... `@pytest.mark.parametrize`
  - descriptive scenario `id`s (e.g. `id="handles_nulls_in_rolling_window"`) instead of encoding data values into the test method name
  - assert with `polars.testing.assert_frame_equal`
- Name tests by behaviour/expectation, not by the specific input used.
- One behaviour per test.
- Cover the happy path, meaningful edge cases, and invalid input only where relevant — don't pad coverage with trivial cases.

## Division by zero

Don't add defensive zero-guards by default — upstream steps enforce that relevant divisors are "≥1, or null" before they reach division logic, so the zero case is already structurally excluded. Do flag it if a calculation is being generalised/extracted into a shared `utils` module for reuse elsewhere — genericising a function can silently break the invariant it was written under.

## Docstrings & naming

- Google-style docstrings (enforced via `pydoclint`, see `pyproject.toml`); document non-obvious performance considerations (e.g. why something is kept lazy, why a collect happens where it does).
- `snake_case` for variables/functions, `PascalCase` for classes.
- Prefer concise, intent-revealing names over long ones that encode full logic or data values. This is a known growth area for the team — naming defaults to more literal, sentence-like names (e.g. `test_function_returns_one_when_this_column_is_one_and_that_column_is_one`). Nudging towards more concise names in review and explain why a shorter name still captures the intent rather than just flagging it as wrong.
- Flag numeric literals whose business meaning isn't obvious; don't extract universally-understood values (0, 1, simple limits/indices) into named constants just for the sake of it.

## Dataset naming in S3 / Athena

Athena names tables from the partition, so: `{dataset}_{sub_dataset_if_relevant}_{order_of_process_if_relevant}_{brief_description}`, e.g. `ind_cqc_02_cleaned`.

## Changelog

When making a substantive code change, keep [CHANGELOG.md](CHANGELOG.md)'s `## [Unreleased]` section up to date:
- Add one short, plain-sentence bullet under whichever subsection fits — `### Added` (new capability), `### Changed` (modified existing behaviour), `### Improved` (performance/quality, no behaviour change), or `### Fixed` (bug fix) — matching the plain English style of existing entries (e.g. "Added validations for estimates data within Estimates by Job Roles Pipeline.").
- One bullet per piece of work, not per edit. If the task's scope changes as it progresses, update that same bullet in place rather than adding a new one, so by the end it reflects what was actually delivered, not the original ask.

## When reviewing code

- Review as a senior engineer: correctness, performance, maintainability — in that order of weight.
- Don't rewrite whole blocks unless the approach is fundamentally flawed; don't suggest renames/restructuring unless the current version is genuinely unclear or harmful to maintainability.
- Structure findings as: 1) Critical (correctness/data integrity/major perf risk), 2) Important (scalability/readability/maintainability), 3) Optional. If there's nothing critical, say so plainly rather than inventing improvements.
- Explain impact concretely (e.g. "this skews the aggregation for X" / "this materialises the full frame before the filter, at N rows that's...").

## Ambiguity

If something is ambiguous, or could materially affect correctness or performance, ask rather than guessing — don't assume business logic that isn't present in the code.
