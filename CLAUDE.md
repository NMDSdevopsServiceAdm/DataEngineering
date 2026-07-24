# DataEngineering — CLAUDE.md

Skills for Care Workforce Intelligence Team. Builds reproducible AWS data pipelines from adult social care workforce data (ASC-WDS). See [README.md](README.md) for full background.

## Stack

- Python 3.11.
- Two engines coexist in this repo:
  - `jobs/` folders — legacy **PySpark**.
  - `fargate/` folders — **Polars**, the target for all new work.
- We are mid-migration from PySpark to Polars. Do not introduce new PySpark code unless explicitly asked or working in a file already containing PySpark code. When migrating a function, add a comment on the old PySpark function pointing at its replacement: `# converted to polars -> filepath.py` (see `.github/PULL_REQUEST_TEMPLATE/polars_migration_template.md`).
- Tests are mid-migration from `unittest` to `pytest`. Write new tests in pytest style (see Testing below). See "Migration & opportunistic upgrade scope" below for how far an upgrade should extend when you're already touching old-format code.

## Migration & opportunistic upgrade scope

- **PySpark → Polars conversions:** you have free rein to improve the function's name, docstring, and test names/structure as part of the conversion — don't feel bound to preserve old naming just for continuity. See the `polars-migration` skill.
- **Opportunistic upgrades generally** (unittest → pytest, old docstring style, unclear naming, etc.): scope the upgrade to the smallest unit that contains your actual change, escalating only as far as you've actually touched:
  - Editing one function/method → upgrade just that function's docstring and its corresponding test(s).
  - Editing a whole class → upgrade the whole class (all its methods, docstrings, and its test class).
  - Editing a whole script/module → upgrade the whole file's docstrings and its test file.
  - Editing at folder level (e.g. migrating a full pipeline stage) → upgrade everything within that folder.
- Don't upgrade sibling functions/files you haven't otherwise touched just because they're nearby and old-format — that's still a mass-rewrite, just a smaller one.

## Environment & workflow

- Run `pipenv shell` (or prefix commands with `pipenv run`) to access this project's installed packages — don't assume they're on the system/global Python.
- The `gh` CLI is not installed in this environment. Don't shell out to `gh`; if a GitHub API action is genuinely needed, say so explicitly rather than assuming it's available.
- Branch names must be **16 characters or fewer**.
- CI/CD automatically runs all new code plus the full unit test suite against full data once pushed to a branch. Local/manual validation should run the relevant unit tests and `terraform validate` where applicable — never attempt to deploy or run code against AWS directly; that's the CI/CD pipeline's job.

## Scale is the top constraint

Datasets here are large in both rows and columns — typically 4M+ rows and 20+ columns, as an order-of-magnitude anchor rather than a hard spec — and OOM is a recurring real problem. When writing or reviewing Polars code, correctness and "idiomatic" style are necessary but not sufficient — always also check for unnecessary materialisation. Concretely:

- Keep the pipeline lazy end-to-end; use `scan_parquet` / `scan_csv`, not `read_*`; `.collect()` once, at the end, not mid-pipeline.
- `.select()` / `.drop()` unneeded columns as early as possible — with wide datasets, column count matters as much as row count.
- Be deliberate around operations that can force materialisation or blow up memory: `.unique()` on high-cardinality columns, `.pivot()`, `.explode()`, joins with row fan-out, `.collect().to_pandas()` round-trips. Even a fan-out-free 1:1 join (e.g. broadcasting one computed value back onto every row) has real memory overhead of its own — see `.over()` vs join-based rewrites below.
- Prefer `Categorical` / `Enum` over `String` for low-cardinality repeated columns.
- Prefer `float32` over `float64` where precision requirements allow — call out explicitly when a trade-off matters (e.g. long aggregations/sums where error can accumulate).
- If a lazy chain is unintentionally broken (an early `.collect()`, a `.to_pandas()`, list-comprehension row-wise logic), flag it — that's a correctness-adjacent issue here, not a style nitpick. Some `.collect()` are ok as they directly follow an aggregation which results in a very small LazyFrame.

### Polars streaming engine — what isn't covered yet

`.collect(engine="streaming")` doesn't yet cover everything — some operations (notably plain `.over()`, `group_by`, equi-joins, `sort`, several aggregates/expressions — see the skill for the full list) silently fall back to the in-memory engine. Full checklist, refresh instructions, and the tracking issue link live in the `polars-streaming-check` skill (`.claude/skills/polars-streaming-check/`) — check there before relying on streaming mode for an OOM-prone pipeline.

### `.over()` vs join-based rewrites — "not streaming" ≠ "uses more memory"

Not being on the streaming list above is not a proxy for "will OOM" — a join that broadcasts a value onto every row (left-join, `join_asof`) can cost several GB *more* than the equivalent `.over()`'s in-memory fallback, even though the join looks "streaming-friendly." Semi/anti-join rewrites are the exception (no merged output), but `pl.concat` to recombine isn't free either. Full reasoning, worked examples, and the measurement snippet to compare before converting live in the `over-vs-join` skill (`.claude/skills/over-vs-join/`) — **temporary, remove after ~2026-10** once the team has enough real examples to trust default judgement.

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

New tests: pytest, one behaviour per test, named by behaviour/expectation rather than input. Full pattern (class/dataclass/parametrize conventions, mocking rules, test-data storage) lives in the `pytest-pattern` skill (`.claude/skills/pytest-pattern/`).

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

Review as a senior engineer: correctness, performance, maintainability — in that order of weight, with findings tiered Critical/Important/Optional. Full checklist (including the PR reviewer checklist) lives in the `review-checklist` skill (`.claude/skills/review-checklist/`).

## Ambiguity

If something is ambiguous, or could materially affect correctness or performance, ask rather than guessing — don't assume business logic that isn't present in the code.
