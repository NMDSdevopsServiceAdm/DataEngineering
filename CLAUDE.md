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
- Be deliberate around operations that can force materialisation or blow up memory: `.unique()` on high-cardinality columns, `.pivot()`, `.explode()`, joins with row fan-out, `.collect().to_pandas()` round-trips.
- Prefer `Categorical` / `Enum` over `String` for low-cardinality repeated columns.
- Prefer `float32` over `float64` where precision requirements allow — call out explicitly when a trade-off matters (e.g. long aggregations/sums where error can accumulate).
- If a lazy chain is unintentionally broken (an early `.collect()`, a `.to_pandas()`, list-comprehension row-wise logic), flag it — that's a correctness-adjacent issue here, not a style nitpick. Some `.collect()` are ok as they directly follow an aggregation which results in a very small LazyFrame.

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

Never hardcode column-name strings. Each project keeps its own column-name class near the project root, e.g. `projects/_04_direct_payment_recipients/direct_payments_column_names.py` defining `DirectPaymentColumnNames`, imported under a short alias:

```python
from projects._04_direct_payment_recipients.direct_payments_column_names import (
    DirectPaymentColumnNames as DP,
)
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

## When reviewing code

- Review as a senior engineer: correctness, performance, maintainability — in that order of weight.
- Don't rewrite whole blocks unless the approach is fundamentally flawed; don't suggest renames/restructuring unless the current version is genuinely unclear or harmful to maintainability.
- Structure findings as: 1) Critical (correctness/data integrity/major perf risk), 2) Important (scalability/readability/maintainability), 3) Optional. If there's nothing critical, say so plainly rather than inventing improvements.
- Explain impact concretely (e.g. "this skews the aggregation for X" / "this materialises the full frame before the filter, at N rows that's...").

## Ambiguity

If something is ambiguous, or could materially affect correctness or performance, ask rather than guessing — don't assume business logic that isn't present in the code.
