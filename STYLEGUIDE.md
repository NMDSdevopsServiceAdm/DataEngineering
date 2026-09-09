# Style Guide

## General Principles
- Write readable, maintainable code
- Be consistent with existing patterns in the project
- Avoid repeating code — reuse via functions or utilities
- Prefer list comprehensions over `for` loops where appropriate
- Use meaningful variable names, especially for intermediate values
- Don't leave commented-out code or `.show()` functions in production scripts
- Remove unused variables and imports before committing
- For temporary or in-development code, use clear `# TODO:` or `# FIXME:` comments (and create a Trello card for each)
- For Polars code, use [LazyFrame](https://docs.pola.rs/api/python/stable/reference/lazyframe/index.html) where possible

## Code Organisation
- All code should be stored in the `projects/` folder

### Project layout
```
projects/
├── my_project/              # A specific project
│   ├── _01_ingest/          # A numbered stage or dataset/job
│   │   ├── fargate/         # Polars jobs and orchestration
│   │   ├── jobs/            # Spark jobs and orchestration
│   │   ├── utils/           # Utilities specific to this stage
│   │   └── tests/           # Unit tests (mirroring jobs/utils structure)
│   │       ├── fargate/
│   │       ├── jobs/
│   │       └── utils/
│   ├── _02_clean/
│   ├── unittest_data/       # Test data rows and schemas used for unit tests within this project
│   └── utils/               # Utilities shared across multiple stages within this project
├── another_project/
├── unittest_data/           # Test data rows and schemas used for unit tests across multiple projects
└── utils/                   # Utilities shared across multiple projects
```

### Organising by Scale
- If a project is **small**, keep all code in a flat structure under one project folder.
- If a project is **large**, split into multiple folders either:
  - **By dataset**
  - **By processing stage/job** (numbering them `_01_ingest`, `_02_clean`, etc.)

Each folder should contain:
  - `jobs/` / `fargate/`: for job logic (jobs for Spark code and fargate for Polars code)
  - `utils/`: for helper functions specific to this job/stage
  - `tests/`: for corresponding tests (also split into `jobs/` / `fargate/` and `utils/`)

### Utility Function Location Rules
| Function usage scope                              | Location                                                |
|---------------------------------------------------|----------------------------------------------------------|
| Used **only** in one job/dataset                  | `projects/<project>/<stage>/utils/*.py`                 |
| Used by **multiple** jobs/datasets in a project   | `projects/<project>/utils/*.py`                         |
| Used by **multiple projects**                     | `projects/utils/utils.py`                               |

## Imports
Note: The VS Code extension 'isort' will do this automatically whenever a file is saved.

- Follow standard three-section order:
  1. Standard library imports
  2. Third-party package imports (e.g. `pandas`, `pyspark`)
  3. Internal package imports
- Separate each section with a blank line
- Alphabetise within each section

Example:

```python
import datetime
import os

import polars as pl

import projects._00_example.fargate.utils.utils as utils
from utils.column_names import ExampleColumnNames as Cols
```

## Docstrings
- Use [Google-style docstrings](https://google.github.io/styleguide/pyguide.html#38-comments-and-docstrings)
- Include:
  - A short one line summary
  - A more detailed description (if required)
  - Arguments (with types)
  - Returns (with types)
  - Raises (if applicable)

Example:
```python
def your_function(df: DataFrame) -> DataFrame:
    """
    Cleans and transforms input data.

    Add additional information here if requires more than one single line of explanation.

    Args:
        df (DataFrame): Raw input data.

    Returns:
        DataFrame: Cleaned and transformed data.

    Raises:
        ValueError: If the DataFrame contains unknown values.
    """
```

## Column Names
- Never hardcode column-name strings
- Column-name classes belong in `utils/column_names`, shared across the repo (alphabetise entries within each class) — not per-project
- Import under a short alias, e.g. `from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC`

## Dataset Names in AWS S3
- Domains are numbered to mirror their owning project's folder number, e.g. `domain=01_cqc` (from `projects/_01_ingest`), `domain=03_ind_cqc` (from `projects/_03_independent_cqc`). A domain shared across multiple projects' outputs (e.g. `99_publication`) is numbered to sort in its logical position instead — `99_` for a terminal layer that draws from several upstream domains.
- Athena uses the dataset partition to name the tables, and the Glue crawler module's `table_prefix` prepends the domain to every discovered table name — so a dataset's own name doesn't need to repeat its domain. Within the domain, dataset names follow `{dataset}_{sub_dataset_if_relevant}_{order_of_process_if_relevant}_{very_brief_description}`
- Multi-stage pipelines within a domain (e.g. `03_ind_cqc`) prefix their dataset names with a numbered product token (`01_filled_posts`, `03_starters_leavers_vacancies`), reserving unused numbers for products that don't exist yet, then a numbered stage within that product

Examples:
    `domain=01_cqc/dataset=providers_01_delta_api`
    `domain=01_cqc/dataset=locations_01_delta_api`
    `domain=01_cqc/dataset=locations_02_delta_flattened`
    `domain=03_ind_cqc/dataset=01_filled_posts_01_merged`
    `domain=03_ind_cqc/dataset=01_filled_posts_02_cleaned`

### Validation report datasets
A validation report lives in the same domain as the dataset it validates, with `_validation` appended to that dataset's name — not in a separate domain. This means it's picked up automatically by that domain's own Glue crawler, without needing a dedicated one.

Example: a report on `domain=01_cqc/dataset=pir_cleaned/` is written to `domain=01_cqc/dataset=pir_cleaned_validation/`.

### Raw bucket upload prefixes are the exception
The raw bucket's own domain prefixes (`domain=ASCWDS`, `domain=CQC`, `domain=ONS`, `domain=capacity_tracker` — where external CSV uploads actually land) are hand-managed outside this repo and can't be renamed to match the schemes above. Only the converted-to-parquet output written to the datasets bucket picks up the numbered domain naming.

## Naming Conventions
- Use `snake_case` for variables and functions
- Use `PascalCase` for class names

## Test Conventions
- Use `pytest` (the repo is mid-migration off `unittest`; migrate a file opportunistically when you're already touching it, don't mass-rewrite passing tests)
- Group tests for each function in a test class
- Include at least one test per function/method
- Each test function should test **one specific behaviour**
- Use mock objects if setup is complex or if it is an orchestrator function
- Store test data rows and schemas in the relevant `test_file_data.py` and `test_file_schemas.py`
- File naming: `test_<module>.py`
- Class naming: `Test<YourFunctionName>` — **must** start with `Test`; pytest's default discovery only collects plain classes prefixed this way (unlike `unittest.TestCase`, which pytest collects regardless of name)
- Test naming: `test_<expected_behaviour>()` or `test_when_<scenario>_returns_<expected_outcome>()`
- For multiple cases against the same function, prefer `@pytest.mark.parametrize` over one test method per case — build cases from a small `@dataclass` carrying a descriptive `id`, so failures read as the scenario, not a row of data

Example:

```python
@dataclass
class CleanIdColumnTestCase:
    id: str
    data: list[Any]

    def as_pytest_param(self):
        return pytest.param(self.data, id=self.id)


cases = [
    CleanIdColumnTestCase(id="does_not_change_valid_ids", data=[...]),
    CleanIdColumnTestCase(id="nulls_invalid_ids", data=[...]),
]


class TestCleanIdColumn:
    @pytest.mark.parametrize("test_data", [c.as_pytest_param() for c in cases])
    def test_function_returns_expected_values(self, test_data):
        input_lf = pl.LazyFrame(test_data, ...)

        returned_lf = job.function_name(input_lf)

        expected_lf = pl.LazyFrame(...)
        pl_testing.assert_frame_equal(expected_lf, returned_lf)
```

### Mocking and Patching
- No `pytest-mock` in this repo — mocking stays on the standard-library `unittest.mock` (`Mock`, `patch`), used as plain decorators, not via `TestCase`
- If patching, define a `PATCH_PATH` string at the top of the test module
- Use this string when applying patches to improve readability and reduce errors
- Patch in the **order the functions are called** and note that **decorators are applied in reverse order**
- Assert with plain `assert`, not `self.assertEqual(...)` (there's no `self` without `TestCase`)

Example:

```python
PATCH_PATH: str = "projects._01_ingest.jobs.my_job"

class TestMain:
    @patch(f"{PATCH_PATH}.utils.save_data")
    @patch(f"{PATCH_PATH}.clean_data")
    @patch(f"{PATCH_PATH}.utils.read_data")
    def test_main_runs_all_functions(
        self,
        read_data_mock: Mock,
        clean_data_mock: Mock,
        save_data_mock: Mock
    ):
    ...
```

### Unit Test Data and Schemas
- Store unit test data in the following files - `*_test_file_data.py` and `*_test_file_schemas.py`
- Save the test data using the principles in the table below.

| Test Data usage scope                             | Location                                                |
|---------------------------------------------------|----------------------------------------------------------|
| Used **only** in one job/dataset                  | `projects/<project>/<stage>/unittest_data/*.py`         |
| Used by **multiple** jobs/datasets in a project   | `projects/<project>/unittest_data/*.py`                 |
| Used by **multiple projects**                     | `projects/utils/unittest_data.py`                       |



## Code reviews
- Each pull request (PR) should have and be linked to a corresponding Trello card
- Explain the intent of your pull requests clearly
- Small, focused commits are better than large, sweeping ones
- Always include test updates with logic changes

## Test Coverage Expectations
- Aim for high test coverage, especially for core logic and data transformations
- Use mock data that reflects realistic edge cases
- Prefer simple test data over large datasets
