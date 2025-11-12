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

import pandas as pd
from pyspark.sql import DataFrame, Window

import projects._00_example.utils.utils as utils
from utils.column_names import Columns
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
- Store all column names in `utils.column_names` (alphabetically)
- Always reference columns via that module — do not hardcode strings
- Helps consistency and reduces typos

## Dataset Names in AWS S3
- Athena uses the dataset partition to name the tables so we will use this structure when saving files to S3
- `{dataset}_{sub_dataset_if_relevant}_{order_of_process_if_relevant}_{very_brief_description}`

Examples:
    `dataset=cqc_providers_01_delta_api`
    `dataset=cqc_locations_01_delta_api`
    `dataset=cqc_locations_02_delta_flattened`
    `dataset=ind_cqc_01_merged`
    `dataset=ind_cqc_02_cleaned`

## Naming Conventions
- Use `snake_case` for variables and functions
- Use `PascalCase` for class names

## Test Conventions
- Use `unittest`
- Group tests for each function in a test class
- Include at least one test per function/method
- Each test function should test **one specific behaviour**
- Use mock objects if setup is complex or if it is an orchestrator function
- Store test data rows and schemas in the relevant `test_file_data.py` and `test_file_schemas.py`
- File naming: `test_<module>.py`
- Class naming: `YourFunctionNameTests`
- Test naming: `test_<expected_behaviour>()` or `test_when_<scenario>_returns_<expected_outcome>()`

Example:

```python
class CleanIdColumnTests:
    def test_does_not_change_valid_ids(self):
        input_lf = pl.LazyFrame(...)

        returned_lf = job.function_name(...)

        expected_lf = pl.LazyFrame(...)
        pl_testing.assert_frame_equal(expected_lf, output_lf)

    def test_when_no_id_returns_null(self):
        ...
    ...
```

### Mocking and Patching
- If patching, define a `PATCH_PATH` string at the top of the test module
- Use this string when applying patches to improve readability and reduce errors
- Patch in the **order the functions are called** and note that **decorators are applied in reverse order**

Example:

```python
PATCH_PATH: str = "projects._01_ingest.jobs.my_job"

class MainTest:
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
