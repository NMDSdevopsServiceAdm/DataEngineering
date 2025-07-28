# Style Guide

## General Principles
- Write readable, maintainable code
- Be consistent with existing patterns in the project
- Avoid repeating code — reuse via functions or utilities
- Prefer list comprehensions over `for` loops where appropriate
- Use meaningful variable names, especially for intermediate values
- Don't leave commented-out code or `.show()` functions in production scripts
- Remove unused variables and imports before committing
- Use logging, not print statements
- For temporary or in-development code, use clear `# TODO:` or `# FIXME:` comments (and create a Trello card for each)

## Code Organisation
- All code should be stored in the `projects/` folder

### Project layout
```
projects/
├── my_project/              # A specific project
│   ├── _01_ingest/          # A numbered stage or dataset/job
│   │   ├── jobs/            # Spark jobs and orchestration
│   │   ├── utils/           # Utilities specific to this stage
│   │   └── tests/           # Unit tests (mirroring jobs/utils structure)
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
  - `jobs/`: for job logic
  - `utils/`: for helper functions specific to this job/stage
  - `tests/`: for corresponding tests (also split into `jobs/` and `utils/`)

### Utility Function Location Rules
| Function usage scope                              | Location                                                |
|---------------------------------------------------|----------------------------------------------------------|
| Used **only** in one job/dataset                  | `projects/<project>/<stage>/utils/*.py`                 |
| Used by **multiple** jobs/datasets in a project   | `projects/<project>/utils/*.py`                         |
| Used by **multiple projects**                     | `projects/utils/utils.py`                               |

## Imports
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

## Naming Conventions
- Use `snake_case` for variables and functions
- Use `PascalCase` for class names

## Test Conventions
- Use `unittest`
- Group tests for each function in a test class
- Include at least one test per function/method
- Each test function should test **one specific behaviour**
- Use mock objects if setup is complex
- Store test data rows and schemas in the relevant `test_file_data.py` and `test_file_schemas.py`
- File naming: `test_<module>.py`
- Class naming: `YourFunctionNameTests`
- Test naming: `test_<unit_of_work>_<expected_outcome>()`

Example:

```python
class TransformDataTests:
    def test_transform_data_removes_nulls(self):
        ...

    def test_transform_data_renames_columns(self):
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
- Each PR should have and be linked to a corresponding Trello card
- Explain the intent of your PRs clearly
- Small, focused commits are better than large, sweeping ones
- Always include test updates with logic changes

## Test Coverage Expectations
- Aim for high test coverage, especially for core logic and data transformations
- Use mock data that reflects realistic edge cases
- Prefer simple test data over large datasets
