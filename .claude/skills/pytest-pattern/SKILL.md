---
name: pytest-pattern
description: Use when writing a new unit test in this repo, or migrating an existing test off unittest. Encodes the repo's dataclass+parametrize pattern, naming conventions, mocking rules, and test-data storage rules from STYLEGUIDE.md.
---

# Writing a test in this repo

New tests are pytest-style (the repo is mid-migration off `unittest`). Migrate opportunistically, scoped to what you're actually touching — don't mass-rewrite passing tests beyond that:

- Editing one function/method → migrate just that function's test(s).
- Editing a whole class → migrate its whole test class.
- Editing a whole script/module → migrate the whole test file.
- Editing at folder level (e.g. a full pipeline-stage migration) → migrate everything within that folder.

## Structure

- Group tests for each function in a `class Test<FunctionName>:` — **must** start with `Test`; pytest's default discovery only collects plain classes prefixed this way (unlike `unittest.TestCase`, which pytest collects regardless of name).
- Include at least one test per function/method.
- Each test tests **one specific behaviour**. Name it by behaviour/expectation, not by the input used: `test_<expected_behaviour>()` or `test_when_<scenario>_returns_<expected_outcome>()`.
- For multiple cases against the same function, prefer `@pytest.mark.parametrize` over one test method per case. Build cases from a small `@dataclass` carrying a descriptive `id`, so failures read as the scenario, not a row of data.
- Assert with `polars.testing.assert_frame_equal`, not manual comparisons.

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

Cover the happy path, meaningful edge cases, and invalid input only where relevant — don't pad coverage with trivial cases.

## Mocking and patching

- No `pytest-mock` in this repo — mocking stays on stdlib `unittest.mock` (`Mock`, `patch`), used as plain decorators, not via `TestCase`.
- Use mocks when setup is complex or the function under test is an orchestrator.
- If patching, define a `PATCH_PATH` string at the top of the test module and use it in every `@patch(...)` for readability.
- Patch in the **order the functions are called**, remembering **decorators apply in reverse order**.
- Assert with plain `assert`, not `self.assertEqual(...)` — there's no `self` without `TestCase`.

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
        save_data_mock: Mock,
    ):
        ...
```

## Test data and schemas

Store rows/schemas in `*_test_file_data.py` / `*_test_file_schemas.py`, at the narrowest scope that covers actual usage:

| Used by | Location |
|---|---|
| One job/dataset only | `projects/<project>/<stage>/unittest_data/*.py` |
| Multiple jobs/datasets in a project | `projects/<project>/unittest_data/*.py` |
| Multiple projects | `projects/utils/unittest_data.py` |

File naming: `test_<module>.py`, mirroring the module under test.

## Running tests locally

Use `pipenv run pytest <path>` (or `pipenv shell` then plain `pytest`) — don't assume packages are on the system Python. Local runs are for validation only: CI/CD runs the full suite against full data automatically once pushed to a branch, so there's no need to chase full-suite/full-data runs locally.
