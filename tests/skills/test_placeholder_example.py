"""
PLACEHOLDER: shows how a snippet is tested. Delete it with the placeholder snippet.

Tests sit under `tests/` because pytest skips dot-directories such as `.claude/`; each
snippet is loaded by its file path.
"""

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import PrimaryServiceType

SNIPPET_PATH = (
    Path(__file__).parents[2]
    / ".claude"
    / "skills"
    / "analytics-library"
    / "snippets"
    / "placeholder_example.py"
)


def load_snippet(path: Path) -> ModuleType:
    """
    Imports a snippet module from its file path.

    Args:
        path (Path): The snippet's .py file.

    Returns:
        ModuleType: The executed snippet module.
    """
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[path.stem] = module
    spec.loader.exec_module(module)
    return module


@dataclass
class PlaceholderExampleTestCase:
    id: str
    input_data: dict[str, Any]
    expected_data: dict[str, Any]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


test_cases = [
    PlaceholderExampleTestCase(
        id="returns_mean_per_group",
        input_data={
            IndCQC.primary_service_type: [
                PrimaryServiceType.non_residential,
                PrimaryServiceType.non_residential,
                PrimaryServiceType.care_home_only,
            ],
            IndCQC.estimate_filled_posts: [1.0, 3.0, 5.0],
        },
        expected_data={
            IndCQC.primary_service_type: [
                PrimaryServiceType.non_residential,
                PrimaryServiceType.care_home_only,
            ],
            IndCQC.estimate_filled_posts: [2.0, 5.0],
        },
    ),
]


class TestPlaceholderExample:
    @pytest.mark.parametrize("case", [c.as_pytest_param() for c in test_cases])
    def test_returns_expected_values(self, case):
        snippet = load_snippet(SNIPPET_PATH)

        returned_lf = snippet.placeholder_example(
            pl.LazyFrame(case.input_data),
            value_columns=[IndCQC.estimate_filled_posts],
            group_columns=[IndCQC.primary_service_type],
        )

        pl_testing.assert_frame_equal(
            returned_lf, pl.LazyFrame(case.expected_data), check_row_order=False
        )
