import importlib.util
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from types import ModuleType
from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import (
    ModelEvaluationColumns as ModelEvaluation,
)
from utils.column_values.categorical_column_values import PrimaryServiceType

SNIPPET_PATH = (
    Path(__file__).parents[2]
    / ".claude"
    / "skills"
    / "analytics-library"
    / "snippets"
    / "group_mean_jumpiness.py"
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
    spec.loader.exec_module(module)
    return module


@dataclass
class GroupMeanJumpinessTestCase:
    id: str
    input_data: dict[str, Any]
    value_columns: list[str]
    group_columns: list[str]
    expected_data: dict[str, Any]

    def as_pytest_param(self):
        return pytest.param(self, id=self.id)


test_cases = [
    # Per-row changes would be 2, 2 and 2; the group means 3 and 7 change by 4.
    GroupMeanJumpinessTestCase(
        id="rows_in_a_group_are_averaged_per_date",
        input_data={
            IndCQC.primary_service_type: [PrimaryServiceType.non_residential] * 4,
            IndCQC.cqc_location_import_date: [
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 2, 1),
            ],
            IndCQC.estimate_filled_posts: [2.0, 4.0, 6.0, 8.0],
        },
        value_columns=[IndCQC.estimate_filled_posts],
        group_columns=[IndCQC.primary_service_type],
        expected_data={
            ModelEvaluation.column_name: [IndCQC.estimate_filled_posts],
            ModelEvaluation.mean_period_to_period_change: [4.0],
        },
    ),
    # Rows alternate between groups: non-residential changes by 10 and care homes by 0,
    # so the mean change is 5. Measuring across groups would add jumps of 50 or more.
    GroupMeanJumpinessTestCase(
        id="not_measured_across_groups",
        input_data={
            IndCQC.primary_service_type: [
                PrimaryServiceType.non_residential,
                PrimaryServiceType.care_home_only,
                PrimaryServiceType.non_residential,
                PrimaryServiceType.care_home_only,
            ],
            IndCQC.cqc_location_import_date: [
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 2, 1),
            ],
            IndCQC.estimate_filled_posts: [20.0, 80.0, 30.0, 80.0],
        },
        value_columns=[IndCQC.estimate_filled_posts],
        group_columns=[IndCQC.primary_service_type],
        expected_data={
            ModelEvaluation.column_name: [IndCQC.estimate_filled_posts],
            ModelEvaluation.mean_period_to_period_change: [5.0],
        },
    ),
]


class TestGroupMeanJumpiness:
    @pytest.mark.parametrize("case", [c.as_pytest_param() for c in test_cases])
    def test_returns_jumpiness_of_group_means(self, case):
        snippet = load_snippet(SNIPPET_PATH)

        returned_lf = snippet.group_mean_jumpiness(
            pl.LazyFrame(case.input_data),
            value_columns=case.value_columns,
            group_columns=case.group_columns,
            date_column=IndCQC.cqc_location_import_date,
        )

        pl_testing.assert_frame_equal(returned_lf, pl.LazyFrame(case.expected_data))
