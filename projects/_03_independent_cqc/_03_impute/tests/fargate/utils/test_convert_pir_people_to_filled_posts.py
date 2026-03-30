from dataclasses import dataclass

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._03_impute.fargate.utils.convert_pir_people_to_filled_posts as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ConvertPirPeopleToFilledPostsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ConvertPirPeopleToFilledPostsSchema as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import CareHome


@dataclass
class ConvertPirCase:
    id: str
    expected_data: list[tuple]


convert_pir_test_cases = [
    ConvertPirCase(
        id="basic_conversion",
        expected_data=[
            (CareHome.not_care_home, 10.0, 14.0, 15.0),
            (CareHome.not_care_home, 10.0, 16.0, 15.0),
        ],
    ),
    ConvertPirCase(
        id="only_applies_to_not_care_home",
        expected_data=[
            (CareHome.not_care_home, 10.0, 15.0, 15.0),
            (CareHome.care_home, 10.0, 10.0, None), # returns null
        ],
    ),
    ConvertPirCase(
        id="null_and_zero_people_return_null",
        expected_data=[
            (CareHome.not_care_home, None, 10.0, None), # null returns null
            (CareHome.not_care_home, 10.0, 15.0, 15.0),
            (CareHome.not_care_home,  0.0, 10.0, None), # zero returns null
        ],
    ),
]  # fmt: skip


class TestConvertPirToFilledPosts:
    CASES = [pytest.param(case, id=case.id) for case in convert_pir_test_cases]

    @pytest.mark.parametrize("case", CASES)
    def test_function_returns_expected_values(self, case):
        expected_lf = pl.LazyFrame(
            case.expected_data,
            Schemas.output_schema,
            orient="row",
        )

        input_lf = expected_lf.drop(IndCQC.pir_filled_posts_model)

        result_lf = job.convert_pir_to_filled_posts(input_lf)

        pl_testing.assert_frame_equal(
            result_lf,
            expected_lf,
            check_row_order=False,
        )


@dataclass
class ComputeGlobalRatioCase:
    id: str
    data: list[tuple]
    expected_ratio: float | None


compute_global_ratio_test_cases = [
    ComputeGlobalRatioCase(
        id="all_valid_rows_included_in_ratio",
        data=[
            (CareHome.not_care_home, 19.0, 25.0),
            (CareHome.not_care_home, 21.0, 35.0),
        ],
        expected_ratio=1.5,
    ),
    ComputeGlobalRatioCase(
        id="ignores_null_zero_and_care_home_rows",
        data=[
            (CareHome.not_care_home, 10.0, 15.0),
            (CareHome.not_care_home, 10.0, None),
            (CareHome.not_care_home, 10.0,  0.0),
            (CareHome.not_care_home, 0.0,  10.0),
            (CareHome.not_care_home, None, 10.0),
            (CareHome.care_home,     10.0, 20.0),
        ],
        expected_ratio=1.5,
    ),
    ComputeGlobalRatioCase(
        id="ignores_rows_which_fail_quality_filter",
        data=[
            (CareHome.not_care_home, 10.0, 15.0),
            (CareHome.not_care_home, 10.0,  7.0), # ratio of 0.7 fails quality filter
        ],
        expected_ratio=1.5,
    ),
    ComputeGlobalRatioCase(
        id="mix_of_valid_and_invalid_rows_only_valid_used",
        data=[
            (CareHome.not_care_home, 20.0, 25.0),
            (CareHome.not_care_home, None, 10.0),
            (CareHome.not_care_home, 10.0, None),
            (CareHome.care_home,     10.0, 10.0),
            (CareHome.not_care_home, 20.0, 35.0),
        ],
        expected_ratio=1.5,
    ),
]  # fmt: skip


class TestComputeGlobalRatio:
    CASES = [pytest.param(case, id=case.id) for case in compute_global_ratio_test_cases]

    @pytest.mark.parametrize("case", CASES)
    def test_compute_global_ratio_returns_expected_ratio(self, case):
        lf = pl.LazyFrame(
            case.data,
            Schemas.input_schema,
            orient="row",
        )

        ratio = job.compute_global_ratio(lf)
        assert ratio == pytest.approx(case.expected_ratio)
