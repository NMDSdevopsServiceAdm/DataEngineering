import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ImputeJobRoleData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ImputeJobRoleSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH = "projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.utils"


class TestPercentageShareHandlingZeroSum:
    @pytest.mark.parametrize(
        "input_, expected",
        [
            pytest.param(
                [5.0, 2.0, 1.0],
                [0.625, 0.25, 0.125],
                id="when_all_values_present",
            ),
            pytest.param(
                [0, 0],
                [0.5, 0.5],
                id="handles_zero_sum_case_with_even_distribution",
            ),
            pytest.param(
                [0, None, 0, None],
                [0.5, None, 0.5, None],
                id="handles_zero_sum_case_with_even_distribution_across_non_nulls",
            ),
        ],
    )
    def test_percentage_share_handling_zero_sum(self, input_, expected):
        input_lf = pl.LazyFrame({"values": input_})
        expected_lf = pl.LazyFrame({"pct_share": expected})
        returned_lf = input_lf.select(
            job.percentage_share_handling_zero_sum("values").alias("pct_share")
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestManagerialFilledPostAdjustmentExpr:
    @pytest.fixture(
        params=[
            case.as_pytest_param() for case in Data.managerial_adjustment_test_cases
        ]
    )
    def input_data(self, request):
        """Provides 4 different test cases to put through the managerial adjustment tests.

        The test data comes with three different output columns: "diff",
        "proportions", "adjusted_estimates". The right one should be selected for
        each expected_lf.

        """
        return request.param

    @pytest.fixture
    def expected_lf_constructor(self, input_data):
        """A helper fixture to construct the expected_lf given output_col."""

        def inner(output_col: str) -> pl.LazyFrame:
            return pl.LazyFrame(
                data=input_data,
                schema=Schemas.managerial_adjustment_expected_schema,
                orient="row",
            ).select(*Schemas.managerial_adjustment_core_schema.keys(), output_col)

        return inner

    @pytest.mark.parametrize(
        "input_, expected",
        [
            pytest.param(["Sarah", "James"], 1, id="more_than_1_capped_to_1"),
            pytest.param(["Sarah"], 1, id="1_stays_1"),
            pytest.param([], 0, id="empty_list_to_0"),
            pytest.param(None, 0, id="null_to_0"),
        ],
    )
    def test_clip_rm_count(self, input_, expected):
        schema = {
            IndCQC.registered_manager_names: pl.List,
            IndCQC.registered_manager_count: pl.UInt32,
        }
        expected_lf = pl.LazyFrame([[input_], [expected]], schema=schema)
        input_lf = expected_lf.drop(IndCQC.registered_manager_count)
        clip_count_expr = job.ManagerialFilledPostAdjustmentExpr._clip_rm_count()
        returned_lf = input_lf.with_columns(
            clip_count_expr.alias(IndCQC.registered_manager_count)
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    @pytest.fixture(
        params=[
            {"method": "_rm_manager_diff", "col": "diff"},
            {"method": "_non_rm_manager_proportions", "col": "proportions"},
            {"method": "build", "col": "adjusted_estimates"},
        ],
        ids=lambda d: d["method"].lstrip("_"),
    )
    def config_data(self, request):
        return request.param

    def test_expr_methods(self, expected_lf_constructor, config_data):
        output_col = config_data["col"]
        expected_lf = expected_lf_constructor(output_col)
        input_lf = expected_lf.drop(output_col)
        expr_method = getattr(
            job.ManagerialFilledPostAdjustmentExpr, config_data["method"]
        )
        returned_lf = input_lf.with_columns(expr_method().alias(output_col))
        pl_testing.assert_frame_equal(returned_lf, expected_lf, abs_tol=0.001)

    def test_build_expr_over_groups(self):
        """Test by grouping over location_id and cqc_location_import_date."""
        output_col = "adjusted_estimates"
        expected_lf = pl.LazyFrame(
            data=Data.managerial_adjustment_grouping_test_data,
            schema=Schemas.managerial_adjustment_expected_schema,
            orient="row",
        ).select(*Schemas.managerial_adjustment_core_schema.keys(), output_col)

        input_lf = expected_lf.drop(output_col)
        returned_lf = input_lf.with_columns(
            job.ManagerialFilledPostAdjustmentExpr.build()
            .over(IndCQC.location_id, IndCQC.cqc_location_import_date)
            .alias(output_col)
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, abs_tol=0.001)
