import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.spike_filter_utils as job
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_test_data import (
    TestSpikeFilterUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

METRIC = IndCQC.estimate_filled_posts_by_job_role


class TestFilterOutZeroJobRoleFilledPosts:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.filter_out_zero_job_role_filled_posts_test_cases
        ],
    )
    def test_filters_rows_as_expected(self, case):
        test_lf = pl.LazyFrame(case.input_data, schema_overrides={METRIC: pl.Float64})
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides={METRIC: pl.Float64}
        )

        returned_lf = job.filter_out_zero_job_role_filled_posts(test_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
