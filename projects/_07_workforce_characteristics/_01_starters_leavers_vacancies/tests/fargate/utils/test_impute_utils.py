import polars as pl
import polars.testing as pl_testing
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.impute_utils as job
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_data import (
    TestImputeUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.slv_job_role_columns import SLVJobRoleColumns as SLVCols

SCHEMA_OVERRIDES = {
    SLVCols.turnover_rate_dedup: pl.Float32,
    SLVCols.turnover_rate_imputed: pl.Float32,
    SLVCols.starter_rate_dedup: pl.Float32,
    SLVCols.starter_rate_imputed: pl.Float32,
}


class TestForwardFillWithinTimeLimit:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.forward_fill_within_time_limit_test_cases
        ],
    )
    def test_returns_expected_fill_values(self, case):
        test_lf = pl.LazyFrame(case.input_data, schema_overrides=SCHEMA_OVERRIDES)
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=SCHEMA_OVERRIDES
        )

        returned_lf = job.forward_fill_within_time_limit(
            test_lf,
            columns_to_fill=case.columns_to_fill,
            partition_by_columns=[IndCQC.location_id, SLVCols.published_job_role_label],
            date_column=IndCQC.cqc_location_import_date,
            time_limit="6mo",
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )
