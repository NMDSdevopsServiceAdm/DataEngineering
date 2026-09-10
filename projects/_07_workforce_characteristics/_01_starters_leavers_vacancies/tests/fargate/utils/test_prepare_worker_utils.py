import polars as pl
import polars.testing as pl_testing
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.prepare_worker_utils as job
from projects._07_workforce_characteristics.unittest_data.polars_slv_test_data import (
    TestPrepareUtilsData as Data,
)
from utils.column_names.slv_job_role_columns import (
    SLVEmploymentStatusColumns as SLVEmpStatus,
)


class TestAggregateEmploymentStatusData:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.aggregate_employment_status_data_test_cases
        ],
    )
    def test_aggregates_employment_status_data(self, case):
        test_lf = pl.LazyFrame(case.input_data)
        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema_overrides={SLVEmpStatus.employment_status_count: pl.UInt32},
        )

        returned_lf = job.aggregate_employment_status_data(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )


class TestReshapeEmploymentStatusData:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.reshape_employment_status_data_test_cases
        ],
    )
    def test_reshapes_employment_status_data(self, case):
        emplstat_count_cols = [
            SLVEmpStatus.employment_status_count,
            SLVEmpStatus.emplstat_perm_count,
            SLVEmpStatus.emplstat_temp_count,
            SLVEmpStatus.emplstat_bank_or_pool_count,
            SLVEmpStatus.emplstat_agency_count,
            SLVEmpStatus.emplstat_other_count,
        ]
        test_lf = pl.LazyFrame(
            case.input_data,
            schema_overrides={SLVEmpStatus.employment_status_count: pl.UInt32},
        )
        expected_lf = pl.LazyFrame(
            case.expected_data,
            schema_overrides={
                column: pl.UInt32
                for column in emplstat_count_cols
                if column in case.expected_data
            },
        )

        returned_lf = job.reshape_employment_status_data(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )
