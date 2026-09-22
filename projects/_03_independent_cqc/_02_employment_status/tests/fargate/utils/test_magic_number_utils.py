import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.magic_number_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_test_data import (
    TestMagicNumberUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

METRIC = IndCQC.estimate_filled_posts_by_job_role_historically_reallocated

JOB_ROLE_ESTIMATES_SCHEMA_OVERRIDES = {
    IndCQC.primary_service_type: CatColType.PrimaryServiceEnumType,
    IndCQC.published_job_role_label: CatColType.PublishedJobRoleLabelCatType,
    METRIC: pl.Float64,
}

EXPECTED_SCHEMA_OVERRIDES = {
    **JOB_ROLE_ESTIMATES_SCHEMA_OVERRIDES,
    EmpStatus.estimated_emp_stat_perm: pl.Float64,
    EmpStatus.estimated_emp_stat_temp: pl.Float64,
    EmpStatus.estimated_emp_stat_bank_or_pool: pl.Float64,
    EmpStatus.estimated_emp_stat_agency: pl.Float64,
    EmpStatus.estimated_emp_stat_other: pl.Float64,
    EmpStatus.estimated_employees: pl.Float64,
}


class TestApplyEmploymentStatusMagicNumbers:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.apply_employment_status_magic_numbers_test_cases
        ],
    )
    def test_splits_metric_and_computes_estimated_employees_column_as_expected(
        self, case
    ):
        job_role_estimates_lf = pl.LazyFrame(
            case.job_role_estimates_data,
            schema_overrides=JOB_ROLE_ESTIMATES_SCHEMA_OVERRIDES,
            orient="row",
        )
        employment_status_rates_lf = pl.LazyFrame(case.employment_status_rates_data)
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=EXPECTED_SCHEMA_OVERRIDES, orient="row"
        )

        returned_lf = job.apply_employment_status_magic_numbers(
            job_role_estimates_lf, employment_status_rates_lf
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
            check_row_order=False,
            check_column_order=False,
        )
