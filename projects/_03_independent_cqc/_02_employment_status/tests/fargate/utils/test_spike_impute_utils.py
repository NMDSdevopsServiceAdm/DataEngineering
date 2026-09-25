import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.spike_impute_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_test_data import (
    TestSpikeImputeUtilsL1Data as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusColumns as EmpStatus,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusSpikeColumns as SpikeCols,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

KEY_SCHEMA = {
    IndCQC.location_id: CatColType.LocationCatType,
    IndCQC.published_job_role_label: CatColType.PublishedJobRoleLabelCatType,
    IndCQC.primary_service_type: CatColType.PrimaryServiceEnumType,
    IndCQC.estimate_filled_posts_by_job_role: pl.Float64,
}
PERCENTAGE_SCHEMA = {
    column: pl.Float32
    for column in [
        EmpStatus.permanent_percentage,
        EmpStatus.temporary_percentage,
        EmpStatus.bank_or_pool_percentage,
        EmpStatus.agency_percentage,
        EmpStatus.other_percentage,
    ]
}
LONG_SCHEMA = {
    SpikeCols.employment_status_label: CatColType.EmploymentStatusCatType,
    SpikeCols.employment_status_rate: pl.Float32,
    SpikeCols.first_known_value: pl.Float32,
    SpikeCols.last_known_value: pl.Float32,
    SpikeCols.unnormalised_rate: pl.Float32,
    SpikeCols.imputed_employment_status_rate: pl.Float32,
}


def schema_for(data: dict) -> dict:
    """Returns the schema overrides relevant to the columns in `data`."""
    all_overrides = KEY_SCHEMA | PERCENTAGE_SCHEMA | LONG_SCHEMA
    return {col: dtype for col, dtype in all_overrides.items() if col in data}


class TestReshapeEmploymentStatusPercentagesToLongRows:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.reshape_test_cases],
    )
    def test_reshapes_as_expected(self, case):
        test_lf = pl.LazyFrame(
            case.input_data, schema_overrides=schema_for(case.input_data)
        )
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=schema_for(case.expected_data)
        )

        returned_lf = job.reshape_employment_status_percentages_to_long_rows(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_row_order=False, check_column_order=False
        )


class TestAddFillBoundaries:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_fill_boundaries_test_cases
        ],
    )
    def test_adds_boundaries_as_expected(self, case):
        test_lf = pl.LazyFrame(
            case.input_data, schema_overrides=schema_for(case.input_data)
        )
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=schema_for(case.expected_data)
        )

        returned_lf = job.add_fill_boundaries(test_lf)

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_row_order=False, check_column_order=False
        )


class TestNormaliseEmploymentStatusRatesWithinJobRole:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.normalise_test_cases],
    )
    def test_normalises_as_expected(self, case):
        test_lf = pl.LazyFrame(
            case.input_data, schema_overrides=schema_for(case.input_data)
        )
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=schema_for(case.expected_data)
        )

        returned_lf = job.normalise_employment_status_rates_within_job_role(
            test_lf,
            rate_column=SpikeCols.unnormalised_rate,
            output_column=SpikeCols.imputed_employment_status_rate,
        )

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_row_order=False, check_column_order=False
        )
