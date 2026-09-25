import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.spike_impute_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc._02_employment_status.fargate.utils.spike_columns import (
    EmploymentStatusSpikeColumns as SpikeCols,
)
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_test_data import (
    TestSpikeImputeUtilsL2Data as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

IMPUTED_COLUMNS = [
    job.status_column(SpikeCols.imputed_employment_status_rate, label)
    for label in job.EMPLOYMENT_STATUS_PERCENTAGE_COLUMNS
]


def schema_for(data: dict) -> dict:
    """Returns schema overrides for `data`: categorical keys, Float32 rates."""
    overrides = {
        IndCQC.location_id: CatColType.LocationCatType,
        IndCQC.published_job_role_label: CatColType.PublishedJobRoleLabelCatType,
        IndCQC.primary_service_type: CatColType.PrimaryServiceEnumType,
        IndCQC.estimate_filled_posts_by_job_role: pl.Float64,
    }
    key_columns = set(overrides) | {IndCQC.cqc_location_import_date}
    rate_columns = {col: pl.Float32 for col in data if col not in key_columns}
    return {
        col: dtype for col, dtype in (overrides | rate_columns).items() if col in data
    }


def run_impute_chain(
    input_lf: pl.LazyFrame, percentage_columns: dict[str, str] | None = None
) -> pl.DataFrame:
    """Runs the full L2 rolling-ratio and impute chain with ticket 2000's periods."""
    wide_lf = job.add_rolling_employment_status_ratios(
        input_lf,
        extrapolation_period="2y",
        interpolation_cap_period="5y",
        percentage_columns=percentage_columns,
    )
    return job.add_imputed_employment_status_rates(
        wide_lf, percentage_columns=percentage_columns
    ).collect()


class TestAddImputedEmploymentStatusRates:
    def test_does_not_change_row_count(self):
        input_lf = pl.LazyFrame(
            Data.impute_chain_rows, schema_overrides=schema_for(Data.impute_chain_rows)
        )

        returned_df = run_impute_chain(input_lf)

        assert returned_df.height == input_lf.collect().height

    def test_imputes_all_five_employment_status_columns(self):
        input_lf = pl.LazyFrame(
            Data.impute_chain_rows, schema_overrides=schema_for(Data.impute_chain_rows)
        )

        returned_df = run_impute_chain(input_lf)

        gap_row = returned_df.filter(
            (pl.col(IndCQC.location_id) == "loc1")
            & (pl.col(IndCQC.cqc_location_import_date) == pl.date(2024, 2, 1))
        )
        assert gap_row.select(pl.col(IMPUTED_COLUMNS).is_not_null().all()).row(0) == (
            True,
        ) * len(IMPUTED_COLUMNS)
        assert gap_row.select(pl.sum_horizontal(IMPUTED_COLUMNS)).item() == (
            pytest.approx(1.0, abs=1e-6)
        )

    def test_imputes_all_columns_in_custom_percentage_mapping(self):
        input_lf = pl.LazyFrame(
            Data.impute_chain_rows_with_dummy,
            schema_overrides=schema_for(Data.impute_chain_rows_with_dummy),
        )
        imputed_columns = [
            job.status_column(SpikeCols.imputed_employment_status_rate, label)
            for label in Data.custom_percentage_columns
        ]

        returned_df = run_impute_chain(input_lf, Data.custom_percentage_columns)

        gap_row = returned_df.filter(
            (pl.col(IndCQC.location_id) == "loc1")
            & (pl.col(IndCQC.cqc_location_import_date) == pl.date(2024, 2, 1))
        )
        assert len(imputed_columns) == 6
        assert gap_row.select(pl.col(imputed_columns).is_not_null().all()).row(0) == (
            True,
        ) * len(imputed_columns)
        assert gap_row.select(pl.sum_horizontal(imputed_columns)).item() == (
            pytest.approx(1.0, abs=1e-6)
        )


class TestNormaliseEmploymentStatusRatesRowWise:
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

        returned_lf = job.normalise_employment_status_rates_row_wise(
            test_lf, percentage_columns=case.percentage_columns
        )

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_row_order=False, check_column_order=False
        )
