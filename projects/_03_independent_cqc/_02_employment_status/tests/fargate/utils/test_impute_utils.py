from datetime import date
from typing import Any

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._02_employment_status.fargate.utils.impute_utils as job
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_test_data import (
    CARE_WORKER,
    FIRST_KNOWN_VALUE_COLUMNS,
    IMPUTED_PCT_COLUMNS,
    LAST_KNOWN_VALUE_COLUMNS,
    LONDON,
    NON_RES,
    NORTH_EAST,
    PCT_COLUMNS,
    ROLLING_AVG_PCT_COLUMNS,
    ROLLING_AVERAGE_PERIOD,
    SHORT_TERM_EXTRAPOLATION_PERIOD,
    SHORT_TERM_INTERPOLATION_CAP_PERIOD,
    status_split,
    IMPUTED_TRENDLINE_PCT_COLUMNS,
)
from projects._03_independent_cqc._02_employment_status.unittest_data.polars_employment_status_test_data import (
    TestImputeUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    EmploymentStatusImputeTempColumns as ImputeTempCols,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

DATE_COLUMNS = [
    IndCQC.cqc_location_import_date,
    ImputeTempCols.first_known_date,
    ImputeTempCols.last_known_date,
    ImputeTempCols.previous_known_date,
    ImputeTempCols.next_known_date,
]
FLOAT32_COLUMNS = (
    PCT_COLUMNS
    + IMPUTED_TRENDLINE_PCT_COLUMNS
    + IMPUTED_PCT_COLUMNS
    + ROLLING_AVG_PCT_COLUMNS
    + FIRST_KNOWN_VALUE_COLUMNS
    + LAST_KNOWN_VALUE_COLUMNS
)


def to_lf(data: dict[str, Any]) -> pl.LazyFrame:
    """Build a LazyFrame, typing the date and percentage columns so all-null columns match."""
    schema_overrides = {
        **{col: pl.Date for col in DATE_COLUMNS if col in data},
        **{col: pl.Float32 for col in FLOAT32_COLUMNS if col in data},
        **{
            col: pl.String
            for col in [
                IndCQC.location_id,
                IndCQC.published_job_role_label,
                IndCQC.primary_service_type,
                IndCQC.current_region,
            ]
            if col in data
        },
    }
    return pl.LazyFrame(data, schema_overrides=schema_overrides)


class TestAddFillBoundaries:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_fill_boundaries_test_cases
        ],
    )
    def test_adds_fill_boundaries(self, case):
        expected_lf = to_lf(case.expected_data)

        returned_lf = job.add_fill_boundaries(to_lf(case.input_data))

        pl_testing.assert_frame_equal(
            returned_lf.select(expected_lf.collect_schema().names()),
            expected_lf,
            check_row_order=False,
        )


class TestAddShortTermImputedPercentages:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_imputed_trendline_pcts_test_cases
        ],
    )
    def test_adds_short_term_imputed_percentages(self, case):
        returned_lf = job.add_imputed_trendline_pcts(
            to_lf(case.input_data),
            extrapolation_period=SHORT_TERM_EXTRAPOLATION_PERIOD,
            interpolation_cap_period=SHORT_TERM_INTERPOLATION_CAP_PERIOD,
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            to_lf(case.expected_data),
            check_row_order=False,
            check_column_order=False,
        )


class TestAddRollingAvgPcts:
    @pytest.mark.parametrize(
        "case",
        [
            pytest.param(case, id=case.id)
            for case in Data.add_rolling_avg_pcts_test_cases
        ],
    )
    def test_adds_rolling_average_percentages(self, case):
        returned_lf = job.add_rolling_avg_pcts(
            to_lf(case.input_data), rolling_period=ROLLING_AVERAGE_PERIOD
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            to_lf(case.expected_data),
            check_row_order=False,
            check_column_order=False,
        )

    def test_add_rolling_avg_pcts_uses_given_group_columns_and_period(self):
        permanent = [0.2, 0.6, 0.8]
        input_data = {
            IndCQC.location_id: ["loc1", "loc2", "loc3"],
            IndCQC.primary_service_type: [NON_RES] * 3,
            IndCQC.current_region: [LONDON, NORTH_EAST, LONDON],
            IndCQC.published_job_role_label: [CARE_WORKER] * 3,
            IndCQC.cqc_location_import_date: [
                date(2024, 1, 1),
                date(2024, 1, 1),
                date(2024, 8, 1),
            ],
            **status_split(permanent, IMPUTED_TRENDLINE_PCT_COLUMNS),
        }
        expected_permanent = [0.4, 0.4, 1.6 / 3]

        returned_lf = job.add_rolling_avg_pcts(
            to_lf(input_data),
            rolling_period="12mo",
            group_columns=[
                IndCQC.primary_service_type,
                IndCQC.published_job_role_label,
            ],
        )

        pl_testing.assert_frame_equal(
            returned_lf,
            to_lf(
                {
                    **input_data,
                    **status_split(expected_permanent, ROLLING_AVG_PCT_COLUMNS),
                }
            ),
            check_row_order=False,
            check_column_order=False,
        )


class TestAddFullImputedPercentages:
    @pytest.mark.parametrize(
        "case",
        [pytest.param(case, id=case.id) for case in Data.add_imputed_pcts_test_cases],
    )
    def test_adds_full_imputed_percentages(self, case):
        returned_lf = job.add_imputed_pcts(to_lf(case.input_data))

        pl_testing.assert_frame_equal(
            returned_lf,
            to_lf(case.expected_data),
            check_row_order=False,
            check_column_order=False,
        )
