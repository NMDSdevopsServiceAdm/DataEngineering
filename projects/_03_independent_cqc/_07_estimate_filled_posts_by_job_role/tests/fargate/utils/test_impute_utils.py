from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc._07_estimate_filled_posts_by_job_role.fargate.utils.impute_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ImputeJobRoleData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ImputeJobRoleSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    MainJobRoleLabels,
    PrimaryServiceType,
)


class TestGetPercentageShareRatios:
    def test_over_groups(self):
        expected_lf = pl.LazyFrame(
            data=[
                (1, "1", date(2026, 1, 1), 1, 0.3333),
                (2, "1", date(2026, 1, 1), 2, 0.6667),
                (3, "1", date(2026, 2, 1), 2, 0.5),
                (4, "1", date(2026, 2, 1), 2, 0.5),
                (5, "2", date(2026, 1, 1), 2, 0.4),
                (6, "2", date(2026, 1, 1), 3, 0.6),
            ],
            schema={
                IndCQC.id_per_locationid_import_date_job_role: pl.Int64,
                IndCQC.location_id: pl.String,
                IndCQC.cqc_location_import_date: pl.Date,
                "vals": pl.Int64,
                "ratios": pl.Float32,
            },
            orient="row",
        )
        input_lf = expected_lf.drop("ratios")
        returned_lf = job.get_percent_share_ratios(
            input_lf, input_col="vals", output_col="ratios"
        ).sort(IndCQC.id_per_locationid_import_date_job_role)
        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.001)


class TestAddFillBoundaries:
    def test_boundaries_describe_each_job_role_series(self):
        expected_lf = pl.LazyFrame(
            data=[
                # A gap, so the nearest known dates differ from the row's own date.
                ("1", "care_worker", date(2024, 1, 1), 0.4, date(2024, 1, 1), date(2024, 3, 1), date(2024, 1, 1), date(2024, 1, 1), 0.4, 0.6),
                ("1", "care_worker", date(2024, 2, 1), None, date(2024, 1, 1), date(2024, 3, 1), date(2024, 1, 1), date(2024, 3, 1), 0.4, 0.6),
                ("1", "care_worker", date(2024, 3, 1), 0.6, date(2024, 1, 1), date(2024, 3, 1), date(2024, 3, 1), date(2024, 3, 1), 0.4, 0.6),
                # A second job role with its own boundaries.
                ("1", "registered_nurse", date(2024, 1, 1), None, date(2024, 2, 1), date(2024, 2, 1), None, date(2024, 2, 1), 0.9, 0.9),
                ("1", "registered_nurse", date(2024, 2, 1), 0.9, date(2024, 2, 1), date(2024, 2, 1), date(2024, 2, 1), date(2024, 2, 1), 0.9, 0.9),
                ("1", "registered_nurse", date(2024, 3, 1), None, date(2024, 2, 1), date(2024, 2, 1), date(2024, 2, 1), None, 0.9, 0.9),
            ],
            schema={
                IndCQC.location_id: pl.String,
                IndCQC.main_job_role_clean_labelled: pl.String,
                IndCQC.cqc_location_import_date: pl.Date,
                IndCQC.ascwds_job_role_ratios: pl.Float32,
                job.TempCols.first_known_date: pl.Date,
                job.TempCols.last_known_date: pl.Date,
                job.TempCols.previous_known_date: pl.Date,
                job.TempCols.next_known_date: pl.Date,
                job.TempCols.first_known_value: pl.Float32,
                job.TempCols.last_known_value: pl.Float32,
            },
            orient="row",
        )  # fmt: skip
        input_lf = expected_lf.select(
            IndCQC.location_id,
            IndCQC.main_job_role_clean_labelled,
            IndCQC.cqc_location_import_date,
            IndCQC.ascwds_job_role_ratios,
        )

        returned_lf = job.add_fill_boundaries(input_lf).sort(
            IndCQC.main_job_role_clean_labelled, IndCQC.cqc_location_import_date
        )

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False, rel_tol=0.0001
        )


class TestAddImputedJobRoleRatiosForTrendline:
    @pytest.mark.parametrize(
        "add_imputed_job_role_ratios_for_trendline_data",
        [
            case.as_pytest_param()
            for case in Data.add_imputed_job_role_ratios_for_trendline_test_cases
        ],
    )
    def test_add_imputed_job_role_ratios_for_trendline(
        self, add_imputed_job_role_ratios_for_trendline_data
    ):
        expected_lf = pl.LazyFrame(
            add_imputed_job_role_ratios_for_trendline_data,
            Schemas.add_imputed_job_role_ratios_for_trendline_expected_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(IndCQC.imputed_job_role_ratios_for_trendline)
        returned_lf = job.add_imputed_job_role_ratios_for_trendline(
            input_lf, extrapolation_period="2y", interpolation_cap_period="5y"
        ).select(
            Schemas.add_imputed_job_role_ratios_for_trendline_expected_schema.keys()
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.0001)


class TestAddImputedASCWDSJobRoleRatios:
    @pytest.mark.parametrize(
        "add_imputed_ascwds_job_role_ratios_data",
        [
            case.as_pytest_param()
            for case in Data.add_imputed_ascwds_job_role_ratios_test_cases
        ],
    )
    def test_add_imputed_ascwds_job_role_ratios(
        self, add_imputed_ascwds_job_role_ratios_data
    ):
        expected_lf = pl.LazyFrame(
            add_imputed_ascwds_job_role_ratios_data,
            Schemas.add_imputed_ascwds_job_role_ratios_expected_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(IndCQC.imputed_ascwds_job_role_ratios)

        # Comparing every column, so a leaked temp column fails the test.
        returned_lf = job.add_imputed_ascwds_job_role_ratios(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.0001)

    def test_submitted_ratios_are_returned_unchanged(self):
        # These three total 0.99999994 in Float32, so re-sharing them would move each by a
        # rounding step and the imputed column would no longer match what was submitted.
        expected_lf = pl.LazyFrame(
            data=[
                ("1", MainJobRoleLabels.care_worker,        date(2024, 1, 1), 18 / 55, 0.5, 18 / 55),
                ("1", MainJobRoleLabels.registered_nurse,   date(2024, 1, 1), 29 / 55, 0.5, 29 / 55),
                ("1", MainJobRoleLabels.senior_care_worker, date(2024, 1, 1), 8 / 55,  0.5, 8 / 55),
            ],
            schema=Schemas.add_imputed_ascwds_job_role_ratios_expected_schema,
            orient="row",
        )  # fmt: skip
        input_lf = expected_lf.drop(IndCQC.imputed_ascwds_job_role_ratios)

        returned_lf = job.add_imputed_ascwds_job_role_ratios(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_exact=True)

    def test_result_correct_when_source_rows_not_sorted_by_date(self):
        sort_key = [
            IndCQC.location_id,
            IndCQC.main_job_role_clean_labelled,
            IndCQC.cqc_location_import_date,
        ]
        # The Jan 1 to Feb 1 gap is 31 of the 60 days spanning Jan 1 to Mar 1, hence
        # 0.696667, not the midpoint 0.70.
        expected_lf = pl.LazyFrame(
            data=[
                ("1", MainJobRoleLabels.care_worker,      date(2024, 1, 1), 0.8,  0.5, 0.8),
                ("1", MainJobRoleLabels.registered_nurse, date(2024, 1, 1), 0.2,  0.5, 0.2),
                ("1", MainJobRoleLabels.care_worker,      date(2024, 2, 1), None, 0.5, 0.696667),
                ("1", MainJobRoleLabels.registered_nurse, date(2024, 2, 1), None, 0.5, 0.303333),
                ("1", MainJobRoleLabels.care_worker,      date(2024, 3, 1), 0.6,  0.5, 0.6),
                ("1", MainJobRoleLabels.registered_nurse, date(2024, 3, 1), 0.4,  0.5, 0.4),
            ],
            schema=Schemas.add_imputed_ascwds_job_role_ratios_expected_schema,
            orient="row",
        )  # fmt: skip
        # Rows can arrive unsorted, e.g. read back from Athena.
        input_lf = expected_lf.drop(IndCQC.imputed_ascwds_job_role_ratios).sort(
            IndCQC.cqc_location_import_date, descending=True
        )

        returned_lf = (
            job.add_imputed_ascwds_job_role_ratios(input_lf)
            .select(Schemas.add_imputed_ascwds_job_role_ratios_expected_schema.keys())
            .sort(sort_key)
        )

        pl_testing.assert_frame_equal(
            returned_lf, expected_lf.sort(sort_key), rel_tol=0.0001
        )


class TestAddImputedASCWDSJobRoleCounts:
    def test_counts_are_the_estimated_posts_at_the_imputed_ratio(self):
        expected_lf = pl.LazyFrame(
            data=[
                (10.0, 0.4, 4.0),
                (10.0, None, None),
                (None, 0.4, None),
            ],
            schema={
                IndCQC.estimate_filled_posts: pl.Float32,
                IndCQC.imputed_ascwds_job_role_ratios: pl.Float32,
                IndCQC.imputed_ascwds_job_role_counts: pl.Float32,
            },
            orient="row",
        )
        input_lf = expected_lf.drop(IndCQC.imputed_ascwds_job_role_counts)

        returned_lf = job.add_imputed_ascwds_job_role_counts(input_lf)

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestCreateASCWDSJobRoleRollingRatio:
    @pytest.mark.parametrize(
        "create_ascwds_job_role_rolling_ratio_data",
        [
            case.as_pytest_param()
            for case in Data.create_ascwds_job_role_rolling_ratio_test_cases
        ],
    )
    def test_create_ascwds_job_role_rolling_ratio(
        self, create_ascwds_job_role_rolling_ratio_data
    ):
        expected_lf = pl.LazyFrame(
            create_ascwds_job_role_rolling_ratio_data,
            Schemas.create_ascwds_job_role_rolling_ratio_expected_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(
            IndCQC.ascwds_job_role_rolling_ratio,
            IndCQC.estimate_filled_posts_size_group,
        )
        # The trendline ratios are also returned, but have their own test class.
        returned_lf = job.create_ascwds_job_role_rolling_ratio(
            input_lf, extrapolation_period="2y", interpolation_cap_period="5y"
        ).select(Schemas.create_ascwds_job_role_rolling_ratio_expected_schema.keys())
        pl_testing.assert_frame_equal(
            returned_lf, expected_lf, check_column_order=False, rel_tol=0.0001
        )


class TestEstimateFilledPostsSizeGroupExpression:
    def test_estimate_filled_posts_size_group_expression(self):
        expected_lf = pl.LazyFrame(
            data=[
                # non-residential
                (24.999, PrimaryServiceType.non_residential, "NR 1 to 24"),
                (25.0, PrimaryServiceType.non_residential, "NR 25 to 49"),
                (33, PrimaryServiceType.non_residential, "NR 25 to 49"),
                (50.0, PrimaryServiceType.non_residential, "NR 50 to 74"),
                (99.999, PrimaryServiceType.non_residential, "NR 75 to 99"),
                (100.0, PrimaryServiceType.non_residential, "NR 100 plus"),
                # care home only
                (1.0, PrimaryServiceType.care_home_only, "COH 1 to 9"),
                (9.999, PrimaryServiceType.care_home_only, "COH 1 to 9"),
                (10.0, PrimaryServiceType.care_home_only, "COH 10 to 19"),
                (19.999, PrimaryServiceType.care_home_only, "COH 10 to 19"),
                (21.0, PrimaryServiceType.care_home_only, "COH 20 to 29"),
                (30.0, PrimaryServiceType.care_home_only, "COH 30 plus"),
                # care home with nursing
                (1.0, PrimaryServiceType.care_home_with_nursing, "CHWN 1 to 19"),
                (19.999, PrimaryServiceType.care_home_with_nursing, "CHWN 1 to 19"),
                (20.0, PrimaryServiceType.care_home_with_nursing, "CHWN 20 to 29"),
                (29.999, PrimaryServiceType.care_home_with_nursing, "CHWN 20 to 29"),
                (30.0, PrimaryServiceType.care_home_with_nursing, "CHWN 30 plus"),
                # unmatched
                (0.0, PrimaryServiceType.non_residential, None),
                (None, PrimaryServiceType.non_residential, None),
                (25.0, "Other Service", None),
            ],
            schema={
                IndCQC.estimate_filled_posts: pl.Float32,
                IndCQC.primary_service_type: pl.String,
                IndCQC.estimate_filled_posts_size_group: pl.String,
            },
            orient="row",
        )
        input_lf = expected_lf.drop(IndCQC.estimate_filled_posts_size_group)
        returned_lf = input_lf.with_columns(
            job.estimate_filled_posts_size_group_expression()
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, rel_tol=0.001)
