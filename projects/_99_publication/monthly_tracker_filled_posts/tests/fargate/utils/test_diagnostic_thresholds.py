from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.diagnostic_thresholds as job
import projects._99_publication.unittest_data.polars_pub_test_data as Data
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub

Cols = job.Cols

long_table_schema = pl.Schema(
    [
        (Cols.metric, pl.String()),
        (Cols.service_type, pl.String()),
        (Cols.period, pl.Date()),
        (Cols.value, pl.Float64()),
    ]
)


def _with_column(schema: pl.Schema, name: str, dtype: pl.DataType) -> pl.Schema:
    return pl.Schema(list(schema.items()) + [(name, dtype)])


class TestToMetricLongFormat:
    clean_schema = pl.Schema(
        [
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
            (Pub.publication_filled_posts, pl.Float32()),
            (Pub.publication_locationid_count, pl.UInt32()),
            (Pub.assessment_filled_posts_long_term, pl.Float32()),
            (Pub.assessment_ct_total_employed_long_term, pl.Float32()),
            (Pub.assessment_filled_posts_medium_term, pl.Float32()),
            (Pub.assessment_ct_total_employed_medium_term, pl.Float32()),
            (Pub.assessment_filled_posts_short_term, pl.Float32()),
            (Pub.assessment_ct_total_employed_short_term, pl.Float32()),
        ]
    )
    window_from_dates = {
        "long_term": date(2021, 7, 1),
        "medium_term": date(2025, 4, 1),
        "short_term": date(2026, 4, 1),
    }

    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.to_metric_long_format_test_cases],
    )
    def test_returns_workbook_series_in_long_format(self, case):
        test_lf = pl.LazyFrame(case.input_data, self.clean_schema, orient="row")

        returned_lf = job.to_metric_long_format(test_lf, self.window_from_dates).filter(
            pl.col(Cols.metric).is_in(case.metrics)
        )

        expected_lf = pl.LazyFrame(case.expected_data, long_table_schema, orient="row")
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestAddIntervalType:
    @pytest.mark.parametrize(
        "case", [case.as_pytest_param() for case in Data.add_interval_type_test_cases]
    )
    def test_labels_each_step_by_the_gap_to_the_previous_period(self, case):
        expected_schema = _with_column(
            long_table_schema, Cols.interval_type, pl.String()
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        returned_lf = job.add_interval_type(expected_lf.drop(Cols.interval_type))

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddPeriodOnPeriodChange:
    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.add_period_on_period_change_test_cases
        ],
    )
    def test_returns_percentage_change_against_the_previous_period(self, case):
        expected_schema = _with_column(
            long_table_schema, Cols.period_on_period_change, pl.Float64()
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        returned_lf = job.add_period_on_period_change(
            expected_lf.drop(Cols.period_on_period_change)
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddChangeSinceMarch:
    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.add_change_since_march_test_cases],
    )
    def test_returns_percentage_change_since_the_reporting_year_baseline(self, case):
        expected_schema = _with_column(
            long_table_schema, Cols.change_since_march, pl.Float64()
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        returned_lf = job.add_change_since_march(
            expected_lf.drop(Cols.change_since_march)
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddSfcCtGap:
    gap_schema = pl.Schema(
        [
            (Cols.metric, pl.String()),
            (Cols.service_type, pl.String()),
            (Cols.period, pl.Date()),
            (Cols.interval_type, pl.String()),
            (Cols.period_on_period_change, pl.Float64()),
        ]
    )

    @pytest.mark.parametrize(
        "case", [case.as_pytest_param() for case in Data.add_sfc_ct_gap_test_cases]
    )
    def test_returns_sfc_minus_ct_step_change_per_window(self, case):
        test_lf = pl.LazyFrame(case.input_data, self.gap_schema, orient="row")

        returned_lf = job.add_sfc_ct_gap(test_lf)

        expected_lf = pl.LazyFrame(case.expected_data, self.gap_schema, orient="row")
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


def _history_method_frames(case) -> tuple[pl.LazyFrame, pl.LazyFrame]:
    schema = pl.Schema(
        [
            (Cols.metric, pl.String()),
            (Cols.service_type, pl.String()),
            (Cols.period, pl.Date()),
            (Cols.interval_type, pl.String()),
            (case.value_col, pl.Float64()),
            (Cols.breached, pl.Boolean()),
            (Cols.insufficient_history, pl.Boolean()),
        ]
    )
    expected_lf = pl.LazyFrame(case.rows, schema, orient="row")
    test_lf = expected_lf.drop(Cols.breached, Cols.insufficient_history)
    return test_lf, expected_lf


class TestFlagMeanStd:
    @pytest.mark.parametrize(
        "case", [case.as_pytest_param() for case in Data.flag_mean_std_test_cases]
    )
    def test_flags_values_outside_mean_plus_or_minus_k_std(self, case):
        test_lf, expected_lf = _history_method_frames(case)

        returned_lf = job.flag_mean_std(test_lf, case.value_col, case.k).select(
            expected_lf.collect_schema().names()
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestFlagMedianMad:
    @pytest.mark.parametrize(
        "case", [case.as_pytest_param() for case in Data.flag_median_mad_test_cases]
    )
    def test_flags_values_outside_median_plus_or_minus_k_mad(self, case):
        test_lf, expected_lf = _history_method_frames(case)

        returned_lf = job.flag_median_mad(test_lf, case.value_col, case.k).select(
            expected_lf.collect_schema().names()
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


band_method_schema = pl.Schema(
    [
        (Cols.metric, pl.String()),
        (Cols.service_type, pl.String()),
        (Cols.period, pl.Date()),
        (Cols.interval_type, pl.String()),
        (Cols.period_on_period_change, pl.Float64()),
        (Cols.lower, pl.Float64()),
        (Cols.upper, pl.Float64()),
        (Cols.breached, pl.Boolean()),
    ]
)
band_limits = {
    job.IntervalType.monthly: 0.02,
    job.IntervalType.quarterly: 0.05,
}


class TestFlagFixedBand:
    @pytest.mark.parametrize(
        "case", [case.as_pytest_param() for case in Data.flag_fixed_band_test_cases]
    )
    def test_flags_values_outside_the_interval_types_tolerance(self, case):
        expected_lf = pl.LazyFrame(case.rows, band_method_schema, orient="row")
        test_lf = expected_lf.drop(Cols.lower, Cols.upper, Cols.breached)

        returned_lf = job.flag_fixed_band(
            test_lf, Cols.period_on_period_change, band_limits
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestFlagCrossSectional:
    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.flag_cross_sectional_test_cases],
    )
    def test_flags_base_types_moving_away_from_the_others(self, case):
        expected_lf = pl.LazyFrame(case.rows, band_method_schema, orient="row")
        test_lf = expected_lf.drop(Cols.lower, Cols.upper, Cols.breached)

        returned_lf = job.flag_cross_sectional(
            test_lf, Cols.period_on_period_change, band_limits
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestAssignTier:
    tier_schema = pl.Schema(
        [
            ("warn_breached", pl.Boolean()),
            ("error_breached", pl.Boolean()),
            (Cols.tier, pl.String()),
        ]
    )

    @pytest.mark.parametrize(
        "case", [case.as_pytest_param() for case in Data.assign_tier_test_cases]
    )
    def test_returns_the_highest_tier_breached(self, case):
        expected_lf = pl.LazyFrame(case.expected_data, self.tier_schema, orient="row")
        test_lf = expected_lf.drop(Cols.tier)

        returned_lf = test_lf.with_columns(
            job.assign_tier("warn_breached", "error_breached")
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestSummariseBacktest:
    flags_schema = pl.Schema(
        [
            (Cols.method, pl.String()),
            (Cols.setting, pl.String()),
            (Cols.basis, pl.String()),
            (Cols.metric, pl.String()),
            (Cols.service_type, pl.String()),
            (Cols.period, pl.Date()),
            (Cols.value, pl.Float64()),
            (Cols.breached, pl.Boolean()),
        ]
    )
    breaks_schema = pl.Schema(
        [
            (Cols.metric, pl.String()),
            (Cols.service_type, pl.String()),
            (Cols.period, pl.Date()),
        ]
    )
    summary_schema = pl.Schema(
        [
            (Cols.method, pl.String()),
            (Cols.setting, pl.String()),
            (Cols.basis, pl.String()),
            (Cols.metric, pl.String()),
            (Cols.service_type, pl.String()),
            (Cols.evaluated_periods, pl.UInt32()),
            (Cols.false_alarms, pl.UInt32()),
            (Cols.expected_breaks, pl.UInt32()),
            (Cols.detected_breaks, pl.UInt32()),
        ]
    )

    def _summarise(self, case) -> pl.LazyFrame:
        return job.summarise_backtest(
            pl.LazyFrame(case.filter_on_flags, self.flags_schema, orient="row"),
            pl.LazyFrame(case.filter_off_flags, self.flags_schema, orient="row"),
            pl.LazyFrame(case.expected_breaks, self.breaks_schema, orient="row"),
        )

    @pytest.mark.parametrize(
        "case", [case.as_pytest_param() for case in Data.summarise_backtest_test_cases]
    )
    def test_counts_false_alarms_and_detected_breaks(self, case):
        returned_lf = self._summarise(case)

        expected_lf = pl.LazyFrame(
            case.expected_data, self.summary_schema, orient="row"
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_output_contains_no_metric_values(self):
        case = Data.summarise_backtest_test_cases[0]

        returned_columns = self._summarise(case).collect_schema().names()

        assert returned_columns == self.summary_schema.names()
