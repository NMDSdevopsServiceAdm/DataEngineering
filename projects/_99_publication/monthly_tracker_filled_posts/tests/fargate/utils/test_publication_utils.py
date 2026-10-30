from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.publication_utils as job
import projects._99_publication.unittest_data.polars_pub_test_data as Data
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PrimaryServiceType,
    PublishedJobGroupLabels,
    PublishedMainService,
    PublishedRegion,
)


class TestReducedDataFilterExpr:
    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.reduced_data_filter_test_cases],
    )
    def test_function_returns_expected_values(self, case):
        date_col = IndCQC.cqc_location_import_date

        expr = job.reduced_data_filter_expr(
            today=case.today,
            fy_start_month=case.fy_start_month,
            lookback_fy_years=case.lookback_fy_years,
            quarter_months=case.quarter_months,
            date_col=date_col,
            cutoff_date=case.cutoff_date,
        )

        df = pl.DataFrame({date_col: case.input_data})

        result = df.with_columns(expr.alias("keep"))

        assert result["keep"].to_list() == case.expected

    def test_function_keeps_monthly_data_from_previous_financial_year_by_default(
        self,
    ):
        date_col = IndCQC.cqc_location_import_date
        expr = job.reduced_data_filter_expr(today=date(2026, 10, 14))

        df = pl.DataFrame({date_col: Data.default_lookback_input_dates})
        result = df.with_columns(expr.alias("keep"))

        assert result["keep"].to_list() == Data.default_lookback_expected


class TestGetRetentionCutoffDate:
    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.retention_cutoff_date_test_cases],
    )
    def test_returns_start_of_financial_year_six_years_back(self, case):
        returned_date = job.get_retention_cutoff_date(case.today)

        assert returned_date == case.expected


class TestHasContinuousDataSinceDate:
    has_continuous_data_since_data_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.ct_care_home_total_employed_imputed, pl.Float32()),
            (IndCQC.ct_non_res_care_workers_employed_imputed, pl.Float32()),
        ]
    )

    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.has_continuous_data_since_date_test_cases
        ],
    )
    def test_flags_locations_with_continuous_data_since_a_given_date(self, case):
        expected_schema = pl.Schema(
            list(self.has_continuous_data_since_data_schema.items())
            + [(case.column_alias, pl.Boolean())]
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        test_lf = expected_lf.drop(case.column_alias)

        returned_lf = test_lf.with_columns(
            job.has_continuous_data_since_date(
                case.column_name, case.from_date, case.column_alias
            )
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestHasConsistentServiceSinceDate:
    schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (
                IndCQC.primary_service_type,
                pl.Enum(
                    [
                        PrimaryServiceType.care_home_with_nursing,
                        PrimaryServiceType.care_home_only,
                        PrimaryServiceType.non_residential,
                    ]
                ),
            ),
        ]
    )

    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.has_consistent_service_since_date_test_cases
        ],
    )
    def test_flags_locations_with_a_single_primary_service_type_since_a_given_date(
        self, case
    ):
        expected_schema = pl.Schema(
            list(self.schema.items()) + [(Pub.consistent_service_short, pl.Boolean())]
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        test_lf = expected_lf.drop(Pub.consistent_service_short)

        returned_lf = test_lf.with_columns(
            job.has_consistent_service_since_date(
                case.from_date, Pub.consistent_service_short
            )
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddDispersionFilter:
    dispersion_filter_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.ct_care_home_total_employed_imputed, pl.Float32()),
            (IndCQC.ct_non_res_care_workers_employed_imputed, pl.Float32()),
        ]
    )

    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.add_dispersion_filter_test_cases],
    )
    def test_identifies_locations_within_ct_posts_dispersion_boundaries(self, case):
        expected_schema = pl.Schema(
            list(self.dispersion_filter_schema.items())
            + [(case.column_alias, pl.Boolean())]
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        test_lf = expected_lf.drop(case.column_alias)

        returned_lf = job.add_dispersion_filter(
            test_lf, case.column_names, case.from_date, case.column_alias
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAggregateToPublicationRows:
    input_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
            (
                IndCQC.estimate_filled_posts_by_job_role,
                pl.Float32(),
            ),
            (Pub.ct_total_employed_imputed, pl.Float32()),
            (Pub.consistent_service_long, pl.Boolean()),
            (Pub.consistent_service_medium, pl.Boolean()),
            (Pub.consistent_service_short, pl.Boolean()),
            (Pub.ct_has_data_long, pl.Boolean()),
            (Pub.ct_has_data_medium, pl.Boolean()),
            (Pub.ct_has_data_short, pl.Boolean()),
            (Pub.ct_dispersion_filter_long, pl.Boolean()),
            (Pub.ct_dispersion_filter_medium, pl.Boolean()),
            (Pub.ct_dispersion_filter_short, pl.Boolean()),
        ]
    )
    expected_schema = pl.Schema(
        [
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
            (Pub.unfiltered_filled_posts, pl.Float32()),
            (Pub.unfiltered_locationid_count, pl.UInt32()),
            (Pub.filtered_long_sfc_filled_posts, pl.Float32()),
            (Pub.filtered_long_sfc_locationid_count, pl.UInt32()),
            (Pub.filtered_long_ct_total_employed, pl.Float32()),
            (Pub.filtered_medium_sfc_filled_posts, pl.Float32()),
            (Pub.filtered_medium_sfc_locationid_count, pl.UInt32()),
            (Pub.filtered_medium_ct_total_employed, pl.Float32()),
            (Pub.filtered_short_sfc_filled_posts, pl.Float32()),
            (Pub.filtered_short_sfc_locationid_count, pl.UInt32()),
            (Pub.filtered_short_ct_total_employed, pl.Float32()),
            (Pub.unfiltered_long_ct_total_employed, pl.Float32()),
        ]
    )

    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.aggregate_to_publication_rows_test_cases
        ],
    )
    def test_returns_expected_data(self, case):
        input_lf = pl.LazyFrame(case.input_data, self.input_schema, orient="row")

        returned_lf = job.aggregate_to_publication_rows(input_lf)

        expected_lf = pl.LazyFrame(
            case.expected_data, self.expected_schema, orient="row"
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_can_aggregate_with_a_narrower_set_of_group_keys(self):
        input_lf = pl.LazyFrame(
            [
                (
                    "1-101",
                    date(2025, 4, 1),
                    "Registered nurse",
                    "London",
                    "Care home with nursing",
                    10.0,
                    5.0,
                    *[True] * 9,
                ),
                (
                    "1-101",
                    date(2025, 4, 1),
                    "Care worker",
                    "London",
                    "Care home with nursing",
                    20.0,
                    5.0,
                    *[True] * 9,
                ),
            ],
            self.input_schema,
            orient="row",
        )

        returned_lf = job.aggregate_to_publication_rows(
            input_lf,
            group_keys=[
                IndCQC.cqc_location_import_date,
                IndCQC.current_region,
                IndCQC.primary_service_type,
            ],
        )

        expected_schema = pl.Schema(
            [
                (IndCQC.cqc_location_import_date, pl.Date()),
                (IndCQC.current_region, pl.String()),
                (IndCQC.primary_service_type, pl.String()),
                (Pub.unfiltered_filled_posts, pl.Float32()),
                (Pub.unfiltered_locationid_count, pl.UInt32()),
                (Pub.filtered_long_sfc_filled_posts, pl.Float32()),
                (Pub.filtered_long_sfc_locationid_count, pl.UInt32()),
                (Pub.filtered_long_ct_total_employed, pl.Float32()),
                (Pub.filtered_medium_sfc_filled_posts, pl.Float32()),
                (Pub.filtered_medium_sfc_locationid_count, pl.UInt32()),
                (Pub.filtered_medium_ct_total_employed, pl.Float32()),
                (Pub.filtered_short_sfc_filled_posts, pl.Float32()),
                (Pub.filtered_short_sfc_locationid_count, pl.UInt32()),
                (Pub.filtered_short_ct_total_employed, pl.Float32()),
                (Pub.unfiltered_long_ct_total_employed, pl.Float32()),
            ]
        )
        expected_lf = pl.LazyFrame(
            [
                (
                    date(2025, 4, 1),
                    "London",
                    "Care home with nursing",
                    30.0,
                    1,
                    30.0,
                    1,
                    5.0,
                    30.0,
                    1,
                    5.0,
                    30.0,
                    1,
                    5.0,
                    5.0,
                ),
            ],
            expected_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_returns_only_publication_columns_when_assessment_is_excluded(self):
        input_lf = pl.LazyFrame(
            [
                (
                    "1-101",
                    date(2025, 4, 1),
                    "Registered nurse",
                    "London",
                    "Care home with nursing",
                    10.0,
                    5.0,
                    *[True] * 9,
                ),
                (
                    "1-102",
                    date(2025, 4, 1),
                    "Registered nurse",
                    "London",
                    "Care home with nursing",
                    20.0,
                    6.0,
                    *[True] * 9,
                ),
            ],
            self.input_schema,
            orient="row",
        )

        returned_lf = job.aggregate_to_publication_rows(
            input_lf, include_assessment=False
        )

        expected_schema = pl.Schema(
            [
                (IndCQC.cqc_location_import_date, pl.Date()),
                (IndCQC.main_job_role_clean_labelled, pl.String()),
                (IndCQC.current_region, pl.String()),
                (IndCQC.primary_service_type, pl.String()),
                (Pub.unfiltered_filled_posts, pl.Float32()),
                (Pub.unfiltered_locationid_count, pl.UInt32()),
            ]
        )
        expected_lf = pl.LazyFrame(
            [
                (
                    date(2025, 4, 1),
                    "Registered nurse",
                    "London",
                    "Care home with nursing",
                    30.0,
                    2,
                ),
            ],
            expected_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_excluding_assessment_needs_no_term_filter_columns(self):
        input_lf = pl.LazyFrame(
            [
                (
                    "1-101",
                    date(2025, 4, 1),
                    "Registered nurse",
                    "London",
                    "Care home with nursing",
                    10.0,
                )
            ],
            pl.Schema(list(self.input_schema.items())[:6]),
            orient="row",
        )

        returned_df = job.aggregate_to_publication_rows(
            input_lf, include_assessment=False
        ).collect()

        assert returned_df[Pub.unfiltered_filled_posts].to_list() == [10.0]


class TestAddRowsForPublicationGroups:
    # Real closed Enum in production - exercises the cast-to-Categorical.
    _primary_service_type_enum = pl.Enum(
        [
            PrimaryServiceType.care_home_with_nursing,
            PrimaryServiceType.care_home_only,
            PrimaryServiceType.non_residential,
        ]
    )
    input_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, _primary_service_type_enum),
            (
                IndCQC.estimate_filled_posts_by_job_role,
                pl.Float32(),
            ),
            (Pub.ct_total_employed_imputed, pl.Float32()),
            (Pub.consistent_service_long, pl.Boolean()),
            (Pub.consistent_service_medium, pl.Boolean()),
            (Pub.consistent_service_short, pl.Boolean()),
            (Pub.ct_has_data_long, pl.Boolean()),
            (Pub.ct_has_data_medium, pl.Boolean()),
            (Pub.ct_has_data_short, pl.Boolean()),
            (Pub.ct_dispersion_filter_long, pl.Boolean()),
            (Pub.ct_dispersion_filter_medium, pl.Boolean()),
            (Pub.ct_dispersion_filter_short, pl.Boolean()),
        ]
    )
    expected_schema = pl.Schema(
        [
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, pl.Categorical()),
            (Pub.unfiltered_filled_posts, pl.Float32()),
            (Pub.unfiltered_locationid_count, pl.UInt32()),
            (Pub.filtered_long_sfc_filled_posts, pl.Float32()),
            (Pub.filtered_long_sfc_locationid_count, pl.UInt32()),
            (Pub.filtered_long_ct_total_employed, pl.Float32()),
            (Pub.filtered_medium_sfc_filled_posts, pl.Float32()),
            (Pub.filtered_medium_sfc_locationid_count, pl.UInt32()),
            (Pub.filtered_medium_ct_total_employed, pl.Float32()),
            (Pub.filtered_short_sfc_filled_posts, pl.Float32()),
            (Pub.filtered_short_sfc_locationid_count, pl.UInt32()),
            (Pub.filtered_short_ct_total_employed, pl.Float32()),
            (Pub.unfiltered_long_ct_total_employed, pl.Float32()),
        ]
    )

    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.add_rows_for_publication_groups_test_cases
        ],
    )
    def test_adds_rollup_rows_for_job_role_service_type_and_region(self, case):
        input_lf = pl.LazyFrame(case.input_data, self.input_schema, orient="row")
        publication_summary_lf = job.aggregate_to_publication_rows(input_lf)

        returned_lf = job.add_rows_for_publication_groups(
            input_lf, publication_summary_lf
        )

        assert returned_lf.collect().height == case.expected_row_count

        expected_lf = pl.LazyFrame(
            case.expected_data, self.expected_schema, orient="row"
        )
        key_columns = [
            IndCQC.cqc_location_import_date,
            IndCQC.main_job_role_clean_labelled,
            IndCQC.current_region,
            IndCQC.primary_service_type,
        ]
        returned_lf = returned_lf.join(
            expected_lf.select(key_columns), on=key_columns, how="semi"
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    def test_adds_rollup_rows_without_assessment_columns(self):
        input_lf = pl.LazyFrame(
            [
                (
                    "1-101",
                    date(2025, 4, 1),
                    "Registered nurse",
                    "London",
                    "Care home with nursing",
                    10.0,
                    5.0,
                    *[True] * 9,
                ),
                (
                    "1-101",
                    date(2025, 4, 1),
                    "Care worker",
                    "London",
                    "Care home with nursing",
                    20.0,
                    5.0,
                    *[True] * 9,
                ),
            ],
            self.input_schema,
            orient="row",
        )
        publication_summary_lf = job.aggregate_to_publication_rows(
            input_lf, include_assessment=False
        )

        returned_lf = job.add_rows_for_publication_groups(
            input_lf, publication_summary_lf, include_assessment=False
        )

        assert returned_lf.collect_schema().names() == [
            IndCQC.cqc_location_import_date,
            IndCQC.main_job_role_clean_labelled,
            IndCQC.current_region,
            IndCQC.primary_service_type,
            Pub.unfiltered_filled_posts,
            Pub.unfiltered_locationid_count,
        ]
        england_total_row = returned_lf.filter(
            (pl.col(IndCQC.current_region) == PublishedRegion.england)
            & (
                pl.col(IndCQC.main_job_role_clean_labelled)
                == PublishedJobGroupLabels.all_job_roles
            )
            & (
                pl.col(IndCQC.primary_service_type).cast(pl.String)
                == PublishedMainService.all_locations
            )
        ).collect()
        assert england_total_row.select(
            Pub.unfiltered_filled_posts, Pub.unfiltered_locationid_count
        ).row(0) == (30.0, 1)


class TestAddPrevRunFilledPostsColumns:
    key_schema = pl.Schema(
        [
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
        ]
    )
    current_schema = pl.Schema(
        list(key_schema.items()) + [(Pub.unfiltered_filled_posts, pl.Float64())]
    )
    prev_schema = current_schema

    def _current_lf(self) -> pl.LazyFrame:
        return pl.LazyFrame(
            [
                (date(2026, 4, 1), "A", "North", "care", 110.0),
                (date(2026, 5, 1), "A", "North", "care", 50.0),
            ],
            self.current_schema,
            orient="row",
        )

    def _prev_lf(self, prev_posts: float = 100.0) -> pl.LazyFrame:
        return pl.LazyFrame(
            [(date(2026, 4, 1), "A", "North", "care", prev_posts)],
            self.prev_schema,
            orient="row",
        )

    def test_adds_prev_run_filled_posts_on_matching_keys_and_null_on_new_keys(self):
        returned_df = job.add_prev_run_filled_posts_columns(
            self._current_lf(), self._prev_lf()
        ).collect()

        assert returned_df[Pub.prev_run_filled_posts].to_list() == [100.0, None]

    def test_adds_perc_change_from_prev_run_as_net_change_fraction(self):
        returned_df = job.add_prev_run_filled_posts_columns(
            self._current_lf(), self._prev_lf()
        ).collect()

        assert returned_df[
            Pub.unfiltered_filled_posts_perc_change_vs_prev_run
        ].to_list() == [
            pytest.approx(0.1),
            None,
        ]
        assert (
            returned_df.schema[Pub.unfiltered_filled_posts_perc_change_vs_prev_run]
            == pl.Float32()
        )

    def test_perc_change_is_null_when_prev_run_filled_posts_is_zero(self):
        returned_df = job.add_prev_run_filled_posts_columns(
            self._current_lf(), self._prev_lf(prev_posts=0.0)
        ).collect()

        assert returned_df[
            Pub.unfiltered_filled_posts_perc_change_vs_prev_run
        ].to_list() == [
            None,
            None,
        ]

    def test_keeps_current_rows_and_columns_unchanged(self):
        returned_lf = job.add_prev_run_filled_posts_columns(
            self._current_lf(), self._prev_lf()
        )

        pl_testing.assert_frame_equal(
            returned_lf.select(self.current_schema.names()), self._current_lf()
        )

    @pytest.mark.parametrize(
        "current_dtype", [pl.Categorical, pl.Enum(["care", "other"])]
    )
    def test_aligns_prev_key_dtypes_to_current(self, current_dtype):
        current_lf = self._current_lf().with_columns(
            pl.col(IndCQC.primary_service_type).cast(current_dtype)
        )

        returned_df = job.add_prev_run_filled_posts_columns(
            current_lf, self._prev_lf()
        ).collect()

        assert returned_df[Pub.prev_run_filled_posts].to_list() == [100.0, None]

    def test_matches_rows_where_a_key_is_null(self):
        current_lf = self._current_lf().with_columns(
            pl.when(pl.col(IndCQC.cqc_location_import_date) == date(2026, 4, 1))
            .then(None)
            .otherwise(pl.col(IndCQC.current_region))
            .alias(IndCQC.current_region)
        )
        prev_lf = pl.LazyFrame(
            [(date(2026, 4, 1), "A", None, "care", 100.0)],
            self.prev_schema,
            orient="row",
        )

        returned_df = job.add_prev_run_filled_posts_columns(
            current_lf, prev_lf
        ).collect()

        assert returned_df[Pub.prev_run_filled_posts].to_list() == [100.0, None]

    def test_adds_null_columns_when_prev_run_lacks_filled_posts(self):
        prev_lf = pl.LazyFrame(
            [(date(2026, 4, 1), "A", "North", "care")],
            self.key_schema,
            orient="row",
        )

        returned_df = job.add_prev_run_filled_posts_columns(
            self._current_lf(), prev_lf
        ).collect()

        assert returned_df[Pub.prev_run_filled_posts].null_count() == 2
        assert (
            returned_df[
                Pub.unfiltered_filled_posts_perc_change_vs_prev_run
            ].null_count()
            == 2
        )

    def test_adds_null_columns_when_there_is_no_prev_run(self):
        returned_df = job.add_prev_run_filled_posts_columns(
            self._current_lf(), None
        ).collect()

        assert returned_df.schema[Pub.prev_run_filled_posts] == pl.Float64()
        assert (
            returned_df.schema[Pub.unfiltered_filled_posts_perc_change_vs_prev_run]
            == pl.Float32()
        )
        assert returned_df[Pub.prev_run_filled_posts].null_count() == 2
        assert (
            returned_df[
                Pub.unfiltered_filled_posts_perc_change_vs_prev_run
            ].null_count()
            == 2
        )


class TestCalcPercChangeFromPreviousApril:
    perc_change_schema = pl.Schema(
        [
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
            (Pub.unfiltered_filled_posts, pl.Float32()),
            (Pub.unfiltered_locationid_count, pl.UInt32()),
        ]
    )

    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.calc_perc_change_from_previous_april_test_cases
        ],
    )
    def test_returns_percentage_change_on_april_rows_against_the_previous_april(
        self, case
    ):
        expected_schema = pl.Schema(
            list(self.perc_change_schema.items()) + [(case.column_alias, pl.Float32())]
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        test_lf = expected_lf.drop(case.column_alias)

        returned_lf = test_lf.with_columns(
            job.calc_perc_change_from_previous_april(
                case.column_name, case.group_columns, case.column_alias
            )
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestCalcPercChangeCumulativeFromGivenPeriodOnwards:
    perc_change_schema = pl.Schema(
        [
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
            (Pub.filtered_long_ct_total_employed, pl.Float32()),
            (Pub.filtered_medium_ct_total_employed, pl.Float32()),
        ]
    )

    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.calc_perc_change_cumulative_from_given_period_onwards_test_cases
        ],
    )
    def test_returns_cumulative_percentage_change_from_the_first_period_in_the_window(
        self, case
    ):
        expected_schema = pl.Schema(
            list(self.perc_change_schema.items()) + [(case.column_alias, pl.Float32())]
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        test_lf = expected_lf.drop(case.column_alias)

        returned_lf = test_lf.with_columns(
            job.calc_perc_change_cumulative_from_given_period_onwards(
                case.column_name, case.from_date, case.group_columns, case.column_alias
            )
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestRoundToPublicationBands:
    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.round_to_publication_bands_test_cases],
    )
    def test_rounds_to_the_publication_controlled_rounding_bands(self, case):
        expected_schema = pl.Schema(
            [(case.column_name, pl.Float32()), (case.column_alias, pl.Float32())]
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        test_lf = expected_lf.drop(case.column_alias)

        returned_lf = test_lf.with_columns(
            job.round_to_publication_bands(case.column_name, case.column_alias)
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestRoundPercentage:
    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.round_percentage_test_cases],
    )
    def test_function_rounds_a_fraction_to_a_one_decimal_place_percentage(self, case):
        schema = pl.Schema([("fraction", pl.Float64()), ("percentage", pl.Float64())])
        expected_lf = pl.LazyFrame(case.expected_data, schema, orient="row")

        returned_lf = expected_lf.select("fraction").with_columns(
            job.round_percentage("fraction").alias("percentage")
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestPercentageAsRoundedFraction:
    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.percentage_as_rounded_fraction_test_cases
        ],
    )
    def test_function_rounds_a_fraction_to_one_decimal_place_of_a_percentage(
        self, case
    ):
        schema = pl.Schema([("fraction", pl.Float64()), ("rounded", pl.Float64())])
        expected_lf = pl.LazyFrame(case.expected_data, schema, orient="row")

        returned_lf = expected_lf.select("fraction").with_columns(
            job.percentage_as_rounded_fraction("fraction").alias("rounded")
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestRoundToWholeNumber:
    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.round_to_whole_number_test_cases],
    )
    def test_function_rounds_to_a_whole_number(self, case):
        schema = pl.Schema([("value", pl.Float64()), ("rounded", pl.Int64())])
        expected_lf = pl.LazyFrame(case.expected_data, schema, orient="row")

        returned_lf = expected_lf.select("value").with_columns(
            job.round_to_whole_number("value").alias("rounded")
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestImportDateToPeriodMonth:
    @pytest.mark.parametrize(
        "import_date, expected",
        [
            pytest.param(date(2026, 9, 1), date(2026, 8, 1), id="mid_year"),
            pytest.param(
                date(2026, 1, 1), date(2025, 12, 1), id="january_goes_to_prior_december"
            ),
        ],
    )
    def test_returns_month_before_import_date(self, import_date, expected):
        df = pl.DataFrame({"import_date": [import_date]})

        returned = df.select(job.import_date_to_period_month(pl.col("import_date")))

        assert returned.item() == expected


class TestPeriodLabelShortExpr:
    @pytest.mark.parametrize(
        "import_date, expected",
        [
            pytest.param(date(2026, 4, 1), "Mar 2026", id="financial_year_start"),
            pytest.param(date(2026, 1, 1), "Dec 2025", id="january_goes_to_prior_year"),
        ],
    )
    def test_returns_month_and_year_before_import_date(self, import_date, expected):
        df = pl.DataFrame({IndCQC.cqc_location_import_date: [import_date]})

        returned = df.select(job.period_label_short_expr())

        assert returned.columns == [Pub.period_label_short]
        assert returned.item() == expected


class TestPeriodLabelLongExpr:
    @pytest.mark.parametrize(
        "import_date, expected",
        [
            pytest.param(date(2026, 4, 1), "March 2026", id="financial_year_start"),
            pytest.param(
                date(2026, 1, 1), "December 2025", id="january_goes_to_prior_year"
            ),
            pytest.param(date(2026, 6, 1), "May 2026", id="short_name_equals_long"),
        ],
    )
    def test_returns_full_month_and_year_before_import_date(
        self, import_date, expected
    ):
        df = pl.DataFrame({IndCQC.cqc_location_import_date: [import_date]})

        returned = df.select(job.period_label_long_expr())

        assert returned.columns == [Pub.period_label_long]
        assert returned.item() == expected
