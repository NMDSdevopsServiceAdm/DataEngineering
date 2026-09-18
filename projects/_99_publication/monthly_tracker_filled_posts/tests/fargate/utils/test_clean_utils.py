from datetime import date

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.clean_utils as job
import projects._99_publication.unittest_data.polars_pub_test_data as Data
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import PrimaryServiceType


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
                IndCQC.estimate_filled_posts_by_job_role_historically_reallocated,
                pl.Float32(),
            ),
            (Pub.ct_total_employed_imputed, pl.Float32()),
            (Pub.consistent_service, pl.Boolean()),
            (Pub.ct_has_data_long_term, pl.Boolean()),
            (Pub.ct_has_data_medium_term, pl.Boolean()),
            (Pub.ct_has_data_short_term, pl.Boolean()),
            (Pub.ct_dispersion_filter_long_term, pl.Boolean()),
            (Pub.ct_dispersion_filter_medium_term, pl.Boolean()),
            (Pub.ct_dispersion_filter_short_term, pl.Boolean()),
        ]
    )
    expected_schema = pl.Schema(
        [
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
            (Pub.publication_filled_posts, pl.Float32()),
            (Pub.publication_locationid_count, pl.UInt32()),
            (Pub.assessment_filled_posts_long_term, pl.Float32()),
            (Pub.assessment_locationid_count_long_term, pl.UInt32()),
            (Pub.assessment_ct_total_employed_long_term, pl.Float32()),
            (Pub.assessment_filled_posts_medium_term, pl.Float32()),
            (Pub.assessment_locationid_count_medium_term, pl.UInt32()),
            (Pub.assessment_ct_total_employed_medium_term, pl.Float32()),
            (Pub.assessment_filled_posts_short_term, pl.Float32()),
            (Pub.assessment_locationid_count_short_term, pl.UInt32()),
            (Pub.assessment_ct_total_employed_short_term, pl.Float32()),
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
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                ),
                (
                    "1-101",
                    date(2025, 4, 1),
                    "Care worker",
                    "London",
                    "Care home with nursing",
                    20.0,
                    8.0,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
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
                (Pub.publication_filled_posts, pl.Float32()),
                (Pub.publication_locationid_count, pl.UInt32()),
                (Pub.assessment_filled_posts_long_term, pl.Float32()),
                (Pub.assessment_locationid_count_long_term, pl.UInt32()),
                (Pub.assessment_ct_total_employed_long_term, pl.Float32()),
                (Pub.assessment_filled_posts_medium_term, pl.Float32()),
                (Pub.assessment_locationid_count_medium_term, pl.UInt32()),
                (Pub.assessment_ct_total_employed_medium_term, pl.Float32()),
                (Pub.assessment_filled_posts_short_term, pl.Float32()),
                (Pub.assessment_locationid_count_short_term, pl.UInt32()),
                (Pub.assessment_ct_total_employed_short_term, pl.Float32()),
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
                    13.0,
                    30.0,
                    1,
                    13.0,
                    30.0,
                    1,
                    13.0,
                ),
            ],
            expected_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestAddRowsForPublicationGroups:
    # primary_service_type is a closed Enum in production (unlike the other
    # dimension columns, which are open Categorical) - using the real Enum
    # here, rather than String, exercises the cast-to-Categorical handling
    # this function relies on to be able to write the new rollup labels into
    # it at all.
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
                IndCQC.estimate_filled_posts_by_job_role_historically_reallocated,
                pl.Float32(),
            ),
            (Pub.ct_total_employed_imputed, pl.Float32()),
            (Pub.consistent_service, pl.Boolean()),
            (Pub.ct_has_data_long_term, pl.Boolean()),
            (Pub.ct_has_data_medium_term, pl.Boolean()),
            (Pub.ct_has_data_short_term, pl.Boolean()),
            (Pub.ct_dispersion_filter_long_term, pl.Boolean()),
            (Pub.ct_dispersion_filter_medium_term, pl.Boolean()),
            (Pub.ct_dispersion_filter_short_term, pl.Boolean()),
        ]
    )
    expected_schema = pl.Schema(
        [
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, pl.Categorical()),
            (Pub.publication_filled_posts, pl.Float32()),
            (Pub.publication_locationid_count, pl.UInt32()),
            (Pub.assessment_filled_posts_long_term, pl.Float32()),
            (Pub.assessment_locationid_count_long_term, pl.UInt32()),
            (Pub.assessment_ct_total_employed_long_term, pl.Float32()),
            (Pub.assessment_filled_posts_medium_term, pl.Float32()),
            (Pub.assessment_locationid_count_medium_term, pl.UInt32()),
            (Pub.assessment_ct_total_employed_medium_term, pl.Float32()),
            (Pub.assessment_filled_posts_short_term, pl.Float32()),
            (Pub.assessment_locationid_count_short_term, pl.UInt32()),
            (Pub.assessment_ct_total_employed_short_term, pl.Float32()),
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

        expected_lf = pl.LazyFrame(
            case.expected_data, self.expected_schema, orient="row"
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)


class TestCalcPercChangeBetweenRows:
    perc_change_schema = pl.Schema(
        [
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.main_job_role_clean_labelled, pl.String()),
            (IndCQC.current_region, pl.String()),
            (IndCQC.primary_service_type, pl.String()),
            (Pub.assessment_ct_total_employed_long_term, pl.Float32()),
            (Pub.assessment_ct_total_employed_medium_term, pl.Float32()),
        ]
    )

    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.calc_perc_change_between_rows_test_cases
        ],
    )
    def test_returns_percentage_change_against_the_previous_period(self, case):
        expected_schema = pl.Schema(
            list(self.perc_change_schema.items()) + [(case.column_alias, pl.Float32())]
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        test_lf = expected_lf.drop(case.column_alias)

        returned_lf = test_lf.with_columns(
            job.calc_perc_change_between_rows(
                case.column_name, case.from_date, case.group_columns, case.column_alias
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
            (Pub.assessment_ct_total_employed_long_term, pl.Float32()),
            (Pub.assessment_ct_total_employed_medium_term, pl.Float32()),
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
