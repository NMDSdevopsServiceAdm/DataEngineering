from datetime import date
from unittest.mock import Mock, call, patch

import polars as pl
from polars.testing import assert_frame_equal

import projects._99_publication.monthly_tracker_filled_posts.fargate._02_clean as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub

PATCH_PATH = "projects._99_publication.monthly_tracker_filled_posts.fargate._02_clean"

TEST_SOURCE = "some/directory"
TEST_DESTINATION = "some/other/directory"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.clean_utils.format_large_number")
    @patch(
        f"{PATCH_PATH}.clean_utils.calc_perc_change_cumulative_from_given_period_onwards"
    )
    @patch(f"{PATCH_PATH}.clean_utils.calc_perc_change_between_rows")
    @patch(f"{PATCH_PATH}.clean_utils.add_rows_for_publication_groups")
    @patch(f"{PATCH_PATH}.clean_utils.aggregate_to_publication_rows")
    @patch(f"{PATCH_PATH}.clean_utils.add_dispersion_filter")
    @patch(f"{PATCH_PATH}.clean_utils.has_continuous_data_since_date")
    @patch(f"{PATCH_PATH}.reduced_data_filter_expr")
    @patch(f"{PATCH_PATH}.date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        date_mock: Mock,
        reduced_data_filter_expr_mock: Mock,
        has_continuous_data_since_date_mock: Mock,
        add_dispersion_filter_mock: Mock,
        aggregate_to_publication_rows_mock: Mock,
        add_rows_for_publication_groups_mock: Mock,
        calc_perc_change_between_rows_mock: Mock,
        calc_perc_change_cumulative_from_given_period_onwards_mock: Mock,
        format_large_number_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        scan_parquet_mock.return_value = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001", "1-001"],
                IndCQC.cqc_location_import_date: [date(2025, 4, 1), date(2026, 4, 1)],
                IndCQC.care_home_status_count: [1, 2],
                IndCQC.ct_care_home_total_employed_imputed: [5.0, None],
                IndCQC.ct_non_res_care_workers_employed_imputed: [None, 6.0],
            }
        )
        date_mock.today.return_value = date(2026, 9, 1)
        date_mock.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        reduced_data_filter_expr_mock.return_value = pl.lit(True)
        has_continuous_data_since_date_mock.side_effect = (
            lambda column_name, from_date, column_alias: pl.lit(True).alias(
                column_alias
            )
        )
        add_dispersion_filter_mock.side_effect = (
            lambda lazy_df, column_names, from_date, column_alias: (
                lazy_df.with_columns(pl.lit(True).alias(column_alias))
            )
        )
        aggregate_to_publication_rows_mock.return_value = pl.LazyFrame(
            {
                Pub.publication_filled_posts: [42.0],
                IndCQC.cqc_location_import_date: [date(2026, 4, 1)],
            }
        )
        add_rows_for_publication_groups_mock.return_value = (
            aggregate_to_publication_rows_mock.return_value
        )
        calc_perc_change_between_rows_mock.side_effect = (
            lambda column_name, from_date, group_columns, column_alias: pl.lit(
                None, dtype=pl.Float32
            ).alias(column_alias)
        )
        calc_perc_change_cumulative_from_given_period_onwards_mock.side_effect = (
            lambda column_name, from_date, group_columns, column_alias: pl.lit(
                None, dtype=pl.Float32
            ).alias(column_alias)
        )
        format_large_number_mock.side_effect = lambda column_name, column_alias: pl.lit(
            "42"
        ).alias(column_alias)

        job.main(TEST_SOURCE, TEST_DESTINATION)

        scan_parquet_mock.assert_called_once_with(TEST_SOURCE)

        reduced_data_filter_expr_mock.assert_called_once_with(
            cutoff_date=date(2020, 4, 1),
        )

        has_continuous_data_since_date_mock.assert_has_calls(
            [
                call(
                    Pub.ct_total_employed_imputed,
                    date(2021, 7, 1),
                    Pub.ct_has_data_long_term,
                ),
                call(
                    Pub.ct_total_employed_imputed,
                    date(2025, 4, 1),
                    Pub.ct_has_data_medium_term,
                ),
                call(
                    Pub.ct_total_employed_imputed,
                    date(2026, 4, 1),
                    Pub.ct_has_data_short_term,
                ),
            ]
        )

        ct_employed_columns = [
            IndCQC.ct_care_home_total_employed_imputed,
            IndCQC.ct_non_res_care_workers_employed_imputed,
        ]
        assert add_dispersion_filter_mock.call_count == 3
        for expected_call, mock_call in zip(
            [
                (
                    ct_employed_columns,
                    date(2021, 7, 1),
                    Pub.ct_dispersion_filter_long_term,
                ),
                (
                    ct_employed_columns,
                    date(2025, 4, 1),
                    Pub.ct_dispersion_filter_medium_term,
                ),
                (
                    ct_employed_columns,
                    date(2026, 4, 1),
                    Pub.ct_dispersion_filter_short_term,
                ),
            ],
            add_dispersion_filter_mock.call_args_list,
        ):
            assert mock_call.args[1:] == expected_call

        cleaned_lf_expected = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001", "1-001"],
                IndCQC.cqc_location_import_date: [date(2025, 4, 1), date(2026, 4, 1)],
                IndCQC.care_home_status_count: [1, 2],
                IndCQC.ct_care_home_total_employed_imputed: [5.0, None],
                IndCQC.ct_non_res_care_workers_employed_imputed: [None, 6.0],
                Pub.consistent_service: [True, False],
                Pub.ct_total_employed_imputed: [5.0, 6.0],
                Pub.ct_has_data_long_term: [True, True],
                Pub.ct_has_data_medium_term: [True, True],
                Pub.ct_has_data_short_term: [True, True],
                Pub.ct_dispersion_filter_long_term: [True, True],
                Pub.ct_dispersion_filter_medium_term: [True, True],
                Pub.ct_dispersion_filter_short_term: [True, True],
            }
        )
        aggregate_to_publication_rows_mock.assert_called_once()
        assert_frame_equal(
            aggregate_to_publication_rows_mock.call_args.args[0],
            cleaned_lf_expected,
            check_column_order=False,
        )

        add_rows_for_publication_groups_mock.assert_called_once()
        assert_frame_equal(
            add_rows_for_publication_groups_mock.call_args.args[0],
            cleaned_lf_expected,
            check_column_order=False,
        )
        assert (
            add_rows_for_publication_groups_mock.call_args.args[1]
            is aggregate_to_publication_rows_mock.return_value
        )

        group_columns = [
            IndCQC.main_job_role_clean_labelled,
            IndCQC.current_region,
            IndCQC.primary_service_type,
        ]
        calc_perc_change_between_rows_mock.assert_has_calls(
            [
                call(
                    Pub.assessment_ct_total_employed_long_term,
                    date(2021, 7, 1),
                    group_columns,
                    Pub.assessment_ct_period_perc_change_long_term,
                ),
                call(
                    Pub.assessment_ct_total_employed_medium_term,
                    date(2025, 4, 1),
                    group_columns,
                    Pub.assessment_ct_period_perc_change_medium_term,
                ),
                call(
                    Pub.assessment_ct_total_employed_short_term,
                    date(2026, 4, 1),
                    group_columns,
                    Pub.assessment_ct_period_perc_change_short_term,
                ),
            ]
        )
        calc_perc_change_cumulative_from_given_period_onwards_mock.assert_has_calls(
            [
                call(
                    Pub.assessment_ct_total_employed_long_term,
                    date(2021, 7, 1),
                    group_columns,
                    Pub.assessment_ct_cumulative_perc_change_long_term,
                ),
                call(
                    Pub.assessment_ct_total_employed_medium_term,
                    date(2025, 4, 1),
                    group_columns,
                    Pub.assessment_ct_cumulative_perc_change_medium_term,
                ),
                call(
                    Pub.assessment_ct_total_employed_short_term,
                    date(2026, 4, 1),
                    group_columns,
                    Pub.assessment_ct_cumulative_perc_change_short_term,
                ),
            ]
        )

        format_large_number_mock.assert_has_calls(
            [
                call(
                    Pub.publication_filled_posts,
                    Pub.publication_filled_posts_formatted,
                ),
                call(
                    Pub.assessment_filled_posts_long_term,
                    Pub.assessment_filled_posts_long_term_formatted,
                ),
                call(
                    Pub.assessment_filled_posts_medium_term,
                    Pub.assessment_filled_posts_medium_term_formatted,
                ),
                call(
                    Pub.assessment_filled_posts_short_term,
                    Pub.assessment_filled_posts_short_term_formatted,
                ),
            ]
        )

        sink_to_parquet_mock.assert_called_once()
        sink_call_kwargs = sink_to_parquet_mock.call_args.kwargs
        assert sink_call_kwargs["output_path"] == TEST_DESTINATION
        expected_sink_lf = aggregate_to_publication_rows_mock.return_value.with_columns(
            pl.lit(None, dtype=pl.Float32).alias(
                Pub.assessment_ct_period_perc_change_long_term
            ),
            pl.lit(None, dtype=pl.Float32).alias(
                Pub.assessment_ct_period_perc_change_medium_term
            ),
            pl.lit(None, dtype=pl.Float32).alias(
                Pub.assessment_ct_period_perc_change_short_term
            ),
            pl.lit(None, dtype=pl.Float32).alias(
                Pub.assessment_ct_cumulative_perc_change_long_term
            ),
            pl.lit(None, dtype=pl.Float32).alias(
                Pub.assessment_ct_cumulative_perc_change_medium_term
            ),
            pl.lit(None, dtype=pl.Float32).alias(
                Pub.assessment_ct_cumulative_perc_change_short_term
            ),
            pl.lit("42").alias(Pub.publication_filled_posts_formatted),
            pl.lit("42").alias(Pub.assessment_filled_posts_long_term_formatted),
            pl.lit("42").alias(Pub.assessment_filled_posts_medium_term_formatted),
            pl.lit("42").alias(Pub.assessment_filled_posts_short_term_formatted),
            pl.lit("Apr 2026").alias(Pub.cqc_location_import_date_abbreviated),
            pl.lit("April 2026").alias(Pub.cqc_location_import_date_full),
        )
        assert_frame_equal(sink_call_kwargs["lazy_df"], expected_sink_lf)

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.clean_utils.format_large_number")
    @patch(
        f"{PATCH_PATH}.clean_utils.calc_perc_change_cumulative_from_given_period_onwards"
    )
    @patch(f"{PATCH_PATH}.clean_utils.calc_perc_change_between_rows")
    @patch(f"{PATCH_PATH}.clean_utils.add_rows_for_publication_groups")
    @patch(f"{PATCH_PATH}.clean_utils.aggregate_to_publication_rows")
    @patch(f"{PATCH_PATH}.clean_utils.add_dispersion_filter")
    @patch(f"{PATCH_PATH}.clean_utils.has_continuous_data_since_date")
    @patch(f"{PATCH_PATH}.reduced_data_filter_expr")
    @patch(f"{PATCH_PATH}.date")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_uses_retention_cutoff_date_for_long_term_once_it_is_later_than_the_earliest_ct_data_date(
        self,
        scan_parquet_mock: Mock,
        date_mock: Mock,
        reduced_data_filter_expr_mock: Mock,
        has_continuous_data_since_date_mock: Mock,
        add_dispersion_filter_mock: Mock,
        aggregate_to_publication_rows_mock: Mock,
        add_rows_for_publication_groups_mock: Mock,
        calc_perc_change_between_rows_mock: Mock,
        calc_perc_change_cumulative_from_given_period_onwards_mock: Mock,
        format_large_number_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        scan_parquet_mock.return_value = pl.LazyFrame(
            {
                IndCQC.care_home_status_count: [1],
                IndCQC.ct_care_home_total_employed_imputed: [5.0],
                IndCQC.ct_non_res_care_workers_employed_imputed: [None],
            }
        )
        date_mock.today.return_value = date(2033, 9, 1)
        date_mock.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        reduced_data_filter_expr_mock.return_value = pl.lit(True)
        has_continuous_data_since_date_mock.side_effect = (
            lambda column_name, from_date, column_alias: pl.lit(True).alias(
                column_alias
            )
        )
        add_dispersion_filter_mock.side_effect = (
            lambda lazy_df, column_names, from_date, column_alias: (
                lazy_df.with_columns(pl.lit(True).alias(column_alias))
            )
        )
        aggregate_to_publication_rows_mock.return_value = pl.LazyFrame(
            {
                Pub.publication_filled_posts: [42.0],
                IndCQC.cqc_location_import_date: [date(2033, 4, 1)],
            }
        )
        add_rows_for_publication_groups_mock.return_value = (
            aggregate_to_publication_rows_mock.return_value
        )
        calc_perc_change_between_rows_mock.side_effect = (
            lambda column_name, from_date, group_columns, column_alias: pl.lit(
                None, dtype=pl.Float32
            ).alias(column_alias)
        )
        calc_perc_change_cumulative_from_given_period_onwards_mock.side_effect = (
            lambda column_name, from_date, group_columns, column_alias: pl.lit(
                None, dtype=pl.Float32
            ).alias(column_alias)
        )
        format_large_number_mock.side_effect = lambda column_name, column_alias: pl.lit(
            "42"
        ).alias(column_alias)

        job.main(TEST_SOURCE, TEST_DESTINATION)

        reduced_data_filter_expr_mock.assert_called_once_with(
            cutoff_date=date(2027, 4, 1),
        )
        has_continuous_data_since_date_mock.assert_any_call(
            Pub.ct_total_employed_imputed,
            date(2027, 4, 1),
            Pub.ct_has_data_long_term,
        )
