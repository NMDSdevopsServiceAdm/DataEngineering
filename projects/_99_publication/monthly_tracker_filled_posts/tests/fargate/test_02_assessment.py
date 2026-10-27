from contextlib import ExitStack
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import projects._99_publication.monthly_tracker_filled_posts.fargate._02_assessment as job
from utils.column_names.ind_cqc_pipeline_columns import (
    ArchiveDateRunNumberPartitionKeys as ArchiveKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub

PATCH_PATH = (
    "projects._99_publication.monthly_tracker_filled_posts.fargate._02_assessment"
)

TEST_SOURCE = "some/prep/directory/"
TEST_RUN_SOURCE = "some/prep/directory/archive_date=2026-10-01/run_number=2/"
TEST_DESTINATION = "some/other/directory"
TEST_RUN_LOG_SOURCE = "some/run/log/directory"
TEST_PREVIOUS_RUN_SOURCE = "some/other/directory/archive_date=2026-09-01/run_number=1/"

# Term start dates for a run on 2026-09-01 whose earliest import date is 2021-04-01.
LONG_TERM_FROM_DATE = date(2021, 7, 1)
MEDIUM_TERM_FROM_DATE = date(2025, 4, 1)
SHORT_TERM_FROM_DATE = date(2026, 4, 1)
# Earliest import date that is later than the earliest Capacity Tracker data.
LATER_FIRST_IMPORT_DATE = date(2027, 4, 1)

PERC_CHANGE_COLUMNS = [
    Pub.filled_posts_april_perc_change,
    Pub.locationid_count_april_perc_change,
    Pub.filled_posts_cum_perc_change_short,
    Pub.locationid_count_cum_perc_change_short,
    Pub.filtered_filled_posts_cum_perc_change_long,
    Pub.filtered_ct_cum_perc_change_long,
    Pub.filtered_filled_posts_cum_perc_change_medium,
    Pub.filtered_ct_cum_perc_change_medium,
    Pub.filtered_filled_posts_cum_perc_change_short,
    Pub.filtered_ct_cum_perc_change_short,
    Pub.filled_posts_cum_perc_change_long,
    Pub.unfiltered_ct_total_employed_cum_perc_change_long,
]


def _patch_publication_utils(stack: ExitStack, name: str, **kwargs) -> Mock:
    return stack.enter_context(
        patch(f"{PATCH_PATH}.publication_utils.{name}", **kwargs)
    )


@pytest.fixture
def mocks():
    """
    Replaces the job's inputs, outputs and publication_utils functions with stand-ins
    that add constant columns, so each test can assert on how main calls them.
    """
    null_perc_change = lambda column_name, from_date, group_columns, column_alias: (
        pl.lit(None, dtype=pl.Float32).alias(column_alias)
    )
    null_april_perc_change = lambda column_name, group_columns, column_alias: (
        pl.lit(None, dtype=pl.Float32).alias(column_alias)
    )
    with ExitStack() as stack:
        mocks = SimpleNamespace(
            scan_parquet=stack.enter_context(patch(f"{PATCH_PATH}.utils.scan_parquet")),
            sink_to_parquet=stack.enter_context(
                patch(f"{PATCH_PATH}.utils.sink_to_parquet")
            ),
            resolve_run_sources=stack.enter_context(
                patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
            ),
            get_prev_published_run=stack.enter_context(
                patch(f"{PATCH_PATH}.run_log_utils.get_previous_published_run_number")
            ),
            date=stack.enter_context(patch(f"{PATCH_PATH}.date")),
            add_previous_columns=_patch_publication_utils(
                stack,
                "add_prev_run_filled_posts_columns",
                side_effect=lambda lazy_df, previous_lf: lazy_df,
            ),
            has_consistent_service=_patch_publication_utils(
                stack,
                "has_consistent_service_since_date",
                side_effect=lambda from_date, column_alias: pl.lit(True).alias(
                    column_alias
                ),
            ),
            has_continuous_data=_patch_publication_utils(
                stack,
                "has_continuous_data_since_date",
                side_effect=lambda column_name, from_date, column_alias: pl.lit(
                    True
                ).alias(column_alias),
            ),
            add_dispersion_filter=_patch_publication_utils(
                stack,
                "add_dispersion_filter",
                side_effect=lambda lazy_df, column_names, from_date, column_alias: (
                    lazy_df.with_columns(pl.lit(True).alias(column_alias))
                ),
            ),
            aggregate=_patch_publication_utils(stack, "aggregate_to_publication_rows"),
            add_rows=_patch_publication_utils(stack, "add_rows_for_publication_groups"),
            perc_change_april=_patch_publication_utils(
                stack,
                "calc_perc_change_from_previous_april",
                side_effect=null_april_perc_change,
            ),
            perc_change_cumulative=_patch_publication_utils(
                stack,
                "calc_perc_change_cumulative_from_given_period_onwards",
                side_effect=null_perc_change,
            ),
        )
        mocks.resolve_run_sources.return_value = [TEST_RUN_SOURCE]
        mocks.get_prev_published_run.return_value = None
        mocks.scan_parquet.return_value = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001", "1-001"],
                IndCQC.cqc_location_import_date: [date(2021, 4, 1), date(2026, 4, 1)],
                IndCQC.ct_care_home_total_employed_imputed: [5.0, None],
                IndCQC.ct_non_res_care_workers_employed_imputed: [None, 6.0],
            }
        )
        mocks.date.today.return_value = date(2026, 9, 1)
        mocks.date.side_effect = lambda *args, **kwargs: date(*args, **kwargs)
        mocks.aggregate.return_value = pl.LazyFrame(
            {
                IndCQC.cqc_location_import_date: [date(2026, 4, 1)],
                Pub.filled_posts: [42.0],
                Pub.filtered_filled_posts_long: [40.0],
            }
        )
        mocks.add_rows.return_value = mocks.aggregate.return_value
        yield mocks


class TestMain:
    def test_main_reads_the_requested_run_from_the_prep_store(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE, run_number=3)

        mocks.resolve_run_sources.assert_called_once_with([TEST_SOURCE], 3)
        mocks.scan_parquet.assert_called_once_with(TEST_RUN_SOURCE)

    def test_main_reads_the_latest_run_when_no_run_number_given(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE)

        mocks.resolve_run_sources.assert_called_once_with([TEST_SOURCE], None)

    def test_main_adds_consistent_service_for_each_term_and_ct_total_employed(
        self, mocks
    ):
        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE)

        mocks.has_consistent_service.assert_has_calls(
            [
                call(LONG_TERM_FROM_DATE, Pub.consistent_service_long),
                call(MEDIUM_TERM_FROM_DATE, Pub.consistent_service_medium),
                call(SHORT_TERM_FROM_DATE, Pub.consistent_service_short),
            ]
        )
        prepared_lf = mocks.aggregate.call_args.args[0]
        assert_frame_equal(
            prepared_lf.select(Pub.ct_total_employed_imputed),
            pl.LazyFrame({Pub.ct_total_employed_imputed: [5.0, 6.0]}),
        )

    def test_main_adds_ct_has_data_and_dispersion_filters_for_each_term(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE)

        mocks.has_continuous_data.assert_has_calls(
            [
                call(
                    Pub.ct_total_employed_imputed,
                    LONG_TERM_FROM_DATE,
                    Pub.ct_has_data_long,
                ),
                call(
                    Pub.ct_total_employed_imputed,
                    MEDIUM_TERM_FROM_DATE,
                    Pub.ct_has_data_medium,
                ),
                call(
                    Pub.ct_total_employed_imputed,
                    SHORT_TERM_FROM_DATE,
                    Pub.ct_has_data_short,
                ),
            ]
        )
        ct_employed_columns = [
            IndCQC.ct_care_home_total_employed_imputed,
            IndCQC.ct_non_res_care_workers_employed_imputed,
        ]
        assert mocks.add_dispersion_filter.call_count == 3
        assert [c.args[1:] for c in mocks.add_dispersion_filter.call_args_list] == [
            (ct_employed_columns, LONG_TERM_FROM_DATE, Pub.ct_dispersion_filter_long),
            (
                ct_employed_columns,
                MEDIUM_TERM_FROM_DATE,
                Pub.ct_dispersion_filter_medium,
            ),
            (
                ct_employed_columns,
                SHORT_TERM_FROM_DATE,
                Pub.ct_dispersion_filter_short,
            ),
        ]

    def test_main_uses_earliest_prepared_date_for_long_term_once_it_is_later_than_the_earliest_ct_data_date(
        self, mocks
    ):
        mocks.scan_parquet.return_value = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001", "1-001"],
                IndCQC.cqc_location_import_date: [
                    LATER_FIRST_IMPORT_DATE,
                    date(2027, 5, 1),
                ],
                IndCQC.ct_care_home_total_employed_imputed: [5.0, None],
                IndCQC.ct_non_res_care_workers_employed_imputed: [None, 6.0],
            }
        )

        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE)

        mocks.has_continuous_data.assert_any_call(
            Pub.ct_total_employed_imputed,
            LATER_FIRST_IMPORT_DATE,
            Pub.ct_has_data_long,
        )

    def test_main_adds_perc_change_columns_for_each_term(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE)

        group_columns = [
            IndCQC.main_job_role_clean_labelled,
            IndCQC.current_region,
            IndCQC.primary_service_type,
        ]
        mocks.perc_change_april.assert_has_calls(
            [
                call(
                    Pub.filled_posts,
                    group_columns,
                    Pub.filled_posts_april_perc_change,
                ),
                call(
                    Pub.locationid_count,
                    group_columns,
                    Pub.locationid_count_april_perc_change,
                ),
            ]
        )
        mocks.perc_change_cumulative.assert_has_calls(
            [
                call(
                    Pub.filled_posts,
                    SHORT_TERM_FROM_DATE,
                    group_columns,
                    Pub.filled_posts_cum_perc_change_short,
                ),
                call(
                    Pub.locationid_count,
                    SHORT_TERM_FROM_DATE,
                    group_columns,
                    Pub.locationid_count_cum_perc_change_short,
                ),
                call(
                    Pub.filtered_filled_posts_long,
                    LONG_TERM_FROM_DATE,
                    group_columns,
                    Pub.filtered_filled_posts_cum_perc_change_long,
                ),
                call(
                    Pub.filtered_ct_total_employed_long,
                    LONG_TERM_FROM_DATE,
                    group_columns,
                    Pub.filtered_ct_cum_perc_change_long,
                ),
                call(
                    Pub.filtered_filled_posts_medium,
                    MEDIUM_TERM_FROM_DATE,
                    group_columns,
                    Pub.filtered_filled_posts_cum_perc_change_medium,
                ),
                call(
                    Pub.filtered_ct_total_employed_medium,
                    MEDIUM_TERM_FROM_DATE,
                    group_columns,
                    Pub.filtered_ct_cum_perc_change_medium,
                ),
                call(
                    Pub.filtered_filled_posts_short,
                    SHORT_TERM_FROM_DATE,
                    group_columns,
                    Pub.filtered_filled_posts_cum_perc_change_short,
                ),
                call(
                    Pub.filtered_ct_total_employed_short,
                    SHORT_TERM_FROM_DATE,
                    group_columns,
                    Pub.filtered_ct_cum_perc_change_short,
                ),
                call(
                    Pub.filled_posts,
                    LONG_TERM_FROM_DATE,
                    group_columns,
                    Pub.filled_posts_cum_perc_change_long,
                ),
                call(
                    Pub.unfiltered_ct_total_employed_long,
                    LONG_TERM_FROM_DATE,
                    group_columns,
                    Pub.unfiltered_ct_total_employed_cum_perc_change_long,
                ),
            ]
        )

    def test_main_sinks_one_dataset_with_publication_and_assessment_columns(
        self, mocks
    ):
        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE)

        mocks.aggregate.assert_called_once()
        assert "include_assessment" not in mocks.aggregate.call_args.kwargs
        mocks.sink_to_parquet.assert_called_once()
        sink_kwargs = mocks.sink_to_parquet.call_args.kwargs
        assert sink_kwargs["output_path"] == TEST_DESTINATION
        assert sink_kwargs["partition_cols"] == [
            ArchiveKeys.archive_date,
            ArchiveKeys.run_number,
        ]
        expected_lf = (
            mocks.aggregate.return_value.with_columns(
                [pl.lit(None, dtype=pl.Float32).alias(c) for c in PERC_CHANGE_COLUMNS]
            )
            .with_columns(
                pl.lit("Mar 2026").alias(Pub.period_label_short),
                pl.lit("March 2026").alias(Pub.period_label_long),
            )
            .with_columns(
                pl.lit("2026-10-01").alias(ArchiveKeys.archive_date),
                pl.lit(2).alias(ArchiveKeys.run_number),
            )
        )
        assert_frame_equal(sink_kwargs["lazy_df"], expected_lf)

    def test_main_adds_short_and_long_period_labels_from_the_import_date(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE)

        sunk_df = mocks.sink_to_parquet.call_args.kwargs["lazy_df"].collect()
        assert sunk_df[Pub.period_label_short].to_list() == ["Mar 2026"]
        assert sunk_df[Pub.period_label_long].to_list() == ["March 2026"]

    def test_main_adds_prev_run_filled_posts_columns_from_the_previous_published_run(
        self, mocks
    ):
        mocks.get_prev_published_run.return_value = 1
        mocks.resolve_run_sources.side_effect = lambda roots, run_number: [
            TEST_PREVIOUS_RUN_SOURCE if roots == [TEST_DESTINATION] else TEST_RUN_SOURCE
        ]
        previous_lf = pl.LazyFrame({"previous": [1]})
        prepared_lf = mocks.scan_parquet.return_value
        mocks.scan_parquet.side_effect = lambda path: {
            TEST_RUN_SOURCE: prepared_lf,
            TEST_PREVIOUS_RUN_SOURCE: previous_lf,
        }[path]

        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE)

        mocks.get_prev_published_run.assert_called_once_with(TEST_RUN_LOG_SOURCE, 2)
        mocks.resolve_run_sources.assert_any_call([TEST_DESTINATION], 1)
        mocks.scan_parquet.assert_any_call(TEST_PREVIOUS_RUN_SOURCE)
        assert mocks.add_previous_columns.call_args.args[1] is previous_lf

    def test_main_passes_no_previous_run_when_none_is_published(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE)

        mocks.scan_parquet.assert_called_once_with(TEST_RUN_SOURCE)
        assert mocks.add_previous_columns.call_args.args[1] is None

    def test_main_adds_the_runs_archive_date_and_number_as_columns(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION, TEST_RUN_LOG_SOURCE)

        sunk_df = mocks.sink_to_parquet.call_args.kwargs["lazy_df"].collect()
        assert sunk_df[ArchiveKeys.archive_date].to_list() == ["2026-10-01"]
        assert sunk_df[ArchiveKeys.run_number].to_list() == [2]
