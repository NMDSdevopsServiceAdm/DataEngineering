from contextlib import ExitStack
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import projects._99_publication.monthly_tracker_filled_posts.fargate._02_assessment as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub

PATCH_PATH = (
    "projects._99_publication.monthly_tracker_filled_posts.fargate._02_assessment"
)

TEST_SOURCE = "some/prep/directory/"
TEST_RUN_SOURCE = "some/prep/directory/archive_date=2026-10-01/run_number=2/"
TEST_DESTINATION = "some/other/directory"

PERC_CHANGE_COLUMNS = [
    Pub.publication_filled_posts_april_perc_change,
    Pub.publication_locationid_count_april_perc_change,
    Pub.publication_filled_posts_cumulative_perc_change_short_term,
    Pub.publication_locationid_count_cumulative_perc_change_short_term,
    Pub.assessment_ct_cumulative_perc_change_long_term,
    Pub.assessment_ct_cumulative_perc_change_medium_term,
    Pub.assessment_ct_cumulative_perc_change_short_term,
    Pub.assessment_filled_posts_cumulative_perc_change_long_term,
    Pub.assessment_ct_total_employed_cumulative_perc_change_long_term_no_filter,
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
            date=stack.enter_context(patch(f"{PATCH_PATH}.date")),
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
                Pub.publication_filled_posts: [42.0],
                Pub.assessment_filled_posts_long_term: [40.0],
            }
        )
        mocks.add_rows.return_value = mocks.aggregate.return_value
        yield mocks


class TestMain:
    def test_main_reads_the_requested_run_from_the_prep_store(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION, run_number=3)

        mocks.resolve_run_sources.assert_called_once_with([TEST_SOURCE], 3)
        mocks.scan_parquet.assert_called_once_with(TEST_RUN_SOURCE)

    def test_main_reads_the_latest_run_when_no_run_number_given(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION)

        mocks.resolve_run_sources.assert_called_once_with([TEST_SOURCE], None)

    def test_main_adds_consistent_service_for_each_term_and_ct_total_employed(
        self, mocks
    ):
        job.main(TEST_SOURCE, TEST_DESTINATION)

        mocks.has_consistent_service.assert_has_calls(
            [
                call(date(2021, 7, 1), Pub.consistent_service_long_term),
                call(date(2025, 4, 1), Pub.consistent_service_medium_term),
                call(date(2026, 4, 1), Pub.consistent_service_short_term),
            ]
        )
        prepared_lf = mocks.aggregate.call_args.args[0]
        assert_frame_equal(
            prepared_lf.select(Pub.ct_total_employed_imputed),
            pl.LazyFrame({Pub.ct_total_employed_imputed: [5.0, 6.0]}),
        )

    def test_main_adds_ct_has_data_and_dispersion_filters_for_each_term(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION)

        mocks.has_continuous_data.assert_has_calls(
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
        assert mocks.add_dispersion_filter.call_count == 3
        assert [c.args[1:] for c in mocks.add_dispersion_filter.call_args_list] == [
            (ct_employed_columns, date(2021, 7, 1), Pub.ct_dispersion_filter_long_term),
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
        ]

    def test_main_uses_earliest_prepared_date_for_long_term_once_it_is_later_than_the_earliest_ct_data_date(
        self, mocks
    ):
        mocks.scan_parquet.return_value = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001", "1-001"],
                IndCQC.cqc_location_import_date: [date(2027, 4, 1), date(2027, 5, 1)],
                IndCQC.ct_care_home_total_employed_imputed: [5.0, None],
                IndCQC.ct_non_res_care_workers_employed_imputed: [None, 6.0],
            }
        )

        job.main(TEST_SOURCE, TEST_DESTINATION)

        mocks.has_continuous_data.assert_any_call(
            Pub.ct_total_employed_imputed,
            date(2027, 4, 1),
            Pub.ct_has_data_long_term,
        )

    def test_main_adds_ct_perc_change_columns_for_each_term(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION)

        group_columns = [
            IndCQC.main_job_role_clean_labelled,
            IndCQC.current_region,
            IndCQC.primary_service_type,
        ]
        mocks.perc_change_april.assert_has_calls(
            [
                call(
                    Pub.publication_filled_posts,
                    group_columns,
                    Pub.publication_filled_posts_april_perc_change,
                ),
                call(
                    Pub.publication_locationid_count,
                    group_columns,
                    Pub.publication_locationid_count_april_perc_change,
                ),
            ]
        )
        mocks.perc_change_cumulative.assert_has_calls(
            [
                call(
                    Pub.publication_filled_posts,
                    date(2026, 4, 1),
                    group_columns,
                    Pub.publication_filled_posts_cumulative_perc_change_short_term,
                ),
                call(
                    Pub.publication_locationid_count,
                    date(2026, 4, 1),
                    group_columns,
                    Pub.publication_locationid_count_cumulative_perc_change_short_term,
                ),
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
                call(
                    Pub.publication_filled_posts,
                    date(2021, 7, 1),
                    group_columns,
                    Pub.assessment_filled_posts_cumulative_perc_change_long_term,
                ),
                call(
                    Pub.assessment_ct_total_employed_long_term_no_filter,
                    date(2021, 7, 1),
                    group_columns,
                    Pub.assessment_ct_total_employed_cumulative_perc_change_long_term_no_filter,
                ),
            ]
        )

    def test_main_sinks_one_dataset_with_publication_and_assessment_columns(
        self, mocks
    ):
        job.main(TEST_SOURCE, TEST_DESTINATION)

        mocks.aggregate.assert_called_once()
        assert "include_assessment" not in mocks.aggregate.call_args.kwargs
        mocks.sink_to_parquet.assert_called_once()
        sink_kwargs = mocks.sink_to_parquet.call_args.kwargs
        assert sink_kwargs["output_path"] == TEST_DESTINATION
        expected_lf = mocks.aggregate.return_value.with_columns(
            [pl.lit(None, dtype=pl.Float32).alias(c) for c in PERC_CHANGE_COLUMNS]
        ).with_columns(pl.lit(2).alias(Pub.run_number))
        assert_frame_equal(sink_kwargs["lazy_df"], expected_lf)

    def test_main_adds_the_runs_number_as_a_column(self, mocks):
        job.main(TEST_SOURCE, TEST_DESTINATION)

        sunk_lf = mocks.sink_to_parquet.call_args.kwargs["lazy_df"]
        assert sunk_lf.collect()[Pub.run_number].to_list() == [2]
