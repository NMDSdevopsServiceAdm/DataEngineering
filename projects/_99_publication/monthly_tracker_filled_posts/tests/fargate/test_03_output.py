from contextlib import ExitStack
from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import projects._99_publication.monthly_tracker_filled_posts.fargate._03_output as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import PrimaryServiceType

PATCH_PATH = "projects._99_publication.monthly_tracker_filled_posts.fargate._03_output"

TEST_SOURCE = "some/prep/directory/"
TEST_RUN_SOURCE = "some/prep/directory/archive_date=2026-10-01/run_number=2/"
TEST_DESTINATION = "some/other/directory"
TEST_T0_DESTINATION = "some/t0/directory"
TEST_T1_DESTINATION = "some/t1/directory"
TEST_T2_DESTINATION = "some/t2/directory"
TEST_TODAY = date(2026, 9, 1)

_service_type_enum = pl.Enum(
    [
        PrimaryServiceType.care_home_with_nursing,
        PrimaryServiceType.care_home_only,
        PrimaryServiceType.non_residential,
    ]
)


def _run_main(run_number: int = 2) -> None:
    job.main(
        TEST_SOURCE,
        TEST_DESTINATION,
        TEST_T0_DESTINATION,
        TEST_T1_DESTINATION,
        TEST_T2_DESTINATION,
        run_number=run_number,
    )


@pytest.fixture
def io_mocks():
    """Replaces the job's inputs and outputs with mocks and fixes today's date."""
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
        )
        mocks.resolve_run_sources.return_value = [TEST_RUN_SOURCE]
        mocks.scan_parquet.return_value = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001", "1-001"],
                IndCQC.cqc_location_import_date: [date(2025, 4, 1), date(2026, 4, 1)],
                IndCQC.estimate_filled_posts_by_job_role: [5.0, 6.0],
            }
        )
        mocks.date.today.return_value = TEST_TODAY
        yield mocks


@pytest.fixture
def mocks(io_mocks):
    """Also replaces the publication_utils functions with stand-ins returning small frames."""
    with ExitStack() as stack:
        for name in [
            "aggregate_to_publication_rows",
            "add_rows_for_publication_groups",
            "build_t0_estimates_download_table",
            "build_t1_filled_posts_perc_change_download_table",
            "build_t2_location_count_perc_change_download_table",
        ]:
            setattr(
                io_mocks,
                name,
                stack.enter_context(patch(f"{PATCH_PATH}.publication_utils.{name}")),
            )
        io_mocks.format_large_number = stack.enter_context(
            patch(
                f"{PATCH_PATH}.publication_utils.format_large_number",
                side_effect=lambda column_name, column_alias: pl.lit("42").alias(
                    column_alias
                ),
            )
        )
        io_mocks.aggregate_to_publication_rows.return_value = pl.LazyFrame(
            {
                IndCQC.cqc_location_import_date: [date(2026, 4, 1)],
                Pub.publication_filled_posts: [42.0],
            }
        )
        io_mocks.add_rows_for_publication_groups.return_value = (
            io_mocks.aggregate_to_publication_rows.return_value
        )
        yield io_mocks


class TestMain:
    def test_main_reads_the_selected_run_from_the_prep_store(self, mocks):
        _run_main(run_number=3)

        mocks.resolve_run_sources.assert_called_once_with([TEST_SOURCE], 3)
        mocks.scan_parquet.assert_called_once_with(TEST_RUN_SOURCE)

    def test_main_does_not_add_ct_flags_or_dispersion_filters(self, mocks):
        _run_main()

        aggregated_lf = mocks.aggregate_to_publication_rows.call_args.args[0]
        assert (
            aggregated_lf.collect_schema().names()
            == mocks.scan_parquet.return_value.collect_schema().names()
        )

    def test_main_aggregates_and_adds_rollups_without_assessment_columns(self, mocks):
        _run_main()

        prepared_lf = mocks.scan_parquet.return_value
        mocks.aggregate_to_publication_rows.assert_called_once_with(
            prepared_lf, include_assessment=False
        )
        mocks.add_rows_for_publication_groups.assert_called_once_with(
            prepared_lf,
            mocks.aggregate_to_publication_rows.return_value,
            include_assessment=False,
        )

    def test_main_adds_formatted_columns_and_import_date_labels(self, mocks):
        _run_main()

        mocks.format_large_number.assert_called_once_with(
            Pub.publication_filled_posts, Pub.publication_filled_posts_formatted
        )
        expected_lf = mocks.aggregate_to_publication_rows.return_value.with_columns(
            pl.lit("42").alias(Pub.publication_filled_posts_formatted),
            pl.lit("Apr 2026").alias(Pub.cqc_location_import_date_abbreviated),
            pl.lit("April 2026").alias(Pub.cqc_location_import_date_full),
        )
        output_sink_kwargs = mocks.sink_to_parquet.call_args_list[0].kwargs
        assert_frame_equal(output_sink_kwargs["lazy_df"], expected_lf)

    def test_main_sinks_output_t0_t1_and_t2_to_their_destinations(self, mocks):
        _run_main()

        output_lf = mocks.sink_to_parquet.call_args_list[0].kwargs["lazy_df"]
        builders = [
            mocks.build_t0_estimates_download_table,
            mocks.build_t1_filled_posts_perc_change_download_table,
            mocks.build_t2_location_count_perc_change_download_table,
        ]
        for builder in builders:
            builder.assert_called_once()
            assert builder.call_args.args[0] is output_lf
            assert builder.call_args.kwargs == {"today": TEST_TODAY}

        assert [
            c.kwargs["output_path"] for c in mocks.sink_to_parquet.call_args_list
        ] == [
            TEST_DESTINATION,
            TEST_T0_DESTINATION,
            TEST_T1_DESTINATION,
            TEST_T2_DESTINATION,
        ]
        assert [
            c.kwargs["lazy_df"] for c in mocks.sink_to_parquet.call_args_list[1:]
        ] == [builder.return_value for builder in builders]

    def test_main_output_has_no_assessment_columns(self, io_mocks):
        io_mocks.scan_parquet.return_value = pl.LazyFrame(
            {
                IndCQC.location_id: ["1-001", "1-001"],
                IndCQC.cqc_location_import_date: [date(2026, 4, 1)] * 2,
                IndCQC.main_job_role_clean_labelled: [
                    "Care worker",
                    "Registered nurse",
                ],
                IndCQC.current_region: ["London"] * 2,
                IndCQC.primary_service_type: [PrimaryServiceType.care_home_only] * 2,
                IndCQC.estimate_filled_posts_by_job_role: [10.0, 20.0],
            },
            schema_overrides={IndCQC.primary_service_type: _service_type_enum},
        )

        _run_main()

        output_lf = io_mocks.sink_to_parquet.call_args_list[0].kwargs["lazy_df"]
        output_columns = output_lf.collect_schema().names()
        assert Pub.publication_filled_posts in output_columns
        assert not [
            column
            for column in output_columns
            if column.startswith(("assessment_", "ct_", "consistent_service"))
        ]
