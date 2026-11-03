from datetime import date
from types import SimpleNamespace
from unittest.mock import patch

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import projects._99_publication.monthly_tracker_filled_posts.fargate._03_output as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub
from utils.column_values.categorical_column_values import (
    PrimaryServiceType,
    PublishedJobGroupLabels,
    PublishedMainService,
)

PATCH_PATH = "projects._99_publication.monthly_tracker_filled_posts.fargate._03_output"

TEST_SOURCE = "some/assessment/directory/"
TEST_RUN_SOURCE = "some/assessment/directory/archive_date=2026-10-01/run_number=2/"
TEST_DESTINATION = "some/other/directory"
TEST_TODAY = date(2026, 10, 6)

input_schema = pl.Schema(
    [
        (IndCQC.cqc_location_import_date, pl.Date()),
        (Pub.period_label_short, pl.String()),
        (Pub.period_label_long, pl.String()),
        (IndCQC.main_job_role_clean_labelled, pl.String()),
        (IndCQC.current_region, pl.String()),
        (IndCQC.primary_service_type, pl.String()),
        (Pub.unfiltered_filled_posts, pl.Float32()),
        (Pub.unfiltered_locationid_count, pl.UInt32()),
        (Pub.unfiltered_filled_posts_april_pct_change, pl.Float64()),
        (Pub.unfiltered_short_sfc_cum_pct_change, pl.Float64()),
        (Pub.unfiltered_locationid_count_april_pct_change, pl.Float64()),
        (
            Pub.unfiltered_short_locationid_count_cum_pct_change,
            pl.Float64(),
        ),
    ]
)
expected_schema = pl.Schema(
    [
        (Pub.period, pl.Date()),
        (Pub.period_label_short, pl.String()),
        (Pub.period_label_long, pl.String()),
        (Pub.region, pl.String()),
        (Pub.main_service, pl.String()),
        (Pub.estimated_filled_posts_rounded, pl.Float32()),
        (Pub.cqc_locations, pl.UInt32()),
        (Pub.unfiltered_filled_posts_april_pct_change, pl.Float64()),
        (Pub.unfiltered_short_sfc_cum_pct_change, pl.Float64()),
        (Pub.unfiltered_locationid_count_april_pct_change, pl.Float64()),
        (
            Pub.unfiltered_short_locationid_count_cum_pct_change,
            pl.Float64(),
        ),
    ]
)


def _input_row(
    import_date: date,
    job_role: str = PublishedJobGroupLabels.all_job_roles,
    region: str = "London",
    values: tuple = (100.0, 10, None, None, None, None),
    period_label_short: str = "Mar 2026",
    period_label_long: str = "March 2026",
) -> tuple:
    return (
        import_date,
        period_label_short,
        period_label_long,
        job_role,
        region,
        PrimaryServiceType.care_home_with_nursing,
        *values,
    )


@pytest.fixture
def mocks():
    """Replaces the job's inputs and outputs with mocks and fixes today's date."""
    with (
        patch(f"{PATCH_PATH}.utils.scan_parquet") as scan_parquet,
        patch(f"{PATCH_PATH}.utils.sink_to_parquet") as sink_to_parquet,
        patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources") as resolve_run_sources,
        patch(f"{PATCH_PATH}.date") as date_mock,
    ):
        resolve_run_sources.return_value = [TEST_RUN_SOURCE]
        date_mock.today.return_value = TEST_TODAY
        yield SimpleNamespace(
            scan_parquet=scan_parquet,
            sink_to_parquet=sink_to_parquet,
            resolve_run_sources=resolve_run_sources,
        )


def _run_main(mocks, input_rows: list[tuple], run_number: int = 2) -> pl.LazyFrame:
    """Runs main on the input rows and returns the frame it sinks."""
    mocks.scan_parquet.return_value = pl.LazyFrame(
        input_rows, input_schema, orient="row"
    )
    job.main(TEST_SOURCE, TEST_DESTINATION, run_number=run_number)
    return mocks.sink_to_parquet.call_args.kwargs["lazy_df"]


class TestMain:
    def test_main_reads_the_selected_run_from_the_assessment_store(self, mocks):
        _run_main(mocks, [_input_row(date(2026, 4, 1))], run_number=3)

        mocks.resolve_run_sources.assert_called_once_with([TEST_SOURCE], 3)
        mocks.scan_parquet.assert_called_once_with(TEST_RUN_SOURCE)

    def test_main_sinks_one_output_to_the_output_destination(self, mocks):
        _run_main(mocks, [_input_row(date(2026, 4, 1))])

        mocks.sink_to_parquet.assert_called_once()
        assert mocks.sink_to_parquet.call_args.kwargs["output_path"] == TEST_DESTINATION

    def test_main_selects_and_renames_the_download_table_columns(self, mocks):
        input_rows = [
            _input_row(
                date(2026, 5, 1),
                values=(121.0, 12, 0.1, 0.2, 0.3, 0.4),
                period_label_short="Apr 2026",
                period_label_long="April 2026",
            ),
        ]

        returned_lf = _run_main(mocks, input_rows)

        expected_lf = pl.LazyFrame(
            [
                (
                    date(2026, 5, 1),
                    "Apr 2026",
                    "April 2026",
                    "London",
                    PublishedMainService.care_home_with_nursing,
                    125.0,
                    12,
                    0.1,
                    0.2,
                    0.3,
                    0.4,
                )
            ],
            expected_schema,
            orient="row",
        )
        assert_frame_equal(returned_lf, expected_lf)

    def test_main_keeps_only_all_job_roles_rows(self, mocks):
        input_rows = [
            _input_row(date(2026, 4, 1), job_role="Registered nurse"),
            _input_row(date(2026, 4, 1)),
        ]

        returned_lf = _run_main(mocks, input_rows)

        assert returned_lf.collect().height == 1

    def test_main_keeps_one_row_per_year_historically_and_every_current_month(
        self, mocks
    ):
        input_rows = [
            _input_row(date(2025, 4, 1)),
            _input_row(date(2025, 7, 1)),
            _input_row(date(2026, 4, 1)),
            _input_row(date(2026, 5, 1)),
        ]

        returned_lf = _run_main(mocks, input_rows)

        assert returned_lf.collect()[Pub.period].to_list() == [
            date(2025, 4, 1),
            date(2026, 4, 1),
            date(2026, 5, 1),
        ]

    def test_main_sorts_by_period_then_region_then_main_service(self, mocks):
        input_rows = [
            _input_row(date(2026, 5, 1), region="London"),
            _input_row(date(2026, 4, 1), region="South West"),
            _input_row(date(2026, 4, 1), region="London"),
        ]

        returned_lf = _run_main(mocks, input_rows)

        assert returned_lf.select(Pub.period, Pub.region).collect().rows() == [
            (date(2026, 4, 1), "London"),
            (date(2026, 4, 1), "South West"),
            (date(2026, 5, 1), "London"),
        ]

    def test_main_passes_percentage_change_columns_through_unchanged(self, mocks):
        input_rows = [
            _input_row(date(2025, 7, 1), values=(1.0, 1, 0.9, 0.9, 0.9, 0.9)),
            _input_row(date(2026, 4, 1), values=(2.0, 2, 0.1, 0.2, 0.3, 0.4)),
            _input_row(date(2026, 5, 1), values=(3.0, 3, None, 0.5, None, 0.6)),
        ]

        returned_lf = _run_main(mocks, input_rows)

        assert returned_lf.select(
            Pub.unfiltered_filled_posts_april_pct_change,
            Pub.unfiltered_short_sfc_cum_pct_change,
            Pub.unfiltered_locationid_count_april_pct_change,
            Pub.unfiltered_short_locationid_count_cum_pct_change,
        ).collect().rows() == [
            (
                pytest.approx(0.1),
                pytest.approx(0.2),
                pytest.approx(0.3),
                pytest.approx(0.4),
            ),
            (None, pytest.approx(0.5), None, pytest.approx(0.6)),
        ]

    def test_main_passes_the_period_labels_through(self, mocks):
        input_rows = [
            _input_row(
                date(2026, 4, 1),
                period_label_short="Some label",
                period_label_long="Some long label",
            )
        ]

        returned_lf = _run_main(mocks, input_rows).collect()

        assert returned_lf[Pub.period_label_short].to_list() == ["Some label"]
        assert returned_lf[Pub.period_label_long].to_list() == ["Some long label"]
