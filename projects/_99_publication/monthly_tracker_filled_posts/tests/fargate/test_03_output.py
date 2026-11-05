from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import projects._99_publication.monthly_tracker_filled_posts.fargate._03_output as job
from projects._99_publication.monthly_tracker_filled_posts.fargate.utils.output_workbook_utils import (
    LogoUnavailableError,
)
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
TEST_WORKBOOK_DESTINATION = "some/workbook/directory/"
TEST_LOGO_SOURCE = "s3://bucket/publication/logo/logo.png"
TEST_LOGO_BYTES = b"png bytes"
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


def _written_lf(rows: list[tuple] | None = None) -> pl.LazyFrame:
    """The publication data as read back after it was written (one row by default)."""
    if rows is None:
        rows = [
            (
                date(2026, 4, 1),
                "Mar 2026",
                "March 2026",
                "London",
                PublishedMainService.care_home_with_nursing,
                100.0,
                10,
                None,
                None,
                None,
                None,
            )
        ]
    return pl.LazyFrame(rows, expected_schema, orient="row")


@pytest.fixture
def mocks():
    """Replaces the job's inputs and outputs with mocks and fixes today's date."""
    calls: list[str] = []
    with (
        patch(f"{PATCH_PATH}.utils.scan_parquet") as scan_parquet,
        patch(f"{PATCH_PATH}.utils.sink_to_parquet") as sink_to_parquet,
        patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources") as resolve_run_sources,
        patch(f"{PATCH_PATH}.output_workbook_utils.get_logo_bytes") as get_logo_bytes,
        patch(
            f"{PATCH_PATH}.output_workbook_utils.build_download_workbook"
        ) as build_download_workbook,
        patch(f"{PATCH_PATH}.workbook_helpers.upload_to_s3") as upload_to_s3,
        patch(f"{PATCH_PATH}.date") as date_mock,
    ):
        resolve_run_sources.return_value = [TEST_RUN_SOURCE]
        date_mock.today.return_value = TEST_TODAY
        get_logo_bytes.return_value = TEST_LOGO_BYTES
        sink_to_parquet.side_effect = lambda **kwargs: calls.append("sink")
        build_download_workbook.side_effect = lambda *args, **kwargs: calls.append(
            "build"
        )
        yield SimpleNamespace(
            scan_parquet=scan_parquet,
            sink_to_parquet=sink_to_parquet,
            resolve_run_sources=resolve_run_sources,
            get_logo_bytes=get_logo_bytes,
            build_download_workbook=build_download_workbook,
            upload_to_s3=upload_to_s3,
            calls=calls,
        )


def _run_main(
    mocks,
    input_rows: list[tuple],
    run_number: int = 2,
    written_lf: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """Runs main on the input rows and returns the frame it sinks."""
    mocks.scan_parquet.side_effect = [
        pl.LazyFrame(input_rows, input_schema, orient="row"),
        _written_lf() if written_lf is None else written_lf,
    ]
    job.main(
        TEST_SOURCE,
        TEST_DESTINATION,
        run_number=run_number,
        workbook_destination=TEST_WORKBOOK_DESTINATION,
        logo_source=TEST_LOGO_SOURCE,
    )
    return mocks.sink_to_parquet.call_args.kwargs["lazy_df"]


class TestMain:
    def test_main_reads_the_selected_run_from_the_assessment_store(self, mocks):
        _run_main(mocks, [_input_row(date(2026, 4, 1))], run_number=3)

        mocks.resolve_run_sources.assert_called_once_with([TEST_SOURCE], 3)
        assert mocks.scan_parquet.call_args_list[0].args == (TEST_RUN_SOURCE,)

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


class TestMainDataDownloadWorkbook:
    def test_writes_the_workbook_after_the_publication_parquet(self, mocks):
        _run_main(mocks, [_input_row(date(2026, 4, 1))])

        assert mocks.calls == ["sink", "build"]

    def test_builds_the_workbook_from_the_written_publication_data(self, mocks):
        written_lf = _written_lf()

        _run_main(mocks, [_input_row(date(2026, 4, 1))], written_lf=written_lf)

        assert mocks.scan_parquet.call_args_list[1].args == (TEST_DESTINATION,)
        assert mocks.build_download_workbook.call_args.args[0] is written_lf

    def test_names_the_workbook_by_archive_date_and_run_number(self, mocks):
        _run_main(mocks, [_input_row(date(2026, 4, 1))])

        built_path = mocks.build_download_workbook.call_args.args[1]
        assert (
            Path(built_path).name == "monthly_tracker_data_download_2026-10-01_2.xlsx"
        )

    def test_uploads_the_workbook_to_the_workbook_destination(self, mocks):
        _run_main(mocks, [_input_row(date(2026, 4, 1))])

        uploaded_path, destination = mocks.upload_to_s3.call_args.args
        built_path = mocks.build_download_workbook.call_args.args[1]
        assert uploaded_path == built_path
        assert destination == TEST_WORKBOOK_DESTINATION

    def test_passes_the_fetched_logo_bytes_to_the_builder(self, mocks):
        _run_main(mocks, [_input_row(date(2026, 4, 1))])

        mocks.get_logo_bytes.assert_called_once_with(TEST_LOGO_SOURCE)
        assert mocks.build_download_workbook.call_args.kwargs["logo"] == TEST_LOGO_BYTES

    def test_raises_when_the_logo_cannot_be_fetched(self, mocks):
        mocks.get_logo_bytes.side_effect = LogoUnavailableError("no logo")

        with pytest.raises(LogoUnavailableError):
            _run_main(mocks, [_input_row(date(2026, 4, 1))])

        mocks.build_download_workbook.assert_not_called()
        mocks.upload_to_s3.assert_not_called()

    def test_raises_when_the_publication_data_has_no_rows(self, mocks):
        with pytest.raises(ValueError, match="no rows"):
            _run_main(mocks, [_input_row(date(2026, 4, 1))], written_lf=_written_lf([]))

        mocks.build_download_workbook.assert_not_called()
        mocks.upload_to_s3.assert_not_called()

    def test_does_not_upload_when_the_workbook_build_fails(self, mocks):
        mocks.build_download_workbook.side_effect = RuntimeError("build failed")

        with pytest.raises(RuntimeError, match="build failed"):
            _run_main(mocks, [_input_row(date(2026, 4, 1))])

        mocks.upload_to_s3.assert_not_called()
