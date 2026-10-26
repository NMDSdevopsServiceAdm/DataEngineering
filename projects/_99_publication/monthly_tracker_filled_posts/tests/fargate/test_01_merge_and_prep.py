from datetime import date
from unittest.mock import Mock, patch

import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate._01_merge_and_prep as job

PATCH_PATH = (
    "projects._99_publication.monthly_tracker_filled_posts.fargate._01_merge_and_prep"
)

TEST_ESTIMATES_ROOT = "some/directory/"
TEST_METADATA_ROOT = "some/metadata/directory/"
TEST_ESTIMATES_SOURCE = "some/directory/archive_date=2026-10-01/run_number=2/"
TEST_METADATA_SOURCE = "some/metadata/directory/archive_date=2026-10-01/run_number=2/"
TEST_DESTINATION = "some/other/directory"
TEST_RUN_DESTINATION = "some/other/directory/archive_date=2026-10-01/run_number=2/"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_runs(
        self,
        resolve_run_sources_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]
        archived_jr_estimate_lf = Mock(name="archived_jr_estimate_lf")
        archived_jr_metadata_lf = Mock(name="archived_jr_metadata_lf")
        scan_parquet_mock.side_effect = [
            archived_jr_estimate_lf,
            archived_jr_metadata_lf,
        ]

        job.main(
            TEST_ESTIMATES_ROOT,
            TEST_METADATA_ROOT,
            TEST_DESTINATION,
        )

        assert scan_parquet_mock.call_count == 2
        scan_parquet_mock.assert_any_call(
            TEST_ESTIMATES_SOURCE,
            selected_columns=job.JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
        )
        scan_parquet_mock.assert_any_call(
            TEST_METADATA_SOURCE,
            selected_columns=job.JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
        )

        archived_jr_estimate_lf.join.assert_called_once_with(
            archived_jr_metadata_lf,
            on=job.IndCQC.id_per_locationid_import_date,
            how="left",
        )
        joined_metadata_lf = archived_jr_estimate_lf.join.return_value
        joined_metadata_lf.filter.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=joined_metadata_lf.filter.return_value.with_columns.return_value,
            output_path=TEST_RUN_DESTINATION,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_resolves_latest_run_when_no_run_number_given(
        self,
        resolve_run_sources_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]

        job.main(TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT, TEST_DESTINATION)

        resolve_run_sources_mock.assert_called_once_with(
            [TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT], None
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_resolves_requested_run_number(
        self,
        resolve_run_sources_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]

        job.main(
            TEST_ESTIMATES_ROOT,
            TEST_METADATA_ROOT,
            TEST_DESTINATION,
            run_number=3,
        )

        resolve_run_sources_mock.assert_called_once_with(
            [TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT], 3
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_logs_the_resolved_run_sources(
        self,
        resolve_run_sources_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
        capsys: pytest.CaptureFixture,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]

        job.main(TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT, TEST_DESTINATION)

        logged = capsys.readouterr().out
        assert TEST_ESTIMATES_SOURCE in logged
        assert TEST_METADATA_SOURCE in logged

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.publication_utils.reduced_data_filter_expr")
    @patch(f"{PATCH_PATH}.publication_utils.get_retention_cutoff_date")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_applies_retention_filter_to_merged_data(
        self,
        resolve_run_sources_mock: Mock,
        get_retention_cutoff_date_mock: Mock,
        reduced_data_filter_expr_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]
        get_retention_cutoff_date_mock.return_value = date(2020, 4, 1)
        archived_jr_estimate_lf = Mock(name="archived_jr_estimate_lf")
        scan_parquet_mock.side_effect = [archived_jr_estimate_lf, Mock()]

        job.main(TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT, TEST_DESTINATION)

        reduced_data_filter_expr_mock.assert_called_once_with(
            cutoff_date=date(2020, 4, 1)
        )
        joined_metadata_lf = archived_jr_estimate_lf.join.return_value
        joined_metadata_lf.filter.assert_called_once_with(
            reduced_data_filter_expr_mock.return_value
        )
        assert (
            sink_to_parquet_mock.call_args.kwargs["lazy_df"]
            is joined_metadata_lf.filter.return_value.with_columns.return_value
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.publication_utils.period_label_long_expr")
    @patch(f"{PATCH_PATH}.publication_utils.period_label_short_expr")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_adds_period_labels_to_filtered_data(
        self,
        resolve_run_sources_mock: Mock,
        period_label_short_expr_mock: Mock,
        period_label_long_expr_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]
        archived_jr_estimate_lf = Mock(name="archived_jr_estimate_lf")
        scan_parquet_mock.side_effect = [archived_jr_estimate_lf, Mock()]

        job.main(TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT, TEST_DESTINATION)

        filtered_lf = archived_jr_estimate_lf.join.return_value.filter.return_value
        filtered_lf.with_columns.assert_called_once_with(
            period_label_short_expr_mock.return_value,
            period_label_long_expr_mock.return_value,
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    @patch(f"{PATCH_PATH}.merge_utils.run_partition_path")
    @patch(f"{PATCH_PATH}.merge_utils.resolve_run_sources")
    def test_main_writes_merged_data_to_the_runs_partition_of_the_destination(
        self,
        resolve_run_sources_mock: Mock,
        run_partition_path_mock: Mock,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        resolve_run_sources_mock.return_value = [
            TEST_ESTIMATES_SOURCE,
            TEST_METADATA_SOURCE,
        ]

        job.main(TEST_ESTIMATES_ROOT, TEST_METADATA_ROOT, TEST_DESTINATION)

        run_partition_path_mock.assert_called_once_with(
            TEST_DESTINATION, TEST_ESTIMATES_SOURCE
        )
        assert (
            sink_to_parquet_mock.call_args.kwargs["output_path"]
            == run_partition_path_mock.return_value
        )
