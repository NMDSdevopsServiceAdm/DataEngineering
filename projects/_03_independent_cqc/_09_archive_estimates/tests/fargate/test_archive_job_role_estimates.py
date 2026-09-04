from unittest.mock import Mock, call, patch

import projects._03_independent_cqc._09_archive_estimates.fargate.archive_job_role_estimates as job

PATCH_PATH = "projects._03_independent_cqc._09_archive_estimates.fargate.archive_job_role_estimates"

ESTIMATES_SOURCE = "some/estimates/directory"
METADATA_SOURCE = "some/metadata/directory"
GEOGRAPHY_SOURCE = "some/geography/directory"
ESTIMATES_DESTINATION = "some/estimates/destination"
METADATA_DESTINATION = "some/metadata/destination"
GEOGRAPHY_DESTINATION = "some/geography/destination"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_scans_all_three_sources_with_expected_columns(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        estimates_lf = Mock(name="estimates_lf")
        metadata_lf = Mock(name="metadata_lf")
        geography_lf = Mock(name="geography_lf")
        scan_parquet_mock.side_effect = [estimates_lf, metadata_lf, geography_lf]

        job.main(
            ESTIMATES_SOURCE,
            METADATA_SOURCE,
            GEOGRAPHY_SOURCE,
            ESTIMATES_DESTINATION,
            METADATA_DESTINATION,
            GEOGRAPHY_DESTINATION,
        )

        assert scan_parquet_mock.call_count == 3
        scan_parquet_mock.assert_has_calls(
            [
                call(
                    ESTIMATES_SOURCE,
                    selected_columns=job.JOB_ROLE_ESTIMATES_ARCHIVE_COLUMNS,
                ),
                call(
                    METADATA_SOURCE,
                    selected_columns=job.JOB_ROLE_METADATA_ARCHIVE_COLUMNS,
                ),
                call(
                    GEOGRAPHY_SOURCE,
                    selected_columns=job.JOB_ROLE_GEOGRAPHY_ARCHIVE_COLUMNS,
                ),
            ]
        )

        assert sink_to_parquet_mock.call_count == 3
        sink_to_parquet_mock.assert_has_calls(
            [
                call(estimates_lf, ESTIMATES_DESTINATION),
                call(metadata_lf, METADATA_DESTINATION),
            ]
        )

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_sinks_deduplicated_geography_data(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        estimates_lf = Mock(name="estimates_lf")
        metadata_lf = Mock(name="metadata_lf")
        geography_lf = Mock(name="geography_lf")
        deduped_geography_lf = Mock(name="deduped_geography_lf")
        geography_lf.unique.return_value = deduped_geography_lf
        scan_parquet_mock.side_effect = [estimates_lf, metadata_lf, geography_lf]

        job.main(
            ESTIMATES_SOURCE,
            METADATA_SOURCE,
            GEOGRAPHY_SOURCE,
            ESTIMATES_DESTINATION,
            METADATA_DESTINATION,
            GEOGRAPHY_DESTINATION,
        )

        geography_lf.unique.assert_called_once_with()
        sink_to_parquet_mock.assert_any_call(
            deduped_geography_lf, GEOGRAPHY_DESTINATION
        )
