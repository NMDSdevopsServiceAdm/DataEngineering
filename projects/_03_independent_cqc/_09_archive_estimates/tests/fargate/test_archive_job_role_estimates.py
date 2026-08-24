from unittest.mock import Mock, call, patch

import projects._03_independent_cqc._09_archive_estimates.fargate.archive_job_role_estimates as job

PATCH_PATH = "projects._03_independent_cqc._09_archive_estimates.fargate.archive_job_role_estimates"

ESTIMATES_SOURCE = "some/estimates/directory"
METADATA_SOURCE = "some/metadata/directory"
OVERALL_ESTIMATES_SOURCE = "some/overall/estimates/directory"
DESTINATION = "an/other/directory"


class TestMain:
    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_scans_all_three_sources_and_sinks_only_the_job_role_estimates(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        estimates_lf = Mock(name="estimates_lf")
        metadata_lf = Mock(name="metadata_lf")
        overall_estimates_lf = Mock(name="overall_estimates_lf")
        scan_parquet_mock.side_effect = [
            estimates_lf,
            metadata_lf,
            overall_estimates_lf,
        ]

        job.main(
            ESTIMATES_SOURCE,
            METADATA_SOURCE,
            OVERALL_ESTIMATES_SOURCE,
            DESTINATION,
        )

        scan_parquet_mock.assert_has_calls(
            [
                call(ESTIMATES_SOURCE),
                call(METADATA_SOURCE),
                call(OVERALL_ESTIMATES_SOURCE),
            ]
        )
        sink_to_parquet_mock.assert_called_once_with(estimates_lf, DESTINATION)
