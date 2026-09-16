from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._02_employment_status.fargate._04_estimate as job

PATCH_PATH = "projects._03_independent_cqc._02_employment_status.fargate._04_estimate"


class TestMain:
    IMPUTED_DATA_SOURCE = "some/source"
    ESTIMATED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.IMPUTED_DATA_SOURCE,
            self.ESTIMATED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.IMPUTED_DATA_SOURCE)

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.ESTIMATED_DATA_DESTINATION,
        )
