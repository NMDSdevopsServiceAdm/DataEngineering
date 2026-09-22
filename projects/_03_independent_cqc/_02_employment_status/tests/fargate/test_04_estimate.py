from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._02_employment_status.fargate._04_estimate as job

PATCH_PATH = "projects._03_independent_cqc._02_employment_status.fargate._04_estimate"


class TestMain:
    IMPUTED_DATA_SOURCE = "some/source"
    EMPLOYMENT_STATUS_RATES_SOURCE = "employment/status/rates/source"
    ESTIMATED_DATA_DESTINATION = "some/destination"

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.mnUtils.apply_employment_status_magic_numbers")
    @patch(f"{PATCH_PATH}.pl.scan_csv")
    @patch(f"{PATCH_PATH}.utils.scan_parquet")
    def test_main_runs(
        self,
        scan_parquet_mock: Mock,
        scan_csv_mock: Mock,
        apply_employment_status_magic_numbers_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):
        job.main(
            self.IMPUTED_DATA_SOURCE,
            self.EMPLOYMENT_STATUS_RATES_SOURCE,
            self.ESTIMATED_DATA_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(self.IMPUTED_DATA_SOURCE)

        scan_csv_mock.assert_called_once_with(
            self.EMPLOYMENT_STATUS_RATES_SOURCE, schema=ANY
        )

        apply_employment_status_magic_numbers_mock.assert_called_once()

        sink_to_parquet_mock.assert_called_once_with(
            lazy_df=ANY,
            output_path=self.ESTIMATED_DATA_DESTINATION,
        )
