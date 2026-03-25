import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._03_impute.fargate.impute_ind_cqc_ascwds_and_pir as job
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys

PATCH_PATH = (
    "projects._03_independent_cqc._03_impute.fargate.impute_ind_cqc_ascwds_and_pir"
)


class ImputeIndCqcAscwdsAndPirTests(unittest.TestCase):
    TEST_CLEANED_IND_CQC_DATA_SOURCE = "some/directory"
    TEST_DESTINATION = "some/other/directory"
    cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.convert_pir_to_filled_posts")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        convert_pir_to_filled_posts_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):

        job.main(
            self.TEST_CLEANED_IND_CQC_DATA_SOURCE,
            self.TEST_DESTINATION,
        )

        scan_parquet_mock.assert_called_once()
        convert_pir_to_filled_posts_mock.assert_called_once()
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            partition_cols=self.cqc_partition_keys,
            append=False,
        )
