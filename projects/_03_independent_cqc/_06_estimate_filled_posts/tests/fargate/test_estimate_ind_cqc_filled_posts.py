import unittest
from unittest.mock import ANY, Mock, patch

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.estimate_ind_cqc_filled_posts as job

PATCH_PATH = "projects._03_independent_cqc._06_estimate_filled_posts.fargate.estimate_ind_cqc_filled_posts"


class EstimateIndCQCFilledPostsTests(unittest.TestCase):
    TEST_BUCKET_NAME = "some/bucket/name"
    TEST_IMPUTED_IND_CQC_DATA_SOURCE = "some/s3/uri"
    TEST_DESTINATION = "some/other/s3/uri"

    mock_data = Mock(name="data")

    @patch(f"{PATCH_PATH}.utils.sink_to_parquet")
    @patch(f"{PATCH_PATH}.enrich_with_model_predictions")
    @patch(f"{PATCH_PATH}.utils.scan_parquet", return_value=mock_data)
    def test_main_runs_successfully(
        self,
        scan_parquet_mock: Mock,
        enrich_with_model_predictions_mock: Mock,
        sink_to_parquet_mock: Mock,
    ):

        job.main(
            self.TEST_BUCKET_NAME,
            self.TEST_IMPUTED_IND_CQC_DATA_SOURCE,
            self.TEST_DESTINATION,
        )

        scan_parquet_mock.assert_called_once_with(
            source=ANY,
            selected_columns=job.ind_cqc_columns,
        )
        self.assertEqual(enrich_with_model_predictions_mock.call_count, 3)
        sink_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            append=False,
        )
