import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from utils import utils

from tests.test_file_data import ONSData as ONSTestData
from tests.test_file_schemas import ONSData as ONSTestSchema

import jobs.split_pa_filled_posts_into_icb_areas as job


class SplitPAFilledPostsIntoICBAreas(unittest.TestCase):
    TEST_SOURCE = "some/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_sample_ons_rows = self.spark.createDataFrame(
            ONSTestData.ons_sample_rows_full, schema=ONSTestSchema.full_schema
        )


class MainTests(SplitPAFilledPostsIntoICBAreas):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock: Mock):
        read_from_parquet_mock.return_value = self.test_sample_ons_rows
        job.main(self.TEST_SOURCE)
        self.assertEqual(read_from_parquet_mock.call_count, 1)
