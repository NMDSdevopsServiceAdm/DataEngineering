import unittest
import warnings


from utils import utils

from tests.test_file_data import ONSData as ONSTestData
from tests.test_file_schemas import ONSData as ONSTestSchema

from tests.test_file_data import CreateListFromRowsOfICBs as TestData
from tests.test_file_schemas import CreateListFromRowsOfICBs as TestSchema

import utils.direct_payments_utils.estimate_direct_payments.split_pa_filled_posts_into_icb_areas as job


class SplitPAFilledPostsIntoICBAreas(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_sample_rows = self.spark.createDataFrame(
            TestData.sample_rows, schema=TestSchema.sample_schema
        )
        self.test_location_df = self.spark.createDataFrame(
            TestData.expected_rows, TestSchema.expected_schema
        )


class CreatePostCodeDirectoryDfTests(SplitPAFilledPostsIntoICBAreas):
    def setUp(self) -> None:
        super().setUp()

    def test_create_postcode_directory_df(self):
        self.spark = utils.get_spark()
        self.test_sample_ons_rows = self.spark.createDataFrame(
            ONSTestData.ons_sample_rows_full, schema=ONSTestSchema.full_schema
        )

        expected_df = job.create_postcode_directory_df(self.test_sample_ons_rows)

        self.assertGreater(self.test_sample_ons_rows.count(), 4)
        self.assertEqual(expected_df.count(), 4)
