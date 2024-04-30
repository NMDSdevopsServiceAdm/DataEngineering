import unittest
import warnings
from unittest.mock import ANY, Mock, patch

from utils import utils

from tests.test_file_data import PAFilledPostsByICBArea as TestData
from tests.test_file_schemas import PAFilledPostsByICBAreaSchema as TestSchema

from utils.direct_payments_utils.direct_payments_column_names import (
    DirectPaymentColumnNames as DPColNames,
)

import jobs.split_pa_filled_posts_into_icb_areas as job


class SplitPAFilledPostsIntoICBAreas(unittest.TestCase):
    TEST_ONS_SOURCE = "some/directory"
    TEST_PA_SOURCE = "some/directory"
    TEST_DESTINATION = "some/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_sample_ons_rows = self.spark.createDataFrame(
            TestData.ons_sample_contemporary_rows,
            schema=TestSchema.ons_sample_contemporary_schema,
        )
        self.test_sample_pa_filled_post_rows = self.spark.createDataFrame(
            TestData.pa_sample_filled_post_rows,
            schema=TestSchema.pa_sample_filled_post_schema,
        )


class MainTests(SplitPAFilledPostsIntoICBAreas):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.write_to_parquet")
    @patch("utils.utils.read_from_parquet")
    def test_main(self, read_from_parquet_mock: Mock, write_to_parquet_mock: Mock):
        read_from_parquet_mock.side_effect = [
            self.test_sample_ons_rows,
            self.test_sample_pa_filled_post_rows,
        ]
        job.main(self.TEST_ONS_SOURCE, self.TEST_PA_SOURCE, self.TEST_DESTINATION)
        self.assertEqual(read_from_parquet_mock.call_count, 2)
        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.TEST_DESTINATION,
            mode="overwrite",
            partitionKeys=[DPColNames.YEAR],
        )


class CountPostcodesPerLA(SplitPAFilledPostsIntoICBAreas):
    def setUp(self) -> None:
        super().setUp()

    def test_count_postcodes_per_la_adds_postcodes_per_la_column(
        self,
    ):
        returned_data = job.count_postcodes_per_la(self.test_sample_ons_rows)
        self.assertTrue(DPColNames.SUM_POSTCODES_PER_LA in returned_data.columns)

    def test_count_postcodes_per_la_has_expected_values_in_new_column(
        self,
    ):
        returned_data = job.count_postcodes_per_la(self.test_sample_ons_rows).collect()
        self.assertEqual(returned_data[0][DPColNames.SUM_POSTCODES_PER_LA], 3)
        self.assertEqual(returned_data[1][DPColNames.SUM_POSTCODES_PER_LA], 3)
        self.assertEqual(returned_data[2][DPColNames.SUM_POSTCODES_PER_LA], 3)
        self.assertEqual(returned_data[3][DPColNames.SUM_POSTCODES_PER_LA], 4)
        self.assertEqual(returned_data[4][DPColNames.SUM_POSTCODES_PER_LA], 4)
        self.assertEqual(returned_data[5][DPColNames.SUM_POSTCODES_PER_LA], 4)
        self.assertEqual(returned_data[6][DPColNames.SUM_POSTCODES_PER_LA], 4)
