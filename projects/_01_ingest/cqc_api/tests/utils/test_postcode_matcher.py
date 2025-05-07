import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.cqc_api.utils.postcode_matcher as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    PostcodeMatcherData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    PostcodeMatcherSchema as Schemas,
)
from utils import utils

PATCH_PATH: str = "projects._01_ingest.cqc_api.utils.postcode_matcher"


class PostcodeMatcherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

        self.locations_df = self.spark.createDataFrame(
            Data.locations_where_all_match_rows,
            Schemas.locations_schema,
        )
        self.postcodes_df = self.spark.createDataFrame(
            Data.postcodes_rows,
            Schemas.postcodes_schema,
        )


class MainTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.join_postcode_data")
    @patch(f"{PATCH_PATH}.cUtils.add_aligned_date_column")
    @patch(f"{PATCH_PATH}.clean_postcode_column")
    def test_main_calls_functions(
        self,
        clean_postcode_column_mock: Mock,
        add_aligned_date_column_mock: Mock,
        join_postcode_data_mock: Mock,
    ):
        join_postcode_data_mock.return_value = self.locations_df, self.locations_df

        job.run_postcode_matching(self.locations_df, self.postcodes_df)

        self.assertEqual(clean_postcode_column_mock.call_count, 2)
        add_aligned_date_column_mock.assert_called_once()
        join_postcode_data_mock.assert_called_once()
