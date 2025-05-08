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
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)

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


class CleanPostcodeColumnTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.clean_postcode_column_rows, Schemas.clean_postcode_column_schema
        )
        self.returned_df = job.clean_postcode_column(
            self.test_df,
            CQCLClean.postal_code,
            CQCLClean.postcode_cleaned,
            drop_col=False,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_clean_postcode_column_rows,
            Schemas.expected_clean_postcode_column_when_col_not_dropped_schema,
        )
        self.returned_data = self.returned_df.sort(CQCLClean.postal_code).collect()
        self.expected_data = self.expected_df.collect()

    def test_clean_postcode_column_returns_expected_values(self):
        self.assertEqual(self.returned_data, self.expected_data)

    def test_clean_postcode_column_returns_expected_columns_when_column_is_not_dropped(
        self,
    ):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_clean_postcode_column_returns_expected_columns_when_column_is_dropped(
        self,
    ):
        returned_when_col_dropped_df = job.clean_postcode_column(
            self.test_df,
            CQCLClean.postal_code,
            CQCLClean.postcode_cleaned,
            drop_col=True,
        )
        expected_when_col_dropped_df = self.spark.createDataFrame(
            [], Schemas.expected_clean_postcode_column_when_col_is_dropped_schema
        )
        self.assertEqual(
            returned_when_col_dropped_df.columns, expected_when_col_dropped_df.columns
        )


class JoinPostcodeDataTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

        # locations_df = self.spark.createDataFrame(
        #     Data.join_postcode_data_locations_rows, Schemas.
        # )
        # postcode_df = self.spark.createDataFrame(
        #     Data.join_postcode_data_postcodes_rows, Schemas.
        # )
        # returned_matched_df, returned_unmatched_df = job.join_postcode_data(
        #     locations_df, postcode_df, CQCLClean.postcode_cleaned
        # )
        # returned_matched_df = self.spark.createDataFrame(
        #     Data.expected_join_postcode_data_matched_rows, Schemas.
        # )
        # returned_unmatched_df = self.spark.createDataFrame(
        #     Data.expected_join_postcode_data_unmatched_rows, Schemas.
        # )
