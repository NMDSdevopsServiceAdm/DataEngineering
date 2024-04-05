import unittest
import warnings
from datetime import date
from unittest.mock import Mock, patch
from pyspark.sql import functions as F

import jobs.reconciliation as job
from utils import utils

from tests.test_file_data import ReconciliationData as Data
from tests.test_file_schemas import ReconciliationSchema as Schemas
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)


class ReconciliationTests(unittest.TestCase):
    TEST_CQC_LOCATION_API_SOURCE = "some/source"
    TEST_ASCWDS_WORKPLACE_SOURCE = "another/source"
    TEST_SINGLE_SUB_DESTINATION = "some/destination"
    TEST_PARENT_DESTINATION = "another/destination"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_location_api_df = self.spark.createDataFrame(
            Data.input_cqc_location_api_rows,
            Schemas.input_cqc_location_api_schema,
        )
        self.test_clean_ascwds_workplace_df = self.spark.createDataFrame(
            Data.input_ascwds_workplace_rows,
            Schemas.input_ascwds_workplace_schema,
        )

        warnings.simplefilter("ignore", ResourceWarning)


class MainTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    @patch("utils.utils.read_from_parquet")
    def test_main_run(
        self,
        read_from_parquet_patch: Mock,
    ):
        read_from_parquet_patch.side_effect = [
            self.test_cqc_location_api_df,
            self.test_clean_ascwds_workplace_df,
        ]

        job.main(
            self.TEST_CQC_LOCATION_API_SOURCE,
            self.TEST_ASCWDS_WORKPLACE_SOURCE,
            self.TEST_SINGLE_SUB_DESTINATION,
            self.TEST_PARENT_DESTINATION,
        )

        self.assertEqual(read_from_parquet_patch.call_count, 2)


class PrepareMostRecentCqcLocationDataTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    def test_prepare_most_recent_cqc_location_df_returns_expected_dataframe(self):
        returned_df = job.prepare_most_recent_cqc_location_df(
            self.test_cqc_location_api_df
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_prepared_most_recent_cqc_location_rows,
            Schemas.expected_prepared_most_recent_cqc_location_schema,
        )
        returned_data = returned_df.sort(CQCL.location_id).collect()
        expected_data = expected_df.sort(CQCL.location_id).collect()

        self.assertEqual(returned_data, expected_data)


class CollectDatesToUseTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()

    def test_collect_dates_to_use_return_correct_value(self):
        df = self.spark.createDataFrame(
            Data.dates_to_use_rows, Schemas.dates_to_use_schema
        )
        (
            first_of_most_recent_month,
            first_of_previous_month,
        ) = job.collect_dates_to_use(df)

        self.assertEqual(first_of_most_recent_month, date(2024, 3, 1))
        self.assertEqual(first_of_previous_month, date(2024, 2, 1))


class RemoveASCWDSHeadOfficeAccountsWithoutLocationIdsTests(ReconciliationTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_remove_head_office_accounts_df = self.spark.createDataFrame(
            Data.remove_head_office_accounts_rows,
            Schemas.remove_head_office_accounts_schema,
        )
        self.returned_df = job.remove_ascwds_head_office_accounts_without_location_ids(
            self.test_remove_head_office_accounts_df,
        )
        self.returned_locations = (
            self.returned_df.select(CQCL.location_id).rdd.flatMap(lambda x: x).collect()
        )

    def test_remove_head_office_accounts_when_registration_status_is_null_and_main_service_id_is_seventy_two(
        self,
    ):
        self.assertFalse("loc_1" in self.returned_locations)

    def test_keep_head_office_accounts_when_registration_status_is_null_and_main_service_id_is_not_seventy_two(
        self,
    ):
        self.assertTrue("loc_2" in self.returned_locations)

    def test_keep_head_office_accounts_when_registration_status_is_not_null(self):
        self.assertTrue("loc_3" in self.returned_locations)
        self.assertTrue("loc_4" in self.returned_locations)
        self.assertTrue("loc_5" in self.returned_locations)
        self.assertTrue("loc_6" in self.returned_locations)
