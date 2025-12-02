import unittest

import projects._03_independent_cqc._02_clean.utils.duplicate_latest_known_ascwds_value_into_following_two_import_dates as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    DuplicateLatestKnownAscwdsValueIntoFollowingTwoImportDates as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    DuplicateLatestKnownAscwdsValueIntoFollowingTwoImportDates as Schemas,
)
from utils import utils


class DuplicateLatestKnownAscwdsValueIntoFollowingTwoImportDates(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

    def test_latest_known_value_is_copied_into_only_following_two_import_dates_when_latest_known_value_is_more_than_3_months_before_latest_import(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.locations_when_latest_known_value_is_more_than_3_months_before_latest_import_rows,
            Schemas.locations_schema,
        )
        returned_df = (
            job.duplicate_latest_known_ascwds_value_into_following_two_import_dates(
                test_df
            )
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_locations_when_latest_known_value_is_more_than_3_months_before_latest_import_rows,
            Schemas.locations_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_latest_known_value_is_copied_into_following_import_date_when_last_known_value_is_1_month_before_latest_import(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.locations_when_latest_known_value_is_1_month_before_latest_import_rows,
            Schemas.locations_schema,
        )
        returned_df = (
            job.duplicate_latest_known_ascwds_value_into_following_two_import_dates(
                test_df
            )
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_locations_when_latest_known_value_is_1_month_before_latest_import_rows,
            Schemas.locations_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_input_data_is_returned_without_change_when_latest_known_value_is_at_the_latest_import(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.locations_when_latest_known_value_is_at_the_latest_import_rows,
            Schemas.locations_schema,
        )
        returned_df = (
            job.duplicate_latest_known_ascwds_value_into_following_two_import_dates(
                test_df
            )
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_locations_when_latest_known_value_is_at_the_latest_import_rows,
            Schemas.locations_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_latest_known_value_is_copied_into_only_following_two_import_dates_when_latest_known_value_is_more_than_3_months_before_latest_import_and_dates_are_out_of_order(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.locations_when_latest_known_value_is_more_than_3_months_before_latest_import_and_dates_are_out_of_order_rows,
            Schemas.locations_schema,
        )
        returned_df = (
            job.duplicate_latest_known_ascwds_value_into_following_two_import_dates(
                test_df
            )
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_locations_when_latest_known_value_is_more_than_3_months_before_latest_import_and_dates_are_out_of_order_rows,
            Schemas.locations_schema,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())
