import unittest

import projects._03_independent_cqc._02_clean.utils.forward_fill_latest_recorded_value as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    DuplicateLatestKnownAscwdsValueIntoFollowingTwoImportDates as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    DuplicateLatestKnownAscwdsValueIntoFollowingTwoImportDates as Schemas,
)
from utils import utils

class TestRepeatLastKnownValue(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.days_to_repeat = 61

    def run_test(self, input_rows, expected_rows):
        test_df = self.spark.createDataFrame(input_rows, Schemas.locations_schema)

        returned_df = job.repeat_last_known_value(
            df=test_df,
            col_to_repeat="ascwds_filled_posts_deduplicated_clean",
            days_to_repeat=self.days_to_repeat,
        )

        expected_df = self.spark.createDataFrame(
            expected_rows, Schemas.locations_schema
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_latest_known_value_more_than_3_months_before_latest_import(
        self,
    ):
        self.run_test(
            Data.locations_when_latest_known_value_is_more_than_3_months_before_latest_import_rows,
            Data.expected_locations_when_latest_known_value_is_more_than_3_months_before_latest_import_rows,
        )

    def test_latest_known_value_is_1_month_before_latest_import(
        self,
    ):
        self.run_test(
            Data.locations_when_latest_known_value_is_1_month_before_latest_import_rows,
            Data.expected_locations_when_latest_known_value_is_1_month_before_latest_import_rows,
        )

    def test_latest_known_value_at_latest_import(
        self,
    ):
        self.run_test(
            Data.locations_when_latest_known_value_is_at_the_latest_import_rows,
            Data.expected_locations_when_latest_known_value_is_at_the_latest_import_rows,
        )

    def test_dates_are_out_of_order(
        self,
    ):
        self.run_test(
            Data.locations_when_latest_known_value_is_more_than_3_months_before_latest_import_and_dates_are_out_of_order_rows,
            Data.expected_locations_when_latest_known_value_is_more_than_3_months_before_latest_import_and_dates_are_out_of_order_rows,
        )

    def test_only_one_month_of_forward_fill(self):
        days_to_repeat = 31
        test_df = self.spark.createDataFrame(
            Data.input_one_month_window_rows,
            Schemas.locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_one_month_window_rows,
            Schemas.locations_schema,
        )
        returned_df = job.repeat_last_known_value(
            test_df, "ascwds_filled_posts_deduplicated_clean", days_to_repeat
        )
        print(returned_df.collect())
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_ninety_days_of_forward_fill(self):
        days_to_repeat = 90
        test_df = self.spark.createDataFrame(
            Data.input_ninety_day_window_rows,
            Schemas.locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_ninety_day_window_rows,
            Schemas.locations_schema,
        )
        returned_df = job.repeat_last_known_value(
            test_df, "ascwds_filled_posts_deduplicated_clean", days_to_repeat
        )
        print(returned_df.collect())

        self.assertEqual(returned_df.collect(), expected_df.collect())
