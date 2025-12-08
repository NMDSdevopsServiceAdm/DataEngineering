import unittest

import projects._03_independent_cqc._02_clean.utils.forward_fill_latest_known_value as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    ForwardFillLatestKnownValue as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    ForwardFillLatestKnownValue as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class ForwardFillLatestKnownValueTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.days_to_repeat = 61

    def run_test(self, input_rows, expected_rows):
        test_df = self.spark.createDataFrame(input_rows, Schemas.locations_schema)

        returned_df = job.forward_fill_latest_known_value(
            df=test_df,
            col_to_repeat=IndCQC.ascwds_filled_posts_dedup_clean,
            days_to_repeat=self.days_to_repeat,
        )

        expected_df = self.spark.createDataFrame(
            expected_rows, Schemas.locations_schema
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_forward_fill_latest_known_value_when_latest_known_value_more_than_3_months_before_latest_import(
        self,
    ):
        self.run_test(
            Data.locations_when_latest_known_value_is_more_than_3_months_before_latest_import_rows,
            Data.expected_locations_when_latest_known_value_is_more_than_3_months_before_latest_import_rows,
        )

    def test_forward_fill_latest_known_value_when_latest_known_value_is_1_month_before_latest_import(
        self,
    ):
        self.run_test(
            Data.locations_when_latest_known_value_is_1_month_before_latest_import_rows,
            Data.expected_locations_when_latest_known_value_is_1_month_before_latest_import_rows,
        )

    def test_forward_fill_latest_known_value_when_latest_known_value_at_latest_import(
        self,
    ):
        self.run_test(
            Data.locations_when_latest_known_value_is_at_the_latest_import_rows,
            Data.expected_locations_when_latest_known_value_is_at_the_latest_import_rows,
        )

    def test_forward_fill_latest_known_value_when_dates_are_out_of_order(
        self,
    ):
        self.run_test(
            Data.locations_when_latest_known_value_is_more_than_3_months_before_latest_import_and_dates_are_out_of_order_rows,
            Data.expected_locations_when_latest_known_value_is_more_than_3_months_before_latest_import_and_dates_are_out_of_order_rows,
        )

    def test_forward_fill_latest_known_value_when_thirty_one_of_forward_fill(self):
        days_to_repeat = 31
        test_df = self.spark.createDataFrame(
            Data.input_one_month_window_rows,
            Schemas.locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_one_month_window_rows,
            Schemas.locations_schema,
        )
        returned_df = job.forward_fill_latest_known_value(
            test_df, IndCQC.ascwds_filled_posts_dedup_clean, days_to_repeat
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_forward_fill_latest_known_value_when_ninety_days_of_forward_fill(self):
        days_to_repeat = 90
        test_df = self.spark.createDataFrame(
            Data.input_ninety_day_window_rows,
            Schemas.locations_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_ninety_day_window_rows,
            Schemas.locations_schema,
        )
        returned_df = job.forward_fill_latest_known_value(
            test_df, IndCQC.ascwds_filled_posts_dedup_clean, days_to_repeat
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())
