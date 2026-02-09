import unittest
import warnings

from pyspark.sql import Window

import projects._03_independent_cqc.utils.utils.utils as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    IndCQCDataUtils as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    IndCQCDataUtils as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestIndCqcFilledPostUtils(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)


class TestMergeColumnsInOrder(TestIndCqcFilledPostUtils):
    def setUp(self) -> None:
        super().setUp()
        self.input_df = self.spark.createDataFrame(
            Data.merge_columns_in_order_when_double_type,
            Schemas.merge_columns_in_order_when_double_type_schema,
        )

        self.returned_df = job.merge_columns_in_order(
            self.input_df,
            ["model_name_1", "model_name_2", "model_name_3"],
            IndCQC.estimate_filled_posts,
            IndCQC.estimate_filled_posts_source,
        )

    def test_merge_columns_in_order_adds_expected_columns(self):
        self.assertIn(IndCQC.estimate_filled_posts, self.returned_df.columns)
        self.assertIn(IndCQC.estimate_filled_posts_source, self.returned_df.columns)

        cols_added = set(self.returned_df.columns) - set(self.input_df.columns)
        self.assertEqual(
            cols_added,
            {IndCQC.estimate_filled_posts, IndCQC.estimate_filled_posts_source},
        )

    def test_merge_columns_in_order_returns_expected_values_when_double_type(self):
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_columns_in_order_when_double_type,
            Schemas.expected_merge_columns_in_order_when_double_type_schema,
        )

        returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(self.returned_df.count(), expected_df.count())
        self.assertEqual(expected_data, returned_data)

    def test_merge_columns_in_order_returns_expected_values_when_map_type(self):
        test_df = self.spark.createDataFrame(
            Data.merge_columns_in_order_when_map_type,
            Schemas.merge_columns_in_order_when_map_type_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_columns_in_order_when_map_type,
            Schemas.expected_merge_columns_in_order_when_map_type_schema,
        )
        returned_df = job.merge_columns_in_order(
            test_df,
            Data.list_of_map_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_merge_columns_in_order_returns_null_when_all_columns_are_null(self):
        test_df = self.spark.createDataFrame(
            Data.merge_columns_in_order_when_all_null,
            Schemas.merge_columns_in_order_when_map_type_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_columns_in_order_when_all_null,
            Schemas.expected_merge_columns_in_order_when_map_type_schema,
        )
        returned_df = job.merge_columns_in_order(
            test_df,
            Data.list_of_map_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class GetSelectedValueFunctionTests(TestIndCqcFilledPostUtils):
    def setUp(self):
        super().setUp()
        self.w = (
            Window.partitionBy(IndCQC.location_id)
            .orderBy(IndCQC.unix_time)
            .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )

    def test_get_selected_value_returns_correct_values_when_selection_equals_first(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.test_first_selection_rows, Schemas.get_selected_value_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_test_first_selection_rows,
            Schemas.expected_get_selected_value_schema,
        )
        returned_df = job.get_selected_value(
            test_df,
            self.w,
            IndCQC.ascwds_filled_posts_dedup_clean,
            IndCQC.posts_rolling_average_model,
            "new_column",
            selection="first",
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id, IndCQC.unix_time).collect(),
            expected_df.collect(),
        )

    def test_get_selected_value_returns_correct_values_when_selection_equals_last(self):
        test_df = self.spark.createDataFrame(
            Data.test_last_selection_rows, Schemas.get_selected_value_schema
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_test_last_selection_rows,
            Schemas.expected_get_selected_value_schema,
        )
        returned_df = job.get_selected_value(
            test_df,
            self.w,
            IndCQC.ascwds_filled_posts_dedup_clean,
            IndCQC.posts_rolling_average_model,
            "new_column",
            selection="last",
        )
        self.assertEqual(
            returned_df.sort(IndCQC.location_id, IndCQC.unix_time).collect(),
            expected_df.collect(),
        )

    def test_get_selected_value_raises_error_when_selection_is_not_permitted(self):
        test_df = self.spark.createDataFrame(
            Data.test_last_selection_rows, Schemas.get_selected_value_schema
        )

        with self.assertRaises(ValueError) as context:
            job.get_selected_value(
                test_df,
                self.w,
                IndCQC.ascwds_filled_posts_dedup_clean,
                IndCQC.posts_rolling_average_model,
                "new_column",
                selection="other",
            )

        self.assertTrue(
            "Error: The selection parameter 'other' was not found. Please use 'first' or 'last'.",
            "Exception does not contain the correct error message",
        )
