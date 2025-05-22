import unittest
import warnings

from pyspark.sql import Window

from tests.test_file_data import IndCQCDataUtils as Data
from tests.test_file_schemas import IndCQCDataUtils as Schemas

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns as IndCQC,
)

import utils.ind_cqc_filled_posts_utils.utils as job


class TestIndCqcFilledPostUtils(unittest.TestCase):
    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)


class TestMergeColumnsInOrder(TestIndCqcFilledPostUtils):
    def setUp(self) -> None:
        super().setUp()
        self.input_df = self.spark.createDataFrame(
            Data.input_rows_for_adding_estimate_filled_posts_and_source,
            Schemas.input_schema_for_adding_estimate_filled_posts_and_source,
        )

        self.returned_df = job.merge_columns_in_order(
            self.input_df,
            ["model_name_1", "model_name_2", "model_name_3"],
            IndCQC.estimate_filled_posts,
            IndCQC.estimate_filled_posts_source,
        )

    def test_merge_columns_in_order_adds_new_columns(self):
        assert IndCQC.estimate_filled_posts in self.returned_df.columns
        assert IndCQC.estimate_filled_posts_source in self.returned_df.columns
        self.assertEqual(len(self.returned_df.columns), len(self.input_df.columns) + 2)

    def test_merge_columns_in_order_returns_expected_values(
        self,
    ):
        expected_df = self.spark.createDataFrame(
            Data.expected_rows_with_estimate_filled_posts_and_source,
            Schemas.expected_schema_with_estimate_filled_posts_and_source,
        )

        returned_data = self.returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.sort(IndCQC.location_id).collect()

        self.assertEqual(self.returned_df.count(), expected_df.count())
        self.assertEqual(expected_data, returned_data)

    def test_merge_columns_in_order_raises_error_when_given_list_of_columns_with_multiple_datatypes(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_columns_in_order_when_df_has_columns_of_multiple_datatypes,
            Schemas.merge_columns_in_order_when_df_has_columns_of_multiple_datatypes_schema,
        )

        list_of_columns_of_multiple_datatypes = [
            IndCQC.care_home_model,
            IndCQC.ascwds_job_role_ratios,
        ]
        column_types = list(
            set(
                [
                    test_df.schema[column].dataType
                    for column in list_of_columns_of_multiple_datatypes
                ]
            )
        )
        with self.assertRaises(ValueError) as context:
            job.merge_columns_in_order(
                test_df,
                list_of_columns_of_multiple_datatypes,
                "merged_column_name",
                "merged_column_source_name",
            )

        self.assertTrue(
            f"The columns to merge must all have the same datatype. Found {column_types}."
            in str(context.exception),
            "Exception does not contain the correct error message",
        )

    def test_merge_columns_in_order_raises_error_when_given_columns_with_datatype_string(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_columns_in_order_when_columns_are_datatype_string,
            Schemas.merge_columns_in_order_when_columns_are_datatype_string_schema,
        )

        list_of_columns_of_datatype_string = [
            IndCQC.ascwds_filled_posts_source,
            IndCQC.ascwds_job_role_ratios_merged_source,
        ]
        column_types = list(
            set(
                [
                    test_df.schema[column].dataType
                    for column in list_of_columns_of_datatype_string
                ]
            )
        )
        with self.assertRaises(ValueError) as context:
            job.merge_columns_in_order(
                test_df,
                list_of_columns_of_datatype_string,
                "merged_column_name",
                "merged_column_source_name",
            )

        self.assertTrue(
            f"Columns to merge must be either 'double' or 'map' type. Found {column_types}."
            in str(context.exception),
            "Exception does not contain the correct error message",
        )

    def test_merge_columns_in_order_returns_ascwds_map_when_only_ascwds_map_populated(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_map_columns_in_order_when_only_ascwds_known,
            Schemas.merge_columns_in_order_using_map_columns_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_map_columns_in_order_when_only_ascwds_known,
            Schemas.expected_merge_columns_in_order_using_map_columns_schema,
        )
        returned_df = job.merge_columns_in_order(
            test_df,
            Data.list_of_map_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_merge_columns_in_order_returns_primary_service_map_when_only_primary_service_map_populated(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_map_columns_in_order_when_only_primary_service_known,
            Schemas.merge_columns_in_order_using_map_columns_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_map_columns_in_order_when_only_primary_service_known,
            Schemas.expected_merge_columns_in_order_using_map_columns_schema,
        )
        returned_df = job.merge_columns_in_order(
            test_df,
            Data.list_of_map_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_merge_columns_in_order_returns_ascwds_map_when_both_map_columns_populated(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_map_columns_in_order_when_both_map_columns_populated,
            Schemas.merge_columns_in_order_using_map_columns_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_map_columns_in_order_when_both_map_columns_populated,
            Schemas.expected_merge_columns_in_order_using_map_columns_schema,
        )
        returned_df = job.merge_columns_in_order(
            test_df,
            Data.list_of_map_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_merge_columns_in_order_returns_null_when_both_map_columns_are_null(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_map_columns_in_order_when_both_null,
            Schemas.merge_columns_in_order_using_map_columns_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_map_columns_in_order_when_both_null,
            Schemas.expected_merge_columns_in_order_using_map_columns_schema,
        )
        returned_df = job.merge_columns_in_order(
            test_df,
            Data.list_of_map_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())

    def test_merge_columns_in_order_returns_ascwds_map_at_both_locations_when_both_map_columns_populated_at_both_locations(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.merge_map_columns_in_order_when_both_map_columns_populated_at_multiple_locations,
            Schemas.merge_columns_in_order_using_map_columns_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_merge_map_columns_in_order_when_both_map_columns_populated_at_multiple_locations,
            Schemas.expected_merge_columns_in_order_using_map_columns_schema,
        )
        returned_df = job.merge_columns_in_order(
            test_df,
            Data.list_of_map_columns_to_be_merged,
            IndCQC.ascwds_job_role_ratios_merged,
            IndCQC.ascwds_job_role_ratios_merged_source,
        )

        self.assertEqual(returned_df.collect(), expected_df.collect())


class TestSourceDescriptionAdded(TestIndCqcFilledPostUtils):
    def setUp(self) -> None:
        super().setUp()

    def test_add_source_description_added_to_source_column_when_required(self):
        input_df = self.spark.createDataFrame(
            Data.source_missing_rows, Schemas.estimated_source_description_schema
        )

        returned_df = job.add_source_description_to_source_column(
            input_df,
            IndCQC.estimate_filled_posts,
            IndCQC.estimate_filled_posts_source,
            "model_name",
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_source_added_rows, Schemas.estimated_source_description_schema
        )

        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.sort(IndCQC.location_id).collect()

        self.assertEqual(returned_df.count(), expected_df.count())
        self.assertEqual(expected_data, returned_data)


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


class CopyAndFillFilledPostsWhenBecomingNotDormant(TestIndCqcFilledPostUtils):
    def setUp(self):
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.copy_and_fill_filled_posts_when_becoming_not_dormant_rows,
            Schemas.copy_and_fill_filled_posts_when_becoming_not_dormant_schema,
        )
        self.returned_df = job.copy_and_fill_filled_posts_when_becoming_not_dormant(
            self.test_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_copy_and_fill_filled_posts_when_becoming_not_dormant_rows,
            Schemas.expected_copy_and_fill_filled_posts_when_becoming_not_dormant_schema,
        )

        self.columns_added_by_function = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

    def test_copy_and_fill_filled_posts_when_becoming_not_dormant_adds_1_expected_column(
        self,
    ):
        self.assertEqual(len(self.columns_added_by_function), 1)
        self.assertEqual(
            self.columns_added_by_function[0],
            IndCQC.estimate_filled_posts_at_point_of_becoming_non_dormant,
        )

    def test_copy_and_fill_filled_posts_when_becoming_not_dormant_returns_expected_values(
        self,
    ):
        returned_data = self.returned_df.sort(
            [IndCQC.location_id, IndCQC.cqc_location_import_date]
        ).collect()
        expected_data = self.expected_df.sort(
            [IndCQC.location_id, IndCQC.cqc_location_import_date]
        ).collect()

        self.assertEqual(returned_data, expected_data)


class OverwriteEstimateFilledPostsWithImputedEstimatedFilledPostsAtPointOfBecomingNonDormant(
    TestIndCqcFilledPostUtils
):
    def setUp(self):
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.overwrite_estimate_filled_posts_with_imputed_rows,
            Schemas.overwrite_estimate_filled_posts_with_imputed_schema,
        )
        self.returned_df = job.overwrite_estimate_filled_posts_with_imputed_estimated_filled_posts_at_point_of_becoming_non_dormant(
            self.test_df
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_overwrite_estimate_filled_posts_with_imputed_rows,
            Schemas.overwrite_estimate_filled_posts_with_imputed_schema,
        )

        self.columns_added_by_function = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

    def test_overwrite_estimate_filled_posts_with_imputed_returns_expected_values(
        self,
    ):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)
