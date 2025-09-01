import unittest
import warnings
from datetime import date
from unittest.mock import ANY, Mock, patch

from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    DateType,
)

import projects._03_independent_cqc._02_clean.jobs.clean_ind_cqc_filled_posts as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    CleanIndCQCData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    CleanIndCQCData as Schemas,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    IndCqcColumns as IndCQC,
)


PATCH_PATH = "projects._03_independent_cqc._02_clean.jobs.clean_ind_cqc_filled_posts"


class CleanIndFilledPostsTests(unittest.TestCase):
    MERGE_IND_CQC_SOURCE = "input_dir"
    CLEANED_IND_CQC_DESTINATION = "output_dir"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]

    def setUp(self):
        self.spark = utils.get_spark()
        self.merge_ind_cqc_test_df = self.spark.createDataFrame(
            Data.merged_rows_for_cleaning_job,
            Schemas.merged_schema_for_cleaning_job,
        )
        warnings.filterwarnings("ignore", category=ResourceWarning)


class MainTests(CleanIndFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.clean_ascwds_filled_post_outliers")
    @patch(f"{PATCH_PATH}.cUtils.create_banded_bed_count_column")
    @patch(f"{PATCH_PATH}.cUtils.calculate_filled_posts_per_bed_ratio")
    @patch(f"{PATCH_PATH}.create_column_with_repeated_values_removed")
    @patch(f"{PATCH_PATH}.calculate_ascwds_filled_posts")
    @patch(f"{PATCH_PATH}.populate_missing_care_home_number_of_beds")
    @patch(f"{PATCH_PATH}.replace_zero_beds_with_null")
    @patch(f"{PATCH_PATH}.remove_duplicate_cqc_care_homes")
    @patch(f"{PATCH_PATH}.calculate_time_registered_for")
    @patch(f"{PATCH_PATH}.calculate_time_since_dormant")
    @patch(f"{PATCH_PATH}.cUtils.reduce_dataset_to_earliest_file_per_month")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main(
        self,
        read_from_parquet_mock: Mock,
        reduce_dataset_to_earliest_file_per_month_mock: Mock,
        calculate_time_since_dormant_mock: Mock,
        calculate_time_registered_for_mock: Mock,
        remove_duplicate_cqc_care_homes_mock: Mock,
        replace_zero_beds_with_null_mock: Mock,
        populate_missing_care_home_number_of_beds_mock: Mock,
        calculate_ascwds_filled_posts_mock: Mock,
        create_column_with_repeated_values_removed_mock: Mock,
        calculate_filled_posts_per_bed_ratio_mock: Mock,
        create_banded_bed_count_column_mock: Mock,
        clean_ascwds_filled_post_outliers_mock: Mock,
        write_to_parquet_mock: Mock,
    ):
        read_from_parquet_mock.return_value = self.merge_ind_cqc_test_df

        job.main(
            self.MERGE_IND_CQC_SOURCE,
            self.CLEANED_IND_CQC_DESTINATION,
        )

        reduce_dataset_to_earliest_file_per_month_mock.assert_called_once()
        calculate_time_registered_for_mock.assert_called_once()
        calculate_time_since_dormant_mock.assert_called_once()
        remove_duplicate_cqc_care_homes_mock.assert_called_once()
        replace_zero_beds_with_null_mock.assert_called_once()
        populate_missing_care_home_number_of_beds_mock.assert_called_once()
        calculate_ascwds_filled_posts_mock.assert_called_once()
        self.assertEqual(create_column_with_repeated_values_removed_mock.call_count, 4)
        self.assertEqual(calculate_filled_posts_per_bed_ratio_mock.call_count, 2)
        create_banded_bed_count_column_mock.assert_called_once()
        clean_ascwds_filled_post_outliers_mock.assert_called_once()

        write_to_parquet_mock.assert_called_once_with(
            ANY,
            self.CLEANED_IND_CQC_DESTINATION,
            mode=ANY,
            partitionKeys=self.partition_keys,
        )

    def test_replace_zero_beds_with_null(self):
        columns = [
            IndCQC.location_id,
            IndCQC.number_of_beds,
        ]
        rows = [
            ("1-000000001", None),
            ("1-000000002", 0),
            ("1-000000003", 1),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.replace_zero_beds_with_null(df)
        self.assertEqual(df.count(), 3)

        df = df.collect()
        self.assertEqual(df[0][IndCQC.number_of_beds], None)
        self.assertEqual(df[1][IndCQC.number_of_beds], None)
        self.assertEqual(df[2][IndCQC.number_of_beds], 1)

    def test_populate_missing_care_home_number_of_beds(self):
        schema = StructType(
            [
                StructField(IndCQC.location_id, StringType(), True),
                StructField(IndCQC.cqc_location_import_date, DateType(), True),
                StructField(IndCQC.care_home, StringType(), True),
                StructField(IndCQC.number_of_beds, IntegerType(), True),
            ]
        )

        input_rows = [
            ("1-000000001", date(2023, 1, 1), "Y", None),
            ("1-000000002", date(2023, 1, 1), "N", None),
            ("1-000000003", date(2023, 1, 1), "Y", 1),
            ("1-000000003", date(2023, 2, 1), "Y", None),
            ("1-000000003", date(2023, 3, 1), "Y", 1),
            ("1-000000004", date(2023, 1, 1), "Y", 1),
            ("1-000000004", date(2023, 2, 1), "Y", 3),
        ]
        input_df = self.spark.createDataFrame(input_rows, schema=schema)

        df = job.populate_missing_care_home_number_of_beds(input_df)
        self.assertEqual(df.count(), 7)

        df = df.sort(IndCQC.location_id, IndCQC.cqc_location_import_date).collect()
        self.assertEqual(df[0][IndCQC.number_of_beds], None)
        self.assertEqual(df[1][IndCQC.number_of_beds], None)
        self.assertEqual(df[2][IndCQC.number_of_beds], 1)
        self.assertEqual(df[3][IndCQC.number_of_beds], 1)
        self.assertEqual(df[4][IndCQC.number_of_beds], 1)
        self.assertEqual(df[5][IndCQC.number_of_beds], 1)
        self.assertEqual(df[6][IndCQC.number_of_beds], 3)

    def test_filter_to_care_homes_with_known_beds(self):
        columns = [
            IndCQC.location_id,
            IndCQC.care_home,
            IndCQC.number_of_beds,
        ]
        rows = [
            ("1-000000001", "Y", None),
            ("1-000000002", "N", None),
            ("1-000000003", "Y", 1),
            ("1-000000004", "N", 1),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.filter_to_care_homes_with_known_beds(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0][IndCQC.location_id], "1-000000003")

    def test_average_beds_per_location(self):
        columns = [
            IndCQC.location_id,
            IndCQC.number_of_beds,
        ]
        rows = [
            ("1-000000001", 1),
            ("1-000000002", 2),
            ("1-000000002", 3),
            ("1-000000003", 2),
            ("1-000000003", 3),
            ("1-000000003", 4),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.average_beds_per_location(df)
        self.assertEqual(df.count(), 3)

        df = df.sort(IndCQC.location_id).collect()
        self.assertEqual(df[0][job.average_number_of_beds], 1)
        self.assertEqual(df[1][job.average_number_of_beds], 2)
        self.assertEqual(df[2][job.average_number_of_beds], 3)

    def test_replace_null_beds_with_average(self):
        columns = [
            IndCQC.location_id,
            IndCQC.number_of_beds,
            job.average_number_of_beds,
        ]
        rows = [
            ("1-000000001", None, None),
            ("1-000000002", None, 1),
            ("1-000000003", 2, 2),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.replace_null_beds_with_average(df)
        self.assertEqual(df.count(), 3)

        df = df.collect()
        self.assertEqual(df[0][IndCQC.number_of_beds], None)
        self.assertEqual(df[1][IndCQC.number_of_beds], 1)
        self.assertEqual(df[2][IndCQC.number_of_beds], 2)

    def test_replace_null_beds_with_average_doesnt_change_known_beds(self):
        columns = [
            IndCQC.location_id,
            IndCQC.number_of_beds,
            job.average_number_of_beds,
        ]
        rows = [
            ("1-000000001", 1, 2),
        ]
        df = self.spark.createDataFrame(rows, columns)

        df = job.replace_null_beds_with_average(df)
        self.assertEqual(df.count(), 1)

        df = df.collect()
        self.assertEqual(df[0][IndCQC.number_of_beds], 1)

class CalculateTimeRegisteredForTests(CleanIndFilledPostsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_calculate_time_registered_returns_one_when_dates_are_on_the_same_day(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_same_day_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_same_day_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_exact_months_apart(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_exact_months_apart_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_exact_months_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_one_day_less_than_a_full_month_apart(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_one_day_less_than_a_full_month_apart_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_one_day_less_than_a_full_month_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_calculate_time_registered_returns_expected_values_when_dates_are_one_day_more_than_a_full_month_apart(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.calculate_time_registered_one_day_more_than_a_full_month_apart_rows,
            Schemas.calculate_time_registered_for_schema,
        )
        returned_df = job.calculate_time_registered_for(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_registered_one_day_more_than_a_full_month_apart_rows,
            Schemas.expected_calculate_time_registered_for_schema,
        )
        returned_data = returned_df.sort(IndCQC.location_id).collect()
        expected_data = expected_df.collect()

        self.assertEqual(returned_data, expected_data)


class CalculateTimeSinceDormant(CleanIndFilledPostsTests):
    def setUp(self):
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.calculate_time_since_dormant_rows,
            Schemas.calculate_time_since_dormant_schema,
        )
        self.returned_df = job.calculate_time_since_dormant(self.test_df)
        self.expected_df = self.spark.createDataFrame(
            Data.expected_calculate_time_since_dormant_rows,
            Schemas.expected_calculate_time_since_dormant_schema,
        )

        self.columns_added_by_function = [
            column
            for column in self.returned_df.columns
            if column not in self.test_df.columns
        ]

    def test_calculate_time_since_dormant_returns_new_column(self):
        self.assertEqual(len(self.columns_added_by_function), 1)
        self.assertEqual(self.columns_added_by_function[0], IndCQC.time_since_dormant)

    def test_calculate_time_since_dormant_returns_expected_values(self):
        returned_data = self.returned_df.sort(
            IndCQC.cqc_location_import_date
        ).collect()
        expected_data = self.expected_df.collect()

        self.assertEqual(returned_data, expected_data)


class RemoveDuplicateCqcCareHomesTests(CleanIndFilledPostsTests):
    def setUp(self):
        super().setUp()

    def test_remove_duplicate_cqc_care_homes_returns_expected_values_when_carehome_and_asc_data_populated(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.remove_cqc_duplicates_when_carehome_and_asc_data_populated_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_cqc_duplicates_when_carehome_and_asc_data_populated_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        returned_df = job.remove_duplicate_cqc_care_homes(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_remove_duplicate_cqc_care_homes_returns_expected_values_when_carehome_and_asc_data_missing_on_earlier_reg_date(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_earlier_reg_date_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_earlier_reg_date_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        returned_df = job.remove_duplicate_cqc_care_homes(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_remove_duplicate_cqc_care_homes_returns_expected_values_when_carehome_and_asc_data_missing_on_later_reg_date(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_later_reg_date_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_later_reg_date_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        returned_df = job.remove_duplicate_cqc_care_homes(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_remove_duplicate_cqc_care_homes_returns_expected_values_when_carehome_and_asc_data_missing_on_all_reg_dates(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_all_reg_dates_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_cqc_duplicates_when_carehome_and_asc_data_missing_on_all_reg_dates_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        returned_df = job.remove_duplicate_cqc_care_homes(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_remove_duplicate_cqc_care_homes_returns_expected_values_when_carehome_and_asc_data_different_on_all_reg_dates(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.remove_cqc_duplicates_when_carehome_and_asc_data_different_on_all_reg_dates_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_cqc_duplicates_when_carehome_and_asc_data_different_on_all_reg_dates_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        returned_df = job.remove_duplicate_cqc_care_homes(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_remove_duplicate_cqc_care_homes_returns_expected_values_when_carehome_and_registration_dates_the_same(
        self,
    ):
        test_df = self.spark.createDataFrame(
            Data.remove_cqc_duplicates_when_carehome_and_registration_dates_the_same_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_cqc_duplicates_when_carehome_and_registration_dates_the_same_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        returned_df = job.remove_duplicate_cqc_care_homes(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )

    def test_remove_duplicate_cqc_care_homes_returns_expected_values_when_non_res(self):
        test_df = self.spark.createDataFrame(
            Data.remove_cqc_duplicates_when_non_res_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_remove_cqc_duplicates_when_non_res_rows,
            Schemas.remove_cqc_duplicates_schema,
        )
        returned_df = job.remove_duplicate_cqc_care_homes(test_df)
        self.assertEqual(
            returned_df.sort(IndCQC.location_id).collect(), expected_df.collect()
        )


class AddColumnWithRepeatedValuesRemovedTests(CleanIndFilledPostsTests):
    def setUp(self):
        super().setUp()
        self.test_purge_outdated_df = self.spark.createDataFrame(
            Data.repeated_value_rows, Schemas.repeated_value_schema
        )
        self.expected_df_without_repeated_values_df = self.spark.createDataFrame(
            Data.expected_without_repeated_values_rows,
            Schemas.expected_without_repeated_values_schema,
        )
        self.returned_df = job.create_column_with_repeated_values_removed(
            self.test_purge_outdated_df,
            column_to_clean="integer_column",
        )
        self.OUTPUT_COLUMN = "integer_column_deduplicated"

        self.returned_data = self.returned_df.sort(
            IndCQC.location_id, IndCQC.cqc_location_import_date
        ).collect()
        self.expected_data = self.expected_df_without_repeated_values_df.sort(
            IndCQC.location_id, IndCQC.cqc_location_import_date
        ).collect()

    def test_first_submitted_value_is_included_in_new_column(self):
        self.assertEqual(
            self.returned_data[0][self.OUTPUT_COLUMN],
            self.expected_data[0][self.OUTPUT_COLUMN],
        )
        self.assertEqual(
            self.returned_data[4][self.OUTPUT_COLUMN],
            self.expected_data[4][self.OUTPUT_COLUMN],
        )

    def test_submitted_value_is_included_if_it_wasnt_repeated(self):
        self.assertEqual(
            self.returned_data[1][self.OUTPUT_COLUMN],
            self.expected_data[1][self.OUTPUT_COLUMN],
        )
        self.assertEqual(
            self.returned_data[5][self.OUTPUT_COLUMN],
            self.expected_data[5][self.OUTPUT_COLUMN],
        )

    def test_repeated_value_entered_as_null_value(self):
        self.assertEqual(
            self.returned_data[2][self.OUTPUT_COLUMN],
            self.expected_data[2][self.OUTPUT_COLUMN],
        )
        self.assertEqual(
            self.returned_data[7][self.OUTPUT_COLUMN],
            self.expected_data[7][self.OUTPUT_COLUMN],
        )

    def test_value_which_has_appeared_before_but_isnt_a_repeat_is_included(self):
        self.assertEqual(
            self.returned_data[6][self.OUTPUT_COLUMN],
            self.expected_data[6][self.OUTPUT_COLUMN],
        )

    def test_returned_df_matches_expected_df(self):
        self.assertEqual(
            self.returned_data,
            self.expected_data,
        )

    def test_returned_df_has_one_additional_column(self):
        self.assertEqual(
            len(self.returned_df.columns), len(self.test_purge_outdated_df.columns) + 1
        )

    def test_returned_df_has_same_number_of_rows(self):
        self.assertEqual(self.returned_df.count(), self.test_purge_outdated_df.count())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
