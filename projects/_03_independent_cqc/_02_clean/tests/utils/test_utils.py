import projects._03_independent_cqc._02_clean.utils.utils as job
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_data import (
    CleanIndCQCData as Data,
)
from projects._03_independent_cqc.unittest_data.ind_cqc_test_file_schemas import (
    CleanIndCQCData as Schemas,
)
from tests.base_test import SparkBaseTest
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class AddColumnWithRepeatedValuesRemovedTests(SparkBaseTest):
    def setUp(self):
        self.test_purge_outdated_df = self.spark.createDataFrame(
            Data.repeated_value_rows, Schemas.repeated_value_schema
        )
        self.expected_df_without_repeated_values = self.spark.createDataFrame(
            Data.expected_without_repeated_values_rows,
            Schemas.expected_without_repeated_values_schema,
        )
        self.returned_df_per_locationid = (
            job.create_column_with_repeated_values_removed(
                self.test_purge_outdated_df,
                column_to_clean="integer_column",
            )
        )
        self.returned_df_per_providerid = (
            job.create_column_with_repeated_values_removed(
                self.test_purge_outdated_df,
                column_to_clean="integer_column",
                column_to_partition_by=IndCQC.provider_id,
            )
        )
        self.OUTPUT_COLUMN = "integer_column_deduplicated"

        self.returned_data_per_locationid = self.returned_df_per_locationid.sort(
            IndCQC.location_id, IndCQC.cqc_location_import_date
        ).collect()
        self.returned_data_per_providerid = self.returned_df_per_providerid.sort(
            IndCQC.location_id, IndCQC.cqc_location_import_date
        ).collect()
        self.expected_data = self.expected_df_without_repeated_values.sort(
            IndCQC.location_id, IndCQC.cqc_location_import_date
        ).collect()

    def test_first_submitted_value_is_included_in_new_column(self):
        self.assertEqual(
            self.returned_data_per_locationid[0][self.OUTPUT_COLUMN],
            self.expected_data[0][self.OUTPUT_COLUMN],
        )
        self.assertEqual(
            self.returned_data_per_locationid[4][self.OUTPUT_COLUMN],
            self.expected_data[4][self.OUTPUT_COLUMN],
        )

    def test_submitted_value_is_included_if_it_wasnt_repeated(self):
        self.assertEqual(
            self.returned_data_per_locationid[1][self.OUTPUT_COLUMN],
            self.expected_data[1][self.OUTPUT_COLUMN],
        )
        self.assertEqual(
            self.returned_data_per_locationid[5][self.OUTPUT_COLUMN],
            self.expected_data[5][self.OUTPUT_COLUMN],
        )

    def test_repeated_value_entered_as_null_value(self):
        self.assertEqual(
            self.returned_data_per_locationid[2][self.OUTPUT_COLUMN],
            self.expected_data[2][self.OUTPUT_COLUMN],
        )
        self.assertEqual(
            self.returned_data_per_locationid[7][self.OUTPUT_COLUMN],
            self.expected_data[7][self.OUTPUT_COLUMN],
        )

    def test_value_which_has_appeared_before_but_isnt_a_repeat_is_included(self):
        self.assertEqual(
            self.returned_data_per_locationid[6][self.OUTPUT_COLUMN],
            self.expected_data[6][self.OUTPUT_COLUMN],
        )

    def test_returned_df_matches_expected_df(self):
        self.assertEqual(
            self.returned_data_per_locationid,
            self.expected_data,
        )

    def test_returned_df_has_one_additional_column(self):
        self.assertEqual(
            len(self.returned_df_per_locationid.columns),
            len(self.test_purge_outdated_df.columns) + 1,
        )

    def test_returned_df_has_same_number_of_rows(self):
        self.assertEqual(
            self.returned_df_per_locationid.count(), self.test_purge_outdated_df.count()
        )
        self.assertEqual(
            self.returned_df_per_locationid.count(), self.test_purge_outdated_df.count()
        )

    def test_returned_df_matches_expected_df_when_partitioned_by_providerid(self):
        self.assertEqual(
            self.returned_data_per_providerid,
            self.expected_data,
        )
