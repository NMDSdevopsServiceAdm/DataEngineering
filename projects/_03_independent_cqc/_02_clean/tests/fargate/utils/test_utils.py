import unittest
import polars as pl
import polars.testing as pl_testing
import projects._03_independent_cqc._02_clean.fargate.utils.utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CleanUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CleanUtilsSchemas as Schemas,
)

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class CreateColumnWithRepeatedValuesRemovedTests(unittest.TestCase):
    def setUp(self):
        self.test_purge_outdated_lf = pl.LazyFrame(
            data=Data.repeated_value_rows,
            schema=Schemas.repeated_value_schema,
            orient="row",
        )
        self.expected_lf_without_repeated_values = pl.LazyFrame(
            data=Data.expected_without_repeated_values_rows,
            schema=Schemas.expected_without_repeated_values_schema,
            orient="row",
        )
        self.returned_lf_per_locationid = (
            job.create_column_with_repeated_values_removed(
                self.test_purge_outdated_lf,
                column_to_clean="integer_column",
            )
        )
        self.returned_lf_per_providerid = (
            job.create_column_with_repeated_values_removed(
                self.test_purge_outdated_lf,
                column_to_clean="integer_column",
                column_to_partition_by=IndCQC.provider_id,
            )
        )
        self.OUTPUT_COLUMN = "integer_column_deduplicated"

        self.returned_data_per_locationid = self.returned_lf_per_locationid.sort(
            IndCQC.location_id, IndCQC.cqc_location_import_date
        ).collect()
        self.returned_data_per_providerid = self.returned_lf_per_providerid.sort(
            IndCQC.location_id, IndCQC.cqc_location_import_date
        ).collect()
        self.expected_data = self.expected_lf_without_repeated_values.sort(
            IndCQC.location_id, IndCQC.cqc_location_import_date
        ).collect()

    def test_create_column_with_repeated_values_removed_when_partitioned_by_location_id(
        self,
    ):
        pl_testing.assert_series_equal(
            self.returned_data_per_locationid.get_column(self.OUTPUT_COLUMN),
            self.expected_data.get_column(self.OUTPUT_COLUMN),
        )

    def test_returned_df_matches_expected_df_when_partitioned_by_location_id(self):
        pl_testing.assert_frame_equal(
            self.returned_data_per_locationid,
            self.expected_data,
        )

    def test_returned_df_has_one_additional_column_when_partitioned_by_location_id(
        self,
    ):
        self.assertIn(self.OUTPUT_COLUMN, self.returned_lf_per_locationid.columns)

        cols_added = set(self.returned_lf_per_locationid.columns) - set(
            self.test_purge_outdated_lf.columns
        )
        self.assertEqual(cols_added, {self.OUTPUT_COLUMN})

    def test_returned_df_has_same_number_of_rows(self):
        self.assertEqual(
            self.returned_lf_per_locationid.collect().height,
            self.test_purge_outdated_lf.collect().height,
        )

    def test_returned_df_matches_expected_df_when_partitioned_by_providerid(self):
        pl_testing.assert_frame_equal(
            self.returned_data_per_providerid,
            self.expected_data,
        )
