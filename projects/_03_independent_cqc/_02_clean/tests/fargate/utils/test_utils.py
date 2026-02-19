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
    def test_create_column_with_repeated_values_removed_returns_correct_lf_when_values_repeat_consecutively_and_partitioned_by_locationId(
        self,
    ):
        test_lf_with_repeaded_values = pl.LazyFrame(
            data=Data.locations_with_repeated_value_rows,
            schema=Schemas.locations_with_repeated_value_schema,
            orient="row",
        )
        expected_lf_without_repeated_values = pl.LazyFrame(
            data=Data.expected_locations_without_repeated_values_when_input_has_repeated_values_rows,
            schema=Schemas.expected_locations_without_repeated_values_schema,
            orient="row",
        )
        returned_lf = job.create_column_with_repeated_values_removed(
            test_lf_with_repeaded_values,
            column_to_clean="integer_column",
        )
        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf_without_repeated_values,
        )

    def test_create_column_with_repeated_values_removed_returns_correct_lf_when_values_dont_repeat_consecutively_and_partitioned_by_locationId(
        self,
    ):
        test_lf_without_repeated_values = pl.LazyFrame(
            data=Data.location_without_repeated_value_rows,
            schema=Schemas.locations_with_repeated_value_schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_locations_without_repeated_value_rows,
            schema=Schemas.expected_locations_without_repeated_values_schema,
            orient="row",
        )
        returned_lf = job.create_column_with_repeated_values_removed(
            test_lf_without_repeated_values,
            column_to_clean="integer_column",
        )
        pl_testing.assert_frame_equal(
            returned_lf,
            expected_lf,
        )

    def test_create_column_with_repeated_values_removed_returns_one_additional_column_when_partitioned_by_location_id(
        self,
    ):
        OUTPUT_COLUMN = "integer_column_deduplicated"
        test_lf_with_repeaded_values = pl.LazyFrame(
            data=Data.locations_with_repeated_value_rows,
            schema=Schemas.locations_with_repeated_value_schema,
            orient="row",
        )
        returned_lf = job.create_column_with_repeated_values_removed(
            test_lf_with_repeaded_values,
            column_to_clean="integer_column",
        )
        self.assertIn(OUTPUT_COLUMN, returned_lf.collect_schema().names())

        cols_added = set(returned_lf.collect_schema().names()) - set(
            test_lf_with_repeaded_values.collect_schema().names()
        )
        self.assertEqual(cols_added, {OUTPUT_COLUMN})

    def test_create_column_with_repeated_values_removed_returns_correct_lf_when_values_dont_repeat_consecutively_and_partitioned_by_providerId(
        self,
    ):
        test_lf_without_repeated_values = pl.LazyFrame(
            data=Data.providers_without_repeated_value_rows,
            schema=Schemas.providers_with_repeated_value_schema,
            orient="row",
        )
        returned_lf_per_providerid = job.create_column_with_repeated_values_removed(
            test_lf_without_repeated_values,
            column_to_clean="integer_column",
            column_to_partition_by=IndCQC.provider_id,
        )
        expected_lf_without_repeated_values = pl.LazyFrame(
            data=Data.expected_providers_without_repeated_value_rows,
            schema=Schemas.expected_providers_without_repeated_values_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(
            returned_lf_per_providerid,
            expected_lf_without_repeated_values,
        )

    def test_create_column_with_repeated_values_removed_returns_correct_lf_when_values_repeat_consecutively_and_partitioned_by_providerId(
        self,
    ):
        test_lf_without_repeated_values = pl.LazyFrame(
            data=Data.providers_with_repeated_value_rows,
            schema=Schemas.providers_with_repeated_value_schema,
            orient="row",
        )
        returned_lf_per_providerid = job.create_column_with_repeated_values_removed(
            test_lf_without_repeated_values,
            column_to_clean="integer_column",
            column_to_partition_by=IndCQC.provider_id,
        )
        expected_lf_without_repeated_values = pl.LazyFrame(
            data=Data.expected_providers_without_repeated_values_when_input_has_repeated_values_rows,
            schema=Schemas.expected_providers_without_repeated_values_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(
            returned_lf_per_providerid,
            expected_lf_without_repeated_values,
        )
