import unittest

import polars as pl
import polars.testing as pl_testing

from projects._01_ingest.cqc_api.fargate.utils import flatten_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    FlattenUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    FlattenUtilsSchema as Schemas,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_values.categorical_column_values import Sector

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.utils.flatten_utils"


class AssignCqcSectorTests(unittest.TestCase):
    def test_assigns_local_authority(self):
        lf = pl.LazyFrame(
            data=Data.assign_cqc_sector,
            schema=Schemas.assign_cqc_sector_schema,
        )
        local_authority_provider_ids = lf.collect()[CQCLClean.provider_id].to_list()

        result_lf = job.assign_cqc_sector(lf, local_authority_provider_ids)

        expected_lf = pl.LazyFrame(
            data=Data.expected_assign_cqc_sector_local_authority,
            schema=Schemas.expected_assign_cqc_sector_schema,
        )
        pl_testing.assert_frame_equal(expected_lf, result_lf)

    def test_assigns_independent(self):
        lf = pl.LazyFrame(
            data=Data.assign_cqc_sector,
            schema=Schemas.assign_cqc_sector_schema,
        )
        local_authority_provider_ids = ["non-matching-id-1", "non-matching-id-2"]

        result_lf = job.assign_cqc_sector(lf, local_authority_provider_ids)

        expected_lf = pl.LazyFrame(
            data=Data.expected_assign_cqc_sector_independent,
            schema=Schemas.expected_assign_cqc_sector_schema,
        )
        pl_testing.assert_frame_equal(expected_lf, result_lf)


class ImputeMissingStructColumnsTests(unittest.TestCase):
    def test_single_struct_column_imputation(self):
        """Tests imputation for a single struct column with None and empty dicts."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_single_struct_col_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_single_struct_col_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])
        result_df = result_lf.collect()
        expected_df = expected_lf.collect()

        pl_testing.assert_frame_equal(result_df, expected_df)

    def test_multiple_struct_columns_independent_imputation(self):
        """Ensures multiple struct columns are imputed independently and correctly."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_multiple_struct_cols_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_multiple_struct_cols_rows,
            schema=Schemas.expected_impute_missing_struct_two_cols_schema,
        )

        result_lf = job.impute_missing_struct_columns(
            lf, [CQCLClean.gac_service_types, CQCLClean.specialisms]
        )
        result_df = result_lf.collect()
        expected_df = expected_lf.collect()

        pl_testing.assert_frame_equal(result_df, expected_df)

    def test_empty_and_partial_structs(self):
        """Treats empty structs or all-null structs as missing, but preserves partially filled ones."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_empty_and_partial_structs_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_empty_and_partial_structs_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])
        result_df = result_lf.collect()
        expected_df = expected_lf.collect()

        pl_testing.assert_frame_equal(result_df, expected_df)

    def test_imputation_partitions_correctly(self):
        """Verifies imputation does not leak values across location_id partitions."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_imputation_partitions_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_imputation_partitions_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])
        result_df = result_lf.collect()
        expected_df = expected_lf.collect()

        pl_testing.assert_frame_equal(result_df, expected_df)

    def test_out_of_order_dates_are_sorted_before_imputation(self):
        """Ensures imputation follows chronological order based on date column."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_out_of_order_dates_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_out_of_order_dates_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])
        result_df = result_lf.collect()
        expected_df = expected_lf.collect()

        pl_testing.assert_frame_equal(result_df, expected_df)

    def test_fully_null_column_remains_null_after_imputation(self):
        """If a struct column is entirely null, imputed column should remain all nulls."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_fully_null_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_fully_null_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])
        result_df = result_lf.collect()
        expected_df = expected_lf.collect()

        pl_testing.assert_frame_equal(result_df, expected_df)

    def test_multiple_partitions_with_varied_missing_patterns(self):
        """Tests complex case with several partitions, ensuring each is filled correctly."""

        lf = pl.LazyFrame(
            Data.impute_missing_struct_multiple_partitions_and_missing_data_rows,
            schema=Schemas.impute_missing_struct_schema,
        )
        expected_lf = pl.LazyFrame(
            Data.expected_impute_missing_struct_multiple_partitions_and_missing_data_rows,
            schema=Schemas.expected_impute_missing_struct_one_col_schema,
        )

        result_lf = job.impute_missing_struct_columns(lf, [CQCLClean.gac_service_types])
        result_df = result_lf.collect()
        expected_df = expected_lf.collect()

        pl_testing.assert_frame_equal(result_df, expected_df)
