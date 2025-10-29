import unittest

import polars as pl
import polars.testing as pl_testing

from projects._01_ingest.cqc_api.fargate.utils import locations_4_clean_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    LocationsCleanUtilsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    LocationsCleanUtilsSchema as Schemas,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.utils.locations_4_clean_utils"


class CleanProviderIdColumnTests(unittest.TestCase):
    def test_does_not_change_valid_ids(self):
        """Input with provider ids which are all populated and less than 14 characters"""
        lf = pl.LazyFrame(
            data=Data.clean_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        returned_lf = job.clean_provider_id_column(lf)

        pl_testing.assert_frame_equal(returned_lf, lf)

    def test_removes_long_provider_ids(self):
        """Input with provider ids which are longer than 14 characters"""
        lf = pl.LazyFrame(
            data=Data.long_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_long_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        returned_lf = job.clean_provider_id_column(lf)

        pl_testing.assert_frame_equal(expected_lf, returned_lf)

    def test_fills_missing_provider_id(self):
        """Input with provider ids which are missing for some instances of a location id"""
        lf = pl.LazyFrame(
            data=Data.missing_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_fill_missing_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        returned_lf = job.clean_provider_id_column(lf)

        pl_testing.assert_frame_equal(expected_lf, returned_lf)


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


class ImputeHistoricRelationshipsTests(unittest.TestCase):
    def test_impute_historic_relationships_when_type_is_none_returns_none(self):
        test_lf = pl.LazyFrame(
            data=Data.impute_historic_relationships_when_type_is_none_returns_none_rows,
            schema=Schemas.impute_historic_relationships_schema,
        )
        returned_lf = job.impute_historic_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_impute_historic_relationships_when_type_is_none_returns_none_rows,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_impute_historic_relationships_when_type_is_predecessor_returns_predecessor(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.impute_historic_relationships_when_type_is_predecessor_returns_predecessor_rows,
            schema=Schemas.impute_historic_relationships_schema,
        )
        returned_lf = job.impute_historic_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_impute_historic_relationships_when_type_is_predecessor_returns_predecessor_rows,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_impute_historic_relationships_when_type_is_successor_returns_none_when_registered(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.impute_historic_relationships_when_type_is_successor_returns_none_when_registered_rows,
            schema=Schemas.impute_historic_relationships_schema,
        )
        returned_lf = job.impute_historic_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_impute_historic_relationships_when_type_is_successor_returns_none_when_registered_rows,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_impute_historic_relationships_when_type_is_successor_returns_successor_when_deregistered(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.impute_historic_relationships_when_type_is_successor_returns_successor_when_deregistered_rows,
            schema=Schemas.impute_historic_relationships_schema,
        )
        returned_lf = job.impute_historic_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_impute_historic_relationships_when_type_is_successor_returns_successor_when_deregistered_rows,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_impute_historic_relationships_when_type_has_both_types_only_returns_predecessors_when_registered(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.impute_historic_relationships_when_type_has_both_types_only_returns_predecessors_when_registered_rows,
            schema=Schemas.impute_historic_relationships_schema,
        )
        returned_lf = job.impute_historic_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_impute_historic_relationships_when_type_has_both_types_only_returns_predecessors_when_registered_rows,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_impute_historic_relationships_when_type_has_both_types_returns_original_values_when_deregistered(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.impute_historic_relationships_when_type_has_both_types_returns_original_values_when_deregistered_rows,
            schema=Schemas.impute_historic_relationships_schema,
        )
        returned_lf = job.impute_historic_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_impute_historic_relationships_when_type_has_both_types_returns_original_values_when_deregistered_rows,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_impute_historic_relationships_where_different_relationships_over_time_returns_first_found(
        self,
    ):
        test_lf = pl.LazyFrame(
            data=Data.impute_historic_relationships_where_different_relationships_over_time_returns_first_found_rows,
            schema=Schemas.impute_historic_relationships_schema,
        )
        returned_lf = job.impute_historic_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_impute_historic_relationships_where_different_relationships_over_time_returns_first_found_rows,
            schema=Schemas.expected_impute_historic_relationships_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class GetRelationshipsWhereTypeIsPredecessorTests(unittest.TestCase):
    def test_get_relationships_where_type_is_none_returns_none(self):
        test_lf = pl.LazyFrame(
            data=Data.get_relationships_where_type_is_none_returns_none_rows,
            schema=Schemas.get_relationships_where_type_is_predecessor_schema,
        )
        returned_lf = job.get_predecessor_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_get_relationships_where_type_is_none_returns_none_rows,
            schema=Schemas.expected_get_relationships_where_type_is_predecessor_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_get_relationships_where_type_is_successor_returns_none(self):
        test_lf = pl.LazyFrame(
            data=Data.get_relationships_where_type_is_successor_returns_none_rows,
            schema=Schemas.get_relationships_where_type_is_predecessor_schema,
        )
        returned_lf = job.get_predecessor_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_get_relationships_where_type_is_successor_returns_none_rows,
            schema=Schemas.expected_get_relationships_where_type_is_predecessor_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_get_relationships_where_type_is_predecessor_returns_predecessor(self):
        test_lf = pl.LazyFrame(
            data=Data.get_relationships_where_type_is_predecessor_returns_predecessor_rows,
            schema=Schemas.get_relationships_where_type_is_predecessor_schema,
        )
        returned_lf = job.get_predecessor_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_get_relationships_where_type_is_predecessor_returns_predecessor_rows,
            schema=Schemas.expected_get_relationships_where_type_is_predecessor_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_get_relationships_where_type_has_both_types_only_returns_predecessor(self):
        test_lf = pl.LazyFrame(
            data=Data.get_relationships_where_type_has_both_types_only_returns_predecessor_rows,
            schema=Schemas.get_relationships_where_type_is_predecessor_schema,
        )
        returned_lf = job.get_predecessor_relationships(test_lf)
        expected_lf = pl.LazyFrame(
            data=Data.expected_get_relationships_where_type_has_both_types_only_returns_predecessor_rows,
            schema=Schemas.expected_get_relationships_where_type_is_predecessor_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
