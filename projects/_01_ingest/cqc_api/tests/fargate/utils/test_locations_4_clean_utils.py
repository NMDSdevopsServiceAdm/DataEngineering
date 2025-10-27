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
