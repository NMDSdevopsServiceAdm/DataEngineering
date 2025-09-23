import unittest

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.cqc_api.fargate.utils.postcode_matcher as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    PostcodeMatcherTest as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    PostcodeMatcherTest as Schemas,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)


class CleanPostcodeColumnTests(unittest.TestCase):
    def setUp(
        self,
    ):
        self.test_df = pl.DataFrame(
            data=Data.clean_postcode_column_rows,
            schema=Schemas.clean_postcode_column_schema,
        )

    def test_returns_expected_values_when_drop_is_false(self):
        returned_df = job.clean_postcode_column(
            self.test_df, CQCL.postal_code, CQCLClean.postcode_cleaned, False
        )
        expected_df = pl.DataFrame(
            data=Data.expected_clean_postcode_column_when_drop_is_false_rows,
            schema=Schemas.expected_clean_postcode_column_when_drop_is_false_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_returns_expected_values_when_drop_is_true(self):
        returned_df = job.clean_postcode_column(
            self.test_df, CQCL.postal_code, CQCLClean.postcode_cleaned, True
        )
        expected_df = pl.DataFrame(
            data=Data.expected_clean_postcode_column_when_drop_is_true_rows,
            schema=Schemas.expected_clean_postcode_column_when_drop_is_true_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)
