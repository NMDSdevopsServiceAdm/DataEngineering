import unittest

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.cqc_api.fargate.delta_clean_cqc_locations as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    CQCLocationsData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    CQCLocationsSchema as Schemas,
)


class MainTests(unittest.TestCase):
    def test_main(self):
        pass


class CleanProviderIdColumnTests(unittest.TestCase):
    def test_does_not_change_valid_ids(self):
        # GIVEN
        input_df = pl.DataFrame(
            data=Data.clean_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        # WHEN
        output_df = job.clean_provider_id_column(input_df)

        # THEN
        self.assertIsInstance(output_df, pl.DataFrame)
        pl_testing.assert_frame_equal(output_df, input_df)

    def test_removes_long_provider_ids(self):
        # GIVEN
        input_df = pl.DataFrame(
            data=Data.long_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )
        expected_df = pl.DataFrame(
            data=Data.expected_long_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        # WHEN
        output_df = job.clean_provider_id_column(input_df)

        # THEN
        pl_testing.assert_frame_equal(expected_df, output_df)

    def test_fills_missing_provider_id(self):
        # GIVEN
        input_df = pl.DataFrame(
            data=Data.missing_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )
        expected_df = pl.DataFrame(
            data=Data.expected_fill_missing_provider_id_column_rows,
            schema=Schemas.clean_provider_id_column_schema,
        )

        # WHEN
        output_df = job.clean_provider_id_column(input_df)

        # THEN
        pl_testing.assert_frame_equal(expected_df, output_df)


if __name__ == "__main__":
    unittest.main()
