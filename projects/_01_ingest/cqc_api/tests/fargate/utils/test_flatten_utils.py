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

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.utils.flatten_utils"


class FlattenStructFieldsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.mappings = [
            ("struct_1", "field_1", "struct_1_field_1"),
            ("struct_2", "field_2", "struct_2_field_2"),
        ]

    def test_handles_empty_source_lists(self):
        """Empty list columns should produce empty flattened lists."""
        lf = pl.LazyFrame(
            data=Data.flatten_struct_fields_empty_struct_row,
            schema=Schemas.flatten_struct_fields_schema,
            orient="row",
        )

        returned_lf = job.flatten_struct_fields(lf, self.mappings)

        expected_lf = pl.LazyFrame(
            data=Data.expected_flatten_struct_fields_empty_struct_row,
            schema=Schemas.expected_flatten_struct_fields_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_extracts_struct_fields_correctly(self):
        """Validate that struct fields are flattened properly."""
        lf = pl.LazyFrame(
            data=Data.flatten_struct_fields_populated_struct_row,
            schema=Schemas.flatten_struct_fields_schema,
            orient="row",
        )

        returned_lf = job.flatten_struct_fields(lf, self.mappings)

        expected_lf = pl.LazyFrame(
            data=Data.expected_flatten_struct_fields_populated_struct_row,
            schema=Schemas.expected_flatten_struct_fields_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
