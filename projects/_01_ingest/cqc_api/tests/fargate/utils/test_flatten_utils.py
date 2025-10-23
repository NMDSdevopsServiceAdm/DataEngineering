import unittest

import polars as pl
import polars.testing as pl_testing

from projects._01_ingest.cqc_api.fargate.utils import flatten_utils as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    ExtractRegisteredManagerNamesData as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    ExtractRegisteredManagerNamesSchema as Schemas,
)

PATCH_PATH = "projects._01_ingest.cqc_api.fargate.utils.flatten_utils"


class ImputeMissingStructColumnsTests(unittest.TestCase):
    pass

    def test_single_struct_column_imputation(self):
        """Tests imputation for a single struct column with None and empty dicts."""

    pass

    def test_multiple_struct_columns_independent_imputation(self):
        """Ensures multiple struct columns are imputed independently and correctly."""

    pass

    def test_empty_and_partial_structs(self):
        """Treats empty structs {} or all-null structs as missing, but preserves partially filled ones."""

    pass

    def test_imputation_partitions_correctly(self):
        """Verifies imputation does not leak values across location_id partitions."""

    pass

    def test_out_of_order_dates_are_sorted_before_imputation(self):
        """Ensures imputation follows chronological order based on date column."""

    pass

    def test_fully_null_column_remains_null_after_imputation(self):
        """If a struct column is entirely null, imputed column should remain all nulls."""

    pass

    def test_multiple_partitions_with_varied_missing_patterns(self):
        """Tests complex case with several partitions, ensuring each is filled correctly."""

    pass
