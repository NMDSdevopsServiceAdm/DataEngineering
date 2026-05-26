import unittest

import polars as pl
import polars.testing as pl_testing

from projects._03_independent_cqc._02_clean.fargate.utils import filtering_utils as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CleanFilteringUtilsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CleanFilteringUtilsSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class AggregateValuesToProviderLevel(unittest.TestCase):
    def test_aggregate_values_to_provider_level_returns_expected_values(self):
        test_lf = pl.LazyFrame(
            data=Data.aggregate_values_to_provider_level_rows,
            schema=Schemas.aggregate_values_to_provider_level_schema,
            orient="row",
        )
        returned_lf = job.aggregate_values_to_provider_level(
            test_lf, IndCQC.ct_care_home_total_employed_cleaned
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_aggregate_values_to_provider_level_rows,
            schema=Schemas.expected_aggregate_values_to_provider_level_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
