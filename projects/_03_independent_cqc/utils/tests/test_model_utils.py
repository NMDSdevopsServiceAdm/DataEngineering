import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc.utils.model_utils as job
from projects._03_independent_cqc.unittest_data.polars_independent_cqc_test_data import (
    TestModelUtilsData as Data,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

SCHEMA_OVERRIDES = {IndCQC.cqc_location_import_date_indexed: pl.UInt32}


class TestAddDateIndex:
    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.add_date_index_test_cases],
    )
    def test_dates_are_densely_ranked_per_partition(self, case):
        returned_lf = job.add_date_index(
            pl.LazyFrame(case.input_data),
            partition_columns=[IndCQC.location_id],
            date_column=IndCQC.cqc_location_import_date,
        )

        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=SCHEMA_OVERRIDES
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
