import unittest

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._03_impute.fargate.utils.convert_pir_people_to_filled_posts as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    ConvertPirPeopleToFilledPostsData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    ConvertPirPeopleToFilledPostsSchema as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class ValidRowsTests(unittest.TestCase):
    def test_valid_rows_filters_correctly(self):
        lf = pl.LazyFrame(
            Data.valid_rows,
            Schemas.input_schema,
            orient="row",
        )

        returned_lf = lf.filter(job.valid_rows())

        expected_lf = pl.LazyFrame(
            Data.expected_valid_rows,
            Schemas.input_schema,
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class GlobalRatioTests(unittest.TestCase):
    pass
