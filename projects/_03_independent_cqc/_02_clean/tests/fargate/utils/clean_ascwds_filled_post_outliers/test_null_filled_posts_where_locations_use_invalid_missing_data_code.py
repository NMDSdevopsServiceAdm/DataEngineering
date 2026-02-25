import unittest

import polars as pl
import polars.testing as pl_testing

from projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers import (
    null_filled_posts_where_locations_use_invalid_missing_data_code as job,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    NullFilledPostsUsingInvalidMissingDataCodeData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    NullFilledPostsUsingInvalidMissingDataCodeSchema as Schemas,
)


class NullFilledPostsUsingInvalidMissingDataCodeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.test_lf = pl.LazyFrame(
            Data.null_filled_posts_using_invalid_missing_data_code_rows,
            Schemas.null_filled_posts_using_invalid_missing_data_code_schema,
            orient="row",
        )

        self.returned_lf = (
            job.null_filled_posts_where_locations_use_invalid_missing_data_code(
                self.test_lf
            )
        )
        self.expected_lf = pl.LazyFrame(
            Data.expected_null_filled_posts_using_invalid_missing_data_code_rows,
            Schemas.null_filled_posts_using_invalid_missing_data_code_schema,
            orient="row",
        )

    def test_function_returns_expected_values(
        self,
    ):
        pl_testing.assert_frame_equal(
            self.expected_lf,
            self.returned_lf,
        )

    def test_missing_data_code_is_correct(
        self,
    ):
        self.assertEqual(job.INVALID_MISSING_DATA_CODE, 999.0)
