import unittest
from datetime import date
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.non_res_brand_id_filter as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

PATCH_PATH: str = (
    "projects._03_independent_cqc._02_clean.fargate.utils.clean_ascwds_filled_post_outliers.non_res_brand_id_filter"
)


class TestNonResBrandIdFilter(unittest.TestCase):
    @patch(f"{PATCH_PATH}.update_filtering_rule")
    def test_function_returns_expected_values(self, update_filtering_rule_mock: Mock):
        schema = {
            IndCQC.cqc_location_import_date: pl.Date,
            IndCQC.care_home: pl.String,
            IndCQC.brand_id: pl.String,
            IndCQC.ascwds_filled_posts_dedup_clean: pl.Float32,
        }

        input_rows = [
            (date(2024, 4, 1), "N", "BD214", 5.0), # After start_date, before end_date, BrandId match and non res location - should be nullified
            (date(2025, 4, 1), "N", "BD214", 5.0), # After start_date, before end_date, BrandId match and non res location - should be nullified
            (date(2024, 2, 1), "N", "BD214", 5.0), # Before start_date - should not be nullified
            (date(2024, 4, 1), "N", "OTHER_BRAND", 5.0), # Other Brand Id - should not be nullified
            (date(2024, 4, 1), "Y", "BD214", 5.0), # Care Home - should not be nullified
        ] # fmt: skip
        expected_rows = [
            (date(2024, 4, 1), "N", "BD214", None),
            (date(2025, 4, 1), "N", "BD214", None),
            (date(2024, 2, 1), "N", "BD214", 5.0),
            (date(2024, 4, 1), "N", "OTHER_BRAND", 5.0),
            (date(2024, 4, 1), "Y", "BD214", 5.0),
        ]
        unfiltered_ind_cqc_lf = pl.LazyFrame(
            input_rows,
            schema,
            orient="row",
        )
        expected_lf = pl.LazyFrame(
            expected_rows,
            schema,
            orient="row",
        )
        update_filtering_rule_mock.return_value = expected_lf

        returned_lf = job.non_res_brand_id_filter(unfiltered_ind_cqc_lf)

        pl_testing.assert_frame_equal(expected_lf, returned_lf)
        update_filtering_rule_mock.assert_called_once()
