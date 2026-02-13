import polars as pl
import unittest

import projects._03_independent_cqc._02_clean.fargate.utils.ascwds_filled_posts_calculator.difference_within_range as job
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_data import (
    CalculateAscwdsFilledPostsDifferenceInRangeData as Data,
)
from projects._03_independent_cqc.unittest_data.polars_ind_cqc_test_file_schemas import (
    CalculateAscwdsFilledPostsDifferenceInRangeSchemas as Schemas,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestAscwdsFilledPostsAbsoluteDiffInRange(unittest.TestCase):
    def setUp(self):
        self.ascwds_total_staff_and_worker_record_lf = pl.LazyFrame(
            Data.test_difference_within_range_rows,
            Schemas.test_difference_within_range_schema,
            orient="row",
        )
        self.returned_lf = job.calculate_ascwds_filled_posts_difference_within_range(
            self.ascwds_total_staff_and_worker_record_lf,
            IndCQC.total_staff_bounded,
            IndCQC.worker_records_bounded,
            IndCQC.ascwds_filled_posts,
            IndCQC.ascwds_filled_posts_source,
        )

    def test_calculate_ascwds_filled_posts_row_count_correct(
        self,
    ):
        self.assertEqual(self.returned_lf.collect().height, 9)

    def test_calculate_ascwds_filled_posts_difference_within_range_values(self):
        df = self.returned_lf.sort(IndCQC.location_id).collect()

        expected_values = [None, 500.0, None, None, None, None, 11.5, 487.5, 8.0]

        actual_values = df[IndCQC.ascwds_filled_posts].to_list()

        self.assertEqual(actual_values, expected_values)

    def test_calculate_ascwds_filled_posts_difference_within_range_sources(self):
        df = self.returned_lf.sort(IndCQC.location_id).collect()

        expected_sources = [
            None,
            job.ascwds_filled_posts_difference_within_range_source_description,
            None,
            None,
            None,
            None,
            job.ascwds_filled_posts_difference_within_range_source_description,
            job.ascwds_filled_posts_difference_within_range_source_description,
            "already populated",
        ]

        actual_sources = df[IndCQC.ascwds_filled_posts_source].to_list()

        self.assertEqual(actual_sources, expected_sources)
