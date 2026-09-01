import unittest

import polars as pl

import projects._03_independent_cqc._06_estimate_filled_posts.fargate.utils.estimate_ind_cqc_filled_posts_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import EstimateFilledPostsSource


class EstimateFilledPostsCastExprTests(unittest.TestCase):
    def test_casts_estimate_filled_posts_to_float32(self):
        lf = pl.LazyFrame(
            {IndCQC.estimate_filled_posts: [1.0]},
            schema={IndCQC.estimate_filled_posts: pl.Float64},
        )
        result_lf = lf.with_columns(job.estimate_filled_posts_cast_expr())
        self.assertEqual(
            result_lf.collect_schema()[IndCQC.estimate_filled_posts], pl.Float32
        )


class EstimateFilledPostsSourceCastExprTests(unittest.TestCase):
    def test_casts_estimate_filled_posts_source_to_enum(self):
        lf = pl.LazyFrame(
            {
                IndCQC.estimate_filled_posts_source: [
                    EstimateFilledPostsSource.ascwds_pir_merged
                ]
            }
        )
        result_lf = lf.with_columns(job.estimate_filled_posts_source_cast_expr())
        self.assertEqual(
            result_lf.collect_schema()[IndCQC.estimate_filled_posts_source],
            CatColType.EstimatesFilledPostSourceEnumType,
        )
