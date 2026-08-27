import polars as pl
import polars.testing as pl_testing

import projects._08_publication._01_job_role_estimates.fargate.utils.merge_utils as job


class TestJoinEstimatesAndMetadata:
    def test_returns_estimates_lf_unchanged(self):
        estimates_lf = pl.LazyFrame({"col": [1, 2, 3]})
        metadata_lf = pl.LazyFrame({"other_col": ["a", "b"]})

        returned_lf = job.join_estimates_and_metadata(estimates_lf, metadata_lf)

        pl_testing.assert_frame_equal(returned_lf, estimates_lf)


class TestJoinGeography:
    def test_returns_merged_lf_unchanged(self):
        merged_lf = pl.LazyFrame({"col": [1, 2, 3]})
        geography_lf = pl.LazyFrame({"other_col": ["a", "b"]})

        returned_lf = job.join_geography(merged_lf, geography_lf)

        pl_testing.assert_frame_equal(returned_lf, merged_lf)
