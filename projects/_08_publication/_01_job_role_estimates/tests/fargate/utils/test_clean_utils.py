import polars as pl

import projects._08_publication._01_job_role_estimates.fargate.utils.clean_utils as job
from utils.column_names.publication_columns import PublicationColumns as Pub


class TestAddCtFilterHasCtData:
    def test_adds_true_for_every_row(self):
        lf = pl.LazyFrame({"col": [1, 2, 3]})

        returned_lf = lf.with_columns(job.add_ct_filter_has_ct_data())

        assert returned_lf.collect()[Pub.ct_has_data].to_list() == [True, True, True]


class TestAddCtFilterConsistentService:
    def test_adds_true_for_every_row(self):
        lf = pl.LazyFrame({"col": [1, 2, 3]})

        returned_lf = lf.with_columns(job.add_ct_filter_consistent_service())

        assert returned_lf.collect()[Pub.consistent_service].to_list() == [
            True,
            True,
            True,
        ]


class TestAddCtFilterDispersionFilter:
    def test_adds_true_for_every_row(self):
        lf = pl.LazyFrame({"col": [1, 2, 3]})

        returned_lf = lf.with_columns(job.add_ct_filter_dispersion_filter())

        assert returned_lf.collect()[Pub.ct_dispersion_filter].to_list() == [
            True,
            True,
            True,
        ]


class TestSplitIntoAssessmentAndPublicationData:
    def test_returns_the_same_lazyframe_for_both_outputs(self):
        lf = pl.LazyFrame({"col": [1, 2, 3]})

        assessment_lf, publication_lf = job.split_into_assessment_and_publication_data(
            lf
        )

        assert assessment_lf is lf
        assert publication_lf is lf
