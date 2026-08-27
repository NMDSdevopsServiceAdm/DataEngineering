import polars as pl

import projects._08_publication._01_job_role_estimates.fargate.utils.clean_utils as job
from utils.column_names.publication_columns import PublicationColumns as Pub


class TestAddCtFilterHasCtData:
    def test_adds_true_for_every_row(self):
        pass


class TestAddCtFilterConsistentService:
    def test_adds_true_for_every_row(self):
        pass


class TestAddCtFilterDispersionFilter:
    def test_adds_true_for_every_row(self):
        pass


class TestSplitIntoAssessmentAndPublicationData:
    def test_returns_the_same_lazyframe_for_both_outputs(self):
        pass
