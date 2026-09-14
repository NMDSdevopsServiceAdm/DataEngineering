import polars as pl

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.clean_utils as job
from utils.column_names.publication_columns import PublicationColumns as Pub


class TestAddCtFilterHasCtData:
    def test_identifies_rows_with_ct_data(self):
        pass


class TestAddCtFilterDispersionFilter:
    def test_identifies_locations_within_ct_posts_dispersion_boundaries(self):
        pass


class TestAggregateToPublicationRows:
    def test_returns_expected_data(self):
        pass


class TestAddRowsForPublicationGroups:
    def test_returns_expected_data(self):
        pass


class TestCalcPercChangeBetweenRows:
    def test_returns_expected_data(self):
        pass


class TestCalcPercChangeCumulativeFromGivenPeriodOnwards:
    def test_returns_expected_data(self):
        pass
