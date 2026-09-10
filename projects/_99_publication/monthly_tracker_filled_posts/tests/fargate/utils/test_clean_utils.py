import polars as pl
import polars.testing as pl_testing
import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.clean_utils as job
from projects._08_publication.unittest_data.polars_pub_test_data import (
    HAS_COLUMN_DATA_SINCE_DATE_TEST_CASES,
    HasColumnDataSinceDateTestCase,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC

CT_HAS_DATA_SCHEMA = pl.Schema(
    [
        (IndCQC.location_id, pl.String()),
        (IndCQC.cqc_location_import_date, pl.Date()),
        (IndCQC.ct_care_home_total_employed_imputed, pl.Float32()),
        (IndCQC.ct_non_res_care_workers_employed_imputed, pl.Float32()),
    ]
)


class TestHasColumnDataSinceDate:
    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in HAS_COLUMN_DATA_SINCE_DATE_TEST_CASES],
    )
    def test_flags_locations_with_column_data_since_a_given_date(
        self, case: HasColumnDataSinceDateTestCase
    ):
        test_lf = pl.LazyFrame(case.test_data, CT_HAS_DATA_SCHEMA, orient="row")

        returned_lf = test_lf.with_columns(
            job.has_column_data_since_date(
                case.column_name, case.from_date, case.column_alias
            )
        )

        expected_schema = pl.Schema(
            list(CT_HAS_DATA_SCHEMA.items()) + [(case.column_alias, pl.Boolean())]
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddCtFilterConsistentService:
    def test_identifies_locations_that_are_always_care_home_or_always_non_res(self):
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
