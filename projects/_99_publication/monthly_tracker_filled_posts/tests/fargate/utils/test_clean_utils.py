import polars as pl
import polars.testing as pl_testing
import pytest

import projects._99_publication.monthly_tracker_filled_posts.fargate.utils.clean_utils as job
import projects._99_publication.unittest_data.polars_pub_test_data as Data
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class TestHasColumnDataSinceDate:
    has_column_data_since_data_schema = pl.Schema(
        [
            (IndCQC.location_id, pl.String()),
            (IndCQC.cqc_location_import_date, pl.Date()),
            (IndCQC.ct_care_home_total_employed_imputed, pl.Float32()),
            (IndCQC.ct_non_res_care_workers_employed_imputed, pl.Float32()),
        ]
    )

    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in Data.has_continuous_data_since_date_test_cases
        ],
    )
    def test_flags_locations_with_column_data_since_a_given_date(self, case):
        expected_schema = pl.Schema(
            list(self.has_column_data_since_data_schema.items())
            + [(case.column_alias, pl.Boolean())]
        )
        expected_lf = pl.LazyFrame(case.expected_data, expected_schema, orient="row")

        test_lf = expected_lf.drop(case.column_alias)

        returned_lf = test_lf.with_columns(
            job.has_continuous_data_since_date(
                case.column_name, case.from_date, case.column_alias
            )
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


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
