from datetime import date

import polars as pl
import pytest

import projects._08_publication._01_job_role_estimates.fargate.utils.clean_utils as job
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.publication_columns import PublicationColumns as Pub

TODAY = date(2026, 9, 1)
CUTOFF_DATE = date(2020, 4, 1)


class TestCqcLocationImportDateFilterExpr:
    @pytest.mark.parametrize(
        "import_date, is_kept",
        [
            pytest.param(date(2020, 3, 31), False, id="just_before_cutoff_is_removed"),
            pytest.param(date(2020, 4, 1), True, id="cutoff_date_is_kept"),
            pytest.param(date(2026, 8, 1), True, id="recent_date_is_kept"),
        ],
    )
    def test_keeps_rows_on_or_after_six_financial_years_before_today(
        self, import_date, is_kept
    ):
        input_lf = pl.LazyFrame({IndCQC.cqc_location_import_date: [import_date]})

        expr = job.cqc_location_import_date_filter_expr(today=TODAY)
        returned_lf = input_lf.filter(expr)

        assert (returned_lf.collect().height == 1) == is_kept

    def test_defaults_today_to_current_date_when_not_passed(self):
        input_lf = pl.LazyFrame({IndCQC.cqc_location_import_date: [CUTOFF_DATE]})

        expr = job.cqc_location_import_date_filter_expr()
        returned_lf = input_lf.filter(expr)

        assert returned_lf.collect().height == 1


class TestAddCtFilterHasCtData:
    def test_identifies_rows_with_ct_data(self):
        pass


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
