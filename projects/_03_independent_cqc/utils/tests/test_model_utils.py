import polars as pl
import polars.testing as pl_testing
import pytest

import projects._03_independent_cqc.utils.model_utils as job
from polars_utils.column_types import CategoricalColumnTypes as CatColType
from projects._03_independent_cqc.unittest_data.polars_independent_cqc_test_data import (
    TestModelUtilsData as Data,
)
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

SCHEMA_OVERRIDES = {IndCQC.cqc_location_import_date_indexed: pl.UInt32}
RATING_SCHEMA_OVERRIDES = {
    IndCQC.latest_overall_rating: pl.Categorical,
    CQCRatings.overall_rating: pl.String,
    CQCRatings.date: pl.String,
    CQCL.assessment_date: pl.String,
}


class TestAddDateIndex:
    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.add_date_index_test_cases],
    )
    def test_dates_are_densely_ranked_per_partition(self, case):
        returned_lf = job.add_date_index(
            pl.LazyFrame(case.input_data),
            partition_columns=[IndCQC.location_id],
            date_column=IndCQC.cqc_location_import_date,
        )

        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=SCHEMA_OVERRIDES
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TestAddLatestOverallRating:
    @staticmethod
    def returned_and_expected_lfs(
        case, location_type: pl.DataType = CatColType.LocationCatType
    ) -> tuple[pl.LazyFrame, pl.LazyFrame]:
        """Run with the data's location IDs cast to `location_type`, and strings in ratings."""
        as_location_type = pl.col(IndCQC.location_id).cast(location_type)
        returned_lf = job.add_latest_overall_rating(
            pl.LazyFrame(
                case.input_data, schema_overrides=RATING_SCHEMA_OVERRIDES
            ).with_columns(as_location_type),
            pl.LazyFrame(case.ratings_data, schema_overrides=RATING_SCHEMA_OVERRIDES),
            location_column=IndCQC.location_id,
            date_column=IndCQC.cqc_location_import_date,
        )
        expected_lf = pl.LazyFrame(
            case.expected_data, schema_overrides=RATING_SCHEMA_OVERRIDES
        ).with_columns(as_location_type)

        return returned_lf, expected_lf

    @pytest.mark.parametrize(
        "case",
        [
            case.as_pytest_param()
            for case in [
                *Data.latest_rating_joined_per_location_test_cases,
                *Data.blank_or_unrated_latest_rating_test_cases,
                *Data.different_case_rating_test_cases,
                *Data.same_date_ratings_test_cases,
                *Data.unrated_location_test_cases,
                Data.existing_rating_replaced_test_case,
            ]
        ],
    )
    def test_returns_latest_real_rating_per_location(self, case):
        returned_lf, expected_lf = self.returned_and_expected_lfs(case)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)

    @pytest.mark.parametrize(
        "case",
        [case.as_pytest_param() for case in Data.rating_as_of_import_date_test_cases],
    )
    # Polars' as-of join takes a different path for categorical keys, so test both.
    @pytest.mark.parametrize(
        "location_type",
        [
            pytest.param(pl.String, id="string_ids"),
            pytest.param(CatColType.LocationCatType, id="categorical_ids"),
        ],
    )
    def test_rating_is_as_of_each_import_date(self, case, location_type):
        returned_lf, expected_lf = self.returned_and_expected_lfs(case, location_type)

        pl_testing.assert_frame_equal(returned_lf, expected_lf, check_row_order=False)
