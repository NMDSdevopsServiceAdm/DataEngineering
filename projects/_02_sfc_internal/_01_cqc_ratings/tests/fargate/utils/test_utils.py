from dataclasses import dataclass

import polars as pl
import polars.testing as pl_testing
import pytest

import projects._02_sfc_internal._01_cqc_ratings.fargate.utils.utils as job
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_data import (
    FlattenCQCRatings as Data,
)
from projects._02_sfc_internal.unittest_data.polars_sfc_test_file_schemas import (
    FlattenCQCRatings as Schemas,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)


class TestKeepLatestPerKey:
    def test_keeps_only_the_latest_row_per_key(self):
        input_lf = pl.LazyFrame(
            {
                "key": ["a", "a", "b"],
                "order": ["2024-01-01", "2024-02-01", "2024-01-01"],
                "value": [1, 2, 3],
            }
        )

        returned_lf = job.keep_latest_per_key(input_lf, "key", "order")

        expected_lf = pl.LazyFrame(
            {"key": ["a", "b"], "order": ["2024-02-01", "2024-01-01"], "value": [2, 3]}
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)


class TestFilterToFirstImportOfMostRecentMonth:
    def test_filters_to_first_day_of_most_recent_month_when_first_day_is_the_1st(self):
        input_lf = pl.LazyFrame(
            {
                "year": ["2023", "2024"],
                "month": ["12", "01"],
                "day": ["01", "01"],
            }
        )

        returned_lf = job.filter_to_first_import_of_most_recent_month(input_lf)

        expected_lf = pl.LazyFrame({"year": ["2024"], "month": ["01"], "day": ["01"]})
        pl_testing.assert_frame_equal(expected_lf, returned_lf)

    def test_filters_to_earliest_day_of_most_recent_month_when_two_imports_that_month(
        self,
    ):
        input_lf = pl.LazyFrame(
            {
                "year": ["2023", "2024", "2024"],
                "month": ["12", "01", "01"],
                "day": ["01", "01", "04"],
            }
        )

        returned_lf = job.filter_to_first_import_of_most_recent_month(input_lf)

        expected_lf = pl.LazyFrame({"year": ["2024"], "month": ["01"], "day": ["01"]})
        pl_testing.assert_frame_equal(expected_lf, returned_lf)

    def test_filters_to_earliest_day_when_earliest_day_is_not_the_1st_of_the_month(
        self,
    ):
        input_lf = pl.LazyFrame(
            {
                "year": ["2023", "2024", "2024"],
                "month": ["12", "01", "01"],
                "day": ["01", "02", "04"],
            }
        )

        returned_lf = job.filter_to_first_import_of_most_recent_month(input_lf)

        expected_lf = pl.LazyFrame({"year": ["2024"], "month": ["01"], "day": ["02"]})
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


@dataclass
class PrepareCurrentRatingsCase:
    id: str
    rows: list
    expected_rows: list

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


prepare_current_ratings_cases = [
    PrepareCurrentRatingsCase(
        id="flattens_and_labels_as_current",
        rows=Data.current_ratings_rows,
        expected_rows=Data.expected_prepare_current_ratings_rows,
    ),
    PrepareCurrentRatingsCase(
        id="maps_ratings_to_columns_by_name_when_key_questions_reordered",
        rows=Data.current_ratings_reordered_key_question_rows,
        expected_rows=Data.expected_prepare_current_ratings_rows,
    ),
    PrepareCurrentRatingsCase(
        id="fills_missing_key_questions_with_null_when_fewer_than_five",
        rows=Data.current_ratings_short_key_question_list_rows,
        expected_rows=Data.expected_prepare_current_ratings_short_key_question_list_rows,
    ),
]


class TestPrepareCurrentRatings:
    @pytest.mark.parametrize(
        "rows,expected_rows",
        [case.as_pytest_param() for case in prepare_current_ratings_cases],
    )
    def test_prepare_current_ratings_returns_expected_values(self, rows, expected_rows):
        input_lf = pl.LazyFrame(
            rows, schema=Schemas.current_ratings_schema, orient="row"
        )

        returned_lf = job.prepare_current_ratings(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.flattened_ratings_with_current_or_historic_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)

    def test_prepare_current_ratings_raises_when_key_question_name_repeated(self):
        input_lf = pl.LazyFrame(
            Data.current_ratings_duplicate_key_question_name_rows,
            schema=Schemas.current_ratings_schema,
            orient="row",
        )

        with pytest.raises(ValueError, match="Duplicate key question names"):
            job.prepare_current_ratings(input_lf).collect()


@dataclass
class PrepareHistoricRatingsCase:
    id: str
    rows: list
    expected_rows: list

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


prepare_historic_ratings_cases = [
    PrepareHistoricRatingsCase(
        id="flattens_recodes_and_labels_as_historic",
        rows=Data.historic_ratings_rows,
        expected_rows=Data.expected_prepare_historic_ratings_rows,
    ),
    PrepareHistoricRatingsCase(
        id="maps_ratings_to_columns_by_name_when_key_questions_reordered",
        rows=Data.historic_ratings_reordered_key_question_rows,
        expected_rows=Data.expected_prepare_historic_ratings_rows,
    ),
    PrepareHistoricRatingsCase(
        id="keeps_separate_rows_for_entries_with_same_date",
        rows=Data.historic_ratings_duplicate_date_rows,
        expected_rows=Data.expected_prepare_historic_ratings_duplicate_date_rows,
    ),
]


class TestPrepareHistoricRatings:
    @pytest.mark.parametrize(
        "rows,expected_rows",
        [case.as_pytest_param() for case in prepare_historic_ratings_cases],
    )
    def test_prepare_historic_ratings_returns_expected_values(
        self, rows, expected_rows
    ):
        input_lf = pl.LazyFrame(
            rows, schema=Schemas.historic_ratings_schema, orient="row"
        )

        returned_lf = job.prepare_historic_ratings(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.flattened_ratings_with_current_or_historic_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)

    def test_prepare_historic_ratings_raises_when_key_question_name_repeated(self):
        input_lf = pl.LazyFrame(
            Data.historic_ratings_duplicate_key_question_name_rows,
            schema=Schemas.historic_ratings_schema,
            orient="row",
        )

        with pytest.raises(ValueError, match="Duplicate key question names"):
            job.prepare_historic_ratings(input_lf).collect()


@dataclass
class PrepareAssessmentRatingsCase:
    id: str
    rows: list
    expected_data: dict

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_data, id=self.id)


prepare_assessment_ratings_cases = [
    PrepareAssessmentRatingsCase(
        id="asg_key_questions_become_columns",
        rows=Data.prepare_assessment_ratings_rows,
        expected_data=Data.expected_prepare_assessment_ratings_data,
    ),
    PrepareAssessmentRatingsCase(
        id="duplicate_key_question_keeps_first_in_raw_order",
        rows=Data.prepare_assessment_ratings_tiebreaker_rows,
        expected_data=Data.expected_prepare_assessment_ratings_tiebreaker_data,
    ),
    PrepareAssessmentRatingsCase(
        id="null_key_question_ratings_are_dropped",
        rows=Data.prepare_assessment_ratings_null_key_question_ratings_rows,
        expected_data=Data.expected_prepare_assessment_ratings_null_key_question_ratings_data,
    ),
    PrepareAssessmentRatingsCase(
        id="unknown_key_question_is_ignored",
        rows=Data.prepare_assessment_ratings_unknown_key_question_rows,
        expected_data=Data.expected_prepare_assessment_ratings_unknown_key_question_data,
    ),
]


class TestPrepareAssessmentRatings:
    @pytest.mark.parametrize(
        "rows,expected_data",
        [case.as_pytest_param() for case in prepare_assessment_ratings_cases],
    )
    def test_returns_expected_rows(self, rows, expected_data):
        input_lf = pl.LazyFrame(
            rows, schema=Schemas.assessment_ratings_input_schema, orient="row"
        )

        returned_df = job.prepare_assessment_ratings(input_lf).collect()

        expected_df = pl.LazyFrame(
            expected_data, schema=Schemas.assessment_ratings_output_schema
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df, check_row_order=False)

    def test_raises_when_overall_rating_is_populated(self):
        input_lf = pl.LazyFrame(
            Data.prepare_assessment_ratings_overall_rows,
            schema=Schemas.assessment_ratings_input_schema,
            orient="row",
        )

        with pytest.raises(ValueError, match="overall object within the assessments"):
            job.prepare_assessment_ratings(input_lf)


class TestRaiseErrorWhenAssessmentLfContainsOverallData:
    def test_raises_when_overall_rating_is_populated(self):
        input_lf = pl.LazyFrame(
            {
                CQCL.source_path: [job.OVERALL_SOURCE_PATH],
                CQCL.rating: ["Good"],
            }
        )

        with pytest.raises(ValueError, match="contains 1 values"):
            job.raise_error_when_assessment_lf_contains_overall_data(input_lf)

    @pytest.mark.parametrize(
        "source_path,rating",
        [
            pytest.param(job.OVERALL_SOURCE_PATH, None, id="null_overall_rating"),
            pytest.param(job.ASG_SOURCE_PATH, "Good", id="rating_only_in_asg"),
        ],
    )
    def test_does_not_raise_when_overall_has_no_rating(self, source_path, rating):
        input_lf = pl.LazyFrame(
            {CQCL.source_path: [source_path], CQCL.rating: [rating]},
            schema={CQCL.source_path: pl.String, CQCL.rating: pl.String},
        )

        job.raise_error_when_assessment_lf_contains_overall_data(input_lf)
