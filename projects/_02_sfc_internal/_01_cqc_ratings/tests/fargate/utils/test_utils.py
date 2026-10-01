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


@dataclass
class PrepareHistoricRatingsCase:
    id: str
    rows: list
    expected_rows: list

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


prepare_historic_ratings_cases = [
    PrepareHistoricRatingsCase(
        id="flattens_and_labels_as_historic",
        rows=Data.historic_ratings_rows,
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


@dataclass
class PrepareAssessmentRatingsCase:
    id: str
    rows: list
    expected_rows: list

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


prepare_assessment_ratings_cases = [
    PrepareAssessmentRatingsCase(
        id="flattens_asg_key_questions_into_columns",
        rows=Data.prepare_assessment_ratings_rows,
        expected_rows=Data.expected_prepare_assessment_ratings_rows,
    ),
    PrepareAssessmentRatingsCase(
        id="overall_rating_is_flattened_with_overall_source_path",
        rows=Data.prepare_assessment_ratings_overall_rows,
        expected_rows=Data.expected_prepare_assessment_ratings_overall_rows,
    ),
    PrepareAssessmentRatingsCase(
        id="keeps_locations_plans_and_sources_as_separate_rows",
        rows=Data.prepare_assessment_ratings_multiple_groups_rows,
        expected_rows=Data.expected_prepare_assessment_ratings_multiple_groups_rows,
    ),
    PrepareAssessmentRatingsCase(
        id="keeps_every_overall_and_asg_entry_within_one_assessment",
        rows=Data.prepare_assessment_ratings_multiple_entries_rows,
        expected_rows=Data.expected_prepare_assessment_ratings_multiple_entries_rows,
    ),
    PrepareAssessmentRatingsCase(
        id="drops_key_questions_that_are_not_one_of_the_five",
        rows=Data.prepare_assessment_ratings_unknown_key_question_rows,
        expected_rows=Data.expected_prepare_assessment_ratings_unknown_key_question_rows,
    ),
    PrepareAssessmentRatingsCase(
        id="drops_row_when_key_question_ratings_is_null_or_empty",
        rows=Data.prepare_assessment_ratings_null_key_question_ratings_rows,
        expected_rows=Data.expected_prepare_assessment_ratings_null_key_question_ratings_rows,
    ),
]


class TestPrepareAssessmentRatings:
    @pytest.mark.parametrize(
        "rows,expected_rows",
        [case.as_pytest_param() for case in prepare_assessment_ratings_cases],
    )
    def test_prepare_assessment_ratings_returns_expected_values(
        self, rows, expected_rows
    ):
        input_lf = pl.LazyFrame(
            rows, schema=Schemas.assessment_ratings_input_schema, orient="row"
        )

        returned_lf = job.prepare_assessment_ratings(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows, schema=Schemas.assessment_ratings_output_schema, orient="row"
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)


class TestRaiseErrorWhenKeyQuestionIsDuplicated:
    def test_raises_error_when_key_question_appears_twice_in_a_plan(self):
        input_lf = pl.LazyFrame(
            Data.raise_error_duplicated_key_question_rows,
            schema=Schemas.key_question_ratings_schema,
            orient="row",
        )

        with pytest.raises(
            ValueError, match="Found 1 key questions listed more than once"
        ):
            job.raise_error_when_key_question_is_duplicated(input_lf)

    def test_does_not_raise_when_each_key_question_appears_once_per_plan(self):
        input_lf = pl.LazyFrame(
            Data.raise_error_unique_key_question_rows,
            schema=Schemas.key_question_ratings_schema,
            orient="row",
        )

        result = job.raise_error_when_key_question_is_duplicated(input_lf)

        assert result is None


class TestPrepareAssessmentRatingsEdgeCases:
    def test_raises_error_when_key_question_is_duplicated_in_the_raw_data(self):
        input_lf = pl.LazyFrame(
            Data.prepare_assessment_ratings_duplicate_key_question_rows,
            schema=Schemas.assessment_ratings_input_schema,
            orient="row",
        )

        with pytest.raises(ValueError, match="listed more than once"):
            job.prepare_assessment_ratings(input_lf).collect()

    def test_returns_all_key_question_columns_when_there_are_no_assessments(self):
        input_lf = pl.LazyFrame(
            [], schema=Schemas.assessment_ratings_input_schema, orient="row"
        )

        returned_lf = job.prepare_assessment_ratings(input_lf)

        expected_lf = pl.LazyFrame(
            [], schema=Schemas.assessment_ratings_output_schema, orient="row"
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


class TestRaiseErrorWhenAssessmentDfContainsOverallData:
    def test_raises_error_when_overall_object_is_populated(self):
        input_lf = pl.LazyFrame(
            Data.raise_error_overall_populated_rows,
            schema=Schemas.assessment_ratings_output_schema,
            orient="row",
        )

        with pytest.raises(ValueError, match="contains 1 values"):
            job.raise_error_when_assessment_df_contains_overall_data(input_lf)

    def test_reports_the_number_of_populated_overall_rows_in_the_error(self):
        input_lf = pl.LazyFrame(
            Data.raise_error_overall_populated_rows * 2,
            schema=Schemas.assessment_ratings_output_schema,
            orient="row",
        )

        with pytest.raises(ValueError, match="contains 2 values"):
            job.raise_error_when_assessment_df_contains_overall_data(input_lf)

    def test_does_not_raise_when_overall_object_is_empty(self):
        input_lf = pl.LazyFrame(
            Data.raise_error_overall_empty_rows,
            schema=Schemas.assessment_ratings_output_schema,
            orient="row",
        )

        result = job.raise_error_when_assessment_df_contains_overall_data(input_lf)

        assert result is None
