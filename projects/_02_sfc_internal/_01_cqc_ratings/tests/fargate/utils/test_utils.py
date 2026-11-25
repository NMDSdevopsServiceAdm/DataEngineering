from dataclasses import dataclass, field
from typing import Any

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
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns as CQCRatings
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
        input_lf = pl.LazyFrame({CQCL.rating: ["Good"]})

        with pytest.raises(ValueError, match="contains 1 values"):
            job.raise_error_when_assessment_lf_contains_overall_data(input_lf)

    def test_does_not_raise_when_overall_has_no_rating(self):
        input_lf = pl.LazyFrame({CQCL.rating: [None]}, schema={CQCL.rating: pl.String})

        job.raise_error_when_assessment_lf_contains_overall_data(input_lf)


@dataclass
class RecodeUnknownCodesToNullCase:
    id: str
    rows: list
    expected_rows: list

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


recode_unknown_codes_to_null_cases = [
    RecodeUnknownCodesToNullCase(
        id="keeps_valid_ratings",
        rows=Data.recode_unknown_codes_valid_ratings_rows,
        expected_rows=Data.recode_unknown_codes_valid_ratings_rows,
    ),
    RecodeUnknownCodesToNullCase(
        id="nulls_every_unknown_label_in_each_rating_column",
        rows=Data.recode_unknown_codes_unknown_label_rows,
        expected_rows=Data.expected_recode_unknown_codes_unknown_label_rows,
    ),
    RecodeUnknownCodesToNullCase(
        id="keeps_null_ratings_null",
        rows=Data.recode_unknown_codes_null_ratings_rows,
        expected_rows=Data.recode_unknown_codes_null_ratings_rows,
    ),
    RecodeUnknownCodesToNullCase(
        id="leaves_non_rating_columns_unchanged",
        rows=Data.recode_unknown_codes_non_rating_columns_rows,
        expected_rows=Data.expected_recode_unknown_codes_non_rating_columns_rows,
    ),
]


class TestRecodeUnknownCodesToNull:
    @pytest.mark.parametrize(
        "rows,expected_rows",
        [case.as_pytest_param() for case in recode_unknown_codes_to_null_cases],
    )
    def test_returns_expected_values(self, rows, expected_rows):
        input_lf = pl.LazyFrame(
            rows,
            schema=Schemas.flattened_ratings_with_current_or_historic_schema,
            orient="row",
        )

        returned_lf = job.recode_unknown_codes_to_null(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.flattened_ratings_with_current_or_historic_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


@dataclass
class RemoveBlankAndDuplicateRowsCase:
    id: str
    rows: list
    expected_rows: list

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


remove_blank_and_duplicate_rows_cases = [
    RemoveBlankAndDuplicateRowsCase(
        id="keeps_row_when_any_one_rating_column_is_populated",
        rows=Data.remove_blank_rows_one_rating_populated_rows,
        expected_rows=Data.remove_blank_rows_one_rating_populated_rows,
    ),
    RemoveBlankAndDuplicateRowsCase(
        id="removes_row_when_all_ratings_null",
        rows=Data.remove_blank_rows_all_ratings_null_rows,
        expected_rows=Data.expected_remove_blank_rows_all_ratings_null_rows,
    ),
    RemoveBlankAndDuplicateRowsCase(
        id="removes_exact_duplicate_rows",
        rows=Data.remove_blank_rows_duplicate_rows,
        expected_rows=Data.expected_remove_blank_rows_duplicate_rows,
    ),
    RemoveBlankAndDuplicateRowsCase(
        id="keeps_rows_that_differ_in_one_column",
        rows=Data.remove_blank_rows_differing_rows,
        expected_rows=Data.remove_blank_rows_differing_rows,
    ),
]


class TestRemoveBlankAndDuplicateRows:
    @pytest.mark.parametrize(
        "rows,expected_rows",
        [case.as_pytest_param() for case in remove_blank_and_duplicate_rows_cases],
    )
    def test_returns_expected_values(self, rows, expected_rows):
        input_lf = pl.LazyFrame(
            rows,
            schema=Schemas.flattened_ratings_with_current_or_historic_schema,
            orient="row",
        )

        returned_lf = job.remove_blank_and_duplicate_rows(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.flattened_ratings_with_current_or_historic_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)


@dataclass
class AddNumericalRatingsCase:
    id: str
    rows: list
    expected_rows: list

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


add_numerical_ratings_cases = [
    AddNumericalRatingsCase(
        id="maps_each_rating_to_its_value",
        rows=Data.add_numerical_ratings_rating_rows,
        expected_rows=Data.expected_add_numerical_ratings_rating_rows,
    ),
    AddNumericalRatingsCase(
        id="matches_rating_case_insensitively",
        rows=Data.add_numerical_ratings_mixed_case_rows,
        expected_rows=Data.expected_add_numerical_ratings_mixed_case_rows,
    ),
    AddNumericalRatingsCase(
        id="maps_null_to_zero",
        rows=Data.add_numerical_ratings_null_rows,
        expected_rows=Data.expected_add_numerical_ratings_null_rows,
    ),
    AddNumericalRatingsCase(
        id="maps_unrecognised_rating_to_zero",
        rows=Data.add_numerical_ratings_unrecognised_rows,
        expected_rows=Data.expected_add_numerical_ratings_unrecognised_rows,
    ),
    AddNumericalRatingsCase(
        id="total_sums_five_key_questions_excluding_overall",
        rows=Data.add_numerical_ratings_total_rows,
        expected_rows=Data.expected_add_numerical_ratings_total_rows,
    ),
]


class TestAddNumericalRatings:
    @pytest.mark.parametrize(
        "rows,expected_rows",
        [case.as_pytest_param() for case in add_numerical_ratings_cases],
    )
    def test_returns_expected_values(self, rows, expected_rows):
        input_lf = pl.LazyFrame(
            rows, schema=Schemas.add_numerical_ratings_schema, orient="row"
        )

        returned_lf = job.add_numerical_ratings(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.expected_add_numerical_ratings_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


@dataclass
class AddLatestRatingFlagColumnCase:
    id: str
    rows: list
    expected_rows: list

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


add_latest_rating_flag_column_cases = [
    AddLatestRatingFlagColumnCase(
        id="flags_row_with_latest_rating_date",
        rows=Data.add_latest_rating_flag_latest_date_rows,
        expected_rows=Data.expected_add_latest_rating_flag_latest_date_rows,
    ),
    AddLatestRatingFlagColumnCase(
        id="flags_each_location_independently",
        rows=Data.add_latest_rating_flag_two_locations_rows,
        expected_rows=Data.expected_add_latest_rating_flag_two_locations_rows,
    ),
    AddLatestRatingFlagColumnCase(
        id="uses_assessment_date_when_rating_dates_tie",
        rows=Data.add_latest_rating_flag_assessment_date_rows,
        expected_rows=Data.expected_add_latest_rating_flag_assessment_date_rows,
    ),
    AddLatestRatingFlagColumnCase(
        id="ranks_null_assessment_date_below_populated_date",
        rows=Data.add_latest_rating_flag_null_assessment_date_rows,
        expected_rows=Data.expected_add_latest_rating_flag_null_assessment_date_rows,
    ),
    AddLatestRatingFlagColumnCase(
        id="flags_one_row_per_location_when_location_rows_are_not_adjacent",
        rows=Data.add_latest_rating_flag_non_adjacent_rows,
        expected_rows=Data.expected_add_latest_rating_flag_non_adjacent_rows,
    ),
    AddLatestRatingFlagColumnCase(
        id="ranks_null_rating_date_last",
        rows=Data.add_latest_rating_flag_null_rating_date_rows,
        expected_rows=Data.expected_add_latest_rating_flag_null_rating_date_rows,
    ),
]


class TestAddLatestRatingFlagColumn:
    @pytest.mark.parametrize(
        "rows,expected_rows",
        [case.as_pytest_param() for case in add_latest_rating_flag_column_cases],
    )
    def test_returns_expected_values(self, rows, expected_rows):
        input_lf = pl.LazyFrame(
            rows, schema=Schemas.add_latest_rating_flag_schema, orient="row"
        )

        returned_lf = job.add_latest_rating_flag_column(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.expected_add_latest_rating_flag_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)

    def test_flags_exactly_one_row_when_both_dates_tie(self):
        input_lf = pl.LazyFrame(
            Data.add_latest_rating_flag_tied_rows,
            schema=Schemas.add_latest_rating_flag_schema,
            orient="row",
        )

        returned_df = job.add_latest_rating_flag_column(input_lf).collect()

        assert returned_df[CQCRatings.latest_rating_flag].sum() == 1

    def test_tie_break_is_independent_of_input_row_order(self):
        rows = Data.add_latest_rating_flag_tied_rows

        flagged_lfs = [
            job.add_latest_rating_flag_column(
                pl.LazyFrame(
                    ordered_rows,
                    schema=Schemas.add_latest_rating_flag_schema,
                    orient="row",
                )
            ).filter(pl.col(CQCRatings.latest_rating_flag) == 1)
            for ordered_rows in (rows, rows[::-1])
        ]

        pl_testing.assert_frame_equal(*flagged_lfs)


@dataclass
class AddGoodAndOutstandingFlagColumnTestCase:
    id: str
    expected_rows: list[Any]

    def as_pytest_param(self):
        return pytest.param(self.expected_rows, id=self.id)


add_good_and_outstanding_flag_column_cases = [
    AddGoodAndOutstandingFlagColumnTestCase(
        id="flags_one_when_min_rating_is_good_or_outstanding",
        expected_rows=Data.expected_add_good_and_outstanding_flag_good_rows,
    ),
    AddGoodAndOutstandingFlagColumnTestCase(
        id="flags_zero_when_any_rating_below_good",
        expected_rows=Data.expected_add_good_and_outstanding_flag_below_good_rows,
    ),
    AddGoodAndOutstandingFlagColumnTestCase(
        id="applies_flag_to_every_row_of_location",
        expected_rows=Data.expected_add_good_and_outstanding_flag_two_locations_rows,
    ),
    AddGoodAndOutstandingFlagColumnTestCase(
        id="flags_zero_when_overall_rating_value_is_null",
        expected_rows=Data.expected_add_good_and_outstanding_flag_null_rows,
    ),
    AddGoodAndOutstandingFlagColumnTestCase(
        id="ignores_null_when_other_rating_is_good",
        expected_rows=Data.expected_add_good_and_outstanding_flag_null_and_good_rows,
    ),
    AddGoodAndOutstandingFlagColumnTestCase(
        id="ignores_null_when_other_rating_is_below_good",
        expected_rows=Data.expected_add_good_and_outstanding_flag_null_and_below_good_rows,
    ),
]


class TestAddGoodAndOutstandingFlagColumn:
    @pytest.mark.parametrize(
        "expected_rows",
        [case.as_pytest_param() for case in add_good_and_outstanding_flag_column_cases],
    )
    def test_adds_expected_flag(self, expected_rows):
        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.expected_add_good_and_outstanding_flag_schema,
            orient="row",
        )
        input_lf = expected_lf.drop(CQCRatings.good_or_outstanding_flag)

        returned_lf = job.add_good_and_outstanding_flag_column(input_lf)

        pl_testing.assert_frame_equal(expected_lf, returned_lf)


@dataclass
class JoinEstablishmentIdsTestCase:
    id: str
    ratings_rows: list[Any]
    ascwds_rows: list[Any]
    expected_rows: list[Any]

    def as_pytest_param(self):
        return pytest.param(
            self.ratings_rows, self.ascwds_rows, self.expected_rows, id=self.id
        )


join_establishment_ids_cases = [
    JoinEstablishmentIdsTestCase(
        id="adds_establishment_id_for_matching_location",
        ratings_rows=Data.join_establishment_ids_matching_ratings_rows,
        ascwds_rows=Data.join_establishment_ids_matching_ascwds_rows,
        expected_rows=Data.expected_join_establishment_ids_matching_rows,
    ),
    JoinEstablishmentIdsTestCase(
        id="null_establishment_id_when_location_not_in_ascwds",
        ratings_rows=Data.join_establishment_ids_unmatched_ratings_rows,
        ascwds_rows=Data.join_establishment_ids_unmatched_ascwds_rows,
        expected_rows=Data.expected_join_establishment_ids_unmatched_rows,
    ),
    JoinEstablishmentIdsTestCase(
        id="does_not_duplicate_rows_when_ascwds_repeats_location_id",
        ratings_rows=Data.join_establishment_ids_repeated_ratings_rows,
        ascwds_rows=Data.join_establishment_ids_repeated_ascwds_rows,
        expected_rows=Data.expected_join_establishment_ids_repeated_rows,
    ),
]


class TestJoinEstablishmentIds:
    @pytest.mark.parametrize(
        "ratings_rows, ascwds_rows, expected_rows",
        [case.as_pytest_param() for case in join_establishment_ids_cases],
    )
    def test_returns_ratings_with_expected_establishment_ids(
        self, ratings_rows, ascwds_rows, expected_rows
    ):
        ratings_lf = pl.LazyFrame(
            ratings_rows,
            schema=Schemas.join_establishment_ids_ratings_schema,
            orient="row",
        )
        ascwds_lf = pl.LazyFrame(
            ascwds_rows,
            schema=Schemas.join_establishment_ids_ascwds_schema,
            orient="row",
        )

        returned_lf = job.join_establishment_ids(ratings_lf, ascwds_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.expected_join_establishment_ids_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


@dataclass
class CreateBenchmarkRatingsDatasetTestCase:
    id: str
    rows: list[Any]
    expected_rows: list[Any]

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


create_benchmark_ratings_dataset_cases = [
    CreateBenchmarkRatingsDatasetTestCase(
        id="renames_and_orders_columns",
        rows=Data.create_benchmark_ratings_dataset_rows,
        expected_rows=Data.expected_create_benchmark_ratings_dataset_rows,
    ),
    CreateBenchmarkRatingsDatasetTestCase(
        id="removes_rows_with_null_establishment_id",
        rows=Data.create_benchmark_ratings_dataset_null_establishment_id_rows,
        expected_rows=Data.expected_create_benchmark_ratings_dataset_null_establishment_id_rows,
    ),
    CreateBenchmarkRatingsDatasetTestCase(
        id="removes_rows_with_null_overall_rating",
        rows=Data.create_benchmark_ratings_dataset_null_overall_rating_rows,
        expected_rows=Data.expected_create_benchmark_ratings_dataset_null_overall_rating_rows,
    ),
]


class TestCreateBenchmarkRatingsDataset:
    @pytest.mark.parametrize(
        "rows, expected_rows",
        [case.as_pytest_param() for case in create_benchmark_ratings_dataset_cases],
    )
    def test_returns_expected_benchmark_ratings(self, rows, expected_rows):
        input_lf = pl.LazyFrame(
            rows,
            schema=Schemas.create_benchmark_ratings_dataset_schema,
            orient="row",
        )

        returned_lf = job.create_benchmark_ratings_dataset(input_lf)

        expected_lf = pl.LazyFrame(
            expected_rows,
            schema=Schemas.expected_create_benchmark_ratings_dataset_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


class TestCreateStandardRatingsDataset:
    def test_keeps_only_standard_columns_in_order(self):
        input_lf = pl.LazyFrame(
            Data.create_standard_ratings_dataset_rows,
            schema=Schemas.create_standard_ratings_dataset_schema,
            orient="row",
        )

        returned_lf = job.create_standard_ratings_dataset(input_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_create_standard_ratings_dataset_rows,
            schema=Schemas.standard_ratings_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)

    def test_removes_duplicate_rows_left_after_dropping_columns(self):
        input_lf = pl.LazyFrame(
            Data.create_standard_ratings_dataset_duplicate_rows,
            schema=Schemas.create_standard_ratings_dataset_schema,
            orient="row",
        )

        returned_lf = job.create_standard_ratings_dataset(input_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_create_standard_ratings_dataset_duplicate_rows,
            schema=Schemas.standard_ratings_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)

    def test_removes_duplicate_rows_containing_nulls(self):
        input_lf = pl.LazyFrame(
            Data.create_standard_ratings_dataset_null_rows,
            schema=Schemas.create_standard_ratings_dataset_schema,
            orient="row",
        )

        returned_lf = job.create_standard_ratings_dataset(input_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_create_standard_ratings_dataset_null_rows,
            schema=Schemas.standard_ratings_schema,
            orient="row",
        )
        pl_testing.assert_frame_equal(expected_lf, returned_lf)


@dataclass
class AddLocationIdHashTestCase:
    id: str
    rows: list[Any]
    expected_rows: list[Any]
    schema: pl.Schema = field(default_factory=lambda: Schemas.location_id_hash_schema)
    expected_schema: pl.Schema = field(
        default_factory=lambda: Schemas.expected_location_id_hash_schema
    )

    def as_pytest_param(self):
        return pytest.param(
            self.rows,
            self.expected_rows,
            self.schema,
            self.expected_schema,
            id=self.id,
        )


add_location_id_hash_cases = [
    AddLocationIdHashTestCase(
        id="hashes_short_location_id",
        rows=Data.location_id_hash_short_rows,
        expected_rows=Data.expected_location_id_hash_short_rows,
    ),
    AddLocationIdHashTestCase(
        id="hashes_long_location_id",
        rows=Data.location_id_hash_long_rows,
        expected_rows=Data.expected_location_id_hash_long_rows,
    ),
    AddLocationIdHashTestCase(
        id="gives_repeated_location_ids_the_same_hash",
        rows=Data.location_id_hash_repeated_rows,
        expected_rows=Data.expected_location_id_hash_repeated_rows,
    ),
    AddLocationIdHashTestCase(
        id="keeps_other_columns_unchanged",
        rows=Data.location_id_hash_other_column_rows,
        expected_rows=Data.expected_location_id_hash_other_column_rows,
        schema=Schemas.location_id_hash_other_column_schema,
        expected_schema=Schemas.expected_location_id_hash_other_column_schema,
    ),
    AddLocationIdHashTestCase(
        id="hashes_categorical_location_id",
        rows=Data.location_id_hash_categorical_rows,
        expected_rows=Data.expected_location_id_hash_categorical_rows,
        schema=Schemas.location_id_hash_categorical_schema,
        expected_schema=Schemas.expected_location_id_hash_categorical_schema,
    ),
    AddLocationIdHashTestCase(
        id="returns_null_hash_when_location_id_is_null",
        rows=Data.location_id_hash_null_rows,
        expected_rows=Data.expected_location_id_hash_null_rows,
    ),
]


class TestAddLocationIdHash:
    @pytest.mark.parametrize(
        "rows, expected_rows, schema, expected_schema",
        [case.as_pytest_param() for case in add_location_id_hash_cases],
    )
    def test_adds_expected_location_id_hash(
        self, rows, expected_rows, schema, expected_schema
    ):
        input_lf = pl.LazyFrame(rows, schema=schema, orient="row")

        returned_lf = job.add_location_id_hash(input_lf)

        expected_lf = pl.LazyFrame(expected_rows, schema=expected_schema, orient="row")
        pl_testing.assert_frame_equal(expected_lf, returned_lf, check_row_order=False)
