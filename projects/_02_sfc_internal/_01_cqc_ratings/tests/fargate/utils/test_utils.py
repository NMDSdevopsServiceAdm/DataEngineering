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

        returned_df = job.keep_latest_per_key(input_lf, "key", "order").collect()

        expected_df = pl.LazyFrame(
            {"key": ["a", "b"], "order": ["2024-02-01", "2024-01-01"], "value": [2, 3]}
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df, check_row_order=False)


class TestFilterToFirstImportOfMostRecentMonth:
    def test_filters_to_first_day_of_most_recent_month_when_first_day_is_the_1st(self):
        input_lf = pl.LazyFrame(
            {
                "year": ["2023", "2024"],
                "month": ["12", "01"],
                "day": ["01", "01"],
            }
        )

        returned_df = job.filter_to_first_import_of_most_recent_month(
            input_lf
        ).collect()

        expected_df = pl.LazyFrame(
            {"year": ["2024"], "month": ["01"], "day": ["01"]}
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df)

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

        returned_df = job.filter_to_first_import_of_most_recent_month(
            input_lf
        ).collect()

        expected_df = pl.LazyFrame(
            {"year": ["2024"], "month": ["01"], "day": ["01"]}
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df)

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

        returned_df = job.filter_to_first_import_of_most_recent_month(
            input_lf
        ).collect()

        expected_df = pl.LazyFrame(
            {"year": ["2024"], "month": ["01"], "day": ["02"]}
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df)


@dataclass
class PrepareCurrentRatingsCase:
    id: str
    rows: list
    expected_rows: list

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


prepare_current_ratings_cases = [
    PrepareCurrentRatingsCase(
        id="flattens_recodes_and_labels_as_current",
        rows=Data.current_ratings_rows,
        expected_rows=Data.expected_prepare_current_ratings_rows,
    ),
    PrepareCurrentRatingsCase(
        id="fills_missing_key_questions_with_null_when_fewer_than_five_present",
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
            rows,
            schema=Schemas.current_ratings_schema,
            orient="row",
        )

        returned_df = job.prepare_current_ratings(input_lf).collect()

        expected_df = pl.LazyFrame(
            expected_rows,
            schema=Schemas.flattened_ratings_with_current_or_historic_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df, check_row_order=False)


class TestPrepareHistoricRatings:
    def test_prepare_historic_ratings_pivots_recodes_and_labels_as_historic(self):
        input_lf = pl.LazyFrame(
            Data.historic_ratings_rows,
            schema=Schemas.historic_ratings_schema,
            orient="row",
        )

        returned_df = job.prepare_historic_ratings(input_lf).collect()

        expected_df = pl.LazyFrame(
            Data.expected_prepare_historic_ratings_rows,
            schema=Schemas.flattened_ratings_with_current_or_historic_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df, check_row_order=False)


@dataclass
class PrepareAssessmentRatingsCase:
    id: str
    rows: list
    expected_rows: list

    def as_pytest_param(self):
        return pytest.param(self.rows, self.expected_rows, id=self.id)


prepare_assessment_ratings_cases = [
    PrepareAssessmentRatingsCase(
        id="single_asg_rating_pivots_key_questions_into_columns",
        rows=Data.prepare_assessment_ratings_rows,
        expected_rows=Data.expected_prepare_assessment_ratings_rows,
    ),
    PrepareAssessmentRatingsCase(
        id="duplicate_key_question_in_raw_array_keeps_first_explode_order_deterministically",
        rows=Data.prepare_assessment_ratings_tiebreaker_rows,
        expected_rows=Data.expected_prepare_assessment_ratings_tiebreaker_rows,
    ),
    PrepareAssessmentRatingsCase(
        id="drops_row_when_key_question_ratings_is_null_not_empty",
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

        returned_df = job.prepare_assessment_ratings(input_lf).collect()

        expected_df = pl.LazyFrame(
            expected_rows,
            schema=Schemas.assessment_ratings_output_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df, check_row_order=False)


class TestRaiseErrorWhenAssessmentDfContainsOverallData:
    def test_raises_error_when_overall_object_is_populated(self):
        input_lf = pl.LazyFrame(
            Data.raise_error_overall_populated_rows,
            schema=Schemas.assessment_ratings_output_schema,
            orient="row",
        )

        with pytest.raises(ValueError, match="contains 1 values"):
            job.raise_error_when_assessment_df_contains_overall_data(input_lf)

    def test_does_not_raise_when_overall_object_is_empty(self):
        input_lf = pl.LazyFrame(
            Data.raise_error_overall_empty_rows,
            schema=Schemas.assessment_ratings_output_schema,
            orient="row",
        )

        result = job.raise_error_when_assessment_df_contains_overall_data(input_lf)

        assert result is None


class TestMergeCqcRatings:
    def test_merge_cqc_ratings_combines_assessment_and_standard_ratings(self):
        assessment_lf = pl.LazyFrame(
            Data.assessment_ratings_for_merging_rows,
            schema=Schemas.merge_assessment_ratings_schema,
            orient="row",
        )
        standard_lf = pl.LazyFrame(
            Data.standard_ratings_for_merging_rows,
            schema=Schemas.merge_standard_ratings_schema,
            orient="row",
        )

        returned_df = job.merge_cqc_ratings(assessment_lf, standard_lf).collect()

        expected_df = pl.LazyFrame(
            Data.expected_merge_cqc_ratings_rows,
            schema=Schemas.merge_expected_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df, check_row_order=False)


class TestRecodeUnknownCodesToNull:
    def test_recodes_non_rating_labels_to_null(self):
        input_lf = pl.LazyFrame(
            Data.recode_unknown_to_null_rows,
            schema=Schemas.flattened_ratings_schema,
            orient="row",
        )

        returned_df = job.recode_unknown_codes_to_null(input_lf).collect()

        expected_df = pl.LazyFrame(
            Data.expected_recode_unknown_to_null_rows,
            schema=Schemas.flattened_ratings_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df, check_row_order=False)


class TestAddCurrentOrHistoricColumn:
    def test_adds_current_label(self):
        input_lf = pl.LazyFrame(
            Data.add_current_or_historic_rows,
            schema=Schemas.flattened_ratings_schema,
            orient="row",
        )

        returned_df = job.add_current_or_historic_column(input_lf, "Current").collect()

        expected_df = pl.LazyFrame(
            Data.expected_add_current_rows,
            schema=Schemas.flattened_ratings_with_current_or_historic_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df)

    def test_adds_historic_label(self):
        input_lf = pl.LazyFrame(
            Data.add_current_or_historic_rows,
            schema=Schemas.flattened_ratings_schema,
            orient="row",
        )

        returned_df = job.add_current_or_historic_column(input_lf, "Historic").collect()

        expected_df = pl.LazyFrame(
            Data.expected_add_historic_rows,
            schema=Schemas.flattened_ratings_with_current_or_historic_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df)


class TestRemoveBlankAndDuplicateRows:
    def test_removes_rows_with_no_ratings_populated(self):
        input_lf = pl.LazyFrame(
            Data.remove_blank_rows_rows,
            schema=Schemas.flattened_ratings_schema,
            orient="row",
        )

        returned_df = job.remove_blank_and_duplicate_rows(input_lf).collect()

        expected_df = pl.LazyFrame(
            Data.expected_remove_blank_rows_rows,
            schema=Schemas.flattened_ratings_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df, check_row_order=False)


class TestAddRatingSequenceColumn:
    def test_adds_ascending_sequence_ordered_oldest_to_newest(self):
        input_lf = pl.LazyFrame(
            Data.add_rating_sequence_rows,
            schema=Schemas.ratings_with_assessment_date_schema,
            orient="row",
        )

        returned_df = job.add_rating_sequence_column(input_lf).collect()

        assert job.CQCRatings.rating_sequence in returned_df.columns

        sequence_by_date = dict(
            zip(
                returned_df[job.CQCRatings.date].to_list(),
                returned_df[job.CQCRatings.rating_sequence].to_list(),
            )
        )
        assert sequence_by_date["2023-01-01"] < sequence_by_date["2024-01-01"]

    def test_adds_descending_sequence_ordered_newest_to_oldest_when_reversed(self):
        input_lf = pl.LazyFrame(
            Data.add_rating_sequence_rows,
            schema=Schemas.ratings_with_assessment_date_schema,
            orient="row",
        )

        returned_df = job.add_rating_sequence_column(input_lf, reversed=True).collect()

        assert job.CQCRatings.reversed_rating_sequence in returned_df.columns

        sequence_by_date = dict(
            zip(
                returned_df[job.CQCRatings.date].to_list(),
                returned_df[job.CQCRatings.reversed_rating_sequence].to_list(),
            )
        )
        assert sequence_by_date["2024-01-01"] < sequence_by_date["2023-01-01"]


class TestAddLatestRatingFlagColumn:
    def test_flags_only_the_row_with_reversed_sequence_of_one(self):
        input_lf = pl.LazyFrame(
            Data.add_latest_rating_flag_rows,
            schema=Schemas.ratings_with_sequence_schema,
            orient="row",
        )

        returned_df = job.add_latest_rating_flag_column(input_lf).collect()

        flag_by_sequence = dict(
            zip(
                returned_df[job.CQCRatings.reversed_rating_sequence].to_list(),
                returned_df[job.CQCRatings.latest_rating_flag].to_list(),
            )
        )
        assert flag_by_sequence[1] == 1
        assert flag_by_sequence[2] == 0


class TestAddNumericalRatings:
    def test_add_numerical_ratings_returns_expected_values(self):
        input_lf = pl.LazyFrame(
            Data.add_numerical_ratings_rows,
            schema=Schemas.numerical_ratings_input_schema,
            orient="row",
        )

        returned_df = job.add_numerical_ratings(input_lf).collect()

        expected_df = pl.LazyFrame(
            Data.expected_add_numerical_ratings_rows,
            schema=Schemas.expected_numerical_ratings_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df, check_row_order=False)


class TestCreateStandardRatingsDataset:
    def test_selects_columns_and_deduplicates_rows(self):
        input_lf = pl.LazyFrame(
            Data.create_standard_ratings_dataset_rows,
            schema=Schemas.full_ratings_schema,
            orient="row",
        )

        returned_df = job.create_standard_ratings_dataset(input_lf).collect()

        assert returned_df.height == 1


class TestAddLocationIdHash:
    def test_adds_a_twenty_character_hash_of_the_location_id(self):
        input_lf = pl.LazyFrame(
            Data.location_id_hash_rows,
            schema=Schemas.location_id_hash_schema,
            orient="row",
        )

        returned_df = job.add_location_id_hash(input_lf).collect()

        assert job.CQCRatings.location_id_hash in returned_df.columns
        for location_id, location_hash in zip(
            returned_df[job.CQCL.location_id].to_list(),
            returned_df[job.CQCRatings.location_id_hash].to_list(),
        ):
            assert len(location_hash) == 20
            assert (
                location_hash
                == job.hashlib.sha256(location_id.encode()).hexdigest()[:20]
            )


class TestSelectRatingsForBenchmarks:
    def test_filters_to_registered_and_current_rating_only(self):
        input_lf = pl.LazyFrame(
            Data.select_ratings_for_benchmarks_rows,
            schema=Schemas.benchmarks_ratings_schema,
            orient="row",
        )

        returned_df = job.select_ratings_for_benchmarks(input_lf).collect()

        expected_df = pl.LazyFrame(
            Data.expected_select_ratings_for_benchmarks_rows,
            schema=Schemas.benchmarks_ratings_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(expected_df, returned_df, check_row_order=False)


class TestAddGoodAndOutstandingFlagColumn:
    def test_flags_locations_whose_minimum_overall_rating_value_is_at_least_good(self):
        input_lf = pl.LazyFrame(
            Data.add_good_or_outstanding_flag_rows,
            schema=Schemas.good_and_outstanding_input_schema,
            orient="row",
        )

        returned_df = job.add_good_and_outstanding_flag_column(input_lf).collect()

        flag_by_location = dict(
            zip(
                returned_df[job.CQCL.location_id].to_list(),
                returned_df[job.CQCRatings.good_or_outstanding_flag].to_list(),
            )
        )
        assert flag_by_location["1-001"] == 1
        assert flag_by_location["1-002"] == 0


class TestJoinEstablishmentIds:
    def test_joins_ascwds_establishment_id_onto_ratings_by_location_id(self):
        ratings_lf = pl.LazyFrame(
            Data.ratings_join_establishment_ids_rows,
            schema=Schemas.join_establishment_ids_input_schema,
            orient="row",
        )
        ascwds_lf = pl.LazyFrame(
            Data.ascwds_join_establishment_ids_rows,
            schema=Schemas.ascwds_join_establishment_ids_schema,
            orient="row",
        )

        returned_df = job.join_establishment_ids(ratings_lf, ascwds_lf).collect()

        expected_df = pl.LazyFrame(
            Data.expected_join_establishment_ids_rows,
            schema=Schemas.expected_join_establishment_ids_schema,
            orient="row",
        ).collect()
        pl_testing.assert_frame_equal(
            expected_df.sort(job.CQCL.location_id),
            returned_df.select(expected_df.columns).sort(job.CQCL.location_id),
        )


class TestCreateBenchmarkRatingsDataset:
    def test_selects_and_renames_columns_and_removes_incomplete_rows(self):
        input_lf = pl.LazyFrame(
            Data.create_benchmark_ratings_dataset_rows,
            schema=Schemas.create_benchmark_ratings_dataset_input_schema,
            orient="row",
        )

        returned_df = job.create_benchmark_ratings_dataset(input_lf).collect()

        assert returned_df.height == 1
        assert returned_df[job.CQCRatings.benchmarks_location_id].to_list() == ["1-001"]
