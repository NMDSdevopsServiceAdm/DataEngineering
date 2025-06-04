import unittest
from unittest.mock import ANY, Mock, patch, call

import projects._02_sfc_internal.cqc_ratings.jobs.flatten_cqc_ratings as job
from projects._02_sfc_internal.unittest_data.sfc_test_file_data import (
    FlattenCQCRatings as Data,
)
from projects._02_sfc_internal.unittest_data.sfc_test_file_schemas import (
    FlattenCQCRatings as Schema,
)

from utils import utils
from utils.column_names.cqc_ratings_columns import (
    CQCRatingsColumns as CQCRatings,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_values.categorical_column_values import (
    CQCCurrentOrHistoricValues,
)


PATCH_PATH = "projects._02_sfc_internal.cqc_ratings.jobs.flatten_cqc_ratings"


class FlattenCQCRatingsTests(unittest.TestCase):
    TEST_LOCATIONS_SOURCE = "some/directory"
    TEST_WORKPLACE_SOURCE = "some/directory"
    TEST_CQC_RATINGS_DESTINATION = "some/other/directory"
    TEST_BENCHMARK_RATINGS_DESTINATION = "some/other/directory"

    def setUp(self) -> None:
        self.spark = utils.get_spark()
        self.test_cqc_locations_df = self.spark.createDataFrame(
            Data.test_cqc_locations_rows, schema=Schema.test_cqc_locations_schema
        )
        self.test_ascwds_df = self.spark.createDataFrame(
            Data.test_ascwds_workplace_rows, schema=Schema.test_ascwds_workplace_schema
        )


class MainTests(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.utils.write_to_parquet")
    @patch(f"{PATCH_PATH}.utils.read_from_parquet")
    def test_main(self, read_from_parquet_patch: Mock, write_to_parquet_patch: Mock):
        read_from_parquet_patch.side_effect = [
            self.test_cqc_locations_df,
            self.test_ascwds_df,
        ]
        job.main(
            self.TEST_LOCATIONS_SOURCE,
            self.TEST_WORKPLACE_SOURCE,
            self.TEST_CQC_RATINGS_DESTINATION,
            self.TEST_BENCHMARK_RATINGS_DESTINATION,
        )
        self.assertEqual(read_from_parquet_patch.call_count, 2)
        self.assertEqual(write_to_parquet_patch.call_count, 2)
        expected_write_to_parquet_calls = [
            call(
                ANY,
                self.TEST_CQC_RATINGS_DESTINATION,
                mode="overwrite",
            ),
            call(
                ANY,
                self.TEST_BENCHMARK_RATINGS_DESTINATION,
                mode="overwrite",
            ),
        ]
        write_to_parquet_patch.assert_has_calls(
            expected_write_to_parquet_calls,
            any_order=True,
        )


class FilterToFirstImportOfMostRecentMonth(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_cqc_df = self.spark.createDataFrame(
            Data.filter_to_first_import_of_most_recent_month_rows,
            Schema.filter_to_first_import_of_most_recent_month_schema,
        )
        self.test_cqc_when_two_imports_in_most_recent_month_df = self.spark.createDataFrame(
            Data.filter_to_first_import_of_most_recent_month_when_two_imports_in_most_recent_month_rows,
            Schema.filter_to_first_import_of_most_recent_month_schema,
        )
        self.test_cqc_when_earliest_date_is_not_first_of_month_df = self.spark.createDataFrame(
            Data.filter_to_first_import_of_most_recent_month_when_earliest_date_is_not_first_of_month_rows,
            Schema.filter_to_first_import_of_most_recent_month_schema,
        )
        self.expected_data = self.spark.createDataFrame(
            Data.expected_filter_to_first_import_of_most_recent_month_rows,
            Schema.filter_to_first_import_of_most_recent_month_schema,
        ).collect()
        self.expected_data_when_not_first_of_month = self.spark.createDataFrame(
            Data.expected_filter_to_first_import_of_most_recent_month_when_earliest_date_is_not_first_of_month_rows,
            Schema.filter_to_first_import_of_most_recent_month_schema,
        ).collect()

    def test_filter_to_first_import_of_most_recent_month_returns_correct_rows_when_most_recent_data_is_the_first_of_the_month(
        self,
    ):
        returned_data = job.filter_to_first_import_of_most_recent_month(
            self.test_cqc_df
        ).collect()
        self.assertEqual(returned_data, self.expected_data)

    def test_filter_to_first_import_of_most_recent_month_returns_correct_rows_when_most_recent_data_is_not_the_first_of_the_month(
        self,
    ):
        returned_data = job.filter_to_first_import_of_most_recent_month(
            self.test_cqc_when_two_imports_in_most_recent_month_df
        ).collect()
        self.assertEqual(returned_data, self.expected_data)

    def test_filter_to_first_import_of_most_recent_month_returns_correct_rows_when_earliest_date_in_month_is_not_the_first_of_the_month(
        self,
    ):
        returned_data = job.filter_to_first_import_of_most_recent_month(
            self.test_cqc_when_earliest_date_is_not_first_of_month_df
        ).collect()
        self.assertEqual(returned_data, self.expected_data_when_not_first_of_month)


class FlattenCurrentRatings(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_cqc_current_ratings_df = self.spark.createDataFrame(
            Data.flatten_current_ratings_rows, Schema.flatten_current_ratings_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_flatten_ratings_rows,
            Schema.expected_flatten_ratings_schema,
        )
        self.returned_df = job.flatten_current_ratings(self.test_cqc_current_ratings_df)

    def test_flatten_current_ratings_returns_correct_columns(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = len(self.expected_df.columns)
        self.assertEqual(returned_columns, expected_columns)

    def test_flatten_current_ratings_returns_correct_rows(self):
        returned_rows = self.returned_df.count()
        expected_rows = self.expected_df.count()
        self.assertEqual(returned_rows, expected_rows)

    def test_flatten_current_ratings_returns_correct_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class FlattenHistoricRatings(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_cqc_historic_ratings_df = self.spark.createDataFrame(
            Data.flatten_historic_ratings_rows, Schema.flatten_historic_ratings_schema
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_flatten_ratings_rows,
            Schema.expected_flatten_ratings_schema,
        )
        self.returned_df = job.flatten_historic_ratings(
            self.test_cqc_historic_ratings_df
        )

    def test_flatten_historic_ratings_returns_correct_columns(self):
        returned_columns = len(self.returned_df.columns)
        expected_columns = len(self.expected_df.columns)
        self.assertEqual(returned_columns, expected_columns)

    def test_flatten_historic_ratings_returns_correct_rows(self):
        returned_rows = self.returned_df.count()
        expected_rows = self.expected_df.count()
        self.assertEqual(returned_rows, expected_rows)

    def test_flatten_historic_ratings_returns_correct_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class RecodeUnknownToNull(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_current_ratings_df = self.spark.createDataFrame(
            Data.recode_unknown_to_null_rows,
            Schema.expected_flatten_ratings_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_recode_unknown_to_null_rows,
            Schema.expected_flatten_ratings_schema,
        )
        self.returned_df = job.recode_unknown_codes_to_null(
            self.test_current_ratings_df
        )

    def test_recode_unknown_codes_to_null_returns_correct_values(self):
        returned_data = self.returned_df.sort(CQCL.location_id).collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class AddCurrentOrHistoricColumn(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.add_current_or_historic_rows, Schema.add_current_or_historic_schema
        )
        self.expected_current_df = self.spark.createDataFrame(
            Data.expected_add_current_rows,
            Schema.expected_add_current_or_historic_schema,
        )
        self.expected_historic_df = self.spark.createDataFrame(
            Data.expected_add_historic_rows,
            Schema.expected_add_current_or_historic_schema,
        )
        self.returned_current_df = job.add_current_or_historic_column(
            self.test_ratings_df, CQCCurrentOrHistoricValues.current
        )
        self.returned_historic_df = job.add_current_or_historic_column(
            self.test_ratings_df, CQCCurrentOrHistoricValues.historic
        )

    def test_add_current_or_historic_column_returns_correct_values_when_passed_current(
        self,
    ):
        returned_data = self.returned_current_df.collect()
        expected_data = self.expected_current_df.collect()
        self.assertEqual(returned_data, expected_data)

    def test_add_current_or_historic_column_returns_correct_values_when_passed_historic(
        self,
    ):
        returned_data = self.returned_historic_df.collect()
        expected_data = self.expected_historic_df.collect()
        self.assertEqual(returned_data, expected_data)


class RemoveBlankRows(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.remove_blank_rows_rows,
            Schema.remove_blank_rows_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_remove_blank_rows_rows,
            Schema.remove_blank_rows_schema,
        )
        self.returned_df = job.remove_blank_and_duplicate_rows(self.test_ratings_df)

    def test_remove_blank_rows_returns_correct_values(self):
        returned_data = self.returned_df.sort(CQCL.location_id).collect()
        expected_data = self.expected_df.sort(CQCL.location_id).collect()
        self.assertEqual(returned_data, expected_data)


class AddRatingSequenceColumn(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.add_rating_sequence_rows,
            Schema.add_rating_sequence_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_rating_sequence_rows,
            Schema.expected_add_rating_sequence_schema,
        )
        self.expected_reversed_df = self.spark.createDataFrame(
            Data.expected_reversed_add_rating_sequence_rows,
            Schema.expected_reversed_add_rating_sequence_schema,
        )
        self.returned_df = job.add_rating_sequence_column(self.test_ratings_df)
        self.returned_reversed_df = job.add_rating_sequence_column(
            self.test_ratings_df, reversed=True
        )

    def test_add_rating_sequence_column_adds_a_new_column_called_rating_sequence(self):
        returned_columns = self.returned_df.columns
        expected_columns = self.expected_df.columns
        expected_new_column = CQCRatings.rating_sequence
        self.assertEqual(len(returned_columns), len(expected_columns))
        self.assertTrue(expected_new_column in returned_columns)

    def test_add_rating_sequence_column_returns_correct_values(self):
        returned_data = self.returned_df.sort(
            CQCL.location_id, CQCRatings.date
        ).collect()
        expected_data = self.expected_df.sort(
            CQCL.location_id, CQCRatings.date
        ).collect()
        self.assertEqual(returned_data, expected_data)

    def test_add_rating_sequence_column_adds_a_new_column_called_reversed_rating_sequence_when_reversed_equals_true(
        self,
    ):
        returned_columns = self.returned_reversed_df.columns
        expected_columns = self.expected_reversed_df.columns
        expected_new_column = CQCRatings.reversed_rating_sequence
        self.assertEqual(len(returned_columns), len(expected_columns))
        self.assertTrue(expected_new_column in returned_columns)

    def test_add_rating_sequence_column_returns_correct_values_when_reversed_equals_true(
        self,
    ):
        returned_data = self.returned_reversed_df.sort(
            CQCL.location_id, CQCRatings.date
        ).collect()
        expected_data = self.expected_reversed_df.sort(
            CQCL.location_id, CQCRatings.date
        ).collect()
        self.assertEqual(returned_data, expected_data)


class AddLatestRatingFlagColumn(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.add_latest_rating_flag_rows,
            Schema.add_latest_rating_flag_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_latest_rating_flag_rows,
            Schema.expected_add_latest_rating_flag_schema,
        )
        self.returned_df = job.add_latest_rating_flag_column(self.test_ratings_df)

    def test_add_latest_rating_flag_column_adds_a_new_column_called_rating_sequence(
        self,
    ):
        returned_columns = self.returned_df.columns
        expected_columns = self.expected_df.columns
        expected_new_column = CQCRatings.latest_rating_flag
        self.assertEqual(len(returned_columns), len(expected_columns))
        self.assertTrue(expected_new_column in returned_columns)

    def test_add_latest_rating_flag_column_returns_correct_values(self):
        returned_data = self.returned_df.sort(
            CQCL.location_id, CQCRatings.reversed_rating_sequence
        ).collect()
        expected_data = self.expected_df.sort(
            CQCL.location_id, CQCRatings.reversed_rating_sequence
        ).collect()
        self.assertEqual(returned_data, expected_data)


class CreateStandardRatingsDataset(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.create_standard_rating_dataset_rows,
            Schema.create_standard_ratings_dataset_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_standard_rating_dataset_rows,
            Schema.expected_create_standard_ratings_dataset_schema,
        )
        self.returned_df = job.create_standard_ratings_dataset(self.test_ratings_df)

    def test_create_standard_ratings_dataset_selects_correct_columns(self):
        returned_columns = self.returned_df.columns
        expected_columns = self.expected_df.columns
        self.assertEqual(sorted(returned_columns), sorted(expected_columns))

    def test_create_standard_ratings_dataset_deduplicates_rows(self):
        returned_rows = self.returned_df.count()
        expected_rows = self.expected_df.count()
        self.assertEqual(returned_rows, expected_rows)


class AddLocationIdHash(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.location_id_hash_rows,
            Schema.location_id_hash_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_location_id_hash_rows,
            Schema.expected_location_id_hash_schema,
        )
        self.returned_df = job.add_location_id_hash(self.test_ratings_df)

    def test_add_location_id_hash_adds_a_column_called_identifier(self):
        returned_columns = self.returned_df.columns
        expected_columns = self.expected_df.columns
        self.assertEqual(returned_columns, expected_columns)

    def test_add_location_id_hash_adds_a_column_with_expected_hashed_ids_when_location_id_has_ten_digits(
        self,
    ):
        test_ratings_df = self.spark.createDataFrame(
            Data.location_id_hash_ten_digit_rows,
            Schema.location_id_hash_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_location_id_hash_ten_digit_rows,
            Schema.expected_location_id_hash_schema,
        )
        returned_df = job.add_location_id_hash(test_ratings_df)

        self.assertEqual(
            returned_df.sort(CQCL.location_id).collect(),
            expected_df.collect(),
        )

    def test_add_location_id_hash_adds_a_column_with_expected_hashed_ids_when_location_id_has_eleven_digits(
        self,
    ):
        test_ratings_df = self.spark.createDataFrame(
            Data.location_id_hash_eleven_digit_rows,
            Schema.location_id_hash_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_location_id_hash_eleven_digit_rows,
            Schema.expected_location_id_hash_schema,
        )
        returned_df = job.add_location_id_hash(test_ratings_df)

        self.assertEqual(
            returned_df.sort(CQCL.location_id).collect(),
            expected_df.collect(),
        )

    def test_add_location_id_hash_adds_a_column_with_expected_hashed_ids_when_location_id_has_twelve_digits(
        self,
    ):
        test_ratings_df = self.spark.createDataFrame(
            Data.location_id_hash_twelve_digit_rows,
            Schema.location_id_hash_schema,
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_location_id_hash_twelve_digit_rows,
            Schema.expected_location_id_hash_schema,
        )
        returned_df = job.add_location_id_hash(test_ratings_df)

        self.assertEqual(
            returned_df.sort(CQCL.location_id).collect(),
            expected_df.collect(),
        )


class SelectRatingsForBenchmarks(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.select_ratings_for_benchmarks_rows,
            Schema.select_ratings_for_benchmarks_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_select_ratings_for_benchmarks_rows,
            Schema.select_ratings_for_benchmarks_schema,
        )
        self.returned_df = job.select_ratings_for_benchmarks(self.test_ratings_df)

    def test_select_ratings_for_benchmarks_removes_correct_rows(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class AddGoodAndOutstandingFlagColumn(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.add_good_or_outstanding_flag_rows,
            Schema.add_good_and_outstanding_flag_column_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_add_good_or_outstanding_flag_rows,
            Schema.expected_add_good_and_outstanding_flag_column_schema,
        )
        self.returned_df = job.add_good_and_outstanding_flag_column(
            self.test_ratings_df
        )

    def test_add_good_or_outstanding_column_adds_a_new_column_called_good_or_outstanding(
        self,
    ):
        returned_columns = self.returned_df.columns
        expected_columns = self.expected_df.columns
        expected_new_column = CQCRatings.good_or_outstanding_flag
        self.assertEqual(len(returned_columns), len(expected_columns))
        self.assertTrue(expected_new_column in returned_columns)

    def test_add_good_or_outstanding_column_returns_correct_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class JoinEstablishmentIds(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.ratings_join_establishment_ids_rows,
            Schema.ratings_join_establishment_ids_schema,
        )
        self.test_ascwds_df = self.spark.createDataFrame(
            Data.ascwds_join_establishment_ids_rows,
            Schema.ascwds_join_establishment_ids_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_join_establishment_ids_rows,
            Schema.expected_join_establishment_ids_schema,
        )
        self.returned_df = job.join_establishment_ids(
            self.test_ratings_df, self.test_ascwds_df
        )

    def test_join_establishment_ids_returns_correct_values(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class CreateBenchmarkRatingsDataset(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()
        self.test_ratings_df = self.spark.createDataFrame(
            Data.create_benchmark_ratings_dataset_rows,
            Schema.create_benchmark_ratings_dataset_schema,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_benchmark_ratings_dataset_rows,
            Schema.expected_create_benchmark_ratings_dataset_schema,
        )
        self.returned_df = job.create_benchmark_ratings_dataset(self.test_ratings_df)

    def test_create_benchmark_ratings_dataset_selects_correct_columns(self):
        returned_columns = self.returned_df.columns
        expected_columns = self.expected_df.columns
        self.assertEqual(returned_columns, expected_columns)

    def test_create_benchmark_ratings_dataset_removes_correct_rows(self):
        returned_data = self.returned_df.collect()
        expected_data = self.expected_df.collect()
        self.assertEqual(returned_data, expected_data)


class AddNumericalRatings(FlattenCQCRatingsTests):
    def setUp(self) -> None:
        super().setUp()

    def test_add_numerical_ratings_returns_expected_results(self):
        test_df = self.spark.createDataFrame(
            Data.add_numerical_ratings_rows, Schema.add_numerical_ratings_schema
        )
        returned_df = job.add_numerical_ratings(test_df)
        expected_df = self.spark.createDataFrame(
            Data.expected_add_numerical_ratings_rows,
            Schema.expected_add_numerical_ratings_schema,
        )
        self.assertEqual(returned_df.collect(), expected_df.collect())


if __name__ == "__main__":
    unittest.main(warnings="ignore")
