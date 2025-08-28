import unittest
from unittest.mock import Mock, patch

import projects._01_ingest.cqc_api.utils.postcode_matcher as job
from projects._01_ingest.unittest_data.ingest_test_file_data import (
    PostcodeMatcherData as Data,
)
from projects._01_ingest.unittest_data.ingest_test_file_schemas import (
    PostcodeMatcherSchema as Schemas,
)
from utils import utils
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)

PATCH_PATH: str = "projects._01_ingest.cqc_api.utils.postcode_matcher"


class PostcodeMatcherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.spark = utils.get_spark()

        self.locations_df = self.spark.createDataFrame(
            Data.locations_where_all_match_rows,
            Schemas.locations_schema,
        )
        self.postcodes_df = self.spark.createDataFrame(
            Data.postcodes_rows,
            Schemas.postcodes_schema,
        )


class MainTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

    @patch(f"{PATCH_PATH}.combine_matched_dataframes")
    @patch(f"{PATCH_PATH}.raise_error_if_unmatched")
    @patch(f"{PATCH_PATH}.truncate_postcode")
    @patch(f"{PATCH_PATH}.create_truncated_postcode_df")
    @patch(f"{PATCH_PATH}.amend_invalid_postcodes")
    @patch(f"{PATCH_PATH}.get_first_successful_postcode_match")
    @patch(f"{PATCH_PATH}.join_postcode_data")
    @patch(f"{PATCH_PATH}.cUtils.add_aligned_date_column")
    @patch(f"{PATCH_PATH}.clean_postcode_column")
    def test_main_calls_functions(
        self,
        clean_postcode_column_mock: Mock,
        add_aligned_date_column_mock: Mock,
        join_postcode_data_mock: Mock,
        get_first_successful_postcode_match_mock: Mock,
        amend_invalid_postcodes_mock: Mock,
        create_truncated_postcode_df_mock: Mock,
        truncate_postcode_mock: Mock,
        raise_error_if_unmatched_mock: Mock,
        combine_matched_dataframes_mock: Mock,
    ):
        join_postcode_data_mock.return_value = self.locations_df, self.locations_df

        job.run_postcode_matching(self.locations_df, self.postcodes_df)

        self.assertEqual(clean_postcode_column_mock.call_count, 2)
        add_aligned_date_column_mock.assert_called_once()
        self.assertEqual(join_postcode_data_mock.call_count, 4)
        get_first_successful_postcode_match_mock.assert_called_once()
        amend_invalid_postcodes_mock.assert_called_once()
        create_truncated_postcode_df_mock.assert_called_once()
        truncate_postcode_mock.assert_called_once()
        raise_error_if_unmatched_mock.assert_called_once()
        combine_matched_dataframes_mock.assert_called_once()

    def test_main_completes_when_all_postcodes_match(self):
        returned_df = job.run_postcode_matching(self.locations_df, self.postcodes_df)

        self.assertEqual(returned_df.count(), self.locations_df.count())

    def test_main_raises_error_when_some_postcodes_do_not_match(self):
        locations_df = self.spark.createDataFrame(
            Data.locations_with_unmatched_postcode_rows,
            Schemas.locations_schema,
        )

        with self.assertRaises(TypeError) as context:
            job.run_postcode_matching(locations_df, self.postcodes_df)

        self.assertTrue(
            "Unmatched postcodes found: [('1-005', 'name 5', '5 road name', 'AA2 5XX')]",
            str(context.exception),
        )


class CleanPostcodeColumnTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

        self.test_df = self.spark.createDataFrame(
            Data.clean_postcode_column_rows, Schemas.clean_postcode_column_schema
        )
        self.returned_df = job.clean_postcode_column(
            self.test_df,
            CQCLClean.postal_code,
            CQCLClean.postcode_cleaned,
            drop_col=False,
        )
        self.expected_df = self.spark.createDataFrame(
            Data.expected_clean_postcode_column_rows,
            Schemas.expected_clean_postcode_column_when_col_not_dropped_schema,
        )
        self.returned_data = self.returned_df.sort(CQCLClean.postcode_cleaned).collect()
        self.expected_data = self.expected_df.collect()

    def test_clean_postcode_column_returns_expected_values(self):
        self.assertEqual(self.returned_data, self.expected_data)

    def test_clean_postcode_column_returns_expected_columns_when_column_is_not_dropped(
        self,
    ):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)

    def test_clean_postcode_column_returns_expected_columns_when_column_is_dropped(
        self,
    ):
        returned_when_col_dropped_df = job.clean_postcode_column(
            self.test_df,
            CQCLClean.postal_code,
            CQCLClean.postcode_cleaned,
            drop_col=True,
        )
        expected_when_col_dropped_df = self.spark.createDataFrame(
            [], Schemas.expected_clean_postcode_column_when_col_is_dropped_schema
        )
        self.assertEqual(
            returned_when_col_dropped_df.columns, expected_when_col_dropped_df.columns
        )


class JoinPostcodeDataTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

        locations_df = self.spark.createDataFrame(
            Data.join_postcode_data_locations_rows,
            Schemas.join_postcode_data_locations_schema,
        )
        postcode_df = self.spark.createDataFrame(
            Data.join_postcode_data_postcodes_rows,
            Schemas.join_postcode_data_postcodes_schema,
        )
        self.returned_matched_df, self.returned_unmatched_df = job.join_postcode_data(
            locations_df, postcode_df, CQCLClean.postcode_cleaned
        )
        self.expected_matched_df = self.spark.createDataFrame(
            Data.expected_join_postcode_data_matched_rows,
            Schemas.expected_join_postcode_data_matched_schema,
        )
        self.expected_unmatched_df = self.spark.createDataFrame(
            Data.expected_join_postcode_data_unmatched_rows,
            Schemas.expected_join_postcode_data_unmatched_schema,
        )

    def test_join_postcode_data_returns_expected_matched_columns(self):
        self.assertEqual(
            sorted(self.returned_matched_df.columns),
            sorted(self.expected_matched_df.columns),
        )

    def test_join_postcode_data_returns_expected_matched_rows(self):
        returned_data = (
            self.returned_matched_df.select(*self.expected_matched_df.columns)
            .sort(CQCLClean.location_id)
            .collect()
        )
        expected_data = self.expected_matched_df.collect()

        self.assertEqual(returned_data, expected_data)

    def test_join_postcode_data_returns_expected_unmatched_columns(self):
        self.assertEqual(
            self.returned_unmatched_df.columns,
            self.expected_unmatched_df.columns,
        )

    def test_join_postcode_data_returns_expected_unmatched_rows(self):
        self.assertEqual(
            self.returned_unmatched_df.sort(CQCLClean.location_id).collect(),
            self.expected_unmatched_df.collect(),
        )


class GetFirstSuccessfulPostcodeMatch(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

        self.unmatched_df = self.spark.createDataFrame(
            Data.first_successful_postcode_unmatched_rows,
            Schemas.first_successful_postcode_unmatched_schema,
        )
        matched_df = self.spark.createDataFrame(
            Data.first_successful_postcode_matched_rows,
            Schemas.first_successful_postcode_matched_schema,
        )
        self.returned_df = job.get_first_successful_postcode_match(
            self.unmatched_df, matched_df
        )
        expected_df = self.spark.createDataFrame(
            Data.expected_get_first_successful_postcode_match_rows,
            Schemas.expected_get_first_successful_postcode_match_schema,
        )

        self.returned_data = self.returned_df.sort(CQCLClean.location_id).collect()
        self.expected_data = expected_df.collect()

    def test_get_first_successful_postcode_match_returns_original_columns(self):
        self.assertEqual(
            sorted(self.returned_df.columns), sorted(self.unmatched_df.columns)
        )

    def test_get_first_successful_postcode_match_returns_original_number_of_rows(self):
        self.assertEqual(self.returned_df.count(), self.unmatched_df.count())

    def test_get_first_successful_postcode_match_returns_expected_postcodes(self):
        for i in range(len(self.expected_data)):
            self.assertEqual(
                self.returned_data[i][CQCLClean.postcode_cleaned],
                self.expected_data[i][CQCLClean.postcode_cleaned],
            )


class AmendInvalidPostcodesTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

    def test_amend_invalid_postcodes_returns_expected_values(self):
        test_df = self.spark.createDataFrame(
            Data.amend_invalid_postcodes_rows, Schemas.amend_invalid_postcodes_schema
        )

        returned_df = job.amend_invalid_postcodes(test_df)

        expected_df = self.spark.createDataFrame(
            Data.expected_amend_invalid_postcodes_rows,
            Schemas.amend_invalid_postcodes_schema,
        )

        self.assertEqual(
            returned_df.sort(CQCLClean.location_id).collect(),
            expected_df.collect(),
        )


class TruncatePostcodeTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.truncate_postcode_rows, Schemas.truncate_postcode_schema
        )
        self.returned_df = job.truncate_postcode(test_df)

        self.expected_df = self.spark.createDataFrame(
            Data.expected_truncate_postcode_rows,
            Schemas.expected_truncate_postcode_schema,
        )
        self.returned_data = self.returned_df.sort(CQCLClean.postcode_cleaned).collect()
        self.expected_data = self.expected_df.collect()

    def test_truncate_postcode_returns_expected_values(self):
        self.assertEqual(self.returned_data, self.expected_data)

    def test_truncate_postcode_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)


class CreateTruncatedPostcodeDfTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

        test_df = self.spark.createDataFrame(
            Data.create_truncated_postcode_df_rows,
            Schemas.create_truncated_postcode_df_schema,
        )
        self.returned_df = job.create_truncated_postcode_df(test_df)

        self.expected_df = self.spark.createDataFrame(
            Data.expected_create_truncated_postcode_df_rows,
            Schemas.expected_create_truncated_postcode_df_schema,
        )
        self.returned_data = self.returned_df.sort(CQCLClean.postcode_cleaned).collect()
        self.expected_data = self.expected_df.collect()

    def test_create_truncated_postcode_df_returns_expected_values(self):
        self.assertEqual(self.returned_data, self.expected_data)

    def test_create_truncated_postcode_df_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)


class RaiseErrorIfUnmatched(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

    def test_raise_error_if_unmatched_raises_type_error_when_contains_data(self):
        unknown_df = self.spark.createDataFrame(
            Data.raise_error_if_unmatched_rows,
            Schemas.raise_error_if_unmatched_schema,
        )

        with self.assertRaises(TypeError) as context:
            job.raise_error_if_unmatched(unknown_df)

        self.assertTrue(
            "Unmatched postcodes found: [('1-001', 'name 1', '1 road name', 'AB1 2CD')]",
            str(context.exception),
        )

    def test_raise_error_if_unmatched_does_not_raise_error_when_df_is_empty(self):
        empty_df = self.spark.createDataFrame(
            [], Schemas.raise_error_if_unmatched_schema
        )

        try:
            job.raise_error_if_unmatched(empty_df)
        except Exception as e:
            self.fail(
                f"raise_error_if_unmatched() raised an exception unexpectedly: {e}"
            )


class CombineMatchedDataframesTests(PostcodeMatcherTests):
    def setUp(self) -> None:
        super().setUp()

        matched_1_df = self.spark.createDataFrame(
            Data.combine_matched_df1_rows,
            Schemas.combine_matched_df1_schema,
        )
        matched_2_df = self.spark.createDataFrame(
            Data.combine_matched_df2_rows,
            Schemas.combine_matched_df2_schema,
        )
        self.returned_df = job.combine_matched_dataframes([matched_1_df, matched_2_df])

        self.expected_df = self.spark.createDataFrame(
            Data.expected_combine_matched_rows,
            Schemas.expected_combine_matched_schema,
        )
        self.returned_data = self.returned_df.sort(CQCLClean.location_id).collect()
        self.expected_data = self.expected_df.collect()

    def test_combine_matched_dataframes_returns_expected_values(self):
        self.assertEqual(self.returned_data, self.expected_data)

    def test_combine_matched_dataframes_returns_expected_columns(self):
        self.assertEqual(self.returned_df.columns, self.expected_df.columns)
