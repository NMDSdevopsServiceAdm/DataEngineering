import unittest
from unittest.mock import Mock, patch

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.cqc_api.fargate.utils.postcode_matcher as job
from projects._01_ingest.unittest_data.polars_ingest_test_file_data import (
    PostcodeMatcherTest as Data,
)
from projects._01_ingest.unittest_data.polars_ingest_test_file_schema import (
    PostcodeMatcherTest as Schemas,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

PATCH_PATH: str = "projects._01_ingest.cqc_api.fargate.utils.postcode_matcher"


class RunPostcodeMatchingTests(unittest.TestCase):
    def setUp(self):
        self.locations_where_all_match_df = pl.DataFrame(
            Data.locations_where_all_match_rows,
            Schemas.locations_schema,
        )
        self.locations_with_unmatched_postcode_df = pl.DataFrame(
            Data.locations_with_unmatched_postcode_rows,
            Schemas.locations_schema,
        )
        self.postcodes_df = pl.DataFrame(
            Data.postcodes_rows,
            Schemas.postcodes_schema,
        )

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
        join_postcode_data_mock.return_value = (
            self.locations_where_all_match_df,
            self.locations_where_all_match_df,
        )

        job.run_postcode_matching(self.locations_where_all_match_df, self.postcodes_df)

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
        returned_df = job.run_postcode_matching(
            self.locations_where_all_match_df, self.postcodes_df
        )

        self.assertEqual(returned_df.height, self.locations_where_all_match_df.height)

    def test_main_raises_error_when_some_postcodes_do_not_match(self):
        with self.assertRaises(TypeError) as context:
            job.run_postcode_matching(
                self.locations_with_unmatched_postcode_df, self.postcodes_df
            )

        self.assertTrue(
            "Unmatched postcodes found: [('1-005', 'name 5', '5 road name', 'AA2 5XX')]",
            str(context.exception),
        )


class CleanPostcodeColumnTests(unittest.TestCase):
    def setUp(
        self,
    ):
        self.test_df = pl.DataFrame(
            data=Data.clean_postcode_column_rows,
            schema=Schemas.clean_postcode_column_schema,
        )

    def test_returns_expected_values_when_drop_is_false(self):
        returned_df = job.clean_postcode_column(
            self.test_df, CQCL.postal_code, CQCLClean.postcode_cleaned, False
        )
        expected_df = pl.DataFrame(
            data=Data.expected_clean_postcode_column_when_drop_is_false_rows,
            schema=Schemas.expected_clean_postcode_column_when_drop_is_false_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)

    def test_returns_expected_values_when_drop_is_true(self):
        returned_df = job.clean_postcode_column(
            self.test_df, CQCL.postal_code, CQCLClean.postcode_cleaned, True
        )
        expected_df = pl.DataFrame(
            data=Data.expected_clean_postcode_column_when_drop_is_true_rows,
            schema=Schemas.expected_clean_postcode_column_when_drop_is_true_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)


class JoinPostcodeDataTests(unittest.TestCase):
    def setUp(
        self,
    ):
        locations_df = pl.DataFrame(
            data=Data.join_postcode_data_locations_rows,
            schema=Schemas.join_postcode_data_locations_schema,
        )
        postcode_df = pl.DataFrame(
            data=Data.join_postcode_data_postcodes_rows,
            schema=Schemas.join_postcode_data_postcodes_schema,
        )
        self.returned_matched_df, self.returned_unmatched_df = job.join_postcode_data(
            locations_df, postcode_df, CQCLClean.postcode_cleaned
        )
        self.expected_matched_df = pl.DataFrame(
            data=Data.expected_join_postcode_data_matched_rows,
            schema=Schemas.expected_join_postcode_data_matched_schema,
        )
        self.expected_unmatched_df = pl.DataFrame(
            data=Data.expected_join_postcode_data_unmatched_rows,
            schema=Schemas.expected_join_postcode_data_unmatched_schema,
        )

    def test_returns_expected_dataframe(self):
        pl_testing.assert_frame_equal(
            self.returned_matched_df, self.expected_matched_df
        )
        pl_testing.assert_frame_equal(
            self.returned_unmatched_df, self.expected_unmatched_df
        )


class GetFirstSuccessfulPostcodeMatchTests(unittest.TestCase):
    def setUp(self):
        unmatched_df = pl.DataFrame(
            Data.first_successful_postcode_unmatched_rows,
            Schemas.first_successful_postcode_unmatched_schema,
        )
        matched_df = pl.DataFrame(
            Data.first_successful_postcode_matched_rows,
            Schemas.first_successful_postcode_matched_schema,
        )
        self.returned_df = job.get_first_successful_postcode_match(
            unmatched_df, matched_df
        )
        self.expected_df = pl.DataFrame(
            Data.expected_get_first_successful_postcode_match_rows,
            Schemas.expected_get_first_successful_postcode_match_schema,
        )

    def test_get_first_successful_postcode_match_returns_expected_dataframe(self):
        pl_testing.assert_frame_equal(self.returned_df, self.expected_df)


class AmendInvalidPostcodesTests(unittest.TestCase):
    def test_amend_invalid_postcodes_returns_expected_values(self):
        test_df = pl.DataFrame(
            Data.amend_invalid_postcodes_rows, Schemas.amend_invalid_postcodes_schema
        )

        returned_df = job.amend_invalid_postcodes(test_df)

        expected_df = pl.DataFrame(
            Data.expected_amend_invalid_postcodes_rows,
            Schemas.amend_invalid_postcodes_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)


class TruncatePostcodeTests(unittest.TestCase):
    def test_truncate_postcode_returns_expected_dataframe(self):
        test_df = pl.DataFrame(
            Data.truncate_postcode_rows, Schemas.truncate_postcode_schema
        )
        returned_df = job.truncate_postcode(test_df)

        expected_df = pl.DataFrame(
            Data.expected_truncate_postcode_rows,
            Schemas.expected_truncate_postcode_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)


class CreateTruncatedPostcodeDfTests(unittest.TestCase):
    def test_create_truncated_postcode_df_returns_expected_dataframe(self) -> None:
        test_df = pl.DataFrame(
            Data.create_truncated_postcode_df_rows,
            Schemas.create_truncated_postcode_df_schema,
        )
        returned_df = job.create_truncated_postcode_df(test_df)
        expected_df = pl.DataFrame(
            Data.expected_create_truncated_postcode_df_rows,
            Schemas.expected_create_truncated_postcode_df_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)


class RaiseErrorIfUnmatched(unittest.TestCase):
    def test_raise_error_if_unmatched_raises_type_error_when_contains_data(self):
        unknown_df = pl.DataFrame(
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
        empty_df = pl.DataFrame([], Schemas.raise_error_if_unmatched_schema)

        try:
            job.raise_error_if_unmatched(empty_df)
        except Exception as e:
            self.fail(
                f"raise_error_if_unmatched() raised an exception unexpectedly: {e}"
            )


class CombineMatchedDataframesTests(unittest.TestCase):
    def test_combine_matched_dataframes_returns_expected_dataframe(self):
        matched_1_df = pl.DataFrame(
            Data.combine_matched_df1_rows,
            Schemas.combine_matched_df1_schema,
        )
        matched_2_df = pl.DataFrame(
            Data.combine_matched_df2_rows,
            Schemas.combine_matched_df2_schema,
        )

        returned_df = job.combine_matched_dataframes([matched_1_df, matched_2_df])
        expected_df = pl.DataFrame(
            Data.expected_combine_matched_rows,
            Schemas.expected_combine_matched_schema,
        )

        pl_testing.assert_frame_equal(returned_df, expected_df)
