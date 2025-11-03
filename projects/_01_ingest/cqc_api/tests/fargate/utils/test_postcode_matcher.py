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
    manual_postcode_corrections_csv_path = "some/path/manual_postcode_corrections.csv"


class MainPostcodeMatcherTests(RunPostcodeMatchingTests):
    def setUp(self):
        self.locations_where_all_match_lf = pl.LazyFrame(
            Data.locations_where_all_match_rows,
            Schemas.locations_schema,
        )
        self.locations_with_unmatched_postcode_lf = pl.LazyFrame(
            Data.locations_with_unmatched_postcode_rows,
            Schemas.locations_schema,
        )
        self.postcodes_lf = pl.LazyFrame(
            Data.postcodes_rows,
            Schemas.postcodes_schema,
        )

    @patch(f"{PATCH_PATH}.raise_error_if_unmatched")
    @patch(f"{PATCH_PATH}.truncate_postcode")
    @patch(f"{PATCH_PATH}.create_truncated_postcode_lf")
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
        create_truncated_postcode_lf_mock: Mock,
        truncate_postcode_mock: Mock,
        raise_error_if_unmatched_mock: Mock,
    ):
        join_postcode_data_mock.return_value = (
            self.locations_where_all_match_lf,
            self.locations_where_all_match_lf,
        )

        job.run_postcode_matching(
            self.locations_where_all_match_lf,
            self.postcodes_lf,
            self.manual_postcode_corrections_csv_path,
        )

        self.assertEqual(clean_postcode_column_mock.call_count, 2)
        add_aligned_date_column_mock.assert_called_once()
        self.assertEqual(join_postcode_data_mock.call_count, 4)
        get_first_successful_postcode_match_mock.assert_called_once()
        amend_invalid_postcodes_mock.assert_called_once()
        create_truncated_postcode_lf_mock.assert_called_once()
        truncate_postcode_mock.assert_called_once()
        raise_error_if_unmatched_mock.assert_called_once()

    @patch(f"{PATCH_PATH}.read_manual_postcode_corrections_csv_to_dict")
    def test_main_completes_when_all_postcodes_match(
        self, mock_read_manual_postcode_corrections_csv_to_dict
    ):
        mock_read_manual_postcode_corrections_csv_to_dict.return_value = (
            Data.postcode_corrections_dict
        )
        returned_lf = job.run_postcode_matching(
            self.locations_where_all_match_lf,
            self.postcodes_lf,
            self.manual_postcode_corrections_csv_path,
        )

        self.assertEqual(
            returned_lf.collect().height,
            self.locations_where_all_match_lf.collect().height,
        )

    def test_main_raises_error_when_some_postcodes_do_not_match(self):
        with self.assertRaises(TypeError) as context:
            job.run_postcode_matching(
                self.locations_with_unmatched_postcode_lf, self.postcodes_lf
            )

        self.assertTrue(
            "Unmatched postcodes found: [('1-005', 'name 5', '5 road name', 'AA2 5XX')]",
            str(context.exception),
        )


class CleanPostcodeColumnTests(RunPostcodeMatchingTests):
    def setUp(
        self,
    ):
        self.postcodes_lf = pl.LazyFrame(
            data=Data.clean_postcode_column_rows,
            schema=Schemas.clean_postcode_column_schema,
        )

    def test_returns_expected_values_when_drop_is_false(self):
        returned_lf = job.clean_postcode_column(
            self.postcodes_lf, CQCL.postal_code, CQCLClean.postcode_cleaned, False
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_clean_postcode_column_when_drop_is_false_rows,
            schema=Schemas.expected_clean_postcode_column_when_drop_is_false_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)

    def test_returns_expected_values_when_drop_is_true(self):
        returned_lf = job.clean_postcode_column(
            self.postcodes_lf, CQCL.postal_code, CQCLClean.postcode_cleaned, True
        )
        expected_lf = pl.LazyFrame(
            data=Data.expected_clean_postcode_column_when_drop_is_true_rows,
            schema=Schemas.expected_clean_postcode_column_when_drop_is_true_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class JoinPostcodeDataTests(RunPostcodeMatchingTests):
    def test_returns_expected_dataframe(self):
        locations_lf = pl.LazyFrame(
            data=Data.join_postcode_data_locations_rows,
            schema=Schemas.join_postcode_data_locations_schema,
        )
        postcode_lf = pl.LazyFrame(
            data=Data.join_postcode_data_postcodes_rows,
            schema=Schemas.join_postcode_data_postcodes_schema,
        )
        returned_matched_lf, returned_unmatched_lf = job.join_postcode_data(
            locations_lf, postcode_lf, CQCLClean.postcode_cleaned
        )
        expected_matched_lf = pl.LazyFrame(
            data=Data.expected_join_postcode_data_matched_rows,
            schema=Schemas.expected_join_postcode_data_matched_schema,
        )
        expected_unmatched_lf = pl.LazyFrame(
            data=Data.expected_join_postcode_data_unmatched_rows,
            schema=Schemas.expected_join_postcode_data_unmatched_schema,
        )

        pl_testing.assert_frame_equal(returned_matched_lf, expected_matched_lf)
        pl_testing.assert_frame_equal(returned_unmatched_lf, expected_unmatched_lf)


class GetFirstSuccessfulPostcodeMatchTests(RunPostcodeMatchingTests):
    def test_get_first_successful_postcode_match_returns_expected_dataframe(self):
        unmatched_lf = pl.LazyFrame(
            Data.first_successful_postcode_unmatched_rows,
            Schemas.first_successful_postcode_unmatched_schema,
        )
        matched_lf = pl.LazyFrame(
            Data.first_successful_postcode_matched_rows,
            Schemas.first_successful_postcode_matched_schema,
        )

        returned_lf = job.get_first_successful_postcode_match(unmatched_lf, matched_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_get_first_successful_postcode_match_rows,
            Schemas.expected_get_first_successful_postcode_match_schema,
        )
        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class AmendInvalidPostcodesTests(RunPostcodeMatchingTests):
    @patch(f"{PATCH_PATH}.read_manual_postcode_corrections_csv_to_dict")
    def test_amend_invalid_postcodes_returns_expected_values(
        self, mock_read_manual_postcode_corrections_csv_to_dict
    ):
        postcodes_lf = pl.LazyFrame(
            Data.amend_invalid_postcodes_rows, Schemas.amend_invalid_postcodes_schema
        )
        mock_read_manual_postcode_corrections_csv_to_dict.return_value = (
            Data.postcode_corrections_dict
        )

        returned_lf = job.amend_invalid_postcodes(
            postcodes_lf, self.manual_postcode_corrections_csv_path
        )

        expected_lf = pl.LazyFrame(
            Data.expected_amend_invalid_postcodes_rows,
            Schemas.amend_invalid_postcodes_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class TruncatePostcodeTests(RunPostcodeMatchingTests):
    def test_truncate_postcode_returns_expected_dataframe(self):
        postcodes_lf = pl.LazyFrame(
            Data.truncate_postcode_rows, Schemas.truncate_postcode_schema
        )
        returned_lf = job.truncate_postcode(postcodes_lf)

        expected_lf = pl.LazyFrame(
            Data.expected_truncate_postcode_rows,
            Schemas.expected_truncate_postcode_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class CreateTruncatedPostcodeDfTests(RunPostcodeMatchingTests):
    def test_create_truncated_postcode_lf_returns_expected_dataframe(self) -> None:
        postcodes_lf = pl.LazyFrame(
            Data.create_truncated_postcode_df_rows,
            Schemas.create_truncated_postcode_df_schema,
        )
        returned_lf = job.create_truncated_postcode_lf(postcodes_lf)
        expected_lf = pl.LazyFrame(
            Data.expected_create_truncated_postcode_df_rows,
            Schemas.expected_create_truncated_postcode_df_schema,
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)


class RaiseErrorIfUnmatched(RunPostcodeMatchingTests):
    def test_raise_error_if_unmatched_raises_type_error_when_contains_data(self):
        unknown_lf = pl.LazyFrame(
            Data.raise_error_if_unmatched_rows,
            Schemas.raise_error_if_unmatched_schema,
        )

        with self.assertRaises(TypeError) as context:
            job.raise_error_if_unmatched(unknown_lf)

        self.assertTrue(
            "Unmatched postcodes found: [('1-001', 'name 1', '1 road name', 'AB1 2CD')]",
            str(context.exception),
        )

    def test_raise_error_if_unmatched_does_not_raise_error_when_lf_is_empty(self):
        empty_lf = pl.LazyFrame([], Schemas.raise_error_if_unmatched_schema)

        try:
            job.raise_error_if_unmatched(empty_lf)
        except Exception as e:
            self.fail(
                f"raise_error_if_unmatched() raised an exception unexpectedly: {e}"
            )
