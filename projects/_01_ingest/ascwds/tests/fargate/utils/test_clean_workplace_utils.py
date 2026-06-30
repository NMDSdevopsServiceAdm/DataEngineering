import unittest

import polars as pl
import polars.testing as pl_testing

import projects._01_ingest.ascwds.fargate.utils.clean_workplace_utils as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


class TestValidWorkplaceFilter:
    def test_valid_workplace_filter_returns_correct_rows(self):
        input_lf = pl.LazyFrame(
            {
                AWPClean.organisation_id: [
                    "305",  # test account
                    "123",
                    "1234",
                    "28470",  # test account
                ],
                AWPClean.establishment_id: [
                    "1",
                    "48904",  # duplicate
                    "12",
                    "50640",  # duplicate
                ],
            }
        )
        expected_lf = pl.LazyFrame(
            {
                AWPClean.organisation_id: ["1234"],
                AWPClean.establishment_id: ["12"],
            }
        )

        returned_lf = input_lf.filter(job.valid_workplace_filter())

        pl_testing.assert_frame_equal(expected_lf, returned_lf)


class JobRoleColsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.returned_list: list[str] = job.job_role_cols

    def test_list_contains_strings_starting_with_jr(self):
        for i in self.returned_list:
            assert i.startswith("jr")

    def test_list_contains_expected_number_of_elements(self):
        print(self.returned_list)
        string_prefix = set([i[:4] for i in self.returned_list])
        self.assertEqual(len(string_prefix), 52)

    def test_list_contains_strings_with_expected_endings(self):
        string_endings_1 = [i[-4:] for i in self.returned_list if len(i) == 8]
        string_endings_2 = [i[-3:] for i in self.returned_list if len(i) == 7]
        string_endings_3 = set(string_endings_1 + string_endings_2)
        expected_endings = {
            "perm",
            "temp",
            "pool",
            "agcy",
            "oth",
            "work",
            "emp",
            "strt",
            "stop",
            "vacy",
        }
        self.assertEqual(string_endings_3, expected_endings)

    def test_list_does_not_contain_strings_with_flag(self):
        for i in self.returned_list:
            assert "flag" not in i

    def test_list_is_expected_length(self):
        self.assertEqual(len(self.returned_list), 520)


class MergeJobRoleColumns(unittest.TestCase):
    def test_function_returns_expected_data(self):
        test_lf = pl.LazyFrame(
            data=[
                (1, 1, 2, 2, 2, 3, 4, 10, 10, 20, 20, 20, 30, 40), # All columns are populated.
                (None, 1, 2, 2, 2, 3, 4, 10, 10, 20, 20, 20, 30, 40), # A giving role is null.
                (1, None, 2, 2, 2, 3, 4, 10, 10, 20, 20, 20, 30, 40), # A receiving role is null.
                (None, None, 2, 2, 2, 3, 4, 10, 10, 20, 20, 20, 30, 40), # All roles in sum group are null.
            ],
            schema={
                AWPClean.job_role_41_employees: pl.Int32,
                AWPClean.job_role_40_employees: pl.Int32,
                AWPClean.job_role_12_employees: pl.Int32,
                AWPClean.job_role_13_employees: pl.Int32,
                AWPClean.job_role_42_employees: pl.Int32,
                AWPClean.job_role_01_employees: pl.Int32, # job role is not touched by function.
                AWPClean.job_role_33_employees: pl.Int32, # personal assistant are removed without merging values.
                AWPClean.job_role_41_starters: pl.Int32,
                AWPClean.job_role_40_starters: pl.Int32,
                AWPClean.job_role_12_starters: pl.Int32,
                AWPClean.job_role_13_starters: pl.Int32,
                AWPClean.job_role_42_starters: pl.Int32,
                AWPClean.job_role_01_starters: pl.Int32,
                AWPClean.job_role_33_starters: pl.Int32,
            },
            orient="row"
        ) # fmt: skip

        test_mapping = {
            "jr40": ["41"],
            "jr42": ["12", "13"],
        }
        test_suffixes = ["emp", "strt"]
        returned_lf = job.merge_job_role_columns(test_lf, test_mapping, test_suffixes)
        expected_lf = pl.LazyFrame(
            data=[
                (2, 6, 3, 20, 60, 30),
                (1, 6, 3, 20, 60, 30),
                (1, 6, 3, 20, 60, 30),
                (None, 6, 3, 20, 60, 30),
            ],
            schema={
                AWPClean.job_role_40_employees: pl.Int32,
                AWPClean.job_role_42_employees: pl.Int32,
                AWPClean.job_role_01_employees: pl.Int32,
                AWPClean.job_role_40_starters: pl.Int32,
                AWPClean.job_role_42_starters: pl.Int32,
                AWPClean.job_role_01_starters: pl.Int32,
            },
            orient="row",
        )

        pl_testing.assert_frame_equal(returned_lf, expected_lf)
