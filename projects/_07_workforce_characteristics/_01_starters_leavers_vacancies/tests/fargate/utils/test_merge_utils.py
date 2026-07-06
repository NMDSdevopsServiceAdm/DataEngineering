from dataclasses import dataclass

import polars as pl
import pytest

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as ASCWKPCols,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils"


class TestCreateListOfColsForAscwds:
    @dataclass
    class TestColumns:
        job_role_01_agency: str = ASCWKPCols.job_role_01_agency
        job_role_01_employees: str = ASCWKPCols.job_role_01_employees
        job_role_01_flag: str = ASCWKPCols.job_role_01_flag
        job_role_02_agency: str = ASCWKPCols.job_role_02_agency
        job_role_02_employees: str = ASCWKPCols.job_role_02_employees
        job_role_02_flag: str = ASCWKPCols.job_role_02_flag
        other_column_1: str = "other_column_1"
        other_column_2: str = "other_column_2"
        duplicate_column: str = "duplicate_column"

    @dataclass
    class NoJobRoleColumns:
        other_column_1: str = "other_column_1"
        other_column_2: str = "other_column_2"
        duplicate_column: str = "duplicate_column"

    test_columns = [
        ASCWKPCols.establishment_id,
        ASCWKPCols.job_role_01_agency,  # duplicate job role column for testing
        "duplicate_column",
        "other_flag_column_to_keep",
    ]

    test_schema_dict = {
        ASCWKPCols.establishment_id: pl.String(),
        ASCWKPCols.job_role_01_agency: pl.String(),
        "duplicate_column": pl.Int64(),
        "other_flag_column_to_keep": pl.Boolean(),
    }

    def test_function_returns_list_of_columns_and_schema_dict(self):
        returned_columns, returned_schema_dict = (
            job.create_list_of_cols_and_schema_dict_for_ascwds(
                column_dataclass=self.TestColumns,
                current_columns=self.test_columns,
                current_schema_dict=self.test_schema_dict,
            )
        )
        assert isinstance(returned_columns, list)
        assert isinstance(returned_schema_dict, dict)

    def test_function_returns_more_columns_in_list_of_columns_and_schema_dict_if_job_role_columns_present(
        self,
    ):
        returned_columns, returned_schema_dict = (
            job.create_list_of_cols_and_schema_dict_for_ascwds(
                column_dataclass=self.TestColumns,
                current_columns=self.test_columns,
                current_schema_dict=self.test_schema_dict,
            )
        )
        assert len(returned_columns) > len(self.test_columns)
        assert len(returned_schema_dict) > len(self.test_schema_dict)

    def test_function_removes_new_flag_columns(self):
        returned_columns, _ = job.create_list_of_cols_and_schema_dict_for_ascwds(
            column_dataclass=self.TestColumns,
            current_columns=self.test_columns,
            current_schema_dict=self.test_schema_dict,
        )
        new_columns = set(returned_columns) - set(self.test_columns)
        for col in new_columns:
            assert "flag" not in col

    def test_function_returns_original_list_and_schema_dict_if_no_job_role_columns(
        self,
    ):
        returned_columns, returned_schema_dict = (
            job.create_list_of_cols_and_schema_dict_for_ascwds(
                column_dataclass=self.NoJobRoleColumns,
                current_columns=self.test_columns,
                current_schema_dict=self.test_schema_dict,
            )
        )
        assert returned_columns == self.test_columns
        assert returned_schema_dict == self.test_schema_dict

    def test_function_returns_original_list_of_columns_within_new_list_of_columns_and_schema_dict_keys(
        self,
    ):
        returned_columns, returned_schema_dict = (
            job.create_list_of_cols_and_schema_dict_for_ascwds(
                column_dataclass=self.TestColumns,
                current_columns=self.test_columns,
                current_schema_dict=self.test_schema_dict,
            )
        )
        for col in self.test_columns:
            assert col in returned_columns
        for col in self.test_schema_dict.keys():
            assert col in returned_schema_dict

    def test_function_handles_duplicate_columns(self):
        returned_columns, returned_schema_dict = (
            job.create_list_of_cols_and_schema_dict_for_ascwds(
                column_dataclass=self.TestColumns,
                current_columns=self.test_columns,
                current_schema_dict=self.test_schema_dict,
            )
        )
        assert len(returned_columns) == len(set(returned_columns))
        assert len(returned_schema_dict) == len(set(returned_schema_dict.keys()))

    def test_function_returns_list_of_columns_that_matches_schema_dict(self):
        returned_columns, returned_schema_dict = (
            job.create_list_of_cols_and_schema_dict_for_ascwds(
                column_dataclass=self.TestColumns,
                current_columns=self.test_columns,
                current_schema_dict=self.test_schema_dict,
            )
        )
        for col in returned_columns:
            assert col in returned_schema_dict.keys()

        for col in returned_schema_dict.keys():
            assert col in returned_columns

    def test_function_adds_string_type_to_schema_dict_for_each_new_column(self):
        returned_columns, returned_schema_dict = (
            job.create_list_of_cols_and_schema_dict_for_ascwds(
                column_dataclass=self.TestColumns,
                current_columns=self.test_columns,
                current_schema_dict=self.test_schema_dict,
            )
        )
        new_columns = set(returned_columns) - set(self.test_columns)
        for col in new_columns:
            assert col in returned_schema_dict.keys()
            assert isinstance(returned_schema_dict[col], pl.String)

    def test_function_retains_data_types_for_existing_columns_in_schema_dict(self):
        returned_columns, returned_schema_dict = (
            job.create_list_of_cols_and_schema_dict_for_ascwds(
                column_dataclass=self.TestColumns,
                current_columns=self.test_columns,
                current_schema_dict=self.test_schema_dict,
            )
        )
        for col in self.test_schema_dict.keys():
            assert col in returned_schema_dict.keys()
            assert isinstance(
                returned_schema_dict[col], self.test_schema_dict[col].__class__
            )

    def test_function_raises_type_error_for_non_dataclass_input(self):
        err_msg = "Input must be a dataclass object"
        with pytest.raises(TypeError, match=err_msg):
            job.create_list_of_cols_and_schema_dict_for_ascwds(
                "not_a_dataclass",
                current_columns=self.test_columns,
                current_schema_dict=self.test_schema_dict,
            )

    def test_function_raises_warning_for_no_job_role_columns(self):
        warn_msg = "Warning: No job role columns found in the dataclass. Returning original list of columns and schema dict."
        with pytest.warns(UserWarning, match=warn_msg):
            job.create_list_of_cols_and_schema_dict_for_ascwds(
                column_dataclass=self.NoJobRoleColumns,
                current_columns=self.test_columns,
                current_schema_dict=self.test_schema_dict,
            )


class TestConvertAscwdsJobRoleColumnsToRows:
    def test_convert_ascwds_job_role_columns_to_rows(self):
        pass


class TestJoinDatasets:
    def test_join_datasets(self):
        pass


class TestApplyEmploymentStatusMagicNumbers:
    def test_apply_employment_status_magic_numbers(self):
        pass
