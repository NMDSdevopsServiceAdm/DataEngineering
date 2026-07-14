import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils"


class TestSLVColsSelector:
    def test_selects_expected_columns(self):
        test_lf = pl.LazyFrame(
            {
                AWPClean.job_role_01_employees: 1,
                AWPClean.job_role_01_starters: 1,
                AWPClean.job_role_01_leavers: 1,
                AWPClean.job_role_01_vacancies: 1,
                AWPClean.job_role_01_temporary: 1,
                AWPClean.job_role_01_flag: 1,
                "jr01permdate": 1,
                "any_other_col": 1,
            }
        )
        returned_cols = test_lf.select(job.slv_cols_selector()).collect_schema().names()
        expected_cols = [
            AWPClean.job_role_01_employees,
            AWPClean.job_role_01_starters,
            AWPClean.job_role_01_leavers,
            AWPClean.job_role_01_vacancies,
        ]

        assert returned_cols == expected_cols

    def test_ignores_non_numeric_job_role_columns(self):
        test_lf = pl.LazyFrame(
            {
                "jr_int_strt": [1],  # int
                "jr_flt_strt": [1.0],  # float
                "jr_str_strt": ["1"],  # string
            }
        )
        returned_cols = test_lf.select(job.slv_cols_selector()).collect_schema().names()

        assert returned_cols == ["jr_int_strt", "jr_flt_strt"]


class TestConvertAscwdsJobRoleColumnsToRows:
    def test_convert_ascwds_job_role_columns_to_rows(self):
        pass


class TestJoinDatasets:
    def test_join_datasets(self):
        pass


class TestApplyEmploymentStatusMagicNumbers:
    def test_apply_employment_status_magic_numbers(self):
        pass
