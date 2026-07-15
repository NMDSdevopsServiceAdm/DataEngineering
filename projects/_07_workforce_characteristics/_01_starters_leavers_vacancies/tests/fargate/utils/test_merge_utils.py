import polars as pl

import projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils as job
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)

PATCH_PATH = "projects._07_workforce_characteristics._01_starters_leavers_vacancies.fargate.utils.merge_utils"


class TestConvertAscwdsJobRoleColumnsToRows:
    def test_convert_ascwds_job_role_columns_to_rows(self):
        pass


class TestJoinDatasets:
    def test_join_datasets(self):
        pass


class TestApplyEmploymentStatusMagicNumbers:
    def test_apply_employment_status_magic_numbers(self):
        pass
