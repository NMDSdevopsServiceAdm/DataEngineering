from dataclasses import dataclass
from datetime import date

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_job_roles import (
    AscwdsWorkplaceJobRolesColumns as AWPJobRoles,
)

IMPORT_DATE = date(2026, 1, 1)


@dataclass
class Data:
    wide_two_job_roles_rows = {
        AWPClean.establishment_id: ["1-001", "1-002"],
        AWPClean.ascwds_workplace_import_date: [IMPORT_DATE, IMPORT_DATE],
        "jr01emp": [5, 7],
        "jr01strt": [1, 2],
        "jr01stop": [0, 1],
        "jr01vacy": [2, 0],
        "jr45emp": [10, 3],
        "jr45strt": [0, 1],
        "jr45stop": [1, 0],
        "jr45vacy": [0, 2],
    }

    expected_long_two_job_roles_rows = {
        AWPClean.establishment_id: ["1-001", "1-001", "1-002", "1-002"],
        AWPClean.ascwds_workplace_import_date: [IMPORT_DATE] * 4,
        AWPJobRoles.job_role_code: ["1", "45", "1", "45"],
        AWPJobRoles.employees: [5, 10, 7, 3],
        AWPJobRoles.starters: [1, 0, 2, 1],
        AWPJobRoles.leavers: [0, 1, 1, 0],
        AWPJobRoles.vacancies: [2, 0, 0, 2],
    }

    wide_one_job_role_all_null_rows = {
        AWPClean.establishment_id: ["1-001"],
        AWPClean.ascwds_workplace_import_date: [IMPORT_DATE],
        "jr01emp": [None],
        "jr01strt": [None],
        "jr01stop": [None],
        "jr01vacy": [None],
    }

    expected_long_one_job_role_all_null_rows = {
        AWPClean.establishment_id: ["1-001"],
        AWPClean.ascwds_workplace_import_date: [IMPORT_DATE],
        AWPJobRoles.job_role_code: ["1"],
        AWPJobRoles.employees: [None],
        AWPJobRoles.starters: [None],
        AWPJobRoles.leavers: [None],
        AWPJobRoles.vacancies: [None],
    }
