from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)


@dataclass
class AscwdsWorkplaceCleanedColumns(AWP):
    ascwds_workplace_import_date: str = "ascwds_workplace_import_date"
    last_logged_in_date: str = "last_logged_in_date"
    data_purge_date: str = "data_purge_date"
    coverage_purge_date: str = "coverage_purge_date"
    master_update_date_org: str = "master_update_date_org"
    keep_if_after_this_date: str = "keep_if_after_this_date"
    total_staff_bounded: str = AWP.total_staff + "_bounded"
    worker_records_bounded: str = AWP.worker_records + "_bounded"
