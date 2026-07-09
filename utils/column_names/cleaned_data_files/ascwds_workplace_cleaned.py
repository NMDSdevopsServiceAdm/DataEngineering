from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)


@dataclass
class AscwdsWorkplaceCleanedColumns(AWP):
    ascwds_workplace_import_date: str = "ascwds_workplace_import_date"
    last_logged_in_date: str = "last_logged_in_date"
    data_last_amended_date: str = "data_last_amended_date"
    workplace_last_active_date: str = "workplace_last_active_date"
    master_update_date_org: str = "master_update_date_org"
    purge_date: str = "purge_date"
    total_staff_bounded: str = AWP.total_staff + "_bounded"
    worker_records_bounded: str = AWP.worker_records + "_bounded"
