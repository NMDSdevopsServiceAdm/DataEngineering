from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
)


@dataclass
class AscwdsWorkplaceCleanedColumns(AWP):
    ascwds_workplace_import_date: str = "ascwds_workplace_import_date"
    purge_data: str = "purge_data"
    last_logged_in_date: str = "last_logged_in_date"
    total_staff_bounded: str = AWP.total_staff + "_bounded"
    worker_records_bounded: str = AWP.worker_records + "_bounded"
    total_staff_deduplicated: str = AWP.total_staff + "_deduplicated"
    worker_records_deduplicated: str = AWP.worker_records + "_deduplicated"
    job_count:str = "job_count"


@dataclass
class AscwdsWorkplaceCleanedValues:
    purge_keep: str = "keep"
    purge_delete: str = "purge"
