from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns,
)


@dataclass
class AscwdsWorkplaceCleanedColumns(AscwdsWorkplaceColumns):
    ascwds_workplace_import_date: str = "ascwds_workplace_import_date"
    purge_data: str = "purge_data"
    last_logged_in_date: str = "last_logged_in_date"
    total_staff_bounded: str = AscwdsWorkplaceColumns.total_staff + "_bounded"
    worker_records_bounded: str = AscwdsWorkplaceColumns.worker_records + "_bounded"


@dataclass
class AscwdsWorkplaceCleanedValues:
    purge_keep: str = "keep"
    purge_delete: str = "purge"
