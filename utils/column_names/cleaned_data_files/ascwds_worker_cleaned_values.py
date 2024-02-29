from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns,
)
from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns,
)


@dataclass
class AscwdsWorkerCleanedColumns(AscwdsWorkerColumns):
    ascwds_worker_import_date: str = "ascwds_worker_import_date"

@dataclass
class CleanAscwdsWorkerDataColumnListsForImport:
    worker_columns_for_clean_ascwds_worker_data_job = [
        AscwdsWorkerColumns.location_id,
        AscwdsWorkerColumns.import_date,
        AscwdsWorkerColumns.establishment_id,
        AscwdsWorkerColumns.main_job_role_id,
        AscwdsWorkerColumns.year,
        AscwdsWorkerColumns.month,
        AscwdsWorkerColumns.day,
    ]

    workplace_columns_for_clean_ascwds_worker_data_job = [AscwdsWorkplaceColumns.import_date, AscwdsWorkplaceColumns.establishment_id]
