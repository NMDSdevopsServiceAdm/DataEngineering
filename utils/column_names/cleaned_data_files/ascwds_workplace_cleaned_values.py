from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns,
)


@dataclass
class AscwdsWorkplaceCleanedColumns(AscwdsWorkplaceColumns):
    ascwds_workplace_import_date: str = "ascwds_workplace_import_date"
