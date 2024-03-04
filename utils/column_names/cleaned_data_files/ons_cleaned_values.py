from dataclasses import dataclass

from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONScol,
)


@dataclass
class OnsCleanedColumns(ONScol):
    ons_import_date: str = "ons_import_date"
