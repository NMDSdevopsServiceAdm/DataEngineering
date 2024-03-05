from dataclasses import dataclass

from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONScol,
)


@dataclass
class OnsCleanedColumns(ONScol):
    contemporary_ons_import_date: str = "contemporary_ons_import_date"
    current_ons_import_date: str = "current_ons_import_date"
    contemporary: str = "contemporary"
    current: str = "current"
