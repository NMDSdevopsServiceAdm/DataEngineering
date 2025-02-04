from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns,
)


@dataclass
class CqcPIRCleanedColumns(CqcPirColumns):
    cqc_pir_import_date: str = "cqc_pir_import_date"
    pir_submission_date_as_date: str = "cqc_pir_submission_date"
    pir_people_directly_employed: str = "pir_people_directly_employed"

    care_home: str = NewCqcLocationApiColumns.care_home
