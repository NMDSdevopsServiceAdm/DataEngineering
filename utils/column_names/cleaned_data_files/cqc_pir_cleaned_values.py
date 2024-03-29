from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns,
)


@dataclass
class CqcPIRCleanedColumns(CqcPirColumns):
    cqc_pir_import_date: str = "cqc_pir_import_date"
    pir_submission_date_as_date: str = "cqc_pir_submission_date"
    people_directly_employed: str = "people_directly_employed"

    care_home: str = CqcLocationApiColumns.care_home


@dataclass
class CqcPIRCleanedValues:
    yes: str = "Y"
    no: str = "N"
    residential: str = "Residential"
    community: str = "Community"
