from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns


@dataclass
class CqcPIRCleanedColumns(CqcPirColumns):
    cqc_pir_import_date: str = "cqc_pir_import_date"
    care_home: str = "careHome"
