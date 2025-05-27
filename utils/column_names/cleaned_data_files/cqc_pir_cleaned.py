from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_pir_columns import CqcPirColumns

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns,
)


@dataclass
class CqcPIRCleanedColumns(CqcPirColumns):
    cqc_pir_import_date: str = "cqc_pir_import_date"
    pir_submission_date_as_date: str = "cqc_pir_submission_date"
    pir_people_directly_employed_cleaned: str = "pir_people_directly_employed_cleaned"

    care_home: str = NewCqcLocationApiColumns.care_home


@dataclass
class NullPeopleDirectlyEmployedTemporaryColumns:
    """The names of the temporary columns created during the clean people_directly_employed outliers process."""

    absolute_deviation: str = "absolute_deviation"
    dispersion_outlier_flag: str = "dispersion_outlier_flag"
    dispersion_ratio: str = "dispersion_ratio"
    max_people_employed: str = "max_people_employed"
    mean_people_employed: str = "mean_people_employed"
    median_absolute_deviation: str = "median_absolute_deviation"
    median_absolute_deviation_flag: str = "median_absolute_deviation_flag"
    median_people_employed: str = "median_people_employed"
    min_people_employed: str = "min_people_employed"
    outlier_flag: str = "outlier_flag"
    submission_count: str = "submission_count"
