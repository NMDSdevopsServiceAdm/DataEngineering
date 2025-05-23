from dataclasses import dataclass

from pyspark.sql import DataFrame

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_pir_cleaned import (
    CqcPirColumns as CQCPIR,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


def remove_duplicate_worker_in_raw_worker_data(raw_worker_df: DataFrame) -> DataFrame:
    """
    This function removes a record known to be a duplicate in the raw data.

    The worker ID exists in other files but not under this establishment
    ID. No worker IDs should be duplicated accross establishments, so this
    record was selected for removal.
    """
    raw_worker_df = raw_worker_df.where(
        (raw_worker_df[AWKClean.worker_id] != "1737540")
        | (raw_worker_df[AWKClean.import_date] != "20230802")
        | (raw_worker_df[AWKClean.establishment_id] != "28208")
    )
    return raw_worker_df


def remove_records_from_locations_data(
    raw_locations_df: DataFrame,
) -> DataFrame:
    """
    This function removes records from the locations dataset.
    """
    raw_locations_df = raw_locations_df.where(
        (
            raw_locations_df[CQCL.location_id]
            != RecordsToRemoveInLocationsData.dental_practice
        )
        & (
            raw_locations_df[CQCL.location_id]
            != RecordsToRemoveInLocationsData.temp_registration
        )
    )
    return raw_locations_df


@dataclass
class RecordsToRemoveInLocationsData:
    """
    This class contains the locations ids that should be removed from
    the locations data.

    Dental Practice:
    The location is listed once as a social care org in the locations
    dataset but is lited as Primary Dental Care on every other row and
    in the providers dataset. The location ID is enough to identify
    and remove this row.

    Temporary Registration:
    The location is listed once as registered in the locations dataset,
    but conatins barely any data and appears to have deregistered very
    quickly. The location ID is enough to identify and remove this row.
    """

    dental_practice: str = "1-12082335777"
    temp_registration: str = "1-127367030"
