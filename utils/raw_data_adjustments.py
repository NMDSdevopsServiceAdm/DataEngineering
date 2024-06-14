from pyspark.sql import DataFrame

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
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


def remove_duplicate_record_in_raw_pir_data(raw_pir_df: DataFrame) -> DataFrame:
    """
    This function removes a record known to be a duplicate in the raw data.

    The location ID for this import date, PIR type and PIR submission date
    exists twice. One of these records had a zero value for cdomicilary
    care and one had a null value, so this function removes the null value.
    """
    raw_pir_df = raw_pir_df.where(
        (raw_pir_df[CQCPIR.location_id] != "1-1199876096")
        | (raw_pir_df[Keys.import_date] != "20230601")
        | (raw_pir_df[CQCPIR.pir_type] != "Residential")
        | (raw_pir_df[CQCPIR.pir_submission_date] != "24-May-23")
        | (raw_pir_df[CQCPIR.domiciliary_care].isNotNull())
    )
    return raw_pir_df


def remove_dental_practice_from_locations_data(raw_pir_df: DataFrame) -> DataFrame:
    """
    This function removes a record which is mislabelled in the raw data
    as a Social Care Org.

    The location is listed once as a social care org in the locations
    dataset but is lited as Primary Dental Care on every other row and
    in the providers dataset. The location ID and import date are enough
    to identify and remove this row.
    """
    return raw_pir_df
