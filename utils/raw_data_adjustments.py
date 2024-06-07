from pyspark.sql import DataFrame

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


def remove_duplicate_worker_in_raw_worker_data(raw_worker_df: DataFrame) -> DataFrame:
    raw_worker_df = raw_worker_df.where(
        (raw_worker_df[AWKClean.worker_id] != "1737540")
        | (raw_worker_df[AWKClean.import_date] != "20230802")
        | (raw_worker_df[AWKClean.establishment_id] != "28208")
    )
    return raw_worker_df


def remove_duplicate_record_in_raw_pir_data(raw_pir_df: DataFrame) -> DataFrame:
    return raw_pir_df
