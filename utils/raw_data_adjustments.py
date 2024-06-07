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
    raw_worker_df = raw_worker_df.where(
        (raw_worker_df[AWKClean.worker_id] != "1737540")
        | (raw_worker_df[AWKClean.import_date] != "20230802")
        | (raw_worker_df[AWKClean.establishment_id] != "28208")
    )
    return raw_worker_df


def remove_duplicate_record_in_raw_pir_data(raw_pir_df: DataFrame) -> DataFrame:
    raw_pir_df = raw_pir_df.where(
        (raw_pir_df[CQCPIR.location_id] != "1-1199876096")
        | (raw_pir_df[Keys.import_date] != "20230601")
        | (raw_pir_df[CQCPIR.pir_type] != "Residential")
        | (raw_pir_df[CQCPIR.pir_submission_date] != "24-May-23")
        | (raw_pir_df[CQCPIR.domiciliary_care].isNotNull())
    )
    return raw_pir_df
