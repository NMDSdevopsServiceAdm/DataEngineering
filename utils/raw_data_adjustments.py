from pyspark.sql import DataFrame

from utils.column_names.raw_data_files.ascwds_worker_columns import (
    AscwdsWorkerColumns as AWK,
)


def remove_duplicate_worker_in_raw_worker_data(raw_worker_df: DataFrame) -> DataFrame:
    if AWK.worker_id in raw_worker_df.columns:
        raw_worker_df = raw_worker_df.where(
            (raw_worker_df[AWK.worker_id] != "1737540")
            | (raw_worker_df[AWK.import_date] != "20230802")
            | (raw_worker_df[AWK.establishment_id] != "28208")
        )
    return raw_worker_df
