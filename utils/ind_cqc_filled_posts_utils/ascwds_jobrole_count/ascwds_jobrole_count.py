from pyspark.sql import DataFrame, functions as F

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)


def count_job_roles_per_establishment(df: DataFrame) -> DataFrame:
    df = df.groupBy(
        F.col(AWKClean.establishment_id),
        F.col(AWKClean.ascwds_worker_import_date),
        F.col(AWKClean.main_job_role_clean_labelled),
    ).agg(F.count(F.col(AWKClean.main_job_role_clean_labelled)).alias("job_count"))
    return df


def mapped_column(df_agg: DataFrame) -> DataFrame:
    df_struct = df_agg.withColumn(
        "struct_column",
        F.struct(F.col(AWKClean.main_job_role_clean_labelled), F.col("job_count")),
    )
    df_mapped = df_struct.groupBy(
        F.col(AWKClean.establishment_id), F.col(AWKClean.ascwds_worker_import_date)
    ).agg(
        F.map_from_entries(F.collect_list("struct_column")).alias(
            "ascwds_main_job_role_counts"
        )
    )
    return df_mapped
