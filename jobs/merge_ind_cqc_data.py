import sys
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.cleaned_data_files.cqc_provider_cleaned_values import (
    CqcProviderCleanedColumns as CQCPClean,
    CqcProviderCleanedValues as CQCPValues,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    MergeIndCqcColumnsToImport as ImportColList,
)

PartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(
    cleaned_cqc_location_source: str,
    cleaned_cqc_pir_source: str,
    cleaned_ascwds_workplace_source: str,
    destination: str,
):
    cqc_location_df = utils.read_from_parquet(
        cleaned_cqc_location_source, selected_columns=ImportColList.cqc_column_list
    )

    ascwds_workplace_df = utils.read_from_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=ImportColList.ascwds_column_list,
    )

    cqc_pir_df = utils.read_from_parquet(cleaned_cqc_pir_source)

    ind_cqc_location_df = filter_df_to_independent_sector_only(cqc_location_df)

    ind_cqc_location_df = join_ascwds_data_into_merged_df(
        ind_cqc_location_df,
        ascwds_workplace_df,
        CQCLClean.cqc_location_import_date,
        AWPClean.ascwds_workplace_import_date,
    )

    utils.write_to_parquet(
        ind_cqc_location_df,
        destination,
        mode="overwrite",
        partitionKeys=PartitionKeys,
    )


def filter_df_to_independent_sector_only(df: DataFrame) -> DataFrame:
    return df.where(F.col(CQCPClean.cqc_sector) == CQCPValues.independent)


def join_ascwds_data_into_merged_df(
    primary_df: DataFrame,
    secondary_df: DataFrame,
    primary_import_date_column: str,
    secondary_import_date_column: str,
) -> DataFrame:
    primary_df_with_secondary_import_date = cUtils.add_aligned_date_column(
        primary_df,
        secondary_df,
        primary_import_date_column,
        secondary_import_date_column,
    )

    import_date_column_to_drop: str = secondary_import_date_column + "_to_drop"
    location_id_column_to_drop: str = AWPClean.location_id + "_to_drop"

    secondary_df = secondary_df.withColumnsRenamed(
        {
            secondary_import_date_column: import_date_column_to_drop,
            AWPClean.location_id: location_id_column_to_drop,
        }
    )

    merged_df = primary_df_with_secondary_import_date.join(
        secondary_df,
        (
            primary_df_with_secondary_import_date[secondary_import_date_column]
            == secondary_df[import_date_column_to_drop]
        )
        & (
            primary_df_with_secondary_import_date[CQCLClean.location_id]
            == secondary_df[location_id_column_to_drop]
        ),
        how="left",
    ).drop(import_date_column_to_drop, location_id_column_to_drop)

    return merged_df


if __name__ == "__main__":
    print("Spark job 'merge_ind_cqc_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cleaned_cqc_location_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        destination,
    ) = utils.collect_arguments(
        (
            "--cleaned_cqc_location_source",
            "Source s3 directory for parquet CQC locations cleaned dataset",
        ),
        (
            "--cleaned_cqc_pir_source",
            "Source s3 directory for parquet CQC pir cleaned dataset",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for parquet ASCWDS workplace cleaned dataset",
        ),
        (
            "--destination",
            "Destination s3 directory for parquet",
        ),
    )
    main(
        cleaned_cqc_location_source,
        cleaned_cqc_pir_source,
        cleaned_ascwds_workplace_source,
        destination,
    )

    print("Spark job 'merge_ind_cqc_data' complete")
