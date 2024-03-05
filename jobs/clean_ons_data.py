import sys

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from utils import utils
import utils.cleaning_utils as cUtils

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.cleaned_data_files.ons_cleaned_values import (
    OnsCleanedColumns as ONSClean,
)


onsPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(ons_source: str, cleaned_ons_destination: str):
    ons_df = utils.read_from_parquet(ons_source)

    ons_df = cUtils.column_to_date(ons_df, Keys.import_date, ONSClean.ons_import_date)

    refactored_ons_df = refactor_columns_as_struct_with_alias(
        ons_df, ONSClean.contemporary
    )

    current_ons_df = prepare_current_ons_data(ons_df)

    refactored_ons_with_current_ons_df = refactored_ons_df.join(
        current_ons_df, ONSClean.postcode, "left"
    )

    utils.write_to_parquet(
        refactored_ons_with_current_ons_df,
        cleaned_ons_destination,
        mode="overwrite",
        partitionKeys=onsPartitionKeys,
    )


def prepare_current_ons_data(df: DataFrame) -> DataFrame:
    max_import_date = df.agg(F.max(ONSClean.ons_import_date)).collect()[0][0]
    current_ons_df = df.filter(F.col(ONSClean.ons_import_date) == max_import_date)

    refactored_df = refactor_columns_as_struct_with_alias(
        current_ons_df, ONSClean.current
    )

    refactored_df = refactored_df.withColumnRenamed(
        ONSClean.ons_import_date, ONSClean.current_ons_import_date
    )

    return refactored_df.drop(Keys.year, Keys.month, Keys.day, Keys.import_date)


def refactor_columns_as_struct_with_alias(df: DataFrame, alias: str) -> DataFrame:
    return df.select(
        ONSClean.postcode,
        ONSClean.ons_import_date,
        F.struct(
            ONSClean.cssr,
            ONSClean.region,
            ONSClean.sub_icb,
            ONSClean.icb,
            ONSClean.icb_region,
            ONSClean.ccg,
            ONSClean.latitude,
            ONSClean.longitude,
            ONSClean.imd_score,
            ONSClean.lower_super_output_area_2011,
            ONSClean.middle_super_output_area_2011,
            ONSClean.rural_urban_indicator_2011,
            ONSClean.lower_super_output_area_2021,
            ONSClean.middle_super_output_area_2021,
            ONSClean.westminster_parliamentary_consitituency,
        ).alias(alias),
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    )


if __name__ == "__main__":
    # Where we tell Glue how to run the file, and what to print out
    print("Spark job 'clean_ons_data' starting...")
    print(f"Job parameters: {sys.argv}")

    source, destination = utils.collect_arguments(
        (
            "--ons_source",
            "Source s3 directory for parquet ONS postcode directory dataset",
        ),
        (
            "--cleaned_ons_destination",
            "Destination s3 directory for cleaned parquet ONS postcode directory dataset",
        ),
    )
    # Python logic ---> all in main
    main(source, destination)

    print("Spark job 'clean_ons_data' complete")
