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

CURRENT_PREFIX:str = "current_"
CONTEMPORARY_PREFIX:str = "contemporary_"


onsPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(ons_source: str, cleaned_ons_destination: str):
    contemporary_ons_df = utils.read_from_parquet(ons_source)

    contemporary_ons_df = cUtils.column_to_date(
        contemporary_ons_df, Keys.import_date, ONSClean.contemporary_ons_import_date
    )

    refactored_contemporary_ons_df = refactor_columns_with_prefix(
        contemporary_ons_df, CONTEMPORARY_PREFIX
    )

    refactored_current_ons_df = prepare_current_ons_data(contemporary_ons_df)

    contemporary_with_current_ons_df = join_current_ons_df_into_contemporary_df(
        refactored_contemporary_ons_df, refactored_current_ons_df
    )

    utils.write_to_parquet(
        contemporary_with_current_ons_df,
        cleaned_ons_destination,
        mode="overwrite",
        partitionKeys=onsPartitionKeys,
    )


def prepare_current_ons_data(df: DataFrame) -> DataFrame:
    max_import_date = df.agg(F.max(ONSClean.contemporary_ons_import_date)).collect()[0][
        0
    ]
    current_ons_df = df.filter(
        F.col(ONSClean.contemporary_ons_import_date) == max_import_date
    )

    refactored_df = refactor_columns_with_prefix(
        current_ons_df, CURRENT_PREFIX
    )

    return refactored_df.drop(Keys.year, Keys.month, Keys.day, Keys.import_date)


def refactor_columns_with_prefix(df: DataFrame, prefix: str) -> DataFrame:
    if (prefix == "contemporary"):
        df = df.select(
            ONSClean.postcode,
            ONSClean.contemporary_ons_import_date,
            ONSClean.cssr.alias(ONSClean.contemporary_cssr),
            ONSClean.region.alias(ONSClean.contemporary_region),
            ONSClean.sub_icb.alias(ONSClean.contemporary_sub_icb),
            ONSClean.icb.alias(ONSClean.contemporary_icb),
            ONSClean.icb_region.alias(ONSClean.contemporary_icb_region),
            ONSClean.ccg.alias(ONSClean.contemporary_ccg),
            ONSClean.latitude.alias(ONSClean.contemporary_latitude),
            ONSClean.longitude.alias(ONSClean.contemporary_longitude),
            ONSClean.imd_score.alias(ONSClean.contemporary_imd_score),
            ONSClean.lower_super_output_area_2011.alias(ONSClean.contemporary_lsoa11),
            ONSClean.middle_super_output_area_2011.alias(ONSClean.contemporary_msoa11),
            ONSClean.rural_urban_indicator_2011.alias(ONSClean.contemporary_rural_urban_ind_11),
            ONSClean.lower_super_output_area_2021.alias(ONSClean.contemporary_lsoa21),
            ONSClean.middle_super_output_area_2021.alias(ONSClean.contemporary_msoa21),
            ONSClean.westminster_parliamentary_consitituency.alias(ONSClean.contemporary_constituancy),
            Keys.year,
            Keys.month,
            Keys.day,
            Keys.import_date,
        )
    elif (prefix == "current"):
        df = df.select(
            ONSClean.postcode,
            ONSClean.contemporary_ons_import_date.alias(ONSClean.current_ons_import_date),
            ONSClean.cssr.alias(ONSClean.current_cssr),
            ONSClean.region.alias(ONSClean.current_region),
            ONSClean.sub_icb.alias(ONSClean.current_sub_icb),
            ONSClean.icb.alias(ONSClean.current_icb),
            ONSClean.icb_region.alias(ONSClean.current_icb_region),
            ONSClean.ccg.alias(ONSClean.current_ccg),
            ONSClean.latitude.alias(ONSClean.current_latitude),
            ONSClean.longitude.alias(ONSClean.current_longitude),
            ONSClean.imd_score.alias(ONSClean.current_imd_score),
            ONSClean.lower_super_output_area_2011.alias(ONSClean.current_lsoa11),
            ONSClean.middle_super_output_area_2011.alias(ONSClean.current_msoa11),
            ONSClean.rural_urban_indicator_2011.alias(ONSClean.current_rural_urban_ind_11),
            ONSClean.lower_super_output_area_2021.alias(ONSClean.current_lsoa21),
            ONSClean.middle_super_output_area_2021.alias(ONSClean.current_msoa21),
            ONSClean.westminster_parliamentary_consitituency.alias(ONSClean.current_constituancy),
            Keys.year,
            Keys.month,
            Keys.day,
            Keys.import_date,
        )
    return df


def join_current_ons_df_into_contemporary_df(
    contemporary_ons_df: DataFrame, current_ons_df: DataFrame
) -> DataFrame:
    return contemporary_ons_df.join(current_ons_df, ONSClean.postcode, "left")


if __name__ == "__main__":
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
    main(source, destination)

    print("Spark job 'clean_ons_data' complete")
