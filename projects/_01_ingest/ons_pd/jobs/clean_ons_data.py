import sys
from pyspark.sql import DataFrame

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)


onsPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(ons_source: str, cleaned_ons_destination: str):
    contemporary_ons_df = utils.read_from_parquet(ons_source)

    contemporary_ons_df = cUtils.column_to_date(
        contemporary_ons_df, Keys.import_date, ONSClean.contemporary_ons_import_date
    )

    refactored_contemporary_ons_df = prepare_contemporary_ons_data(contemporary_ons_df)

    refactored_current_ons_df = prepare_current_ons_data(contemporary_ons_df)

    cleaned_df = refactored_contemporary_ons_df.join(
        refactored_current_ons_df, ONSClean.postcode, "left"
    )

    truncated_df = cUtils.truncate_postcode(
        cleaned_df,
        ONSClean.postcode,
        drop_postcode_col=True,
    )

    # TODO - count splits in each and keep the most common

    utils.write_to_parquet(
        cleaned_df,
        cleaned_ons_destination,
        mode="overwrite",
        partitionKeys=onsPartitionKeys,
    )

    utils.write_to_parquet(
        truncated_df,
        truncated_ons_destination,
        mode="overwrite",
        partitionKeys=onsPartitionKeys,
    )


def prepare_current_ons_data(df: DataFrame) -> DataFrame:
    df = utils.filter_df_to_maximum_value_in_column(
        df, ONSClean.contemporary_ons_import_date
    )
    current_ons_df = df.select(
        df[ONSClean.postcode],
        df[ONSClean.contemporary_ons_import_date].alias(
            ONSClean.current_ons_import_date
        ),
        df[ONSClean.cssr].alias(ONSClean.current_cssr),
        df[ONSClean.region].alias(ONSClean.current_region),
        df[ONSClean.sub_icb].alias(ONSClean.current_sub_icb),
        df[ONSClean.icb].alias(ONSClean.current_icb),
        df[ONSClean.icb_region].alias(ONSClean.current_icb_region),
        df[ONSClean.ccg].alias(ONSClean.current_ccg),
        df[ONSClean.latitude].alias(ONSClean.current_latitude),
        df[ONSClean.longitude].alias(ONSClean.current_longitude),
        df[ONSClean.imd_score].alias(ONSClean.current_imd_score),
        df[ONSClean.lower_super_output_area_2011].alias(ONSClean.current_lsoa11),
        df[ONSClean.middle_super_output_area_2011].alias(ONSClean.current_msoa11),
        df[ONSClean.rural_urban_indicator_2011].alias(
            ONSClean.current_rural_urban_ind_11
        ),
        df[ONSClean.lower_super_output_area_2021].alias(ONSClean.current_lsoa21),
        df[ONSClean.middle_super_output_area_2021].alias(ONSClean.current_msoa21),
        df[ONSClean.parliamentary_constituency].alias(ONSClean.current_constituency),
    )

    return current_ons_df


def prepare_contemporary_ons_data(df: DataFrame) -> DataFrame:
    df = df.select(
        df[ONSClean.postcode],
        df[ONSClean.contemporary_ons_import_date],
        df[ONSClean.cssr].alias(ONSClean.contemporary_cssr),
        df[ONSClean.region].alias(ONSClean.contemporary_region),
        df[ONSClean.sub_icb].alias(ONSClean.contemporary_sub_icb),
        df[ONSClean.icb].alias(ONSClean.contemporary_icb),
        df[ONSClean.icb_region].alias(ONSClean.contemporary_icb_region),
        df[ONSClean.ccg].alias(ONSClean.contemporary_ccg),
        df[ONSClean.latitude].alias(ONSClean.contemporary_latitude),
        df[ONSClean.longitude].alias(ONSClean.contemporary_longitude),
        df[ONSClean.imd_score].alias(ONSClean.contemporary_imd_score),
        df[ONSClean.lower_super_output_area_2011].alias(ONSClean.contemporary_lsoa11),
        df[ONSClean.middle_super_output_area_2011].alias(ONSClean.contemporary_msoa11),
        df[ONSClean.rural_urban_indicator_2011].alias(
            ONSClean.contemporary_rural_urban_ind_11
        ),
        df[ONSClean.lower_super_output_area_2021].alias(ONSClean.contemporary_lsoa21),
        df[ONSClean.middle_super_output_area_2021].alias(ONSClean.contemporary_msoa21),
        df[ONSClean.parliamentary_constituency].alias(
            ONSClean.contemporary_constituency
        ),
        df[Keys.year],
        df[Keys.month],
        df[Keys.day],
        df[Keys.import_date],
    )
    return df


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
