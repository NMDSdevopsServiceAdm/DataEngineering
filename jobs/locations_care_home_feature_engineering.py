import argparse
import sys
from dataclasses import dataclass

import pyspark
import pyspark.sql.functions as F

from utils import utils
from utils.feature_engineering_dictionaries import (
    SERVICES_LOOKUP,
    RURAL_URBAN_INDICATOR_LOOKUP,
)
from utils.features.helper import (
    filter_records_since_snapshot_date,
    vectorise_dataframe,
    column_expansion_with_dict,
    get_list_of_distinct_ons_regions,
    add_service_count_to_data,
    add_rui_data_data_frame,
    explode_column_from_distinct_values,
    add_date_diff_into_df,
)


@dataclass
class ColNamesFromPrepareLocations:
    ons_region: str = "ons_region"
    services_offered: str = "services_offered"
    cqc_sector: str = "cqc_sector"
    rui_indicator: str = "rui_2011"
    number_of_beds: str = "number_of_beds"
    people_directly_employed: str = "people_directly_employed"
    carehome: str = "carehome"
    snapshot_date: str = "snapshot_date"


@dataclass
class NewColNames:
    service_count: str = "service_count"
    date_diff: str = "date_diff"


@dataclass
class FeatureNames:
    care_home: str = "features"


def main(prepared_locations_source, destination=None):
    spark = utils.get_spark()

    features_from_prepare_locations = ColNamesFromPrepareLocations()
    new_cols_for_features = NewColNames()
    services_dict = SERVICES_LOOKUP
    rural_urban_indicator_dict = RURAL_URBAN_INDICATOR_LOOKUP

    locations_df = spark.read.option("basePath", prepared_locations_source).parquet(
        prepared_locations_source
    )
    max_snapshot = utils.get_max_snapshot_partitions(destination)
    locations_df = filter_records_since_snapshot_date(locations_df, max_snapshot)

    filtered_loc_data = filter_locations_df_for_independent_care_home_data(
        df=locations_df,
        carehome_col_name=features_from_prepare_locations.carehome,
        cqc_col_name=features_from_prepare_locations.cqc_sector,
    )
    data_with_service_count = add_service_count_to_data(
        df=filtered_loc_data,
        new_col_name=new_cols_for_features.service_count,
        col_to_check=features_from_prepare_locations.services_offered,
    )
    service_keys = list(services_dict.keys())
    data_with_expanded_services = column_expansion_with_dict(
        df=data_with_service_count,
        col_name=features_from_prepare_locations.services_offered,
        lookup_dict=services_dict,
    )
    rui_indicators = list(rural_urban_indicator_dict.keys())
    data_with_rui = add_rui_data_data_frame(
        df=data_with_expanded_services,
        rui_col_name=features_from_prepare_locations.rui_indicator,
        lookup_dict=rural_urban_indicator_dict,
    )

    distinct_regions = get_list_of_distinct_ons_regions(
        df=data_with_rui,
        col_name=features_from_prepare_locations.ons_region,
    )

    data_with_region_cols, regions = explode_column_from_distinct_values(
        df=data_with_rui,
        column_name=features_from_prepare_locations.ons_region,
        col_prefix="ons_",
        col_list_set=set(distinct_regions),
    )

    data_with_date_diff = add_date_diff_into_df(
        df=data_with_region_cols,
        new_col_name=new_cols_for_features.date_diff,
        snapshot_date_col=features_from_prepare_locations.snapshot_date,
    )

    list_for_vectorisation: list[str] = sorted(
        [
            new_cols_for_features.service_count,
            features_from_prepare_locations.number_of_beds,
            new_cols_for_features.date_diff,
        ]
        + service_keys
        + regions
        + rui_indicators
    )

    vectorised_dataframe = vectorise_dataframe(
        df=data_with_date_diff, list_for_vectorisation=list_for_vectorisation
    )
    features_df = vectorised_dataframe.select(
        "locationid",
        "snapshot_date",
        "ons_region",
        "number_of_beds",
        "people_directly_employed",
        "snapshot_year",
        "snapshot_month",
        "snapshot_day",
        "carehome",
        "features",
        "job_count",
    )

    print("distinct_regions")
    print(distinct_regions)
    print("number_of_features:")
    print(len(list_for_vectorisation))
    print(f"length of feature df: {vectorised_dataframe.count()}")

    if destination:
        print(f"Exporting as parquet to {destination}")
        utils.write_to_parquet(
            features_df,
            destination,
            mode="append",
            partitionKeys=["snapshot_year", "snapshot_month", "snapshot_day"],
        )
    return features_df


def filter_locations_df_for_independent_care_home_data(
    df: pyspark.sql.DataFrame, carehome_col_name: str, cqc_col_name: str
) -> pyspark.sql.DataFrame:
    care_home_data = df.filter(F.col(carehome_col_name) == "Y")
    independent_care_home_data = care_home_data.filter(
        F.col(cqc_col_name) == "Independent"
    )
    return independent_care_home_data


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--prepared_locations_source",
        help="Source S3 directory for data engineering prepared locations dataset",
        required=True,
    )
    parser.add_argument(
        "--destination",
        help="A destination directory for outputting locations dataset after feature engineering and vectorizing",
        required=True,
    )
    args, _ = parser.parse_known_args()

    return args.prepared_locations_source, args.destination


if __name__ == "__main__":
    print("Spark job 'locations_feature_engineering' starting...")
    print(f"Job parameters: {sys.argv}")

    (prepared_locations_source, destination) = collect_arguments()

    main(prepared_locations_source, destination)

    print("Spark job 'locations_feature_engineering' complete")
