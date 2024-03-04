import argparse
import sys
from dataclasses import dataclass
from typing import List

import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as INDCQC
from utils.feature_engineering_dictionaries import (
    RURAL_URBAN_INDICATOR_LOOKUP,
    SERVICES_LOOKUP,
)
from utils.features.helper import (
    add_date_diff_into_df,
    add_rui_data_data_frame,
    add_service_count_to_data,
    column_expansion_with_dict,
    explode_column_from_distinct_values,
    filter_records_since_snapshot_date,
    get_list_of_distinct_ons_regions,
    vectorise_dataframe,
)


@dataclass
class NewColNames:
    service_count: str = "service_count"
    date_diff: str = "date_diff"


@dataclass
class FeatureNames:
    care_home: str = "features"


def main(cleaned_cqc_ind_source, destination):
    new_cols_for_features = NewColNames()
    services_dict = SERVICES_LOOKUP
    rural_urban_indicator_dict = RURAL_URBAN_INDICATOR_LOOKUP

    locations_df = utils.read_from_parquet(cleaned_cqc_ind_source)

    filtered_loc_data = filter_locations_df_for_independent_non_res_care_home_data(
        df=locations_df,
        carehome_col_name=INDCQC.care_home,
        cqc_col_name=INDCQC.cqc_sector,
    )

    filtered_data_with_employee_col = convert_col_to_integer_col(
        df=filtered_loc_data,
        col_name=INDCQC.people_directly_employed,
    )

    data_with_service_count = add_service_count_to_data(
        df=filtered_data_with_employee_col,
        new_col_name=new_cols_for_features.service_count,
        col_to_check=INDCQC.services_offered,
    )

    service_keys = list(services_dict.keys())
    data_with_expanded_services = column_expansion_with_dict(
        df=data_with_service_count,
        col_name=INDCQC.services_offered,
        lookup_dict=services_dict,
    )

    rui_indicators = list(rural_urban_indicator_dict.keys())
    data_with_rui = add_rui_data_data_frame(
        df=data_with_expanded_services,
        rui_col_name=INDCQC.current_rural_urban_indicator_2011,
        lookup_dict=rural_urban_indicator_dict,
    )

    distinct_regions = get_list_of_distinct_ons_regions(
        df=data_with_rui,
        col_name=INDCQC.current_region,
    )

    data_with_region_cols, regions = explode_column_from_distinct_values(
        df=data_with_rui,
        column_name=INDCQC.current_region,
        col_prefix="ons_",
        col_list_set=set(distinct_regions),
    )

    data_with_date_diff = add_date_diff_into_df(
        df=data_with_region_cols,
        new_col_name=new_cols_for_features.date_diff,
        snapshot_date_col=INDCQC.cqc_location_import_date,
    )

    list_for_vectorisation: List[str] = sorted(
        [
            new_cols_for_features.service_count,
            INDCQC.people_directly_employed,
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
        "date",
        "ons_region",
        "number_of_beds",
        "people_directly_employed",
        "year",
        "month",
        "day",
        "carehome",
        "features",
        "job_count",
    )

    print("distinct_regions")
    print(distinct_regions)
    print("number_of_features:")
    print(len(list_for_vectorisation))
    print(f"length of feature df: {vectorised_dataframe.count()}")

    print(f"Exporting as parquet to {destination}")
    utils.write_to_parquet(
        features_df,
        destination,
        mode="append",
        partitionKeys=["year", "month", "day"],
    )


def convert_col_to_integer_col(df, col_name):
    df = df.withColumn(col_name, F.col(col_name).cast("int"))
    return df


def filter_locations_df_for_independent_non_res_care_home_data(
    df: DataFrame, carehome_col_name: str, cqc_col_name: str
) -> DataFrame:
    care_home_data = df.filter(F.col(carehome_col_name) == "N")
    independent_care_home_data = care_home_data.filter(
        F.col(cqc_col_name) == "Independent"
    )
    return independent_care_home_data


def collect_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "---cleaned_cqc_ind_source",
        help="Source S3 directory for cleaned cqc ind data",
        required=True,
    )
    parser.add_argument(
        "--prepared_non_res_ind_cqc_destination",
        help="A destination directory for outputting prepared non res ind cqc data",
        required=True,
    )
    args, _ = parser.parse_known_args()

    return args.prepared_locations_source, args.destination


if __name__ == "__main__":
    print("Spark job 'prepare_non_res_ind_cqc_features' starting...")
    print(f"Job parameters: {sys.argv}")

    (cleaned_cqc_ind_source, destination) = collect_arguments()

    main(cleaned_cqc_ind_source, destination)

    print("Spark job 'prepare_non_res_ind_cqc_features' complete")
