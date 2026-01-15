import os

os.environ["SPARK_VERSION"] = "3.5"

from pyspark.sql import DataFrame

from projects._03_independent_cqc._04_feature_engineering.utils.value_labels import (
    RegionLabels,
    RuralUrbanLabels,
    ServicesLabels,
    SpecialismsLabels,
)
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome


def main(
    ind_cqc_filled_posts_cleaned_source: str,
    care_home_ind_cqc_features_destination: str,
) -> DataFrame:
    print("Creating care home features dataset...")

    locations_df = utils.read_from_parquet(ind_cqc_filled_posts_cleaned_source)

    filtered_df = utils.select_rows_with_value(
        locations_df, IndCQC.care_home, CareHome.care_home
    )
    filtered_df = utils.select_rows_with_non_null_value(
        filtered_df, IndCQC.number_of_beds
    )
