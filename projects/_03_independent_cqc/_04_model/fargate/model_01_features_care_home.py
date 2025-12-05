import polars as pl

import projects._03_independent_cqc._04_model.utils.feature_utils as fUtils
import projects._03_independent_cqc._04_model.utils.paths as pUtils
import projects._03_independent_cqc._04_model.utils.validate_model_definitions as vUtils
from polars_utils import utils
from projects._03_independent_cqc._04_model.registry.model_registry import (
    model_registry,
)
from projects._03_independent_cqc._04_model.utils.value_labels import (
    RegionLabels,
    RuralUrbanLabels,
    ServicesLabels,
    SpecialismsLabels,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_names.ind_cqc_pipeline_columns import ModelRegistryKeys as MRKeys
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_values.categorical_column_values import CareHome

partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]


def main(bucket_name: str, model_name: str) -> None:
    """
    Creates a features dataset specific to the model.

    The steps in this function are:
        1. Create paths for source and destination files
        2. Validate required model and model definitions exist, then assign them to variables
        3. Load source dataset
        4. Apply feature engineering steps
        5. Select relevant columns and non-null feature values
        6. Saves the features dataset to parquet

    Args:
        bucket_name (str): the bucket (name only) in which to source and save the datasets to
        model_name (str): the name of the model to create features for
    """
    print(f"Creating {model_name} features dataset...")

    source = pUtils.generate_ind_cqc_path(bucket_name)
    destination = pUtils.generate_features_path(bucket_name, model_name)

    vUtils.validate_model_definition(
        model_name,
        required_keys=[MRKeys.dependent, MRKeys.features],
        model_registry=model_registry,
    )
    dependent_col = model_registry[model_name][MRKeys.dependent]
    feature_cols = model_registry[model_name][MRKeys.features]

    lf = utils.scan_parquet(source).filter(
        pl.col(IndCQC.care_home) == CareHome.care_home
    )
    lf = fUtils.add_date_index_column(lf)

    lf = fUtils.add_array_column_count(
        lf, IndCQC.service_count, IndCQC.services_offered
    )
    lf = fUtils.cap_integer_at_max_value(
        lf,
        IndCQC.service_count,
        max_value=4,
        new_col_name=IndCQC.service_count_capped,
    )

    lf = fUtils.add_array_column_count(
        lf, IndCQC.activity_count, IndCQC.regulated_activities_offered
    )
    lf = fUtils.cap_integer_at_max_value(
        lf,
        IndCQC.activity_count,
        max_value=3,
        new_col_name=IndCQC.activity_count_capped,
    )

    lf = fUtils.expand_encode_and_extract_features(
        lf,
        IndCQC.services_offered,
        ServicesLabels.care_home_labels_dict,
        is_array_col=True,
    )
    lf = fUtils.expand_encode_and_extract_features(
        lf,
        IndCQC.specialisms_offered,
        SpecialismsLabels.labels_dict,
        is_array_col=True,
    )
    lf = fUtils.expand_encode_and_extract_features(
        lf,
        IndCQC.current_rural_urban_indicator_2011,
        RuralUrbanLabels.care_home_labels_dict,
        is_array_col=False,
    )
    lf = fUtils.expand_encode_and_extract_features(
        lf,
        IndCQC.current_region,
        RegionLabels.labels_dict,
        is_array_col=False,
    )

    features_lf = fUtils.select_and_filter_features_data(
        lf, feature_cols, dependent_col, partition_keys
    )

    features_lf = features_lf.cast({pl.Int64: pl.UInt32})
    features_lf = features_lf.cast({pl.Int32: pl.UInt32})
    features_lf = features_lf.cast({pl.Int8: pl.UInt8})

    print("Schema before sinking:")
    print(lf.describe())

    utils.sink_to_parquet(
        features_lf,
        destination,
        partition_cols=partition_keys,
        append=False,
    )


if __name__ == "__main__":

    args = utils.get_args(
        ("--bucket_name", "The bucket to source and save the datasets to"),
        ("--model_name", "The name of the model to create features for"),
    )

    main(bucket_name=args.bucket_name, model_name=args.model_name)
