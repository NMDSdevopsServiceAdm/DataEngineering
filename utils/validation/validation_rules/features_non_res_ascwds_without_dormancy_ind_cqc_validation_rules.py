from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)
from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class NonResASCWDSWithoutDormancyIndCqcFeaturesValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
            IndCqcColumns.features,
            IndCqcColumns.current_region,
            IndCqcColumns.current_rural_urban_indicator_2011,
            IndCqcColumns.imputed_registration_date,
            IndCqcColumns.time_registered,
            IndCqcColumns.service_count,
            IndCqcColumns.activity_count,
            IndCqcColumns.specialism_count,
        ],
        RuleName.index_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
        ],
        RuleName.min_values: {
            IndCqcColumns.ascwds_pir_merged: 1.0,
            IndCqcColumns.time_registered_capped_at_four_years: 1.0,
            # IndCqcColumns.service_count: 1, # Temporarily removed whilst we fix DQ
            IndCqcColumns.activity_count: 0,
            IndCqcColumns.specialism_count: 0,
            IndCqcColumns.cqc_location_import_date_indexed: 1,
        },
        RuleName.max_values: {
            IndCqcColumns.ascwds_pir_merged: 3000.0,
            IndCqcColumns.time_registered_capped_at_four_years: 48.0,
        },
        RuleName.categorical_values_in_columns: {
            IndCqcColumns.care_home: CatValues.care_home_column_non_care_home_values.categorical_values,
            IndCqcColumns.current_region: CatValues.current_region_column_values.categorical_values,
            IndCqcColumns.current_rural_urban_indicator_2011: CatValues.current_rui_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            IndCqcColumns.care_home: CatValues.care_home_column_non_care_home_values.count_of_categorical_values,
            IndCqcColumns.current_region: CatValues.current_region_column_values.count_of_categorical_values,
            IndCqcColumns.current_rural_urban_indicator_2011: CatValues.current_rui_column_values.count_of_categorical_values,
        },
    }
