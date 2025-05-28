from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)
from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class NonResASCWDSWithDormancyIndCqcFeaturesValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
            IndCqcColumns.dormancy,
            IndCqcColumns.features,
            IndCqcColumns.current_region,
            IndCqcColumns.current_rural_urban_indicator_2011,
            IndCqcColumns.imputed_registration_date,
            IndCqcColumns.time_registered_capped_at_ten_years,
            IndCqcColumns.time_since_dormant_capped_at_ten_years,
            IndCqcColumns.service_count,
            IndCqcColumns.activity_count_capped,
        ],
        RuleName.index_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
        ],
        RuleName.min_values: {
            IndCqcColumns.ascwds_pir_merged: 1.0,
            IndCqcColumns.time_registered_capped_at_ten_years: 1.0,
            IndCqcColumns.time_since_dormant_capped_at_ten_years: 1.0,
            IndCqcColumns.service_count_capped: 1,
            IndCqcColumns.activity_count_capped: 0,
            IndCqcColumns.cqc_location_import_date_indexed: 1,
        },
        RuleName.max_values: {
            IndCqcColumns.ascwds_pir_merged: 3000.0,
            IndCqcColumns.activity_count_capped: 3,
            IndCqcColumns.time_registered_capped_at_ten_years: 120.0,
            IndCqcColumns.time_since_dormant_capped_at_ten_years: 120.0,
        },
        RuleName.categorical_values_in_columns: {
            IndCqcColumns.care_home: CatValues.care_home_column_non_care_home_values.categorical_values,
            IndCqcColumns.dormancy: CatValues.dormancy_column_values.categorical_values,
            IndCqcColumns.current_region: CatValues.current_region_column_values.categorical_values,
            IndCqcColumns.current_rural_urban_indicator_2011: CatValues.current_rui_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            IndCqcColumns.care_home: CatValues.care_home_column_non_care_home_values.count_of_categorical_values,
            IndCqcColumns.dormancy: CatValues.dormancy_column_values.count_of_categorical_values,
            IndCqcColumns.current_region: CatValues.current_region_column_values.count_of_categorical_values,
            IndCqcColumns.current_rural_urban_indicator_2011: CatValues.current_rui_column_values.count_of_categorical_values,
        },
    }
