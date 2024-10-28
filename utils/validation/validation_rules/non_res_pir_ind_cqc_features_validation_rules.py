from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCqc
from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class NonResPirIndCqcFeaturesValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            IndCqc.location_id,
            IndCqc.cqc_location_import_date,
            IndCqc.care_home,
            IndCqc.features,
        ],
        RuleName.index_columns: [
            IndCqc.location_id,
            IndCqc.cqc_location_import_date,
        ],
        RuleName.min_values: {
            IndCqc.people_directly_employed_dedup: 1.0,
            IndCqc.imputed_non_res_people_directly_employed: 0.01,
        },
        RuleName.max_values: {
            IndCqc.people_directly_employed_dedup: 3000.0,
            IndCqc.imputed_non_res_people_directly_employed: 3000.0,
        },
        RuleName.categorical_values_in_columns: {
            IndCqc.care_home: CatValues.care_home_column_non_care_home_values.categorical_values,
        },
        RuleName.distinct_values: {
            IndCqc.care_home: CatValues.care_home_column_non_care_home_values.count_of_categorical_values,
        },
    }
