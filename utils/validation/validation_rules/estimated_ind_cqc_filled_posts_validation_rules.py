from dataclasses import dataclass
import time

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsCategoricalValues as CatValues,
)
from utils.validation.validation_rule_custom_type import CustomValidationRules
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class EstimatedIndCqcFilledPostsValidationRules:
    rules_to_check = {
        RuleName.size_of_dataset: None,
        RuleName.complete_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.ascwds_workplace_import_date,
            IndCqcColumns.cqc_location_import_date,
            IndCqcColumns.care_home,
            IndCqcColumns.primary_service_type,
            IndCqcColumns.current_ons_import_date,
            IndCqcColumns.current_cssr,
            IndCqcColumns.current_region,
            IndCqcColumns.unix_time,
            IndCqcColumns.estimate_filled_posts,
            IndCqcColumns.estimate_filled_posts_source,
        ],
        RuleName.index_columns: [
            IndCqcColumns.location_id,
            IndCqcColumns.cqc_location_import_date,
        ],
        RuleName.min_values: {
            IndCqcColumns.ascwds_filled_posts: 1.0,
            IndCqcColumns.ascwds_pir_merged: 1.0,
            IndCqcColumns.care_home_model: 1.0,
            # IndCqcColumns.imputed_posts_non_res_with_dormancy_model: 1.0, # temporarily removed until non res models are fixed
            IndCqcColumns.estimate_filled_posts: 1.0,
            IndCqcColumns.number_of_beds: 1,
            IndCqcColumns.pir_people_directly_employed_dedup: 1,
            IndCqcColumns.non_res_pir_linear_regression_model: 0.01,
            IndCqcColumns.unix_time: 1262304000,  # 1st Jan 2010 in unix time
        },
        RuleName.max_values: {
            IndCqcColumns.ascwds_filled_posts: 3000.0,
            IndCqcColumns.ascwds_pir_merged: 3000.0,
            IndCqcColumns.imputed_posts_care_home_model: 3000.0,
            # IndCqcColumns.imputed_posts_non_res_with_dormancy_model: 3000.0, # temporarily removed until non res models are fixed
            IndCqcColumns.care_home_model: 3000.0,
            # IndCqcColumns.estimate_filled_posts: 3000.0, # temporarily removed until non res models are fixed
            IndCqcColumns.non_res_with_dormancy_model: 3000.0,
            IndCqcColumns.non_res_without_dormancy_model: 3000.0,
            IndCqcColumns.number_of_beds: 500,
            # IndCqcColumns.pir_people_directly_employed_dedup: 3000, # temporarily removed until PIR data is cleaned
            IndCqcColumns.non_res_pir_linear_regression_model: 3000.0,
            IndCqcColumns.unix_time: int(time.time()),  # current unix time
        },
        RuleName.categorical_values_in_columns: {
            IndCqcColumns.care_home: CatValues.care_home_column_values.categorical_values,
            IndCqcColumns.primary_service_type: CatValues.primary_service_type_column_values.categorical_values,
            IndCqcColumns.current_cssr: CatValues.current_cssr_column_values.categorical_values,
            IndCqcColumns.current_region: CatValues.current_region_column_values.categorical_values,
            IndCqcColumns.ascwds_filled_posts_source: CatValues.ascwds_filled_posts_source_column_values.categorical_values,
            IndCqcColumns.estimate_filled_posts_source: CatValues.estimate_filled_posts_source_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            IndCqcColumns.care_home: CatValues.care_home_column_values.count_of_categorical_values,
            IndCqcColumns.primary_service_type: CatValues.primary_service_type_column_values.count_of_categorical_values,
            IndCqcColumns.current_cssr: CatValues.current_cssr_column_values.count_of_categorical_values,
            IndCqcColumns.current_region: CatValues.current_region_column_values.count_of_categorical_values,
            IndCqcColumns.ascwds_filled_posts_source: CatValues.ascwds_filled_posts_source_column_values.count_of_categorical_values,
            # IndCqcColumns.estimate_filled_posts_source: CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values, # temporarily removed whilst working on non res models - imputed_posts_non_res_with_dormancy_model not currently being selected
        },
        RuleName.custom_type: CustomValidationRules.care_home_and_primary_service_type,
    }
