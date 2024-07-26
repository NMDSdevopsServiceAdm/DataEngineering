from dataclasses import dataclass
import time

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)
from utils.column_values.categorical_columns_by_dataset import (
    EstimatedIndCQCFilledPostsCategoricalValues as CatValues,
)
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
            IndCqcColumns.number_of_beds: 1,
            IndCqcColumns.people_directly_employed: 1,
            IndCqcColumns.unix_time: 1262304000,  # 1st Jan 2010 in unix time
            IndCqcColumns.estimate_filled_posts: 1.0,
            IndCqcColumns.people_directly_employed_dedup: 1,
            IndCqcColumns.ascwds_filled_posts: 1.0,
            IndCqcColumns.ascwds_filled_posts_dedup_clean: 1.0,
            IndCqcColumns.rolling_average_model: 0.0,
            IndCqcColumns.interpolation_model: 0.0,
        },
        RuleName.max_values: {
            IndCqcColumns.number_of_beds: 500,
            IndCqcColumns.people_directly_employed: 10000,
            IndCqcColumns.unix_time: int(time.time()),  # current unix time
            IndCqcColumns.estimate_filled_posts: 3000.0,
            IndCqcColumns.people_directly_employed_dedup: 10000,
            IndCqcColumns.ascwds_filled_posts: 3000.0,
            IndCqcColumns.ascwds_filled_posts_dedup_clean: 3000.0,
            IndCqcColumns.rolling_average_model: 3000.0,
            IndCqcColumns.extrapolation_care_home_model: 3000.0,
            IndCqcColumns.interpolation_model: 3000.0,
            IndCqcColumns.care_home_model: 3000.0,
            IndCqcColumns.non_res_with_dormancy_model: 3000.0,
            IndCqcColumns.non_res_without_dormancy_model: 3000.0,
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
            IndCqcColumns.estimate_filled_posts_source: CatValues.estimate_filled_posts_source_column_values.count_of_categorical_values,
        },
    }
