from dataclasses import dataclass

from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_values.categorical_columns_by_dataset import (
    LocationApiRawCategoricalValues as CatValues,
)

from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class LocationsAPIRawValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            CQCL.location_id,
            Keys.import_date,
            CQCL.care_home,
            CQCL.registration_status,
            CQCL.name,
        ],
        RuleName.index_columns: [
            CQCL.location_id,
            Keys.import_date,
        ],
        RuleName.categorical_values_in_columns: {
            CQCL.care_home: CatValues.care_home_column_values.categorical_values,
            CQCL.registration_status: CatValues.registration_status_column_values.categorical_values,
            CQCL.dormancy: CatValues.dormancy_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            CQCL.care_home: CatValues.care_home_column_values.count_of_categorical_values,
            CQCL.registration_status: CatValues.registration_status_column_values.count_of_categorical_values,
            CQCL.dormancy: CatValues.dormancy_column_values.count_of_categorical_values,
        },
    }
