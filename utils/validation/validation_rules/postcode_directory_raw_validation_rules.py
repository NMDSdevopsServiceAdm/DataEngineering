from dataclasses import dataclass

from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_values.categorical_columns_by_dataset import (
    PostcodeDirectoryRawCategoricalValues as CatValues,
)
from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class PostcodeDirectoryRawValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            ONS.postcode,
            Keys.import_date,
            ONS.cssr,
            ONS.region,
            ONS.rural_urban_indicator_2011,
        ],
        RuleName.index_columns: [
            ONS.postcode,
            Keys.import_date,
        ],
        RuleName.categorical_values_in_columns: {
            ONS.cssr: CatValues.cssr_column_values.categorical_values,
            ONS.region: CatValues.region_column_values.categorical_values,
            ONS.rural_urban_indicator_2011: CatValues.rui_column_values.categorical_values,
        },
        RuleName.distinct_values: {
            ONS.cssr: CatValues.cssr_column_values.count_of_categorical_values,
            ONS.region: CatValues.region_column_values.count_of_categorical_values,
            ONS.rural_urban_indicator_2011: CatValues.rui_column_values.count_of_categorical_values,
        },
    }
