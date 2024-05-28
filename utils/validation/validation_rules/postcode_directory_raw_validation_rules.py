from dataclasses import dataclass

from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)

from utils.validation.validation_rule_names import RuleNames as RuleName
from utils.validation.categorical_column_values import (
    ONSCategoricalValues,
    ONSDistinctValues,
)


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
            ONS.cssr: ONSCategoricalValues.cssrs,
            ONS.region: ONSCategoricalValues.regions,
            ONS.rural_urban_indicator_2011: ONSCategoricalValues.rural_urban_indicators,
        },
        RuleName.distinct_values: {
            ONS.cssr: ONSDistinctValues.cssrs,
            ONS.region: ONSDistinctValues.regions,
            ONS.rural_urban_indicator_2011: ONSDistinctValues.rural_urban_indicators,
        },
    }
