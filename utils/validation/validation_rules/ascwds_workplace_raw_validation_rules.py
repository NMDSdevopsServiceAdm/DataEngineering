from dataclasses import dataclass

from utils.column_names.raw_data_files.ascwds_workplace_columns import (
    AscwdsWorkplaceColumns as AWP,
    PartitionKeys as Keys,
)

from utils.validation.validation_rule_names import RuleNames as RuleName


@dataclass
class ASCWDSWorkplaceCleanedValidationRules:
    rules_to_check = {
        RuleName.complete_columns: [
            AWP.establishment_id,
            Keys.import_date,
        ],
        RuleName.index_columns: [
            AWP.establishment_id,
            Keys.import_date,
        ],
    }
