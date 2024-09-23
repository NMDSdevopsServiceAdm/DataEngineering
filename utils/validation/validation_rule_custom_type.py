from dataclasses import dataclass

from utils.validation.validation_rule_names import CustomTypeArguments


@dataclass
class CustomValidationRules:
    care_home_and_primary_service_type = {
        CustomTypeArguments.column_condition: "(carehome = 'N' AND primary_service_type = 'non-residential') OR (carehome = 'Y' AND primary_service_type = 'Care home with nursing') OR (carehome = 'Y' AND primary_service_type = 'Care home without nursing')",
        CustomTypeArguments.constraint_name: "care_home_and_primary_service_type",
        CustomTypeArguments.hint: "The data in carehome and primary_service_type should be related.",
    }
