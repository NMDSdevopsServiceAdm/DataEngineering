from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC
from utils.column_values.categorical_column_values import (
    CareHome,
    PrimaryServiceType,
    PrimaryServiceTypeSecondLevel,
    Services,
)
from utils.validation.validation_rule_names import CustomTypeArguments


@dataclass
class CustomValidationRules:
    care_home_and_primary_service_type = {
        CustomTypeArguments.column_condition: f"({IndCQC.care_home} = '{CareHome.not_care_home}' AND {IndCQC.primary_service_type} = '{PrimaryServiceType.non_residential}') OR ({IndCQC.care_home} = '{CareHome.care_home}' AND {IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_with_nursing}') OR ({IndCQC.care_home} = '{CareHome.care_home}' AND {IndCQC.primary_service_type} = '{PrimaryServiceType.care_home_only}')",
        CustomTypeArguments.constraint_name: "care_home_and_primary_service_type",
        CustomTypeArguments.hint: "The data in carehome and primary_service_type should be related.",
    }

    primary_service_type_second_level_shared_lives = {
        CustomTypeArguments.column_condition: f"(NOT {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.shared_lives}' OR ({IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.shared_lives}' AND array_contains({IndCQC.services_offered}, '{Services.shared_lives}')))",
        CustomTypeArguments.constraint_name: "primary_service_type_second_level_shared_lives",
        CustomTypeArguments.hint: "When services_offered contains 'Shared Lives' then primary_service_type_second_level must equal 'Shared Lives'",
    }

    primary_service_type_second_level_care_home_with_nursing = {
        CustomTypeArguments.column_condition: f"(NOT {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.care_home_with_nursing}' OR ({IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.care_home_with_nursing}' AND array_contains({IndCQC.services_offered}, '{Services.care_home_service_with_nursing}') AND NOT array_contains({IndCQC.services_offered}, '{Services.shared_lives}')))",
        CustomTypeArguments.constraint_name: "primary_service_type_second_level_care_home_with_nursing",
        CustomTypeArguments.hint: "When services_offered contains 'Care home service with nursing', but not 'Shared Lives' then primary_service_type_second_level must equal 'Care home service with nursing'",
    }

    primary_service_type_second_level_care_home_without_nursing = {
        CustomTypeArguments.column_condition: f"(NOT {IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.care_home_only}' OR ({IndCQC.primary_service_type_second_level} = '{PrimaryServiceTypeSecondLevel.care_home_only}' AND array_contains({IndCQC.services_offered}, '{Services.care_home_service_without_nursing}') AND NOT (array_contains({IndCQC.services_offered}, '{Services.shared_lives}') OR array_contains({IndCQC.services_offered}, '{Services.care_home_service_with_nursing}'))))",
        CustomTypeArguments.constraint_name: "primary_service_type_second_level_care_home_without_nursing",
        CustomTypeArguments.hint: "When services_offered contains 'Care home service without nursing', but not 'Shared Lives' or 'Care home service with nursing' then primary_service_type_second_level must equal 'Care home service without nursing'",
    }
