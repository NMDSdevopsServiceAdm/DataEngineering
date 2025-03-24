from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsServices:
    """The possible values of the services feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.services_column_values.column_name

    care_home_labels_dict = {
        "service_care_home_with_nursing": CatValues.services_column_values.care_home_service_with_nursing,
        "service_care_home_without_nursing": CatValues.services_column_values.care_home_service_without_nursing,
        "service_domiciliary": CatValues.services_column_values.domiciliary_care_service,
        "service_extra_care_housing": CatValues.services_column_values.extra_care_housing_services,
        "service_shared_lives": CatValues.services_column_values.shared_lives,
        "service_specialist_college": CatValues.services_column_values.specialist_college_service,
        "service_supported_living": CatValues.services_column_values.supported_living_service,
    }

    non_res_model_labels_dict = {
        "service_domiciliary": CatValues.services_column_values.domiciliary_care_service,
        "service_extra_care_housing": CatValues.services_column_values.extra_care_housing_services,
        "service_shared_lives": CatValues.services_column_values.shared_lives,
        "service_supported_living": CatValues.services_column_values.supported_living_service,
    }
