from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsServices:
    """The possible values of the services feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.services_column_values.column_name

    labels_dict = {
        "service_1": CatValues.services_column_values.care_home_service_with_nursing,
        "service_2": CatValues.services_column_values.care_home_service_without_nursing,
        "service_3": CatValues.services_column_values.community_based_services_for_people_who_misuse_substances,
        "service_4": CatValues.services_column_values.hospice_services,
        "service_5": CatValues.services_column_values.domiciliary_care_service,
        "service_6": CatValues.services_column_values.remote_clinical_advice_service,
        "service_7": CatValues.services_column_values.acute_services_without_overnight_beds,
        "service_8": CatValues.services_column_values.specialist_college_service,
        "service_9": CatValues.services_column_values.ambulance_service,
        "service_10": CatValues.services_column_values.extra_care_housing_services,
        "service_11": CatValues.services_column_values.urgent_care_services,
        "service_12": CatValues.services_column_values.supported_living_service,
        "service_13": CatValues.services_column_values.prison_healthcare_services,
        "service_14": CatValues.services_column_values.community_based_services_for_people_with_mental_health_needs,
        "service_15": CatValues.services_column_values.community_healthcare_service,
        "service_16": CatValues.services_column_values.community_based_services_for_people_with_a_learning_disability,
        "service_17": CatValues.services_column_values.community_health_care_services_nurses_agency_only,
        "service_18": CatValues.services_column_values.dental_service,
        "service_19": CatValues.services_column_values.mobile_doctors_service,
        "service_20": CatValues.services_column_values.long_term_conditions_services,
        "service_21": CatValues.services_column_values.doctors_consultation_service,
        "service_22": CatValues.services_column_values.shared_lives,
        "service_23": CatValues.services_column_values.acute_services_with_overnight_beds,
        "service_24": CatValues.services_column_values.diagnostic_and_screening_service,
        "service_25": CatValues.services_column_values.residential_substance_misuse_treatment_and_rehabilitation_service,
        "service_26": CatValues.services_column_values.rehabilitation_services,
        "service_27": CatValues.services_column_values.doctors_treatment_service,
        "service_28": CatValues.services_column_values.hospice_services_at_home,
        "service_29": CatValues.services_column_values.hospital_services_for_people_with_mental_health_needs,
    }
