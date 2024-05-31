from dataclasses import dataclass

from utils.value_lists.cqc_locations_value_lists import Services


@dataclass
class FeatureEngineeringValueLabelsServices:
    """The possible values of the services feature in the independent CQC estimates pipeline"""

    column_name: str = Services.column_name

    labels_dict = {
        "service_1": Services.care_home_service_with_nursing,
        "service_2": Services.care_home_service_without_nursing,
        "service_3": Services.community_based_services_for_people_who_misuse_substances,
        "service_4": Services.hospice_services,
        "service_5": Services.domiciliary_care_service,
        "service_6": Services.remote_clinical_advice_service,
        "service_7": Services.acute_services_without_overnight_beds,
        "service_8": Services.specialist_college_service,
        "service_9": Services.ambulance_service,
        "service_10": Services.extra_care_housing_services,
        "service_11": Services.urgent_care_services,
        "service_12": Services.supported_living_service,
        "service_13": Services.prison_healthcare_services,
        "service_14": Services.community_based_services_for_people_with_mental_health_needs,
        "service_15": Services.community_healthcare_service,
        "service_16": Services.community_based_services_for_people_with_a_learning_disability,
        "service_17": Services.community_health_care_services_nurses_agency_only,
        "service_18": Services.dental_service,
        "service_19": Services.mobile_doctors_service,
        "service_20": Services.long_term_conditions_services,
        "service_21": Services.doctors_consultation_service,
        "service_22": Services.shared_lives,
        "service_23": Services.acute_services_with_overnight_beds,
        "service_24": Services.diagnostic_and_screening_service,
        "service_25": Services.residential_substance_misuse_treatment_and_rehabilitation_service,
        "service_26": Services.rehabilitation_services,
        "service_27": Services.doctors_treatment_service,
        "service_28": Services.hospice_services_at_home,
        "service_29": Services.hospital_services_for_people_with_mental_health_needs,
    }
