from dataclasses import dataclass

@dataclass
class CqcPirColumns:
    location_id:str = "Location_ID"
    location_name:str = "Location_name"
    pir_type:str = "PIR_type"
    pir_submission_date:str = "PIR_submission_date"
    people_directly_employed:str = "How_many_people_are_directly_employed_and_deliver_regulated_activities_at_your_service_as_part_of_their_daily_duties"
    staff_leavers:str = "How_many_staff_have_left_your_service_in_the_past_12_months"
    staff_vacancies:str = "How_many_staff_vacancies_do_you_have"
    shared_lives_leavers:str ="How_many_Shared_Lives_workers_have_left_your_service_in_the_past_12_months"
    shared_lives_vacancies:str = "How_many_Shared_Lives_worker_vacancies_do_you_have"
    primary_inspection_category:str = "Location_primary_inspection_category"
    region:str = "Location_region"
    local_authority:str ="Location_local_authority"
    number_of_beds:str = "Location_beds"
    domiciliary_care:str = "Service_type_Domiciliary_care_service"
    location_status:str = "Location_status"

