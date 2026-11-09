from dataclasses import dataclass


@dataclass
class HackathonColumns:
    import_month: str = "import_month"
    end_month: str = "end_month"
    establishment_in_both_snapshots: str = "establishment_in_both_snapshots"
    is_leaver: str = "is_leaver"
    latest_save_date: str = "latest_save_date"
    months_since_save_start: str = "months_since_save_start"
    months_since_save_end: str = "months_since_save_end"
    establishment_active: str = "establishment_active"
    main_service_type: str = "main_service_type"

    gender_labelled: str = "gender_labelled"
    ethnicity_labelled: str = "ethnicity_labelled"
    nationality_labelled: str = "nationality_labelled"
    country_of_birth_labelled: str = "country_of_birth_labelled"
    disabled_labelled: str = "disabled_labelled"
    hourly_or_annual_pay_labelled: str = "hourly_or_annual_pay_labelled"
    social_care_qual_held_labelled: str = "social_care_qual_held_labelled"
    level_social_care_qualification_held_labelled: str = (
        "level_social_care_qualification_held_labelled"
    )
    care_certificate_status_labelled: str = "care_certificate_status_labelled"
    apprentice_labelled: str = "apprentice_labelled"
    zero_hours_labelled: str = "zero_hours_labelled"
    source_of_recruitment_labelled: str = "source_of_recruitment_labelled"
