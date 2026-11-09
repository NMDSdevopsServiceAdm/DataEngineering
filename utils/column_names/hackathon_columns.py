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
    nationality_group: str = "nationality_group"
    country_of_birth_group: str = "country_of_birth_group"
