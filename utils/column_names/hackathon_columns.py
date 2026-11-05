from dataclasses import dataclass


@dataclass
class HackathonColumns:
    import_month: str = "import_month"
    end_month: str = "end_month"
    is_leaver: str = "is_leaver"
    latest_save_date: str = "latest_save_date"
    months_since_save_start: str = "months_since_save_start"
    months_since_save_end: str = "months_since_save_end"
    establishment_active: str = "establishment_active"
