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
    hours: str = "hours"
    hourly_pay: str = "hourly_pay"
    nlw_hourly_rate: str = "nlw_hourly_rate"
    hourly_pay_minus_nlw: str = "hourly_pay_minus_nlw"
    nlw_effective_date: str = "nlw_effective_date"
    nlw_is_apprentice: str = "nlw_is_apprentice"
    nlw_band_start: str = "nlw_band_start"
