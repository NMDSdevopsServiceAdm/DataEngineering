from dataclasses import dataclass


@dataclass
class CoverageColumns:
    in_ascwds: str = "in_ascwds"
    in_ascwds_last_month: str = "in_ascwds_last_month"
    in_ascwds_change: str = "in_ascwds_change"
    la_monthly_locations_count: str = "la_monthly_locations_count"
    la_monthly_locations_in_ascwds_count: str = "la_monthly_locations_in_ascwds_count"
    la_monthly_coverage: str = "la_monthly_coverage"
    la_monthly_coverage_last_month: str = "la_monthly_coverage_last_month"
    coverage_monthly_change: str = "coverage_monthly_change"
    locations_monthly_change: str = "locations_monthly_change"
    new_registration: str = "new_registration"
    new_registrations_monthly: str = "new_registrations_monthly"
    new_registrations_ytd: str = "new_registrations_ytd"
