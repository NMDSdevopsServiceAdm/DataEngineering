from dataclasses import dataclass


@dataclass
class CoverageColumns:
    in_ascwds: str = "in_ascwds"
    in_ascwds_last_month: str = "in_ascwds_last_month"
    in_ascwds_change: str = "in_ascwds_change"
    la_monthly_coverage: str = "la_monthly_coverage"
    coverage_monthly_change: str = "coverage_monthly_change"
    locations_monthly_change: str = "locations_monthly_change"
    new_registrations_monthly: str = "new_registrations_monthly"
    new_registrations_ytd: str = "new_registrations_ytd"
