from dataclasses import dataclass


@dataclass
class CoverageColumns:
    in_ascwds: str = "in_ascwds"
    in_ascwds_last_month: str = "in_ascwds_last_month"
    in_ascwds_change: str = "in_ascwds_change"
    coverage_monthly_change: str = "coverage_monthly_change"
