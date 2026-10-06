from dataclasses import dataclass


@dataclass
class PublicationDownloadColumns:
    period: str = "period"
    period_label: str = "period_label"
    region: str = "region"
    main_service: str = "main_service"
    estimated_filled_posts: str = "estimated_filled_posts"
    cqc_locations: str = "cqc_locations"
    annual_percentage_change: str = "annual_percentage_change"
    monthly_percentage_change: str = "monthly_percentage_change"
