from dataclasses import dataclass


@dataclass
class CQCRatingsColumns:
    date: str = "Date"
    overall_rating: str = "Overall_rating"
    safe_rating: str = "Safe_rating"
    well_led_rating: str = "Well-led_rating"
    caring_rating: str = "Caring_rating"
    responsive_rating: str = "Responsive_rating"
    effective_rating: str = "Effective_rating"
