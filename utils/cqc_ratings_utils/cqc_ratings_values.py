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
    current_or_historic: str = "Current_or_historic"
    rating_sequence: str = "Rating_sequence"
    reversed_rating_sequence: str = "Reversed_rating_sequence"
    latest_rating_flag: str = "Latest_rating_flag"


@dataclass
class CQCRatingsValues:
    current: str = "Current"
    historic: str = "Historic"
