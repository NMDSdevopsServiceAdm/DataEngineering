from dataclasses import dataclass


@dataclass
class CQCRatingsColumns:
    date: str = "rating_date"
    overall_rating: str = "Overall_rating"
    safe_rating: str = "Safe_rating"
    well_led_rating: str = "Well-led_rating"
    caring_rating: str = "Caring_rating"
    responsive_rating: str = "Responsive_rating"
    effective_rating: str = "Effective_rating"
    total_rating_value: str = "Total_rating_value"
    safe_rating_value: str = "Safe_rating_value"
    well_led_rating_value: str = "Well-led_rating_value"
    caring_rating_value: str = "Caring_rating_value"
    responsive_rating_value: str = "Responsive_rating_value"
    effective_rating_value: str = "Effective_rating_value"
    current_or_historic: str = "Current_or_historic"
    rating_sequence: str = "Rating_sequence"
    reversed_rating_sequence: str = "Reversed_rating_sequence"
    latest_rating_flag: str = "Latest_rating_flag"
    good_or_outstanding_flag: str = "flag_good_or_outstanding_current_overall_rating"
    inspection_date: str = "inspection_date"
    benchmarks_location_id: str = "cqc_location_id"
    benchmarks_establishment_id: str = "ascwds_establishment_id"
    benchmarks_overall_rating: str = "overall_rating"
    location_id_hash: str = "Identifier"
