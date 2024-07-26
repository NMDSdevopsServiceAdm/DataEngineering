from dataclasses import dataclass


@dataclass
class NullOutlierColumns:
    filled_posts_per_bed_ratio: str = "filled_posts_per_bed_ratio"
    avg_filled_posts_per_bed_ratio: str = "avg_filled_posts_per_bed_ratio"
    number_of_beds_banded: str = "number_of_beds_banded"
    residual: str = "residual"
    standardised_residual: str = "standardised_residual"
    expected_filled_posts: str = "expected_filled_posts"
    lower_percentile: str = "lower_percentile"
    upper_percentile: str = "upper_percentile"
