from dataclasses import dataclass

from utils.cqc_ratings_utils.cqc_ratings_values import (
    CQCRatingsColumns as CQCRatings,
)


@dataclass
class CQCRatingsValueLabelsUnknownCodes:
    """The possible values of cqc ratings which signify they are unknown"""

    overall_column_name: str = CQCRatings.overall_rating
    safe_column_name: str = CQCRatings.safe_rating
    well_led_column_name: str = CQCRatings.well_led_rating
    caring_column_name: str = CQCRatings.caring_rating
    responsive_column_name: str = CQCRatings.responsive_rating
    effective_column_name: str = CQCRatings.effective_rating

    labels_dict = {
        "Inspected but not rated": None,
        "No published rating": None,
        "Insufficient evidence to rate": None,
    }
