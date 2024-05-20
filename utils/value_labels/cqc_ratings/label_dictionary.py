from utils.value_labels.cqc_ratings.cqc_ratings import (
    CQCRatingsValueLabelsUnknownCodes as CQCRatings,
)


unknown_ratings_labels_dict = {
    CQCRatings.overall_column_name: CQCRatings.labels_dict,
    CQCRatings.safe_column_name: CQCRatings.labels_dict,
    CQCRatings.well_led_column_name: CQCRatings.labels_dict,
    CQCRatings.caring_column_name: CQCRatings.labels_dict,
    CQCRatings.responsive_column_name: CQCRatings.labels_dict,
    CQCRatings.effective_column_name: CQCRatings.labels_dict,
}
