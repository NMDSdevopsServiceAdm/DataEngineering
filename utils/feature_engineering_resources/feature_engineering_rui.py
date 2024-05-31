from dataclasses import dataclass

from utils.column_values.ons_postcode_directory_values import CurrentRUI


@dataclass
class FeatureEngineeringValueLabelsRUI:
    """The possible values of the current rui feature in the independent CQC estimates pipeline"""

    column_name: str = CurrentRUI.column_name

    labels_dict = {
        "indicator_1": CurrentRUI.rural_hamlet_sparse,
        "indicator_2": CurrentRUI.rural_hamlet,
        "indicator_3": CurrentRUI.rural_village,
        "indicator_4": CurrentRUI.rural_town_sparse,
        "indicator_5": CurrentRUI.rural_town,
        "indicator_6": CurrentRUI.urban_city_sparse,
        "indicator_7": CurrentRUI.urban_city,
        "indicator_8": CurrentRUI.urban_major,
        "indicator_9": CurrentRUI.urban_minor,
        "indicator_10": CurrentRUI.rural_village_sparse,
    }
