from dataclasses import dataclass

from utils.column_values.ons_postcode_directory_values import CurrentRegion


@dataclass
class FeatureEngineeringValueLabelsRegion:
    """The possible values of the current_region feature in the independent CQC estimates pipeline"""

    column_name: str = CurrentRegion.column_name

    labels_dict = {
        "ons_east_midlands": CurrentRegion.east_midlands,
        "ons_eastern": CurrentRegion.eastern,
        "ons_london": CurrentRegion.london,
        "ons_north_east": CurrentRegion.north_east,
        "ons_north_west": CurrentRegion.north_west,
        "ons_south_east": CurrentRegion.south_east,
        "ons_south_west": CurrentRegion.south_west,
        "ons_west_midlands": CurrentRegion.west_midlands,
        "ons_yorkshire_and_the_humber": CurrentRegion.yorkshire_and_the_humber,
    }
