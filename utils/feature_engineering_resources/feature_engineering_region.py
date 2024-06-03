from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsRegion:
    """The possible values of the current_region feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.current_region_column_values.column_name

    labels_dict = {
        "ons_east_midlands": CatValues.current_region_column_values.east_midlands,
        "ons_eastern": CatValues.current_region_column_values.eastern,
        "ons_london": CatValues.current_region_column_values.london,
        "ons_north_east": CatValues.current_region_column_values.north_east,
        "ons_north_west": CatValues.current_region_column_values.north_west,
        "ons_south_east": CatValues.current_region_column_values.south_east,
        "ons_south_west": CatValues.current_region_column_values.south_west,
        "ons_west_midlands": CatValues.current_region_column_values.west_midlands,
        "ons_yorkshire_and_the_humber": CatValues.current_region_column_values.yorkshire_and_the_humber,
    }
