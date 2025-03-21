from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsRegion:
    """The possible values of the current_region feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.current_region_column_values.column_name

    care_home_labels_dict = {
        "region_east_midlands": CatValues.current_region_column_values.east_midlands,
        "region_eastern": CatValues.current_region_column_values.eastern,
        "region_london": CatValues.current_region_column_values.london,
        "region_north_east": CatValues.current_region_column_values.north_east,
        "region_north_west": CatValues.current_region_column_values.north_west,
        "region_south_east": CatValues.current_region_column_values.south_east,
        "region_south_west": CatValues.current_region_column_values.south_west,
        "region_west_midlands": CatValues.current_region_column_values.west_midlands,
        "region_yorkshire_and_the_humber": CatValues.current_region_column_values.yorkshire_and_the_humber,
    }

    non_res_model_labels_dict = {
        "region_east_midlands": CatValues.current_region_column_values.east_midlands,
        "region_eastern": CatValues.current_region_column_values.eastern,
        "region_london": CatValues.current_region_column_values.london,
        "region_north_east": CatValues.current_region_column_values.north_east,
        "region_north_west": CatValues.current_region_column_values.north_west,
        "region_south_east": CatValues.current_region_column_values.south_east,
        "region_south_west": CatValues.current_region_column_values.south_west,
        "region_west_midlands": CatValues.current_region_column_values.west_midlands,
        # "region_yorkshire_and_the_humber": CatValues.current_region_column_values.yorkshire_and_the_humber,
    }
