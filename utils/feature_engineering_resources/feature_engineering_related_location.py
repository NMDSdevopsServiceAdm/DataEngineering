from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsRelatedLocation:
    """The possible values of the related_location feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.related_location_column_values.column_name

    labels_dict = {
        "no_related_location": CatValues.related_location_column_values.no_related_location,
    }
