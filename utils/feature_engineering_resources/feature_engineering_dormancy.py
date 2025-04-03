from dataclasses import dataclass

from utils.column_values.categorical_columns_by_dataset import (
    FeatureEngineeringCategoricalValues as CatValues,
)


@dataclass
class FeatureEngineeringValueLabelsDormancy:
    """The possible values of the dormancy feature in the independent CQC estimates pipeline"""

    column_name: str = CatValues.dormancy_column_values.column_name

    labels_dict = {
        "not_dormant": CatValues.dormancy_column_values.not_dormant,
    }
