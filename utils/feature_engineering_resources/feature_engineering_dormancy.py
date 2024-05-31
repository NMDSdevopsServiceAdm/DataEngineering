from dataclasses import dataclass

from utils.column_values.cqc_locations_values import Dormancy


@dataclass
class FeatureEngineeringValueLabelsDormancy:
    """The possible values of the dormancy feature in the independent CQC estimates pipeline"""

    column_name: str = Dormancy.column_name

    labels_dict = {
        "dorm_N": Dormancy.not_dormant,
        "dorm_Y": Dormancy.dormant,
    }
