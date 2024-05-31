from dataclasses import dataclass

from utils.value_lists.cqc_locations_value_lists import Dormancy


@dataclass
class FeatureEngineeringValueLabelsDormancy:
    """The possible values of the dormancy feature in the independent CQC estimates pipeline"""

    column_name: str = Dormancy.column_name

    labels_dict = {
        "dorm_N": Dormancy.not_dormant,
        "dorm_Y": Dormancy.dormant,
    }
