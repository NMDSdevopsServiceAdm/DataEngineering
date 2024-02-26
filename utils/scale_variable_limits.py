from dataclasses import dataclass


@dataclass
class GeneralLimits:
    non_zero_lower_bound: int = 1


@dataclass
class AscwdsScaleVariableLimits(GeneralLimits):
    total_staff_lower_limit = GeneralLimits.non_zero_lower_bound
    worker_records_lower_limit = GeneralLimits.non_zero_lower_bound
