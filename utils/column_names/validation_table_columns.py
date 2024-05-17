from dataclasses import dataclass


@dataclass
class Validation:
    check: str = "check"
    check_level: str = "check_level"
    check_status: str = "check_status"
    constraint: str = "constraint"
    constraint_status: str = "constraint_status"
    constraint_message: str = "constraint_message"
