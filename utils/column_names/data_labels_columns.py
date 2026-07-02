from dataclasses import dataclass


@dataclass
class DataLabelsColumns:
    column_name: str = "column_name"
    code: str = "code"
    label: str = "label"
