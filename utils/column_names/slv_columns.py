from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


@dataclass
class StartersLeaversVacanciesColumns:
    location_id = IndCQC.location_id
