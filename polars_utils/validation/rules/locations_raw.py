from enum import Enum

from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)


class Rules(tuple, Enum):
    complete_columns = [CQCL.location_id, Keys.import_date, CQCL.name]
    index_columns = [CQCL.location_id, Keys.import_date]
