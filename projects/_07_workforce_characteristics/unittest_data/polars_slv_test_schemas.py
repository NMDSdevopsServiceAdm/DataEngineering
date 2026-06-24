from dataclasses import dataclass

import polars as pl

from utils.column_names.slv_columns import StartersLeaversVacanciesColumns as SLV


@dataclass
class SLVMergeTestSchemas:
    test = {SLV.location_id: pl.String}
