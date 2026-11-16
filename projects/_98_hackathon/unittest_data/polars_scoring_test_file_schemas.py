from dataclasses import dataclass

import polars as pl

from utils.column_names.cleaned_data_files.ascwds_worker_cleaned import (
    AscwdsWorkerCleanedColumns as AWKClean,
)
from utils.column_names.hackathon_columns import HackathonColumns as HackCols


@dataclass
class ScoringSchemas:
    answers_schema = pl.Schema(
        [
            (AWKClean.establishment_id, pl.String()),
            (AWKClean.worker_id, pl.String()),
            (HackCols.is_leaver, pl.Boolean()),
            (HackCols.valid_for_analysis, pl.Boolean()),
        ]
    )

    predictions_schema = pl.Schema(
        [
            (AWKClean.establishment_id, pl.String()),
            (AWKClean.worker_id, pl.String()),
            (HackCols.predicted_probability, pl.Float64()),
        ]
    )

    predictions_without_probability_schema = pl.Schema(
        [
            (AWKClean.establishment_id, pl.String()),
            (AWKClean.worker_id, pl.String()),
        ]
    )

    predictions_with_extra_column_schema = pl.Schema(
        [
            (AWKClean.establishment_id, pl.String()),
            (AWKClean.worker_id, pl.String()),
            (HackCols.predicted_probability, pl.Float64()),
            ("extra", pl.Int64()),
        ]
    )
