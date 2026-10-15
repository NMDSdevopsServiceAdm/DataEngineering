from dataclasses import dataclass

import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import (
    ExtrapolationColumns as ExtrapCol,
)
from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns as IndCQC


class InterpolationSchema:
    interpolation_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.ascwds_pir_merged: pl.Float64,
        IndCQC.extrapolation_forwards: pl.Float64,
        IndCQC.interpolation_model: pl.Float32,
    }

    calculate_residual_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.ascwds_pir_merged: pl.Float64,
        IndCQC.extrapolation_forwards: pl.Float64,
        IndCQC.residual: pl.Float64,
    }

    days_between_submissions_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.ascwds_pir_merged: pl.Float64,
        IndCQC.days_between_submissions: pl.Int64,
        IndCQC.proportion_of_days_between_submissions: pl.Float64,
    }

    calculate_interpolated_values_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.ascwds_pir_merged: pl.Float64,
        IndCQC.previous_non_null_value: pl.Float64,
        IndCQC.residual: pl.Float64,
        IndCQC.days_between_submissions: pl.Int64,
        IndCQC.proportion_of_days_between_submissions: pl.Float64,
        IndCQC.interpolation_model: pl.Float32,
    }


@dataclass
class ModelExtrapolation:
    model_extrapolation_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.ascwds_pir_merged: pl.Float32,
        IndCQC.posts_rolling_average_model: pl.Float32,
        IndCQC.extrapolation_forwards: pl.Float32,
        IndCQC.extrapolation_model: pl.Float32,
    }

    expected_extrapolation_aggregates_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.ascwds_pir_merged: pl.Float32,
        IndCQC.posts_rolling_average_model: pl.Float32,
        ExtrapCol.first_submission_time: pl.Date,
        ExtrapCol.final_submission_time: pl.Date,
        ExtrapCol.first_value: pl.Float32,
        ExtrapCol.first_model: pl.Float32,
    }

    get_previous_value_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.ascwds_pir_merged: pl.Float32,
        ExtrapCol.previous_value: pl.Float32,
    }


@dataclass
class ModelImputation:
    expected_model_imputation_schema = {
        IndCQC.location_id: pl.String,
        IndCQC.cqc_location_import_date: pl.Date,
        IndCQC.care_home: pl.String,
        "null_values": pl.Float32,
        "trend_model": pl.Float32,
        "imputed_values": pl.Float32,
    }
