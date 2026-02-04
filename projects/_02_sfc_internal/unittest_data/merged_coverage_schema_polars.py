import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AscWdsColumns,
)
from utils.column_names.coverage_columns import CoverageColumns
from utils.column_names.cqc_ratings_columns import CQCRatingsColumns
from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
    PartitionKeys as Keys,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.reconciliation_columns import ReconciliationColumns


class ValidateMonthlyCoverageData:
    cqc_locations_schema = {
        CQCLClean.cqc_location_import_date: pl.Date,
        CQCLClean.location_id: pl.String,
        CQCLClean.provider_id: pl.String,
        CQCLClean.name: pl.String,
        CQCLClean.postal_code: pl.String,
        CQCLClean.care_home: pl.String,
        CQCLClean.number_of_beds: pl.Int32,
        Keys.year: pl.String,
        Keys.month: pl.String,
        Keys.day: pl.String,
    }

    merged_coverage_schema = {
        IndCqcColumns.location_id: pl.String,
        IndCqcColumns.name: pl.String,
        IndCqcColumns.cqc_location_import_date: pl.Date,
        IndCqcColumns.care_home: pl.String,
        IndCqcColumns.provider_id: pl.String,
        IndCqcColumns.cqc_sector: pl.String,
        IndCqcColumns.imputed_registration_date: pl.Date,
        IndCqcColumns.primary_service_type: pl.String,
        IndCqcColumns.current_ons_import_date: pl.Date,
        IndCqcColumns.postcode: pl.String,
        IndCqcColumns.current_cssr: pl.String,
        IndCqcColumns.current_region: pl.String,
        IndCqcColumns.current_rural_urban_indicator_2011: pl.String,
        CoverageColumns.in_ascwds: pl.Int32,
        CoverageColumns.la_monthly_coverage: pl.Float64,
        CoverageColumns.locations_monthly_change: pl.Float64,
        CoverageColumns.new_registrations_monthly: pl.Int32,
        CoverageColumns.new_registrations_ytd: pl.Int32,
        AscWdsColumns.master_update_date: pl.Date,
        AscWdsColumns.nmds_id: pl.String,
        IndCqcColumns.dormancy: pl.String,
        CQCRatingsColumns.overall_rating: pl.String,
        IndCqcColumns.provider_name: pl.String,
        ReconciliationColumns.parents_or_singles_and_subs: pl.String,
        CoverageColumns.coverage_monthly_change: pl.Float64,
        AscWdsColumns.last_logged_in_date: pl.Date,
        Keys.year: pl.String,
        Keys.month: pl.String,
        Keys.day: pl.String,
    }
