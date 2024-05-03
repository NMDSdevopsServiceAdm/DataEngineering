from dataclasses import dataclass

from utils.column_names.ind_cqc_pipeline_columns import (
    IndCqcColumns,
)


@dataclass
class MergedIndCqcValidationRules:
    complete_columns = [
        IndCqcColumns.location_id,
        IndCqcColumns.ascwds_workplace_import_date,
        IndCqcColumns.cqc_location_import_date,
        IndCqcColumns.cqc_pir_import_date,
        IndCqcColumns.care_home,
        IndCqcColumns.provider_id,
        IndCqcColumns.cqc_sector,
        IndCqcColumns.registration_status,
        IndCqcColumns.registration_date,
        IndCqcColumns.dormancy,
        IndCqcColumns.number_of_beds,
        IndCqcColumns.primary_service_type,
        IndCqcColumns.contemporary_ons_import_date,
        IndCqcColumns.contemporary_cssr,
        IndCqcColumns.contemporary_region,
        IndCqcColumns.current_ons_import_date,
        IndCqcColumns.current_cssr,
        IndCqcColumns.current_region,
        IndCqcColumns.current_rural_urban_indicator_2011,
        IndCqcColumns.people_directly_employed,
        IndCqcColumns.establishment_id,
        IndCqcColumns.organisation_id,
    ]

    index_columns = [IndCqcColumns.location_id, IndCqcColumns.cqc_location_import_date]
