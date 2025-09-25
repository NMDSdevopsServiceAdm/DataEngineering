from dataclasses import dataclass

import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)


@dataclass
class CQCLocationsSchema:
    clean_provider_id_column_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCL.provider_id, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    clean_registration_date_column_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCL.registration_date, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    clean_registration_date_column_output_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCL.registration_date, pl.String()),
            (Keys.import_date, pl.String()),
            (CQCLClean.imputed_registration_date, pl.String()),
        ]
    )

    impute_historic_relationships_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.registration_status, pl.String()),
            (
                CQCL.relationships,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.related_location_id: pl.String(),
                            CQCL.related_location_name: pl.String(),
                            CQCL.type: pl.String(),
                            CQCL.reason: pl.String(),
                        }
                    )
                ),
            ),
        ]
    )

    expected_impute_historic_relationships_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.registration_status, pl.String()),
            (
                CQCL.relationships,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.related_location_id: pl.String(),
                            CQCL.related_location_name: pl.String(),
                            CQCL.type: pl.String(),
                            CQCL.reason: pl.String(),
                        }
                    )
                ),
            ),
            (
                CQCLClean.imputed_relationships,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.related_location_id: pl.String(),
                            CQCL.related_location_name: pl.String(),
                            CQCL.type: pl.String(),
                            CQCL.reason: pl.String(),
                        }
                    )
                ),
            ),
        ]
    )

    get_predecessor_relationships_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.registration_status, pl.String()),
            (
                CQCLClean.first_known_relationships,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.related_location_id: pl.String(),
                            CQCL.related_location_name: pl.String(),
                            CQCL.type: pl.String(),
                            CQCL.reason: pl.String(),
                        }
                    ),
                ),
            ),
        ]
    )

    expected_get_predecessor_relationships_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.registration_status, pl.String()),
            (
                CQCLClean.first_known_relationships,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.related_location_id: pl.String(),
                            CQCL.related_location_name: pl.String(),
                            CQCL.type: pl.String(),
                            CQCL.reason: pl.String(),
                        }
                    ),
                ),
            ),
            (
                CQCLClean.relationships_predecessors_only,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.related_location_id: pl.String(),
                            CQCL.related_location_name: pl.String(),
                            CQCL.type: pl.String(),
                            CQCL.reason: pl.String(),
                        }
                    ),
                ),
            ),
        ]
    )

    impute_struct_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (
                CQCLClean.gac_service_types,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.name: pl.String(),
                            CQCL.description: pl.String(),
                        }
                    )
                ),
            ),
        ]
    )

    expected_impute_struct_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (
                CQCLClean.gac_service_types,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.name: pl.String(),
                            CQCL.description: pl.String(),
                        }
                    )
                ),
            ),
            (
                CQCLClean.imputed_gac_service_types,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.name: pl.String(),
                            CQCL.description: pl.String(),
                        }
                    )
                ),
            ),
        ]
    )

    allocate_primary_service_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (
                CQCLClean.imputed_gac_service_types,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.name: pl.String(),
                            CQCL.description: pl.String(),
                        }
                    )
                ),
            ),
        ]
    )

    expected_allocate_primary_service_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (
                CQCLClean.imputed_gac_service_types,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.name: pl.String(),
                            CQCL.description: pl.String(),
                        }
                    )
                ),
            ),
            (CQCLClean.primary_service_type, pl.String()),
        ]
    )

    align_care_home_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.primary_service_type, pl.String()),
        ]
    )

    expected_align_care_home_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.primary_service_type, pl.String()),
            (CQCLClean.care_home, pl.String()),
        ]
    )

    related_location_flag_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (
                CQCLClean.imputed_relationships,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.related_location_id: pl.String(),
                            CQCL.related_location_name: pl.String(),
                            CQCL.type: pl.String(),
                            CQCL.reason: pl.String(),
                        }
                    ),
                ),
            ),
        ]
    )

    expected_related_location_flag_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (
                CQCLClean.imputed_relationships,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.related_location_id: pl.String(),
                            CQCL.related_location_name: pl.String(),
                            CQCL.type: pl.String(),
                            CQCL.reason: pl.String(),
                        }
                    ),
                ),
            ),
            (CQCLClean.related_location, pl.String()),
        ]
    )

    remove_specialist_colleges_fact_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )
    remove_specialist_colleges_dim_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (Keys.import_date, pl.String()),
            (CQCLClean.services_offered, pl.List(pl.String())),
        ]
    )

    expected_remove_specialist_colleges_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    assign_cqc_sector_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCL.provider_id, pl.String()),
        ]
    )

    expected_assign_cqc_sector_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCL.provider_id, pl.String()),
            (CQCLClean.cqc_sector, pl.String()),
        ]
    )


@dataclass
class ExtractRegisteredManagerNamesSchema:
    contact_struct = pl.Struct(
        [
            pl.Field(CQCL.person_family_name, pl.String()),
            pl.Field(CQCL.person_given_name, pl.String()),
            pl.Field(CQCL.person_roles, pl.List(pl.String())),
            pl.Field(CQCL.person_title, pl.String()),
        ]
    )

    extract_registered_manager_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (
                CQCLClean.imputed_regulated_activities,
                pl.List(
                    pl.Struct(
                        [
                            pl.Field(CQCL.name, pl.String()),
                            pl.Field(CQCL.code, pl.String()),
                            pl.Field(CQCL.contacts, pl.List(contact_struct)),
                        ]
                    )
                ),
            ),
        ]
    )

    extract_contacts_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (
                CQCLClean.imputed_regulated_activities,
                pl.List(
                    pl.Struct(
                        [
                            pl.Field(CQCL.name, pl.String()),
                            pl.Field(CQCL.code, pl.String()),
                            pl.Field(CQCL.contacts, pl.List(contact_struct)),
                        ]
                    )
                ),
            ),
        ]
    )
    expected_extract_contacts_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (
                CQCLClean.imputed_regulated_activities,
                pl.List(
                    pl.Struct(
                        [
                            pl.Field(CQCL.name, pl.String()),
                            pl.Field(CQCL.code, pl.String()),
                            pl.Field(CQCL.contacts, pl.List(contact_struct)),
                        ]
                    )
                ),
            ),
            (CQCLClean.all_contacts_flat, pl.List(pl.List(contact_struct))),
        ]
    )
