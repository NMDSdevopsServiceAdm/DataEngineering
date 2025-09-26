from dataclasses import dataclass

import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
)

from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
    DimensionPartitionKeys as DimKeys,
)


@dataclass
class CQCLocationsSchema:
    create_dimension_from_postcode_input_extra_cols_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.name, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.postal_address_line1, pl.String()),
            (CQCLClean.postal_code, pl.String()),
            (CQCLClean.contacts, pl.String()),
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
            (Keys.import_date, pl.String()),
        ]
    )

    expected_create_dimension_from_postcode_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.name, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.postal_address_line1, pl.String()),
            (CQCLClean.postal_code, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    create_dimension_from_postcode_input_missing_cols_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.name, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.postal_address_line1, pl.String()),
            (CQCLClean.contacts, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    create_dimension_delta_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
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
            (Keys.import_date, pl.String()),
        ]
    )

    create_dimension_delta_dim_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
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
            (Keys.import_date, pl.String()),
            (DimKeys.year, pl.String()),
            (DimKeys.month, pl.String()),
            (DimKeys.day, pl.String()),
            (DimKeys.last_updated, pl.String()),
        ]
    )

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
            (Keys.import_date, pl.String()),
        ]
    )

    expected_impute_struct_input_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
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
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (Keys.import_date, pl.String()),
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

    remove_locations_without_ra_fact_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    remove_locations_without_ra_dim_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (
                CQCLClean.imputed_regulated_activities,
                pl.List(
                    pl.Struct(
                        {
                            CQCL.name: pl.String(),
                            CQCL.code: pl.String(),
                            CQCL.contacts: pl.List(
                                pl.Struct(
                                    {
                                        CQCL.person_family_name: pl.String(),
                                        CQCL.person_given_name: pl.String(),
                                        CQCL.person_roles: pl.String(),
                                        CQCL.person_title: pl.String(),
                                    }
                                )
                            ),
                        }
                    )
                ),
            ),
            (Keys.import_date, pl.String()),
        ]
    )

    expected_remove_locations_without_ra_to_remove_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
        ]
    )

    remove_rows_to_remove_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    remove_rows_target_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.registration_status, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    remove_rows_to_remove_unmatched_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_sector, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    select_registered_locations_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.registration_status, pl.String()),
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
    activity_struct_list = pl.List(
        pl.Struct(
            [
                pl.Field(CQCL.name, pl.String()),
                pl.Field(CQCL.code, pl.String()),
                pl.Field(CQCL.contacts, pl.List(contact_struct)),
            ]
        )
    )

    extract_registered_manager_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.imputed_regulated_activities, activity_struct_list),
        ]
    )


@dataclass
class PostcodeMatcherTest:
    locations_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCL.name, pl.String()),
            (CQCL.postal_address_line1, pl.String()),
            (CQCL.postal_code, pl.String()),
        ]
    )
    postcodes_schema = pl.Schema(
        [
            (ONS.postcode, pl.String()),
            (ONSClean.contemporary_ons_import_date, pl.Date()),
            (ONSClean.contemporary_cssr, pl.String()),
            (ONSClean.contemporary_sub_icb, pl.String()),
            (ONSClean.contemporary_ccg, pl.String()),
            (ONSClean.current_cssr, pl.String()),
            (ONSClean.current_sub_icb, pl.String()),
        ]
    )

    clean_postcode_column_schema = pl.Schema([(CQCL.postal_code, pl.String())])
    expected_clean_postcode_column_when_drop_is_false_schema = pl.Schema(
        [(CQCL.postal_code, pl.String()), (CQCLClean.postcode_cleaned, pl.String())]
    )
    expected_clean_postcode_column_when_drop_is_true_schema = pl.Schema(
        [(CQCLClean.postcode_cleaned, pl.String())]
    )

    join_postcode_data_locations_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (ONSClean.contemporary_ons_import_date, pl.Date()),
            (CQCLClean.postcode_cleaned, pl.String()),
        ]
    )
    join_postcode_data_postcodes_schema = pl.Schema(
        [
            (CQCLClean.postcode_cleaned, pl.String()),
            (ONSClean.contemporary_ons_import_date, pl.Date()),
            (ONSClean.current_cssr, pl.String()),
        ]
    )
    expected_join_postcode_data_matched_schema = pl.Schema(
        list(join_postcode_data_locations_schema.items())
        + [(ONSClean.current_cssr, pl.String())]
    )
    expected_join_postcode_data_unmatched_schema = join_postcode_data_locations_schema

    first_successful_postcode_unmatched_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.postcode_cleaned, pl.String()),
        ]
    )
    first_successful_postcode_matched_schema = pl.Schema(
        list(first_successful_postcode_unmatched_schema.items())
        + [(ONSClean.current_cssr, pl.String())]
    )
    expected_get_first_successful_postcode_match_schema = (
        first_successful_postcode_unmatched_schema
    )

    amend_invalid_postcodes_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.postcode_cleaned, pl.String()),
        ]
    )

    truncate_postcode_schema = pl.Schema(
        [
            (CQCLClean.postcode_cleaned, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
        ]
    )
    expected_truncate_postcode_schema = pl.Schema(
        list(truncate_postcode_schema.items())
        + [(CQCLClean.postcode_truncated, pl.String())]
    )

    create_truncated_postcode_df_schema = pl.Schema(
        [
            (CQCLClean.postcode_cleaned, pl.String()),
            (CQCLClean.contemporary_ons_import_date, pl.Date()),
            (CQCLClean.contemporary_cssr, pl.String()),
            (CQCLClean.contemporary_ccg, pl.String()),
            (CQCLClean.contemporary_sub_icb, pl.String()),
            (CQCLClean.current_cssr, pl.String()),
            (CQCLClean.current_sub_icb, pl.String()),
        ]
    )
    expected_create_truncated_postcode_df_schema = pl.Schema(
        [
            (ONSClean.contemporary_ons_import_date, pl.Date()),
            (ONSClean.contemporary_cssr, pl.String()),
            (ONSClean.contemporary_ccg, pl.String()),
            (ONSClean.contemporary_sub_icb, pl.String()),
            (ONSClean.current_cssr, pl.String()),
            (ONSClean.current_sub_icb, pl.String()),
            (CQCLClean.postcode_truncated, pl.String()),
        ]
    )

    raise_error_if_unmatched_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.name, pl.String()),
            (CQCLClean.postal_address_line1, pl.String()),
            (CQCLClean.postcode_cleaned, pl.String()),
        ]
    )

    combine_matched_df1_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date),
            (CQCLClean.postcode_cleaned, pl.String()),
            (ONSClean.current_cssr, pl.String()),
        ]
    )
    combine_matched_df2_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.postcode_cleaned, pl.String()),
            (CQCLClean.postcode_truncated, pl.String()),
            (ONSClean.current_cssr, pl.String()),
        ]
    )
    expected_combine_matched_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.postcode_cleaned, pl.String()),
            (ONSClean.current_cssr, pl.String()),
            (CQCLClean.postcode_truncated, pl.String()),
        ]
    )
