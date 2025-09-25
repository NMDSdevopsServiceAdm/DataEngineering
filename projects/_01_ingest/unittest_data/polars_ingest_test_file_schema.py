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
class PostcodeMatcherTest:
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
