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


@dataclass
class FlattenUtilsSchema:
    impute_missing_struct_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.import_date, pl.String()),
            (
                CQCLClean.gac_service_types,
                pl.List(
                    pl.Struct(
                        {
                            CQCLClean.name: pl.String(),
                            CQCLClean.description: pl.String(),
                        }
                    )
                ),
            ),
            (
                CQCLClean.specialisms,
                pl.List(pl.Struct({CQCLClean.name: pl.String()})),
            ),
        ]
    )
    expected_impute_missing_struct_one_col_schema = pl.Schema(
        list(impute_missing_struct_schema.items())
        + [
            (
                CQCLClean.imputed_gac_service_types,
                pl.List(
                    pl.Struct(
                        {
                            CQCLClean.name: pl.String(),
                            CQCLClean.description: pl.String(),
                        }
                    )
                ),
            )
        ]
    )
    expected_impute_missing_struct_two_cols_schema = pl.Schema(
        list(impute_missing_struct_schema.items())
        + [
            (
                CQCLClean.imputed_gac_service_types,
                pl.List(
                    pl.Struct(
                        {
                            CQCLClean.name: pl.String(),
                            CQCLClean.description: pl.String(),
                        }
                    )
                ),
            ),
            (
                CQCLClean.imputed_specialisms,
                pl.List(pl.Struct({CQCLClean.name: pl.String()})),
            ),
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
    activity_struct = pl.Struct(
        [
            pl.Field(CQCL.name, pl.String()),
            pl.Field(CQCL.code, pl.String()),
            pl.Field(CQCL.contacts, pl.List(contact_struct)),
        ]
    )
    activity_struct_list = pl.List(activity_struct)

    explode_contacts_information_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.imputed_regulated_activities, activity_struct_list),
        ]
    )
    expected_explode_contacts_information_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.imputed_regulated_activities, activity_struct),
            (CQCLClean.contacts_exploded, contact_struct),
        ]
    )

    select_and_create_full_name_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.contacts_exploded, contact_struct),
        ]
    )
    expected_select_and_create_full_name_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.contacts_full_name, pl.String()),
        ]
    )

    add_registered_manager_names_full_lf_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.care_home, pl.String()),
        ]
    )
    registered_manager_names_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.contacts_full_name, pl.String()),
        ]
    )
    expected_add_registered_manager_names_schema = pl.Schema(
        list(add_registered_manager_names_full_lf_schema.items())
        + [(CQCLClean.registered_manager_names, pl.List(pl.String()))]
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
            (CQCLClean.registration_status, pl.String()),
            (CQCLClean.type, pl.String()),
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
