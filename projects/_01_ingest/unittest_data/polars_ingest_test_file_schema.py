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
    flatten_struct_fields_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.import_date, pl.String()),
            (
                "struct_1",
                pl.List(pl.Struct({"field_1": pl.String(), "field_2": pl.String()})),
            ),
            (
                "struct_2",
                pl.List(pl.Struct({"field_1": pl.String(), "field_2": pl.String()})),
            ),
        ]
    )
    expected_flatten_struct_fields_schema = pl.Schema(
        list(flatten_struct_fields_schema.items())
        + [
            ("struct_1_field_1", pl.List(pl.String())),
            ("struct_2_field_2", pl.List(pl.String())),
        ]
    )


@dataclass
class FullFlattenUtilsSchema:
    load_latest_snapshot_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.import_date, pl.Int32()),
        ]
    )

    create_full_snapshot_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.number_of_beds, pl.Int32()),
            (CQCLClean.import_date, pl.Int32()),
        ]
    )

    apply_partitions_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.year, pl.Int32()),
            (CQCLClean.month, pl.Int32()),
            (CQCLClean.day, pl.Int32()),
            (CQCLClean.import_date, pl.Int32()),
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
            (CQCLClean.regulated_activities, activity_struct_list),
        ]
    )
    expected_explode_contacts_information_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.regulated_activities, activity_struct),
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
class LocationsCleanUtilsSchema:
    save_deregistered_locations_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.provider_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.registration_status, pl.String()),
            (CQCLClean.deregistration_date, pl.Date()),
        ]
    )
    expected_save_deregistered_locations_schema = pl.Schema(
        [
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.registration_status, pl.String()),
            (CQCLClean.deregistration_date, pl.Date()),
        ]
    )

    clean_provider_id_column_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.provider_id, pl.String()),
            (Keys.import_date, pl.String()),
        ]
    )

    impute_missing_values_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
            (CQCLClean.provider_id, pl.String()),
            (CQCLClean.services_offered, pl.List(pl.String())),
        ]
    )

    assign_cqc_sector_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (CQCL.provider_id, pl.String()),
        ]
    )
    expected_assign_cqc_sector_schema = pl.Schema(
        list(assign_cqc_sector_schema.items()) + [(CQCLClean.cqc_sector, pl.String())]
    )

    primary_service_type_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.provider_id, pl.String()),
            (CQCLClean.services_offered, pl.List(pl.String())),
        ]
    )
    expected_primary_service_type_schema = pl.Schema(
        list(primary_service_type_schema.items())
        + [(CQCLClean.primary_service_type, pl.String())]
    )

    realign_carehome_column_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.care_home, pl.String()),
            (CQCLClean.primary_service_type, pl.String()),
        ]
    )

    add_related_location_column_schema = pl.Schema(
        [
            (CQCL.location_id, pl.String()),
            (
                CQCLClean.relationships_types,
                pl.List(pl.String()),
            ),
        ]
    )
    expected_add_related_location_column_schema = pl.Schema(
        list(add_related_location_column_schema.items())
        + [(CQCLClean.related_location, pl.String())]
    )

    clean_and_impute_registration_date_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.registration_date, pl.Date()),
            (CQCLClean.cqc_location_import_date, pl.Date()),
        ]
    )
    expected_clean_and_impute_registration_date_schema = pl.Schema(
        list(clean_and_impute_registration_date_schema.items())
        + [
            (CQCLClean.imputed_registration_date, pl.Date()),
        ]
    )

    classify_specialisms_schema = pl.Schema(
        [
            (CQCLClean.location_id, pl.String()),
            (CQCLClean.specialisms_offered, pl.List(pl.String())),
        ]
    )
    exected_classify_specialisms_schema = pl.Schema(
        list(classify_specialisms_schema.items())
        + [
            (CQCLClean.specialism_dementia, pl.String()),
            (CQCLClean.specialism_learning_disabilities, pl.String()),
            (CQCLClean.specialism_mental_health, pl.String()),
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
