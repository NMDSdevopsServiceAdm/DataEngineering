from dataclasses import dataclass

import polars as pl

from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.cleaned_data_files.ons_cleaned import (
    OnsCleanedColumns as ONSClean,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    DimensionPartitionKeys as DimKeys,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.raw_data_files.ons_columns import (
    OnsPostcodeDirectoryColumns as ONS,
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
