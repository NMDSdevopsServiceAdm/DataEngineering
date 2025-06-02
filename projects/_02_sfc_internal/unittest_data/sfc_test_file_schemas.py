from dataclasses import dataclass

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
)

from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)
from utils.column_names.reconciliation_columns import (
    ReconciliationColumns as ReconColumn,
)


@dataclass
class ReconciliationSchema:
    input_ascwds_workplace_schema = StructType(
        [
            StructField(AWPClean.ascwds_workplace_import_date, DateType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.parent_permission, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.registration_type, StringType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.main_service_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
        ]
    )
    input_cqc_location_api_schema = StructType(
        [
            StructField(Keys.import_date, StringType(), True),
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.deregistration_date, StringType(), True),
        ]
    )


@dataclass
class ReconciliationUtilsSchema:
    input_ascwds_workplace_schema = ReconciliationSchema.input_ascwds_workplace_schema
    input_cqc_location_api_schema = ReconciliationSchema.input_cqc_location_api_schema

    expected_prepared_most_recent_cqc_location_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.registration_status, StringType(), True),
            StructField(CQCL.deregistration_date, DateType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    dates_to_use_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )
    dates_to_use_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCLClean.cqc_location_import_date, DateType(), True),
        ]
    )

    regtype_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.registration_type, StringType(), True),
        ]
    )

    remove_head_office_accounts_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.main_service_id, StringType(), True),
        ]
    )

    filter_to_relevant_schema = StructType(
        [
            StructField(CQCLClean.location_id, StringType(), True),
            StructField(CQCLClean.registration_status, StringType(), True),
            StructField(CQCLClean.deregistration_date, DateType(), True),
            StructField(ReconColumn.parents_or_singles_and_subs, StringType(), True),
        ]
    )

    parents_or_singles_and_subs_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField(AWPClean.parent_permission, StringType(), True),
        ]
    )
    expected_parents_or_singles_and_subs_schema = StructType(
        [
            *parents_or_singles_and_subs_schema,
            StructField(ReconColumn.parents_or_singles_and_subs, StringType(), True),
        ]
    )

    parents_or_singles_and_subs_schema = StructType(
        [
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField(AWPClean.parent_permission, StringType(), True),
        ]
    )
    expected_parents_or_singles_and_subs_schema = StructType(
        [
            *parents_or_singles_and_subs_schema,
            StructField(ReconColumn.parents_or_singles_and_subs, StringType(), True),
        ]
    )

    add_singles_and_subs_description_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.deregistration_date, DateType(), True),
        ]
    )

    expected_singles_and_subs_description_schema = StructType(
        [
            *add_singles_and_subs_description_schema,
            StructField(ReconColumn.description, StringType(), True),
        ]
    )

    create_missing_columns_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
        ]
    )

    expected_create_missing_columns_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(ReconColumn.sector, StringType(), True),
            StructField(ReconColumn.sfc_region, StringType(), True),
            StructField(ReconColumn.name, StringType(), True),
            StructField(ReconColumn.nmds, StringType(), True),
            StructField(ReconColumn.workplace_id, StringType(), True),
            StructField(ReconColumn.requester_name, StringType(), True),
            StructField(ReconColumn.requester_name_2, StringType(), True),
            StructField(ReconColumn.status, StringType(), True),
            StructField(ReconColumn.technician, StringType(), True),
            StructField(ReconColumn.manual_call_log, StringType(), True),
            StructField(ReconColumn.mode, StringType(), True),
            StructField(ReconColumn.priority, StringType(), True),
            StructField(ReconColumn.category, StringType(), True),
            StructField(ReconColumn.sub_category, StringType(), True),
            StructField(ReconColumn.is_requester_named, StringType(), True),
            StructField(ReconColumn.security_question, StringType(), True),
            StructField(ReconColumn.website, StringType(), True),
            StructField(ReconColumn.item, StringType(), True),
            StructField(ReconColumn.phone, IntegerType(), True),
        ]
    )

    final_column_selection_schema = StructType(
        [
            StructField("extra_column", StringType(), True),
            StructField(ReconColumn.mode, StringType(), True),
            StructField(ReconColumn.priority, StringType(), True),
            StructField(ReconColumn.category, StringType(), True),
            StructField(ReconColumn.sub_category, StringType(), True),
            StructField(ReconColumn.is_requester_named, StringType(), True),
            StructField(ReconColumn.security_question, StringType(), True),
            StructField(ReconColumn.website, StringType(), True),
            StructField(ReconColumn.item, StringType(), True),
            StructField(ReconColumn.phone, IntegerType(), True),
            StructField(ReconColumn.workplace_id, StringType(), True),
            StructField(ReconColumn.subject, StringType(), True),
            StructField(ReconColumn.nmds, StringType(), True),
            StructField(ReconColumn.name, StringType(), True),
            StructField(ReconColumn.description, StringType(), True),
            StructField(ReconColumn.requester_name, StringType(), True),
            StructField(ReconColumn.requester_name_2, StringType(), True),
            StructField(ReconColumn.sector, StringType(), True),
            StructField(ReconColumn.status, StringType(), True),
            StructField(ReconColumn.technician, StringType(), True),
            StructField(ReconColumn.sfc_region, StringType(), True),
            StructField(ReconColumn.manual_call_log, StringType(), True),
        ]
    )

    expected_final_column_selection_schema = StructType(
        [
            StructField(ReconColumn.subject, StringType(), True),
            StructField(ReconColumn.nmds, StringType(), True),
            StructField(ReconColumn.name, StringType(), True),
            StructField(ReconColumn.description, StringType(), True),
            StructField(ReconColumn.requester_name_2, StringType(), True),
            StructField(ReconColumn.requester_name, StringType(), True),
            StructField(ReconColumn.sector, StringType(), True),
            StructField(ReconColumn.status, StringType(), True),
            StructField(ReconColumn.technician, StringType(), True),
            StructField(ReconColumn.sfc_region, StringType(), True),
            StructField(ReconColumn.manual_call_log, StringType(), True),
            StructField(ReconColumn.mode, StringType(), True),
            StructField(ReconColumn.priority, StringType(), True),
            StructField(ReconColumn.category, StringType(), True),
            StructField(ReconColumn.sub_category, StringType(), True),
            StructField(ReconColumn.is_requester_named, StringType(), True),
            StructField(ReconColumn.security_question, StringType(), True),
            StructField(ReconColumn.website, StringType(), True),
            StructField(ReconColumn.item, StringType(), True),
            StructField(ReconColumn.phone, IntegerType(), True),
            StructField(ReconColumn.workplace_id, StringType(), True),
        ]
    )

    add_subject_column_schema = StructType(
        [
            StructField("id", StringType(), True),
        ]
    )

    expected_add_subject_column_schema = StructType(
        [
            *add_subject_column_schema,
            StructField(ReconColumn.subject, StringType(), True),
        ]
    )

    new_issues_schema = StructType(
        [
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )
    unique_schema = StructType(
        [
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )

    expected_join_array_of_nmdsids_schema = StructType(
        [
            *unique_schema,
            StructField("new_column", StringType(), True),
        ]
    )

    create_parents_description_schema = StructType(
        [
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(ReconColumn.new_potential_subs, StringType(), True),
            StructField(ReconColumn.old_potential_subs, StringType(), True),
            StructField(
                ReconColumn.missing_or_incorrect_potential_subs, StringType(), True
            ),
        ]
    )

    expected_create_parents_description_schema = StructType(
        [
            *create_parents_description_schema,
            StructField(ReconColumn.description, StringType(), True),
        ]
    )

    get_ascwds_parent_accounts_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
            StructField(AWPClean.is_parent, StringType(), True),
            StructField("other column", StringType(), True),
        ]
    )

    expected_get_ascwds_parent_accounts_schema = StructType(
        [
            StructField(AWPClean.nmds_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(AWPClean.establishment_name, StringType(), True),
            StructField(AWPClean.organisation_id, StringType(), True),
            StructField(AWPClean.establishment_type, StringType(), True),
            StructField(AWPClean.region_id, StringType(), True),
        ]
    )

    cqc_data_for_join_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(CQCL.name, StringType(), True),
        ]
    )
    ascwds_data_for_join_schema = StructType(
        [
            StructField(AWPClean.location_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
        ]
    )
    expected_data_for_join_schema = StructType(
        [
            StructField(CQCL.location_id, StringType(), True),
            StructField(AWPClean.establishment_id, StringType(), True),
            StructField(CQCL.name, StringType(), True),
        ]
    )
