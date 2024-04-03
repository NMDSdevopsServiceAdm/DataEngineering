import sys

from pyspark.sql import DataFrame, functions as F
from typing import Tuple
from datetime import date

from utils import utils
import utils.cleaning_utils as cUtils
from utils.column_names.cleaned_data_files.cqc_location_cleaned_values import (
    CqcLocationCleanedColumns as CQCLClean,
    CqcLocationCleanedValues as CQCLValues,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)
from utils.reconciliation_utils.reconciliation_values import (
    ReconciliationColumns as ReconColumn,
    ReconciliationValues as ReconValues,
    ReconciliationDict as ReconDict,
)

cqc_locations_columns_to_import = [
    CQCLClean.import_date,
    CQCLClean.location_id,
    CQCLClean.registration_status,
    CQCLClean.deregistration_date,
]
cleaned_ascwds_workplace_columns_to_import = [
    AWPClean.ascwds_workplace_import_date,
    AWPClean.establishment_id,
    AWPClean.nmds_id,
    AWPClean.is_parent,
    AWPClean.parent_id,
    AWPClean.organisation_id,
    AWPClean.parent_permission,
    AWPClean.establishment_type,
    AWPClean.registration_type,
    AWPClean.location_id,
    AWPClean.main_service_id,
    AWPClean.establishment_name,
    AWPClean.region_id,
]


def main(
    cqc_location_api_source: str,
    cleaned_ascwds_workplace_source: str,
    reconciliation_single_and_subs_destination: str,
    reconciliation_parents_destination: str,
) -> DataFrame:
    cqc_location_df = utils.read_from_parquet(
        cqc_location_api_source, cqc_locations_columns_to_import
    )
    ascwds_workplace_df = utils.read_from_parquet(
        cleaned_ascwds_workplace_source,
        selected_columns=cleaned_ascwds_workplace_columns_to_import,
    )

    cqc_location_df = prepare_most_recent_cqc_location_df(cqc_location_df)
    (
        first_of_most_recent_month,
        first_of_previous_month,
    ) = collect_dates_to_use(cqc_location_df)

    latest_ascwds_workplace_df = prepare_latest_cleaned_ascwds_workforce_data(
        ascwds_workplace_df
    )

    merged_ascwds_cqc_df = join_cqc_location_data_into_ascwds_workplace_df(
        latest_ascwds_workplace_df, cqc_location_df
    )

    reconciliation_df = filter_to_locations_relevant_to_reconcilition_process(
        merged_ascwds_cqc_df, first_of_most_recent_month, first_of_previous_month
    )

    reconciliation_df = create_missing_columns_required_for_output(reconciliation_df)

    single_and_sub_df = (
        create_reconciliation_outputs_for_ascwds_single_and_sub_accounts(
            reconciliation_df
        )
    )

    #     create_reconciliation_outputs_for_ascwds_parent_accounts(
    #         ascwds_with_deregistration_date_df,
    #         first_of_previous_month,
    #         reconciliation_parents_destination,
    #     )

    write_to_csv(single_and_sub_df, reconciliation_single_and_subs_destination)


def filter_df_to_maximum_value_in_column(
    df: DataFrame, column_to_filter_on: str
) -> DataFrame:
    max_date = df.agg(F.max(column_to_filter_on)).collect()[0][0]

    return df.filter(F.col(column_to_filter_on) == max_date)


def prepare_most_recent_cqc_location_df(cqc_location_df: DataFrame) -> DataFrame:
    cqc_location_df = cUtils.column_to_date(
        cqc_location_df, CQCLClean.import_date, CQCLClean.cqc_location_import_date
    ).drop(CQCLClean.import_date)
    cqc_location_df = filter_df_to_maximum_value_in_column(
        cqc_location_df, CQCLClean.cqc_location_import_date
    )
    cqc_location_df = utils.format_date_fields(
        cqc_location_df,
        date_column_identifier=CQCLClean.deregistration_date,
        raw_date_format="yyyy-MM-dd",
    )
    return cqc_location_df


def collect_dates_to_use(df: DataFrame) -> Tuple[date, date, date]:
    dates_df = df.select(F.max(CQCLClean.cqc_location_import_date).alias("most_recent"))
    dates_df = dates_df.withColumn(
        "start_of_month", F.trunc(F.col("most_recent"), "mon")
    )
    dates_df = dates_df.withColumn(
        "start_of_previous_month", F.add_months(F.col("start_of_month"), -1)
    )
    dates_collected = dates_df.collect()
    first_of_most_recent_month = dates_collected[0]["start_of_month"]
    first_of_previous_month = dates_collected[0]["start_of_previous_month"]

    return (
        first_of_most_recent_month,
        first_of_previous_month,
    )


def prepare_latest_cleaned_ascwds_workforce_data(
    ascwds_workplace_df: str,
) -> DataFrame:
    df = filter_df_to_maximum_value_in_column(
        ascwds_workplace_df, AWPClean.ascwds_workplace_import_date
    )
    df = filter_to_cqc_registration_type_only(df)
    df = remove_ascwds_head_office_accounts(df)
    df = df.replace(ReconDict.region_id_dict, subset=[AWPClean.region_id])
    df = df.replace(
        ReconDict.establishment_type_dict, subset=[AWPClean.establishment_type]
    )
    df = add_parent_sub_or_single_col_to_df(df)
    df = add_ownership_col_to_df(df)
    df = add_potentials_col_to_df(df)

    return df


def add_parent_sub_or_single_col_to_df(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.parent_sub_or_single,
        F.when(
            (F.col(AWPClean.is_parent) == 1),
            F.lit(ReconValues.parent),
        )
        .when(
            ((F.col(AWPClean.is_parent) == 0) & (F.col(AWPClean.parent_id) > 0)),
            F.lit(ReconValues.subsidiary),
        )
        .otherwise(F.lit(ReconValues.single)),
    ).drop(AWPClean.is_parent, AWPClean.parent_id)


def add_ownership_col_to_df(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.ownership,
        F.when(
            (F.col(AWPClean.parent_permission) == 1),
            F.lit(ReconValues.parent),
        ).otherwise(F.lit(ReconValues.workplace)),
    ).drop(AWPClean.parent_permission)


def add_potentials_col_to_df(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.potentials,
        F.when(
            (
                (
                    (F.col(ReconColumn.parent_sub_or_single) == ReconValues.single)
                    | (
                        F.col(ReconColumn.parent_sub_or_single)
                        == ReconValues.subsidiary
                    )
                )
                & (F.col(ReconColumn.ownership) == ReconValues.workplace)
            ),
            F.lit(ReconValues.singles_and_subs),
        ).otherwise(F.lit(ReconValues.parents)),
    ).drop(ReconColumn.parent_sub_or_single, ReconColumn.ownership)


def filter_to_cqc_registration_type_only(df: DataFrame) -> DataFrame:
    return df.filter(F.col(AWPClean.registration_type) == "2")


def remove_ascwds_head_office_accounts(df: DataFrame) -> DataFrame:
    return df.where(F.col(AWPClean.main_service_id) != "72")


def join_cqc_location_data_into_ascwds_workplace_df(
    ascwds_workplace_df: DataFrame,
    cqc_location_df: DataFrame,
) -> DataFrame:
    ascwds_workplace_df = ascwds_workplace_df.withColumnRenamed(
        AWPClean.location_id, CQCLClean.location_id
    )
    return ascwds_workplace_df.join(cqc_location_df, CQCLClean.location_id, "left")


def filter_to_locations_relevant_to_reconcilition_process(
    df: DataFrame, first_of_most_recent_month: date, first_of_previous_month: date
) -> DataFrame:
    df = df.where(
        F.col(CQCLClean.registration_status).isNull()
        | (
            (
                (F.col(CQCLClean.registration_status) == CQCLValues.deregistered)
                & (F.col(CQCLClean.deregistration_date) < first_of_most_recent_month)
            )
            & (
                (F.col(ReconColumn.potentials) == ReconValues.parents)
                | (
                    (F.col(ReconColumn.potentials) == ReconValues.singles_and_subs)
                    & (F.col(CQCLClean.deregistration_date) >= first_of_previous_month)
                )
            )
        )
    )
    return df


def create_reconciliation_outputs_for_ascwds_single_and_sub_accounts(
    reconciliation_df: DataFrame,
) -> DataFrame:
    singles_and_subs_df = reconciliation_df.where(
        F.col(ReconColumn.potentials) == ReconValues.singles_and_subs
    )
    singles_and_subs_df = add_singles_and_sub_description_column(singles_and_subs_df)
    singles_and_subs_df = singles_and_subs_df.withColumn(
        ReconColumn.subject, F.lit("CQC Reconcilliation Work")
    )
    return final_column_selection(singles_and_subs_df)


def add_singles_and_sub_description_column(df: DataFrame) -> DataFrame:
    return df.withColumn(
        ReconColumn.description,
        F.when(
            F.col(CQCLClean.deregistration_date).isNotNull(),
            F.lit("Potential (new): Deregistered ID"),
        ).otherwise(F.lit("Potential (new): Regtype")),
    )


# def create_reconciliation_outputs_for_ascwds_parent_accounts(
#     reconciliation_df: DataFrame,
#     first_of_previous_month: date,
#     destination: str,
# ):
#     parents_df = reconciliation_df.where(
#         F.col(ReconColumn.potentials) == ReconValues.parents
#     )
#     unique_parentids_df = parents_df.select(
#         F.col(AWPClean.organisation_id)
#     ).dropDuplicates()

#     new_issues_df = parents_df.where(
#         F.col(CQCLClean.deregistration_date) >= first_of_previous_month
#     )
#     old_issues_df = parents_df.where(
#         F.col(CQCLClean.deregistration_date) < first_of_previous_month
#     )
#     missing_or_incorrect_df = parents_df.where(
#         F.col(CQCLClean.deregistration_date).isNull()
#     )
#     print("new_issues_df")
#     new_issues_df.show()
#     print("old_issues_df")
#     old_issues_df.show()
#     print("missing_or_incorrect_df")
#     missing_or_incorrect_df.show()

#     print("row332")
#     unique_parentids_df.show(truncate=False)
#     unique_parentids_df = join_array_of_nmdsids_into_unique_orgid_df(
#         new_issues_df, ReconColumn.new_potential_subs, unique_parentids_df
#     )
#     print("row337")
#     unique_parentids_df.show(truncate=False)
#     unique_parentids_df = join_array_of_nmdsids_into_unique_orgid_df(
#         old_issues_df, ReconColumn.old_potential_subs, unique_parentids_df
#     )
#     unique_parentids_df = join_array_of_nmdsids_into_unique_orgid_df(
#         missing_or_incorrect_df,
#         ReconColumn.missing_or_incorrect_potential_subs,
#         unique_parentids_df,
#     )

#     print("row348")
#     unique_parentids_df.show(truncate=False)
#     unique_parentids_df = unique_parentids_df.withColumn(
#         ReconColumn.description,
#         F.concat(
#             F.when(
#                 F.col(ReconColumn.new_potential_subs).isNotNull(),
#                 F.concat(F.col(ReconColumn.new_potential_subs), F.lit(" ")),
#             ).otherwise(F.lit("")),
#             F.when(
#                 F.col(ReconColumn.old_potential_subs).isNotNull(),
#                 F.concat(F.col(ReconColumn.old_potential_subs), F.lit(" ")),
#             ).otherwise(F.lit("")),
#             F.when(
#                 F.col(ReconColumn.missing_or_incorrect_potential_subs).isNotNull(),
#                 F.concat(
#                     F.col(ReconColumn.missing_or_incorrect_potential_subs), F.lit(" ")
#                 ),
#             ).otherwise(F.lit("")),
#         ),
#     )

#     print("row363")
#     unique_parentids_df.show(truncate=False)
#     unique_parentids_df = unique_parentids_df.withColumn(
#         ReconColumn.subject, F.lit("CQC Reconcilliation Work - Parent")
#     )

#     parents_output_df = (
#         create_missing_columns_required_for_output_and_reorder_for_saving(
#             unique_parentids_df
#         )
#     )
#     parents_output_df.show(truncate=False)
#     # write_to_csv(parents_output_df, destination)


# def join_array_of_nmdsids_into_unique_orgid_df(
#     df_with_issues: DataFrame, new_col_name: str, unique_df: DataFrame
# ) -> DataFrame:
#     unique_issues_parents_df = unique_parents_with_array_of_nmdsids(
#         df_with_issues, new_col_name
#     )
#     return unique_df.join(unique_issues_parents_df, AWPClean.organisation_id, "left")


# def unique_parents_with_array_of_nmdsids(df: DataFrame, new_col_name: str) -> DataFrame:
#     subs_at_parent_df = df.select(AWPClean.organisation_id, AWPClean.nmds_id)
#     subs_at_parent_df = (
#         subs_at_parent_df.groupby(AWPClean.organisation_id)
#         .agg(F.collect_set(AWPClean.nmds_id))
#         .alias(new_col_name)
#     )

#     subs_at_parent_df = subs_at_parent_df.withColumn(
#         new_col_name, F.concat_ws(", ", F.col(new_col_name))
#     )
#     subs_at_parent_df = subs_at_parent_df.withColumn(
#         new_col_name, F.concat(F.lit(new_col_name), F.lit(": "), F.col(new_col_name))
#     )

#     return subs_at_parent_df


def create_missing_columns_required_for_output(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumnRenamed(AWPClean.establishment_type, ReconColumn.sector)
    df = df.withColumnRenamed(AWPClean.region_id, ReconColumn.sfc_region)
    df = df.withColumnRenamed(AWPClean.establishment_name, ReconColumn.name)
    return (
        df.withColumn(ReconColumn.nmds, F.col(AWPClean.nmds_id))
        .withColumn(ReconColumn.workplace_id, F.col(AWPClean.nmds_id))
        .withColumn(
            ReconColumn.requester_name,
            F.concat(ReconColumn.nmds, F.lit(" "), ReconColumn.name),
        )
        .withColumn(
            ReconColumn.requester_name_2,
            F.concat(ReconColumn.nmds, F.lit(" "), ReconColumn.name),
        )
        .withColumn(ReconColumn.status, F.lit("Open"))
        .withColumn(ReconColumn.technician, F.lit("_"))
        .withColumn(ReconColumn.manual_call_log, F.lit("No"))
        .withColumn(ReconColumn.mode, F.lit("Internal"))
        .withColumn(ReconColumn.priority, F.lit("Priority 5"))
        .withColumn(ReconColumn.category, F.lit("Workplace"))
        .withColumn(ReconColumn.sub_category, F.lit("Reports"))
        .withColumn(ReconColumn.is_requester_named, F.lit("Yes"))
        .withColumn(ReconColumn.security_question, F.lit("N/A"))
        .withColumn(ReconColumn.website, F.lit("ASC-WDS"))
        .withColumn(ReconColumn.item, F.lit("CQC work"))
        .withColumn(ReconColumn.phone, F.lit(0))
    )


def final_column_selection(df: DataFrame) -> DataFrame:
    return df.select(
        ReconColumn.subject,
        ReconColumn.nmds,
        ReconColumn.name,
        ReconColumn.description,
        ReconColumn.requester_name_2,
        ReconColumn.requester_name,
        ReconColumn.sector,
        ReconColumn.status,
        ReconColumn.technician,
        ReconColumn.sfc_region,
        ReconColumn.manual_call_log,
        ReconColumn.mode,
        ReconColumn.priority,
        ReconColumn.category,
        ReconColumn.sub_category,
        ReconColumn.is_requester_named,
        ReconColumn.security_question,
        ReconColumn.website,
        ReconColumn.item,
        ReconColumn.phone,
        ReconColumn.workplace_id,
    ).sort(ReconColumn.description, ReconColumn.nmds)


def write_to_csv(df: DataFrame, output_dir: str):
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)


if __name__ == "__main__":
    print("Spark job 'reconciliation' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_api_source,
        cleaned_ascwds_workplace_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_location_api_source",
            "Source s3 directory for initial CQC location api dataset",
        ),
        (
            "--cleaned_ascwds_workplace_source",
            "Source s3 directory for cleaned parquet ASCWDS workplace dataset",
        ),
        (
            "--reconciliation_single_and_subs_destination",
            "Destination s3 directory for reconciliation parquet singles and subs dataset",
        ),
        (
            "--reconciliation_parents_destination",
            "Destination s3 directory for reconciliation parquet parents dataset",
        ),
    )
    main(
        cqc_location_api_source,
        cleaned_ascwds_workplace_source,
        reconciliation_single_and_subs_destination,
        reconciliation_parents_destination,
    )

    print("Spark job 'reconciliation' complete")
