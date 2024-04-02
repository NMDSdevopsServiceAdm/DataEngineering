from pyspark.sql import DataFrame, functions as F

from utils.reconciliation_utils.reconciliation_values import (
    ReconciliationColumns as Recon,
)
from utils.column_names.cleaned_data_files.ascwds_workplace_cleaned_values import (
    AscwdsWorkplaceCleanedColumns as AWPClean,
)


def write_to_csv(df: DataFrame, output_dir: str):
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_dir)


def create_missing_columns_required_for_output_and_reorder_for_saving(
    df: DataFrame,
) -> DataFrame:
    df = df.withColumnRenamed(AWPClean.establishment_type, Recon.sector)
    df = df.withColumnRenamed(AWPClean.region_id, Recon.sfc_region)
    df = df.withColumnRenamed(AWPClean.establishment_name, Recon.name)
    df = (
        df.withColumn(Recon.nmds, F.lit(AWPClean.nmds_id))
        .withColumn(Recon.workplace_id, F.lit(AWPClean.nmds_id))
        .withColumn(Recon.requester_name, F.concat(Recon.nmds, F.lit(" "), Recon.name))
        .withColumn(
            Recon.requester_name_2, F.concat(Recon.nmds, F.lit(" "), Recon.name)
        )
        .withColumn(Recon.status, F.lit("Open"))
        .withColumn(Recon.technician, F.lit("_"))
        .withColumn(Recon.manual_call_log, F.lit("No"))
        .withColumn(Recon.mode, F.lit("Internal"))
        .withColumn(Recon.priority, F.lit("Priority 5"))
        .withColumn(Recon.category, F.lit("Workplace"))
        .withColumn(Recon.sub_category, F.lit("Reports"))
        .withColumn(Recon.is_requester_named, F.lit("Yes"))
        .withColumn(Recon.security_question, F.lit("N/A"))
        .withColumn(Recon.website, F.lit("ASC-WDS"))
        .withColumn(Recon.item, F.lit("CQC work"))
        .withColumn(Recon.phone, F.lit(0))
    )

    return df.selectExpr(
        Recon.subject,
        Recon.nmds,
        Recon.name,
        Recon.description,
        Recon.requester_name_2,
        Recon.requester_name,
        Recon.sector,
        Recon.status,
        Recon.technician,
        Recon.sfc_region,
        Recon.manual_call_log,
        Recon.mode,
        Recon.priority,
        Recon.category,
        Recon.sub_category,
        Recon.is_requester_named,
        Recon.security_question,
        Recon.website,
        Recon.item,
        Recon.phone,
        Recon.workplace_id,
    ).sort(Recon.description)
