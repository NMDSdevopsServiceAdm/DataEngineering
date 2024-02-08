import sys

from utils import utils

from pyspark.sql.dataframe import DataFrame

import pyspark.sql.functions as F

from utils.column_names.cleaned_data_files.cqc_location_data_columns import (
    CqcLocationCleanedColumns as CleanedColumns,
)
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    CqcLocationApiColumns as CQCL,
)
from utils.column_names.cleaned_data_files.cqc_provider_data_columns_values import (
    CqcProviderCleanedColumns as CQCPClean,
)
from utils.column_names.cleaned_data_files.cqc_location_data_columns import (
    CqcLocationCleanedColumns as CQCLClean,
)

cqcPartitionKeys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

NURSING_HOME_IDENTIFIER = "Care home with nursing"
NONE_NURSING_HOME_IDENTIFIER = "Care home without nursing"
NONE_RESIDENTIAL_IDENTIFIER = "non-residential"


def main(
    cqc_location_source: str,
    cleaned_provider_source: str,
    cleaned_cqc_location_destintion: str,
):
    cqc_location_df = utils.read_from_parquet(cqc_location_source)
    cqc_provider_df = utils.read_from_parquet(cleaned_provider_source)

    cqc_location_df = remove_invalid_postcodes(cqc_location_df)

    cqc_location_df = join_cqc_provider_data(cqc_location_df, cqc_provider_df)

    cqc_location_df = allocate_primary_service_type(cqc_location_df)

    utils.write_to_parquet(
        cqc_location_df,
        cleaned_cqc_location_destintion,
        append=True,
        partitionKeys=cqcPartitionKeys,
    )


def remove_invalid_postcodes(df: DataFrame):
    post_codes_mapping = {
        "B69 E3G": "B69 3EG",
        "UB4 0EJ.": "UB4 0EJ",
        "TS17 3TB": "TS18 3TB",
        "TF7 3BY": "TF4 3BY",
        "S26 4DW": "S26 4WD",
        "B7 5DP": "B7 5PD",
        "DE1 IBT": "DE1 1BT",
        "YO61 3FF": "YO61 3FN",
        "L20 4QC": "L20 4QG",
        "PR! 9HL": "PR1 9HL",
        "N8 5HY": "N8 7HS",
        "PA20 3AR": "PO20 3BD",
        "CRO 4TB": "CR0 4TB",
        "PA20 3BD": "PO20 3BD",
        "LE65 3LP": "LE65 2RW",
        "NG6 3DG": "NG5 2AT",
        "HU17 ORH": "HU17 0RH",
        "SY2 9JN": "SY3 9JN",
        "CA1 2XT": "CA1 2TX",
        "MK9 1HF": "MK9 1FH",
        "HP20 1SN.": "HP20 1SN",
        "CH41 1UE": "CH41 1EU",
        "WR13 3JS": "WF13 3JS",
        "B12 ODG": "B12 0DG",
        "PO8 4PY": "PO4 8PY",
        "TS20 2BI": "TS20 2BL",
        "NG10 9LA": "NN10 9LA",
        "SG1 8AL": "SG18 8AL",
        "CCT8 8SA": "CT8 8SA",
        "OX4 2XQ": "OX4 2SQ",
        "B66 2FF": "B66 2AL",
        "EC2 5UU": "EC2M 5UU",
        "HU21 0LS": "HU170LS",
        "PL7 1RP": "PL7 1RF",
        "WF12 2SE": "WF13 2SE",
        "N12 8FP": "N12 8NP",
        "ST4 4GF": "ST4 7AA",
        "BN6 4EA": "BN16 4EA",
        "B97 6DT": "B97 6AT",
    }

    map_func = F.udf(lambda row: post_codes_mapping.get(row, row))
    return df.withColumn(CQCL.postcode, map_func(F.col(CQCL.postcode)))


def allocate_primary_service_type(df: DataFrame):
    return df.withColumn(
        CleanedColumns.primary_service_type,
        F.when(
            F.array_contains(
                df[CQCL.gac_service_types].description,
                "Care home service with nursing",
            ),
            NURSING_HOME_IDENTIFIER,
        )
        .when(
            F.array_contains(
                df[CQCL.gac_service_types].description,
                "Care home service without nursing",
            ),
            NONE_NURSING_HOME_IDENTIFIER,
        )
        .otherwise(NONE_RESIDENTIAL_IDENTIFIER),
    )


def join_cqc_provider_data(locations_df: DataFrame, provider_df: DataFrame):
    provider_data_to_join_df = provider_df.select(
        provider_df[CQCPClean.provider_id].alias("provider_id_to_drop"),
        provider_df[CQCPClean.name].alias(CQCLClean.provider_name),
        provider_df[CQCPClean.cqc_sector],
        provider_df[Keys.import_date].alias("import_date_to_drop"),
    )
    columns_to_join = [
        locations_df[CQCL.provider_id]
        == provider_data_to_join_df["provider_id_to_drop"],
        locations_df[Keys.import_date]
        == provider_data_to_join_df["import_date_to_drop"],
    ]
    joined_df = locations_df.join(
        provider_data_to_join_df, columns_to_join, how="left"
    ).drop("provider_id_to_drop", "import_date_to_drop")

    return joined_df


if __name__ == "__main__":
    print("Spark job 'clean_cqc_location_data' starting...")
    print(f"Job parameters: {sys.argv}")

    (
        cqc_location_source,
        cleaned_provider_source,
        cleaned_cqc_location_destination,
    ) = utils.collect_arguments(
        (
            "--cqc_location_source",
            "Source s3 directory for parquet CQC locations dataset",
        ),
        (
            "--cqc_provider_cleaned",
            "Source s3 directory for cleaned parquet CQC provider dataset",
        ),
        (
            "--cleaned_cqc_location_destination",
            "Destination s3 directory for cleaned parquet CQC locations dataset",
        ),
    )
    main(cqc_location_source, cleaned_provider_source, cleaned_cqc_location_destination)

    print("Spark job 'clean_cqc_location_data' complete")
