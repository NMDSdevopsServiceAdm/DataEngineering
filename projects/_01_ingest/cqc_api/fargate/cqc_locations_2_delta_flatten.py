from polars_utils import utils
from polars_utils.cleaning_utils import column_to_date
from projects._01_ingest.cqc_api.fargate.utils import flatten_utils as fUtils
from projects._01_ingest.cqc_api.fargate.utils.extract_registered_manager_names import (
    extract_registered_manager_names,
)
from utils.column_names.cleaned_data_files.cqc_location_cleaned import (
    CqcLocationCleanedColumns as CQCLClean,
)
from utils.column_names.ind_cqc_pipeline_columns import PartitionKeys as Keys
from utils.column_names.raw_data_files.cqc_location_api_columns import (
    NewCqcLocationApiColumns as CQCL,
)

cqc_partition_keys = [Keys.year, Keys.month, Keys.day, Keys.import_date]

cqc_location_cols_to_import = [
    CQCLClean.location_id,
    CQCLClean.provider_id,
    CQCLClean.name,
    CQCLClean.postal_address_line1,
    CQCLClean.postal_code,
    CQCLClean.registration_status,
    CQCLClean.registration_date,
    CQCLClean.deregistration_date,
    CQCLClean.type,
    CQCLClean.relationships,
    CQCLClean.care_home,
    CQCLClean.number_of_beds,
    CQCLClean.dormancy,
    CQCLClean.gac_service_types,
    CQCLClean.regulated_activities,
    CQCLClean.specialisms,
    Keys.import_date,
    Keys.year,
    Keys.month,
    Keys.day,
]


def main(
    cqc_locations_api_delta_source: str,
    cqc_locations_flattened_destination: str,
) -> None:
    cqc_lf = utils.scan_parquet(
        cqc_locations_api_delta_source,
        selected_columns=cqc_location_cols_to_import,
    )
    print("CQC Location LazyFrame read in")

    cqc_lf = column_to_date(cqc_lf, CQCLClean.registration_date)
    cqc_lf = column_to_date(cqc_lf, CQCLClean.deregistration_date)

    fields_to_flatten = [
        (CQCL.gac_service_types, CQCL.description, CQCLClean.services_offered),
        (CQCL.specialisms, CQCL.name, CQCLClean.specialisms_offered),
        (CQCL.regulated_activities, CQCL.name, CQCLClean.regulated_activities_offered),
        (CQCL.relationships, CQCL.type, CQCLClean.relationships_types),
    ]
    cqc_lf = fUtils.flatten_struct_fields(cqc_lf, fields_to_flatten)

    cqc_lf = extract_registered_manager_names(cqc_lf)

    cqc_lf = cqc_lf.drop(
        CQCL.gac_service_types,
        CQCL.specialisms,
        CQCL.regulated_activities,
        CQCL.relationships,
    )

    utils.sink_to_parquet(
        cqc_lf,
        cqc_locations_flattened_destination,
        partition_cols=cqc_partition_keys,
        append=False,
    )


if __name__ == "__main__":
    print("Running Flatten CQC Locations job")

    args = utils.get_args(
        (
            "--cqc_locations_api_delta_source",
            "S3 URI to read CQC locations raw API delta data from",
        ),
        (
            "--cqc_locations_flattened_destination",
            "S3 URI to save flattened CQC locations data to",
        ),
    )

    main(
        cqc_locations_api_delta_source=args.cqc_locations_api_delta_source,
        cqc_locations_flattened_destination=args.cqc_locations_flattened_destination,
    )

    print("Finished Flatten CQC Locations job")
