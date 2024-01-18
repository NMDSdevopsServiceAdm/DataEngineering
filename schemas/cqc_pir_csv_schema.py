from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
)

from utils.ind_cqc_column_names.cqc_pir_columns import (
    CqcPirColumns as ColNames,
)


PIR_CSV = StructType(
    fields=[
        StructField(ColNames.location_id, StringType(), False),
        StructField(ColNames.location_name, StringType(), False),
        StructField(ColNames.pir_type, StringType(), False),
        StructField(ColNames.pir_submission_date, StringType(), False),
        StructField(
            ColNames.people_directly_employed,
            IntegerType(),
            True,
        ),
        StructField(
            ColNames.staff_leavers,
            IntegerType(),
            True,
        ),
        StructField(ColNames.staff_vacancies, IntegerType(), True),
        StructField(
            ColNames.shared_lives_leavers,
            IntegerType(),
            True,
        ),
        StructField(
            ColNames.shared_lives_vacancies, IntegerType(), True
        ),
        StructField(ColNames.primary_inspection_category, StringType(), False),
        StructField(ColNames.region, StringType(), False),
        StructField(ColNames.local_authority, StringType(), False),
        StructField(ColNames.number_of_beds, IntegerType(), False),
        StructField(ColNames.domiciliary_care, StringType(), True),
        StructField(ColNames.location_status, StringType(), False),
    ]
)

PIR_CSV_OLD = StructType(
    fields=[
        StructField("Location_ID", StringType(), False),
        StructField("Location_name", StringType(), False),
        StructField("PIR_type", StringType(), False),
        StructField("PIR_submission_date", StringType(), False),
        StructField(
            "How_many_people_are_directly_employed_and_deliver_regulated_activities_at_your_service_as_part_of_their_daily_duties",
            IntegerType(),
            True,
        ),
        StructField(
            "How_many_staff_have_left_your_service_in_the_past_12_months",
            IntegerType(),
            True,
        ),
        StructField("How_many_staff_vacancies_do_you_have", IntegerType(), True),
        StructField(
            "How_many_Shared_Lives_workers_have_left_your_service_in_the_past_12_months",
            IntegerType(),
            True,
        ),
        StructField(
            "How_many_Shared_Lives_worker_vacancies_do_you_have", IntegerType(), True
        ),
        StructField("Location_primary_inspection_category", StringType(), False),
        StructField("Location_region", StringType(), False),
        StructField("Location_local_authority", StringType(), False),
        StructField("Location_beds", IntegerType(), False),
        StructField("Service_type_Domiciliary_care_service", StringType(), True),
        StructField("Location_status", StringType(), False),
    ]
)
