from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
)

from utils.column_names.raw_data_files.cqc_pir_columns import (
    CqcPirColumns as ColNames,
)


PIR_SCHEMA = StructType(
    fields=[
        StructField(ColNames.location_id, StringType(), False),
        StructField(ColNames.location_name, StringType(), False),
        StructField(ColNames.pir_type, StringType(), False),
        StructField(ColNames.pir_submission_date, StringType(), False),
        StructField(
            ColNames.pir_people_directly_employed,
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
        StructField(ColNames.shared_lives_vacancies, IntegerType(), True),
        StructField(ColNames.primary_inspection_category, StringType(), False),
        StructField(ColNames.region, StringType(), False),
        StructField(ColNames.local_authority, StringType(), False),
        StructField(ColNames.number_of_beds, IntegerType(), False),
        StructField(ColNames.domiciliary_care, StringType(), True),
        StructField(ColNames.location_status, StringType(), False),
    ]
)
