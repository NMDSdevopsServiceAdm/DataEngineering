from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    ArrayType,
    FloatType,
    IntegerType,
)

SPSS_JOBS_ESTIMATES = StructType(
    fields=[
        StructField("LOCATIONID", StringType(), True),
        StructField("Main_Service_Group", IntegerType(), # TODO),
        StructField("Care_homes_beds"),
        StructField("WEIGHTING_CSSR"),
        StructField("WEIGHTING_REGION"),
        StructField("totalstaff"),
        StructField("wkrrecs"),
        StructField("jr28work"),
        StructField("Data_Used"),
        StructField("All_jobs"),
        StructField("Snapshot_date"),
        StructField("Local_authority"),
        StructField("Region"),
        StructField("Main_service"),
        StructField("Data_used_string"),
    ]
)
