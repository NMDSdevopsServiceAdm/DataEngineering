import pyspark
import pyspark.sql.functions as F


def job_count_from_ascwds_is_not_populated(col_name: str) -> pyspark.sql.Column:
    return F.col(col_name).isNull()
