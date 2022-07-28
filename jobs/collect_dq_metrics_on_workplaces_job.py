import sys

from pydeequ.repository import *
from pydeequ.anomaly_detection import *
from pydeequ.analyzers import *
from pydeequ.verification import *

from utils import utils


def main(source):
    spark = utils.get_spark()
    metrics_location = f"{source}metrics.json"
    metricsRepository = FileSystemMetricsRepository(spark, metrics_location)

    workplaces = spark.read.parquet(source)

    columnns_to_profile = [
        "totalstaff",
        "totalstarters",
        "totalleavers",
        "totalvacancies",
        "establishmentid",
        "providerid",
        "locationid",
    ]

    # ~~~~~~~~~~ Profiling columns ~~~~~~~~~~~~
    print("GET COLUMNS PROFILES")

    result = (
        ColumnProfilerRunner(spark)
        .onData(workplaces.select(*columnns_to_profile))
        .run()
    )

    for col, profile in result.profiles.items():
        print(f"profile for column: {col}")
        print(profile)

    # ~~~~~~~ Suggestions for constraints to put on columns ~~~~~~~~~~~

    print("GET CONSTRAINT SUGGESTIONS")

    suggestionResult = (
        ConstraintSuggestionRunner(spark)
        .onData(workplaces.select(*columnns_to_profile))
        .addConstraintRule(DEFAULT())
        .run()
    )

    print(suggestionResult)

    # ~~~~~~~~~ Defining check to run against the data ~~~~~~~~~~~~~
    tags = {
        "dataset": "workplace",
        "domain": "ASCWDS",
    }

    result_key = ResultKey(spark_session=spark, tags=tags)
    verification_suite = (
        VerificationSuite(spark)
        .onData(workplaces)
        .useRepository(metricsRepository)
        .saveOrAppendResult(result_key)
        .addCheck(
            Check(spark, CheckLevel.Error, "Data quality failure")
            .isComplete("establishmentid")
            .hasUniqueness(["establishmentid", "import_date"])
        )
        .addAnomalyCheck(RelativeRateOfChangeStrategy(maxRateIncrease=2.0), Size())
    )

    # ~~~~~~~~~~~ Getting the results of these checks
    check_results = VerificationResult.checkResultsAsDataFrame(spark, result.run())

    has_error = check_results.where(check_results.constraint_status == "Failure")
    #  Can fail job here if there are any errors in this dataframe. IE if some of the constraints haven't been met

    # ~~~~~~~~~~~~~ Saving the results of these checks to S3
    verification_suite.saveOrAppendResult(result_key).run()

    return


if __name__ == "__main__":
    print(
        "Spark job collect data quality metrics on ASC WDS worker dataset starting..."
    )
    print(f"Job parameters: {sys.argv}")

    source = utils.collect_arguments(
        ("--source", "S3 path to the workplace dataset"),
    )
    main(source)
