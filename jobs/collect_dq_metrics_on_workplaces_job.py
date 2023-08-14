import sys

from pydeequ.repository import FileSystemMetricsRepository, ResultKey
from pydeequ.anomaly_detection import RelativeRateOfChangeStrategy
from pydeequ.analyzers import AnalysisRunner, Size, Mean
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.profiles import ColumnProfilerRunner
from pydeequ.suggestions import ConstraintSuggestionRunner, DEFAULT
from pydeequ.checks import Check, CheckLevel

from utils import utils


def main(source):
    spark = utils.get_spark()
    workplaces = spark.read.parquet(source)
    try:
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

        print("COLUMN PROFILES")

        result = (
            ColumnProfilerRunner(spark)
            .onData(workplaces.select(*columnns_to_profile))
            .run()
        )

        for col, profile in result.profiles.items():
            print(f"profile for column: {col}")
            print(profile)

        # ~~~~~~~ Suggestions for constraints to put on columns ~~~~~~~~~~~

        print("CONSTRAINT SUGGESTIONS")

        suggestionResult = (
            ConstraintSuggestionRunner(spark)
            .onData(workplaces.select(*columnns_to_profile))
            .addConstraintRule(DEFAULT())
            .run()
        )

        print(suggestionResult)

        # ~~~~~~~~~~ Setup where to store metric results and any additional tags to save alongside the metrics
        metrics_location = f"{source}metrics.json"
        metricsRepository = FileSystemMetricsRepository(spark, metrics_location)
        tags = {
            "dataset": "workplace",
            "domain": "ASCWDS",
        }
        result_key = ResultKey(spark_session=spark, tags=tags)

        # ~~~~~~~~ Running analysis on the data
        workplaces = workplaces.withColumn(
            "totalstaff", workplaces.totalstaff.cast("int")
        )
        analysis_runners = (
            AnalysisRunner(spark)
            .onData(workplaces)
            .useRepository(metricsRepository)
            .saveOrAppendResult(result_key)
            .addAnalyzer(Size())
            .addAnalyzer(Mean("totalstaff"))
        )
        analysis_runners.run()

        # ~~~~~~~~~ Defining checks to run against the data
        verification_suite = (
            VerificationSuite(spark)
            .onData(workplaces)
            .useRepository(metricsRepository)
            .saveOrAppendResult(result_key)
            .addCheck(
                Check(spark, CheckLevel.Error, "Data quality failure")
                .isComplete("establishmentid")
                .hasUniqueness(["establishmentid", "import_date"], lambda unq: unq == 1)
            )
            # .addAnomalyCheck(RelativeRateOfChangeStrategy(maxRateIncrease=2.0), Size())
        )

        # ~~~~~~~~~~~ Getting the results of these checks
        check_results = VerificationResult.checkResultsAsDataFrame(
            spark, verification_suite.run()
        )
        check_results.show()
        errors = check_results.where(check_results.constraint_status == "Failure")
        has_error = errors.count() > 0
        print(has_error)

        #  Can fail job here if any of the constraints haven't been met

        # ~~~~~~~~~~~~~ Saving the results of these checks to S3
        verification_suite.saveOrAppendResult(result_key).run()
    finally:
        spark.sparkContext._gateway.close()
        spark.stop()
    return


if __name__ == "__main__":
    print(
        "Spark job collect data quality metrics on ASC WDS worker dataset starting..."
    )
    print(f"Job parameters: {sys.argv}")

    (source,) = utils.collect_arguments(
        ("--source", "S3 path to the workplace dataset"),
    )

    main(source)
