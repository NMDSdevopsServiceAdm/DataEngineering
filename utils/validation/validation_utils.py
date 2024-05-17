import os

os.environ["SPARK_VERSION"] = "3.3"

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import (
    VerificationRunBuilder,
    VerificationSuite,
    VerificationResult,
)
from pyspark.sql.dataframe import DataFrame

from utils import utils
from utils.validation.validation_rule_names import RuleNames as RuleToCheck


def validate_dataset(dataset: DataFrame, rules: dict) -> DataFrame:
    spark = utils.get_spark()
    verification_run = VerificationSuite(spark).onData(dataset)
    verification_run = add_checks_to_run(verification_run, rules)
    check_result = verification_run.run()
    check_result_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
    return check_result_df


def add_checks_to_run(
    run: VerificationRunBuilder, rules_to_check: dict
) -> VerificationRunBuilder:
    for rule in rules_to_check.keys():
        check = create_check(run, rule, rules_to_check[rule])
        run = run.addCheck(check)
    return run


def create_check(run: VerificationRunBuilder, rule_name: str, rule) -> Check:
    if rule_name == RuleToCheck.size_of_dataset:
        check = create_check_of_size_of_dataset(rule)
    elif rule_name == RuleToCheck.complete_columns:
        check = create_check_for_column_completeness(rule)
    elif rule_name == RuleToCheck.index_columns:
        check = create_check_of_uniqueness_of_two_index_columns(rule)
    elif rule_name == RuleToCheck.min_values:
        for column_name in rule.keys():
            check = create_check_of_min_values(column_name, rule[column_name])
            run = run.addCheck(check)
    elif rule_name == RuleToCheck.max_values:
        check = create_check_of_max_values(rule)
    elif rule_name == RuleToCheck.categorical_values_in_columns:
        check = create_check_of_categorical_values_in_columns(rule)
    elif rule_name == RuleToCheck.distinct_values:
        check = create_check_of_number_of_distinct_values(rule)
    else:
        raise ValueError("Unknown rule to check")
    return check


def create_check_for_column_completeness(complete_columns: list) -> Check:
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, "Column is complete")
    for column in complete_columns:
        check = check.isComplete(column, f"Completeness of {column} should be 1.")
    return check


def create_check_of_uniqueness_of_two_index_columns(column_names: list) -> Check:
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, "Index columns are unique")
    check = check.hasUniqueness(
        column_names, lambda x: x == 1, "Uniqueness should be 1."
    )
    return check


def create_check_of_size_of_dataset(expected_size: int) -> Check:
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, "Size of dataset")
    check = check.hasSize(
        lambda x: x == expected_size,
        f"DataFrame row count should be {expected_size}.",
    )
    return check


def create_check_of_min_values(column_name: str, min_value: int) -> Check:
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, f"Min value in column")
    check = check.hasMin(
        column_name,
        lambda x: x >= min_value,
        f"The minimum value for {column_name} should be {min_value}.",
    )
    return check


def create_check_of_max_values(column_maximums: dict) -> Check:
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, "Max value in column")
    for column in column_maximums.keys():
        check = check.hasMax(
            column,
            lambda x: x <= column_maximums[column],
            f"The maximum value for {column} should be {column_maximums[column]}.",
        )
    return check


def create_check_of_categorical_values_in_columns(categorical_values: dict) -> Check:
    spark = utils.get_spark()
    check = Check(
        spark, CheckLevel.Warning, "Categorical values are in list of expected values"
    )
    for column in categorical_values.keys():
        check = check.isContainedIn(
            column,
            categorical_values[column],
            hint=f"Values in {column} should be one of :{categorical_values[column]}.",
        )
    return check


def create_check_of_number_of_distinct_values(distinct_values: dict) -> Check:
    spark = utils.get_spark()
    check = Check(
        spark, CheckLevel.Warning, "Column contains correct number of distinct values"
    )
    for column in distinct_values.keys():
        check = check.hasNumberOfDistinctValues(
            column=column,
            assertion=lambda x: x == distinct_values[column],
            binningUdf=None,
            maxBins=distinct_values[column],
            hint=f"The number of distinct values in {column} should be {distinct_values[column]}.",
        )
    return check
