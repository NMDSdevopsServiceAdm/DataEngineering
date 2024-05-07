import os
import sys

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
        check = create_check(rule, rules_to_check[rule])
        run = run.addCheck(check)
    return run


def create_check(rule_name: str, rule) -> Check:
    if rule_name == RuleToCheck.size_of_dataset:
        check = create_check_of_size_of_dataset(rule)
    elif rule_name == RuleToCheck.complete_columns:
        check = create_check_for_column_completeness(rule)
    elif rule_name == RuleToCheck.index_columns:
        check = create_check_of_uniqueness_of_two_index_columns(rule)
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
