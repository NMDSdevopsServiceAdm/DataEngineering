import os

os.environ["SPARK_VERSION"] = "3.3"

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import (
    VerificationRunBuilder,
    VerificationSuite,
    VerificationResult,
)
from pyspark.sql import (
    DataFrame,
    functions as F,
)

from utils import utils
from utils.column_names.validation_table_columns import Validation
from utils.validation.validation_rule_names import (
    RuleNames as RuleToCheck,
    CustomTypeArguments,
)


def validate_dataset(dataset: DataFrame, rules: dict) -> DataFrame:
    """
    Handles the overarching process of validating a dataset.

    This function creates a verification suite on the given datasets, populates the verification
    suite with checks, runs the checks and then formats the results as a dataframe.

    Args:
        dataset(DataFrame): The dataset which requires validation.
        rules(dict): A dictionary of validation rules to apply to the dataset.

    Returns:
        DataFrame: A dataframe containing the results of each check.
    """
    spark = utils.get_spark()
    verification_run = VerificationSuite(spark).onData(dataset)
    verification_run = add_checks_to_run(verification_run, rules)
    check_result = verification_run.run()
    check_result_df = VerificationResult.checkResultsAsDataFrame(spark, check_result)
    return check_result_df


def add_checks_to_run(
    run: VerificationRunBuilder, rules_to_check: dict
) -> VerificationRunBuilder:
    """
    Adds the checks listed in the ruleset to the verification suite.

    This function adds the checks definied in the given ruleset to the varification run.
    If a rule is not recognised, a value error is raised.

    Args:
        run(VerificationRunBuilder): A verification run for a particular dataset.
        rules_to_check(dict): A dictionary of validation rules to apply to the dataset.

    Returns:
        VerificationRunBuilder: A verification run containing all the listed checks.

    Raises:
        ValueError: If rule in rules_to_check is not recognised.
    """
    for rule_name in rules_to_check.keys():
        rule = rules_to_check[rule_name]
        if rule_name == RuleToCheck.size_of_dataset:
            check = create_check_of_size_of_dataset(rule)
            run = run.addCheck(check)
        elif rule_name == RuleToCheck.complete_columns:
            check = create_check_for_column_completeness(rule)
            run = run.addCheck(check)
        elif rule_name == RuleToCheck.index_columns:
            check = create_check_of_uniqueness_of_two_index_columns(rule)
            run = run.addCheck(check)
        elif rule_name == RuleToCheck.min_values:
            for column_name in rule.keys():
                check = create_check_of_min_values(column_name, rule[column_name])
                run = run.addCheck(check)
        elif rule_name == RuleToCheck.max_values:
            for column_name in rule.keys():
                check = create_check_of_max_values(column_name, rule[column_name])
                run = run.addCheck(check)
        elif rule_name == RuleToCheck.categorical_values_in_columns:
            check = create_check_of_categorical_values_in_columns(rule)
            run = run.addCheck(check)
        elif rule_name == RuleToCheck.distinct_values:
            for column_name in rule.keys():
                check = create_check_of_number_of_distinct_values(
                    column_name, rule[column_name]
                )
                run = run.addCheck(check)
        elif rule_name == RuleToCheck.custom_type:
            check = create_check_of_custom_type(
                rule[CustomTypeArguments.column_condition],
                rule[CustomTypeArguments.constraint_name],
                rule[CustomTypeArguments.hint],
            )
            run = run.addCheck(check)
        else:
            raise ValueError("Unknown rule to check")
    return run


def create_check_for_column_completeness(complete_columns: list) -> Check:
    """
    Creates a check of column completeness to add to a verification run.

    This function creates a check of column completeness for the given columns to add to a verification run.

    Args:
        complete_columns(list): A list of columns in the dataset which should have no null values.

    Returns:
        Check: A check of column completeness to add to a verification run.
    """
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, "Column is complete")
    for column in complete_columns:
        check = check.isComplete(column, f"Completeness of {column} should be 1.")
    return check


def create_check_of_uniqueness_of_two_index_columns(column_names: list) -> Check:
    """
    Creates a check of the uniqueness of two index columns to add to a verification run.

    This function creates a check of the uniqueness of two index columns for the given columns to add to a verification run.

    Args:
        column_names(list): A list of two columns in the dataset which should be unique indexes.

    Returns:
        Check: A check of column uniqueness to add to a verification run.
    """
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, "Index columns are unique")
    check = check.hasUniqueness(
        column_names, lambda x: x == 1, "Uniqueness should be 1."
    )
    return check


def create_check_of_size_of_dataset(expected_size: int) -> Check:
    """
    Creates a check of the number of rows in the dataset to add to a verification run.

    This function creates a check of the number of rows in the dataset to add to a verification run.

    Args:
        expected_size(int): The number of rows expected in the dataset.

    Returns:
        Check: A check of the number of rows in the dataset to add to a verification run.
    """
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, "Size of dataset")
    check = check.hasSize(
        lambda x: x == expected_size,
        f"DataFrame row count should be {expected_size}.",
    )
    return check


def create_check_of_min_values(column_name: str, min_value: int) -> Check:
    """
    Creates a check of the minimum value of a column to add to a verification run.

    This function creates a check of the minimum value of the given column to add to a verification run.

    Args:
        column_name(str): A numerical column in the dataset.
        min_value(int): The minimum permitted value of the given column.

    Returns:
        Check: A check of the minimum value of a column to add to a verification run.
    """
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, f"Min value in column")
    check = check.hasMin(
        column_name,
        lambda x: x >= min_value,
        f"The minimum value for {column_name} should be {min_value}.",
    )
    return check


def create_check_of_max_values(column_name: str, max_value: int) -> Check:
    """
    Creates a check of the maximum value of a column to add to a verification run.

    This function creates a check of the maximum value of the given column to add to a verification run.

    Args:
        column_name(str): A numerical column in the dataset.
        max_value(int): The maximum permitted value of the given column.

    Returns:
        Check: A check of the maximum value of a column to add to a verification run.
    """
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, "Max value in column")
    check = check.hasMax(
        column_name,
        lambda x: x <= max_value,
        f"The maximum value for {column_name} should be {max_value}.",
    )
    return check


def create_check_of_categorical_values_in_columns(categorical_values: dict) -> Check:
    """
    Creates a check of the categorical values in columns to add to a verification run.

    This function creates a check of the categorical values of the given columns to add to a verification run.

    Args:
        categorical_values(dict): A dictionary of column names and lists of categorical values permittedin those columns.

    Returns:
        Check: A check of the categorical values in columns to add to a verification run.
    """
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


def create_check_of_number_of_distinct_values(
    column_name: str, distinct_values: int
) -> Check:
    """
    Creates a check of the number of distinct values in a column to add to a verification run.

    This function creates a check of the number of distinct values in the given column to add to a verification run.

    Args:
        column_name(str): A categorical column in the dataset.
        distinct_values(int): The number of distinct values that should be present in the given column.

    Returns:
        Check: A check of the categorical values in columns to add to a verification run.
    """
    spark = utils.get_spark()
    check = Check(
        spark, CheckLevel.Warning, "Column contains correct number of distinct values"
    )
    check = check.hasNumberOfDistinctValues(
        column=column_name,
        assertion=lambda x: x == distinct_values,
        binningUdf=None,
        maxBins=distinct_values,
        hint=f"The number of distinct values in {column_name} should be {distinct_values}.",
    )
    return check


def create_check_of_custom_type(rule: str, constraint_name: str, hint: str) -> Check:
    """
    Creates a custom check to add to a verification run.

    This function creates a custom check using a SQL WHERE clause to add to a verification run.

    Args:
        rule(str): A custom rule in the format of a SQL WHERE clause.
        constraint_name(str): A name for the custom constraint.
        hint(str): A hint for when the custom conatraint fails.

    Returns:
        Check: A custom check to add to a verification run.
    """
    spark = utils.get_spark()
    check = Check(spark, CheckLevel.Warning, "custom type")
    check = check.satisfies(
        columnCondition=rule,
        constraintName=constraint_name,
        assertion=lambda x: x == 1,
        hint=hint,
    )
    return check


def add_column_with_length_of_string(
    df: DataFrame, column_names: list[str]
) -> DataFrame:
    """
    Adds new columns to the dataframe containing the length of the given columns.

    This function generates a new column in the dataframe for every column given and
    populates it with the length of the given column. THe length columns are named with
    the original column name and the suffix _length.

    Args:
        df(DataFrame): A dataframe with the given columns.
        column_names(list[str]): A list of strings with the column names to evaluate.

    Returns:
        DataFrame: A dataframe with additional columns containing the length of each given column.
    """
    for column_name in column_names:
        new_column_name = column_name + "_length"
        df = df.withColumn(new_column_name, F.length(column_name))
    return df


def raise_exception_if_any_checks_failed(df: DataFrame) -> None:
    """
    Checks the results of the validation checks and raises an error if any checks have failed.

    This function identifies any validation failures and raises an error if any are detected.
    Args:
        df(DataFrame): A dataframe of validation results.

    Raises:
        ValueError: Data quaility failures detected.
    """
    df = df.where(df[Validation.constraint_status] == "Failure")

    failures_count = df.count()
    if failures_count == 0:
        return

    raise ValueError("Data quaility failures detected.")
