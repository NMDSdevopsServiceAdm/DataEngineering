import pytest

from utils.column_names.ind_cqc_pipeline_columns import (
    StartersLeaversVacanciesColumns as SLVCols,
)
from utils.column_values.categorical_column_values import (
    LeaversFilteringRule,
    StartersFilteringRule,
    VacanciesFilteringRule,
)
from utils.column_values.categorical_columns_by_dataset import (
    SLVCleanCategoricalValues,
)


@pytest.mark.parametrize(
    "rule_class", [StartersFilteringRule, LeaversFilteringRule, VacanciesFilteringRule]
)
def test_slv_rule_classes_have_populated_and_missing_values(rule_class):
    assert rule_class.populated == "populated"
    assert rule_class.missing_data == "missing_data"


@pytest.mark.parametrize(
    "rule_class", [StartersFilteringRule, LeaversFilteringRule, VacanciesFilteringRule]
)
def test_slv_rule_classes_include_not_known_code_value(rule_class):
    assert (
        rule_class.contained_invalid_missing_data_code
        == "contained_invalid_missing_data_code"
    )


@pytest.mark.parametrize(
    "attribute, rule_class, column",
    [
        (
            "starters_filtering_rule_column_values",
            StartersFilteringRule,
            SLVCols.starters_filtering_rule,
        ),
        (
            "leavers_filtering_rule_column_values",
            LeaversFilteringRule,
            SLVCols.leavers_filtering_rule,
        ),
        (
            "vacancies_filtering_rule_column_values",
            VacanciesFilteringRule,
            SLVCols.vacancies_filtering_rule,
        ),
    ],
)
def test_slv_clean_categorical_values_use_per_metric_rule(
    attribute, rule_class, column
):
    values = getattr(SLVCleanCategoricalValues, attribute)

    assert isinstance(values, rule_class)
    assert values.column_name == column
