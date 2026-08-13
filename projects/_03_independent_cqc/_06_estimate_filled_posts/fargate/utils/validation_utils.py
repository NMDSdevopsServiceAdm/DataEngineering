import polars as pl

from utils.column_names.ind_cqc_pipeline_columns import IndCqcColumns
from utils.column_values.categorical_column_values import (
    CareHome,
    PrimaryServiceType,
    PrimaryServiceTypeSecondLevel,
    Services,
)


def care_home_matches_primary_service_type_expr() -> pl.Expr:
    """
    The data in care_home and primary_service_type should be related: a
    non-care-home location must have a non-residential primary service type,
    and a care home must have one of the two care-home primary service types.

    Returns:
        pl.Expr: a boolean expression, true where care_home and
        primary_service_type are consistent.
    """
    return (
        (
            pl.col(IndCqcColumns.care_home).eq(CareHome.not_care_home)
            & pl.col(IndCqcColumns.primary_service_type).eq(
                PrimaryServiceType.non_residential
            )
        )
        | (
            pl.col(IndCqcColumns.care_home).eq(CareHome.care_home)
            & pl.col(IndCqcColumns.primary_service_type).eq(
                PrimaryServiceType.care_home_with_nursing
            )
        )
        | (
            pl.col(IndCqcColumns.care_home).eq(CareHome.care_home)
            & pl.col(IndCqcColumns.primary_service_type).eq(
                PrimaryServiceType.care_home_only
            )
        )
    )


def _services_offered_consistency_expr(
    second_level_value: str,
    required_service: str,
    excluded_services: list[str],
) -> pl.Expr:
    """
    Shared engine for the services_offered consistency rules: if
    primary_service_type_second_level equals second_level_value,
    services_offered must contain required_service and must not contain any
    of excluded_services. Rows where second_level_value doesn't apply always
    pass.

    Args:
        second_level_value (str): the primary_service_type_second_level value
            the rule applies to.
        required_service (str): the services_offered value required when the
            rule applies.
        excluded_services (list[str]): services_offered values that must not
            be present when the rule applies.

    Returns:
        pl.Expr: a boolean expression, true where the rule holds or does not
        apply.
    """
    applies = pl.col(IndCqcColumns.primary_service_type_second_level).eq(
        second_level_value
    )
    services_offered = pl.col(IndCqcColumns.services_offered)
    contains_required = services_offered.list.contains(required_service)
    excludes_others = (
        ~pl.any_horizontal(
            [services_offered.list.contains(service) for service in excluded_services]
        )
        if excluded_services
        else pl.lit(True)
    )
    return ~applies | (applies & contains_required & excludes_others)


def shared_lives_services_offered_expr() -> pl.Expr:
    """
    If primary_service_type_second_level is 'Shared Lives', services_offered
    must contain 'Shared Lives'.

    Returns:
        pl.Expr: a boolean expression, true where the rule holds or does not
        apply.
    """
    return _services_offered_consistency_expr(
        PrimaryServiceTypeSecondLevel.shared_lives,
        Services.shared_lives,
        excluded_services=[],
    )


def care_home_with_nursing_services_offered_expr() -> pl.Expr:
    """
    If primary_service_type_second_level is 'Care home with nursing',
    services_offered must contain 'Care home service with nursing' and must
    not contain 'Shared Lives'.

    Returns:
        pl.Expr: a boolean expression, true where the rule holds or does not
        apply.
    """
    return _services_offered_consistency_expr(
        PrimaryServiceTypeSecondLevel.care_home_with_nursing,
        Services.care_home_service_with_nursing,
        excluded_services=[Services.shared_lives],
    )


def care_home_without_nursing_services_offered_expr() -> pl.Expr:
    """
    If primary_service_type_second_level is 'Care home without nursing',
    services_offered must contain 'Care home service without nursing' and
    must not contain 'Shared Lives' or 'Care home service with nursing'.

    Returns:
        pl.Expr: a boolean expression, true where the rule holds or does not
        apply.
    """
    return _services_offered_consistency_expr(
        PrimaryServiceTypeSecondLevel.care_home_only,
        Services.care_home_service_without_nursing,
        excluded_services=[
            Services.shared_lives,
            Services.care_home_service_with_nursing,
        ],
    )
