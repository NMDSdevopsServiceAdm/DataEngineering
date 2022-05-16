import os
from .dev import constants as dev_constants
from .prod import constants as prod_constants

OS_ENVIRONEMNT_VARIABLE = "environment"
DEVELOPMENT = "dev"
PRODUCTION = "prod"

DEFAULT_ENVIRONMENT = DEVELOPMENT
CONSTANTS_MAPPING = {DEVELOPMENT: dev_constants, PRODUCTION: prod_constants}


def get_current_envionment():
    return os.getenv(OS_ENVIRONEMNT_VARIABLE, DEFAULT_ENVIRONMENT)


def get_ascwds_base_path():
    env = get_current_envionment()
    return CONSTANTS_MAPPING[env].ASCWDS_WORKPLACE_BASE_PATH


def get_cqc_locations_base_path():
    env = get_current_envionment()
    return CONSTANTS_MAPPING[env].CQC_LOCATIONS_BASE_PATH


def get_cqc_providers_base_path():
    env = get_current_envionment()
    return CONSTANTS_MAPPING[env].CQC_PROVIDERS_BASE_PATH


def get_pir_base_path():
    env = get_current_envionment()
    return CONSTANTS_MAPPING[env].PIR_BASE_PATH
