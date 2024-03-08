import unittest
import warnings
from unittest.mock import ANY, Mock, patch


import jobs.estimate_ind_cqc_filled_posts as job
from utils import utils
from utils.column_names.ind_cqc_pipeline_columns import (
    PartitionKeys as Keys,
)


class EstimateIndCQCFilledPostsTests(unittest.TestCase):
    CLEANED_IND_CQC_TEST_DATA = "some/cleaned/data"
    CARE_HOME_FEATURES = "care home features"
    NON_RES_FEATURES = "non res features"
    CARE_HOME_MODEL = "care home model"
    NON_RES_MODEL = "non res model"
    METRICS_DESTINATION = "metrics destination"
    ESTIMATES_DESTINATION = "estimates destination"
    JOB_RUN_ID = "job run id"
    JOB_NAME = "job name"
    partition_keys = [
        Keys.year,
        Keys.month,
        Keys.day,
        Keys.import_date,
    ]

    def setUp(self):
        self.spark = utils.get_spark()

        warnings.filterwarnings("ignore", category=ResourceWarning)







if __name__ == "__main__":
    unittest.main(warnings="ignore")
