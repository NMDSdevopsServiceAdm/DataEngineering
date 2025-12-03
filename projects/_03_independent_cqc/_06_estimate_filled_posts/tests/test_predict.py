import pickle
import unittest

import polars as pl

from projects._03_independent_cqc._06_estimate_filled_posts.fargate import (
    predict as job,
)

DATA = [
    {
        "id": 0,
        "some_data": "twc",
        "column1": 4.282563167045816,
        "column2": 14.991974662600391,
        "target1": 72.3,
        "target2": 18.7,
    },
    {
        "id": 1,
        "some_data": "gau",
        "column1": 0.664230851848101,
        "column2": 13.491249197604272,
        "target1": None,
        "target2": 12.3,
    },
    {
        "id": 2,
        "some_data": "haf",
        "column1": 1.0595815339757726,
        "column2": 1.3563722949064094,
        "target1": 9.2,
        "target2": 16.1,
    },
    {
        "id": 3,
        "some_data": "mhi",
        "column1": 0.8524166681302742,
        "column2": 4.044653479214844,
        "target1": None,
        "target2": 14.8,
    },
    {
        "id": 4,
        "some_data": "bob",
        "column1": 1.3372796035927237,
        "column2": 13.948877925620359,
        "target1": 61.1,
        "target2": None,
    },
    {
        "id": 5,
        "some_data": "sgk",
        "column1": 3.9462004453154202,
        "column2": 0.0013412754490804701,
        "target1": None,
        "target2": 5.7,
    },
    {
        "id": 6,
        "some_data": "uji",
        "column1": 3.728062059772931,
        "column2": 8.24038376348465,
        "target1": 46.3,
        "target2": None,
    },
    {
        "id": 7,
        "some_data": "zjm",
        "column1": 2.7467034428924246,
        "column2": 16.356472987007894,
        "target1": None,
        "target2": 53.2,
    },
    {
        "id": 8,
        "some_data": "adm",
        "column1": 3.115037163306382,
        "column2": 9.162622401164658,
        "target1": 44.1,
        "target2": 37.1,
    },
    {
        "id": 9,
        "some_data": "coe",
        "column1": 1.7355224480249831,
        "column2": 4.865495734864124,
        "target1": None,
        "target2": None,
    },
]

WRONG_DATA1 = [
    {
        "id": 0,
        "some_data": "twc",
        "feature1": 4.282563167045816,
        "feature2": 14.991974662600391,
        "target3": 72.3,
        "target4": 18.7,
    },
    {
        "id": 1,
        "some_data": "gau",
        "feature1": 0.664230851848101,
        "feature2": 13.491249197604272,
        "target3": None,
        "target4": 12.3,
    },
    {
        "id": 2,
        "some_data": "haf",
        "feature1": 1.0595815339757726,
        "feature2": 1.3563722949064094,
        "target3": 9.2,
        "target4": 16.1,
    },
    {
        "id": 3,
        "some_data": "mhi",
        "feature1": 0.8524166681302742,
        "feature2": 4.044653479214844,
        "target3": None,
        "target4": 14.8,
    },
    {
        "id": 4,
        "some_data": "bob",
        "feature1": 1.3372796035927237,
        "feature2": 13.948877925620359,
        "target3": 61.1,
        "target4": None,
    },
    {
        "id": 5,
        "some_data": "sgk",
        "feature1": 3.9462004453154202,
        "feature2": 0.0013412754490804701,
        "target3": None,
        "target4": 5.7,
    },
    {
        "id": 6,
        "some_data": "uji",
        "feature1": 3.728062059772931,
        "feature2": 8.24038376348465,
        "target3": 46.3,
        "target4": None,
    },
    {
        "id": 7,
        "some_data": "zjm",
        "feature1": 2.7467034428924246,
        "feature2": 16.356472987007894,
        "target3": None,
        "target4": 53.2,
    },
    {
        "id": 8,
        "some_data": "adm",
        "feature1": 3.115037163306382,
        "feature2": 9.162622401164658,
        "target3": 44.1,
        "target4": 37.1,
    },
    {
        "id": 9,
        "some_data": "coe",
        "feature1": 1.7355224480249831,
        "feature2": 4.865495734864124,
        "target3": None,
        "target4": None,
    },
]

WRONG_DATA2 = [
    {
        "id": 0,
        "some_data": "twc",
        "feature1": 4.282563167045816,
        "feature2": 14.991974662600391,
        "target1": 72.3,
        "target2": 18.7,
    },
    {
        "id": 1,
        "some_data": "gau",
        "feature1": 0.664230851848101,
        "feature2": 13.491249197604272,
        "target1": None,
        "target2": 12.3,
    },
    {
        "id": 2,
        "some_data": "haf",
        "feature1": 1.0595815339757726,
        "feature2": 1.3563722949064094,
        "target1": 9.2,
        "target2": 16.1,
    },
    {
        "id": 3,
        "some_data": "mhi",
        "feature1": 0.8524166681302742,
        "feature2": 4.044653479214844,
        "target1": None,
        "target2": 14.8,
    },
    {
        "id": 4,
        "some_data": "bob",
        "feature1": 1.3372796035927237,
        "feature2": 13.948877925620359,
        "target1": 61.1,
        "target2": None,
    },
    {
        "id": 5,
        "some_data": "sgk",
        "feature1": 3.9462004453154202,
        "feature2": 0.0013412754490804701,
        "target1": None,
        "target2": 5.7,
    },
    {
        "id": 6,
        "some_data": "uji",
        "feature1": 3.728062059772931,
        "feature2": 8.24038376348465,
        "target1": 46.3,
        "target2": None,
    },
    {
        "id": 7,
        "some_data": "zjm",
        "feature1": 2.7467034428924246,
        "feature2": 16.356472987007894,
        "target1": None,
        "target2": 53.2,
    },
    {
        "id": 8,
        "some_data": "adm",
        "feature1": 3.115037163306382,
        "feature2": 9.162622401164658,
        "target1": 44.1,
        "target2": 37.1,
    },
    {
        "id": 9,
        "some_data": "coe",
        "feature1": 1.7355224480249831,
        "feature2": 4.865495734864124,
        "target1": None,
        "target2": None,
    },
]


class TestModelPrediction(unittest.TestCase):

    def setUp(self):
        with open(
            "projects/_03_independent_cqc/_06_estimate_filled_posts/tests/model_for_test1.pkl",
            "rb",
        ) as f:
            self.model1 = pickle.load(f)
        with open(
            "projects/_03_independent_cqc/_06_estimate_filled_posts/tests/model_for_test2.pkl",
            "rb",
        ) as f:
            self.model2 = pickle.load(f)
        self.standard_data = pl.DataFrame(DATA)
        self.wrong_data1 = pl.DataFrame(WRONG_DATA1)
        self.wrong_data2 = pl.DataFrame(WRONG_DATA2)

    def test_predict_returns_dataframe_with_same_schema_as_source(self):
        result = job.predict(self.standard_data, self.model1)
        self.assertIsInstance(result, pl.DataFrame)
        self.assertEqual(result.shape, (len(DATA), 6))
        self.assertListEqual(
            ["id", "some_data", "column1", "column2", "target1", "target2"],
            result.columns,
        )

    def test_predict_populates_single_target_column(self):
        result = job.predict(self.standard_data, self.model1)
        result_values = result["target1"].to_list()
        self.assertTrue(all([v != 0 for v in result_values]))
        self.assertTrue(all(v is not None for v in result_values))

    def test_predict_populates_multiple_columns(self):
        result = job.predict(self.standard_data, self.model2)
        result_values1 = result["target1"].to_list()
        self.assertTrue(all([v != 0 for v in result_values1]))
        self.assertTrue(all(v is not None for v in result_values1))
        result_values2 = result["target2"].to_list()
        self.assertTrue(all([v != 0 for v in result_values2]))
        self.assertTrue(all(v is not None for v in result_values2))

    def test_raises_exception_if_target_columns_not_present(self):
        with self.assertRaises(job.TargetsNotFound):
            job.predict(self.wrong_data1, self.model2)

    def test_raises_exception_if_feature_columns_not_present(self):
        with self.assertRaises(job.FeaturesNotFound):
            job.predict(self.wrong_data2, self.model2)
