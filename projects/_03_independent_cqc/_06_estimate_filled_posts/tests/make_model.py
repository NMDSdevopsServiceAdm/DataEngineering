import pickle
import string
from random import choices, random

import polars as pl

from projects._03_independent_cqc._05_model.utils.model import Model, ModelType


def main():
    data = [
        {
            "id": n,
            "some_data": "".join(choices(string.ascii_lowercase, k=3)),
            "column1": 5 * random(),
            "column2": 20 * random(),
        }
        for n in range(10)
    ]

    [
        d.update(
            {
                "target1": 3 * d["column1"] + 4 * d["column2"] + (4 * random() - 2),
                "target2": 4 * d["column1"] + 5 * d["column2"] + (9 * random() - 2),
            }
        )
        for d in data
    ]

    df = pl.DataFrame(data)

    standard_model = Model(
        model_type=ModelType.SIMPLE_LINEAR.value,
        model_identifier="test_linear_model",
        model_params={},
        version_parameter_location="/some/location",
        processed_location="some/prefix",
        target_columns=["target1", "target2"],
        feature_columns=["column1", "column2"],
    )

    standard_model.fit(df)

    with open("model_for_test2.pkl", "wb") as f:
        pickle.dump(standard_model, f)


if __name__ == "__main__":
    main()
