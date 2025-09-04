import os
from model_registry import model_definitions
from model import Model
from utils.version_manager import ModelVersionManager


MODEL_IDENTIFIER = "non_res_pir"

model_definition = model_definitions[MODEL_IDENTIFIER]

model = Model(**model_definition)

data = model.get_raw_data(bucket_name=os.environ.get("BUCKET"))

train_df, test_df = Model.create_train_and_test_datasets(data)

fitted_model = model.fit(train_df)

validation = model.validate(test_df)

version_manager = ModelVersionManager(
    s3_bucket=os.environ.get("S3_BUCKET"),
    s3_prefix=os.environ.get("PREFIX"),
    param_store_name=model.version_parameter_location,
    default_patch=True,
)

version_manager.prompt_and_save(model=fitted_model)


if __name__ == "__main__":
    (branch_name, model_name, data_source) = utils.collect_arguments(
        (
            "--branch_name",
            "The name of the branch currently being used",
        ),
        (
            "--model_name",
            "The name of the model to train",
        ),
        (
            "--data_source",
            "The prefix of the data source to use",
        ),
    )
    main(branch_name, model_name, data_source)
