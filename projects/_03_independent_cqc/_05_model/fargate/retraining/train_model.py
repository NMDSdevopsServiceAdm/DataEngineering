import logging
import os
import sys

from polars.exceptions import PolarsError
from botocore.exceptions import ClientError

from projects._03_independent_cqc._05_model.model_registry import (
    model_definitions,
)
from projects._03_independent_cqc._05_model.utils.model import (
    Model,
    ModelNotTrainedError,
)
from polars_utils import utils
from projects._03_independent_cqc._05_model.utils.version_manager import (
    ModelVersionManager,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

TOPIC_ARN = os.environ.get("MODEL_RETRAIN_TOPIC_ARN", default="test_retrain_model")
S3_DATA_SOURCE = os.environ.get(
    "MODEL_RETRAIN_S3_SOURCE_BUCKET", default="test_raw_data_bucket"
)
MODEL_S3_BUCKET = os.environ.get("MODEL_S3_BUCKET", default="test_model_s3_bucket")
MODEL_S3_PREFIX = os.environ.get("MODEL_S3_PREFIX", default="test_model_s3_prefix")
ERROR_SUBJECT = "Model Retraining Failure"


def main(
    model_name: str, process_date_str: str, seed: int = None
) -> ModelVersionManager:
    """
    Executes model retraining for standard predefined model.

    Args:
        model_name (str): Name of model to retrain
        process_date_str (str): The datetime the training data was processed in format YYYYMMDDHHmmss
        seed (int): (Optional) Seed for test set generation

    Returns:
        ModelVersionManager: ModelVersionManager object with stored version details

    Raises:
        KeyError: If model doesn't exist
        ModelNotTrainedError: If model is not trained
        ValueError: If the model type is invalid
        TypeError: If the scikit-learn model failed to instantiate
        ClientError: If there is an error originating in AWS
        PolarsError: If there is an error originating in Polars
        Exception: If there is an unexplained error
    """

    def get_error_notification(model_id: str, error: str) -> str:
        return (
            f"The training job for model {model_id} FAILED."
            f"The error message was: {error}"
            f"See Cloudwatch Logs for details."
        )

    subject = None
    message = None
    try:
        model_definition = model_definitions[model_name]

        model = Model(**model_definition)

        logger.info(f"Training model {model_name}...")
        logger.info(f"Model: {model_definition}")

        data = model.get_raw_data(
            bucket_name=S3_DATA_SOURCE, process_date_str=process_date_str
        )
        logger.info(f"Raw Data: {data.collect().shape}")

        logger.info("Creating train and test datasets...")
        train_df, test_df = Model.create_train_and_test_datasets(data, seed=seed)

        logger.info(f"Fitting {str(model.model)}...")
        model.fit(train_df)

        logger.info("Validating model...")
        validation = model.validate(test_df)

        logger.info("Saving model and version...")
        version_manager = ModelVersionManager(
            s3_bucket=MODEL_S3_BUCKET,
            s3_prefix=f"{MODEL_S3_PREFIX}/{model_name}",
            param_store_name=model.version_parameter_location,
            default_patch=True,
        )

        version_manager.prompt_and_save(model=model)

        subject = f"Successful retraining of {model.model_identifier}."
        message = (
            f"The model {model.model_identifier} was trained successfully.\n"
            f"The R2 value in training was {model.training_score}\n"
            f"The R2 value in testing was {model.testing_score}\n"
            f"The difference is {validation}.\n"
            f"The model version is {version_manager.current_version}.\n"
            f"The serialised model is stored at {version_manager.storage_location_uri}."
        )
        return version_manager

    except KeyError as e:
        logger.error(e)
        logger.error(sys.argv)
        logger.error("Check that the model name is valid.")
        subject = ERROR_SUBJECT
        message = get_error_notification(model_name, str(e))
        raise
    except TypeError as e:
        logger.error(e)
        logger.error(sys.argv)
        logger.error(
            "It is likely the model failed to instantiate. Check the parameters."
        )
        subject = ERROR_SUBJECT
        message = get_error_notification(model_name, str(e))
        raise
    except ValueError as e:
        logger.error(e)
        logger.error(sys.argv)
        logger.error(
            "Check that you specified a valid model_type in your model definition."
        )
        logger.error(model_definitions[model_name])
        subject = ERROR_SUBJECT
        message = get_error_notification(model_name, str(e))
        raise
    except PolarsError as e:
        logger.error(e)
        logger.error(sys.argv)
        logger.error(
            "This error originated in Polars. Check that Polars is able to read from S3."
        )
        subject = ERROR_SUBJECT
        message = get_error_notification(model_name, str(e))
        raise
    except ModelNotTrainedError as e:
        logger.error(e)
        logger.error(sys.argv)
        subject = ERROR_SUBJECT
        message = get_error_notification(model_name, str(e))
        raise
    except ClientError as e:
        logger.error(e)
        logger.error(sys.argv)
        logger.error(
            "There was an error while serialising the model or saving the version parameter."
        )
        subject = ERROR_SUBJECT
        message = get_error_notification(model_name, str(e))
        raise
    except Exception as e:
        logger.error(e)
        subject = ERROR_SUBJECT
        message = get_error_notification(model_name, str(e))
        raise
    finally:
        if TOPIC_ARN == "test_retrain_model":
            logger.warning("Test topic only, no live topic set.")
        utils.send_sns_notification(
            subject=subject,
            message=message,
            topic_arn=TOPIC_ARN,
        )


if __name__ == "__main__":
    (model_id) = utils.collect_arguments(
        (
            "--model_name",
            "The name of the model to train",
        )
    )
    vm = main(model_name=model_id, process_date_str=process_datetime)
