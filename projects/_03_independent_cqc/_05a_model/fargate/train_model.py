import logging
import os
import sys

from polars.exceptions import PolarsError
from botocore.exceptions import ClientError

from projects._03_independent_cqc._05a_model.fargate.model_registry import (
    model_definitions,
)
from projects._03_independent_cqc._05a_model.utils.model import (
    Model,
    ModelNotTrainedError,
)
from polars_utils import utils
from projects._03_independent_cqc._05a_model.utils.version_manager import (
    ModelVersionManager,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

TOPIC_ARN = os.environ.get("MODEL_RETRAIN_TOPIC_ARN")
MODEL_S3_BUCKET = os.environ.get("MODEL_S3_BUCKET")
MODEL_S3_PREFIX = os.environ.get("MODEL_S3_PREFIX")

def main(model_name: str, raw_data_bucket: str) -> None:
    try:
        model_definition = model_definitions[model_name]

        model = Model(**model_definition)

        data = model.get_raw_data(bucket_name=raw_data_bucket)

        train_df, test_df = Model.create_train_and_test_datasets(data)

        fitted_model = model.fit(train_df)

        validation = model.validate(test_df)

        version_manager = ModelVersionManager(
            s3_bucket=MODEL_S3_BUCKET,
            s3_prefix=MODEL_S3_PREFIX,
            param_store_name=model.version_parameter_location,
            default_patch=True,
        )

        version_manager.prompt_and_save(model=model)

        scoring = {
            "train_score": model.training_score,
            "test_score": model.testing_score,
            "score_difference": validation,
        }

        subject = f'Successful retraining of {model.model_identifier}.'
        message = (
            f"The model {model.model_identifier} was trained successfully.\n"
            f"The R2 value in traing was {scoring['train_score']}\n"
            f"The R2 value in testing was {scoring['test_score']}\n"
            f"The difference is {scoring['score_difference']}.\n"
            f"The model version is {version_manager.current_version}.\n"
            f"The serialised model is stored at {version_manager.storage_location_uri}.")

    except KeyError as e:
        logger.error(e)
        logger.error(sys.argv)
        logger.error("Check that the model name is valid.")
        raise
    except TypeError as e:
        logger.error(e)
        logger.error(sys.argv)
        logger.error(
            "It is likely the model failed to instantiate. Check the parameters."
        )
        raise
    except ValueError as e:
        logger.error(e)
        logger.error(sys.argv)
        logger.error(
            "Check that you specified a valid model_type in your model definition."
        )
        logger.error(model_definitions[model_name])
        raise
    except PolarsError as e:
        logger.error(e)
        logger.error(sys.argv)
        logger.error(
            "This error originated in Polars. Check that Polars is able to read from S3."
        )
        raise
    except ModelNotTrainedError as e:
        logger.error(e)
        logger.error(sys.argv)
        raise
    except ClientError as e:
        logger.error(e)
        logger.error(sys.argv)
        logger.error(
            "There was an error while serialising the model or saving the version parameter."
        )
        raise
    except Exception as e:
        logger.error(e)
        raise
    finally:
        utils.send_sns_notification(
            subject=subject,
            message=message,
            topic_arn=TOPIC_ARN,
        )


if __name__ == "__main__":
    (model_id, data_source) = utils.collect_arguments(
        (
            "--model_name",
            "The name of the model to train",
        ),
        (
            "--data_source",
            "The prefix of the data source to use",
        ),
    )
    main(model_name=model_id, raw_data_bucket=data_source)
