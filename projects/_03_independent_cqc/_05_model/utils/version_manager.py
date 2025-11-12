import io
import json
import os
import pickle
from enum import Enum
from typing import Literal

import boto3
from botocore.exceptions import ClientError

from projects._03_independent_cqc._05_model.utils.model import Model

REGION = os.environ.get("AWS_REGION", "eu-west-2")


class EnumChangeType(Enum):
    MAJOR = 1
    MINOR = 2
    PATCH = 3


ChangeType = Literal[EnumChangeType.MAJOR, EnumChangeType.MINOR, EnumChangeType.PATCH]


class ModelVersionManager:
    """
    Manages semantic versioning for machine learning models using AWS Systems Manager
    Parameter Store.
    """

    def __init__(self, s3_bucket, s3_prefix, param_store_name, default_patch=False):
        if param_store_name[0] != "/":
            print(
                "Parameter store name must be fully-qualified, including leading slash, e.g. /my/model/version"
            )
            raise ValueError("Parameter store name must be fully-qualified")
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.ssm_client = boto3.client("ssm", region_name=REGION)
        self.s3_client = boto3.client("s3", region_name=REGION)
        self.param_store_name = param_store_name
        self.default_patch = default_patch
        self.current_version = None
        self.storage_location_uri = None

    def get_current_version(self) -> str:
        """
        Retrieves the current model version from Parameter Store.

        Returns:
            str: The current version string (e.g., "1.2.3").

        Raises:
            ClientError: If there is an error while connecting to the AWS API
        """
        try:
            response = self.ssm_client.get_parameter(
                Name=self.param_store_name, WithDecryption=False
            )
            raw_value = json.loads(response["Parameter"]["Value"])
            return raw_value["Current Version"]
        except ClientError as e:
            print(f"ERROR: Boto3 Error while retrieving parameter: {e}")
            raise

    def update_parameter_store(self, new_version: str) -> None:
        """
        Updates the version number in Parameter Store.

        Args:
            new_version (str): The new version string.

        Raises:
            Exception: If there is an error while connecting to the AWS API
        """
        try:
            param_dict = {"Current Version": new_version}
            param_str = json.dumps(param_dict)
            self.ssm_client.put_parameter(
                Name=self.param_store_name,
                Value=param_str,
                Type="String",
                Overwrite=True,
            )
            print(
                f"Successfully updated Parameter Store with new version: {new_version}"
            )
        except Exception as e:
            print(f"ERROR: Error updating Parameter Store: {e}")
            raise

    def increment_version(self, current_version: str, change_type: ChangeType) -> str:
        """
        Increments the version number based on the change type.

        Args:
            current_version (str): The current version string.
            change_type (ChangeType): 'MAJOR', 'MINOR', or 'PATCH'.

        Returns:
            str: The new version string.

        Raises:
            ValueError: If the change type is invalid.
        """
        parts = [int(p) for p in current_version.split(".")]
        if change_type == EnumChangeType.MAJOR:
            parts[0] += 1
            parts[1] = 0
            parts[2] = 0
        elif change_type == EnumChangeType.MINOR:
            parts[1] += 1
            parts[2] = 0
        elif change_type == EnumChangeType.PATCH:
            parts[2] += 1
        else:
            raise ValueError(
                "Invalid change type. Must be  '1'(major), 'minor', or 'patch'."
            )

        return ".".join(map(str, parts))

    def get_new_version(self, change_type: ChangeType) -> str:
        """
        Calculates and returns the new version number.

        Args:
            change_type (ChangeType): 'MAJOR', 'MINOR', or 'PATCH'.

        Returns:
            str: The new version string.

        Raises:
            ValueError: If the change type is invalid.
        """
        try:
            current_version = self.get_current_version()
            new_version = self.increment_version(current_version, change_type)
            return new_version
        except (self.ssm_client.exceptions.ParameterNotFound, ClientError):
            print(
                f"ERROR: Parameter '{self.param_store_name}' not found. Initializing to 0.1.0."
            )
            return "0.1.0"
        except ValueError as e:
            print(f"ERROR: Error getting new version: {e}")
            raise

    def save_model(self, model: Model, new_version: str) -> str:
        """
        Saves the trained model to S3 with the version number in the path.

        Args:
            model(Model): The trained model object to be saved.
            new_version (str): The new version string.

        Returns:
            str: The storage location of the new version.
        """
        prefix = f"{self.s3_prefix}/{new_version}/model.pkl"
        buffer = io.BytesIO()
        pickle.dump(model, buffer)
        buffer.seek(0)

        self.s3_client.upload_fileobj(buffer, self.s3_bucket, prefix)

        storage_location_uri = f"s3://{self.s3_bucket}/{prefix}"
        return storage_location_uri

    def prompt_change(self, prompt_num=0) -> ChangeType:
        """Prompts user for input to give version."""
        selection = input(
            "Is this a \n1. Major?\n2. Minor?\n3. Patch change?\n(1/2/3): "
        ).lower()
        if selection not in ["1", "2", "3"] and prompt_num == 0:
            print("ERROR: Invalid change type. Try again, choose 1, 2 or 3.")
            repeat_result = self.prompt_change(prompt_num=1)
            return repeat_result
        elif selection not in ["1", "2", "3"]:
            print("ERROR: Invalid change type. Model not saved.")
            raise ValueError("Invalid change type.")
        match selection:
            case "1":
                return EnumChangeType.MAJOR
            case "2":
                return EnumChangeType.MINOR
            case "3":
                return EnumChangeType.PATCH
            case _:
                raise ValueError("Invalid change type.")

    def prompt_and_save(self, model: Model) -> None:
        """
        Prompts the user for a change type and handles the versioning and saving process.

        Args:
            model (Model): The trained model object.
        """
        if self.default_patch:
            change_type = EnumChangeType.PATCH
        else:
            should_save = input(
                "Do you want to save this new model version? (only yes to save): "
            ).lower()
            if should_save != "yes":
                print("ERROR: Model not saved. Exiting.")
                return
            change_type = self.prompt_change()

        new_version = self.get_new_version(change_type)
        loc = self.save_model(model, new_version)
        model.set_version(new_version)
        self.storage_location_uri = loc
        print(f"Saved model to {loc}")
        self.update_parameter_store(new_version)
        self.current_version = new_version
