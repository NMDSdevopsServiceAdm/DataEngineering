import json
import os
from enum import Enum
from json.decoder import JSONDecodeError
from typing import Any

import boto3


class JobType(Enum):
    ECS = "ECS"
    GLUE = "Glue"

    @classmethod
    def from_error(cls, error):
        if "StoppedReason" in error:
            return cls.ECS
        elif "ErrorMessage" in error:
            return cls.GLUE
        else:
            return None


def ecs_job_failure_message(
    statemachine_name: str, execution_details_url: str, error_json: dict
) -> str:
    """Generates a readable failure message from an ECS error.

    Args:
        statemachine_name (str): the name of the calling state machine
        execution_details_url (str): the URL of the state machine execution
        error_json (dict): the error from a JSON

    Returns:
        str: a failure message with the full details of the failure
    """
    cluster = error_json["ClusterArn"].split("/")[-1]
    task_id = error_json["TaskArn"].split("/")[-1]
    container = error_json["Containers"][0]["Name"]
    command = " ".join(error_json["Overrides"]["ContainerOverrides"][0]["Command"])
    monitoring_url = f"https://eu-west-2.console.aws.amazon.com/ecs/v2/clusters/{cluster}/tasks/{task_id}/configuration?selectedContainer={container}"

    return (
        f"Execution of the step function {statemachine_name} has failed. "
        f"The failure occured on ECS task {task_id} in cluster {cluster} with reason: \n\n"
        f"{error_json['StoppedReason']} \n\n"
        f"The ECS task was run with the following command: {command}\n\n"
        f"View the run details of the ECS task here: {monitoring_url} \n\n"
        "View the execution details for the state function here: "
        f"{execution_details_url} \n"
    )


def glue_job_failure_message(
    statemachine_name: str, execution_details_url: str, error_json: dict
) -> str:
    """Generates a readable failure message from a Glue error.

    Args:
        statemachine_name (str): the name of the calling state machine
        execution_details_url (str): the URL of the state machine execution
        error_json (dict): the error from a JSON

    Returns:
        str: a failure message with the full details of the failure
    """
    job_run_id = error_json["Id"]
    job_name = error_json["JobName"]
    monitoring_url = f"https://eu-west-2.console.aws.amazon.com/gluestudio/home?region=eu-west-2#/job/{job_name}/run/{job_run_id}?from=monitoring"

    return (
        f"Execution of the step function {statemachine_name} has failed. "
        f"The failure occured on glue job {job_name} with error: \n\n"
        f"{error_json['ErrorMessage']} \n\n"
        f"View the run details of the glue job here: {monitoring_url} \n\n"
        "View the execution details for the state function here: "
        f"{execution_details_url} \n"
    )


def generic_failure_message(
    statemachine_name: str, execution_details_url: str, error: str
) -> str:
    """Generates a generic failure message.

    Args:
        statemachine_name (str): the name of the calling state machine
        execution_details_url (str): the URL of the state machine execution
        error (str): the error as a raw string

    Returns:
        str: a failure message with the simple details of the failure
    """
    return (
        f"Execution of the step function {statemachine_name} has failed with error. \n\n"
        f"{error} \n\n"
        f"View the execution details for the state function here: {execution_details_url} \n"
    )


def send_sns_notification(
    sns_client: Any, sns_topic_arn: str, message_params: dict
) -> dict:
    """Sends an SNS message to a topic using a failure message.

    Args:
        sns_client (Any): the SNS client to use for publishing
        sns_topic_arn (str): the name of the SNS topic to which this should publish
        message_params (dict): the message parameters from the caller

    Raises:
        ValueError: in case of an unknown job type

    Returns:
        dict: the repsonse from the SNS Client
    """
    error = message_params["Error"]
    statemachine_name = message_params["StateMachineName"]
    execution_details_url = f"https://eu-west-2.console.aws.amazon.com/states/home?region=eu-west-2#/v2/executions/details/{message_params['ExecutionId']}"

    try:
        error_json = json.loads(error)
        job_type = JobType.from_error(error_json)
        if job_type == JobType.ECS:
            message = ecs_job_failure_message(
                statemachine_name, execution_details_url, error_json
            )
        elif job_type == JobType.GLUE:
            message = glue_job_failure_message(
                statemachine_name, execution_details_url, error_json
            )
        else:
            raise ValueError(f"Unknown job type: {job_type}")
    except (JSONDecodeError, ValueError):
        message = generic_failure_message(
            statemachine_name, execution_details_url, error
        )

    return sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message,
        Subject="Failed pipeline execution notification",
    )


def main(event, _, sns_client=None, sf_client=None):
    if not sns_client:
        sns_client = boto3.client("sns")
    if not sf_client:
        sf_client = boto3.client("stepfunctions")

    callback_token = event["CallbackToken"]
    sns_topic_arn = os.getenv("SNS_TOPIC_ARN")

    if not sns_topic_arn:
        raise AttributeError("SNS_TOPIC_ARN environment variable not set")

    try:
        sns_response = send_sns_notification(sns_client, sns_topic_arn, event)
        sf_client.send_task_success(
            taskToken=callback_token, output=json.dumps(sns_response)
        )
    except Exception as err:
        sf_client.send_task_failure(
            taskToken=callback_token, error=f"{type(err)}", cause=str(err)
        )
