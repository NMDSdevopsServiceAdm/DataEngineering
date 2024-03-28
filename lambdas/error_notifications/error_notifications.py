import json
import os

import boto3


class MessageClient:
    def send(self, message) -> object:
        NotImplementedError()


class Email(MessageClient):
    def __init__(self, client=None) -> None:
        if client:
            self.client = client
        else:
            self.client = boto3.client("sns")

        self.snsTopicArn = os.getenv("SNS_TOPIC_ARN")

    def send(self, message):
        response = self.client.publish(
            TopicArn=self.snsTopicArn,
            Message=message,
            Subject="Failed execution notification",
        )

        return response


class Teams(MessageClient):
    # example of a different client that can be added
    # all functionality that allows it to work is
    # abstracted into this class
    def __init__(self) -> None:
        self.client = "teams client"

    def send(self, message):
        NotImplementedError()


class BaseError:
    def __init__(self, error) -> None:
        self.message = error

    def send(self, client: MessageClient):
        return client.send(self.message)


class StateMachineError(BaseError):
    def __init__(self, error, execution_id, statemachine_name) -> None:
        execution_details_url = self.get_state_machine_execution_url(execution_id)
        self.message = self.__generic_failure_message(
            statemachine_name, error, execution_details_url
        )

    def __generic_failure_message(
        self, statemachine_name, error, execution_details_url
    ):
        message = (
            f"Execution of the step function {statemachine_name} has failed with error. \n\n"
            f"{error} \n\n"
            f"View the execution details for the state function here: {execution_details_url} \n"
        )
        return message

    def get_state_machine_execution_url(self, execution_id):
        return "https://eu-west-2.console.aws.amazon.com/states/home?region=eu-west-2#/v2/executions/details/{execution_id}".format(
            execution_id=execution_id
        )


class StateMachineGlueError(StateMachineError):
    def __init__(self, error, execution_id, statemachine_name) -> None:
        execution_details_url = super().get_state_machine_execution_url(execution_id)
        self.message = self.__glue_job_failure_message(
            statemachine_name, error, execution_details_url
        )

    def __glue_job_failure_message(
        self, statemachine_name, error, execution_details_url
    ):
        error_json = json.loads(error)
        message = error_json["ErrorMessage"]
        jr_id = error_json["Id"]
        jr_name = error_json["JobName"]
        glue_job_monitoring = self.__get_monitoring_url(jr_id, jr_name)

        email_message = (
            f"Execution of the step function {statemachine_name} has failed. "
            f"The failure occured on glue job {jr_name} with error: \n\n"
            f"{message} \n\n"
            f"View the run details of the glue job here: {glue_job_monitoring} \n\n"
            "View the execution details for the state function here: "
            f"{execution_details_url} \n"
        )
        return email_message

    def __get_monitoring_url(self, job_run_id, job_name):
        return "https://eu-west-2.console.aws.amazon.com/gluestudio/home?region=eu-west-2#/job/{job_name}/run/{job_run_id}?from=monitoring".format(
            job_name=job_name, job_run_id=job_run_id
        )


def success_callback(token, sns_response, sf_client):
    sf_client.send_task_success(taskToken=token, output=json.dumps(sns_response))


def failure_callback(token, error, cause, sf_client):
    sf_client.send_task_failure(taskToken=token, error=error, cause=cause)


def get_error_type(event):
    error = event["Error"]
    if "StateMachineName" in event:
        execution_id = event["ExecutionId"]
        statemachine_name = event["StateMachineName"]

        try:
            error_json = json.loads(event["Error"])
        except json.decoder.JSONDecodeError:
            error_json = {}

        if "JobName" in error_json:
            return StateMachineGlueError(error, execution_id, statemachine_name)
        else:
            return StateMachineError(error, execution_id, statemachine_name)

    return BaseError(error)


def get_message_client(event) -> MessageClient:
    # can look at the event and decide which client to use
    # new clients can be added easily as they only need to
    # implement the send method and inherit from MessageClient
    return Email()


def main(event, _, sns_client=None, sf_client=None):
    if not sf_client:
        sf_client = boto3.client("stepfunctions")

    send_callback = "CallbackToken" in event
    error = get_error_type(event)
    try:
        client = get_message_client(event)
        response = error.send(client)
        if send_callback:
            success_callback(event["CallbackToken"], response, sf_client)
    except Exception as err:
        if send_callback:
            failure_callback(
                event["CallbackToken"], f"{type(err)}", f"{err}", sf_client
            )
