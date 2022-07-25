import boto3
import os
import json


def get_monitoring_url(job_run_id, job_name):
    return "https://eu-west-2.console.aws.amazon.com/gluestudio/home?region=eu-west-2#/job/{job_name}/run/{job_run_id}?from=monitoring".format(
        job_name=job_name, job_run_id=job_run_id
    )


def get_state_machine_execution_url(execution_id):
    return "https://eu-west-2.console.aws.amazon.com/states/home?region=eu-west-2#/v2/executions/details/{execution_id}".format(
        execution_id=execution_id
    )


def glue_job_failure_message(statemachine_name, error, execution_details_url):
    error_json = json.loads(error)
    message = error_json["ErrorMessage"]
    jr_id = error_json["Id"]
    jr_name = error_json["JobName"]
    glue_job_monitoring = get_monitoring_url(jr_id, jr_name)

    email_message = (
        f"Execution of the step function {statemachine_name} has failed. "
        f"The failure occured on glue job {jr_name} with error: \n\n"
        f"{message} \n\n"
        f"View the run details of the glue job here: {glue_job_monitoring} \n\n"
        "View the execution details for the state function here: "
        f"{execution_details_url} \n"
    )
    return email_message


def generic_failure_message(statemachine_name, error, execution_details_url):
    message = (
        f"Execution of the step function {statemachine_name} has failed woth error. \n\n"
        f"{error} \n\n"
        f"View the execution details for the state function here: {execution_details_url} \n"
    )
    return message


def send_sns_notification(snsTopicArn, message_params, sns_client):
    error = message_params["Error"]
    execution_id = message_params["ExecutionId"]
    statemachine_name = message_params["StateMachineName"]
    execution_details_url = get_state_machine_execution_url(execution_id)

    try:
        message = glue_job_failure_message(
            statemachine_name, error, execution_details_url
        )
    except json.decoder.JSONDecodeError:
        message = generic_failure_message(
            statemachine_name, error, execution_details_url
        )

    response = sns_client.publish(
        TopicArn=snsTopicArn,
        Message=message,
        Subject="Failed pipeline execution notification",
    )

    return response


def success_callback(token, sns_resposne, sf_client):
    sf_client.send_task_success(taskToken=token, output=json.dumps(sns_resposne))


def failure_callback(token, error, cause, sf_client):
    sf_client.send_task_failure(taskToken=token, error=error, cause=cause)


def main(event, _, sns_client=None, sf_client=None):
    if not sns_client:
        sns_client = boto3.client("sns")
    if not sf_client:
        sf_client = boto3.client("stepfunctions")

    snsTopicArn = os.getenv("SNS_TOPIC_ARN")
    try:
        sns_response = send_sns_notification(snsTopicArn, event, sns_client)
        success_callback(event["CallbackToken"], sns_response, sf_client)
    except Exception as err:
        failure_callback(event["CallbackToken"], f"{type(err)}", f"{err}", sf_client)
