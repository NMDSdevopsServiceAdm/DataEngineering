import boto3
import json
import os
import unittest

from botocore.stub import Stubber, ANY

from lambdas.error_notifications import error_notifications

EXAMPLE_GLUE_FAILURE_PAYLOAD = {
    "StateMachineId": "arn:aws:states:eu-west-2:1234523454:stateMachine:notifications-on-pipeline-fail-DataEngineeringPipeline",
    "ExecutionStartTime": "2022-07-25T10:07:59.147Z",
    "Error": '{"AllocatedCapacity":12,"Arguments":{"--destination":"s3://sfc-notifications-on-pipeline-fail-datasets/domain=data_engineering/dataset=locations_prepared/version=1.0.0/","--cqc_provider_source":"s3://sfc-notifications-on-pipeline-fail-datasets/domain=CQC/dataset=providers-api/","--pir_source":"s3://sfc-notifications-on-pipeline-fail-datasets/domain=CQC/dataset=pir/","--workplace_source":"s3://sfc-notifications-on-pipeline-fail-datasets/domain=ASCWDS/dataset=workplace/","--cqc_location_source":"s3://sfc-notifications-on-pipeline-fail-datasets/domain=CQC/dataset=locations-api/"},"Attempt":0,"CompletedOn":1658743757541,"DPUSeconds":240.0,"ErrorMessage":"AnalysisException: Path does not exist: s3://sfc-notifications-on-pipeline-fail-datasets/domain=ASCWDS/dataset=workplace","ExecutionTime":53,"GlueVersion":"3.0","Id":"jr_21e3085332b17768c24bf9d8c26378c4bf5f9a7c17ae6e6e2a5dbc6ff0fb36e4","JobName":"notifications-on-pipeline-fail-prepare_locations_job","JobRunState":"FAILED","LastModifiedOn":1658743757541,"LogGroupName":"/aws-glue/jobs","MaxCapacity":12.0,"NumberOfWorkers":6,"PredecessorRuns":[],"StartedOn":1658743679354,"Timeout":2880,"WorkerType":"G.2X"}',
    "StateMachineName": "notifications-on-pipeline-fail-DataEngineeringPipeline",
    "ExecutionId": "arn:aws:states:eu-west-2:1234523454:execution:notifications-on-pipeline-fail-DataEngineeringPipeline:680d6ef4-66af-eba4-8c4f-6549380eb9a5",
    "CallbackToken": "jfoieruhggiubergiube",
}

EXAMPLE_GENERIC_FAILURE_PAYLOAD = {
    "StateMachineId": "arn:aws:states:eu-west-2:1234523454:stateMachine:notifications-on-pipeline-fail-DataEngineeringPipeline",
    "ExecutionStartTime": "2022-07-25T10:07:59.147Z",
    "Error": "Crawler with name notifications-on-pipeline-fail-data_engineering_data_engineering has already started (Service: Glue, Status Code: 400, Request ID: e4f91543-dbca-49c9-8c52-fcd5db89efd9)",
    "StateMachineName": "notifications-on-pipeline-fail-DataEngineeringPipeline",
    "ExecutionId": "arn:aws:states:eu-west-2:1234523454:execution:notifications-on-pipeline-fail-DataEngineeringPipeline:680d6ef4-66af-eba4-8c4f-6549380eb9a5",
    "CallbackToken": "jfoieruhggiubergiube",
}


class ErrorNotifications(unittest.TestCase):
    def setUp(self) -> None:
        self.sns_client = boto3.client("sns")
        self.sns_stubber = Stubber(self.sns_client)
        self.sf_client = boto3.client("stepfunctions")
        self.sf_stubber = Stubber(self.sf_client)

    def test_sns_called_with_topic_arn(self):
        topic_arn = "arn:aws:sns:eu-west-2:1234523454:my-topic"
        os.environ["SNS_TOPIC_ARN"] = topic_arn
        self.mock_sns_publish(topic_arn=topic_arn)
        self.mock_task_success()

        error_notifications.main(
            EXAMPLE_GLUE_FAILURE_PAYLOAD, {}, self.sns_client, self.sf_client
        )
        self.sf_stubber.assert_no_pending_responses()

    def test_sns_correctly_formats_email_text_for_glue_job_errors(self):
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:eu-west-2:1234523454:my-topic"
        expected_message = (
            "Execution of the step function notifications-on-pipeline-fail-DataEngineeringPipeline has failed. "
            "The failure occured on glue job notifications-on-pipeline-fail-prepare_locations_job with error: \n\n"
            "AnalysisException: Path does not exist: s3://sfc-notifications-on-pipeline-fail-datasets/domain=ASCWDS/dataset=workplace \n\n"
            "View the run details of the glue job here: https://eu-west-2.console.aws.amazon.com/gluestudio/home?region=eu-west-2#/job/notifications-on-pipeline-fail-prepare_locations_job/run/jr_21e3085332b17768c24bf9d8c26378c4bf5f9a7c17ae6e6e2a5dbc6ff0fb36e4?from=monitoring \n\n"
            "View the execution details for the state function here: https://eu-west-2.console.aws.amazon.com/states/home?region=eu-west-2#/v2/executions/details/arn:aws:states:eu-west-2:1234523454:execution:notifications-on-pipeline-fail-DataEngineeringPipeline:680d6ef4-66af-eba4-8c4f-6549380eb9a5 \n"
        )
        self.mock_sns_publish(expected_message=expected_message)
        self.mock_task_success()

        error_notifications.main(
            EXAMPLE_GLUE_FAILURE_PAYLOAD, {}, self.sns_client, self.sf_client
        )
        self.sf_stubber.assert_no_pending_responses()

    def test_sns_correctly_formats_email_text_for_all_other_errors(self):
        os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:eu-west-2:1234523454:my-topic"
        expected_message = (
            "Execution of the step function notifications-on-pipeline-fail-DataEngineeringPipeline has failed woth error. \n\n"
            "Crawler with name notifications-on-pipeline-fail-data_engineering_data_engineering has already started (Service: Glue, Status Code: 400, Request ID: e4f91543-dbca-49c9-8c52-fcd5db89efd9) \n\n"
            "View the execution details for the state function here: https://eu-west-2.console.aws.amazon.com/states/home?region=eu-west-2#/v2/executions/details/arn:aws:states:eu-west-2:1234523454:execution:notifications-on-pipeline-fail-DataEngineeringPipeline:680d6ef4-66af-eba4-8c4f-6549380eb9a5 \n"
        )
        self.mock_sns_publish(expected_message=expected_message)
        self.mock_task_success()

        error_notifications.main(
            EXAMPLE_GENERIC_FAILURE_PAYLOAD, {}, self.sns_client, self.sf_client
        )
        self.sf_stubber.assert_no_pending_responses()

    def test_when_successfully_published_calls_successful_callback(self):
        self.mock_sns_publish()

        self.mock_task_success(
            token=EXAMPLE_GLUE_FAILURE_PAYLOAD["CallbackToken"],
            output=json.dumps({"MessageId": "463rtgygi3"}),
        )

        error_notifications.main(
            EXAMPLE_GLUE_FAILURE_PAYLOAD, {}, self.sns_client, self.sf_client
        )

        self.sf_stubber.assert_no_pending_responses()

    def test_when_it_errors_calls_failure_callback(self):
        self.sns_stubber.add_client_error(
            "publish",
            service_error_code="NotFoundException",
            service_message="SNS topic could not be found",
            expected_params={"TopicArn": ANY, "Subject": ANY, "Message": ANY},
        )
        self.sns_stubber.activate()

        expected_params = {
            "taskToken": EXAMPLE_GLUE_FAILURE_PAYLOAD["CallbackToken"],
            "error": "<class 'botocore.exceptions.ClientError'>",
            "cause": "An error occurred (NotFoundException) when calling the Publish operation: SNS topic could not be found",
        }
        self.sf_stubber.add_response("send_task_failure", {}, expected_params)
        self.sf_stubber.activate()

        error_notifications.main(
            EXAMPLE_GLUE_FAILURE_PAYLOAD, {}, self.sns_client, self.sf_client
        )

        self.sf_stubber.assert_no_pending_responses()

    def mock_task_success(self, token=ANY, output=ANY):
        self.sf_stubber.add_response(
            "send_task_success", {}, {"taskToken": token, "output": output}
        )
        self.sf_stubber.activate()

    def mock_sns_publish(
        self, MessageIdResponse="463rtgygi3", topic_arn=ANY, expected_message=ANY
    ):
        exepcted_params = {
            "TopicArn": topic_arn,
            "Subject": ANY,
            "Message": expected_message,
        }
        self.sns_stubber.add_response(
            "publish", {"MessageId": MessageIdResponse}, exepcted_params
        )
        self.sns_stubber.activate()
