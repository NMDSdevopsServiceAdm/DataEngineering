import json
import os
import time

import boto3
import polars as pl
from moto import mock_aws

from polars_utils import run_diagnostics as job

JOB_NAME = "cqc_locations_4_full_clean"
DATASETS_BUCKET = "sfc-branch-datasets"
PIPELINE_RESOURCES_BUCKET = "sfc-branch-pipeline-resources"


def create_pipeline_resources_bucket() -> None:
    s3 = boto3.client("s3", region_name="eu-west-2")
    s3.create_bucket(
        Bucket=PIPELINE_RESOURCES_BUCKET,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


def read_json_object(bucket: str, key: str) -> dict:
    s3 = boto3.client("s3", region_name="eu-west-2")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body.decode("utf-8"))


class TestInit:
    def test_derives_pipeline_resources_bucket_from_datasets_bucket(self):
        diagnostics = job.RunDiagnostics(JOB_NAME, DATASETS_BUCKET)

        assert diagnostics.bucket == PIPELINE_RESOURCES_BUCKET

    def test_prefix_is_scoped_to_job_name(self):
        diagnostics = job.RunDiagnostics(JOB_NAME, DATASETS_BUCKET)

        assert diagnostics.prefix.startswith(f"diagnostics/{JOB_NAME}/")


class TestCheckpoint:
    @mock_aws
    def test_writes_memory_and_thread_snapshot_to_s3(self):
        create_pipeline_resources_bucket()
        diagnostics = job.RunDiagnostics(JOB_NAME, DATASETS_BUCKET)

        diagnostics.checkpoint("stage_one")

        s3 = boto3.client("s3", region_name="eu-west-2")
        objects = s3.list_objects_v2(
            Bucket=PIPELINE_RESOURCES_BUCKET,
            Prefix=f"{diagnostics.prefix}/checkpoints/",
        )
        keys = [obj["Key"] for obj in objects.get("Contents", [])]
        assert len(keys) == 1

        body = read_json_object(PIPELINE_RESOURCES_BUCKET, keys[0])
        assert body["stage"] == "stage_one"
        assert "timestamp" in body
        assert body["rss_bytes"] > 0
        assert body["num_threads"] > 0

    @mock_aws
    def test_includes_explain_plan_when_lazyframe_given(self):
        create_pipeline_resources_bucket()
        diagnostics = job.RunDiagnostics(JOB_NAME, DATASETS_BUCKET)
        lf = pl.LazyFrame({"a": [1, 2, 3]}).filter(pl.col("a") > 1)

        diagnostics.checkpoint("stage_with_plan", lf=lf)

        s3 = boto3.client("s3", region_name="eu-west-2")
        objects = s3.list_objects_v2(
            Bucket=PIPELINE_RESOURCES_BUCKET,
            Prefix=f"{diagnostics.prefix}/checkpoints/",
        )
        key = objects["Contents"][0]["Key"]
        body = read_json_object(PIPELINE_RESOURCES_BUCKET, key)

        assert "FILTER" in body["explain"]

    @mock_aws
    def test_omits_explain_when_no_lazyframe_given(self):
        create_pipeline_resources_bucket()
        diagnostics = job.RunDiagnostics(JOB_NAME, DATASETS_BUCKET)

        diagnostics.checkpoint("stage_without_plan")

        s3 = boto3.client("s3", region_name="eu-west-2")
        objects = s3.list_objects_v2(
            Bucket=PIPELINE_RESOURCES_BUCKET,
            Prefix=f"{diagnostics.prefix}/checkpoints/",
        )
        key = objects["Contents"][0]["Key"]
        body = read_json_object(PIPELINE_RESOURCES_BUCKET, key)

        assert "explain" not in body


class TestStartAndStop:
    @mock_aws
    def test_start_sets_polars_verbose_env_var(self):
        create_pipeline_resources_bucket()
        diagnostics = job.RunDiagnostics(
            JOB_NAME, DATASETS_BUCKET, sample_interval_seconds=0.05
        )

        diagnostics.start()
        try:
            assert os.environ["POLARS_VERBOSE"] == "1"
        finally:
            diagnostics.stop()

    @mock_aws
    def test_stop_leaves_no_daemon_threads_running(self):
        create_pipeline_resources_bucket()
        diagnostics = job.RunDiagnostics(
            JOB_NAME, DATASETS_BUCKET, sample_interval_seconds=0.05
        )
        diagnostics.start()

        diagnostics.stop()

        assert not diagnostics._sampler_thread.is_alive()
        assert not diagnostics._stderr_thread.is_alive()


class TestStderrCapture:
    @mock_aws
    def test_lines_written_to_stderr_are_forwarded_to_s3(self):
        create_pipeline_resources_bucket()
        diagnostics = job.RunDiagnostics(
            JOB_NAME, DATASETS_BUCKET, sample_interval_seconds=60
        )
        diagnostics.start()

        try:
            os.write(2, b"polars streaming fallback notice\n")
            time.sleep(0.2)

            s3 = boto3.client("s3", region_name="eu-west-2")
            objects = s3.list_objects_v2(
                Bucket=PIPELINE_RESOURCES_BUCKET,
                Prefix=f"{diagnostics.prefix}/stderr/",
            )
            keys = [obj["Key"] for obj in objects.get("Contents", [])]
            assert len(keys) == 1

            body = s3.get_object(Bucket=PIPELINE_RESOURCES_BUCKET, Key=keys[0])[
                "Body"
            ].read()
            assert b"polars streaming fallback notice" in body
        finally:
            diagnostics.stop()
