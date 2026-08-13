import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone

import boto3
import polars as pl
import psutil


class RunDiagnostics:
    """Captures memory, thread, and Polars streaming-fallback evidence for a job run.

    Every sample/checkpoint is written immediately as its own small S3
    object rather than buffered locally, so evidence already captured
    survives even if the process is killed (e.g. by the OOM killer) before
    it can flush anything to disk or the container's own log stream.

    `POLARS_VERBOSE` must already be set to "1" in the process environment
    *before this process starts* (e.g. on the throwaway task's Fargate
    environment block) for streaming-fallback notices to actually appear in
    the stderr capture below. Setting it via `os.environ` inside `start()` is
    too late: Polars' Rust core reads the flag once, and by the time `start()`
    runs, `polars` is already imported (this module alone imports it at the
    top) - `start()` only warns if it looks unset, it can't fix it.

    Args:
        job_name (str): Name of the job being diagnosed, used to group this
            run's objects under a stable S3 prefix.
        data_bucket (str): The workspace's datasets bucket name (e.g.
            "sfc-<workspace>-datasets"), used to derive the
            pipeline-resources bucket diagnostics are written to.
        sample_interval_seconds (int): How often the background sampler
            snapshots memory/thread usage. Defaults to 10.
    """

    def __init__(
        self, job_name: str, data_bucket: str, sample_interval_seconds: int = 10
    ) -> None:
        self._bucket = data_bucket[:-8] + "pipeline-resources"
        run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
        self._prefix = f"diagnostics/{job_name}/{run_id}"
        self._sample_interval_seconds = sample_interval_seconds
        self._s3_client = boto3.client("s3")
        self._process = psutil.Process()
        self._stop_event = threading.Event()
        self._saved_stderr_fd: int | None = None
        self._sampler_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None

    @property
    def bucket(self) -> str:
        """The pipeline-resources bucket this run's diagnostics are written to."""
        return self._bucket

    @property
    def prefix(self) -> str:
        """The S3 key prefix this run's diagnostics are written under."""
        return self._prefix

    def start(self) -> "RunDiagnostics":
        """Starts the background memory sampler and the Polars verbose/stderr capture.

        Returns:
            RunDiagnostics: self, so this can be chained onto construction.
        """
        if os.environ.get("POLARS_VERBOSE") != "1":
            print(
                "WARNING: run_diagnostics - POLARS_VERBOSE is not set to '1' in "
                "this process's environment. Polars is already imported by this "
                "point, so setting it now would have no effect - set it before "
                "the process starts (e.g. on the task definition) or Polars "
                "streaming-fallback notices won't appear in the stderr capture."
            )
        self._start_stderr_capture()
        self._start_sampler()
        return self

    def stop(self) -> None:
        """Stops the background sampler and restores the original stderr fd."""
        self._stop_event.set()

        if self._saved_stderr_fd is not None:
            os.dup2(self._saved_stderr_fd, 2)
            os.close(self._saved_stderr_fd)
            self._saved_stderr_fd = None

        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=1)
        if self._sampler_thread is not None:
            self._sampler_thread.join(timeout=1)

    def checkpoint(self, stage_name: str, lf: pl.LazyFrame | None = None) -> None:
        """Records a single point-in-time diagnostic snapshot.

        Args:
            stage_name (str): Label identifying the pipeline stage this
                checkpoint marks, e.g. "before_postcode_join_step_1".
            lf (pl.LazyFrame | None): If given, its `.explain()` text plan is
                captured alongside the memory snapshot. This is cheap: explain()
                doesn't execute the query, it only prints the optimized plan.
        """
        payload = self._snapshot()
        payload["stage"] = stage_name
        if lf is not None:
            payload["explain"] = lf.explain()

        self._put_object(
            f"checkpoints/{stage_name}_{time.time_ns()}.json",
            json.dumps(payload, default=str).encode("utf-8"),
            "application/json",
        )

    def _snapshot(self) -> dict:
        memory_info = self._process.memory_info()
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "rss_bytes": memory_info.rss,
            "num_threads": self._process.num_threads(),
        }

    def _start_sampler(self) -> None:
        def sample_loop() -> None:
            while not self._stop_event.wait(self._sample_interval_seconds):
                self._put_object(
                    f"samples/{time.time_ns()}.json",
                    json.dumps(self._snapshot(), default=str).encode("utf-8"),
                    "application/json",
                )

        self._sampler_thread = threading.Thread(target=sample_loop, daemon=True)
        self._sampler_thread.start()

    def _start_stderr_capture(self) -> None:
        # Polars' engine writes its POLARS_VERBOSE diagnostics (including
        # streaming-fallback notices) via Rust directly to the OS-level stderr
        # file descriptor, bypassing sys.stderr - so capturing it needs an
        # fd-level redirect, not a Python-level one.
        read_fd, write_fd = os.pipe()
        self._saved_stderr_fd = os.dup(2)
        os.dup2(write_fd, 2)
        os.close(write_fd)

        def tee_loop() -> None:
            with os.fdopen(read_fd) as piped_stderr:
                for line in piped_stderr:
                    os.write(self._saved_stderr_fd, line.encode())
                    self._put_object(
                        f"stderr/{time.time_ns()}.log",
                        line.encode("utf-8"),
                        "text/plain",
                    )

        self._stderr_thread = threading.Thread(target=tee_loop, daemon=True)
        self._stderr_thread.start()

    def _put_object(self, key_suffix: str, body: bytes, content_type: str) -> None:
        try:
            self._s3_client.put_object(
                Bucket=self._bucket,
                Key=f"{self._prefix}/{key_suffix}",
                Body=body,
                ContentType=content_type,
            )
        except Exception as error:
            print(f"WARNING: run_diagnostics failed to write {key_suffix}: {error}")
