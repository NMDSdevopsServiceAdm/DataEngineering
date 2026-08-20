import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone

import boto3
import polars as pl
import psutil


def _sanitize_key_segment(value: str) -> str:
    """Replaces "/" so a caller-supplied name can't alter the intended S3 key structure."""
    return value.replace("/", "-")


class RunDiagnostics:
    """Captures memory, thread, and Polars streaming-fallback evidence for a job run.

    Every sample and checkpoint is written immediately as its own small S3
    object rather than buffered locally, so evidence already captured
    survives even if the process is killed (e.g. by the OOM killer) before
    it can flush anything to disk or the container's own log stream. Stderr
    lines are the one exception: they're batched and flushed periodically
    (see `stderr_flush_interval_seconds`) rather than one S3 write per line,
    since Polars' verbose output can arrive fast enough that a synchronous
    per-line S3 call would back up the underlying pipe and stall the very
    process being diagnosed.

    `POLARS_VERBOSE` must already be set to "1" in the process environment
    *before this process starts* (e.g. on the throwaway task's Fargate
    environment block) for streaming-fallback notices to actually appear in
    the stderr capture below. Setting it via `os.environ` inside `start()` is
    too late: Polars' Rust core reads the flag once, and by the time `start()`
    runs, `polars` is already imported (this module alone imports it at the
    top) - `start()` only warns if it looks unset, it can't fix it.

    Args:
        job_name (str): Name of the job being diagnosed, used to group this
            run's objects under a stable S3 prefix. Any "/" is replaced with
            "-" so it can't alter the intended key structure.
        data_bucket (str): The workspace's datasets bucket name (e.g.
            "sfc-<workspace>-datasets"), used to derive the
            pipeline-resources bucket diagnostics are written to.
        sample_interval_seconds (float): How often the background sampler
            snapshots memory/thread usage. Defaults to 10.
        stderr_flush_interval_seconds (float): How often buffered stderr
            lines are flushed to S3 as one batch. Defaults to 1.
    """

    def __init__(
        self,
        job_name: str,
        data_bucket: str,
        sample_interval_seconds: float = 10,
        stderr_flush_interval_seconds: float = 1,
    ) -> None:
        self._bucket = data_bucket[:-8] + "pipeline-resources"
        run_id = f"{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
        self._prefix = f"diagnostics/{_sanitize_key_segment(job_name)}/{run_id}"
        self._sample_interval_seconds = sample_interval_seconds
        self._stderr_flush_interval_seconds = stderr_flush_interval_seconds
        self._s3_client = boto3.client("s3")
        self._process = psutil.Process()
        self._stop_event = threading.Event()
        self._saved_stderr_fd: int | None = None
        self._sampler_thread: threading.Thread | None = None
        self._stderr_thread: threading.Thread | None = None
        self._stderr_flush_thread: threading.Thread | None = None
        self._stderr_buffer: list[str] = []
        self._stderr_buffer_lock = threading.Lock()

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

        # Redirect fd 2 away from the pipe first - this drops the pipe's last
        # writer reference, so the reader thread hits EOF and exits on its
        # own. Don't close/null _saved_stderr_fd until after that join: the
        # reader thread is still writing its line-echo to it right up until
        # EOF, and closing it out from under that write would crash the
        # thread mid-line instead of losing at most the trailing partial line.
        if self._saved_stderr_fd is not None:
            os.dup2(self._saved_stderr_fd, 2)

        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=1)

        if self._saved_stderr_fd is not None:
            os.close(self._saved_stderr_fd)
            self._saved_stderr_fd = None

        # The reader thread above has stopped appending by now, so this is
        # guaranteed to catch everything since the last periodic flush.
        self._flush_stderr_buffer()
        if self._stderr_flush_thread is not None:
            self._stderr_flush_thread.join(timeout=1)
        if self._sampler_thread is not None:
            self._sampler_thread.join(timeout=1)

    def checkpoint(self, stage_name: str, lf: pl.LazyFrame | None = None) -> None:
        """Records a single point-in-time diagnostic snapshot.

        Args:
            stage_name (str): Label identifying the pipeline stage this
                checkpoint marks, e.g. "before_postcode_join_step_1". Any "/"
                is replaced with "-" in the S3 key; the raw value is still
                recorded as-is in the payload's "stage" field.
            lf (pl.LazyFrame | None): If given, its `.explain()` text plan is
                captured alongside the memory snapshot. This is cheap: explain()
                doesn't execute the query, it only prints the optimized plan.
        """
        payload = self._snapshot()
        payload["stage"] = stage_name
        if lf is not None:
            payload["explain"] = lf.explain()

        self._put_object(
            f"checkpoints/{_sanitize_key_segment(stage_name)}_{time.time_ns()}.json",
            json.dumps(payload, default=str).encode("utf-8"),
            "application/json",
        )

    def _snapshot(self) -> dict:
        memory_info = self._process.memory_info()
        # Excludes RunDiagnostics' own background threads (sampler, stderr
        # tee, stderr flush) so this reflects only the diagnosed job's threads.
        own_threads = sum(
            1
            for t in (
                self._sampler_thread,
                self._stderr_thread,
                self._stderr_flush_thread,
            )
            if t is not None and t.is_alive()
        )
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "rss_bytes": memory_info.rss,
            "num_threads": self._process.num_threads() - own_threads,
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
            # errors="replace" so a stray non-UTF-8 byte (a native panic, a
            # C-extension warning) can't kill this thread and leave stderr
            # redirected into a pipe nobody's draining.
            with os.fdopen(read_fd, errors="replace") as piped_stderr:
                for line in piped_stderr:
                    os.write(self._saved_stderr_fd, line.encode())
                    with self._stderr_buffer_lock:
                        self._stderr_buffer.append(line)

        def flush_loop() -> None:
            while not self._stop_event.wait(self._stderr_flush_interval_seconds):
                self._flush_stderr_buffer()

        self._stderr_thread = threading.Thread(target=tee_loop, daemon=True)
        self._stderr_thread.start()
        self._stderr_flush_thread = threading.Thread(target=flush_loop, daemon=True)
        self._stderr_flush_thread.start()

    def _flush_stderr_buffer(self) -> None:
        with self._stderr_buffer_lock:
            if not self._stderr_buffer:
                return
            lines, self._stderr_buffer = self._stderr_buffer, []

        self._put_object(
            f"stderr/{time.time_ns()}.log",
            "".join(lines).encode("utf-8"),
            "text/plain",
        )

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
