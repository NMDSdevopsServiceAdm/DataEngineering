import threading
import time
from contextlib import contextmanager
from typing import Iterator

import polars as pl
import psutil


def _rss_mb() -> float:
    return psutil.Process().memory_info().rss / (1024 * 1024)


@contextmanager
def profile_step(label: str, interval_seconds: float = 5) -> Iterator[None]:
    """
    Logs elapsed time and periodic/peak RSS memory usage for a step in a job.

    Prints are explicitly flushed so recent samples reach CloudWatch even if the
    process is later SIGKILLed by an out-of-memory kill, which gives no chance
    for Python's normal buffered stdout to flush.

    Args:
        label (str): Identifies this step in the log output.
        interval_seconds (float): How often to log a periodic RSS sample while
            the step is running. Defaults to 5.

    Yields:
        None: Control returns to the wrapped `with` block.
    """
    start = time.monotonic()
    start_rss = _rss_mb()
    peak = start_rss
    stop_event = threading.Event()

    def _sample_loop() -> None:
        nonlocal peak
        while not stop_event.wait(interval_seconds):
            rss = _rss_mb()
            peak = max(peak, rss)
            elapsed = time.monotonic() - start
            print(
                f"[MEMORY] {label} t+{elapsed:.1f}s RSS={rss:.1f}MB",
                flush=True,
            )

    sampler = threading.Thread(target=_sample_loop, daemon=True)
    sampler.start()
    try:
        yield
    finally:
        stop_event.set()
        sampler.join(timeout=interval_seconds)
        end_rss = _rss_mb()
        peak = max(peak, end_rss)
        elapsed = time.monotonic() - start
        print(
            f"[MEMORY] {label} done: elapsed={elapsed:.2f}s "
            f"start={start_rss:.1f}MB end={end_rss:.1f}MB peak={peak:.1f}MB "
            f"delta={end_rss - start_rss:.1f}MB",
            flush=True,
        )


def checkpoint(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Forces immediate materialization of a LazyFrame, then re-wraps it as lazy.

    Temporary diagnostic aid: while the pipeline stays lazy, every step fuses
    into one execution at the terminal sink call, so `profile_step` around
    each step only measures the fused whole, not that step individually.
    Checkpointing after a step forces its execution to happen there and then,
    making `profile_step`'s timing/RSS attributable to that specific step.
    This intentionally breaks cross-step operator fusion/pushdown, so it
    should only be used for this memory-spike investigation, not left in
    production code.

    Args:
        lf (pl.LazyFrame): The LazyFrame to materialize.

    Returns:
        pl.LazyFrame: A new LazyFrame wrapping the materialized DataFrame.
    """
    return lf.collect().lazy()


def log_query_plan(label: str, lf: pl.LazyFrame) -> None:
    """
    Logs the optimized Polars query plan for a LazyFrame.

    Args:
        label (str): Identifies this plan in the log output.
        lf (pl.LazyFrame): The LazyFrame whose accumulated query plan to log.

    Returns:
        None: This function does not return any value.
    """
    print(f"[PLAN] {label}:\n{lf.explain(optimized=True)}", flush=True)
