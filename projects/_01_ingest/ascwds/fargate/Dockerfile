FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies
COPY polars_utils polars_utils
COPY projects/_01_ingest/ascwds/fargate/utils projects/_01_ingest/ascwds/fargate/utils
COPY utils utils
COPY projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/utils /app/projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/utils

# Copy all python jobs to WORKDIR
COPY projects/_01_ingest/ascwds/fargate/*.py .

COPY projects/_01_ingest/ascwds/fargate/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
