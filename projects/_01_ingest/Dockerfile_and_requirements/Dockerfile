FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies. Installed before any source is copied so that a source-only
# change doesn't invalidate the pip layer.
COPY docker_requirements/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Shared libraries
COPY polars_utils polars_utils
COPY utils/file_utils.py utils/file_utils.py
COPY utils/column_names utils/column_names
COPY utils/column_values utils/column_values
COPY projects/_01_ingest/ascwds/fargate/utils projects/_01_ingest/ascwds/fargate/utils

# Copy python jobs to WORKDIR
COPY projects/_01_ingest/cqc_pir/fargate/*.py .
COPY projects/_01_ingest/ascwds/fargate/*.py .

ENTRYPOINT ["python"]
