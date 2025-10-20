FROM python:3.11-slim

RUN mkdir -p /tmp/polars
WORKDIR /app

ENV POLARS_TEMP_DIR=/tmp/polars
ENV PYTHONPATH=/app

# Dependencies
COPY projects/_03_independent_cqc/_01_merge/fargate/utils /app/projects/_03_independent_cqc/_01_merge/fargate/utils
COPY polars_utils /app/polars_utils
COPY utils/column_names /app/utils/column_names/
COPY utils/column_values /app/utils/column_values/
COPY utils/value_labels /app/utils/value_labels
COPY projects/_03_independent_cqc/_01_merge/fargate/prepare_job_role_counts.py /app/prepare_job_role_counts.py

# Copy all python jobs to WORKDIR
COPY projects/_03_independent_cqc/_01_merge/fargate/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
