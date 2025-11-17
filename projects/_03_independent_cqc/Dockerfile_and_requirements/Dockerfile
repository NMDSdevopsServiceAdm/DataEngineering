FROM python:3.11-slim

RUN mkdir -p /tmp/polars
WORKDIR /app

ENV POLARS_TEMP_DIR=/tmp/polars
ENV PYTHONPATH=/app

# Dependencies
COPY projects/_03_independent_cqc/_01_merge/fargate/utils /app/projects/_03_independent_cqc/_01_merge/fargate/utils
COPY projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/utils /app/projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/utils
COPY polars_utils /app/polars_utils
COPY utils/column_names /app/utils/column_names/
COPY utils/column_values /app/utils/column_values/
COPY utils/value_labels /app/utils/value_labels

COPY projects/_03_independent_cqc/_01_merge/fargate/prepare_job_role_counts.py /app/prepare_job_role_counts.py
COPY projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/estimate_ind_cqc_filled_posts_by_job_role.py /app/estimate_ind_cqc_filled_posts_by_job_role.py

# Copy all python jobs to WORKDIR

COPY projects/_03_independent_cqc/Dockerfile_and_requirements/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
