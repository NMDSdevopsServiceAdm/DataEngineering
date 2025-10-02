FROM python:3.11-slim

RUN mkdir -p /tmp/polars
WORKDIR /app

ENV POLARS_TEMP_DIR=/tmp/polars
ENV PYTHONPATH=/app

# Dependencies
COPY projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/utils projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/utils
COPY polars_utils polars_utils
COPY utils/column_names utils/column_names/
COPY projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/estimate_ind_cqc_filled_posts_by_job_role.py projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/estimate_ind_cqc_filled_posts_by_job_role.py

# Copy all python jobs to WORKDIR
COPY projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
