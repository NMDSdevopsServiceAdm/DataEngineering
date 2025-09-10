FROM python:3.11-slim

ENV POLARS_TEMP_DIR=/tmp/polars

RUN mkdir -p /tmp/polars

WORKDIR /app

ENV PYTHONPATH=/app

COPY polars_utils /app/polars_utils
COPY projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/requirements.txt /app/requirements.txt
COPY utils/column_names utils/column_names/
COPY projects/_03_independent_cqc/_07_estimate_filled_posts_by_job_role/fargate/estimate_ind_cqc_filled_posts_by_job_role.py /app/estimate_ind_cqc_filled_posts_by_job_role.py

RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
