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
COPY projects/_04_direct_payment_recipients/fargate/utils /app/projects/_04_direct_payment_recipients/fargate/utils
COPY projects/_04_direct_payment_recipients/direct_payments_config.py /app/projects/_04_direct_payment_recipients/direct_payments_config.py
COPY polars_utils /app/polars_utils
COPY utils/file_utils.py /app/utils/file_utils.py
COPY utils/column_names /app/utils/column_names/
COPY utils/column_values /app/utils/column_values/
COPY utils/value_labels /app/utils/value_labels

# Copy python jobs to WORKDIR
COPY projects/_04_direct_payment_recipients/fargate/_01_merge.py /app/_01_merge.py
COPY projects/_04_direct_payment_recipients/fargate/_02_estimate.py /app/_02_estimate.py
COPY projects/_04_direct_payment_recipients/fargate/validate_02_estimate.py /app/validate_02_estimate.py
COPY projects/_04_direct_payment_recipients/fargate/_03_estimates_by_icb_area.py /app/_03_estimates_by_icb_area.py

ENTRYPOINT ["python"]
