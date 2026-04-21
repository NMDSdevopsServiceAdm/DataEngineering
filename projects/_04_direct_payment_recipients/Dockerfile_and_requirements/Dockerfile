FROM python:3.11-slim

WORKDIR /app

RUN mkdir -p /polars_scratch && chmod 777 /polars_scratch
ENV POLARS_TEMP_DIR=/polars_scratch
ENV PYTHONPATH=/app

# Dependencies
COPY projects/_04_direct_payment_recipients/fargate/utils /app/projects/_04_direct_payment_recipients/fargate/utils
COPY projects/_04_direct_payment_recipients/direct_payments_column_names.py /app/projects/_04_direct_payment_recipients/direct_payments_column_names.py
COPY polars_utils /app/polars_utils

# Copy python jobs to WORKDIR
COPY projects/_04_direct_payment_recipients/fargate/estimate_direct_payments.py /app/estimate_direct_payments.py

COPY projects/_04_direct_payment_recipients/Dockerfile_and_requirements/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python"]
