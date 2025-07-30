FROM python:3.12-slim

ENV POLARS_TEMP_DIR=/tmp/polars

RUN mkdir -p /tmp/polars

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY delta_download_cqc_providers.py .

CMD ["python", "delta_download_cqc_providers.py", "--destination_prefix", "s3://spike-polars-data",
    "--start_timestamp", "2025-07-15:T01:30Z", "--end_timestamp", "2025-07-23:T01:30Z"]