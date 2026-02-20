FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
RUN pip install --no-cache-dir \
    opentelemetry-sdk==1.23.0 \
    opentelemetry-exporter-otlp-proto-grpc==1.23.0 \
    opentelemetry-api==1.23.0

COPY send_test_events.py .

CMD ["python", "-u", "send_test_events.py"]
