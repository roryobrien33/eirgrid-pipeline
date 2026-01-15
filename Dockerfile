FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    tzdata \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY src /app/src
COPY db /app/db
COPY docker/entrypoint.sh /app/entrypoint.sh

RUN chmod 755 /app/entrypoint.sh

ENV PYTHONPATH=/app/src

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["python", "-m", "pipeline.daily_forecast_runner", "--help"]
