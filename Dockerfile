FROM python:3.12-slim

WORKDIR /app

# librdkafka is required by confluent-kafka (audit log collector)
RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "scheduler.main"]
