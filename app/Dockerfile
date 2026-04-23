FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src \
    TZ=Asia/Tokyo

WORKDIR /app
COPY requirements.txt .

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && pip install -r requirements.txt \
    && playwright install-deps chromium \
    && rm -rf /var/lib/apt/lists/*

COPY src ./src
RUN useradd -m appuser && chown -R appuser:appuser /app
USER appuser

RUN playwright install chromium