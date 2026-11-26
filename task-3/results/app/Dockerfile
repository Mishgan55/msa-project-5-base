FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY export_shipments.py .
RUN useradd --create-home --uid 10001 exporter && mkdir -p /exports && chown -R exporter:exporter /exports /app
USER 10001

ENTRYPOINT ["python", "/app/export_shipments.py"]
