# Dockerfile para Visualization Module
FROM python:3.11

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

RUN mkdir -p /app/logs

RUN useradd -m -u 1001 visualization && chown -R visualization:visualization /app
USER visualization

ENV VISUALIZATION_MODE=full_sync
ENV FLASK_ENV=production

EXPOSE 3000

CMD ["python", "main.py"]

HEALTHCHECK --interval=60s --timeout=15s --start-period=60s --retries=3 \
     CMD curl -f http://localhost:9200/_cluster/health || python -c "
import requests
try:
    import os
    es_uri = os.getenv('ELASTICSEARCH_URI', 'http://elasticsearch:9200')
    r = requests.get(f'{es_uri}/_cluster/health', timeout=10)
    exit(0 if r.status_code == 200 else 1)
except:
    exit(1)
"