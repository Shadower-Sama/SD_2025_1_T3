FROM python:3.11

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

RUN mkdir -p /app/logs

RUN useradd -m -u 1001 cache && chown -R cache:cache /app
USER cache

ENV CACHE_POLICY=LRU
ENV CACHE_SIZE=1000
ENV FLASK_ENV=production

EXPOSE 8080

CMD ["python", "main.py"]

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1