FROM python:3.11

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

RUN mkdir -p /app/logs

RUN useradd -m -u 1001 processor && chown -R processor:processor /app
USER processor

ENV PROCESSING_INTERVAL=600
ENV BATCH_SIZE=1000

CMD ["python", "main.py"]

HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1