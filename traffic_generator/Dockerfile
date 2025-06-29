FROM python:3.11-slim

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Configurar directorio de trabajo
WORKDIR /app

# Copiar requirements
COPY requirements.txt .

# Instalar dependencias Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY main.py .

# Crear directorio para logs
RUN mkdir -p /app/logs

# Configurar usuario no-root
RUN useradd -m -u 1001 generator && chown -R generator:generator /app
USER generator

# Variables de entorno por defecto
ENV TRAFFIC_MODE=comparative
ENV POISSON_LAMBDA=10
ENV EXPONENTIAL_LAMBDA=0.1
ENV TEST_DURATION_MINUTES=10

# Comando por defecto
CMD ["python", "main.py"]

# Healthcheck
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1