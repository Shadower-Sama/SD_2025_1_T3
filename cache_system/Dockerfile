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
RUN useradd -m -u 1001 cache && chown -R cache:cache /app
USER cache

# Variables de entorno por defecto
ENV CACHE_POLICY=LRU
ENV CACHE_SIZE=1000
ENV FLASK_ENV=production

# Exponer puerto
EXPOSE 8080

# Comando por defecto
CMD ["python", "main.py"]

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1