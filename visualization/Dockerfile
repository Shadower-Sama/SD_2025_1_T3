# Dockerfile para Visualization Module
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
RUN useradd -m -u 1001 visualization && chown -R visualization:visualization /app
USER visualization

# Variables de entorno por defecto
ENV VISUALIZATION_MODE=full_sync
ENV FLASK_ENV=production

# Exponer puerto
EXPOSE 3000

# Comando por defecto
CMD ["python", "main.py"]

# Healthcheck
HEALTHCHECK --interval=60s --timeout=15s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:9200/_cluster/health || python -c "
