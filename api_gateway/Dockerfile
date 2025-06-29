# Dockerfile para API Gateway
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
RUN useradd -m -u 1001 apigateway && chown -R apigateway:apigateway /app
USER apigateway

# Variables de entorno por defecto
ENV API_HOST=0.0.0.0
ENV API_PORT=8000
ENV WORKERS=4

# Exponer puerto
EXPOSE 8000

# Comando por defecto
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"]

# Healthcheck
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1