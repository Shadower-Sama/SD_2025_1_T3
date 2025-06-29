echo "Configurando directorio pig_scripts..."

# Crear estructura de directorios
mkdir -p logs
mkdir -p output
mkdir -p temp
mkdir -p udfs

# Crear archivo de configuración de Pig
cat > pig.properties << EOF
# Configuración de Apache Pig para Análisis de Tráfico
# Generado automáticamente

# Configuración de logging
pig.logfile=logs/pig.log
pig.log.level=INFO

# Configuración de Hadoop
mapreduce.job.reduces=4
mapreduce.map.memory.mb=2048
mapreduce.reduce.memory.mb=4096

# Configuración de almacenamiento
pig.temp.dir=temp/

# Configuración de UDFs
pig.additional.jars=udfs/

# Configuración específica para tráfico
traffic.rm.bounds.north=-33.0
traffic.rm.bounds.south=-34.0
traffic.rm.bounds.east=-70.0
traffic.rm.bounds.west=-71.5

# Configuración de timeouts
pig.exec.timeout=1800000
EOF

# Crear script de ejecución de pipeline completo
cat > run_full_pipeline.sh << 'EOF'
#!/bin/bash

# Pipeline completo de procesamiento Pig
# Ejecuta filtrado, análisis y agregaciones en secuencia

set -e

echo "Iniciando pipeline completo de Pig..."

# Variables de configuración
INPUT_RAW="/traffic_data/raw_events.csv"
OUTPUT_BASE="/traffic_data/pig_output_$(date +%Y%m%d_%H%M%S)"

# Crear directorios de salida
hdfs dfs -mkdir -p $OUTPUT_BASE

echo "1. Ejecutando filtrado y limpieza..."
pig -param INPUT_PATH=$INPUT_RAW \
    -param OUTPUT_PATH=$OUTPUT_BASE/filtered \
    -param STATS_PATH=$OUTPUT_BASE/filtering_stats \
    filtering.pig

echo "2. Ejecutando análisis de datos..."
pig -param INPUT_PATH=$OUTPUT_BASE/filtered \
    -param OUTPUT_BASE=$OUTPUT_BASE/analysis \
    analysis.pig

echo "3. Ejecutando agregaciones complejas..."
pig -param INPUT_PATH=$OUTPUT_BASE/filtered \
    -param OUTPUT_BASE=$OUTPUT_BASE/aggregations \
    aggregation.pig

echo "Pipeline completado exitosamente!"
echo "Resultados disponibles en: $OUTPUT_BASE"

# Mostrar resumen de archivos generados
echo "Archivos generados:"
hdfs dfs -ls -R $OUTPUT_BASE | grep -v "^d" | wc -l
EOF

chmod +x run_full_pipeline.sh

# Crear script de limpieza
cat > cleanup.sh << 'EOF'
#!/bin/bash

# Script de limpieza para pig_scripts

echo "Limpiando archivos temporales..."

# Limpiar logs antiguos (más de 7 días)
find logs/ -name "*.log" -mtime +7 -delete 2>/dev/null || true

# Limpiar archivos temporales
rm -rf temp/* 2>/dev/null || true

# Limpiar outputs antiguos en HDFS (más de 30 días)
hdfs dfs -ls /traffic_data/ | grep pig_output | while read line; do
    dir_date=$(echo $line | awk '{print $6}')
    if [[ "$dir_date" < "$(date -d '30 days ago' +%Y-%m-%d)" ]]; then
        dir_path=$(echo $line | awk '{print $8}')
        echo "Eliminando directorio antiguo: $dir_path"
        hdfs dfs -rm -r $dir_path
    fi
done

echo "Limpieza completada."
EOF

chmod +x cleanup.sh

# Crear archivo README para pig_scripts
cat > README.md << 'EOF'
# Scripts de Apache Pig para Análisis de Tráfico

Este directorio contiene los scripts de Apache Pig para el procesamiento distribuido de eventos de tráfico.

## Estructura de Archivos

```
pig_scripts/
├── filtering.pig           # Filtrado y limpieza de datos
├── analysis.pig           # Análisis básico de eventos
├── aggregation.pig        # Agregaciones complejas
├── run_full_pipeline.sh   # Ejecutar pipeline completo
├── cleanup.sh             # Limpiar archivos temporales
├── pig.properties         # Configuración de Pig
├── logs/                  # Logs de ejecución
├── output/                # Resultados locales
├── temp/                  # Archivos temporales
└── udfs/                  # User Defined Functions
```

## Scripts Principales

### 1. filtering.pig
- **Propósito**: Filtrado, limpieza y estandarización de eventos
- **Entrada**: Eventos raw desde MongoDB/HDFS
- **Salida**: Eventos filtrados y enriquecidos
- **Funciones**:
  - Validación de coordenadas RM
  - Eliminación de duplicados
  - Estandarización de campos
  - Cálculo de quality_score

### 2. analysis.pig
- **Propósito**: Análisis básico de eventos de tráfico
- **Entrada**: Eventos filtrados
- **Salida**: Métricas por municipio, tipo, tiempo
- **Análisis incluidos**:
  - Eventos por municipio
  - Análisis temporal (por hora)
  - Eventos críticos
  - Análisis de atascos
  - Hotspots geográficos

### 3. aggregation.pig
- **Propósito**: Agregaciones complejas y análisis avanzado
- **Entrada**: Eventos filtrados
- **Salida**: Métricas sofisticadas y KPIs
- **Análisis incluidos**:
  - Correlaciones evento-municipio
  - Análisis de propagación
  - Impacto económico estimado
  - Detección de anomalías
  - Dashboard ejecutivo

## Uso

### Ejecutar pipeline completo:
```bash
./run_full_pipeline.sh
```

### Ejecutar script individual:
```bash
# Filtrado
pig -param INPUT_PATH=/traffic_data/raw_events.csv \
    -param OUTPUT_PATH=/traffic_data/filtered \
    filtering.pig

# Análisis
pig -param INPUT_PATH=/traffic_data/filtered \
    -param OUTPUT_BASE=/traffic_data/analysis \
    analysis.pig

# Agregaciones
pig -param INPUT_PATH=/traffic_data/filtered \
    -param OUTPUT_BASE=/traffic_data/aggregations \
    aggregation.pig
```

### Parámetros configurables:
- `INPUT_PATH`: Ruta de datos de entrada
- `OUTPUT_PATH/OUTPUT_BASE`: Ruta base de salida
- `MIN_EVENTS`: Mínimo de eventos para análisis (default: 5)
- `LOOKBACK_DAYS`: Días hacia atrás para análisis (default: 30)

## Resultados Generados

### Filtrado:
- `filtered_events`: Eventos limpios y estandarizados
- `filtering_stats`: Estadísticas del proceso de filtrado

### Análisis:
- `municipality_analysis`: Métricas por municipio
- `event_type_analysis`: Análisis por tipo de evento
- `hourly_analysis`: Distribución temporal
- `critical_events_analysis`: Eventos de alta prioridad
- `geographic_hotspots`: Zonas de alta densidad

### Agregaciones:
- `event_municipality_correlations`: Matriz de correlaciones
- `high_risk_geographic_zones`: Zonas de alto riesgo
- `critical_traffic_corridors`: Corredores críticos
- `economic_impact_analysis`: Impacto económico estimado
- `executive_dashboard_kpis`: KPIs para dashboard

## Monitoreo

### Ver logs:
```bash
tail -f logs/pig.log
```

### Verificar resultados en HDFS:
```bash
hdfs dfs -ls /traffic_data/pig_output_*/
```

### Limpiar archivos antiguos:
```bash
./cleanup.sh
```

## Configuración

El archivo `pig.properties` contiene la configuración principal:
- Configuración de memoria para MapReduce
- Configuración de logging
- Parámetros específicos del dominio (límites RM)

## Troubleshooting

### Error común: Memoria insuficiente
```bash
# Aumentar memoria en pig.properties
mapreduce.map.memory.mb=4096
mapreduce.reduce.memory.mb=8192
```

### Error común: Archivo no encontrado
```bash
# Verificar que el archivo existe en HDFS
hdfs dfs -ls /traffic_data/
```

### Error común: Permisos
```bash
# Verificar permisos de HDFS
hdfs dfs -chmod 755 /traffic_data/
```

## Extensiones

### Agregar nuevo análisis:
1. Crear nueva sección en script correspondiente
2. Definir GROUP y FOREACH operations
3. Agregar STORE para guardar resultados
4. Documentar en README

### Crear UDF personalizada:
1. Compilar JAR y colocar en `udfs/`
2. Registrar en script: `REGISTER 'udfs/custom.jar'`
3. Usar en script: `custom_function(field)`

## Contacto

Para soporte con scripts Pig, revisar logs en `logs/` o consultar documentación de Apache Pig.
EOF

echo "Configuración de pig_scripts completada!"
echo ""
echo "Archivos creados:"
echo "  - filtering.pig"
echo "  - analysis.pig" 
echo "  - aggregation.pig"
echo "  - run_full_pipeline.sh"
echo "  - cleanup.sh"
echo "  - pig.properties"
echo "  - README.md"
echo ""
echo "Directorios creados:"
echo "  - logs/"
echo "  - output/"
echo "  - temp/"
echo "  - udfs/"
echo ""
echo "Para ejecutar el pipeline completo:"
echo "  ./run_full_pipeline.sh"