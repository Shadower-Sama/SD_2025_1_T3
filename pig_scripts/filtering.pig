



-- Definir parámetros de entrada
%default INPUT_PATH '/traffic_data/raw_events.csv'
%default OUTPUT_PATH '/traffic_data/filtered_events'
%default STATS_PATH '/traffic_data/filtering_stats'

-- Cargar datos de entrada desde HDFS
raw_events = LOAD '$INPUT_PATH' USING PigStorage(',') AS (
    event_id:chararray,
    event_type:chararray,
    subtype:chararray,
    description:chararray,
    street:chararray,
    city:chararray,
    municipality:chararray,
    timestamp:chararray,
    confidence:int,
    severity:int,
    source:chararray,
    latitude:double,
    longitude:double,
    length:double,
    delay:double,
    speed:double,
    level:int,
    num_thumbs_up:int
);

-- Filtrar eventos válidos
-- 1. Campos obligatorios no nulos
-- 2. Coordenadas dentro de la Región Metropolitana
-- 3. Valores numéricos válidos
valid_events = FILTER raw_events BY (
    event_id IS NOT NULL AND
    event_type IS NOT NULL AND
    timestamp IS NOT NULL AND
    latitude >= -34.0 AND latitude <= -33.0 AND
    longitude >= -71.5 AND longitude <= -70.0 AND
    confidence >= 0 AND confidence <= 10 AND
    severity >= 0 AND severity <= 10 AND
    level >= 0 AND
    num_thumbs_up >= 0
);

-- Eliminar duplicados exactos basados en event_id
unique_events = DISTINCT valid_events;

-- Limpiar y estandarizar datos
standardized_events = FOREACH unique_events GENERATE
    event_id,
    LOWER(TRIM(event_type)) as event_type,
    LOWER(TRIM(subtype)) as subtype,
    SUBSTRING(TRIM(description), 0, 500) as description,
    SUBSTRING(TRIM(street), 0, 200) as street,
    SUBSTRING(TRIM(city), 0, 100) as city,
    UPPER(SUBSTRING(TRIM(municipality), 0, 1)) + LOWER(SUBSTRING(TRIM(municipality), 1, 100)) as municipality,
    timestamp,
    (confidence < 0 ? 0 : (confidence > 10 ? 10 : confidence)) as confidence,
    (severity < 0 ? 0 : (severity > 10 ? 10 : severity)) as severity,
    LOWER(TRIM(source)) as source,
    latitude,
    longitude,
    (length < 0.0 ? 0.0 : length) as length,
    (delay < 0.0 ? 0.0 : delay) as delay,
    (speed < 0.0 ? 0.0 : speed) as speed,
    (level < 0 ? 0 : level) as level,
    (num_thumbs_up < 0 ? 0 : num_thumbs_up) as num_thumbs_up;

-- Agregar campos calculados
enriched_events = FOREACH standardized_events GENERATE
    *,
    -- Calcular score de calidad básico
    (float)((confidence/10.0 * 0.4) + 
           ((description != '' ? 1.0 : 0.0) * 0.2) + 
           ((street != '' ? 1.0 : 0.0) * 0.2) + 
           ((num_thumbs_up > 0 ? 1.0 : 0.0) * 0.2)) as quality_score,
    
    -- Clasificar por prioridad
    (severity >= 7 ? 'HIGH' : 
     (severity >= 4 ? 'MEDIUM' : 'LOW')) as priority_level,
    
    -- Determinar categoría temporal
    SUBSTRING(timestamp, 11, 2) as event_hour;

-- Guardar eventos filtrados y enriquecidos
STORE enriched_events INTO '$OUTPUT_PATH' USING PigStorage(',');

-- Generar estadísticas de filtrado
raw_count = FOREACH (GROUP raw_events ALL) GENERATE 
    'raw_events' as metric,
    COUNT(raw_events) as count;

valid_count = FOREACH (GROUP valid_events ALL) GENERATE 
    'valid_events' as metric,
    COUNT(valid_events) as count;

unique_count = FOREACH (GROUP unique_events ALL) GENERATE 
    'unique_events' as metric,
    COUNT(unique_events) as count;

final_count = FOREACH (GROUP enriched_events ALL) GENERATE 
    'enriched_events' as metric,
    COUNT(enriched_events) as count;

-- Estadísticas por fuente
source_stats = FOREACH (GROUP enriched_events BY source) GENERATE
    CONCAT('source_', group) as metric,
    COUNT(enriched_events) as count;

-- Estadísticas por municipio
municipality_stats = FOREACH (GROUP enriched_events BY municipality) GENERATE
    CONCAT('municipality_', REPLACE(group, ' ', '_')) as metric,
    COUNT(enriched_events) as count;

-- Consolidar todas las estadísticas
all_stats = UNION raw_count, valid_count, unique_count, final_count, source_stats, municipality_stats;

-- Guardar estadísticas
STORE all_stats INTO '$STATS_PATH' USING PigStorage(',');

-- Mostrar resumen en consola
DUMP raw_count;
DUMP valid_count;
DUMP unique_count;
DUMP final_count;