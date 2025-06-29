-- Script Pig para análisis de eventos de tráfico
-- Genera métricas y agregaciones para la toma de decisiones
-- Autor: Sistema de Análisis de Tráfico RM
-- Fecha: 2025-06-29

-- Definir parámetros
%default INPUT_PATH '/traffic_data/filtered_events'
%default OUTPUT_BASE '/traffic_data/analysis'
%default MIN_EVENTS 5

-- Cargar eventos procesados
events = LOAD '$INPUT_PATH' USING PigStorage(',') AS (
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
    num_thumbs_up:int,
    quality_score:float,
    priority_level:chararray,
    event_hour:chararray
);

-- ========================================
-- 1. ANÁLISIS POR MUNICIPIO
-- ========================================

municipality_analysis = GROUP events BY municipality;
municipality_stats = FOREACH municipality_analysis GENERATE
    group as municipality,
    COUNT(events) as total_events,
    AVG(events.confidence) as avg_confidence,
    AVG(events.severity) as avg_severity,
    AVG(events.quality_score) as avg_quality,
    SUM(events.delay) as total_delay,
    MAX(events.severity) as max_severity,
    COUNT(FILTER events BY events.priority_level == 'HIGH') as high_priority_events;

-- Filtrar municipios con actividad significativa
active_municipalities = FILTER municipality_stats BY total_events >= $MIN_EVENTS;

-- Ordenar por número de eventos
municipality_ranked = ORDER active_municipalities BY total_events DESC;

STORE municipality_ranked INTO '$OUTPUT_BASE/municipality_analysis' USING PigStorage(',');

-- ========================================
-- 2. ANÁLISIS POR TIPO DE EVENTO
-- ========================================

event_type_analysis = GROUP events BY event_type;
event_type_stats = FOREACH event_type_analysis GENERATE
    group as event_type,
    COUNT(events) as total_events,
    AVG(events.confidence) as avg_confidence,
    AVG(events.severity) as avg_severity,
    AVG(events.delay) as avg_delay,
    COUNT(FILTER events BY events.priority_level == 'HIGH') as high_priority_count;

event_type_ranked = ORDER event_type_stats BY total_events DESC;

STORE event_type_ranked INTO '$OUTPUT_BASE/event_type_analysis' USING PigStorage(',');

-- ========================================
-- 3. ANÁLISIS TEMPORAL (POR HORA)
-- ========================================

hourly_analysis = GROUP events BY event_hour;
hourly_stats = FOREACH hourly_analysis GENERATE
    group as hour,
    COUNT(events) as total_events,
    AVG(events.severity) as avg_severity,
    AVG(events.delay) as avg_delay,
    COUNT(FILTER events BY events.event_type == 'jam') as jam_count,
    COUNT(FILTER events BY events.event_type == 'accident') as accident_count;

hourly_ordered = ORDER hourly_stats BY hour ASC;

STORE hourly_ordered INTO '$OUTPUT_BASE/hourly_analysis' USING PigStorage(',');

-- ========================================
-- 4. ANÁLISIS DE EVENTOS CRÍTICOS
-- ========================================

critical_events = FILTER events BY (
    severity >= 7 OR 
    (event_type == 'accident' AND confidence >= 7) OR
    priority_level == 'HIGH'
);

critical_by_municipality = GROUP critical_events BY municipality;
critical_stats = FOREACH critical_by_municipality GENERATE
    group as municipality,
    COUNT(critical_events) as critical_count,
    AVG(critical_events.severity) as avg_critical_severity,
    SUM(critical_events.delay) as total_critical_delay;

critical_ranked = ORDER critical_stats BY critical_count DESC;

STORE critical_ranked INTO '$OUTPUT_BASE/critical_events_analysis' USING PigStorage(',');

-- ========================================
-- 5. ANÁLISIS DE ATASCOS (JAMS)
-- ========================================

jam_events = FILTER events BY event_type == 'jam';

jam_analysis = FOREACH jam_events GENERATE
    municipality,
    street,
    delay,
    length,
    level,
    severity,
    quality_score;

jam_by_municipality = GROUP jam_analysis BY municipality;
jam_municipality_stats = FOREACH jam_by_municipality GENERATE
    group as municipality,
    COUNT(jam_analysis) as total_jams,
    AVG(jam_analysis.delay) as avg_delay,
    SUM(jam_analysis.length) as total_length,
    AVG(jam_analysis.level) as avg_level,
    MAX(jam_analysis.delay) as max_delay;

jam_municipality_ranked = ORDER jam_municipality_stats BY total_jams DESC;

STORE jam_municipality_ranked INTO '$OUTPUT_BASE/jam_analysis' USING PigStorage(',');

-- Análisis de atascos por calle
jam_by_street = GROUP jam_analysis BY (municipality, street);
jam_street_stats = FOREACH jam_by_street GENERATE
    FLATTEN(group) as (municipality, street),
    COUNT(jam_analysis) as jam_frequency,
    AVG(jam_analysis.delay) as avg_delay,
    AVG(jam_analysis.severity) as avg_severity;

-- Top calles con más atascos
jam_streets_filtered = FILTER jam_street_stats BY jam_frequency >= 3;
jam_streets_ranked = ORDER jam_streets_filtered BY jam_frequency DESC;
top_jam_streets = LIMIT jam_streets_ranked 20;

STORE top_jam_streets INTO '$OUTPUT_BASE/top_jam_streets' USING PigStorage(',');

-- ========================================
-- 6. ANÁLISIS DE CALIDAD DE DATOS
-- ========================================

quality_analysis = GROUP events BY source;
quality_stats = FOREACH quality_analysis GENERATE
    group as source,
    COUNT(events) as total_events,
    AVG(events.confidence) as avg_confidence,
    AVG(events.quality_score) as avg_quality,
    AVG(events.num_thumbs_up) as avg_thumbs_up,
    COUNT(FILTER events BY events.quality_score >= 0.7) as high_quality_events;

quality_ranked = ORDER quality_stats BY avg_quality DESC;

STORE quality_ranked INTO '$OUTPUT_BASE/data_quality_analysis' USING PigStorage(',');

-- ========================================
-- 7. HOTSPOTS GEOGRÁFICOS
-- ========================================

-- Crear grid geográfico (aproximadamente 1km x 1km)
events_with_grid = FOREACH events GENERATE
    *,
    FLOOR((latitude + 34.0) * 100) as lat_grid,
    FLOOR((longitude + 71.5) * 100) as lng_grid;

grid_analysis = GROUP events_with_grid BY (lat_grid, lng_grid);
grid_stats = FOREACH grid_analysis GENERATE
    FLATTEN(group) as (lat_grid, lng_grid),
    COUNT(events_with_grid) as event_density,
    AVG(events_with_grid.severity) as avg_severity,
    AVG(events_with_grid.confidence) as avg_confidence,
    COUNT(FILTER events_with_grid BY events_with_grid.priority_level == 'HIGH') as high_priority_density;

-- Filtrar zonas con densidad significativa
hotspots = FILTER grid_stats BY event_density >= $MIN_EVENTS;
hotspots_ranked = ORDER hotspots BY event_density DESC;
top_hotspots = LIMIT hotspots_ranked 50;

STORE top_hotspots INTO '$OUTPUT_BASE/geographic_hotspots' USING PigStorage(',');

-- ========================================
-- 8. PATRONES TEMPORALES AVANZADOS
-- ========================================

-- Extraer día de la semana y hora del timestamp
events_with_temporal = FOREACH events GENERATE
    *,
    SUBSTRING(timestamp, 0, 10) as event_date,
    SUBSTRING(timestamp, 11, 2) as event_hour;

-- Análisis por día
daily_analysis = GROUP events_with_temporal BY event_date;
daily_stats = FOREACH daily_analysis GENERATE
    group as date,
    COUNT(events_with_temporal) as daily_count,
    AVG(events_with_temporal.severity) as avg_daily_severity,
    COUNT(FILTER events_with_temporal BY events_with_temporal.event_type == 'jam') as daily_jams,
    COUNT(FILTER events_with_temporal BY events_with_temporal.event_type == 'accident') as daily_accidents;

daily_ordered = ORDER daily_stats BY date DESC;
recent_daily = LIMIT daily_ordered 30;  -- Últimos 30 días

STORE recent_daily INTO '$OUTPUT_BASE/daily_patterns' USING PigStorage(',');

-- ========================================
-- 9. CORRELACIONES EVENTO-MUNICIPIO
-- ========================================

event_municipality_matrix = FOREACH events GENERATE
    municipality,
    event_type,
    1 as count;

event_municipality_grouped = GROUP event_municipality_matrix BY (municipality, event_type);
event_municipality_counts = FOREACH event_municipality_grouped GENERATE
    FLATTEN(group) as (municipality, event_type),
    SUM(event_municipality_matrix.count) as total_count;

-- Filtrar combinaciones significativas
significant_combinations = FILTER event_municipality_counts BY total_count >= 3;
combinations_ranked = ORDER significant_combinations BY total_count DESC;

STORE combinations_ranked INTO '$OUTPUT_BASE/event_municipality_matrix' USING PigStorage(',');

-- ========================================
-- 10. RESUMEN EJECUTIVO
-- ========================================

total_events_summary = FOREACH (GROUP events ALL) GENERATE
    'total_events' as metric,
    COUNT(events) as value;

avg_severity_summary = FOREACH (GROUP events ALL) GENERATE
    'avg_severity' as metric,
    AVG(events.severity) as value;

high_priority_summary = FOREACH (GROUP (FILTER events BY priority_level == 'HIGH') ALL) GENERATE
    'high_priority_events' as metric,
    COUNT($1) as value;

top_municipality_summary = FOREACH (GROUP events BY municipality) GENERATE
    group as municipality,
    COUNT(events) as event_count;

top_municipality_ordered = ORDER top_municipality_summary BY event_count DESC;
top_municipality = LIMIT top_municipality_ordered 1;

executive_summary = UNION total_events_summary, avg_severity_summary, high_priority_summary;

STORE executive_summary INTO '$OUTPUT_BASE/executive_summary' USING PigStorage(',');
STORE top_municipality INTO '$OUTPUT_BASE/top_municipality' USING PigStorage(',');

-- ========================================
-- LOGS Y VERIFICACIONES
-- ========================================

-- Mostrar estadísticas clave en la consola
DUMP total_events_summary;
DUMP avg_severity_summary;
DUMP high_priority_summary;