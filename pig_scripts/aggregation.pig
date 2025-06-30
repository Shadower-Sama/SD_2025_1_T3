

-- Definir parámetros
%default INPUT_PATH '/traffic_data/filtered_events'
%default OUTPUT_BASE '/traffic_data/aggregations'
%default LOOKBACK_DAYS 30
%default MIN_SAMPLE_SIZE 10

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
-- 1. MATRIZ DE CORRELACIÓN EVENTO-MUNICIPIO
-- ========================================

correlation_matrix = FOREACH events GENERATE
    municipality,
    event_type,
    severity,
    1 as occurrence;

correlation_grouped = GROUP correlation_matrix BY (municipality, event_type);
correlation_stats = FOREACH correlation_grouped GENERATE
    FLATTEN(group) as (municipality, event_type),
    COUNT(correlation_matrix) as frequency,
    AVG(correlation_matrix.severity) as avg_severity,
    SUM(correlation_matrix.occurrence) as total_occurrences;

-- Calcular intensidad relativa
municipality_totals = FOREACH (GROUP correlation_matrix BY municipality) GENERATE
    group as municipality,
    COUNT(correlation_matrix) as municipality_total;

event_type_totals = FOREACH (GROUP correlation_matrix BY event_type) GENERATE
    group as event_type,
    COUNT(correlation_matrix) as event_type_total;

-- Unir para calcular correlaciones
correlation_with_totals = JOIN correlation_stats BY municipality, municipality_totals BY municipality;
correlation_final = FOREACH correlation_with_totals GENERATE
    correlation_stats::municipality as municipality,
    correlation_stats::event_type as event_type,
    correlation_stats::frequency as frequency,
    correlation_stats::avg_severity as avg_severity,
    (float)correlation_stats::frequency / (float)municipality_totals::municipality_total as relative_frequency;

-- Filtrar correlaciones significativas
significant_correlations = FILTER correlation_final BY 
    frequency >= $MIN_SAMPLE_SIZE AND relative_frequency >= 0.1;

correlation_ranked = ORDER significant_correlations BY relative_frequency DESC;

STORE correlation_ranked INTO '$OUTPUT_BASE/event_municipality_correlations' USING PigStorage(',');

-- ========================================
-- 2. ANÁLISIS DE DENSIDAD GEOGRÁFICA AVANZADO
-- ========================================

-- Crear múltiples niveles de granularidad geográfica
events_with_grids = FOREACH events GENERATE
    *,
    -- Grid fino (~500m)
    FLOOR((latitude + 34.0) * 200) as fine_lat_grid,
    FLOOR((longitude + 71.5) * 200) as fine_lng_grid,
    -- Grid medio (~1km)
    FLOOR((latitude + 34.0) * 100) as medium_lat_grid,
    FLOOR((longitude + 71.5) * 100) as medium_lng_grid,
    -- Grid grueso (~2km)
    FLOOR((latitude + 34.0) * 50) as coarse_lat_grid,
    FLOOR((longitude + 71.5) * 50) as coarse_lng_grid;

-- Análisis de densidad a nivel medio
medium_grid_analysis = GROUP events_with_grids BY (medium_lat_grid, medium_lng_grid);
medium_density = FOREACH medium_grid_analysis GENERATE
    FLATTEN(group) as (lat_grid, lng_grid),
    COUNT(events_with_grids) as event_density,
    AVG(events_with_grids.severity) as avg_severity,
    AVG(events_with_grids.confidence) as avg_confidence,
    AVG(events_with_grids.quality_score) as avg_quality,
    COUNT(FILTER events_with_grids BY events_with_grids.event_type == 'accident') as accident_density,
    COUNT(FILTER events_with_grids BY events_with_grids.event_type == 'jam') as jam_density,
    SUM(events_with_grids.delay) as total_delay_impact;

-- Calcular score de riesgo compuesto
risk_scored_density = FOREACH medium_density GENERATE
    *,
    -- Score de riesgo: densidad * severidad * impacto en delays
    (float)(event_density * avg_severity * (total_delay_impact/1000.0)) as risk_score;

-- Filtrar y rankear zonas de alto riesgo
high_risk_zones = FILTER risk_scored_density BY 
    event_density >= $MIN_SAMPLE_SIZE AND avg_severity >= 3.0;

risk_zones_ranked = ORDER high_risk_zones BY risk_score DESC;
top_risk_zones = LIMIT risk_zones_ranked 25;

STORE top_risk_zones INTO '$OUTPUT_BASE/high_risk_geographic_zones' USING PigStorage(',');

-- ========================================
-- 3. ANÁLISIS TEMPORAL MULTIDIMENSIONAL
-- ========================================

-- Enriquecer eventos con información temporal
events_temporal_enriched = FOREACH events GENERATE
    *,
    SUBSTRING(timestamp, 0, 10) as event_date,
    SUBSTRING(timestamp, 11, 2) as event_hour,
    -- Calcular día de la semana (simplificado)
    (GetDay(ToDate(SUBSTRING(timestamp, 0, 10), 'yyyy-MM-dd')) == 1 ? 'Sunday' :
     (GetDay(ToDate(SUBSTRING(timestamp, 0, 10), 'yyyy-MM-dd')) == 2 ? 'Monday' :
      (GetDay(ToDate(SUBSTRING(timestamp, 0, 10), 'yyyy-MM-dd')) == 3 ? 'Tuesday' :
       (GetDay(ToDate(SUBSTRING(timestamp, 0, 10), 'yyyy-MM-dd')) == 4 ? 'Wednesday' :
        (GetDay(ToDate(SUBSTRING(timestamp, 0, 10), 'yyyy-MM-dd')) == 5 ? 'Thursday' :
         (GetDay(ToDate(SUBSTRING(timestamp, 0, 10), 'yyyy-MM-dd')) == 6 ? 'Friday' : 'Saturday')))))) as day_of_week,
    -- Categorizar períodos del día
    (event_hour >= '06' AND event_hour < '10' ? 'morning_rush' :
     (event_hour >= '10' AND event_hour < '16' ? 'midday' :
      (event_hour >= '16' AND event_hour < '20' ? 'evening_rush' :
       (event_hour >= '20' AND event_hour < '23' ? 'evening' : 'night')))) as time_period;

-- Análisis por período del día y día de la semana
temporal_pattern_analysis = GROUP events_temporal_enriched BY (day_of_week, time_period);
temporal_patterns = FOREACH temporal_pattern_analysis GENERATE
    FLATTEN(group) as (day_of_week, time_period),
    COUNT(events_temporal_enriched) as event_frequency,
    AVG(events_temporal_enriched.severity) as avg_severity,
    AVG(events_temporal_enriched.delay) as avg_delay,
    COUNT(FILTER events_temporal_enriched BY events_temporal_enriched.event_type == 'jam') as jam_frequency,
    COUNT(FILTER events_temporal_enriched BY events_temporal_enriched.event_type == 'accident') as accident_frequency;

temporal_patterns_ranked = ORDER temporal_patterns BY event_frequency DESC;

STORE temporal_patterns_ranked INTO '$OUTPUT_BASE/temporal_patterns_detailed' USING PigStorage(',');

-- ========================================
-- 4. ANÁLISIS DE SECUENCIAS DE EVENTOS
-- ========================================

-- Agrupar eventos por ubicación próxima y tiempo
events_for_sequences = FOREACH events_with_grids GENERATE
    event_id,
    event_type,
    municipality,
    street,
    timestamp,
    medium_lat_grid,
    medium_lng_grid,
    severity;

-- Agrupar por zona geográfica
location_sequences = GROUP events_for_sequences BY (medium_lat_grid, medium_lng_grid);
sequence_analysis = FOREACH location_sequences {
    sorted_events = ORDER events_for_sequences BY timestamp;
    GENERATE 
        FLATTEN(group) as (lat_grid, lng_grid),
        COUNT(events_for_sequences) as event_count,
        sorted_events as events_sequence;
}

-- Filtrar secuencias significativas
significant_sequences = FILTER sequence_analysis BY event_count >= 5;

-- Analizar patrones en secuencias
sequence_patterns = FOREACH significant_sequences GENERATE
    lat_grid,
    lng_grid,
    event_count,
    -- Extraer municipio más común
    FLATTEN(STREAM(events_sequence)) as (event_id, event_type, municipality, street, timestamp, lat_g, lng_g, severity);

sequence_municipality_summary = FOREACH (GROUP sequence_patterns BY (lat_grid, lng_grid, municipality)) GENERATE
    FLATTEN(group) as (lat_grid, lng_grid, municipality),
    COUNT(sequence_patterns) as municipality_events;

dominant_municipality_per_sequence = FOREACH (GROUP sequence_municipality_summary BY (lat_grid, lng_grid)) {
    sorted_by_count = ORDER sequence_municipality_summary BY municipality_events DESC;
    top_municipality = LIMIT sorted_by_count 1;
    GENERATE 
        group.lat_grid as lat_grid,
        group.lng_grid as lng_grid,
        FLATTEN(top_municipality.municipality) as dominant_municipality,
        SUM(sequence_municipality_summary.municipality_events) as total_sequence_events;
}

sequence_hotspots = ORDER dominant_municipality_per_sequence BY total_sequence_events DESC;

STORE sequence_hotspots INTO '$OUTPUT_BASE/event_sequence_hotspots' USING PigStorage(',');

-- ========================================
-- 5. ANÁLISIS DE EFECTIVIDAD DE RESPUESTA
-- ========================================

-- Calcular métricas de engagement y efectividad
effectiveness_analysis = GROUP events BY event_type;
effectiveness_metrics = FOREACH effectiveness_analysis GENERATE
    group as event_type,
    COUNT(events) as total_events,
    AVG(events.num_thumbs_up) as avg_engagement,
    AVG(events.confidence) as avg_confidence,
    -- Ratio de efectividad basado en engagement y confianza
    (float)(AVG(events.num_thumbs_up) * AVG(events.confidence)) / 100.0 as effectiveness_ratio,
    -- Distribución de severidad
    COUNT(FILTER events BY events.severity <= 3) as low_severity_count,
    COUNT(FILTER events BY events.severity > 3 AND events.severity <= 6) as medium_severity_count,
    COUNT(FILTER events BY events.severity > 6) as high_severity_count;

effectiveness_ranked = ORDER effectiveness_metrics BY effectiveness_ratio DESC;

STORE effectiveness_ranked INTO '$OUTPUT_BASE/response_effectiveness_analysis' USING PigStorage(',');

-- ========================================
-- 6. IDENTIFICACIÓN DE CORREDORES CRÍTICOS
-- ========================================

-- ========================================
-- 6. IDENTIFICACIÓN DE CORREDORES CRÍTICOS
-- ========================================

-- Analizar calles con mayor impacto
street_corridor_analysis = FOREACH events GENERATE
    municipality,
    street,
    event_type,
    severity,
    delay,
    length,
    quality_score;

street_impact_analysis = GROUP street_corridor_analysis BY (municipality, street);
street_metrics = FOREACH street_impact_analysis GENERATE
    FLATTEN(group) as (municipality, street),
    COUNT(street_corridor_analysis) as total_incidents,
    AVG(street_corridor_analysis.severity) as avg_severity,
    SUM(street_corridor_analysis.delay) as total_delay_impact,
    AVG(street_corridor_analysis.length) as avg_incident_length,
    COUNT(FILTER street_corridor_analysis BY street_corridor_analysis.event_type == 'jam') as jam_incidents,
    COUNT(FILTER street_corridor_analysis BY street_corridor_analysis.event_type == 'accident') as accident_incidents,
    -- Score de criticidad del corredor
    (float)(COUNT(street_corridor_analysis) * AVG(street_corridor_analysis.severity) * SUM(street_corridor_analysis.delay) / 1000.0) as corridor_criticality_score;

-- Filtrar corredores con actividad significativa
critical_corridors = FILTER street_metrics BY 
    total_incidents >= $MIN_SAMPLE_SIZE AND 
    total_delay_impact > 0 AND
    (jam_incidents > 0 OR accident_incidents > 0);

corridors_ranked = ORDER critical_corridors BY corridor_criticality_score DESC;
top_critical_corridors = LIMIT corridors_ranked 30;

STORE top_critical_corridors INTO '$OUTPUT_BASE/critical_traffic_corridors' USING PigStorage(',');

-- ========================================
-- 7. ANÁLISIS DE PROPAGACIÓN DE EVENTOS
-- ========================================

-- Identificar eventos que podrían estar relacionados por proximidad temporal y geográfica
events_with_propagation_data = FOREACH events_with_grids GENERATE
    event_id,
    event_type,
    municipality,
    timestamp,
    medium_lat_grid,
    medium_lng_grid,
    severity,
    delay;

-- Agrupar por zona y analizar secuencias temporales
propagation_zones = GROUP events_with_propagation_data BY (medium_lat_grid, medium_lng_grid);
propagation_analysis = FOREACH propagation_zones {
    time_ordered = ORDER events_with_propagation_data BY timestamp;
    GENERATE
        FLATTEN(group) as (lat_grid, lng_grid),
        COUNT(events_with_propagation_data) as zone_event_count,
        time_ordered as temporal_sequence;
}

-- Filtrar zonas con múltiples eventos para análisis de propagación
propagation_candidates = FILTER propagation_analysis BY zone_event_count >= 3;

-- Calcular métricas de propagación
propagation_metrics = FOREACH propagation_candidates GENERATE
    lat_grid,
    lng_grid,
    zone_event_count,
    -- Extraer información de la secuencia temporal
    FLATTEN(temporal_sequence) as (event_id, event_type, municipality, timestamp, lat_g, lng_g, severity, delay);

-- Agrupar por zona para calcular estadísticas de propagación
zone_propagation_stats = FOREACH (GROUP propagation_metrics BY (lat_grid, lng_grid)) GENERATE
    FLATTEN(group) as (lat_grid, lng_grid),
    COUNT(propagation_metrics) as total_events,
    AVG(propagation_metrics.severity) as avg_severity,
    SUM(propagation_metrics.delay) as cumulative_delay,
    -- Calcular diversidad de tipos de eventos
    COUNT(DISTINCT propagation_metrics.event_type) as event_type_diversity;

-- Identificar zonas de alta propagación
high_propagation_zones = FILTER zone_propagation_stats BY 
    total_events >= 5 AND 
    event_type_diversity >= 2 AND
    cumulative_delay > 100;

propagation_zones_ranked = ORDER high_propagation_zones BY cumulative_delay DESC;

STORE propagation_zones_ranked INTO '$OUTPUT_BASE/event_propagation_zones' USING PigStorage(',');

-- ========================================
-- 8. ANÁLISIS DE IMPACTO ECONÓMICO ESTIMADO
-- ========================================

-- Calcular impacto económico basado en delays y tipos de eventos
economic_impact_data = FOREACH events GENERATE
    municipality,
    event_type,
    delay,
    length,
    severity,
    -- Estimación de vehículos afectados basada en severidad y longitud
    (int)(severity * length * 0.1) as estimated_vehicles_affected,
    -- Costo estimado por minuto de delay (en pesos chilenos)
    (delay * 50.0) as estimated_cost_per_vehicle;

economic_analysis = FOREACH economic_impact_data GENERATE
    *,
    -- Impacto económico total estimado
    (float)(estimated_vehicles_affected * estimated_cost_per_vehicle) as total_estimated_impact;

-- Agregación por municipio
municipality_economic_impact = GROUP economic_analysis BY municipality;
municipality_impact_summary = FOREACH municipality_economic_impact GENERATE
    group as municipality,
    COUNT(economic_analysis) as total_incidents,
    SUM(economic_analysis.total_estimated_impact) as total_economic_impact,
    AVG(economic_analysis.total_estimated_impact) as avg_incident_impact,
    SUM(economic_analysis.delay) as total_delay_minutes,
    SUM(economic_analysis.estimated_vehicles_affected) as total_vehicles_affected;

economic_impact_ranked = ORDER municipality_impact_summary BY total_economic_impact DESC;

STORE economic_impact_ranked INTO '$OUTPUT_BASE/economic_impact_analysis' USING PigStorage(',');

-- Análisis por tipo de evento
event_type_economic_impact = GROUP economic_analysis BY event_type;
event_type_impact_summary = FOREACH event_type_economic_impact GENERATE
    group as event_type,
    COUNT(economic_analysis) as incident_frequency,
    SUM(economic_analysis.total_estimated_impact) as total_economic_impact,
    AVG(economic_analysis.total_estimated_impact) as avg_impact_per_incident,
    AVG(economic_analysis.delay) as avg_delay_per_incident;

event_economic_ranked = ORDER event_type_impact_summary BY total_economic_impact DESC;

STORE event_economic_ranked INTO '$OUTPUT_BASE/event_type_economic_impact' USING PigStorage(',');

-- ========================================
-- 9. ÍNDICES DE PERFORMANCE DEL SISTEMA
-- ========================================

-- Calcular índices de performance y calidad del sistema de análisis
system_performance_metrics = FOREACH (GROUP events ALL) GENERATE
    COUNT(events) as total_processed_events,
    AVG(events.quality_score) as avg_data_quality,
    COUNT(FILTER events BY events.quality_score >= 0.8) as high_quality_events,
    COUNT(FILTER events BY events.quality_score < 0.3) as low_quality_events,
    AVG(events.confidence) as avg_confidence,
    COUNT(DISTINCT events.source) as data_source_diversity,
    COUNT(DISTINCT events.municipality) as municipality_coverage;

-- Métricas de cobertura temporal
temporal_coverage = FOREACH (GROUP events BY SUBSTRING(timestamp, 0, 10)) GENERATE
    group as date,
    COUNT(events) as daily_event_count;

temporal_stats = FOREACH (GROUP temporal_coverage ALL) GENERATE
    COUNT(temporal_coverage) as days_with_data,
    AVG(temporal_coverage.daily_event_count) as avg_daily_events,
    MIN(temporal_coverage.daily_event_count) as min_daily_events,
    MAX(temporal_coverage.daily_event_count) as max_daily_events;

STORE system_performance_metrics INTO '$OUTPUT_BASE/system_performance_metrics' USING PigStorage(',');
STORE temporal_stats INTO '$OUTPUT_BASE/temporal_coverage_stats' USING PigStorage(',');

-- ========================================
-- 10. DASHBOARD EJECUTIVO - MÉTRICAS CLAVE
-- ========================================

-- KPIs principales para dashboard ejecutivo
executive_kpis = FOREACH (GROUP events ALL) GENERATE
    'total_events' as kpi_name,
    (long)COUNT(events) as kpi_value;

high_severity_kpi = FOREACH (GROUP (FILTER events BY severity >= 7) ALL) GENERATE
    'high_severity_events' as kpi_name,
    (long)COUNT($1) as kpi_value;

avg_response_quality_kpi = FOREACH (GROUP events ALL) GENERATE
    'avg_response_quality' as kpi_name,
    (long)(AVG(events.quality_score) * 100) as kpi_value;

total_delay_impact_kpi = FOREACH (GROUP events ALL) GENERATE
    'total_delay_minutes' as kpi_name,
    (long)SUM(events.delay) as kpi_value;

most_affected_municipality = FOREACH (GROUP events BY municipality) GENERATE
    group as municipality,
    COUNT(events) as event_count;

top_municipality_ordered = ORDER most_affected_municipality BY event_count DESC;
top_municipality_kpi = FOREACH (LIMIT top_municipality_ordered 1) GENERATE
    'most_affected_municipality' as kpi_name,
    municipality as kpi_text_value,
    event_count as kpi_value;

-- Consolidar KPIs
all_kpis = UNION executive_kpis, high_severity_kpi, avg_response_quality_kpi, total_delay_impact_kpi;

STORE all_kpis INTO '$OUTPUT_BASE/executive_dashboard_kpis' USING PigStorage(',');
STORE top_municipality_kpi INTO '$OUTPUT_BASE/top_municipality_kpi' USING PigStorage(',');

-- ========================================
-- 11. ALERTAS Y ANOMALÍAS
-- ========================================

-- Detectar patrones anómalos que requieren atención
anomaly_detection = FOREACH events GENERATE
    municipality,
    event_type,
    severity,
    delay,
    timestamp,
    SUBSTRING(timestamp, 11, 2) as hour;

-- Identificar picos de actividad inusuales
hourly_activity = GROUP anomaly_detection BY (municipality, hour);
hourly_activity_stats = FOREACH hourly_activity GENERATE
    FLATTEN(group) as (municipality, hour),
    COUNT(anomaly_detection) as hourly_event_count,
    AVG(anomaly_detection.severity) as avg_hourly_severity;

-- Calcular baseline de actividad normal
municipality_hourly_baseline = FOREACH (GROUP hourly_activity_stats BY municipality) GENERATE
    group as municipality,
    AVG(hourly_activity_stats.hourly_event_count) as baseline_hourly_events,
    AVG(hourly_activity_stats.avg_hourly_severity) as baseline_severity;

-- Identificar anomalías (actividad > 2x baseline)
hourly_with_baseline = JOIN hourly_activity_stats BY municipality, municipality_hourly_baseline BY municipality;
anomaly_candidates = FOREACH hourly_with_baseline GENERATE
    hourly_activity_stats::municipality as municipality,
    hourly_activity_stats::hour as hour,
    hourly_activity_stats::hourly_event_count as current_activity,
    municipality_hourly_baseline::baseline_hourly_events as baseline,
    (float)(hourly_activity_stats::hourly_event_count / municipality_hourly_baseline::baseline_hourly_events) as activity_ratio;

anomalies_detected = FILTER anomaly_candidates BY activity_ratio >= 2.0;
anomalies_ranked = ORDER anomalies_detected BY activity_ratio DESC;

STORE anomalies_ranked INTO '$OUTPUT_BASE/activity_anomalies' USING PigStorage(',');

-- ========================================
-- LOGS Y VERIFICACIONES FINALES
-- ========================================

-- Mostrar estadísticas de procesamiento en consola
DUMP system_performance_metrics;
DUMP all_kpis;

-- Verificar que se generaron los archivos principales
exec_summary_check = FOREACH (GROUP events ALL) GENERATE
    'aggregation_processing_completed' as status,
    COUNT(events) as total_events_processed,
    CurrentTime() as completion_timestamp;