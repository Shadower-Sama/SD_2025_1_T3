import json
import logging
import os
import subprocess
import tempfile
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from pymongo import MongoClient
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PigDataProcessor:
    """Procesador de datos usando Apache Pig para análisis distribuido"""
    
    def __init__(self, mongodb_uri: str, hadoop_uri: str):
        self.mongodb_uri = mongodb_uri
        self.hadoop_uri = hadoop_uri
        
        self.mongodb_client = MongoClient(mongodb_uri)
        self.db = self.mongodb_client.traffic_db
        self.raw_collection = self.db.waze_events
        self.processed_collection = self.db.processed_events
        self.analysis_collection = self.db.analysis_results
        
        self.pig_scripts_dir = "/app/scripts"
        os.makedirs(self.pig_scripts_dir, exist_ok=True)
        
        logger.info("Procesador Pig inicializado")
    
    def extract_data_to_hdfs(self) -> str:
        """Extrae datos de MongoDB y los sube a HDFS"""
        
        logger.info("Extrayendo datos de MongoDB a HDFS")
        
        try:
            cutoff_time = datetime.now() - timedelta(hours=24)
            cursor = self.raw_collection.find(
                {'timestamp': {'$gte': cutoff_time}},
                {'_id': 0}  # Excluir ObjectId
            )
            
            events = []
            for doc in cursor:
                if 'timestamp' in doc:
                    doc['timestamp'] = doc['timestamp'].isoformat()
                
                cleaned_doc = self.clean_event_data(doc)
                if cleaned_doc:
                    events.append(cleaned_doc)
            
            logger.info(f"Extraídos {len(events)} eventos para procesamiento")
            
            if not events:
                logger.warning("No hay eventos para procesar")
                return ""
            
            temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
            
            if events:
                headers = list(events[0].keys())
                temp_file.write(','.join(headers) + '\n')
                
                for event in events:
                    row = []
                    for header in headers:
                        value = event.get(header, '')
                        if isinstance(value, dict):
                            value = json.dumps(value)
                        elif value is None:
                            value = ''
                        row.append(str(value))
                    temp_file.write(','.join(row) + '\n')
            
            temp_file.close()
            
            hdfs_path = f"/traffic_data/raw_events_{int(time.time())}.csv"
            hdfs_command = f"hdfs dfs -put {temp_file.name} {hdfs_path}"
            
            result = subprocess.run(hdfs_command, shell=True, capture_output=True, text=True)
            
            if result.returncode == 0:
                logger.info(f"Datos subidos a HDFS: {hdfs_path}")
                os.unlink(temp_file.name)  
                return hdfs_path
            else:
                logger.error(f"Error subiendo a HDFS: {result.stderr}")
                os.unlink(temp_file.name)
                return ""
                
        except Exception as e:
            logger.error(f"Error extrayendo datos a HDFS: {e}")
            return ""
    
    def clean_event_data(self, event: Dict) -> Optional[Dict]:
        """Limpia y valida datos de un evento"""
        
        try:
            
            required_fields = ['event_id', 'event_type', 'timestamp']
            
            for field in required_fields:
                if field not in event or not event[field]:
                    return None
            
            
            cleaned = {
                'event_id': str(event['event_id']),
                'event_type': str(event.get('event_type', 'unknown')).lower(),
                'subtype': str(event.get('subtype', '')).lower(),
                'description': str(event.get('description', ''))[:500],  
                'street': str(event.get('street', 'unknown'))[:200],
                'city': str(event.get('city', 'unknown'))[:100],
                'municipality': str(event.get('municipality', 'unknown'))[:100],
                'timestamp': event['timestamp'],
                'confidence': int(event.get('confidence', 0)),
                'severity': int(event.get('severity', 0)),
                'source': str(event.get('source', 'unknown'))
            }
            
           
            location = event.get('location', {})
            if isinstance(location, dict):
                cleaned['latitude'] = float(location.get('lat', 0))
                cleaned['longitude'] = float(location.get('lng', 0))
            else:
                cleaned['latitude'] = 0.0
                cleaned['longitude'] = 0.0
            
            
            if not (-34.0 <= cleaned['latitude'] <= -33.0 and -71.5 <= cleaned['longitude'] <= -70.0):
                cleaned['latitude'] = -33.4489  
                cleaned['longitude'] = -70.6693
            
            
            cleaned['length'] = float(event.get('length', 0))
            cleaned['delay'] = float(event.get('delay', 0))
            cleaned['speed'] = float(event.get('speed', 0))
            cleaned['level'] = int(event.get('level', 0))
            cleaned['num_thumbs_up'] = int(event.get('num_thumbs_up', 0))
            
            return cleaned
            
        except Exception as e:
            logger.warning(f"Error limpiando evento {event.get('event_id', 'unknown')}: {e}")
            return None
    
    def create_filtering_script(self, input_path: str, output_path: str) -> str:
        """Crea script Pig para filtrado y limpieza"""
        
        script_content = f"""
-- Script Pig para filtrado y limpieza de eventos de tráfico



-- Registrar funciones UDF si es necesario
-- REGISTER '/path/to/custom-udfs.jar';

-- Cargar datos de entrada
raw_events = LOAD '{input_path}' USING PigStorage(',') AS (
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
valid_events = FILTER raw_events BY (
    event_id IS NOT NULL AND
    event_type IS NOT NULL AND
    timestamp IS NOT NULL AND
    latitude >= -34.0 AND latitude <= -33.0 AND
    longitude >= -71.5 AND longitude <= -70.0 AND
    confidence >= 0 AND
    severity >= 0
);

-- Eliminar duplicados basados en event_id
unique_events = DISTINCT valid_events;

-- Estandarizar tipos de eventos
standardized_events = FOREACH unique_events GENERATE
    event_id,
    LOWER(event_type) as event_type,
    LOWER(subtype) as subtype,
    description,
    street,
    city,
    LOWER(municipality) as municipality,
    timestamp,
    confidence,
    severity,
    source,
    latitude,
    longitude,
    length,
    delay,
    speed,
    level,
    num_thumbs_up;

-- Guardar eventos limpios
STORE standardized_events INTO '{output_path}' USING PigStorage(',');

-- Estadísticas de filtrado
raw_count = FOREACH (GROUP raw_events ALL) GENERATE COUNT(raw_events);
valid_count = FOREACH (GROUP valid_events ALL) GENERATE COUNT(valid_events);
unique_count = FOREACH (GROUP unique_events ALL) GENERATE COUNT(unique_events);

STORE raw_count INTO '{output_path}_stats_raw' USING PigStorage(',');
STORE valid_count INTO '{output_path}_stats_valid' USING PigStorage(',');
STORE unique_count INTO '{output_path}_stats_unique' USING PigStorage(',');
"""
        
        script_path = os.path.join(self.pig_scripts_dir, f"filtering_{int(time.time())}.pig")
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        return script_path
    
    def create_analysis_script(self, input_path: str, output_base_path: str) -> str:
        """Crea script Pig para análisis de datos"""
        
        script_content = f"""
-- Script Pig para análisis de eventos de tráfico
-- Genera métricas y agregaciones para la toma de decisiones

-- Cargar eventos procesados
events = LOAD '{input_path}' USING PigStorage(',') AS (
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

-- 1. Análisis por municipio
municipality_analysis = GROUP events BY municipality;
municipality_stats = FOREACH municipality_analysis GENERATE
    group as municipality,
    COUNT(events) as total_events,
    AVG(events.confidence) as avg_confidence,
    AVG(events.severity) as avg_severity,
    SUM(events.delay) as total_delay;

STORE municipality_stats INTO '{output_base_path}/municipality_analysis' USING PigStorage(',');

-- 2. Análisis por tipo de evento
event_type_analysis = GROUP events BY event_type;
event_type_stats = FOREACH event_type_analysis GENERATE
    group as event_type,
    COUNT(events) as total_events,
    AVG(events.confidence) as avg_confidence,
    AVG(events.severity) as avg_severity;

STORE event_type_stats INTO '{output_base_path}/event_type_analysis' USING PigStorage(',');

-- 3. Análisis temporal (por hora)
-- Extraer hora del timestamp (formato ISO)
events_with_hour = FOREACH events GENERATE
    *,
    SUBSTRING(timestamp, 11, 2) as hour;

hourly_analysis = GROUP events_with_hour BY hour;
hourly_stats = FOREACH hourly_analysis GENERATE
    group as hour,
    COUNT(events_with_hour) as total_events,
    AVG(events_with_hour.severity) as avg_severity;

STORE hourly_stats INTO '{output_base_path}/hourly_analysis' USING PigStorage(',');

-- 4. Análisis de severidad alta
high_severity_events = FILTER events BY severity >= 4;
high_severity_by_municipality = GROUP high_severity_events BY municipality;
high_severity_stats = FOREACH high_severity_by_municipality GENERATE
    group as municipality,
    COUNT(high_severity_events) as high_severity_count;

STORE high_severity_stats INTO '{output_base_path}/high_severity_analysis' USING PigStorage(',');

-- 5. Análisis de atascos (jams)
jam_events = FILTER events BY event_type == 'jam';
jam_analysis = FOREACH jam_events GENERATE
    municipality,
    street,
    delay,
    length,
    level;

jam_by_municipality = GROUP jam_analysis BY municipality;
jam_stats = FOREACH jam_by_municipality GENERATE
    group as municipality,
    COUNT(jam_analysis) as total_jams,
    AVG(jam_analysis.delay) as avg_delay,
    SUM(jam_analysis.length) as total_length,
    AVG(jam_analysis.level) as avg_level;

STORE jam_stats INTO '{output_base_path}/jam_analysis' USING PigStorage(',');

-- 6. Top calles con más incidentes
street_analysis = GROUP events BY (municipality, street);
street_stats = FOREACH street_analysis GENERATE
    FLATTEN(group) as (municipality, street),
    COUNT(events) as incident_count,
    AVG(events.severity) as avg_severity;

-- Ordenar por número de incidentes (descendente)
top_streets = ORDER street_stats BY incident_count DESC;
top_20_streets = LIMIT top_streets 20;

STORE top_20_streets INTO '{output_base_path}/top_streets_analysis' USING PigStorage(',');

-- 7. Análisis de confiabilidad por fuente
source_analysis = GROUP events BY source;
source_stats = FOREACH source_analysis GENERATE
    group as source,
    COUNT(events) as total_events,
    AVG(events.confidence) as avg_confidence,
    AVG(events.num_thumbs_up) as avg_thumbs_up;

STORE source_stats INTO '{output_base_path}/source_analysis' USING PigStorage(',');

-- 8. Eventos críticos (alta severidad + baja confianza)
critical_events = FILTER events BY (severity >= 4 AND confidence <= 3);
critical_by_municipality = GROUP critical_events BY municipality;
critical_stats = FOREACH critical_by_municipality GENERATE
    group as municipality,
    COUNT(critical_events) as critical_count;

STORE critical_stats INTO '{output_base_path}/critical_events_analysis' USING PigStorage(',');
"""
        
        script_path = os.path.join(self.pig_scripts_dir, f"analysis_{int(time.time())}.pig")
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        return script_path
    
    def create_aggregation_script(self, input_path: str, output_path: str) -> str:
        """Crea script Pig para agregaciones complejas"""
        
        script_content = f"""
-- Script Pig para agregaciones complejas y métricas avanzadas

events = LOAD '{input_path}' USING PigStorage(',') AS (
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

-- 1. Matriz de correlación evento-municipio
event_municipality_matrix = FOREACH events GENERATE
    municipality,
    event_type,
    1 as count;

event_municipality_grouped = GROUP event_municipality_matrix BY (municipality, event_type);
event_municipality_counts = FOREACH event_municipality_grouped GENERATE
    FLATTEN(group) as (municipality, event_type),
    SUM(event_municipality_matrix.count) as total_count;

STORE event_municipality_counts INTO '{output_path}/event_municipality_matrix' USING PigStorage(',');

-- 2. Análisis de densidad de eventos por zona geográfica
-- Dividir la RM en cuadrículas de ~1km
events_with_grid = FOREACH events GENERATE
    *,
    FLOOR((latitude + 34.0) * 100) as lat_grid,
    FLOOR((longitude + 71.5) * 100) as lng_grid;

grid_analysis = GROUP events_with_grid BY (lat_grid, lng_grid);
grid_density = FOREACH grid_analysis GENERATE
    FLATTEN(group) as (lat_grid, lng_grid),
    COUNT(events_with_grid) as event_density,
    AVG(events_with_grid.severity) as avg_severity,
    AVG(events_with_grid.confidence) as avg_confidence;

-- Filtrar zonas con al menos 5 eventos
significant_grids = FILTER grid_density BY event_density >= 5;

STORE significant_grids INTO '{output_path}/grid_density_analysis' USING PigStorage(',');

-- 3. Tendencias temporales detalladas
events_with_temporal = FOREACH events GENERATE
    *,
    SUBSTRING(timestamp, 0, 10) as date,
    SUBSTRING(timestamp, 11, 2) as hour,
    SUBSTRING(timestamp, 14, 2) as minute;

-- Análisis por día de la semana (simplificado)
daily_analysis = GROUP events_with_temporal BY date;
daily_stats = FOREACH daily_analysis GENERATE
    group as date,
    COUNT(events_with_temporal) as daily_count,
    AVG(events_with_temporal.severity) as avg_severity;

STORE daily_stats INTO '{output_path}/daily_trends' USING PigStorage(',');

-- 4. Eventos consecutivos en la misma calle (posibles incidentes relacionados)
street_events = FOREACH events GENERATE
    municipality,
    street,
    timestamp,
    event_type,
    severity;

street_grouped = GROUP street_events BY (municipality, street);
street_sequences = FOREACH street_grouped {{
    sorted = ORDER street_events BY timestamp;
    GENERATE group.municipality as municipality,
             group.street as street,
             COUNT(street_events) as event_count,
             sorted;
}};

-- Filtrar calles con múltiples eventos
busy_streets = FILTER street_sequences BY event_count >= 3;

STORE busy_streets INTO '{output_path}/street_sequences' USING PigStorage(',');

-- 5. Análisis de efectividad de respuesta (basado en thumbs up)
response_analysis = GROUP events BY event_type;
response_effectiveness = FOREACH response_analysis GENERATE
    group as event_type,
    COUNT(events) as total_events,
    AVG(events.num_thumbs_up) as avg_thumbs_up,
    AVG(events.confidence) as avg_confidence,
    (double)SUM(events.num_thumbs_up) / (double)COUNT(events) as effectiveness_ratio;

STORE response_effectiveness INTO '{output_path}/response_effectiveness' USING PigStorage(',');

-- 6. Hotspots de tráfico (zonas con alta concentración de eventos)
municipality_severity = FOREACH events GENERATE
    municipality,
    severity,
    (severity >= 4 ? 1 : 0) as is_severe;

municipality_hotspots = GROUP municipality_severity BY municipality;
hotspot_analysis = FOREACH municipality_hotspots GENERATE
    group as municipality,
    COUNT(municipality_severity) as total_events,
    SUM(municipality_severity.is_severe) as severe_events,
    (double)SUM(municipality_severity.is_severe) / (double)COUNT(municipality_severity) as severity_ratio,
    AVG(municipality_severity.severity) as avg_severity;

-- Ordenar por ratio de severidad
hotspots_ranked = ORDER hotspot_analysis BY severity_ratio DESC;

STORE hotspots_ranked INTO '{output_path}/traffic_hotspots' USING PigStorage(',');
"""
        
        script_path = os.path.join(self.pig_scripts_dir, f"aggregation_{int(time.time())}.pig")
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        return script_path
    
    def execute_pig_script(self, script_path: str) -> bool:
        """Ejecuta un script Pig"""
        
        logger.info(f"Ejecutando script Pig: {script_path}")
        
        try:
            
            pig_command = f"pig -f {script_path}"
            
          
            result = subprocess.run(
                pig_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=1800  #
            )
            
            if result.returncode == 0:
                logger.info(f"Script Pig ejecutado exitosamente: {script_path}")
                if result.stdout:
                    logger.debug(f"Stdout: {result.stdout}")
                return True
            else:
                logger.error(f"Error ejecutando script Pig: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error(f"Timeout ejecutando script Pig: {script_path}")
            return False
        except Exception as e:
            logger.error(f"Excepción ejecutando script Pig: {e}")
            return False
    
    def download_results_from_hdfs(self, hdfs_path: str) -> List[Dict]:
        """Descarga resultados desde HDFS"""
        
        logger.info(f"Descargando resultados desde HDFS: {hdfs_path}")
        
        try:
            
            temp_file = tempfile.NamedTemporaryFile(mode='w+', suffix='.csv', delete=False)
            temp_file.close()
            
            
            hdfs_command = f"hdfs dfs -get {hdfs_path}/* {temp_file.name}"
            result = subprocess.run(hdfs_command, shell=True, capture_output=True, text=True)
            
            if result.returncode != 0:
                logger.error(f"Error descargando desde HDFS: {result.stderr}")
                os.unlink(temp_file.name)
                return []
            
          
            results = []
            try:
                with open(temp_file.name, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            parts = line.split(',')
                            results.append(parts)
                
                os.unlink(temp_file.name)
                return results
                
            except Exception as e:
                logger.error(f"Error leyendo archivo de resultados: {e}")
                os.unlink(temp_file.name)
                return []
                
        except Exception as e:
            logger.error(f"Error descargando resultados: {e}")
            return []
    
    def save_analysis_results(self, analysis_type: str, results: List[Dict]):
        """Guarda resultados de análisis en MongoDB"""
        
        if not results:
            return
        
        try:
            analysis_doc = {
                'analysis_type': analysis_type,
                'timestamp': datetime.now(),
                'results': results,
                'total_records': len(results)
            }
            
            self.analysis_collection.insert_one(analysis_doc)
            logger.info(f"Guardados {len(results)} resultados de análisis '{analysis_type}'")
            
        except Exception as e:
            logger.error(f"Error guardando resultados de análisis: {e}")
    
    def run_complete_pipeline(self) -> Dict:
        """Ejecuta el pipeline completo de procesamiento"""
        
        logger.info("Iniciando pipeline completo de procesamiento con Pig")
        
        pipeline_results = {
            'start_time': datetime.now(),
            'stages': {},
            'success': False
        }
        
        try:
            
            logger.info("=== Etapa 1: Extracción a HDFS ===")
            hdfs_input_path = self.extract_data_to_hdfs()
            
            if not hdfs_input_path:
                pipeline_results['error'] = "Error extrayendo datos a HDFS"
                return pipeline_results
            
            pipeline_results['stages']['extraction'] = {
                'success': True,
                'hdfs_path': hdfs_input_path
            }
            
            
            logger.info("=== Etapa 2: Filtrado y limpieza ===")
            filtered_path = f"/traffic_data/filtered_{int(time.time())}"
            filtering_script = self.create_filtering_script(hdfs_input_path, filtered_path)
            
            if self.execute_pig_script(filtering_script):
                pipeline_results['stages']['filtering'] = {
                    'success': True,
                    'output_path': filtered_path,
                    'script_path': filtering_script
                }
            else:
                pipeline_results['stages']['filtering'] = {'success': False}
                pipeline_results['error'] = "Error en filtrado"
                return pipeline_results
            
          
            logger.info("=== Etapa 3: Análisis básico ===")
            analysis_path = f"/traffic_data/analysis_{int(time.time())}"
            analysis_script = self.create_analysis_script(f"{filtered_path}/part-*", analysis_path)
            
            if self.execute_pig_script(analysis_script):
                pipeline_results['stages']['analysis'] = {
                    'success': True,
                    'output_path': analysis_path,
                    'script_path': analysis_script
                }
            else:
                pipeline_results['stages']['analysis'] = {'success': False}
               
            
           
            logger.info("=== Etapa 4: Agregaciones complejas ===")
            aggregation_path = f"/traffic_data/aggregation_{int(time.time())}"
            aggregation_script = self.create_aggregation_script(f"{filtered_path}/part-*", aggregation_path)
            
            if self.execute_pig_script(aggregation_script):
                pipeline_results['stages']['aggregation'] = {
                    'success': True,
                    'output_path': aggregation_path,
                    'script_path': aggregation_script
                }
            else:
                pipeline_results['stages']['aggregation'] = {'success': False}
            
            logger.info("=== Etapa 5: Descarga y almacenamiento de resultados ===")
            self.download_and_store_results(analysis_path, aggregation_path)
            
            pipeline_results['success'] = True
            pipeline_results['end_time'] = datetime.now()
            pipeline_results['duration'] = (pipeline_results['end_time'] - pipeline_results['start_time']).total_seconds()
            
            logger.info(f"Pipeline completado exitosamente en {pipeline_results['duration']:.2f} segundos")
            
        except Exception as e:
            logger.error(f"Error en pipeline: {e}")
            pipeline_results['error'] = str(e)
            pipeline_results['end_time'] = datetime.now()
        
        return pipeline_results
    
    def download_and_store_results(self, analysis_path: str, aggregation_path: str):
        """Descarga resultados de análisis y agregación desde HDFS"""
        
        basic_analyses = {
            'municipality_analysis': 'municipality',
            'event_type_analysis': 'event_type',
            'hourly_analysis': 'hourly',
            'high_severity_analysis': 'high_severity',
            'jam_analysis': 'jam',
            'top_streets_analysis': 'top_streets',
            'source_analysis': 'source',
            'critical_events_analysis': 'critical_events'
        }
        
        for analysis_name, analysis_key in basic_analyses.items():
            hdfs_analysis_path = f"{analysis_path}/{analysis_name}"
            results = self.download_results_from_hdfs(hdfs_analysis_path)
            
            if results:
                dict_results = []
                for result in results:
                    if len(result) > 1:  
                        dict_results.append({
                            'data': result,
                            'analysis_type': analysis_key
                        })
                
                self.save_analysis_results(f"basic_{analysis_key}", dict_results)
        
        complex_aggregations = {
            'event_municipality_matrix': 'event_municipality_correlation',
            'grid_density_analysis': 'geographic_density',
            'daily_trends': 'temporal_trends',
            'street_sequences': 'street_incident_sequences',
            'response_effectiveness': 'response_effectiveness',
            'traffic_hotspots': 'traffic_hotspots'
        }
        
        
        for agg_name, agg_key in complex_aggregations.items():
            hdfs_agg_path = f"{aggregation_path}/{agg_name}"
            results = self.download_results_from_hdfs(hdfs_agg_path)
            
            if results:
                dict_results = []
                for result in results:
                    if len(result) > 1:
                        dict_results.append({
                            'data': result,
                            'aggregation_type': agg_key
                        })
                
                self.save_analysis_results(f"complex_{agg_key}", dict_results)
    
    def get_processing_stats(self) -> Dict:
        """Obtiene estadísticas del procesamiento"""
        
        try:
            total_raw_events = self.raw_collection.count_documents({})
            
            total_processed_events = self.processed_collection.count_documents({})
            
            total_analysis_results = self.analysis_collection.count_documents({})
            
            recent_cutoff = datetime.now() - timedelta(hours=24)
            recent_analyses = list(self.analysis_collection.find(
                {'timestamp': {'$gte': recent_cutoff}},
                {'analysis_type': 1, 'timestamp': 1, 'total_records': 1}
            ).sort('timestamp', -1).limit(10))
            
            for analysis in recent_analyses:
                analysis['_id'] = str(analysis['_id'])
                analysis['timestamp'] = analysis['timestamp'].isoformat()
            
            return {
                'raw_events_count': total_raw_events,
                'processed_events_count': total_processed_events,
                'analysis_results_count': total_analysis_results,
                'recent_analyses': recent_analyses,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {'error': str(e)}
    
    def run_incremental_processing(self, hours_back: int = 1):
        """Ejecuta procesamiento incremental de eventos recientes"""
        
        logger.info(f"Iniciando procesamiento incremental de últimas {hours_back} horas")
        
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours_back)
            
            recent_events = list(self.raw_collection.find(
                {'timestamp': {'$gte': cutoff_time}},
                {'_id': 0}
            ))
            
            if not recent_events:
                logger.info("No hay eventos recientes para procesar")
                return {'processed_count': 0}
            
            processed_events = []
            for event in recent_events:
                cleaned = self.clean_event_data(event)
                if cleaned:
                    processed_events.append(cleaned)
            
            if processed_events:
                for event in processed_events:
                    self.processed_collection.replace_one(
                        {'event_id': event['event_id']},
                        event,
                        upsert=True
                    )
                
                logger.info(f"Procesados {len(processed_events)} eventos incrementalmente")
                
                return {
                    'processed_count': len(processed_events),
                    'raw_count': len(recent_events),
                    'processing_rate': len(processed_events) / len(recent_events) if recent_events else 0
                }
            
            return {'processed_count': 0}
            
        except Exception as e:
            logger.error(f"Error en procesamiento incremental: {e}")
            return {'error': str(e)}

def main():
    """Función principal del procesador"""
    
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@localhost:27017/traffic_db?authSource=admin')
    hadoop_uri = os.getenv('HADOOP_URI', 'hadoop:9000')
    
    processor = PigDataProcessor(mongodb_uri, hadoop_uri)
    
    mode = os.getenv('PROCESSING_MODE', 'full_pipeline')
    
    if mode == 'full_pipeline':
        results = processor.run_complete_pipeline()
        logger.info(f"Resultados del pipeline: {json.dumps(results, default=str, indent=2)}")
        
    elif mode == 'incremental':
        hours_back = int(os.getenv('INCREMENTAL_HOURS', '1'))
        results = processor.run_incremental_processing(hours_back)
        logger.info(f"Procesamiento incremental completado: {results}")
        
    elif mode == 'stats':
        stats = processor.get_processing_stats()
        logger.info(f"Estadísticas de procesamiento: {json.dumps(stats, indent=2)}")
        
    else:
        logger.error(f"Modo no reconocido: {mode}")

if __name__ == "__main__":
    main()