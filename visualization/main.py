import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from elasticsearch import Elasticsearch, helpers
from pymongo import MongoClient
import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TrafficVisualizationSystem:
    """Sistema de visualización para análisis de tráfico"""
    
    def __init__(self, elasticsearch_uri: str, mongodb_uri: str, kibana_uri: str = None):
        self.elasticsearch_uri = elasticsearch_uri
        self.mongodb_uri = mongodb_uri
        self.kibana_uri = kibana_uri or elasticsearch_uri.replace('9200', '5601')
        
        self.es = Elasticsearch([elasticsearch_uri])
        
        self.mongo_client = MongoClient(mongodb_uri)
        self.db = self.mongo_client.traffic_db
        
        self.events_index = 'traffic-events'
        self.analysis_index = 'traffic-analysis'
        self.cache_metrics_index = 'cache-metrics'
        
        logger.info("Sistema de visualización inicializado")
    
    def create_elasticsearch_indices(self):
        """Crea los índices necesarios en Elasticsearch"""
        
        events_mapping = {
            "mappings": {
                "properties": {
                    "event_id": {"type": "keyword"},
                    "event_type": {"type": "keyword"},
                    "subtype": {"type": "keyword"},
                    "description": {"type": "text", "analyzer": "spanish"},
                    "street": {"type": "keyword"},
                    "city": {"type": "keyword"},
                    "municipality": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "location": {"type": "geo_point"},
                    "confidence": {"type": "integer"},
                    "severity": {"type": "integer"},
                    "source": {"type": "keyword"},
                    "length": {"type": "float"},
                    "delay": {"type": "float"},
                    "speed": {"type": "float"},
                    "level": {"type": "integer"},
                    "num_thumbs_up": {"type": "integer"},
                    "processed_timestamp": {"type": "date"}
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "index": {
                    "refresh_interval": "5s"
                }
            }
        }
        
        analysis_mapping = {
            "mappings": {
                "properties": {
                    "analysis_type": {"type": "keyword"},
                    "analysis_subtype": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "municipality": {"type": "keyword"},
                    "event_type": {"type": "keyword"},
                    "metric_name": {"type": "keyword"},
                    "metric_value": {"type": "float"},
                    "count": {"type": "integer"},
                    "avg_confidence": {"type": "float"},
                    "avg_severity": {"type": "float"},
                    "total_delay": {"type": "float"},
                    "period": {"type": "keyword"}
                }
            }
        }
        
        cache_mapping = {
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "cache_policy": {"type": "keyword"},
                    "hit_rate": {"type": "float"},
                    "miss_rate": {"type": "float"},
                    "total_hits": {"type": "integer"},
                    "total_misses": {"type": "integer"},
                    "cache_size": {"type": "integer"},
                    "capacity": {"type": "integer"},
                    "avg_response_time": {"type": "float"},
                    "avg_cache_time": {"type": "float"},
                    "avg_db_time": {"type": "float"}
                }
            }
        }
        
        indices = [
            (self.events_index, events_mapping),
            (self.analysis_index, analysis_mapping),
            (self.cache_metrics_index, cache_mapping)
        ]
        
        for index_name, mapping in indices:
            try:
                if self.es.indices.exists(index=index_name):
                    logger.info(f"Índice {index_name} ya existe")
                else:
                    self.es.indices.create(index=index_name, body=mapping)
                    logger.info(f"Índice {index_name} creado exitosamente")
            except Exception as e:
                logger.error(f"Error creando índice {index_name}: {e}")
    
    def sync_events_to_elasticsearch(self, limit: int = 10000):
        """Sincroniza eventos desde MongoDB a Elasticsearch"""
        
        logger.info(f"Sincronizando eventos a Elasticsearch (límite: {limit})")
        
        try:
            cutoff_time = datetime.now() - timedelta(days=7) 
            
            cursor = self.db.waze_events.find(
                {'timestamp': {'$gte': cutoff_time}},
                {'_id': 0}
            ).sort('timestamp', -1).limit(limit)
            
            docs = []
            for event in cursor:
                
                es_doc = self.transform_event_for_es(event)
                if es_doc:
                    docs.append({
                        '_index': self.events_index,
                        '_id': event['event_id'],
                        '_source': es_doc
                    })
            
            if docs:
                
                helpers.bulk(self.es, docs, chunk_size=1000, request_timeout=60)
                logger.info(f"Sincronizados {len(docs)} eventos a Elasticsearch")
            else:
                logger.info("No hay eventos para sincronizar")
            
            return len(docs)
            
        except Exception as e:
            logger.error(f"Error sincronizando eventos: {e}")
            return 0
    
    def transform_event_for_es(self, event: Dict) -> Optional[Dict]:
        """Transforma un evento de MongoDB para Elasticsearch"""
        
        try:
           
            location = event.get('location', {})
            if isinstance(location, dict) and 'lat' in location and 'lng' in location:
                geo_point = {
                    'lat': float(location['lat']),
                    'lon': float(location['lng'])
                }
            else:
                geo_point = None
            
            
            timestamp = event.get('timestamp')
            if isinstance(timestamp, str):
                timestamp = timestamp
            elif hasattr(timestamp, 'isoformat'):
                timestamp = timestamp.isoformat()
            else:
                timestamp = datetime.now().isoformat()
            
            es_doc = {
                'event_id': event.get('event_id'),
                'event_type': event.get('event_type', 'unknown'),
                'subtype': event.get('subtype', ''),
                'description': event.get('description', ''),
                'street': event.get('street', ''),
                'city': event.get('city', ''),
                'municipality': event.get('municipality', ''),
                'timestamp': timestamp,
                'confidence': int(event.get('confidence', 0)),
                'severity': int(event.get('severity', 0)),
                'source': event.get('source', 'unknown'),
                'length': float(event.get('length', 0)),
                'delay': float(event.get('delay', 0)),
                'speed': float(event.get('speed', 0)),
                'level': int(event.get('level', 0)),
                'num_thumbs_up': int(event.get('num_thumbs_up', 0)),
                'processed_timestamp': datetime.now().isoformat()
            }
            
            if geo_point:
                es_doc['location'] = geo_point
            
            return es_doc
            
        except Exception as e:
            logger.warning(f"Error transformando evento {event.get('event_id', 'unknown')}: {e}")
            return None
    
    def sync_analysis_results_to_elasticsearch(self):
        """Sincroniza resultados de análisis a Elasticsearch"""
        
        logger.info("Sincronizando resultados de análisis a Elasticsearch")
        
        try:
            
            cutoff_time = datetime.now() - timedelta(days=1)
            
            cursor = self.db.analysis_results.find(
                {'timestamp': {'$gte': cutoff_time}},
                {'_id': 0}
            )
            
            docs = []
            for analysis in cursor:
                
                analysis_type = analysis.get('analysis_type', 'unknown')
                timestamp = analysis.get('timestamp', datetime.now())
                
                if isinstance(timestamp, datetime):
                    timestamp = timestamp.isoformat()
                
                results = analysis.get('results', [])
                
                for i, result in enumerate(results):
                    es_doc = self.transform_analysis_for_es(analysis_type, result, timestamp)
                    if es_doc:
                        docs.append({
                            '_index': self.analysis_index,
                            '_id': f"{analysis_type}_{int(time.time())}_{i}",
                            '_source': es_doc
                        })
            
            if docs:
                helpers.bulk(self.es, docs, chunk_size=500)
                logger.info(f"Sincronizados {len(docs)} resultados de análisis")
            
            return len(docs)
            
        except Exception as e:
            logger.error(f"Error sincronizando análisis: {e}")
            return 0
    
    def transform_analysis_for_es(self, analysis_type: str, result: Dict, timestamp: str) -> Optional[Dict]:
        """Transforma un resultado de análisis para Elasticsearch"""
        
        try:
           
            es_doc = {
                'analysis_type': analysis_type,
                'timestamp': timestamp
            }
            
            
            data = result.get('data', [])
            if not data:
                return None
            
            if 'municipality' in analysis_type:
                if len(data) >= 4:
                    es_doc.update({
                        'analysis_subtype': 'municipality',
                        'municipality': data[0],
                        'count': int(data[1]) if data[1].isdigit() else 0,
                        'avg_confidence': float(data[2]) if data[2].replace('.', '').isdigit() else 0,
                        'avg_severity': float(data[3]) if data[3].replace('.', '').isdigit() else 0
                    })
                    
            elif 'event_type' in analysis_type:
                if len(data) >= 3:
                    es_doc.update({
                        'analysis_subtype': 'event_type',
                        'event_type': data[0],
                        'count': int(data[1]) if data[1].isdigit() else 0,
                        'avg_confidence': float(data[2]) if data[2].replace('.', '').isdigit() else 0
                    })
                    
            elif 'hourly' in analysis_type:
                if len(data) >= 3:
                    es_doc.update({
                        'analysis_subtype': 'hourly',
                        'period': f"hour_{data[0]}",
                        'count': int(data[1]) if data[1].isdigit() else 0,
                        'avg_severity': float(data[2]) if data[2].replace('.', '').isdigit() else 0
                    })
                    
            elif 'jam' in analysis_type:
                if len(data) >= 5:
                    es_doc.update({
                        'analysis_subtype': 'jam_analysis',
                        'municipality': data[0],
                        'count': int(data[1]) if data[1].isdigit() else 0,
                        'avg_delay': float(data[2]) if data[2].replace('.', '').isdigit() else 0,
                        'total_length': float(data[3]) if data[3].replace('.', '').isdigit() else 0,
                        'avg_level': float(data[4]) if data[4].replace('.', '').isdigit() else 0
                    })
                    
            else:
                
                es_doc.update({
                    'analysis_subtype': 'generic',
                    'metric_name': analysis_type,
                    'metric_value': float(data[0]) if len(data) > 0 and data[0].replace('.', '').isdigit() else 0
                })
            
            return es_doc
            
        except Exception as e:
            logger.warning(f"Error transformando análisis {analysis_type}: {e}")
            return None
    
    def create_kibana_dashboards(self):
        """Crea dashboards predefinidos en Kibana"""
        
        logger.info("Creando dashboards en Kibana")
        
        
        traffic_dashboard = {
            "version": "8.8.0",
            "objects": [
                {
                    "id": "traffic-overview-dashboard",
                    "type": "dashboard",
                    "attributes": {
                        "title": "Análisis de Tráfico - Región Metropolitana",
                        "description": "Dashboard principal para análisis de tráfico",
                        "panelsJSON": json.dumps([
                            {
                                "gridData": {"x": 0, "y": 0, "w": 24, "h": 8},
                                "panelIndex": "1",
                                "embeddableConfig": {},
                                "panelRefName": "panel_1"
                            },
                            {
                                "gridData": {"x": 24, "y": 0, "w": 24, "h": 8},
                                "panelIndex": "2",
                                "embeddableConfig": {},
                                "panelRefName": "panel_2"
                            }
                        ]),
                        "timeRestore": True,
                        "timeTo": "now",
                        "timeFrom": "now-24h",
                        "refreshInterval": {
                            "pause": False,
                            "value": 60000
                        }
                    }
                }
            ]
        }
        
       
        municipality_viz = {
            "id": "events-by-municipality",
            "type": "visualization",
            "attributes": {
                "title": "Eventos por Municipio",
                "visState": json.dumps({
                    "title": "Eventos por Municipio",
                    "type": "histogram",
                    "params": {
                        "grid": {"categoryLines": False, "style": {"color": "#eee"}},
                        "categoryAxes": [
                            {
                                "id": "CategoryAxis-1",
                                "type": "category",
                                "position": "bottom",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear"},
                                "labels": {"show": True, "truncate": 100},
                                "title": {}
                            }
                        ],
                        "valueAxes": [
                            {
                                "id": "ValueAxis-1",
                                "name": "LeftAxis-1",
                                "type": "value",
                                "position": "left",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear", "mode": "normal"},
                                "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                                "title": {"text": "Count"}
                            }
                        ],
                        "seriesParams": [
                            {
                                "show": "true",
                                "type": "histogram",
                                "mode": "stacked",
                                "data": {"label": "Count", "id": "1"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "showCircles": True
                            }
                        ]
                    },
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "schema": "metric",
                            "params": {}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "terms",
                            "schema": "segment",
                            "params": {
                                "field": "municipality",
                                "size": 20,
                                "order": "desc",
                                "orderBy": "1"
                            }
                        }
                    ]
                }),
                "uiStateJSON": "{}",
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": self.events_index,
                        "query": {"match_all": {}},
                        "filter": []
                    })
                }
            }
        }
        
        events_map = {
            "id": "traffic-events-map",
            "type": "visualization",
            "attributes": {
                "title": "Mapa de Eventos de Tráfico",
                "visState": json.dumps({
                    "title": "Mapa de Eventos de Tráfico",
                    "type": "tile_map",
                    "params": {
                        "colorSchema": "Yellow to Red",
                        "mapType": "Scaled Circle Markers",
                        "isDesaturated": True,
                        "addTooltip": True,
                        "heatClusterSize": 1.5,
                        "legendPosition": "bottomright",
                        "mapZoom": 10,
                        "mapCenter": [-33.4489, -70.6693],
                        "wms": {
                            "enabled": False,
                            "options": {
                                "format": "image/png",
                                "transparent": True
                            }
                        }
                    },
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "schema": "metric",
                            "params": {}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "geohash_grid",
                            "schema": "segment",
                            "params": {
                                "field": "location",
                                "autoPrecision": True,
                                "precision": 2,
                                "useGeocentroid": True
                            }
                        }
                    ]
                }),
                "uiStateJSON": json.dumps({
                    "mapZoom": 10,
                    "mapCenter": [-33.4489, -70.6693]
                }),
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": self.events_index,
                        "query": {"match_all": {}},
                        "filter": []
                    })
                }
            }
        }
        
        events_timeline = {
            "id": "events-timeline",
            "type": "visualization",
            "attributes": {
                "title": "Timeline de Eventos",
                "visState": json.dumps({
                    "title": "Timeline de Eventos",
                    "type": "line",
                    "params": {
                        "grid": {"categoryLines": False, "style": {"color": "#eee"}},
                        "categoryAxes": [
                            {
                                "id": "CategoryAxis-1",
                                "type": "category",
                                "position": "bottom",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear"},
                                "labels": {"show": True, "truncate": 100},
                                "title": {}
                            }
                        ],
                        "valueAxes": [
                            {
                                "id": "ValueAxis-1",
                                "name": "LeftAxis-1",
                                "type": "value",
                                "position": "left",
                                "show": True,
                                "style": {},
                                "scale": {"type": "linear", "mode": "normal"},
                                "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                                "title": {"text": "Count"}
                            }
                        ],
                        "seriesParams": [
                            {
                                "show": "true",
                                "type": "line",
                                "mode": "normal",
                                "data": {"label": "Count", "id": "1"},
                                "valueAxis": "ValueAxis-1",
                                "drawLinesBetweenPoints": True,
                                "showCircles": True
                            }
                        ],
                        "addTimeMarker": False
                    },
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "schema": "metric",
                            "params": {}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "date_histogram",
                            "schema": "segment",
                            "params": {
                                "field": "timestamp",
                                "interval": "auto",
                                "customInterval": "2h",
                                "min_doc_count": 1,
                                "extended_bounds": {}
                            }
                        }
                    ]
                }),
                "uiStateJSON": "{}",
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": self.events_index,
                        "query": {"match_all": {}},
                        "filter": []
                    })
                }
            }
        }
        
        cache_dashboard = {
            "id": "cache-performance-dashboard",
            "type": "dashboard",
            "attributes": {
                "title": "Rendimiento del Sistema de Caché",
                "description": "Dashboard para monitorear el rendimiento del caché",
                "panelsJSON": json.dumps([
                    {
                        "gridData": {"x": 0, "y": 0, "w": 24, "h": 8},
                        "panelIndex": "1",
                        "embeddableConfig": {},
                        "panelRefName": "panel_cache_hit_rate"
                    },
                    {
                        "gridData": {"x": 24, "y": 0, "w": 24, "h": 8},
                        "panelIndex": "2",
                        "embeddableConfig": {},
                        "panelRefName": "panel_response_times"
                    }
                ]),
                "timeRestore": True,
                "timeTo": "now",
                "timeFrom": "now-4h"
            }
        }
        
        kibana_objects = [
            traffic_dashboard,
            municipality_viz,
            events_map,
            events_timeline,
            cache_dashboard
        ]
        
        created_count = 0
        for obj in kibana_objects:
            if self.create_kibana_object(obj):
                created_count += 1
        
        logger.info(f"Creados {created_count}/{len(kibana_objects)} objetos en Kibana")
        return created_count
    
    def create_kibana_object(self, kibana_object: Dict) -> bool:
        """Crea un objeto en Kibana via API"""
        
        try:
            kibana_api_url = f"{self.kibana_uri}/api/saved_objects/{kibana_object['type']}/{kibana_object['id']}"
            
            headers = {
                'Content-Type': 'application/json',
                'kbn-xsrf': 'true'
            }
            
            response = requests.post(
                kibana_api_url,
                headers=headers,
                json=kibana_object['attributes'],
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"Objeto Kibana creado: {kibana_object['id']}")
                return True
            else:
                logger.warning(f"Error creando objeto Kibana {kibana_object['id']}: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Excepción creando objeto Kibana {kibana_object.get('id', 'unknown')}: {e}")
            return False
    
    def sync_cache_metrics(self, cache_uri: str):
        """Sincroniza métricas del sistema de caché"""
        
        try:
            response = requests.get(f"{cache_uri}/cache/stats", timeout=10)
            
            if response.status_code == 200:
                stats = response.json()
                
                es_doc = {
                    'timestamp': datetime.now().isoformat(),
                    'cache_policy': stats.get('policy', 'unknown'),
                    'hit_rate': stats.get('hit_rate', 0),
                    'miss_rate': stats.get('miss_rate', 0),
                    'total_hits': stats.get('hits', 0),
                    'total_misses': stats.get('misses', 0),
                    'cache_size': stats.get('size', 0),
                    'capacity': stats.get('capacity', 0)
                }
                
                performance = stats.get('performance', {})
                if performance:
                    es_doc.update({
                        'avg_response_time': performance.get('avg_query_time', 0),
                        'avg_cache_time': performance.get('avg_cache_time', 0),
                        'avg_db_time': performance.get('avg_db_time', 0)
                    })
                
                self.es.index(
                    index=self.cache_metrics_index,
                    document=es_doc
                )
                
                logger.info("Métricas de caché sincronizadas")
                return True
                
        except Exception as e:
            logger.error(f"Error sincronizando métricas de caché: {e}")
            return False
    
    def create_index_patterns(self):
        """Crea patrones de índice en Kibana"""
        
        index_patterns = [
            {
                'id': 'traffic-events-pattern',
                'title': self.events_index,
                'timeFieldName': 'timestamp'
            },
            {
                'id': 'traffic-analysis-pattern',
                'title': self.analysis_index,
                'timeFieldName': 'timestamp'
            },
            {
                'id': 'cache-metrics-pattern',
                'title': self.cache_metrics_index,
                'timeFieldName': 'timestamp'
            }
        ]
        
        created_count = 0
        for pattern in index_patterns:
            try:
                url = f"{self.kibana_uri}/api/saved_objects/index-pattern/{pattern['id']}"
                headers = {
                    'Content-Type': 'application/json',
                    'kbn-xsrf': 'true'
                }
                
                data = {
                    'attributes': {
                        'title': pattern['title'],
                        'timeFieldName': pattern['timeFieldName']
                    }
                }
                
                response = requests.post(url, headers=headers, json=data, timeout=30)
                
                if response.status_code in [200, 201]:
                    logger.info(f"Patrón de índice creado: {pattern['id']}")
                    created_count += 1
                else:
                    logger.warning(f"Error creando patrón {pattern['id']}: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"Error creando patrón {pattern['id']}: {e}")
        
        return created_count
    
    def run_full_sync(self, cache_uri: str = None):
        """Ejecuta sincronización completa del sistema"""
        
        logger.info("Iniciando sincronización completa")
        
        results = {
            'start_time': datetime.now(),
            'operations': {}
        }
        
        try:
            self.create_elasticsearch_indices()
            results['operations']['indices_created'] = True
            
            events_synced = self.sync_events_to_elasticsearch()
            results['operations']['events_synced'] = events_synced
            
            analysis_synced = self.sync_analysis_results_to_elasticsearch()
            results['operations']['analysis_synced'] = analysis_synced
            
            if cache_uri:
                cache_synced = self.sync_cache_metrics(cache_uri)
                results['operations']['cache_metrics_synced'] = cache_synced
            
            patterns_created = self.create_index_patterns()
            results['operations']['index_patterns_created'] = patterns_created
            
            dashboards_created = self.create_kibana_dashboards()
            results['operations']['dashboards_created'] = dashboards_created
            
            results['success'] = True
            results['end_time'] = datetime.now()
            results['duration'] = (results['end_time'] - results['start_time']).total_seconds()
            
            logger.info(f"Sincronización completa exitosa en {results['duration']:.2f} segundos")
            
        except Exception as e:
            logger.error(f"Error en sincronización completa: {e}")
            results['success'] = False
            results['error'] = str(e)
            results['end_time'] = datetime.now()
        
        return results
    
    def get_visualization_stats(self) -> Dict:
        """Obtiene estadísticas del sistema de visualización"""
        
        try:
            stats = {}
            
            for index in [self.events_index, self.analysis_index, self.cache_metrics_index]:
                try:
                    if self.es.indices.exists(index=index):
                        count = self.es.count(index=index)['count']
                        stats[f"{index}_documents"] = count
                    else:
                        stats[f"{index}_documents"] = 0
                except:
                    stats[f"{index}_documents"] = 0
            
            try:
                cluster_health = self.es.cluster.health()
                stats['elasticsearch_status'] = cluster_health['status']
                stats['elasticsearch_nodes'] = cluster_health['number_of_nodes']
            except:
                stats['elasticsearch_status'] = 'unknown'
            
            try:
                mongodb_stats = {
                    'raw_events': self.db.waze_events.count_documents({}),
                    'processed_events': self.db.processed_events.count_documents({}),
                    'analysis_results': self.db.analysis_results.count_documents({})
                }
                stats.update(mongodb_stats)
            except Exception as e:
                stats['mongodb_error'] = str(e)
            
            stats['last_updated'] = datetime.now().isoformat()
            
            return stats
            
        except Exception as e:
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}

def main():
    """Función principal del módulo de visualización"""
    
    elasticsearch_uri = os.getenv('ELASTICSEARCH_URI', 'http://localhost:9200')
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@localhost:27017/traffic_db?authSource=admin')
    cache_uri = os.getenv('CACHE_URI', 'http://localhost:8080')
    
    logger.info("Esperando que Elasticsearch esté disponible...")
    for attempt in range(30):
        try:
            es_test = Elasticsearch([elasticsearch_uri])
            if es_test.ping():
                logger.info("Elasticsearch disponible")
                break
        except:
            pass
        
        logger.info(f"Intento {attempt + 1}/30 - Elasticsearch no disponible, esperando...")
        time.sleep(10)
    else:
        logger.error("Elasticsearch no disponible después de 5 minutos")
        return
    
    viz_system = TrafficVisualizationSystem(elasticsearch_uri, mongodb_uri)
    
    mode = os.getenv('VISUALIZATION_MODE', 'full_sync')
    
    if mode == 'full_sync':
        results = viz_system.run_full_sync(cache_uri)
        logger.info(f"Resultados de sincronización: {json.dumps(results, default=str, indent=2)}")
        
    elif mode == 'events_only':
        viz_system.create_elasticsearch_indices()
        events_synced = viz_system.sync_events_to_elasticsearch()
        logger.info(f"Eventos sincronizados: {events_synced}")
        
    elif mode == 'continuous':
        logger.info("Iniciando sincronización continua")
        while True:
            try:
                viz_system.sync_events_to_elasticsearch(limit=1000)
                viz_system.sync_analysis_results_to_elasticsearch()
                viz_system.sync_cache_metrics(cache_uri)
                
                logger.info("Ciclo de sincronización completado")
                time.sleep(300) 
                
            except KeyboardInterrupt:
                logger.info("Sincronización continua interrumpida")
                break
            except Exception as e:
                logger.error(f"Error en sincronización continua: {e}")
                time.sleep(60)
                
    elif mode == 'stats':
        
        stats = viz_system.get_visualization_stats()
        logger.info(f"Estadísticas de visualización: {json.dumps(stats, indent=2)}")
        
    else:
        logger.error(f"Modo no reconocido: {mode}")

if __name__ == "__main__":
    main()