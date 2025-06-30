

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import redis
from flask import Flask, request, jsonify
from pymongo import MongoClient
import hashlib

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CachePolicy(ABC):
    """Clase abstracta para políticas de caché"""
    
    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        pass
    
    @abstractmethod
    def put(self, key: str, value: Any) -> None:
        pass
    
    @abstractmethod
    def size(self) -> int:
        pass
    
    @abstractmethod
    def clear(self) -> None:
        pass
    
    @abstractmethod
    def stats(self) -> Dict:
        pass

class LRUCache(CachePolicy):
    """Implementación de caché LRU (Least Recently Used)"""
    
    def __init__(self, capacity: int):
        self.capacity = capacity
        self.cache = OrderedDict()
        self.hits = 0
        self.misses = 0
    
    def get(self, key: str) -> Optional[Any]:
        if key in self.cache:
            self.cache.move_to_end(key)
            self.hits += 1
            return self.cache[key]
        
        self.misses += 1
        return None
    
    def put(self, key: str, value: Any) -> None:
        if key in self.cache:
            self.cache.move_to_end(key)
        elif len(self.cache) >= self.capacity:
            self.cache.popitem(last=False)
        
        self.cache[key] = value
    
    def size(self) -> int:
        return len(self.cache)
    
    def clear(self) -> None:
        self.cache.clear()
        self.hits = 0
        self.misses = 0
    
    def stats(self) -> Dict:
        total_requests = self.hits + self.misses
        hit_rate = self.hits / total_requests if total_requests > 0 else 0
        
        return {
            'policy': 'LRU',
            'capacity': self.capacity,
            'size': len(self.cache),
            'hits': self.hits,
            'misses': self.misses,
            'hit_rate': hit_rate,
            'miss_rate': 1 - hit_rate
        }


class TrafficCacheSystem:
    """Sistema de caché para consultas de tráfico"""
    
    def __init__(self, redis_uri: str, mongodb_uri: str, cache_policy: str = 'LRU', cache_size: int = 1000):
        self.redis_client = redis.from_url(redis_uri, decode_responses=True)
        self.mongodb_client = MongoClient(mongodb_uri)
        self.db = self.mongodb_client.traffic_db
        self.collection = self.db.waze_events
        
        self.cache_policy_name = cache_policy
        if cache_policy == 'LRU':
            self.cache = LRUCache(cache_size)
        elif cache_policy == 'LFU':
            self.cache = LFUCache(cache_size)
        elif cache_policy == 'FIFO':
            self.cache = FIFOCache(cache_size)
        else:
            raise ValueError(f"Política de caché no soportada: {cache_policy}")
        
        self.query_times = []
        self.cache_times = []
        self.db_times = []
        
        logger.info(f"Sistema de caché inicializado con política {cache_policy}")
    
    def _generate_cache_key(self, query: Dict) -> str:
        """Genera una clave única para la consulta"""
        query_str = json.dumps(query, sort_keys=True)
        return hashlib.md5(query_str.encode()).hexdigest()
    
    def _execute_db_query(self, query: Dict) -> List[Dict]:
        """Ejecuta consulta en la base de datos"""
        start_time = time.time()
        
        try:
            mongo_filter = {}
            
            if 'municipality' in query:
                mongo_filter['municipality'] = query['municipality']
            
            if 'event_type' in query:
                mongo_filter['event_type'] = query['event_type']
            
            if 'date_range' in query:
                date_range = query['date_range']
                if 'start' in date_range or 'end' in date_range:
                    mongo_filter['timestamp'] = {}
                    if 'start' in date_range:
                        mongo_filter['timestamp']['$gte'] = datetime.fromisoformat(date_range['start'])
                    if 'end' in date_range:
                        mongo_filter['timestamp']['$lte'] = datetime.fromisoformat(date_range['end'])
            
            if 'location' in query:
                location = query['location']
                if 'radius' in location and 'lat' in location and 'lng' in location:
                    mongo_filter['location'] = {
                        '$geoWithin': {
                            '$centerSphere': [
                                [location['lng'], location['lat']],
                                location['radius'] / 6371  
                            ]
                        }
                    }
            
            
            cursor = self.collection.find(mongo_filter)
            
            
            if 'limit' in query:
                cursor = cursor.limit(query['limit'])
            else:
                cursor = cursor.limit(1000)  
            
            cursor = cursor.sort('timestamp', -1)
            
            results = []
            for doc in cursor:
                doc['_id'] = str(doc['_id'])  
                if 'timestamp' in doc:
                    doc['timestamp'] = doc['timestamp'].isoformat()
                results.append(doc)
            
            db_time = time.time() - start_time
            self.db_times.append(db_time)
            
            logger.info(f"Consulta DB ejecutada en {db_time:.3f}s, {len(results)} resultados")
            return results
            
        except Exception as e:
            logger.error(f"Error ejecutando consulta DB: {e}")
            return []
    
    def get_traffic_data(self, query: Dict) -> Tuple[List[Dict], Dict]:
        """
        Obtiene datos de tráfico, usando caché si está disponible
        Retorna (datos, métricas)
        """
        start_time = time.time()
        
        cache_key = self._generate_cache_key(query)
        
        cache_start = time.time()
        cached_result = self.cache.get(cache_key)
        cache_time = time.time() - cache_start
        
        if cached_result is not None:
            self.cache_times.append(cache_time)
            total_time = time.time() - start_time
            self.query_times.append(total_time)
            
            metrics = {
                'cache_hit': True,
                'total_time': total_time,
                'cache_time': cache_time,
                'db_time': 0,
                'result_count': len(cached_result)
            }
            
            logger.info(f"Cache HIT para consulta en {total_time:.3f}s")
            return cached_result, metrics
        
        results = self._execute_db_query(query)
        
        self.cache.put(cache_key, results)
        
        try:
            self.redis_client.setex(
                f"traffic_cache:{cache_key}",
                3600,  
                json.dumps(results)
            )
        except Exception as e:
            logger.warning(f"Error guardando en Redis: {e}")
        
        total_time = time.time() - start_time
        self.query_times.append(total_time)
        self.cache_times.append(cache_time)
        
        metrics = {
            'cache_hit': False,
            'total_time': total_time,
            'cache_time': cache_time,
            'db_time': self.db_times[-1] if self.db_times else 0,
            'result_count': len(results)
        }
        
        logger.info(f"Cache MISS para consulta en {total_time:.3f}s")
        return results, metrics
    
    def get_aggregated_data(self, aggregation_type: str, params: Dict = None) -> Tuple[List[Dict], Dict]:
        """Obtiene datos agregados con caché"""
        if params is None:
            params = {}
        
        query = {
            'aggregation': aggregation_type,
            'params': params
        }
        
        cache_key = self._generate_cache_key(query)
        cached_result = self.cache.get(cache_key)
        
        if cached_result is not None:
            return cached_result, {'cache_hit': True}
        
        start_time = time.time()
        
        try:
            if aggregation_type == 'events_by_municipality':
                pipeline = [
                    {'$group': {
                        '_id': '$municipality',
                        'count': {'$sum': 1},
                        'last_event': {'$max': '$timestamp'}
                    }},
                    {'$sort': {'count': -1}}
                ]
                
            elif aggregation_type == 'events_by_type':
                pipeline = [
                    {'$group': {
                        '_id': '$event_type',
                        'count': {'$sum': 1}
                    }},
                    {'$sort': {'count': -1}}
                ]
                
            elif aggregation_type == 'hourly_distribution':
                pipeline = [
                    {'$group': {
                        '_id': {'$hour': '$timestamp'},
                        'count': {'$sum': 1}
                    }},
                    {'$sort': {'_id': 1}}
                ]
                
            elif aggregation_type == 'recent_events':
                hours_back = params.get('hours', 24)
                cutoff_time = datetime.now() - timedelta(hours=hours_back)
                
                pipeline = [
                    {'$match': {'timestamp': {'$gte': cutoff_time}}},
                    {'$sort': {'timestamp': -1}},
                    {'$limit': params.get('limit', 100)}
                ]
                
            else:
                return [], {'error': f'Tipo de agregación no soportado: {aggregation_type}'}
            
            results = list(self.collection.aggregate(pipeline))
            
            for result in results:
                if '_id' in result and hasattr(result['_id'], 'isoformat'):
                    result['_id'] = result['_id'].isoformat()
                if 'last_event' in result and hasattr(result['last_event'], 'isoformat'):
                    result['last_event'] = result['last_event'].isoformat()
            
            self.cache.put(cache_key, results)
            
            db_time = time.time() - start_time
            
            metrics = {
                'cache_hit': False,
                'db_time': db_time,
                'result_count': len(results)
            }
            
            return results, metrics
            
        except Exception as e:
            logger.error(f"Error en agregación {aggregation_type}: {e}")
            return [], {'error': str(e)}
    
    def clear_cache(self) -> Dict:
        """Limpia el caché y retorna estadísticas"""
        stats_before = self.cache.stats()
        self.cache.clear()
        
        try:
            # Limpiar también Redis
            keys = self.redis_client.keys("traffic_cache:*")
            if keys:
                self.redis_client.delete(*keys)
            redis_cleared = len(keys)
        except Exception as e:
            logger.warning(f"Error limpiando Redis: {e}")
            redis_cleared = 0
        
        return {
            'local_cache_cleared': stats_before['size'],
            'redis_keys_cleared': redis_cleared,
            'previous_stats': stats_before
        }
    
    def get_cache_stats(self) -> Dict:
        """Retorna estadísticas detalladas del caché"""
        base_stats = self.cache.stats()
        
        # Estadísticas de rendimiento
        avg_query_time = sum(self.query_times) / len(self.query_times) if self.query_times else 0
        avg_cache_time = sum(self.cache_times) / len(self.cache_times) if self.cache_times else 0
        avg_db_time = sum(self.db_times) / len(self.db_times) if self.db_times else 0
        
        # Información de Redis
        try:
            redis_info = self.redis_client.info()
            redis_memory = redis_info.get('used_memory_human', 'N/A')
            redis_keys = len(self.redis_client.keys("traffic_cache:*"))
        except Exception:
            redis_memory = 'N/A'
            redis_keys = 0
        
        return {
            **base_stats,
            'performance': {
                'avg_query_time': avg_query_time,
                'avg_cache_time': avg_cache_time,
                'avg_db_time': avg_db_time,
                'total_queries': len(self.query_times)
            },
            'redis': {
                'memory_usage': redis_memory,
                'cached_keys': redis_keys
            },
            'database': {
                'total_events': self.collection.count_documents({})
            }
        }


app = Flask(__name__)


redis_uri = os.getenv('REDIS_URI', 'redis://localhost:6379')
mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@localhost:27017/traffic_db?authSource=admin')
cache_policy = os.getenv('CACHE_POLICY', 'LRU')
cache_size = int(os.getenv('CACHE_SIZE', '1000'))

cache_system = TrafficCacheSystem(redis_uri, mongodb_uri, cache_policy, cache_size)

@app.route('/query', methods=['POST'])
def query_traffic_data():
    """Endpoint para consultar datos de tráfico"""
    try:
        query = request.get_json()
        if not query:
            return jsonify({'error': 'Query JSON requerido'}), 400
        
        results, metrics = cache_system.get_traffic_data(query)
        
        return jsonify({
            'data': results,
            'metrics': metrics,
            'query': query
        })
        
    except Exception as e:
        logger.error(f"Error en query endpoint: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/aggregation/<aggregation_type>', methods=['GET', 'POST'])
def get_aggregation(aggregation_type):
    """Endpoint para consultas agregadas"""
    try:
        if request.method == 'POST':
            params = request.get_json() or {}
        else:
            params = request.args.to_dict()
        
        results, metrics = cache_system.get_aggregated_data(aggregation_type, params)
        
        return jsonify({
            'data': results,
            'metrics': metrics,
            'aggregation_type': aggregation_type,
            'params': params
        })
        
    except Exception as e:
        logger.error(f"Error en aggregation endpoint: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/cache/stats', methods=['GET'])
def get_cache_stats():
    """Endpoint para estadísticas del caché"""
    try:
        stats = cache_system.get_cache_stats()
        return jsonify(stats)
        
    except Exception as e:
        logger.error(f"Error obteniendo estadísticas: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/cache/clear', methods=['POST'])
def clear_cache():
    """Endpoint para limpiar el caché"""
    try:
        result = cache_system.clear_cache()
        return jsonify(result)
        
    except Exception as e:
        logger.error(f"Error limpiando caché: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Verificar conexiones
        cache_system.redis_client.ping()
        cache_system.mongodb_client.admin.command('ping')
        
        return jsonify({
            'status': 'healthy',
            'cache_policy': cache_policy,
            'cache_size': cache_size,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 500

if __name__ == '__main__':
    logger.info(f"Iniciando servidor de caché con política {cache_policy}")
    app.run(host='0.0.0.0', port=8080, debug=True)