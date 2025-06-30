import asyncio
import json
import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List

import aiohttp
import numpy as np
from pymongo import MongoClient


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TrafficGenerator:
    """Generador de tráfico sintético para pruebas del sistema de caché"""
    
    def __init__(self, mongodb_uri: str, cache_uri: str, poisson_lambda: float = 10, exponential_lambda: float = 0.1):
        self.mongodb_uri = mongodb_uri
        self.cache_uri = cache_uri
        self.poisson_lambda = poisson_lambda
        self.exponential_lambda = exponential_lambda
        
   
        self.mongodb_client = MongoClient(mongodb_uri)
        self.db = self.mongodb_client.traffic_db
        self.collection = self.db.waze_events
        
      
        self.requests_sent = 0
        self.responses_received = 0
        self.errors = 0
        self.response_times = []
        
        # Patrones de consulta
        self.query_patterns = self._define_query_patterns()
        
        logger.info("Generador de tráfico inicializado")
    
    def _define_query_patterns(self) -> List[Dict]:
        """Define patrones de consulta realistas"""
        
        
        try:
            municipalities = list(self.collection.distinct('municipality'))
            if not municipalities:
                municipalities = ['Santiago', 'Las Condes', 'Providencia', 'Ñuñoa', 'Maipú']
        except:
            municipalities = ['Santiago', 'Las Condes', 'Providencia', 'Ñuñoa', 'Maipú']
        
        event_types = ['alert', 'jam', 'accident', 'road_closure', 'hazard']
        
        patterns = [
            
            {
                'type': 'municipality_query',
                'weight': 0.3,
                'template': {
                    'municipality': None,  
                    'limit': 100
                }
            },
            
            
            {
                'type': 'event_type_query',
                'weight': 0.2,
                'template': {
                    'event_type': None,  
                    'limit': 50
                }
            },
            
          
            {
                'type': 'recent_events',
                'weight': 0.25,
                'template': {
                    'date_range': {
                        'start': None,  
                        'end': None
                    },
                    'limit': 200
                }
            },
            
         
            {
                'type': 'location_query',
                'weight': 0.15,
                'template': {
                    'location': {
                        'lat': -33.4489,  
                        'lng': -70.6693,
                        'radius': 5  
                    },
                    'limit': 150
                }
            },
            
        
            {
                'type': 'combined_query',
                'weight': 0.1,
                'template': {
                    'municipality': None,
                    'event_type': None,
                    'limit': 75
                }
            }
        ]
        
        self.municipalities = municipalities
        self.event_types = event_types
        
        return patterns
    
    def generate_query(self) -> Dict:
        """Genera una consulta basada en los patrones definidos"""
        
       
        weights = [pattern['weight'] for pattern in self.query_patterns]
        pattern = np.random.choice(self.query_patterns, p=weights)
        
     
        query = pattern['template'].copy()
        
        if pattern['type'] == 'municipality_query':
            query['municipality'] = random.choice(self.municipalities)
            
        elif pattern['type'] == 'event_type_query':
            query['event_type'] = random.choice(self.event_types)
            
        elif pattern['type'] == 'recent_events':
            end_time = datetime.now()
            hours_back = random.choice([1, 6, 12, 24, 48]) 
            start_time = end_time - timedelta(hours=hours_back)
            
            query['date_range'] = {
                'start': start_time.isoformat(),
                'end': end_time.isoformat()
            }
            
        elif pattern['type'] == 'location_query':
            lat_offset = random.uniform(-0.1, 0.1)
            lng_offset = random.uniform(-0.1, 0.1)
            
            query['location'] = {
                'lat': -33.4489 + lat_offset,
                'lng': -70.6693 + lng_offset,
                'radius': random.choice([1, 2, 5, 10, 15])
            }
            
        elif pattern['type'] == 'combined_query':
            query['municipality'] = random.choice(self.municipalities)
            query['event_type'] = random.choice(self.event_types)
        
        return query
    
    def generate_aggregation_query(self) -> tuple:
        """Genera consultas de agregación"""
        
        aggregation_types = [
            ('events_by_municipality', {}),
            ('events_by_type', {}),
            ('hourly_distribution', {}),
            ('recent_events', {'hours': random.choice([1, 6, 12, 24]), 'limit': random.choice([50, 100, 200])})
        ]
        
        return random.choice(aggregation_types)
    
    async def send_query(self, session: aiohttp.ClientSession, query: Dict) -> Dict:
        """Envía una consulta al sistema de caché"""
        
        start_time = time.time()
        
        try:
            async with session.post(
                f"{self.cache_uri}/query",
                json=query,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                response_data = await response.json()
                response_time = time.time() - start_time
                
                self.requests_sent += 1
                self.responses_received += 1
                self.response_times.append(response_time)
                
                return {
                    'success': True,
                    'response_time': response_time,
                    'status_code': response.status,
                    'cache_hit': response_data.get('metrics', {}).get('cache_hit', False),
                    'result_count': response_data.get('metrics', {}).get('result_count', 0),
                    'query': query
                }
                
        except Exception as e:
            self.errors += 1
            response_time = time.time() - start_time
            
            logger.error(f"Error enviando consulta: {e}")
            
            return {
                'success': False,
                'response_time': response_time,
                'error': str(e),
                'query': query
            }
    
    async def send_aggregation_query(self, session: aiohttp.ClientSession, agg_type: str, params: Dict) -> Dict:
        """Envía una consulta de agregación"""
        
        start_time = time.time()
        
        try:
            async with session.post(
                f"{self.cache_uri}/aggregation/{agg_type}",
                json=params,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                
                response_data = await response.json()
                response_time = time.time() - start_time
                
                self.requests_sent += 1
                self.responses_received += 1
                self.response_times.append(response_time)
                
                return {
                    'success': True,
                    'response_time': response_time,
                    'status_code': response.status,
                    'cache_hit': response_data.get('metrics', {}).get('cache_hit', False),
                    'result_count': response_data.get('metrics', {}).get('result_count', 0),
                    'aggregation_type': agg_type,
                    'params': params
                }
                
        except Exception as e:
            self.errors += 1
            response_time = time.time() - start_time
            
            return {
                'success': False,
                'response_time': response_time,
                'error': str(e),
                'aggregation_type': agg_type,
                'params': params
            }
    
    def poisson_arrival_times(self, duration_seconds: int) -> List[float]:
        """Genera tiempos de llegada siguiendo una distribución de Poisson"""
        
        arrival_times = []
        current_time = 0
        
        while current_time < duration_seconds:
            inter_arrival_time = np.random.exponential(1.0 / self.poisson_lambda)
            current_time += inter_arrival_time
            
            if current_time < duration_seconds:
                arrival_times.append(current_time)
        
        return arrival_times
    
    def exponential_arrival_times(self, duration_seconds: int) -> List[float]:
        """Genera tiempos de llegada siguiendo una distribución exponencial"""
        
        arrival_times = []
        current_time = 0
        
        while current_time < duration_seconds:
            inter_arrival_time = np.random.exponential(1.0 / self.exponential_lambda)
            current_time += inter_arrival_time
            
            if current_time < duration_seconds:
                arrival_times.append(current_time)
        
        return arrival_times
    
    async def run_poisson_traffic(self, duration_minutes: int = 10):
        """Ejecuta tráfico siguiendo distribución de Poisson"""
        
        logger.info(f"Iniciando tráfico Poisson (λ={self.poisson_lambda}) por {duration_minutes} minutos")
        
        duration_seconds = duration_minutes * 60
        arrival_times = self.poisson_arrival_times(duration_seconds)
        
        logger.info(f"Generadas {len(arrival_times)} llegadas para distribución Poisson")
        
        start_time = time.time()
        results = []
        
        async with aiohttp.ClientSession() as session:
            for arrival_time in arrival_times:
                elapsed = time.time() - start_time
                wait_time = arrival_time - elapsed
                
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                
                if random.random() < 0.8:
                    query = self.generate_query()
                    result = await self.send_query(session, query)
                else:
                    agg_type, params = self.generate_aggregation_query()
                    result = await self.send_aggregation_query(session, agg_type, params)
                
                results.append(result)
                
                if len(results) % 50 == 0:
                    cache_hits = sum(1 for r in results if r.get('cache_hit', False))
                    hit_rate = cache_hits / len(results) if results else 0
                    avg_response_time = np.mean([r['response_time'] for r in results])
                    logger.info(f"Progreso: {len(results)} consultas, Hit rate: {hit_rate:.2%}, Tiempo promedio: {avg_response_time:.3f}s")
        
        return self.analyze_results(results, "Poisson")
    
    async def run_exponential_traffic(self, duration_minutes: int = 10):
        """Ejecuta tráfico siguiendo distribución exponencial"""
        
        logger.info(f"Iniciando tráfico Exponencial (λ={self.exponential_lambda}) por {duration_minutes} minutos")
        
        duration_seconds = duration_minutes * 60
        arrival_times = self.exponential_arrival_times(duration_seconds)
        
        logger.info(f"Generadas {len(arrival_times)} llegadas para distribución Exponencial")
        
        start_time = time.time()
        results = []
        
        async with aiohttp.ClientSession() as session:
            for arrival_time in arrival_times:
                elapsed = time.time() - start_time
                wait_time = arrival_time - elapsed
                
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                
                if random.random() < 0.8:
                    query = self.generate_query()
                    result = await self.send_query(session, query)
                else:
                    agg_type, params = self.generate_aggregation_query()
                    result = await self.send_aggregation_query(session, agg_type, params)
                
                results.append(result)
                
                if len(results) % 50 == 0:
                    cache_hits = sum(1 for r in results if r.get('cache_hit', False))
                    hit_rate = cache_hits / len(results) if results else 0
                    avg_response_time = np.mean([r['response_time'] for r in results])
                    logger.info(f"Progreso: {len(results)} consultas, Hit rate: {hit_rate:.2%}, Tiempo promedio: {avg_response_time:.3f}s")
        
        return self.analyze_results(results, "Exponencial")
    
    async def run_burst_traffic(self, num_bursts: int = 5, burst_size: int = 20, burst_interval: int = 30):
        """Ejecuta tráfico en ráfagas para probar el comportamiento del caché"""
        
        logger.info(f"Iniciando tráfico en ráfagas: {num_bursts} ráfagas de {burst_size} consultas cada {burst_interval}s")
        
        all_results = []
        
        async with aiohttp.ClientSession() as session:
            for burst_num in range(num_bursts):
                logger.info(f"Ejecutando ráfaga {burst_num + 1}/{num_bursts}")
                
                burst_tasks = []
                
                for _ in range(burst_size):
                    if random.random() < 0.7:
                        query = self.generate_query()
                        task = self.send_query(session, query)
                    else:
                        agg_type, params = self.generate_aggregation_query()
                        task = self.send_aggregation_query(session, agg_type, params)
                    
                    burst_tasks.append(task)
                
                burst_results = await asyncio.gather(*burst_tasks, return_exceptions=True)
                
                valid_results = [r for r in burst_results if isinstance(r, dict)]
                all_results.extend(valid_results)
                
                cache_hits = sum(1 for r in valid_results if r.get('cache_hit', False))
                hit_rate = cache_hits / len(valid_results) if valid_results else 0
                avg_response_time = np.mean([r['response_time'] for r in valid_results]) if valid_results else 0
                
                logger.info(f"Ráfaga {burst_num + 1} completada: {len(valid_results)} consultas, Hit rate: {hit_rate:.2%}, Tiempo promedio: {avg_response_time:.3f}s")
                
                if burst_num < num_bursts - 1:
                    await asyncio.sleep(burst_interval)
        
        return self.analyze_results(all_results, "Ráfagas")
    
    def analyze_results(self, results: List[Dict], traffic_type: str) -> Dict:
        """Analiza los resultados de las pruebas"""
        
        if not results:
            return {'error': 'No hay resultados para analizar'}
        
        successful_results = [r for r in results if r.get('success', False)]
        cache_hits = [r for r in successful_results if r.get('cache_hit', False)]
        cache_misses = [r for r in successful_results if not r.get('cache_hit', False)]
        
        response_times = [r['response_time'] for r in successful_results]
        cache_hit_times = [r['response_time'] for r in cache_hits]
        cache_miss_times = [r['response_time'] for r in cache_misses]
        
        analysis = {
            'traffic_type': traffic_type,
            'summary': {
                'total_requests': len(results),
                'successful_requests': len(successful_results),
                'failed_requests': len(results) - len(successful_results),
                'success_rate': len(successful_results) / len(results) if results else 0
            },
            'cache_performance': {
                'total_hits': len(cache_hits),
                'total_misses': len(cache_misses),
                'hit_rate': len(cache_hits) / len(successful_results) if successful_results else 0,
                'miss_rate': len(cache_misses) / len(successful_results) if successful_results else 0
            },
            'response_times': {
                'overall': {
                    'mean': float(np.mean(response_times)) if response_times else 0,
                    'median': float(np.median(response_times)) if response_times else 0,
                    'std': float(np.std(response_times)) if response_times else 0,
                    'min': float(np.min(response_times)) if response_times else 0,
                    'max': float(np.max(response_times)) if response_times else 0,
                    'p95': float(np.percentile(response_times, 95)) if response_times else 0,
                    'p99': float(np.percentile(response_times, 99)) if response_times else 0
                },
                'cache_hits': {
                    'mean': float(np.mean(cache_hit_times)) if cache_hit_times else 0,
                    'median': float(np.median(cache_hit_times)) if cache_hit_times else 0,
                    'std': float(np.std(cache_hit_times)) if cache_hit_times else 0
                } if cache_hit_times else {},
                'cache_misses': {
                    'mean': float(np.mean(cache_miss_times)) if cache_miss_times else 0,
                    'median': float(np.median(cache_miss_times)) if cache_miss_times else 0,
                    'std': float(np.std(cache_miss_times)) if cache_miss_times else 0
                } if cache_miss_times else {}
            }
        }
        
        if cache_hit_times and cache_miss_times:
            analysis['cache_speedup'] = {
                'mean_speedup': float(np.mean(cache_miss_times) / np.mean(cache_hit_times)),
                'median_speedup': float(np.median(cache_miss_times) / np.median(cache_hit_times))
            }
        
        return analysis
    
    async def run_comparative_test(self, duration_minutes: int = 10):
        """Ejecuta pruebas comparativas con ambas distribuciones"""
        
        logger.info("Iniciando pruebas comparativas de distribuciones")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.cache_uri}/cache/clear") as response:
                    clear_result = await response.json()
                    logger.info(f"Caché limpiado: {clear_result}")
        except Exception as e:
            logger.warning(f"Error limpiando caché: {e}")
        
        results = {}
        
        logger.info("=== Iniciando prueba con distribución Poisson ===")
        self.reset_stats()
        results['poisson'] = await self.run_poisson_traffic(duration_minutes)
        
        await asyncio.sleep(10)
        
        logger.info("=== Iniciando prueba con distribución Exponencial ===")
        self.reset_stats()
        results['exponential'] = await self.run_exponential_traffic(duration_minutes)
        
        await asyncio.sleep(10)
        
        logger.info("=== Iniciando prueba con tráfico en ráfagas ===")
        self.reset_stats()
        results['burst'] = await self.run_burst_traffic()
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.cache_uri}/cache/stats") as response:
                    cache_stats = await response.json()
                    results['final_cache_stats'] = cache_stats
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas finales: {e}")
        
        return results
    
    def reset_stats(self):
        """Reinicia las estadísticas del generador"""
        self.requests_sent = 0
        self.responses_received = 0
        self.errors = 0
        self.response_times = []
    
    def get_generator_stats(self) -> Dict:
        """Retorna estadísticas del generador"""
        return {
            'requests_sent': self.requests_sent,
            'responses_received': self.responses_received,
            'errors': self.errors,
            'error_rate': self.errors / self.requests_sent if self.requests_sent > 0 else 0,
            'avg_response_time': np.mean(self.response_times) if self.response_times else 0,
            'total_response_times': len(self.response_times)
        }
    
    async def run_continuous_traffic(self, hours: int = 1):
        """Ejecuta tráfico continuo mezclando ambas distribuciones"""
        
        logger.info(f"Iniciando tráfico continuo por {hours} horas")
        
        end_time = time.time() + (hours * 3600)
        results = []
        
        async with aiohttp.ClientSession() as session:
            while time.time() < end_time:
                current_minute = int((time.time() - (end_time - hours * 3600)) / 60)
                use_poisson = (current_minute // 10) % 2 == 0
                
                if use_poisson:
                    wait_time = np.random.exponential(1.0 / self.poisson_lambda)
                else:
                    wait_time = np.random.exponential(1.0 / self.exponential_lambda)
                
                await asyncio.sleep(wait_time)
                
                if random.random() < 0.8:
                    query = self.generate_query()
                    result = await self.send_query(session, query)
                else:
                    agg_type, params = self.generate_aggregation_query()
                    result = await self.send_aggregation_query(session, agg_type, params)
                
                results.append(result)
                
                if len(results) % 100 == 0:
                    recent_results = results[-100:]
                    cache_hits = sum(1 for r in recent_results if r.get('cache_hit', False))
                    hit_rate = cache_hits / len(recent_results)
                    avg_time = np.mean([r['response_time'] for r in recent_results])
                    
                    logger.info(f"Tráfico continuo: {len(results)} consultas totales, "
                              f"Hit rate reciente: {hit_rate:.2%}, "
                              f"Tiempo promedio: {avg_time:.3f}s, "
                              f"Distribución actual: {'Poisson' if use_poisson else 'Exponencial'}")
        
        return self.analyze_results(results, "Continuo")

async def main():
    """Función principal del generador de tráfico"""
    
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@localhost:27017/traffic_db?authSource=admin')
    cache_uri = os.getenv('CACHE_URI', 'http://localhost:8080')
    poisson_lambda = float(os.getenv('POISSON_LAMBDA', '10'))
    exponential_lambda = float(os.getenv('EXPONENTIAL_LAMBDA', '0.1'))
    
    generator = TrafficGenerator(mongodb_uri, cache_uri, poisson_lambda, exponential_lambda)
    
    logger.info("Esperando que el sistema de caché esté disponible...")
    for attempt in range(30):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{cache_uri}/health", timeout=aiohttp.ClientTimeout(total=5)) as response:
                    if response.status == 200:
                        logger.info("Sistema de caché disponible")
                        break
        except:
            pass
        
        logger.info(f"Intento {attempt + 1}/30 - Sistema de caché no disponible, esperando...")
        await asyncio.sleep(10)
    else:
        logger.error("Sistema de caché no disponible después de 5 minutos")
        return
    
    mode = os.getenv('TRAFFIC_MODE', 'comparative')
    
    if mode == 'comparative':
        duration = int(os.getenv('TEST_DURATION_MINUTES', '10'))
        results = await generator.run_comparative_test(duration)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"/app/logs/traffic_test_results_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Resultados guardados en {filename}")
        
        for dist_type, result in results.items():
            if isinstance(result, dict) and 'summary' in result:
                summary = result['summary']
                cache_perf = result['cache_performance']
                response_times = result['response_times']['overall']
                
                logger.info(f"\n=== Resumen {dist_type.upper()} ===")
                logger.info(f"Consultas exitosas: {summary['successful_requests']}/{summary['total_requests']}")
                logger.info(f"Hit rate: {cache_perf['hit_rate']:.2%}")
                logger.info(f"Tiempo promedio: {response_times['mean']:.3f}s")
                logger.info(f"P95: {response_times['p95']:.3f}s")
    
    elif mode == 'continuous':
        hours = int(os.getenv('CONTINUOUS_HOURS', '1'))
        results = await generator.run_continuous_traffic(hours)
        
        logger.info("Tráfico continuo completado")
        logger.info(f"Estadísticas finales: {generator.get_generator_stats()}")
    
    elif mode == 'poisson':
        duration = int(os.getenv('TEST_DURATION_MINUTES', '10'))
        results = await generator.run_poisson_traffic(duration)
        logger.info(f"Prueba Poisson completada: {results}")
    
    elif mode == 'exponential':
        duration = int(os.getenv('TEST_DURATION_MINUTES', '10'))
        results = await generator.run_exponential_traffic(duration)
        logger.info(f"Prueba Exponencial completada: {results}")
    
    elif mode == 'burst':
        num_bursts = int(os.getenv('NUM_BURSTS', '5'))
        burst_size = int(os.getenv('BURST_SIZE', '20'))
        burst_interval = int(os.getenv('BURST_INTERVAL', '30'))
        results = await generator.run_burst_traffic(num_bursts, burst_size, burst_interval)
        logger.info(f"Prueba de ráfagas completada: {results}")
    
    else:
        logger.error(f"Modo no reconocido: {mode}")

if __name__ == "__main__":
    asyncio.run(main())