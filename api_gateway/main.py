#!/usr/bin/env python3
"""
API Gateway para el Sistema de Análisis de Tráfico
Proporciona una interfaz unificada para todos los servicios
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import requests
from elasticsearch import Elasticsearch
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from pymongo import MongoClient
import redis
import uvicorn

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Modelos Pydantic
class TrafficQuery(BaseModel):
    municipality: Optional[str] = None
    event_type: Optional[str] = None
    date_range: Optional[Dict] = None
    location: Optional[Dict] = None
    limit: Optional[int] = 100

class CacheConfig(BaseModel):
    policy: str
    size: int

class AnalysisRequest(BaseModel):
    analysis_type: str
    parameters: Optional[Dict] = {}

# Configuración de la aplicación
app = FastAPI(
    title="Sistema de Análisis de Tráfico - API Gateway",
    description="API unificada para el análisis de tráfico en la Región Metropolitana",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class APIGateway:
    """Gateway principal del sistema"""
    
    def __init__(self):
        # URIs de servicios
        self.mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@mongodb:27017/traffic_db?authSource=admin')
        self.redis_uri = os.getenv('REDIS_URI', 'redis://redis:6379')
        self.elasticsearch_uri = os.getenv('ELASTICSEARCH_URI', 'http://elasticsearch:9200')
        self.cache_service_uri = os.getenv('CACHE_SERVICE_URI', 'http://cache_system:8080')
        self.visualization_uri = os.getenv('VISUALIZATION_URI', 'http://visualization:3000')
        
        # Conectar a servicios
        self.setup_connections()
        
        # Métricas del gateway
        self.requests_count = 0
        self.error_count = 0
        self.response_times = []
        
        logger.info("API Gateway inicializado")
    
    def setup_connections(self):
        """Configura las conexiones a los servicios"""
        try:
            # MongoDB
            self.mongo_client = MongoClient(self.mongodb_uri)
            self.db = self.mongo_client.traffic_db
            
            # Redis
            self.redis_client = redis.from_url(self.redis_uri, decode_responses=True)
            
            # Elasticsearch
            self.es_client = Elasticsearch([self.elasticsearch_uri])
            
            logger.info("Conexiones establecidas exitosamente")
            
        except Exception as e:
            logger.error(f"Error estableciendo conexiones: {e}")
            raise
    
    def check_service_health(self, service_url: str, timeout: int = 5) -> bool:
        """Verifica la salud de un servicio"""
        try:
            response = requests.get(f"{service_url}/health", timeout=timeout)
            return response.status_code == 200
        except:
            return False
    
    def proxy_request(self, service_url: str, endpoint: str, method: str = 'GET', 
                     data: Dict = None, params: Dict = None) -> Dict:
        """Hace proxy de requests a otros servicios"""
        
        start_time = time.time()
        self.requests_count += 1
        
        try:
            url = f"{service_url}{endpoint}"
            
            if method.upper() == 'GET':
                response = requests.get(url, params=params, timeout=30)
            elif method.upper() == 'POST':
                response = requests.post(url, json=data, params=params, timeout=30)
            else:
                raise ValueError(f"Método HTTP no soportado: {method}")
            
            response_time = time.time() - start_time
            self.response_times.append(response_time)
            
            if response.status_code == 200:
                return response.json()
            else:
                self.error_count += 1
                raise HTTPException(
                    status_code=response.status_code,
                    detail=f"Error en servicio: {response.text}"
                )
                
        except requests.exceptions.RequestException as e:
            self.error_count += 1
            response_time = time.time() - start_time
            self.response_times.append(response_time)
            
            logger.error(f"Error en proxy request a {service_url}{endpoint}: {e}")
            raise HTTPException(
                status_code=503,
                detail=f"Servicio no disponible: {str(e)}"
            )
    
    def get_system_overview(self) -> Dict:
        """Obtiene un resumen general del sistema"""
        try:
            overview = {
                'timestamp': datetime.now().isoformat(),
                'services': {},
                'data_summary': {},
                'performance': {}
            }
            
            # Estado de servicios
            services = {
                'cache_system': self.cache_service_uri,
                'elasticsearch': self.elasticsearch_uri,
                'mongodb': 'mongodb://mongodb:27017',
                'redis': self.redis_uri
            }
            
            for service_name, service_url in services.items():
                if service_name == 'cache_system':
                    overview['services'][service_name] = self.check_service_health(service_url)
                elif service_name == 'elasticsearch':
                    try:
                        health = self.es_client.cluster.health()
                        overview['services'][service_name] = health['status'] in ['yellow', 'green']
                    except:
                        overview['services'][service_name] = False
                elif service_name == 'mongodb':
                    try:
                        self.mongo_client.admin.command('ping')
                        overview['services'][service_name] = True
                    except:
                        overview['services'][service_name] = False
                elif service_name == 'redis':
                    try:
                        self.redis_client.ping()
                        overview['services'][service_name] = True
                    except:
                        overview['services'][service_name] = False
            
            # Resumen de datos
            try:
                overview['data_summary'] = {
                    'total_events': self.db.waze_events.count_documents({}),
                    'processed_events': self.db.processed_events.count_documents({}),
                    'analysis_results': self.db.analysis_results.count_documents({}),
                    'recent_events_24h': self.db.waze_events.count_documents({
                        'timestamp': {'$gte': datetime.now() - timedelta(hours=24)}
                    })
                }
            except Exception as e:
                overview['data_summary'] = {'error': str(e)}
            
            # Métricas de rendimiento del gateway
            avg_response_time = sum(self.response_times) / len(self.response_times) if self.response_times else 0
            error_rate = self.error_count / self.requests_count if self.requests_count > 0 else 0
            
            overview['performance'] = {
                'total_requests': self.requests_count,
                'error_count': self.error_count,
                'error_rate': error_rate,
                'avg_response_time': avg_response_time
            }
            
            return overview
            
        except Exception as e:
            logger.error(f"Error obteniendo overview del sistema: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}

# Instancia global del gateway
gateway = APIGateway()

# Endpoints principales

@app.get("/")
async def root():
    """Endpoint raíz con información básica"""
    return {
        "service": "Sistema de Análisis de Tráfico - API Gateway",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "health": "/health",
            "overview": "/overview",
            "query": "/query",
            "cache": "/cache/*",
            "analysis": "/analysis/*",
            "visualization": "/visualization/*"
        }
    }

@app.get("/health")
async def health_check():
    """Health check del API Gateway"""
    try:
        # Verificar conexiones básicas
        gateway.mongo_client.admin.command('ping')
        gateway.redis_client.ping()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "gateway_metrics": {
                "requests_processed": gateway.requests_count,
                "error_rate": gateway.error_count / gateway.requests_count if gateway.requests_count > 0 else 0
            }
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Gateway unhealthy: {str(e)}")

@app.get("/overview")
async def system_overview():
    """Resumen general del estado del sistema"""
    return gateway.get_system_overview()

# Endpoints para consultas de datos

@app.post("/query")
async def query_traffic_data(query: TrafficQuery):
    """Consulta datos de tráfico a través del sistema de caché"""
    try:
        query_dict = query.dict(exclude_none=True)
        result = gateway.proxy_request(
            gateway.cache_service_uri,
            "/query",
            method="POST",
            data=query_dict
        )
        return result
    except Exception as e:
        logger.error(f"Error en query: {e}")
        raise

@app.get("/events/recent")
async def get_recent_events(
    hours: int = Query(24, description="Horas hacia atrás"),
    limit: int = Query(100, description="Número máximo de eventos")
):
    """Obtiene eventos recientes"""
    try:
        end_time = datetime.now()
        start_time = end_time - timedelta(hours=hours)
        
        query = {
            "date_range": {
                "start": start_time.isoformat(),
                "end": end_time.isoformat()
            },
            "limit": limit
        }
        
        result = gateway.proxy_request(
            gateway.cache_service_uri,
            "/query",
            method="POST",
            data=query
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events/by-municipality/{municipality}")
async def get_events_by_municipality(
    municipality: str,
    limit: int = Query(100, description="Número máximo de eventos")
):
    """Obtiene eventos por municipio"""
    try:
        query = {
            "municipality": municipality,
            "limit": limit
        }
        
        result = gateway.proxy_request(
            gateway.cache_service_uri,
            "/query",
            method="POST",
            data=query
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoints para caché

@app.get("/cache/stats")
async def get_cache_stats():
    """Obtiene estadísticas del sistema de caché"""
    return gateway.proxy_request(gateway.cache_service_uri, "/cache/stats")

@app.post("/cache/clear")
async def clear_cache():
    """Limpia el caché del sistema"""
    return gateway.proxy_request(gateway.cache_service_uri, "/cache/clear", method="POST")

@app.get("/cache/config")
async def get_cache_config():
    """Obtiene configuración actual del caché"""
    stats = gateway.proxy_request(gateway.cache_service_uri, "/cache/stats")
    return {
        "policy": stats.get("policy", "unknown"),
        "capacity": stats.get("capacity", 0),
        "current_size": stats.get("size", 0)
    }

# Endpoints para agregaciones

@app.get("/aggregation/{agg_type}")
async def get_aggregation(agg_type: str):
    """Obtiene datos agregados"""
    return gateway.proxy_request(
        gateway.cache_service_uri,
        f"/aggregation/{agg_type}"
    )

@app.post("/aggregation/{agg_type}")
async def post_aggregation(agg_type: str, params: Dict = None):
    """Obtiene datos agregados con parámetros"""
    return gateway.proxy_request(
        gateway.cache_service_uri,
        f"/aggregation/{agg_type}",
        method="POST",
        data=params or {}
    )

# Endpoints para análisis

@app.get("/analysis/municipalities")
async def get_municipality_analysis():
    """Análisis por municipios"""
    return gateway.proxy_request(
        gateway.cache_service_uri,
        "/aggregation/events_by_municipality"
    )

@app.get("/analysis/event-types")
async def get_event_type_analysis():
    """Análisis por tipos de evento"""
    return gateway.proxy_request(
        gateway.cache_service_uri,
        "/aggregation/events_by_type"
    )

@app.get("/analysis/hourly")
async def get_hourly_analysis():
    """Análisis de distribución horaria"""
    return gateway.proxy_request(
        gateway.cache_service_uri,
        "/aggregation/hourly_distribution"
    )

# Endpoints para métricas y monitoreo

@app.get("/metrics/system")
async def get_system_metrics():
    """Métricas generales del sistema"""
    try:
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "gateway": {
                "requests_processed": gateway.requests_count,
                "errors": gateway.error_count,
                "avg_response_time": sum(gateway.response_times) / len(gateway.response_times) if gateway.response_times else 0,
                "uptime": "N/A"  # Se podría implementar con tiempo de inicio
            }
        }
        
        # Agregar métricas de caché
        try:
            cache_stats = gateway.proxy_request(gateway.cache_service_uri, "/cache/stats")
            metrics["cache"] = cache_stats
        except:
            metrics["cache"] = {"status": "unavailable"}
        
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics/performance")
async def get_performance_metrics():
    """Métricas de rendimiento detalladas"""
    try:
        # Métricas del gateway
        gateway_metrics = {
            "total_requests": gateway.requests_count,
            "error_count": gateway.error_count,
            "error_rate": gateway.error_count / gateway.requests_count if gateway.requests_count > 0 else 0,
            "response_times": {
                "count": len(gateway.response_times),
                "avg": sum(gateway.response_times) / len(gateway.response_times) if gateway.response_times else 0,
                "min": min(gateway.response_times) if gateway.response_times else 0,
                "max": max(gateway.response_times) if gateway.response_times else 0
            }
        }
        
        # Métricas de MongoDB
        try:
            db_stats = gateway.db.command("dbStats")
            mongodb_metrics = {
                "collections": db_stats.get("collections", 0),
                "data_size": db_stats.get("dataSize", 0),
                "storage_size": db_stats.get("storageSize", 0),
                "indexes": db_stats.get("indexes", 0)
            }
        except:
            mongodb_metrics = {"status": "unavailable"}
        
        # Métricas de Elasticsearch
        try:
            es_stats = gateway.es_client.cluster.stats()
            elasticsearch_metrics = {
                "nodes": es_stats["nodes"]["count"]["total"],
                "indices": es_stats["indices"]["count"],
                "docs": es_stats["indices"]["docs"]["count"],
                "store_size": es_stats["indices"]["store"]["size_in_bytes"]
            }
        except:
            elasticsearch_metrics = {"status": "unavailable"}
        
        return {
            "timestamp": datetime.now().isoformat(),
            "gateway": gateway_metrics,
            "mongodb": mongodb_metrics,
            "elasticsearch": elasticsearch_metrics
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoints para búsquedas avanzadas

@app.post("/search/events")
async def search_events(
    query: str = Query(..., description="Término de búsqueda"),
    limit: int = Query(50, description="Número máximo de resultados")
):
    """Búsqueda de texto en eventos"""
    try:
        # Buscar en MongoDB usando regex
        regex_query = {"$regex": query, "$options": "i"}
        
        cursor = gateway.db.waze_events.find({
            "$or": [
                {"description": regex_query},
                {"street": regex_query},
                {"municipality": regex_query}
            ]
        }).limit(limit).sort("timestamp", -1)
        
        results = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            if "timestamp" in doc and hasattr(doc["timestamp"], "isoformat"):
                doc["timestamp"] = doc["timestamp"].isoformat()
            results.append(doc)
        
        return {
            "query": query,
            "results": results,
            "count": len(results),
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search/municipalities")
async def get_municipalities():
    """Lista de municipios disponibles"""
    try:
        municipalities = gateway.db.waze_events.distinct("municipality")
        return {
            "municipalities": sorted([m for m in municipalities if m]),
            "count": len(municipalities),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/search/event-types")
async def get_event_types():
    """Lista de tipos de eventos disponibles"""
    try:
        event_types = gateway.db.waze_events.distinct("event_type")
        return {
            "event_types": sorted([e for e in event_types if e]),
            "count": len(event_types),
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Middleware para logging
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    
    # Procesar request
    response = await call_next(request)
    
    # Log de la request
    process_time = time.time() - start_time
    logger.info(
        f"{request.method} {request.url.path} - "
        f"Status: {response.status_code} - "
        f"Time: {process_time:.3f}s - "
        f"Client: {request.client.host}"
    )
    
    return response

# Manejo de errores
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "timestamp": datetime.now().isoformat(),
            "path": request.url.path
        }
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Error no manejado en {request.url.path}: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Error interno del servidor",
            "message": str(exc),
            "timestamp": datetime.now().isoformat(),
            "path": request.url.path
        }
    )

def main():
    """Función principal para ejecutar el API Gateway"""
    
    # Configuración del servidor
    host = os.getenv('API_HOST', '0.0.0.0')
    port = int(os.getenv('API_PORT', '8000'))
    workers = int(os.getenv('WORKERS', '1'))
    
    logger.info(f"Iniciando API Gateway en {host}:{port}")
    
    # Verificar conexiones antes de iniciar
    try:
        gateway.setup_connections()
        logger.info("Conexiones verificadas exitosamente")
    except Exception as e:
        logger.error(f"Error en conexiones iniciales: {e}")
        logger.info("Continuando de todas formas...")
    
    # Configuración de uvicorn
    config = uvicorn.Config(
        app,
        host=host,
        port=port,
        workers=workers,
        log_level="info",
        access_log=True
    )
    
    server = uvicorn.Server(config)
    server.run()

if __name__ == "__main__":
    main()