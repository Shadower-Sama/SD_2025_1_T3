#!/usr/bin/env python3
"""
Data Processor - Módulo de Filtrado y Homogeneización
Procesa eventos raw de Waze para limpiar, estandarizar y preparar datos
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set

import pandas as pd
from pymongo import MongoClient
import numpy as np

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataProcessor:
    """Procesador de datos para filtrado y homogeneización"""
    
    def __init__(self, mongodb_uri: str, processing_interval: int = 600, batch_size: int = 1000):
        self.mongodb_uri = mongodb_uri
        self.processing_interval = processing_interval
        self.batch_size = batch_size
        
        # Conectar a MongoDB
        self.mongodb_client = MongoClient(mongodb_uri)
        self.db = self.mongodb_client.traffic_db
        self.raw_collection = self.db.waze_events
        self.processed_collection = self.db.processed_events
        self.duplicate_collection = self.db.duplicate_events
        self.invalid_collection = self.db.invalid_events
        
        # Crear índices optimizados
        self.create_indices()
        
        # Configuración de filtros
        self.rm_bounds = {
            'north': -33.0,
            'south': -34.0,
            'east': -70.0,
            'west': -71.5
        }
        
        # Mapeo de municipios
        self.municipality_mapping = self.load_municipality_mapping()
        
        # Estadísticas de procesamiento
        self.stats = {
            'processed_count': 0,
            'duplicate_count': 0,
            'invalid_count': 0,
            'standardized_count': 0,
            'last_processing_time': None
        }
        
        logger.info("Data Processor inicializado")
    
    def create_indices(self):
        """Crea índices optimizados para las colecciones"""
        try:
            # Índices para eventos raw
            self.raw_collection.create_index([("event_id", 1)], unique=False)
            self.raw_collection.create_index([("timestamp", -1)])
            self.raw_collection.create_index([("processed", 1)])
            
            # Índices para eventos procesados
            self.processed_collection.create_index([("event_id", 1)], unique=True)
            self.processed_collection.create_index([("timestamp", -1)])
            self.processed_collection.create_index([("municipality", 1)])
            self.processed_collection.create_index([("event_type", 1)])
            self.processed_collection.create_index([("location", "2d")])
            
            logger.info("Índices de base de datos creados")
            
        except Exception as e:
            logger.warning(f"Error creando índices: {e}")
    
    def load_municipality_mapping(self) -> Dict:
        """Carga mapeo de coordenadas a municipios de la RM"""
        return {
            'santiago': {
                'aliases': ['santiago', 'stgo', 'centro'],
                'bounds': {'lat': (-33.47, -33.42), 'lng': (-70.68, -70.63)},
                'canonical': 'Santiago'
            },
            'las_condes': {
                'aliases': ['las condes', 'lascondes', 'condes'],
                'bounds': {'lat': (-33.42, -33.38), 'lng': (-70.68, -70.50)},
                'canonical': 'Las Condes'
            },
            'providencia': {
                'aliases': ['providencia', 'provi'],
                'bounds': {'lat': (-33.45, -33.41), 'lng': (-70.65, -70.60)},
                'canonical': 'Providencia'
            },
            'nunoa': {
                'aliases': ['ñuñoa', 'nunoa', 'noa'],
                'bounds': {'lat': (-33.47, -33.43), 'lng': (-70.62, -70.58)},
                'canonical': 'Ñuñoa'
            },
            'la_reina': {
                'aliases': ['la reina', 'lareina'],
                'bounds': {'lat': (-33.45, -33.41), 'lng': (-70.58, -70.52)},
                'canonical': 'La Reina'
            },
            'vitacura': {
                'aliases': ['vitacura', 'vita'],
                'bounds': {'lat': (-33.40, -33.36), 'lng': (-70.60, -70.55)},
                'canonical': 'Vitacura'
            },
            'lo_barnechea': {
                'aliases': ['lo barnechea', 'lobarnechea', 'barnechea'],
                'bounds': {'lat': (-33.37, -33.30), 'lng': (-70.58, -70.45)},
                'canonical': 'Lo Barnechea'
            },
            'maipu': {
                'aliases': ['maipú', 'maipu'],
                'bounds': {'lat': (-33.52, -33.48), 'lng': (-70.82, -70.70)},
                'canonical': 'Maipú'
            },
            'puente_alto': {
                'aliases': ['puente alto', 'puentealto', 'puente'],
                'bounds': {'lat': (-33.65, -33.55), 'lng': (-70.65, -70.55)},
                'canonical': 'Puente Alto'
            },
            'san_miguel': {
                'aliases': ['san miguel', 'sanmiguel'],
                'bounds': {'lat': (-33.50, -33.46), 'lng': (-70.68, -70.63)},
                'canonical': 'San Miguel'
            },
            'la_florida': {
                'aliases': ['la florida', 'laflorida', 'florida'],
                'bounds': {'lat': (-33.55, -33.50), 'lng': (-70.62, -70.55)},
                'canonical': 'La Florida'
            },
            'penalolen': {
                'aliases': ['peñalolén', 'penalolen'],
                'bounds': {'lat': (-33.50, -33.45), 'lng': (-70.55, -70.48)},
                'canonical': 'Peñalolén'
            },
            'macul': {
                'aliases': ['macul'],
                'bounds': {'lat': (-33.50, -33.46), 'lng': (-70.62, -70.58)},
                'canonical': 'Macul'
            },
            'san_joaquin': {
                'aliases': ['san joaquín', 'san joaquin', 'sanjoaquin'],
                'bounds': {'lat': (-33.52, -33.48), 'lng': (-70.65, -70.61)},
                'canonical': 'San Joaquín'
            }
        }
    
    def is_valid_coordinate(self, lat: float, lng: float) -> bool:
        """Verifica si las coordenadas están dentro de la RM"""
        if not lat or not lng:
            return False
        
        return (self.rm_bounds['south'] <= lat <= self.rm_bounds['north'] and
                self.rm_bounds['west'] <= lng <= self.rm_bounds['east'])
    
    def standardize_municipality(self, municipality: str, lat: float = None, lng: float = None) -> str:
        """Estandariza el nombre del municipio"""
        if not municipality:
            if lat and lng and self.is_valid_coordinate(lat, lng):
                return self.get_municipality_from_coordinates(lat, lng)
            return 'Desconocido'
        
        # Limpiar y normalizar
        municipality_clean = municipality.lower().strip()
        
        # Buscar en mapeo
        for key, data in self.municipality_mapping.items():
            if municipality_clean in data['aliases']:
                return data['canonical']
        
        # Si no se encuentra, intentar con coordenadas
        if lat and lng and self.is_valid_coordinate(lat, lng):
            coord_municipality = self.get_municipality_from_coordinates(lat, lng)
            if coord_municipality != 'Región Metropolitana':
                return coord_municipality
        
        # Capitalizar primera letra de cada palabra
        return municipality.title() if municipality else 'Desconocido'
    
    def get_municipality_from_coordinates(self, lat: float, lng: float) -> str:
        """Determina el municipio basado en coordenadas"""
        if not self.is_valid_coordinate(lat, lng):
            return 'Fuera de RM'
        
        for key, data in self.municipality_mapping.items():
            bounds = data['bounds']
            if (bounds['lat'][0] <= lat <= bounds['lat'][1] and
                bounds['lng'][0] <= lng <= bounds['lng'][1]):
                return data['canonical']
        
        return 'Región Metropolitana'
    
    def standardize_event_type(self, event_type: str, subtype: str = None) -> str:
        """Estandariza el tipo de evento"""
        if not event_type:
            return 'unknown'
        
        event_type_clean = event_type.lower().strip()
        
        # Mapeo de tipos de eventos
        type_mapping = {
            'jam': 'jam',
            'traffic_jam': 'jam',
            'atasco': 'jam',
            'embotellamiento': 'jam',
            'alert': 'alert',
            'alerta': 'alert',
            'accident': 'accident',
            'accidente': 'accident',
            'crash': 'accident',
            'choque': 'accident',
            'road_closure': 'road_closure',
            'cierre': 'road_closure',
            'corte': 'road_closure',
            'construction': 'construction',
            'construccion': 'construction',
            'obras': 'construction',
            'hazard': 'hazard',
            'peligro': 'hazard',
            'weather': 'weather',
            'clima': 'weather',
            'police': 'police',
            'policia': 'police',
            'carabineros': 'police'
        }
        
        # Buscar mapeo directo
        if event_type_clean in type_mapping:
            return type_mapping[event_type_clean]
        
        # Buscar en subtype si no se encuentra en type
        if subtype:
            subtype_clean = subtype.lower().strip()
            if subtype_clean in type_mapping:
                return type_mapping[subtype_clean]
        
        return event_type_clean
    
    def clean_description(self, description: str) -> str:
        """Limpia y estandariza la descripción"""
        if not description:
            return ''
        
        # Eliminar caracteres especiales y normalizar
        cleaned = description.strip()
        
        # Limitar longitud
        if len(cleaned) > 500:
            cleaned = cleaned[:497] + '...'
        
        return cleaned
    
    def validate_event(self, event: Dict) -> tuple[bool, str]:
        """Valida si un evento es procesable"""
        
        # Campos requeridos
        if not event.get('event_id'):
            return False, "event_id faltante"
        
        if not event.get('timestamp'):
            return False, "timestamp faltante"
        
        # Validar timestamp
        try:
            if isinstance(event['timestamp'], str):
                datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
            elif not isinstance(event['timestamp'], datetime):
                return False, "timestamp inválido"
        except:
            return False, "formato de timestamp inválido"
        
        # Validar coordenadas si existen
        location = event.get('location', {})
        if location:
            lat = location.get('lat')
            lng = location.get('lng')
            
            if lat is not None and lng is not None:
                try:
                    lat_float = float(lat)
                    lng_float = float(lng)
                    
                    # Coordenadas deben estar en un rango razonable
                    if not (-90 <= lat_float <= 90 and -180 <= lng_float <= 180):
                        return False, "coordenadas fuera de rango"
                        
                except (ValueError, TypeError):
                    return False, "coordenadas inválidas"
        
        # Validar campos numéricos
        numeric_fields = ['confidence', 'severity', 'level', 'num_thumbs_up']
        for field in numeric_fields:
            value = event.get(field)
            if value is not None:
                try:
                    float_val = float(value)
                    if float_val < 0:
                        return False, f"{field} negativo"
                except (ValueError, TypeError):
                    return False, f"{field} no numérico"
        
        return True, "válido"
    
    def process_single_event(self, event: Dict) -> Optional[Dict]:
        """Procesa un evento individual"""
        
        # Validar evento
        is_valid, reason = self.validate_event(event)
        if not is_valid:
            logger.debug(f"Evento inválido {event.get('event_id', 'unknown')}: {reason}")
            return None
        
        try:
            # Extraer y limpiar ubicación
            location = event.get('location', {})
            lat = None
            lng = None
            
            if location:
                try:
                    lat = float(location.get('lat', 0)) if location.get('lat') is not None else None
                    lng = float(location.get('lng', 0)) if location.get('lng') is not None else None
                except (ValueError, TypeError):
                    lat = lng = None
            
            # Crear evento procesado
            processed_event = {
                'event_id': str(event['event_id']),
                'original_event_type': event.get('event_type', ''),
                'event_type': self.standardize_event_type(
                    event.get('event_type'), 
                    event.get('subtype')
                ),
                'subtype': event.get('subtype', ''),
                'description': self.clean_description(event.get('description', '')),
                'street': event.get('street', '')[:200] if event.get('street') else '',
                'city': event.get('city', '')[:100] if event.get('city') else '',
                'original_municipality': event.get('municipality', ''),
                'municipality': self.standardize_municipality(
                    event.get('municipality'), lat, lng
                ),
                'timestamp': event['timestamp'],
                'processed_timestamp': datetime.now(),
                'confidence': max(0, min(10, int(event.get('confidence', 0)))),
                'severity': max(0, min(10, int(event.get('severity', 0)))),
                'source': event.get('source', 'unknown'),
                'num_thumbs_up': max(0, int(event.get('num_thumbs_up', 0))),
                'level': max(0, int(event.get('level', 0))),
                'length': max(0.0, float(event.get('length', 0))),
                'delay': max(0.0, float(event.get('delay', 0))),
                'speed': max(0.0, float(event.get('speed', 0))),
                'data_quality_score': 0.0
            }
            
            # Procesar ubicación
            if lat is not None and lng is not None:
                if self.is_valid_coordinate(lat, lng):
                    processed_event['location'] = {'lat': lat, 'lng': lng}
                    processed_event['coordinates_valid'] = True
                else:
                    # Coordenadas fuera de RM, usar coordenadas por defecto
                    processed_event['location'] = {'lat': -33.4489, 'lng': -70.6693}
                    processed_event['coordinates_valid'] = False
            else:
                # Sin coordenadas, usar centro de Santiago
                processed_event['location'] = {'lat': -33.4489, 'lng': -70.6693}
                processed_event['coordinates_valid'] = False
            
            # Calcular score de calidad
            processed_event['data_quality_score'] = self.calculate_quality_score(processed_event)
            
            # Agregar metadatos de procesamiento
            processed_event['processing_metadata'] = {
                'processed_at': datetime.now(),
                'processor_version': '1.0',
                'municipality_standardized': processed_event['municipality'] != processed_event['original_municipality'],
                'coordinates_corrected': not processed_event['coordinates_valid']
            }
            
            return processed_event
            
        except Exception as e:
            logger.error(f"Error procesando evento {event.get('event_id', 'unknown')}: {e}")
            return None
    
    def calculate_quality_score(self, event: Dict) -> float:
        """Calcula un score de calidad para el evento (0-1)"""
        score = 0.0
        max_score = 0.0
        
        # Completitud de datos (40% del score)
        completeness_score = 0.0
        completeness_fields = [
            ('description', 0.1),
            ('street', 0.1),
            ('municipality', 0.1),
            ('coordinates_valid', 0.1)
        ]
        
        for field, weight in completeness_fields:
            max_score += weight
            if field == 'coordinates_valid':
                if event.get(field, False):
                    completeness_score += weight
            elif event.get(field):
                completeness_score += weight
        
        score += completeness_score
        
        # Confiabilidad (30% del score)
        confidence = event.get('confidence', 0) / 10.0
        confidence_weight = 0.3
        score += confidence * confidence_weight
        max_score += confidence_weight
        
        # Validación de source (20% del score)
        source = event.get('source', '')
        source_weight = 0.2
        if source in ['waze_api', 'waze_web']:
            score += source_weight
        elif source == 'synthetic':
            score += source_weight * 0.5
        max_score += source_weight
        
        # Engagement (10% del score)
        thumbs_up = min(event.get('num_thumbs_up', 0), 10) / 10.0
        engagement_weight = 0.1
        score += thumbs_up * engagement_weight
        max_score += engagement_weight
        
        return score / max_score if max_score > 0 else 0.0
    
    def detect_duplicates(self, events: List[Dict]) -> tuple[List[Dict], List[Dict]]:
        """Detecta y separa eventos duplicados"""
        
        unique_events = []
        duplicates = []
        seen_ids = set()
        
        # Agrupar por criterios de similaridad
        similarity_groups = {}
        
        for event in events:
            event_id = event['event_id']
            
            # Duplicado exacto por ID
            if event_id in seen_ids:
                duplicates.append(event)
                continue
            
            # Crear clave de similaridad
            location = event.get('location', {})
            similarity_key = (
                event.get('municipality', ''),
                event.get('event_type', ''),
                round(location.get('lat', 0), 3),  # Redondear a ~100m
                round(location.get('lng', 0), 3),
                event['timestamp'].date() if isinstance(event['timestamp'], datetime) else str(event['timestamp'])[:10]
            )
            
            if similarity_key in similarity_groups:
                # Evento similar encontrado, comparar calidad
                existing_event = similarity_groups[similarity_key]
                existing_quality = existing_event.get('data_quality_score', 0)
                current_quality = event.get('data_quality_score', 0)
                
                if current_quality > existing_quality:
                    # Evento actual es mejor, reemplazar
                    duplicates.append(existing_event)
                    similarity_groups[similarity_key] = event
                else:
                    # Evento existente es mejor, marcar actual como duplicado
                    duplicates.append(event)
            else:
                similarity_groups[similarity_key] = event
            
            seen_ids.add(event_id)
        
        unique_events = list(similarity_groups.values())
        
        logger.info(f"Detectados {len(duplicates)} duplicados de {len(events)} eventos")
        
        return unique_events, duplicates
    
    def process_batch(self, limit: int = None) -> Dict:
        """Procesa un lote de eventos sin procesar"""
        
        limit = limit or self.batch_size
        
        # Obtener eventos sin procesar
        query = {'processed': {'$ne': True}}
        cursor = self.raw_collection.find(query).limit(limit).sort('timestamp', 1)
        
        raw_events = list(cursor)
        
        if not raw_events:
            logger.info("No hay eventos para procesar")
            return {
                'processed': 0,
                'duplicates': 0,
                'invalid': 0,
                'batch_size': 0
            }
        
        logger.info(f"Procesando lote de {len(raw_events)} eventos")
        
        # Procesar eventos
        processed_events = []
        invalid_events = []
        
        for raw_event in raw_events:
            processed = self.process_single_event(raw_event)
            if processed:
                processed_events.append(processed)
            else:
                invalid_events.append(raw_event)
        
        # Detectar duplicados
        unique_events, duplicate_events = self.detect_duplicates(processed_events)
        
        # Guardar resultados
        results = {
            'processed': 0,
            'duplicates': 0,
            'invalid': 0,
            'batch_size': len(raw_events)
        }
        
        # Insertar eventos únicos
        if unique_events:
            try:
                self.processed_collection.insert_many(unique_events, ordered=False)
                results['processed'] = len(unique_events)
                logger.info(f"Insertados {len(unique_events)} eventos procesados")
            except Exception as e:
                logger.error(f"Error insertando eventos procesados: {e}")
        
        # Guardar duplicados
        if duplicate_events:
            try:
                for dup in duplicate_events:
                    dup['duplicate_detected_at'] = datetime.now()
                self.duplicate_collection.insert_many(duplicate_events, ordered=False)
                results['duplicates'] = len(duplicate_events)
                logger.info(f"Guardados {len(duplicate_events)} duplicados")
            except Exception as e:
                logger.error(f"Error guardando duplicados: {e}")
        
        # Guardar eventos inválidos
        if invalid_events:
            try:
                for inv in invalid_events:
                    inv['invalid_detected_at'] = datetime.now()
                self.invalid_collection.insert_many(invalid_events, ordered=False)
                results['invalid'] = len(invalid_events)
                logger.info(f"Guardados {len(invalid_events)} eventos inválidos")
            except Exception as e:
                logger.error(f"Error guardando eventos inválidos: {e}")
        
        # Marcar eventos como procesados
        event_ids = [event['event_id'] for event in raw_events]
        self.raw_collection.update_many(
            {'event_id': {'$in': event_ids}},
            {'$set': {'processed': True, 'processed_at': datetime.now()}}
        )
        
        # Actualizar estadísticas
        self.stats['processed_count'] += results['processed']
        self.stats['duplicate_count'] += results['duplicates']
        self.stats['invalid_count'] += results['invalid']
        self.stats['last_processing_time'] = datetime.now()
        
        logger.info(f"Lote completado: {results}")
        
        return results
    
    def get_processing_stats(self) -> Dict:
        """Obtiene estadísticas de procesamiento"""
        
        try:
            # Estadísticas de colecciones
            collection_stats = {
                'raw_events': self.raw_collection.count_documents({}),
                'processed_events': self.processed_collection.count_documents({}),
                'duplicate_events': self.duplicate_collection.count_documents({}),
                'invalid_events': self.invalid_collection.count_documents({}),
                'unprocessed_events': self.raw_collection.count_documents({'processed': {'$ne': True}})
            }
            
            # Estadísticas de calidad
            pipeline = [
                {'$group': {
                    '_id': None,
                    'avg_quality_score': {'$avg': '$data_quality_score'},
                    'min_quality_score': {'$min': '$data_quality_score'},
                    'max_quality_score': {'$max': '$data_quality_score'}
                }}
            ]
            
            quality_stats = list(self.processed_collection.aggregate(pipeline))
            quality_data = quality_stats[0] if quality_stats else {}
            
            # Estadísticas por municipio
            municipality_pipeline = [
                {'$group': {
                    '_id': '$municipality',
                    'count': {'$sum': 1},
                    'avg_quality': {'$avg': '$data_quality_score'}
                }},
                {'$sort': {'count': -1}},
                {'$limit': 10}
            ]
            
            municipality_stats = list(self.processed_collection.aggregate(municipality_pipeline))
            
            return {
                'timestamp': datetime.now().isoformat(),
                'processing_stats': self.stats,
                'collection_stats': collection_stats,
                'quality_stats': {
                    'avg_quality_score': quality_data.get('avg_quality_score', 0),
                    'min_quality_score': quality_data.get('min_quality_score', 0),
                    'max_quality_score': quality_data.get('max_quality_score', 0)
                },
                'top_municipalities': municipality_stats,
                'processing_rate': {
                    'total_processed': self.stats['processed_count'],
                    'success_rate': (self.stats['processed_count'] / 
                                   (self.stats['processed_count'] + self.stats['invalid_count'])
                                   if (self.stats['processed_count'] + self.stats['invalid_count']) > 0 else 0),
                    'duplicate_rate': (self.stats['duplicate_count'] / 
                                     (self.stats['processed_count'] + self.stats['duplicate_count'])
                                     if (self.stats['processed_count'] + self.stats['duplicate_count']) > 0 else 0)
                }
            }
            
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def cleanup_old_data(self, days_to_keep: int = 30):
        """Limpia datos antiguos para mantener el rendimiento"""
        
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        try:
            # Limpiar eventos duplicados antiguos
            dup_result = self.duplicate_collection.delete_many(
                {'duplicate_detected_at': {'$lt': cutoff_date}}
            )
            
            # Limpiar eventos inválidos antiguos
            inv_result = self.invalid_collection.delete_many(
                {'invalid_detected_at': {'$lt': cutoff_date}}
            )
            
            logger.info(f"Limpieza completada: {dup_result.deleted_count} duplicados, "
                       f"{inv_result.deleted_count} inválidos eliminados")
            
        except Exception as e:
            logger.error(f"Error en limpieza: {e}")
    
    async def run_continuous_processing(self):
        """Ejecuta procesamiento continuo"""
        
        logger.info(f"Iniciando procesamiento continuo cada {self.processing_interval} segundos")
        
        while True:
            try:
                start_time = time.time()
                
                # Procesar lote
                results = self.process_batch()
                
                # Log de resultados
                processing_time = time.time() - start_time
                logger.info(f"Procesamiento completado en {processing_time:.2f}s: {results}")
                
                # Limpieza periódica (cada hora)
                if self.stats['processed_count'] % 100 == 0:
                    self.cleanup_old_data()
                
                # Esperar hasta el próximo ciclo
                await asyncio.sleep(self.processing_interval)
                
            except Exception as e:
                logger.error(f"Error en procesamiento continuo: {e}")
                await asyncio.sleep(60)  # Esperar 1 minuto antes de reintentar
    
    def run_single_batch(self, limit: int = None):
        """Ejecuta un solo lote de procesamiento"""
        
        logger.info("Ejecutando procesamiento de lote único")
        
        start_time = time.time()
        results = self.process_batch(limit)
        processing_time = time.time() - start_time
        
        logger.info(f"Lote completado en {processing_time:.2f}s")
        logger.info(f"Resultados: {results}")
        
        return results
    
    def reprocess_failed_events(self):
        """Reprocesa eventos que fallaron anteriormente"""
        
        logger.info("Reprocesando eventos fallidos")
        
        # Buscar eventos marcados como inválidos que podrían ser reprocesables
        recent_cutoff = datetime.now() - timedelta(hours=24)
        
        failed_events = list(self.invalid_collection.find({
            'invalid_detected_at': {'$gte': recent_cutoff}
        }).limit(100))
        
        if not failed_events:
            logger.info("No hay eventos fallidos recientes para reprocesar")
            return
        
        logger.info(f"Reprocesando {len(failed_events)} eventos fallidos")
        
        reprocessed = 0
        for event in failed_events:
            processed = self.process_single_event(event)
            if processed:
                try:
                    self.processed_collection.insert_one(processed)
                    self.invalid_collection.delete_one({'_id': event['_id']})
                    reprocessed += 1
                except Exception as e:
                    logger.warning(f"Error reprocesando evento {event.get('event_id')}: {e}")
        
        logger.info(f"Reprocesados exitosamente: {reprocessed}/{len(failed_events)} eventos")

def main():
    """Función principal del procesador de datos"""
    
    # Configuración desde variables de entorno
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@localhost:27017/traffic_db?authSource=admin')
    processing_interval = int(os.getenv('PROCESSING_INTERVAL', '600'))  # 10 minutos por defecto
    batch_size = int(os.getenv('BATCH_SIZE', '1000'))
    mode = os.getenv('PROCESSING_MODE', 'continuous')
    
    # Crear procesador
    processor = DataProcessor(mongodb_uri, processing_interval, batch_size)
    
    # Ejecutar según modo
    if mode == 'continuous':
        # Procesamiento continuo
        logger.info("Modo continuo activado")
        asyncio.run(processor.run_continuous_processing())
        
    elif mode == 'single_batch':
        # Un solo lote
        limit = int(os.getenv('BATCH_LIMIT', '0')) or None
        results = processor.run_single_batch(limit)
        print(json.dumps(results, indent=2))
        
    elif mode == 'reprocess':
        # Reprocesar eventos fallidos
        processor.reprocess_failed_events()
        
    elif mode == 'stats':
        # Mostrar estadísticas
        stats = processor.get_processing_stats()
        print(json.dumps(stats, indent=2, default=str))
        
    elif mode == 'cleanup':
        # Limpieza de datos
        days = int(os.getenv('CLEANUP_DAYS', '30'))
        processor.cleanup_old_data(days)
        
    else:
        logger.error(f"Modo no reconocido: {mode}")
        logger.info("Modos disponibles: continuous, single_batch, reprocess, stats, cleanup")

if __name__ == "__main__":
    main()