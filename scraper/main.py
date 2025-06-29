

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import aiohttp
import pymongo
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WazeScraper:
    """Scraper para extraer eventos de tráfico desde Waze"""
    
    def __init__(self, mongodb_uri: str, scrape_interval: int = 300):
        self.mongodb_uri = mongodb_uri
        self.scrape_interval = scrape_interval
        self.client = None
        self.db = None
        self.collection = None
        
        # Coordenadas de la Región Metropolitana
        self.RM_BOUNDS = {
            'north': -33.0,
            'south': -34.0,
            'east': -70.0,
            'west': -71.5
        }
        
        # Configuración de Chrome para scraping
        self.chrome_options = Options()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--disable-gpu')
        self.chrome_options.add_argument('--window-size=1920,1080')
        
    def connect_database(self):
        """Conecta a la base de datos MongoDB"""
        try:
            self.client = MongoClient(self.mongodb_uri)
            self.db = self.client.traffic_db
            self.collection = self.db.waze_events
            
            # Crear índices para optimizar consultas
            self.collection.create_index([("timestamp", pymongo.DESCENDING)])
            self.collection.create_index([("location", pymongo.GEO2D)])
            self.collection.create_index([("event_type", 1)])
            self.collection.create_index([("municipality", 1)])
            
            logger.info("Conexión exitosa a MongoDB")
            
        except Exception as e:
            logger.error(f"Error conectando a MongoDB: {e}")
            raise
    
    async def get_waze_data_api(self) -> List[Dict]:
        """
        Obtiene datos de Waze usando la API no oficial
        """
        events = []
        
        # URL de la API de Waze (no oficial)
        waze_api_url = "https://www.waze.com/row-rtserver/web/TGeoRSS"
        
        params = {
            'tk': 'ccp_partner',
            'ccp_partner_name': 'traffic_analysis',
            'format': 'JSON',
            'types': 'alerts,jams,irregularities',
            'polygon': f"{self.RM_BOUNDS['west']},{self.RM_BOUNDS['south']};"
                      f"{self.RM_BOUNDS['east']},{self.RM_BOUNDS['south']};"
                      f"{self.RM_BOUNDS['east']},{self.RM_BOUNDS['north']};"
                      f"{self.RM_BOUNDS['west']},{self.RM_BOUNDS['north']}"
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(waze_api_url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        
                        # Procesar alertas
                        if 'alerts' in data:
                            for alert in data['alerts']:
                                event = self.process_alert(alert)
                                if event:
                                    events.append(event)
                        
                        # Procesar atascos
                        if 'jams' in data:
                            for jam in data['jams']:
                                event = self.process_jam(jam)
                                if event:
                                    events.append(event)
                                    
                        logger.info(f"Obtenidos {len(events)} eventos desde API")
                        
        except Exception as e:
            logger.error(f"Error obteniendo datos de API Waze: {e}")
            
        return events
    
    def scrape_waze_web(self) -> List[Dict]:
        """
        Scraping de la página web de Waze como método alternativo
        """
        events = []
        driver = None
        
        try:
            driver = webdriver.Chrome(options=self.chrome_options)
            
            # Navegar a Waze Live Map centrado en Santiago
            santiago_url = "https://www.waze.com/es-419/live-map/directions?"
            santiago_url += "to=ll.-33.4489%2C-70.6693&from=ll.-33.4489%2C-70.6693"
            
            driver.get(santiago_url)
            
            # Esperar a que cargue la página
            wait = WebDriverWait(driver, 20)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            
            # Buscar elementos de eventos en el mapa
            event_selectors = [
                '[data-testid*="alert"]',
                '[data-testid*="jam"]',
                '[data-testid*="incident"]',
                '.waze-icon-alerts',
                '.waze-icon-traffic'
            ]
            
            for selector in event_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    
                    for element in elements[:50]:  # Limitar a 50 eventos por selector
                        try:
                            event_data = self.extract_event_data(element)
                            if event_data:
                                events.append(event_data)
                        except Exception as e:
                            logger.debug(f"Error extrayendo evento: {e}")
                            continue
                            
                except Exception as e:
                    logger.debug(f"Error con selector {selector}: {e}")
                    continue
            
            logger.info(f"Extraídos {len(events)} eventos desde web scraping")
            
        except Exception as e:
            logger.error(f"Error en web scraping: {e}")
            
        finally:
            if driver:
                driver.quit()
                
        return events
    
    def process_alert(self, alert: Dict) -> Optional[Dict]:
        """Procesa una alerta de Waze"""
        try:
            return {
                'event_id': alert.get('uuid', f"alert_{int(time.time())}"),
                'event_type': 'alert',
                'subtype': alert.get('type', 'unknown'),
                'description': alert.get('reportDescription', ''),
                'location': {
                    'lat': alert['location']['y'] if 'location' in alert else None,
                    'lng': alert['location']['x'] if 'location' in alert else None
                },
                'street': alert.get('street', ''),
                'city': alert.get('city', ''),
                'municipality': self.determine_municipality(
                    alert['location']['y'] if 'location' in alert else None,
                    alert['location']['x'] if 'location' in alert else None
                ),
                'timestamp': datetime.now(timezone.utc),
                'confidence': alert.get('confidence', 0),
                'num_thumbs_up': alert.get('nThumbsUp', 0),
                'severity': alert.get('reportRating', 0),
                'source': 'waze_api'
            }
        except Exception as e:
            logger.error(f"Error procesando alerta: {e}")
            return None
    
    def process_jam(self, jam: Dict) -> Optional[Dict]:
        """Procesa un atasco de Waze"""
        try:
            return {
                'event_id': jam.get('uuid', f"jam_{int(time.time())}"),
                'event_type': 'jam',
                'subtype': 'traffic_jam',
                'description': f"Atasco nivel {jam.get('level', 0)}",
                'location': {
                    'lat': jam['line'][0]['y'] if 'line' in jam and jam['line'] else None,
                    'lng': jam['line'][0]['x'] if 'line' in jam and jam['line'] else None
                },
                'street': jam.get('street', ''),
                'city': jam.get('city', ''),
                'municipality': self.determine_municipality(
                    jam['line'][0]['y'] if 'line' in jam and jam['line'] else None,
                    jam['line'][0]['x'] if 'line' in jam and jam['line'] else None
                ),
                'timestamp': datetime.now(timezone.utc),
                'length': jam.get('length', 0),
                'delay': jam.get('delay', 0),
                'speed': jam.get('speed', 0),
                'level': jam.get('level', 0),
                'source': 'waze_api'
            }
        except Exception as e:
            logger.error(f"Error procesando jam: {e}")
            return None
    
    def extract_event_data(self, element) -> Optional[Dict]:
        """Extrae datos de un elemento del DOM"""
        try:
            # Obtener información básica del elemento
            event_type = 'unknown'
            description = ''
            
            # Intentar determinar el tipo de evento por clases CSS
            class_names = element.get_attribute('class') or ''
            if 'alert' in class_names.lower():
                event_type = 'alert'
            elif 'jam' in class_names.lower() or 'traffic' in class_names.lower():
                event_type = 'jam'
            elif 'accident' in class_names.lower():
                event_type = 'accident'
            
            # Intentar obtener texto descriptivo
            try:
                description = element.get_attribute('title') or element.text or ''
            except:
                description = ''
            
            return {
                'event_id': f"web_{int(time.time())}_{hash(description)}",
                'event_type': event_type,
                'subtype': 'web_scraped',
                'description': description,
                'location': {
                    'lat': -33.4489,  # Santiago centro por defecto
                    'lng': -70.6693
                },
                'street': 'Unknown',
                'city': 'Santiago',
                'municipality': 'Santiago',
                'timestamp': datetime.now(timezone.utc),
                'source': 'waze_web'
            }
            
        except Exception as e:
            logger.error(f"Error extrayendo datos del elemento: {e}")
            return None
    
    def determine_municipality(self, lat: float, lng: float) -> str:
        """Determina la comuna basada en coordenadas"""
        if not lat or not lng:
            return 'Unknown'
        
        # Mapeo básico de coordenadas a comunas principales de la RM
        municipalities = {
            'Santiago': {'lat_range': (-33.47, -33.42), 'lng_range': (-70.68, -70.63)},
            'Las Condes': {'lat_range': (-33.42, -33.38), 'lng_range': (-70.68, -70.50)},
            'Providencia': {'lat_range': (-33.45, -33.41), 'lng_range': (-70.65, -70.60)},
            'Ñuñoa': {'lat_range': (-33.47, -33.43), 'lng_range': (-70.62, -70.58)},
            'La Reina': {'lat_range': (-33.45, -33.41), 'lng_range': (-70.58, -70.52)},
            'Vitacura': {'lat_range': (-33.40, -33.36), 'lng_range': (-70.60, -70.55)},
            'Lo Barnechea': {'lat_range': (-33.37, -33.30), 'lng_range': (-70.58, -70.45)},
            'Maipú': {'lat_range': (-33.52, -33.48), 'lng_range': (-70.82, -70.70)},
            'Puente Alto': {'lat_range': (-33.65, -33.55), 'lng_range': (-70.65, -70.55)}
        }
        
        for municipality, bounds in municipalities.items():
            if (bounds['lat_range'][0] <= lat <= bounds['lat_range'][1] and
                bounds['lng_range'][0] <= lng <= bounds['lng_range'][1]):
                return municipality
        
        return 'Región Metropolitana'
    
    def save_events(self, events: List[Dict]):
        """Guarda eventos en MongoDB"""
        if not events:
            return
        
        try:
            # Eliminar duplicados basados en event_id
            unique_events = []
            seen_ids = set()
            
            for event in events:
                if event['event_id'] not in seen_ids:
                    unique_events.append(event)
                    seen_ids.add(event['event_id'])
            
            if unique_events:
                result = self.collection.insert_many(unique_events, ordered=False)
                logger.info(f"Guardados {len(result.inserted_ids)} eventos únicos")
            
        except pymongo.errors.BulkWriteError as e:
            # Ignorar errores de duplicados
            logger.info(f"Algunos eventos ya existían: {len(e.details['writeErrors'])} duplicados")
            
        except Exception as e:
            logger.error(f"Error guardando eventos: {e}")
    
    def generate_synthetic_events(self, count: int = 100) -> List[Dict]:
        """Genera eventos sintéticos para pruebas cuando Waze no está disponible"""
        import random
        
        events = []
        event_types = ['alert', 'jam', 'accident', 'road_closure', 'hazard']
        municipalities = ['Santiago', 'Las Condes', 'Providencia', 'Ñuñoa', 'Maipú', 'Puente Alto']
        
        for i in range(count):
            municipality = random.choice(municipalities)
            event_type = random.choice(event_types)
            
            # Coordenadas aleatorias dentro de la RM
            lat = random.uniform(-33.7, -33.3)
            lng = random.uniform(-70.9, -70.4)
            
            event = {
                'event_id': f"synthetic_{i}_{int(time.time())}",
                'event_type': event_type,
                'subtype': f"{event_type}_synthetic",
                'description': f"Evento sintético {event_type} en {municipality}",
                'location': {'lat': lat, 'lng': lng},
                'street': f"Calle {random.randint(1, 999)}",
                'city': municipality,
                'municipality': municipality,
                'timestamp': datetime.now(timezone.utc),
                'confidence': random.randint(1, 10),
                'severity': random.randint(1, 5),
                'source': 'synthetic'
            }
            
            events.append(event)
        
        return events
    
    async def run_scraper(self):
        """Ejecuta el proceso de scraping principal"""
        logger.info("Iniciando scraper de Waze")
        
        while True:
            try:
                all_events = []
                
                # Intentar API primero
                api_events = await self.get_waze_data_api()
                all_events.extend(api_events)
                
                # Si no hay suficientes eventos, intentar web scraping
                if len(api_events) < 10:
                    web_events = self.scrape_waze_web()
                    all_events.extend(web_events)
                
                # Si aún no hay suficientes eventos, generar sintéticos
                if len(all_events) < 50:
                    synthetic_events = self.generate_synthetic_events(100)
                    all_events.extend(synthetic_events)
                    logger.info("Agregados eventos sintéticos para pruebas")
                
                # Guardar eventos
                self.save_events(all_events)
                
                # Estadísticas
                total_events = self.collection.count_documents({})
                logger.info(f"Total de eventos en base de datos: {total_events}")
                
                # Esperar hasta el próximo scraping
                logger.info(f"Esperando {self.scrape_interval} segundos para próximo scraping")
                await asyncio.sleep(self.scrape_interval)
                
            except Exception as e:
                logger.error(f"Error en ciclo de scraping: {e}")
                await asyncio.sleep(60)  # Esperar 1 minuto antes de reintentar

def main():
    """Función principal"""
    mongodb_uri = os.getenv('MONGODB_URI', 'mongodb://admin:password123@localhost:27017/traffic_db?authSource=admin')
    scrape_interval = int(os.getenv('SCRAPE_INTERVAL', '300'))
    
    scraper = WazeScraper(mongodb_uri, scrape_interval)
    scraper.connect_database()
    
    # Ejecutar scraper
    asyncio.run(scraper.run_scraper())

if __name__ == "__main__":
    main()