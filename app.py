import os
import sqlite3
import requests
import uuid
import re
import xml.etree.ElementTree as ET
from flask import Flask, request, send_file, after_this_request

app = Flask(__name__)

# Mirror rápido
OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"

ABBREVIATIONS = {
    "West": "W", "North": "N", "South": "S", "East": "E",
    "Avenue": "Ave Av", "Street": "St", "Boulevard": "Blvd",
    "Road": "Rd", "Drive": "Dr", "Lane": "Ln", "Court": "Ct",
    "Place": "Pl", "Square": "Sq", "Highway": "Hwy"
}

LANG_LABELS = {
    'es': { 'highway': 'Calle', 'amenity': 'Lugar' },
    'en': { 'highway': 'Street', 'amenity': 'Place' },
    'pt': { 'highway': 'Rua', 'amenity': 'Lugar' },
    'fr': { 'highway': 'Rue', 'amenity': 'Lieu' },
    'default': { 'highway': 'Street', 'amenity': 'Place' }
}

# Clasificador de Tipos para Iconos
def get_place_type(tags):
    amenity = tags.get('amenity', '')
    shop = tags.get('shop', '')
    leisure = tags.get('leisure', '')
    highway = tags.get('highway', '')
    
    if amenity in ['fuel', 'charging_station']: return 'fuel'
    if amenity in ['restaurant', 'fast_food', 'cafe', 'bar', 'pub']: return 'food'
    if amenity in ['bank', 'atm']: return 'bank'
    if amenity in ['parking']: return 'parking'
    if amenity in ['school', 'university', 'kindergarten']: return 'school'
    if amenity in ['hospital', 'clinic', 'pharmacy', 'doctors']: return 'health'
    if amenity in ['cinema', 'theatre', 'casino']: return 'entertainment'
    if shop in ['supermarket', 'convenience', 'greengrocer']: return 'market'
    if shop: return 'shop'
    if leisure in ['park', 'garden', 'pitch']: return 'park'
    if highway: return 'street'
    
    if tags.get('addr:housenumber'): return 'home'
    
    return 'other'

# Función para limpiar velocidad
def parse_speed_limit(tags):
    raw_speed = tags.get('maxspeed', '')
    if not raw_speed: return None
    
    # Extraemos solo el número (ej: "50 mph" -> "50")
    match = re.match(r"([0-9]+)", raw_speed)
    if match:
        return match.group(1)
    return None

@app.route('/', methods=['GET'])
def health_check():
    return "Car Launcher API (Regional Search Ready) is Running", 200

# --- NUEVO: Endpoint para buscar regiones por nombre (Geocoding) ---
@app.route('/resolve_region', methods=['GET'])
def resolve_region():
    try:
        query = request.args.get('name')
        if not query:
            return "Falta el nombre", 400

        # Usamos la API pública de Nominatim (Requiere User-Agent)
        nominatim_url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': query,
            'format': 'json',
            'limit': 1,
            'polygon_geojson': 0
        }
        headers = {'User-Agent': 'CarLauncher/1.0'}
        
        response = requests.get(nominatim_url, params=params, headers=headers)
        
        if response.status_code != 200:
            return "Error en Nominatim", 502
            
        data = response.json()
        
        if not data:
            return "No se encontró la región", 404
            
        # Nominatim devuelve boundingbox como: [minLat, maxLat, minLon, maxLon] (strings)
        result = data[0]
        bbox = result.get('boundingbox')
        
        return {
            "name": result.get('display_name'),
            "minLat": float(bbox[0]),
            "maxLat": float(bbox[1]),
            "minLon": float(bbox[2]),
            "maxLon": float(bbox[3])
        }

    except Exception as e:
        return f"Error: {e}", 500

@app.route('/generate_db', methods=['GET'])
def generate_db():
    filename = f"map_{uuid.uuid4()}.db"
    conn = None
    try:
        min_lat = request.args.get('minLat')
        min_lon = request.args.get('minLon')
        max_lat = request.args.get('maxLat')
        max_lon = request.args.get('maxLon')
        lang_code = request.args.get('lang', 'en')
        labels = LANG_LABELS.get(lang_code, LANG_LABELS['default'])
        
        if not all([min_lat, min_lon, max_lat, max_lon]):
            return "Faltan coordenadas", 400

        query = f"""
        [out:xml][timeout:180];
        (
          node["name"]({min_lat},{min_lon},{max_lat},{max_lon});
          way["name"]({min_lat},{min_lon},{max_lat},{max_lon});
          node["addr:housenumber"]({min_lat},{min_lon},{max_lat},{max_lon});
          way["addr:housenumber"]({min_lat},{min_lon},{max_lat},{max_lon});
          way["maxspeed"]({min_lat},{min_lon},{max_lat},{max_lon});
        );
        out center;
        """
        
        print(f"Descargando Speed-Ready: {min_lat},{min_lon}")
        
        headers = {'User-Agent': 'CarLauncher/1.0', 'Accept-Encoding': 'gzip'}
        response = requests.get(OVERPASS_URL, params={'data': query}, headers=headers, stream=True)
        
        if response.status_code != 200:
            return f"OSM Error {response.status_code}", 502

        response.raw.decode_content = True

        conn = sqlite3.connect(filename)
        cursor = conn.cursor()
        cursor.execute('PRAGMA synchronous = OFF') 
        cursor.execute('PRAGMA journal_mode = MEMORY')
        
        cursor.execute('''
            CREATE VIRTUAL TABLE search_index USING fts4(
                osm_id, 
                name, 
                address, 
                lat, 
                lon, 
                keywords,
                type,
                speed_limit
            );
        ''')

        context = ET.iterparse(response.raw, events=('end',))
        batch = []
        
        for event, elem in context:
            if elem.tag in ('node', 'way'):
                raw_id = elem.get('id')
                type_prefix = "n" if elem.tag == 'node' else "w"
                unique_osm_id = f"{type_prefix}{raw_id}"

                tags = {tag.get('k'): tag.get('v') for tag in elem.findall('tag')}
                
                raw_name = tags.get('name')
                street = tags.get('addr:street')
                number = tags.get('addr:housenumber')
                
                place_type = get_place_type(tags)
                speed = parse_speed_limit(tags)
                
                lat, lon = None, None
                if elem.tag == 'node':
                    lat, lon = elem.get('lat'), elem.get('lon')
                elif elem.tag == 'way':
                    center = elem.find('center')
                    if center: lat, lon = center.get('lat'), center.get('lon')

                if lat and lon:
                    if raw_name or (street and number) or speed:
                        
                        if street and number:
                            address_name = f"{street} {number}"
                            subtitle = labels['highway']
                            kw_addr = address_name
                            for full, abbr in ABBREVIATIONS.items():
                                if full in address_name:
                                    kw_addr += " " + address_name.replace(full, abbr)

                            addr_type = 'home'
                            batch.append((f"{unique_osm_id}_addr", address_name, subtitle, lat, lon, kw_addr, addr_type, None))

                        poi_name = raw_name
                        if not poi_name and speed:
                            poi_name = street if street else labels['highway']

                        if poi_name:
                            poi_subtitle = ""
                            if street and number:
                                poi_subtitle = f"{street} {number}"
                            elif 'amenity' in tags:
                                poi_subtitle = labels['amenity']
                            else:
                                poi_subtitle = labels['highway']

                            kw_poi = poi_name
                            if street: kw_poi += " " + street
                            
                            batch.append((unique_osm_id, poi_name, poi_subtitle, lat, lon, kw_poi, place_type, speed))

                elem.clear()
                if len(batch) >= 2000:
                    cursor.executemany("INSERT INTO search_index VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)
                    batch = []

        if batch:
            cursor.executemany("INSERT INTO search_index VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)

        conn.commit()
        conn.close()

        @after_this_request
        def remove_file(response):
            try:
                if os.path.exists(filename): os.remove(filename)
            except: pass
            return response

        return send_file(filename, as_attachment=True, download_name="offline_data.db")

    except Exception as e:
        if conn: conn.close()
        return f"Error: {e}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
