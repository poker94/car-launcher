import os
import sqlite3
import requests
import uuid
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

@app.route('/', methods=['GET'])
def health_check():
    return "Car Launcher API (Multi-Map Merge) is Running", 200

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

        # Query optimizada (solo nombres o direcciones)
        query = f"""
        [out:xml][timeout:180];
        (
          node["name"]({min_lat},{min_lon},{max_lat},{max_lon});
          way["name"]({min_lat},{min_lon},{max_lat},{max_lon});
          node["addr:housenumber"]({min_lat},{min_lon},{max_lat},{max_lon});
          way["addr:housenumber"]({min_lat},{min_lon},{max_lat},{max_lon});
        );
        out center;
        """
        
        print(f"Descargando Merge-Ready: {min_lat},{min_lon}")
        
        headers = {'User-Agent': 'CarLauncher/1.0', 'Accept-Encoding': 'gzip'}
        response = requests.get(OVERPASS_URL, params={'data': query}, headers=headers, stream=True)
        
        if response.status_code != 200:
            return f"OSM Error {response.status_code}", 502

        response.raw.decode_content = True

        conn = sqlite3.connect(filename)
        cursor = conn.cursor()
        cursor.execute('PRAGMA synchronous = OFF') 
        cursor.execute('PRAGMA journal_mode = MEMORY')
        
        # --- CAMBIO IMPORTANTE: Columna 'osm_id' ---
        cursor.execute('''
            CREATE VIRTUAL TABLE search_index USING fts4(
                osm_id,  -- ID Único para evitar duplicados al fusionar
                name, 
                address, 
                lat, 
                lon, 
                keywords
            );
        ''')

        context = ET.iterparse(response.raw, events=('end',))
        batch = []
        
        for event, elem in context:
            if elem.tag in ('node', 'way'):
                # Generamos ID único: "n12345" o "w67890"
                raw_id = elem.get('id')
                type_prefix = "n" if elem.tag == 'node' else "w"
                unique_osm_id = f"{type_prefix}{raw_id}"

                tags = {tag.get('k'): tag.get('v') for tag in elem.findall('tag')}
                
                raw_name = tags.get('name')
                street = tags.get('addr:street')
                number = tags.get('addr:housenumber')
                
                lat, lon = None, None
                if elem.tag == 'node':
                    lat, lon = elem.get('lat'), elem.get('lon')
                elif elem.tag == 'way':
                    center = elem.find('center')
                    if center: lat, lon = center.get('lat'), center.get('lon')

                if lat and lon:
                    # 1. Dirección
                    if street and number:
                        address_name = f"{street} {number}"
                        subtitle = labels['highway']
                        kw_addr = address_name
                        for full, abbr in ABBREVIATIONS.items():
                            if full in address_name:
                                kw_addr += " " + address_name.replace(full, abbr)

                        # Guardamos con unique_osm_id + sufijo "_addr" para diferenciarlo del negocio
                        batch.append((f"{unique_osm_id}_addr", address_name, subtitle, lat, lon, kw_addr))

                    # 2. Negocio
                    if raw_name:
                        poi_name = raw_name
                        poi_subtitle = ""
                        if street and number:
                            poi_subtitle = f"{street} {number}"
                        elif 'amenity' in tags:
                            poi_subtitle = labels['amenity']
                        else:
                            poi_subtitle = labels['highway']

                        kw_poi = poi_name
                        if street: kw_poi += " " + street
                        
                        batch.append((unique_osm_id, poi_name, poi_subtitle, lat, lon, kw_poi))

                elem.clear()
                if len(batch) >= 2000:
                    cursor.executemany("INSERT INTO search_index VALUES (?, ?, ?, ?, ?, ?)", batch)
                    batch = []

        if batch:
            cursor.executemany("INSERT INTO search_index VALUES (?, ?, ?, ?, ?, ?)", batch)

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
