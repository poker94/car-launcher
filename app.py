import os
import sqlite3
import requests
import uuid
import xml.etree.ElementTree as ET
from flask import Flask, request, send_file, after_this_request

app = Flask(__name__)

# Usamos el mirror rápido
OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"

# --- DICCIONARIO DE ABREVIATURAS ---
# Esto ayuda a que "W" encuentre "West", "Av" encuentre "Avenue", etc.
ABBREVIATIONS = {
    "West": "W", "North": "N", "South": "S", "East": "E",
    "Avenue": "Ave Av", "Street": "St", "Boulevard": "Blvd",
    "Road": "Rd", "Drive": "Dr", "Lane": "Ln", "Court": "Ct",
    "Place": "Pl", "Square": "Sq", "Highway": "Hwy"
}

@app.route('/', methods=['GET'])
def health_check():
    return "Car Launcher API (Smart Search) is Running", 200

@app.route('/generate_db', methods=['GET'])
def generate_db():
    filename = f"map_{uuid.uuid4()}.db"
    conn = None
    try:
        min_lat = request.args.get('minLat')
        min_lon = request.args.get('minLon')
        max_lat = request.args.get('maxLat')
        max_lon = request.args.get('maxLon')
        
        if not all([min_lat, min_lon, max_lat, max_lon]):
            return "Faltan coordenadas", 400

        query = f"""
        [out:xml][timeout:180];
        (
          node["name"]({min_lat},{min_lon},{max_lat},{max_lon});
          way["name"]({min_lat},{min_lon},{max_lat},{max_lon});
        );
        out center;
        """
        
        print(f"Descargando (Smart Mode): {min_lat},{min_lon}")
        
        headers = {
            'User-Agent': 'CarLauncher/1.0',
            'Accept-Encoding': 'gzip'
        }
        
        response = requests.get(OVERPASS_URL, params={'data': query}, headers=headers, stream=True)
        
        if response.status_code != 200:
            return f"OSM Error {response.status_code}", 502

        response.raw.decode_content = True

        conn = sqlite3.connect(filename)
        cursor = conn.cursor()
        cursor.execute('PRAGMA synchronous = OFF') 
        cursor.execute('PRAGMA journal_mode = MEMORY')
        
        # --- CAMBIO IMPORTANTE: Agregamos columna 'keywords' ---
        cursor.execute('''
            CREATE VIRTUAL TABLE search_index USING fts4(
                name, 
                address, 
                lat, 
                lon,
                keywords  -- Columna oculta para búsquedas inteligentes
            );
        ''')

        context = ET.iterparse(response.raw, events=('end',))
        
        batch = []
        count = 0
        
        for event, elem in context:
            if elem.tag in ('node', 'way'):
                tags = {}
                for tag in elem.findall('tag'):
                    k = tag.get('k')
                    v = tag.get('v')
                    tags[k] = v
                
                name = tags.get('name')
                
                if name:
                    addr = ""
                    if 'addr:street' in tags:
                        addr = f"{tags['addr:street']} {tags.get('addr:housenumber', '')}"
                    elif 'amenity' in tags:
                        addr = tags['amenity']
                    elif 'highway' in tags:
                        addr = "Calle"
                        
                    # --- LÓGICA DE ALIAS ---
                    # Generamos una versión del nombre con abreviaturas
                    # Ej: "West Washington" -> "West Washington W Washington"
                    keywords = name
                    for full, abbr in ABBREVIATIONS.items():
                        if full in name:
                            # Agregamos la versión abreviada a las palabras clave
                            keywords += " " + name.replace(full, abbr)
                    
                    lat = None
                    lon = None
                    
                    if elem.tag == 'node':
                        lat = elem.get('lat')
                        lon = elem.get('lon')
                    elif elem.tag == 'way':
                        center = elem.find('center')
                        if center is not None:
                            lat = center.get('lat')
                            lon = center.get('lon')

                    if lat and lon:
                        # Guardamos: name, addr, lat, lon, KEYWORDS
                        batch.append((name, addr, lat, lon, keywords))
                        count += 1

                elem.clear()
                
                if len(batch) >= 1000:
                    cursor.executemany("INSERT INTO search_index (name, address, lat, lon, keywords) VALUES (?, ?, ?, ?, ?)", batch)
                    batch = []

        if batch:
            cursor.executemany("INSERT INTO search_index (name, address, lat, lon, keywords) VALUES (?, ?, ?, ?, ?)", batch)

        conn.commit()
        conn.close()
        print(f"¡ÉXITO! {count} items indexados.")

        @after_this_request
        def remove_file(response):
            try:
                if os.path.exists(filename):
                    os.remove(filename)
            except Exception as error:
                app.logger.error("Error removing file", error)
            return response

        return send_file(filename, as_attachment=True, download_name="offline_data.db")

    except Exception as e:
        if conn: conn.close()
        if os.path.exists(filename): os.remove(filename)
        print(f"Error crítico: {e}")
        return f"Error interno: {str(e)}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
