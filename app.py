import os
import sqlite3
import requests
import uuid
import xml.etree.ElementTree as ET
from flask import Flask, request, send_file, after_this_request

app = Flask(__name__)

# Mirror rápido
OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"

# 1. DICCIONARIO DE ABREVIATURAS (Para búsqueda inteligente)
ABBREVIATIONS = {
    "West": "W", "North": "N", "South": "S", "East": "E",
    "Avenue": "Ave Av", "Street": "St", "Boulevard": "Blvd",
    "Road": "Rd", "Drive": "Dr", "Lane": "Ln", "Court": "Ct",
    "Place": "Pl", "Square": "Sq", "Highway": "Hwy"
}

# 2. DICCIONARIO DE IDIOMAS (Para etiquetas visuales)
# Puedes agregar más idiomas aquí (pt, fr, it, etc.)
LANG_LABELS = {
    'es': { 'highway': 'Calle', 'amenity': 'Lugar' },
    'en': { 'highway': 'Street', 'amenity': 'Place' },
    'pt': { 'highway': 'Rua', 'amenity': 'Lugar' },
    'fr': { 'highway': 'Rue', 'amenity': 'Lieu' },
    'default': { 'highway': 'Street', 'amenity': 'Place' }
}

@app.route('/', methods=['GET'])
def health_check():
    return "Car Launcher API (Multi-Lang) is Running", 200

@app.route('/generate_db', methods=['GET'])
def generate_db():
    filename = f"map_{uuid.uuid4()}.db"
    conn = None
    try:
        min_lat = request.args.get('minLat')
        min_lon = request.args.get('minLon')
        max_lat = request.args.get('maxLat')
        max_lon = request.args.get('maxLon')
        
        # Leemos el idioma (por defecto inglés si no llega nada)
        lang_code = request.args.get('lang', 'en')
        
        # Seleccionamos las etiquetas según el idioma
        labels = LANG_LABELS.get(lang_code, LANG_LABELS['default'])
        
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
        
        print(f"Descargando: {min_lat},{min_lon} Idioma: {lang_code}")
        
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
        
        cursor.execute('''
            CREATE VIRTUAL TABLE search_index USING fts4(
                name, 
                address, 
                lat, 
                lon,
                keywords
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
                        # Si OSM ya trae calle y número, usamos eso
                        addr = f"{tags['addr:street']} {tags.get('addr:housenumber', '')}"
                    elif 'amenity' in tags:
                        # Usamos la etiqueta traducida o el tipo de amenity
                        addr = labels['amenity'] 
                        # Opcional: si quieres el tipo específico (ej: School) descomenta esto:
                        # addr = tags['amenity'].capitalize()
                    elif 'highway' in tags:
                        # AQUÍ LA MAGIA: Usamos la traducción según el idioma
                        addr = labels['highway']
                        
                    keywords = name
                    for full, abbr in ABBREVIATIONS.items():
                        if full in name:
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
        return f"Error interno: {str(e)}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
