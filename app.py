import os
import sqlite3
import requests
import uuid
import xml.etree.ElementTree as ET
from flask import Flask, request, send_file, after_this_request

app = Flask(__name__)

# Configuración
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

@app.route('/', methods=['GET'])
def health_check():
    return "Car Launcher API (Gzip Fix) is Running", 200

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

        # Solicitamos XML
        query = f"""
        [out:xml][timeout:180];
        (
          node["name"]({min_lat},{min_lon},{max_lat},{max_lon});
          way["name"]({min_lat},{min_lon},{max_lat},{max_lon});
        );
        out center;
        """
        
        print(f"Iniciando descarga Stream OSM para: {min_lat},{min_lon}")
        
        # stream=True es vital
        response = requests.get(OVERPASS_URL, params={'data': query}, stream=True)
        
        # --- CORRECCIÓN CRÍTICA ---
        # 1. Verificar si OSM nos rechazó la conexión antes de intentar leer
        if response.status_code != 200:
            error_msg = f"OSM Error {response.status_code}: {response.text[:200]}" # Leemos solo el principio
            print(error_msg)
            return error_msg, 502

        # 2. ACTIVAR DESCOMPRESIÓN AUTOMÁTICA
        # Esto evita que el XML parser reciba basura binaria (Gzip) y explote
        response.raw.decode_content = True

        # Preparamos la DB
        conn = sqlite3.connect(filename)
        cursor = conn.cursor()
        cursor.execute('PRAGMA synchronous = OFF') 
        cursor.execute('PRAGMA journal_mode = MEMORY')
        
        cursor.execute('''
            CREATE VIRTUAL TABLE search_index USING fts4(
                name, 
                address, 
                lat, 
                lon
            );
        ''')

        # Procesamiento por Streaming
        # Ahora sí, response.raw entrega texto limpio, no zip
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
                        batch.append((name, addr, lat, lon))
                        count += 1

                elem.clear()
                
                if len(batch) >= 1000:
                    cursor.executemany("INSERT INTO search_index (name, address, lat, lon) VALUES (?, ?, ?, ?)", batch)
                    batch = []

        if batch:
            cursor.executemany("INSERT INTO search_index (name, address, lat, lon) VALUES (?, ?, ?, ?)", batch)

        conn.commit()
        conn.close()
        print(f"¡ÉXITO! Procesados {count} elementos.")

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
        print(f"Error crítico en Python: {e}")
        return f"Error interno: {str(e)}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
