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
    return "Car Launcher API (Stream Optimized) is Running", 200

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

        # 1. Solicitamos XML en lugar de JSON (out:xml)
        # Usamos stream=True para no cargar todo en memoria
        query = f"""
        [out:xml][timeout:180];
        (
          node["name"]({min_lat},{min_lon},{max_lat},{max_lon});
          way["name"]({min_lat},{min_lon},{max_lat},{max_lon});
        );
        out center;
        """
        
        print(f"Iniciando descarga Stream OSM...")
        # stream=True es la clave aquí
        response = requests.get(OVERPASS_URL, params={'data': query}, stream=True)
        
        if response.status_code != 200:
            return f"Error OSM: {response.status_code}", 502

        # 2. Preparamos la DB
        conn = sqlite3.connect(filename)
        cursor = conn.cursor()
        cursor.execute('PRAGMA synchronous = OFF') # Acelera la escritura
        cursor.execute('PRAGMA journal_mode = MEMORY')
        
        cursor.execute('''
            CREATE VIRTUAL TABLE search_index USING fts4(
                name, 
                address, 
                lat, 
                lon
            );
        ''')

        # 3. Procesamiento por Streaming (Iterparse)
        # Esto lee el archivo XML a medida que se descarga
        context = ET.iterparse(response.raw, events=('end',))
        
        batch = []
        count = 0
        
        for event, elem in context:
            if elem.tag in ('node', 'way'):
                tags = {}
                # Extraer tags hijos
                for tag in elem.findall('tag'):
                    k = tag.get('k')
                    v = tag.get('v')
                    tags[k] = v
                
                name = tags.get('name')
                
                if name:
                    # Lógica de dirección
                    addr = ""
                    if 'addr:street' in tags:
                        addr = f"{tags['addr:street']} {tags.get('addr:housenumber', '')}"
                    elif 'amenity' in tags:
                        addr = tags['amenity']
                    elif 'highway' in tags:
                        addr = "Calle"
                        
                    # Lógica de coordenadas
                    lat = None
                    lon = None
                    
                    if elem.tag == 'node':
                        lat = elem.get('lat')
                        lon = elem.get('lon')
                    elif elem.tag == 'way':
                        # En 'out center', los ways tienen un hijo <center>
                        center = elem.find('center')
                        if center is not None:
                            lat = center.get('lat')
                            lon = center.get('lon')

                    if lat and lon:
                        batch.append((name, addr, lat, lon))
                        count += 1

                # 4. Limpieza de memoria fundamental
                elem.clear()
                
                # Insertar en lotes de 1000 para no saturar RAM
                if len(batch) >= 1000:
                    cursor.executemany("INSERT INTO search_index (name, address, lat, lon) VALUES (?, ?, ?, ?)", batch)
                    batch = []

        # Insertar los restantes
        if batch:
            cursor.executemany("INSERT INTO search_index (name, address, lat, lon) VALUES (?, ?, ?, ?)", batch)

        conn.commit()
        conn.close()
        print(f"Procesados {count} elementos con éxito.")

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