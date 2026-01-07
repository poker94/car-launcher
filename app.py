import os
import sqlite3
import requests
import uuid
from flask import Flask, request, send_file, after_this_request

app = Flask(__name__)

# Configuración
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# --- RUTA 1: HEALTH CHECK (Para el "Despertador") ---
# Esta ruta no hace nada pesado, solo dice "Estoy vivo".
@app.route('/', methods=['GET'])
def health_check():
    return "Car Launcher API is Running OK", 200

# --- RUTA 2: GENERADOR DE DB (Para la App) ---
@app.route('/generate_db', methods=['GET'])
def generate_db():
    try:
        # 1. Leer coordenadas
        min_lat = request.args.get('minLat')
        min_lon = request.args.get('minLon')
        max_lat = request.args.get('maxLat')
        max_lon = request.args.get('maxLon')
        
        if not all([min_lat, min_lon, max_lat, max_lon]):
            return "Faltan coordenadas", 400

        filename = f"map_{uuid.uuid4()}.db"
        
        # 2. Consultar OpenStreetMap (Timeout 90s)
        # Buscamos nodos y calles con nombre
        query = f"""
        [out:json][timeout:90];
        (
          node["name"]({min_lat},{min_lon},{max_lat},{max_lon});
          way["name"]({min_lat},{min_lon},{max_lat},{max_lon});
        );
        out center;
        """
        
        print(f"Consultando OSM: {min_lat},{min_lon},{max_lat},{max_lon}")
        response = requests.get(OVERPASS_URL, params={'data': query})
        
        if response.status_code != 200:
            return f"Error OSM: {response.status_code}", 502

        data = response.json()
        elements = data.get('elements', [])
        
        if not elements:
             return "No se encontraron lugares en esta área", 404

        # 3. Crear Base de Datos SQLite (Usando FTS4 para compatibilidad total)
        conn = sqlite3.connect(filename)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE VIRTUAL TABLE search_index USING fts4(
                name, 
                address, 
                lat, 
                lon
            );
        ''')

        insert_list = []
        for el in elements:
            tags = el.get('tags', {})
            name = tags.get('name', '')
            
            addr = ""
            if 'addr:street' in tags:
                addr = f"{tags['addr:street']} {tags.get('addr:housenumber', '')}"
            elif 'amenity' in tags:
                addr = tags['amenity']
            elif 'highway' in tags:
                addr = "Calle"
            
            lat = el.get('lat')
            lon = el.get('lon')
            
            if not lat and 'center' in el:
                lat = el['center'].get('lat')
                lon = el['center'].get('lon')

            if name and lat and lon:
                insert_list.append((name, addr, lat, lon))

        cursor.executemany("INSERT INTO search_index (name, address, lat, lon) VALUES (?, ?, ?, ?)", insert_list)
        conn.commit()
        conn.close()

        # 4. Programar autodestrucción del archivo para limpiar el servidor
        @after_this_request
        def remove_file(response):
            try:
                if os.path.exists(filename):
                    os.remove(filename)
            except Exception as error:
                app.logger.error("Error removing file", error)
            return response

        # 5. Enviar al celular
        return send_file(filename, as_attachment=True, download_name="offline_data.db")

    except Exception as e:
        print(f"Error: {e}")
        return f"Error interno: {str(e)}", 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)