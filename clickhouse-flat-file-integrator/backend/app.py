from flask import Flask, request, jsonify, send_file, send_from_directory
from clickhouse_client import ClickHouseClient
from file_handler import FileHandler
from ingestion import ingest_clickhouse_to_file, ingest_file_to_clickhouse
from flask_cors import CORS
from flask_socketio import SocketIO
import os
import threading
import eventlet

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

# Global variable to track progress
progress_data = {
    "current": 0,
    "total": 0,
    "status": "idle"
}

def reset_progress():
    global progress_data
    progress_data = {
        "current": 0,
        "total": 0,
        "status": "idle"
    }

def update_progress(current, total, status="processing"):
    global progress_data
    progress_data["current"] = current
    progress_data["total"] = total
    progress_data["status"] = status
    socketio.emit('progress_update', progress_data)

# Serve frontend files
@app.route('/')
def serve_frontend():
    return send_from_directory('../frontend', 'index.html')

@app.route('/<path:path>')
def serve_static(path):
    if os.path.exists(os.path.join('../frontend', path)):
        return send_from_directory('../frontend', path)
    return "Not found", 404

# Test endpoint to verify API functionality
@app.route('/test')
def test():
    return jsonify({"status": "success", "message": "API is working correctly"})

@app.route('/connect_clickhouse', methods=['POST'])
def connect_clickhouse():
    data = request.json
    try:
        app.logger.info(f"Connecting to ClickHouse with: host={data['host']}, port={data['port']}, db={data['database']}, user={data['user']}")
        client = ClickHouseClient(**data)
        tables = client.get_tables()
        return jsonify({"status": "success", "tables": tables})
    except Exception as e:
        app.logger.error(f"ClickHouse connection error: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route('/get_columns', methods=['POST'])
def get_columns():
    data = request.json
    try:
        if data['source'] == 'clickhouse':
            app.logger.info(f"Getting columns for ClickHouse table: {data.get('table', 'No table specified')}")
            client = ClickHouseClient(**data['conn'])
            columns = client.get_columns(data['table'])
            app.logger.info(f"Successfully retrieved {len(columns)} columns")
            return jsonify({"status": "success", "columns": columns})
        else:
            app.logger.info(f"Getting columns from file: {data.get('filepath', 'No filepath specified')}")
            handler = FileHandler(data['filepath'], data['delimiter'])
            columns = handler.get_columns()
            app.logger.info(f"Successfully retrieved {len(columns)} columns from file")
            return jsonify({"status": "success", "columns": columns})
    except Exception as e:
        app.logger.error(f"Error getting columns: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route('/preview_data', methods=['POST'])
def preview_data():
    data = request.json
    try:
        if data['source'] == 'clickhouse':
            client = ClickHouseClient(**data['conn'])
            result = client.preview_data(data['table'], data['columns'])
        else:
            handler = FileHandler(data['filepath'], data['delimiter'])
            result = handler.preview_data(data['columns'])
        return jsonify(result)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

# Function to handle ingestion in a separate thread with progress reporting
def run_ingestion(data):
    try:
        reset_progress()
        update_progress(0, 100, "preparing")
        
        if data['direction'] == 'clickhouse_to_file':
            count = ingest_clickhouse_to_file(data, update_progress)
        else:
            count = ingest_file_to_clickhouse(data, update_progress)
        
        # Final update showing completion
        update_progress(100, 100, "completed")
        socketio.emit('ingestion_complete', {"status": "success", "count": count})
    except Exception as e:
        app.logger.error(f"Ingestion error: {str(e)}")
        update_progress(0, 100, "error")
        socketio.emit('ingestion_error', {"status": "error", "message": str(e)})

@app.route('/ingest', methods=['POST'])
def ingest():
    data = request.json
    try:
        # Start ingestion in a separate thread
        threading.Thread(target=run_ingestion, args=(data,)).start()
        return jsonify({"status": "started"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@app.route('/download/<path:filename>', methods=['GET'])
def download_file(filename):
    try:
        filepath = os.path.join('data', filename)
        return send_file(filepath, as_attachment=True)
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 400

@socketio.on('connect')
def handle_connect():
    app.logger.info('Client connected to WebSocket')
    # Send current progress state on connection
    socketio.emit('progress_update', progress_data)

@socketio.on('disconnect')
def handle_disconnect():
    app.logger.info('Client disconnected from WebSocket')

if __name__ == '__main__':
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True)