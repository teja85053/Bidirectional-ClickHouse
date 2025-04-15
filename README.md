# ClickHouse Flat File Ingestion Tool

A web-based tool for seamless data transfer between ClickHouse databases and flat files (CSV).

## Overview

This tool provides a user-friendly interface for data engineers and analysts to:

- Export data from ClickHouse tables to CSV files
- Import data from CSV files into ClickHouse tables
- Preview data before transfer
- Monitor transfer progress in real-time
- View total records processed

## Features

- Bidirectional data transfer (ClickHouse ↔ CSV)
- Column selection for targeted data import/export
- Real-time progress monitoring via WebSockets
- Data preview (first 100 rows) before transfer
- Display total record count after ingestion
- Error handling with friendly messages
- JWT token-based ClickHouse authentication
- Secure file handling and SQL injection protection
- Progress bar for ingestion progress

## Architecture

The application consists of:

1. **Frontend**: HTML/CSS/JavaScript web interface
2. **Backend API**: Flask/Python server
3. **Core Components**:
   - ClickHouse Client: Connects using user, password, and JWT token
   - File Handler: Manages CSV file uploads, parsing, and validation
   - Ingestion System: Orchestrates efficient batch transfers and record counting

## Installation

### Prerequisites

- Python 3.11+
- Docker (for running ClickHouse)
- Web browser

### Setup

1. Clone this repository:
   ```bash
   git clone https://github.com/teja85053/clickhouse-flat-file-tool.git
   cd clickhouse-flat-file-tool
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create a directory for flat file data:
   ```bash
   mkdir data
   ```

5. Start a ClickHouse container (if not already running):
   ```bash
   docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 clickhouse/clickhouse-server
   ```

6. Inside ClickHouse:
   - Create a user teja
   - Create a database uk
   - Create example tables: uk_price_paid, ontime

## Usage

### Starting the Server
```bash
python app.py
```

The UI is accessible at: http://localhost:5000

### Using the Tool

1. **Select Direction**:
   - ClickHouse → File: Export to CSV
   - File → ClickHouse: Import CSV to DB

2. **Configure Connections**:
   - Host, port, database name, user, and JWT token for ClickHouse
   - File name and delimiter for flat file

3. **Select Data**:
   - Choose table (ClickHouse) or upload CSV
   - Select columns to include
   - (Bonus) Option to join tables by specifying JOIN keys

4. **Data Operations**:
   - Preview first 100 rows
   - Start ingestion
   - View record count and progress

## Security Considerations

- JWT-based ClickHouse authentication
- Path and input sanitization
- Column/table name validation to prevent SQL injection

## API Reference

### Endpoints

- GET `/test`: API health check
- POST `/connect_clickhouse`: Establish ClickHouse connection
- POST `/get_columns`: Get available columns
- POST `/preview_data`: Preview up to 100 rows
- POST `/ingest`: Run the data ingestion
- GET `/download/<filename>`: Download the exported file

### WebSocket Events

- `connect`: WebSocket handshake
- `progress_update`: Progress updates
- `ingestion_complete`: Successful transfer notification
- `ingestion_error`: Error during ingestion

## File Structure

```bash
clickhouse-flat-file-tool/
├── backend/
│   ├── app.py               # Flask API
│   ├── clickhouse_client.py # ClickHouse connector
│   ├── file_handler.py      # CSV file operations
│   ├── ingestion.py         # Core data ingestion logic
│   ├── utils.py             # Helper functions
├── frontend/
│   ├── index.html           # UI interface
├── requirements.txt         # Dependencies
├── clickhouse_tool.log      # Runtime logs
└── data/                    # CSV storage
```

## Development

### Adding New Features

- New File Types: Extend FileHandler in file_handler.py
- New DB Support: Add client module similar to clickhouse_client.py

### Testing

#### Datasets Used

ClickHouse sample datasets:
- uk_price_paid
- ontime

#### Sample Test Cases

- Export selected columns from uk_price_paid to CSV
- Import a local CSV into a new ClickHouse table and verify record count
- (Bonus) Join uk_price_paid and ontime on a specified key and export
- Simulate connection/authentication failures
- Preview first 100 rows before ingestion (optional)

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch
3. Submit a Pull Request with clear changes

## License

This project is licensed under the MIT License.

## Acknowledgments


- [ClickHouse](https://clickhouse.tech/) — Open-source columnar database used for high-performance analytics
- [Flask](https://flask.palletsprojects.com/) — Lightweight Python web framework used for the backend API
- [Socket.IO](https://socket.io/) — Real-time communication library used for progress updates
- [ClickHouse Example Datasets](https://clickhouse.com/docs/en/getting-started/example-datasets) — Sample dataset (`uk_price_paid`, `ontime`) used for testing
- [Docker](https://www.docker.com/) — Used for containerizing and running the ClickHouse server
