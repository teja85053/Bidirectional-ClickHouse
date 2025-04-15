from clickhouse_client import ClickHouseClient
from file_handler import FileHandler
import os

def ingest_clickhouse_to_file(config):
    """
    Transfer data from ClickHouse to a CSV file
    
    Args:
        config (dict): Configuration with connection details, table, columns, filepath, and delimiter
        
    Returns:
        int: Number of records transferred
    """
    try:
        # Connect to ClickHouse
        client = ClickHouseClient(**config['conn'])
        
        # Fetch data from ClickHouse
        data = client.fetch_data(config['table'], config['columns'])
        
        # Ensure the directory exists
        directory = os.path.dirname(config['filepath'])
        if directory:
            os.makedirs(directory, exist_ok=True)
        
        # Write data to file
        handler = FileHandler(config['filepath'], config['delimiter'])
        handler.write_data(config['columns'], data)
        
        return len(data)
    except Exception as e:
        raise Exception(f"Error in ClickHouse to file ingestion: {str(e)}")

def ingest_file_to_clickhouse(config):
    """
    Transfer data from a CSV file to ClickHouse
    
    Args:
        config (dict): Configuration with connection details, table, columns, filepath, and delimiter
        
    Returns:
        int: Number of records transferred
    """
    try:
        # Read data from file
        handler = FileHandler(config['filepath'], config['delimiter'])
        data = handler.read_data(config['columns'])
        
        if not data:
            return 0
        
        # Insert data into ClickHouse
        client = ClickHouseClient(**config['conn'])
        client.insert_data(config['table'], config['columns'], data)
        
        return len(data)
    except Exception as e:
        raise Exception(f"Error in file to ClickHouse ingestion: {str(e)}")