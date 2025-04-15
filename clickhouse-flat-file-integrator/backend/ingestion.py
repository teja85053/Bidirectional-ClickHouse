from clickhouse_client import ClickHouseClient
from file_handler import FileHandler
import os
import time

def ingest_clickhouse_to_file(config, progress_callback=None):
    """
    Transfer data from ClickHouse to a CSV file
    
    Args:
        config (dict): Configuration with connection details, table, columns, filepath, and delimiter
        progress_callback (function): Callback function to report progress
        
    Returns:
        int: Number of records transferred
    """
    try:
        if progress_callback:
            progress_callback(10, 100, "connecting")
        
        # Connect to ClickHouse
        client = ClickHouseClient(**config['conn'])
        
        if progress_callback:
            progress_callback(20, 100, "fetching_metadata")
            
        # Get total row count for progress calculation
        total_count = client.count_rows(config['table'])
        
        if progress_callback:
            progress_callback(30, 100, "fetching_data")
        
        # Fetch data from ClickHouse
        data = client.fetch_data(config['table'], config['columns'])
        
        if progress_callback:
            progress_callback(70, 100, "writing_file")
        
        # Ensure the directory exists
        directory = os.path.dirname(config['filepath'])
        if directory:
            os.makedirs(directory, exist_ok=True)
        
        # Write data to file
        handler = FileHandler(config['filepath'], config['delimiter'])
        handler.write_data(config['columns'], data)
        
        if progress_callback:
            progress_callback(95, 100, "finalizing")
            time.sleep(0.5)  # Small delay to show progress completion
        
        return len(data)
    except Exception as e:
        raise Exception(f"Error in ClickHouse to file ingestion: {str(e)}")

def ingest_file_to_clickhouse(config, progress_callback=None):
    """
    Transfer data from a CSV file to ClickHouse
    
    Args:
        config (dict): Configuration with connection details, table, columns, filepath, and delimiter
        progress_callback (function): Callback function to report progress
        
    Returns:
        int: Number of records transferred
    """
    try:
        if progress_callback:
            progress_callback(10, 100, "reading_file")
        
        # Read data from file
        handler = FileHandler(config['filepath'], config['delimiter'])
        data = handler.read_data(config['columns'], progress_callback)
        
        if not data:
            return 0
        
        if progress_callback:
            progress_callback(60, 100, "connecting_to_clickhouse")
        
        # Insert data into ClickHouse
        client = ClickHouseClient(**config['conn'])
        
        if progress_callback:
            progress_callback(70, 100, "inserting_data")
        
        # Insert data in batches to show progress
        batch_size = 1000
        total_batches = (len(data) + batch_size - 1) // batch_size
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            client.insert_data(config['table'], config['columns'], batch)
            
            batch_num = i // batch_size + 1
            progress = 70 + (25 * batch_num / total_batches)
            
            if progress_callback:
                progress_callback(int(progress), 100, f"inserting_batch_{batch_num}_of_{total_batches}")
        
        if progress_callback:
            progress_callback(95, 100, "finalizing")
            time.sleep(0.5)  # Small delay to show progress completion
        
        return len(data)
    except Exception as e:
        raise Exception(f"Error in file to ClickHouse ingestion: {str(e)}")