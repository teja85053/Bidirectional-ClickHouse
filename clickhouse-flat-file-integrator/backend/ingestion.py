from clickhouse_client import ClickHouseClient
from file_handler import FileHandler
import os
import time
import datetime

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
        os.makedirs(os.path.dirname(os.path.abspath(config['filepath'])), exist_ok=True)
        
        # Write data to file
        handler = FileHandler(config['filepath'], config['delimiter'])
        handler.write_data(config['columns'], data)
        
        if progress_callback:
            progress_callback(95, 100, "finalizing")
            time.sleep(0.5)  # Small delay to show progress completion
        
        return len(data)
    except Exception as e:
        if progress_callback:
            progress_callback(0, 100, "error")
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
        
        # Connect to ClickHouse
        client = ClickHouseClient(**config['conn'])
        
        if progress_callback:
            progress_callback(70, 100, "preparing_data")
        
        # Process data types according to the ClickHouse schema
        # This is important for handling date/time fields and other specific types
        processed_data = []
        for row in data:
            processed_row = []
            for i, col in enumerate(config['columns']):
                value = row[i]
                
                # Handle special data types
                if col == 'FlightDate' and value:
                    # Convert FlightDate string to proper date object if needed
                    try:
                        if isinstance(value, str):
                            # Try to parse date from string format
                            value = datetime.datetime.strptime(value, '%Y-%m-%d').date()
                    except ValueError:
                        # Keep original value if parsing fails
                        pass
                    
                # Add more type conversions as needed based on schema
                # Example for numeric fields
                elif col in ('Year', 'Quarter', 'Month', 'DayofMonth', 
                           'DOT_ID_Reporting_Airline', 'OriginAirportID', 
                           'DestAirportID', 'CRSDepTime', 'DepTime', 
                           'DepDelay', 'ArrDelay', 'Cancelled'):
                    try:
                        # Convert numeric fields to appropriate types
                        if value == '':
                            value = 0  # Default value for empty numeric fields
                        elif col == 'Year':
                            value = int(value)
                        elif col in ('Quarter', 'Month', 'DayofMonth', 'Cancelled'):
                            value = int(value)
                        elif col in ('DepDelay', 'ArrDelay'):
                            value = int(float(value))
                        else:
                            value = int(value)
                    except (ValueError, TypeError):
                        # Use default value if conversion fails
                        value = 0
                
                processed_row.append(value)
            
            processed_data.append(processed_row)
        
        if progress_callback:
            progress_callback(75, 100, "inserting_data")
        
        # Insert data in batches to show progress
        batch_size = 1000
        total_batches = (len(processed_data) + batch_size - 1) // batch_size
        
        for i in range(0, len(processed_data), batch_size):
            batch = processed_data[i:i+batch_size]
            client.insert_data(config['table'], config['columns'], batch)
            
            batch_num = i // batch_size + 1
            progress = 75 + (20 * batch_num / total_batches)
            
            if progress_callback:
                progress_callback(int(progress), 100, f"inserting_batch_{batch_num}_of_{total_batches}")
        
        if progress_callback:
            progress_callback(95, 100, "finalizing")
            time.sleep(0.5)  # Small delay to show progress completion
        
        return len(processed_data)
    except Exception as e:
        if progress_callback:
            progress_callback(0, 100, "error")
        raise Exception(f"Error in file to ClickHouse ingestion: {str(e)}")