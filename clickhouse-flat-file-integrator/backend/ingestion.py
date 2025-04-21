from clickhouse_client import ClickHouseClient
from file_handler import FileHandler
import os
import time
import datetime
import re

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
        
        # Identify which table schema to use for type conversion
        table_name = config['table'].lower()
        
        # Process data types according to the appropriate ClickHouse schema
        processed_data = []
        for row in data:
            processed_row = []
            
            # Choose the appropriate data type processor based on the table
            if "ontime" in table_name:
                processed_row = process_ontime_row(row, config['columns'])
            elif "uk_price_paid" in table_name:
                processed_row = process_uk_price_paid_row(row, config['columns'])
            else:
                # Generic processing for unknown tables
                processed_row = process_generic_row(row, config['columns'])
            
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

def process_ontime_row(row, columns):
    """
    Process a row of data for the ontime table schema
    
    Args:
        row (list): The row data
        columns (list): The column names
    
    Returns:
        list: The processed row with correct data types
    """
    processed_row = []
    for i, col in enumerate(columns):
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
            
        # Handle numeric fields for ontime table
        elif col in ('Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek',
                   'DOT_ID_Reporting_Airline', 'OriginAirportID', 'OriginAirportSeqID', 
                   'OriginCityMarketID', 'OriginWac', 'DestAirportID', 'DestAirportSeqID',
                   'DestCityMarketID', 'DestWac', 'CRSDepTime', 'DepTime', 
                   'DepDelay', 'DepDelayMinutes', 'DepDel15', 'TaxiIn', 'TaxiOut',
                   'CRSArrTime', 'ArrTime', 'ArrDelay', 'ArrDelayMinutes', 'ArrDel15',
                   'Cancelled', 'Diverted', 'CRSElapsedTime', 'ActualElapsedTime',
                   'AirTime', 'Flights', 'Distance', 'DistanceGroup'):
            try:
                # Convert numeric fields to appropriate types
                if value == '':
                    value = 0  # Default value for empty numeric fields
                elif col in ('Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek'):
                    value = int(value)
                elif col in ('DepDelay', 'ArrDelay', 'Cancelled', 'Diverted'):
                    value = int(float(value))
                else:
                    value = int(value)
            except (ValueError, TypeError):
                # Use default value if conversion fails
                value = 0
        
        processed_row.append(value)
    
    return processed_row

def process_uk_price_paid_row(row, columns):
    """
    Process a row of data for the uk_price_paid table schema
    
    Args:
        row (list): The row data
        columns (list): The column names
    
    Returns:
        list: The processed row with correct data types
    """
    processed_row = []
    for i, col in enumerate(columns):
        value = row[i]
        
        # Handle specific data types for uk_price_paid table
        if col == 'price':
            try:
                value = int(float(value)) if value else 0
            except (ValueError, TypeError):
                value = 0
                
        elif col == 'date':
            try:
                if isinstance(value, str):
                    # Try to parse date from string format (supports multiple formats)
                    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']:
                        try:
                            value = datetime.datetime.strptime(value, fmt).date()
                            break
                        except ValueError:
                            continue
            except Exception:
                # Keep original value if parsing fails
                pass
                
        elif col == 'type':
            # Map text values to enum numbers
            type_map = {
                'terraced': 1,
                'semi-detached': 2, 
                'detached': 3, 
                'flat': 4,
                'other': 0
            }
            value = type_map.get(value.lower() if isinstance(value, str) else '', 0)
                
        elif col == 'is_new':
            try:
                # Convert to boolean integer (0 or 1)
                if isinstance(value, str):
                    if value.lower() in ('yes', 'true', '1', 'y'):
                        value = 1
                    elif value.lower() in ('no', 'false', '0', 'n'):
                        value = 0
                    else:
                        value = int(bool(value))
                else:
                    value = int(bool(value))
            except (ValueError, TypeError):
                value = 0
                
        elif col == 'duration':
            # Map text values to enum numbers
            duration_map = {
                'freehold': 1,
                'leasehold': 2,
                'unknown': 0
            }
            value = duration_map.get(value.lower() if isinstance(value, str) else '', 0)
                
        elif col in ('postcode1', 'postcode2'):
            # Process postcodes to ensure they're properly formatted
            if isinstance(value, str):
                # Convert to uppercase and remove spaces
                value = value.upper().strip()
            else:
                value = str(value)
        
        processed_row.append(value)
    
    return processed_row

def process_generic_row(row, columns):
    """
    Generic processing for unknown table schemas
    
    Args:
        row (list): The row data
        columns (list): The column names
    
    Returns:
        list: The processed row with basic type inference
    """
    processed_row = []
    for i, col in enumerate(columns):
        value = row[i]
        
        # Basic type inference
        if col.lower().endswith('date'):
            # Try to convert any date-like column
            try:
                if isinstance(value, str) and value:
                    for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']:
                        try:
                            value = datetime.datetime.strptime(value, fmt).date()
                            break
                        except ValueError:
                            continue
            except Exception:
                pass
        
        # Attempt to convert numeric-looking columns
        elif re.match(r'^(id|count|num|amount|price|total|sum|qty|quantity).*$', col.lower()):
            try:
                if value == '':
                    value = 0
                else:
                    value = int(float(value))
            except (ValueError, TypeError):
                pass
        
        processed_row.append(value)
    
    return processed_row