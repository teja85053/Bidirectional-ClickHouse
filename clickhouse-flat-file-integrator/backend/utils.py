import os
import re
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("clickhouse_tool.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("clickhouse_tool")

def validate_filepath(filepath):
    """
    Validate file path for security
    
    Args:
        filepath (str): Path to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    # Check for directory traversal attempts
    normalized_path = os.path.normpath(filepath)
    if ".." in normalized_path:
        logger.warning(f"Potential directory traversal attempt detected: {filepath}")
        return False
    
    # Check if it's within the allowed directories
    allowed_prefixes = ["./data/", "data/", "../data/"]
    if not any(normalized_path.startswith(prefix) for prefix in allowed_prefixes):
        if not normalized_path.startswith("/"):  # Allow absolute paths for flexibility
            logger.warning(f"File path outside of allowed directories: {filepath}")
            return False
    
    return True

def sanitize_filename(filename):
    """
    Sanitize filename to prevent directory traversal and other issues
    
    Args:
        filename (str): Filename to sanitize
        
    Returns:
        str: Sanitized filename
    """
    # Remove potentially dangerous characters
    filename = re.sub(r'[\\/*?:"<>|]', '', filename)
    
    # Prevent directory traversal
    filename = os.path.basename(filename)
    
    return filename

def get_timestamp():
    """
    Get current timestamp in a file-friendly format
    
    Returns:
        str: Timestamp string
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def generate_export_filename(table_name):
    """
    Generate a filename for exports based on table name and timestamp
    
    Args:
        table_name (str): Name of the table being exported
        
    Returns:
        str: Generated filename
    """
    sanitized_table = sanitize_filename(table_name)
    timestamp = get_timestamp()
    return f"data/export_{sanitized_table}_{timestamp}.csv"

def validate_table_name(table_name):
    """
    Validate ClickHouse table name to prevent SQL injection
    
    Args:
        table_name (str): Table name to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    pattern = r'^[a-zA-Z0-9_]+$'
    return bool(re.match(pattern, table_name))

def validate_column_name(column_name):
    """
    Validate ClickHouse column name to prevent SQL injection
    
    Args:
        column_name (str): Column name to validate
        
    Returns:
        bool: True if valid, False otherwise
    """
    pattern = r'^[a-zA-Z0-9_]+$'
    return bool(re.match(pattern, column_name))