from clickhouse_driver import Client
import re

class ClickHouseClient:
    def __init__(self, host, port, database, user, jwt_token=None):
        """
        Initialize a connection to ClickHouse server
        
        Args:
            host (str): ClickHouse server hostname or IP
            port (int): ClickHouse server port
            database (str): Database name
            user (str): Username for authentication
            jwt_token (str): JWT token or password for authentication
        """
        try:
            self.client = Client(
                host=host,
                port=port,
                database=database,
                user=user,
                password=jwt_token  # Using password field for both JWT token or regular password
            )
            # Test connection
            self.client.execute("SELECT 1")
            
            # Store connection details for reuse
            self.connection_details = {
                'host': host,
                'port': port,
                'database': database,
                'user': user,
                'jwt_token': jwt_token
            }
        except Exception as e:
            raise ConnectionError(f"Failed to connect to ClickHouse: {str(e)}")

    def get_tables(self):
        """
        Get list of tables in the current database
        
        Returns:
            list: List of table names
        """
        try:
            return [row[0] for row in self.client.execute("SHOW TABLES")]
        except Exception as e:
            raise Exception(f"Failed to get tables: {str(e)}")

    def get_columns(self, table):
        """
        Get columns of a specific table
        
        Args:
            table (str): Table name
            
        Returns:
            list: List of column names
        """
        # Sanitize table name to prevent SQL injection
        if not re.match(r'^[a-zA-Z0-9_]+$', table):
            raise ValueError("Invalid table name. Only alphanumeric characters and underscores are allowed.")
            
        try:
            return [row[0] for row in self.client.execute(f"DESCRIBE TABLE {table}")]
        except Exception as e:
            raise Exception(f"Failed to get columns for table '{table}': {str(e)}")

    def count_rows(self, table):
        """
        Count the number of rows in a table
        
        Args:
            table (str): Table name
            
        Returns:
            int: Number of rows in the table
        """
        # Sanitize table name to prevent SQL injection
        if not re.match(r'^[a-zA-Z0-9_]+$', table):
            raise ValueError("Invalid table name. Only alphanumeric characters and underscores are allowed.")
            
        try:
            result = self.client.execute(f"SELECT count() FROM {table}")
            return result[0][0]
        except Exception as e:
            raise Exception(f"Failed to count rows in table '{table}': {str(e)}")

    def fetch_data(self, table, columns):
        """
        Fetch data from a table with specified columns
        
        Args:
            table (str): Table name
            columns (list): List of column names
            
        Returns:
            list: List of rows with data
        """
        # Sanitize table name and column names
        if not re.match(r'^[a-zA-Z0-9_]+$', table):
            raise ValueError("Invalid table name. Only alphanumeric characters and underscores are allowed.")
            
        for col in columns:
            if not re.match(r'^[a-zA-Z0-9_]+$', col):
                raise ValueError(f"Invalid column name: '{col}'. Only alphanumeric characters and underscores are allowed.")
        
        try:
            query = f"SELECT {', '.join(columns)} FROM {table}"
            return self.client.execute(query)
        except Exception as e:
            raise Exception(f"Failed to fetch data from table '{table}': {str(e)}")

    def insert_data(self, table, columns, data):
        """
        Insert data into a table
        
        Args:
            table (str): Table name
            columns (list): List of column names
            data (list): List of rows to insert
            
        Returns:
            None
        """
        # Sanitize table name and column names
        if not re.match(r'^[a-zA-Z0-9_]+$', table):
            raise ValueError("Invalid table name. Only alphanumeric characters and underscores are allowed.")
            
        for col in columns:
            if not re.match(r'^[a-zA-Z0-9_]+$', col):
                raise ValueError(f"Invalid column name: '{col}'. Only alphanumeric characters and underscores are allowed.")
        
        if not data:
            raise ValueError("No data to insert")
            
        try:
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES"
            self.client.execute(query, data)
        except Exception as e:
            raise Exception(f"Failed to insert data into table '{table}': {str(e)}")

    def preview_data(self, table, columns):
        """
        Preview data from a table with specified columns (limited to 100 rows)
        
        Args:
            table (str): Table name
            columns (list): List of column names
            
        Returns:
            dict: Dictionary with columns and rows
        """
        # Sanitize table name and column names
        if not re.match(r'^[a-zA-Z0-9_]+$', table):
            raise ValueError("Invalid table name. Only alphanumeric characters and underscores are allowed.")
            
        for col in columns:
            if not re.match(r'^[a-zA-Z0-9_]+$', col):
                raise ValueError(f"Invalid column name: '{col}'. Only alphanumeric characters and underscores are allowed.")
        
        try:
            query = f"SELECT {', '.join(columns)} FROM {table} LIMIT 100"
            result = self.client.execute(query)
            return {"columns": columns, "rows": result}
        except Exception as e:
            raise Exception(f"Failed to preview data from table '{table}': {str(e)}")