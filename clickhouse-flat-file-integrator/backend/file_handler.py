import csv
import os

class FileHandler:
    def __init__(self, filepath, delimiter):
        """
        Initialize file handler for CSV operations
        
        Args:
            filepath (str): Path to the CSV file
            delimiter (str): CSV delimiter character
        """
        self.filepath = filepath
        self.delimiter = delimiter or ','  # Default to comma if not provided

    def get_columns(self):
        """
        Get column names from the CSV file
        
        Returns:
            list: List of column names
        """
        try:
            with open(self.filepath, newline='', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=self.delimiter)
                return next(reader)
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {self.filepath}")
        except Exception as e:
            raise Exception(f"Error reading file: {str(e)}")

    def read_data(self, selected_columns):
        """
        Read data from the CSV file for selected columns
        
        Args:
            selected_columns (list): List of column names to read
            
        Returns:
            list: List of rows with data for selected columns
        """
        try:
            with open(self.filepath, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                data = []
                for row in reader:
                    try:
                        data.append([row[col] for col in selected_columns])
                    except KeyError as e:
                        # Skip rows with missing columns or continue with None values
                        # data.append([row.get(col, None) for col in selected_columns])
                        continue
                return data
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {self.filepath}")
        except Exception as e:
            raise Exception(f"Error reading data: {str(e)}")

    def write_data(self, columns, data):
        """
        Write data to CSV file
        
        Args:
            columns (list): List of column names
            data (list): List of rows to write
            
        Returns:
            None
        """
        # Ensure directory exists
        directory = os.path.dirname(self.filepath)
        if directory:
            os.makedirs(directory, exist_ok=True)
        
        try:
            with open(self.filepath, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter=self.delimiter)
                writer.writerow(columns)
                writer.writerows(data)
        except PermissionError:
            raise PermissionError(f"Permission denied writing to file: {self.filepath}")
        except Exception as e:
            raise Exception(f"Error writing data: {str(e)}")

    def preview_data(self, selected_columns):
        """
        Preview data from the CSV file for selected columns (limited to 100 rows)
        
        Args:
            selected_columns (list): List of column names to preview
            
        Returns:
            dict: Dictionary with columns and rows
        """
        try:
            rows = self.read_data(selected_columns)[:100]
            return {"columns": selected_columns, "rows": rows}
        except Exception as e:
            raise Exception(f"Error previewing data: {str(e)}")