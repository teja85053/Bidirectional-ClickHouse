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

    def read_data(self, selected_columns, progress_callback=None):
        """
        Read data from the CSV file for selected columns
        
        Args:
            selected_columns (list): List of column names to read
            progress_callback (function): Callback function to report progress
            
        Returns:
            list: List of rows with data for selected columns
        """
        try:
            # First pass to count rows for progress reporting
            with open(self.filepath, newline='', encoding='utf-8') as f:
                row_count = sum(1 for _ in f) - 1  # Subtract header row
            
            if progress_callback:
                progress_callback(15, 100, "counting_rows")
            
            with open(self.filepath, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=self.delimiter)
                data = []
                rows_processed = 0
                
                for row in reader:
                    try:
                        data.append([row[col] for col in selected_columns])
                    except KeyError:
                        # Skip rows with missing columns
                        continue
                    
                    rows_processed += 1
                    if progress_callback and row_count > 0 and rows_processed % (max(1, row_count // 100)) == 0:
                        progress_pct = min(59, 15 + (rows_processed / row_count) * 45)
                        progress_callback(int(progress_pct), 100, "reading_data")
                
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