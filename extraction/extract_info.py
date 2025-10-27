import pandas as pd
from typing import Dict

class Extraction():
    def __init__(self, path_file):
        self.input_path = path_file
        self.file_columns = ["Table Details", "Joins", "Object Details"]
        self.excel_file = pd.ExcelFile(self.input_path)
        self.excel_info: Dict[str, pd.DataFrame] = {}
        self._read_file()
        print(self.excel_info)
    
    def _read_file(self):
        """
        """
        for col in self.file_columns:
            df = pd.read_excel(self.excel_file, sheet_name= col, engine="openpyxl", header= 1)
            self.excel_info[col] = df