import pandas as pd
from typing import Dict

class Extraction():
    def __init__(self, path_file):
        self.input_path = path_file
        self.file_columns = ["Table Details", "Joins", "Object Details"]
        self.excel_file = pd.ExcelFile(self.input_path)
        self.excel_info: Dict[str, pd.DataFrame] = {}
        self._read_file()
    
    def _read_file(self):
        """
        Read from the same excel file, the tabs: Table Details, Joins and Object Details and save each one in excel_info
        """
        for col in self.file_columns:
            df = pd.read_excel(self.excel_file, sheet_name= col, engine="openpyxl", header= 1)
            self.excel_info[col] = df
    
    def get_info(self):
        """
        """
        self._read_file()
        objects_details = self.excel_info['Object Details']
        objects_details_filtered = objects_details[(objects_details['Obj Select'].notnull()) & (objects_details['Obj Where'].isnull())].copy()
        filter_details = objects_details[(objects_details['Obj Where'].notna())].copy()