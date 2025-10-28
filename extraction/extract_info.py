import pandas as pd
from collections import defaultdict
from typing import Dict, List

class Extraction():
    def __init__(self, path_file):
        self.input_path = path_file
        self.file_columns = ["Table Details", "Joins", "Object Details"]
        self.excel_file = pd.ExcelFile(self.input_path)
        self.excel_info: Dict[str, pd.DataFrame] = {}
        self.table_details: pd.DataFrame = None
        self.joins: pd.DataFrame = None
        self.objects_detials: pd.DataFrame = None
        self._read_file()
    
    def _read_file(self):
        """
        Read from the same excel file, the tabs: Table Details, Joins and Object Details and save each one in excel_info
        """
        for col in self.file_columns:
            df = pd.read_excel(self.excel_file, sheet_name= col, engine="openpyxl", header= 1)
            self.excel_info[col] = df
    
    def _build_sql(self, table_fields: List, table_name: str):
        """
        """
        fields = table_fields[table_name]
        if len(fields) > 1:
            prefixed_fields = [f"{table_name}.{field}" for field in fields]
            sql = f"concat_ws('-', {', '.join(prefixed_fields)})"
        elif len(fields) == 1:
            sql = f"{table_name}.{fields[0]}"

        return sql

    def _join_expressions(self):
        """
        """
        final_relationships = []
        for expression in self.joins["Join Expression"]:
            fields_by_table = defaultdict(list)
            conditions = expression.upper().split(' AND ') if isinstance(expression, str) else []

            for condition in conditions:
                # Split each row by using '='
                parts = [part.strip().strip('"') for part in condition.split('=', 1)]

                # We are only admit 2 parts
                if len(parts) != 2:
                    continue
                left, right = parts

                # We only admit parts which contains '.'
                if '.' not in left or '.' not in right:
                    continue

                left_arr = left.split('.')
                right_arr = right.split('.')
                
                fields_by_table[left_arr[-2]].append(left_arr[-1])
                fields_by_table[right_arr[-2]].append(right_arr[-1])

            if len(fields_by_table) == 2:
                table_a_name, table_b_name = list(fields_by_table.keys())
                
                # Generating SQL for both tables
                sql_a = self._build_sql(fields_by_table, table_a_name)
                sql_b = self._build_sql(fields_by_table, table_b_name)

                #agregamos las relaciones a la lista final
                if sql_a and sql_b:
                    # direccion 1: A -> B
                    final_relationships.append({
                        'originTable': table_a_name,
                        'endTable': table_b_name,
                        'sql': sql_a
                    })
                    # direccion 2: B -> A
                    final_relationships.append({
                        'originTable': table_b_name,
                        'endTable': table_a_name,
                        'sql': sql_b
                    })

        return pd.DataFrame(final_relationships)
    
    def get_info(self):
        """
        """
        # Saving excel information
        self._read_file()
        self.table_details = self.excel_info['Table Details']
        self.joins = self.excel_info['Joins']
        self.objects_details = self.excel_info['Object Details']

        objects_details_filtered = self.objects_details[(self.objects_details['Obj Select'].notnull()) & (self.objects_details['Obj Where'].isnull())].copy()
        filter_details = self.objects_details[(self.objects_details['Obj Where'].notna())].copy()

        foreignKeys = self._join_expressions()