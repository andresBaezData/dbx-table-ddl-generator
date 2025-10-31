import pandas as pd
import re
from .utils import drop_comments, get_from
from typing import Set

class GenerateNotebook:
    def __init__(self, derived_tables, alias_tables, original_tables):
        self.derived_tables = derived_tables
        self.alias_tables = alias_tables
        self.original_tables = original_tables
        self.ntb_tables: pd.DataFrame = None
        self.created_objects: Set = set()

    def _is_case(self, sql: str) -> int:
        """
        """
        if "case when" not in sql.lower():
            return 0
        
        # Drop comments with /* and --
        no_comments = drop_comments(sql)

        return 1 if re.search(r"\bCASE\s+WHEN\b", no_comments, flags=re.IGNORECASE) else 0
    
    def _cast_info(self, ddl: pd.DataFrame):
        """
        Get CAST information from each table to create an ALTER CELL in Databricks notebook.
        """
        if not ddl.empty:
            cast_info = {}
            for _, row in ddl.iterrows():
                no_comments = drop_comments(row['SQL Script'])
                cast_set = set()
                cast_set.update(re.findall(r'cast\(.*?\)(?=\s+AS)', string= no_comments, flags= re.IGNORECASE))
                if not cast_set:
                    continue
                from_cast = get_from(no_comments)
                cast_info[from_cast] = cast_set
            
            return cast_info
        else:
            return None
    
    def _concat_info(self, ddl: pd.DataFrame):
        """
        Get || information from each table to create an ALTER CELL in Databricks notebook.
        """
        if not ddl.empty:
            concat_info = {}
            for _, row in ddl.iterrows():
                no_comments = drop_comments(row['SQL Script'])
                concat_set = set()
                concat_set.update(re.findall(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+\s*\|\|\s*'.*?'\s*\|\|\s*[A-Za-z0-9_]+\.[A-Za-z0-9_]+", string= no_comments))
                if not concat_set:
                    continue
                from_concat = get_from(no_comments)
                concat_info[from_concat] = concat_set
            
            return concat_info
        else:
            return None
        
    def _generate_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create flags for each SQL function identified.
        """
        # Cast filter
        df['is_cast'] = df['SQL Script'].str.contains(r'cast\(', case= False, regex= True).astype('int')
        # Case filter
        df['is_case'] = df['SQL Script'].apply(self._is_case)
        # Concat filter: Using ||
        df['is_concat'] = df['SQL Script'].str.contains(r'\|', regex= True).astype('int')
        # Concat filter: Using concat_ws
        df['is_concat_ws'] = df['SQL Script'].str.contains(r'concat_ws', regex= True).astype('int')
        # Trim filter
        df['is_trim'] = df['SQL Script'].str.contains(r'trim', regex= True).astype('int')

        return df
        
    def generate_notebook(self):
        """
        """
        filtered_alias = self._generate_flags(self.alias_tables)
        filtered_original = self._generate_flags(self.original_tables)

        for filter in [filtered_alias, filtered_original]:
            create_object = set()
            # Cast case: We are not creating this objects, instead we are going to CAST directly in the FROM object
            is_cast = filter[filter['is_cast'] == 1]
            cast_info = self._cast_info(is_cast)
            
            # Case filter: We are going to create this objects
            is_case = filter[filter['is_case'] == 1]
            if not is_case.empty:
                create_object.update((is_case['Table_name']).to_list())
            
            # Case concat using ||
            is_concat = filter[filter['is_concat'] == 1]
            concat_info = self._concat_info(is_concat)


        

