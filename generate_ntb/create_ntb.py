import pandas as pd
import re
from .utils import drop_comments

class GenerateNotebook:
    def __init__(self, derived_tables, alias_tables, original_tables):
        self.derived_tables = derived_tables
        self.alias_tables = alias_tables
        self.original_tables = original_tables
        self.ntb_tables: pd.DataFrame = None

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
        Get CAST information from each table to create an ALTER CELL in Databricks.
        """
        if not ddl.empty:
            cast_info = {}
            for _, row in ddl.iterrows():
                no_comments = drop_comments(row['SQL Script'])
                cast_set = set()
                cast_set.update(re.findall(r'cast\(.*?\)(?=\s+AS)', string= no_comments, flags= re.IGNORECASE))
                if not cast_set:
                    continue
                cast_info[row['Table_name']] = cast_set
        
        return cast_info
        
    def _generate_flags(self, df: pd.DataFrame) -> pd.DataFrame:
        """
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
        # Where filter
        df['is_where'] = df['SQL Script'].str.contains(r'where', case= False, regex= True).astype('int')
        # Join filter
        df['is_join'] = df['SQL Script'].str.contains(r'join', case= False, regex= True).astype('int')
        return df
        
    def generate_notebook(self):
        """
        """
        filtered_alias = self._generate_flags(self.alias_tables)
        filtered_original = self._generate_flags(self.original_tables)

        # Cast case
        is_cast = filtered_original[filtered_original['is_cast'] == 1]
        cast_info = self._cast_info(is_cast)
        

