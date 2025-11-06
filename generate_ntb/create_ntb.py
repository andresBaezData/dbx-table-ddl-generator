import pandas as pd
import re
from .utils import drop_comments, get_from
from typing import Dict, List

class GenerateNotebook:
    def __init__(self, derived_tables, alias_tables, original_tables):
        self.derived_tables = derived_tables
        self.alias_tables = alias_tables
        self.original_tables = original_tables

    def _notebook_content(self):
        """
        Create a dictionary with the notebook content.
        """
        self.notebook = {
            "cells": [],
            "metadata": {},
            "nbformat": 4,
            "nbformat_minor": 5
        }
    
    def _create_cell(self, type: str, execution_count= None, metadata: Dict = {}, output: List = [], source: List = []):
        """
        Create all kind of cells within a jupyter notebook.
        """
        cell = {
            "cell_type": type,
            "execution_count": execution_count,
            "metadata": metadata,
            "outputs": output,
            "source": source
        }
        self.notebook['cells'].append(cell)

    def _import_libraries(self):
        """
        Create the first cell of the notebook, with the necessary libraries used in the notebook.
        """
        source = [
            '%python\n',
            'from pyspark.sql import Row\n',
            'from pyspark.sql.types import StructType, StructField, StringType, BooleanType\n',
            'schema = StruckType([\n',
            "StructField('object_name', StringType(), True),\n",
            "StructField('universe', StringType(), True),\n",
            "StructField('type', StringType(), True),\n",
            "StructField('ddl_content', StringType(), True),\n",
            "StructField('ddl_path', StringType(), True),\n",
            "StructField('parent_view', StringType(), True),\n",
            "StructField('schema', StringType(), True),\n",
            "StructField('ddl_databricks', StringType(), True),\n",
            "StructField('ddl_databricks_transformed', StringType(), True),\n",
            "StructField('is_simple_select', BooleanType(), True),\n",
            "StructField('run', BooleanType(), True),\n",
            '])'
        ]
        self._create_cell(type= "code", source= source)

    def _insert_cell(self, object_name: str, universe_name: str, type: str):
        """
        """
        source = [
            f'ddl = spark.sql(f"SHOW CREATE TABLE {{out_catalog}}.{{out_schema}}.{object_name}").first()[0]\n',
            'row = Row(\n',
            f'object_name= "{object_name}", universe= "{universe_name}", type= "{type},"\n',
            'ddl_content = None, ddl_path= None, parent_view= None, ddl_databricks= None,\n'
            f'schema= "{universe_name}", ddl_databricks_transformed= ddl, is_simple_select= False, run= True\n',
            ')\n',
            'spark.createDataFrane([row], schema= schema).write.mode("append").insertInto(f"{{out_catalog}}.{{out_schema}}.universe_definitions")'
        ]

    def generate_notebook(self):
        """
        """
        queries = pd.concat([self.derived_tables, self.alias_tables, self.original_tables])
        
        # Add to the source of each script a tb_ in the table_name 
        pattern = r"(FROM\s+\{.*?\}\.\{.*?\}\.)(\w+)"
        queries['SQL Script'] = queries['SQL Script'].str.replace(pattern, r"\1tb_\2", regex=True)

        # Notebook content
        self._notebook_content()
        
        # Run variables
        self._create_cell(type= "code", source= ["%run ./00variables"])
        # Python cell with imports
        self._import_libraries()
        
        for _, row in queries.iterrows():
            table_name = row['Table_name'].lower()
            sql_script = row['SQL Script']
            universe_name = row['Universe Name']
            object_type = row['Type']

            # Markdown cells per object created in Databricks
            self._create_cell(type= "markdown", source= [f"### View: {table_name}"])

            # Spark ddl sql
            spark_sql = f'spark.sql(f"""\n{sql_script}\n""")'
            self._create_cell(type= "code", source= [spark_sql])

            # INSERT INTO or UPDATE / MERGE in universe_definitions

