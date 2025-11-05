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

    def generate_notebook(self):
        """
        """
        queries = pd.concat([self.derived_tables, self.alias_tables, self.original_tables])
        self._notebook_content()
        self._create_cell(type= "code", source= ["%python"])

        for _, row in queries.iterrows():
            table_name = row['Table_name']
            sql_script = row['SQL Script']

            # Markdown cells per object created in Databricks
            self._create_cell(type= "markdown", source= [f"### View: {table_name}"])

            # Spark ddl sql
            spark_sql = f'spark.sql(f"""\n{sql_script}\n""")'
            self._create_cell(type= "code", source= [spark_sql])

            # INSERT INTO in universe_definitions

