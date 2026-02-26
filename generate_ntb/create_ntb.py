import pandas as pd
import json
import logging
from typing import Dict, List
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, substr, regexp_extract, regexp_replace
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

spark = SparkSession.builder.appName("GenerateNotebook").getOrCreate()

class GenerateNotebook:
    def __init__(self, tables, path):
        self.all_tables = spark.createDataFrame(tables, schema=StructType()) if tables.empty else spark.createDataFrame(tables)
        self.ntb_path = path

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

    def _import_libraries(self, out_schema: str):
        """
        Create the first cell of the notebook, with the necessary libraries used in the notebook.
        """
        source = [
            '%python\n',
            'import re\n',
            'from delta.tables import DeltaTable\n',
            'from pyspark.sql import Row, DataFrame\n',
            'from pyspark.sql.functions import col, lit, when\n',
            'from pyspark.sql.types import BooleanType\n',
            'from typing import Dict\n',
            '\n',
            'def get_sources(query: str, object_variables: Dict) -> DataFrame:\n',
            '    """\n',
            '    Extract the sources from a a SQL DDL query and returns a dataframe with that information.\n',
            '    """\n',
            '    # Capture catalog.schema.table\n',
            '    query_lower = query.lower()\n',
            "    pattern = re.compile(r'\\b(?:from|join)\\s+(?:[a-zA-Z_][\\w]*\\.)?([a-zA-Z_][\\w]*)\\.([a-zA-Z_][\\w]*)', re.IGNORECASE)\n",
            '    sources = pattern.findall(query_lower)\n',
            '\n',
            "    # Remove elements from source which catalog = ''\n",
            "    sources = [(schema, table) for catalog, schema, table in sources if catalog != '']\n",
            '\n',
            '    # Get the target table / view\n',
            "    dbx_pattern = re.compile(r'\\b(?:replace table|replace view)\\s+(?:[a-zA-Z_][\\w]*\\.)?[a-zA-Z_][\\w]*\\.([a-zA-Z_][\\w]*)')\n",
            '    dbx_name = re.search(dbx_pattern, query_lower).group(1)\n',
            '\n',
            '    # Keep sources unique\n',
            '    sources = set(sources)\n',
            '\n',
            '    rows = []\n',
            '    for schema, table in sources:\n',
            "        type_df = universe_df.filter((col('databricks_name') == table) & (col('universe') == object_variables['object_universe'])).select('type')\n",
            '        if type_df.isEmpty(): raise ValueError(f"Table {schema}.{table} not found in universe definitions")\n',
            '        parent_type = type_df.first()["type"]\n',
            '\n',        
            '        row = Row(\n',
            "            object_name= object_variables['object_name'], universe= object_variables['object_universe'],\n",
            "            type= object_variables['object_type'], schema= object_variables['object_schema'],\n",
            '            parent_databricks_name = table, parent_schema = schema, parent_type = parent_type,\n',
            '            ddl_databricks_transformed= query\n',
            '        )\n',
            '\n',
            '        rows.append(row)\n',
            '\n',
            '    object_df = spark.createDataFrame(rows)\n',
            '    object_df = object_df.withColumn("databricks_name", lit(dbx_name))\n',
            '    object_df = object_df.withColumn("is_simple_select", lit(False).cast(BooleanType()))\n',
            '    object_df = object_df.withColumn("run", lit(True).cast(BooleanType()))\n',
            '    return object_df\n',
            '\n',
            '# Variables\n',
            'in_catalog = "dev_teradata_migration"\n',
            'out_catalog = "dev_teradata_migration"\n',
            f'out_schema = "{out_schema}"\n',
            '\n',
            "universe_df = spark.table('dev_teradata_migration.report_test_data.universe_definitions')\n",
            "universe_delta = DeltaTable.forName(spark,'dev_teradata_migration.report_test_data.universe_definitions')"
        ]
        self._create_cell(type= "code", source= source)

    def _update_schema_based_on_metadata_table(self, tables):
        """
        Update the schema of the tables based on the metadata table universe_definitions to repoint to general table.
        """
        # Only derived tables
        derived_tables = tables.filter(col('internal_type') == 'derived')
        # Only alias and original views
        non_derived_tables = tables.filter((col('internal_type').isin('alias', 'original')))
        
        try:
            catalog = 'dev_teradata_migration'
            schema = 'report_test_data'
            metadata_table = 'universe_definitions'
            universe_raw = non_derived_tables.first()['universe_name']
            universe = universe_raw.replace(' ', '_')
            # Read metadata table to get the object and schema names of the new source tables
            sql_query = f" WITH simple_views AS ( \
                SELECT object_name, schema\
                FROM {catalog}.{schema}.{metadata_table}\
                WHERE universe = '{universe}'\
                AND is_simple_select = 'true'\
                AND type='view'\
            )\
            SELECT `tb`.`object_name`, `tb`.`schema` AS new_schema, `vw`.`schema` AS old_schema\
            FROM {catalog}.{schema}.{metadata_table} AS tb\
            RIGHT JOIN simple_views AS vw\
                ON tb.object_name = vw.object_name\
            WHERE universe = '{universe}' \
            AND type = 'table'\
            AND `tb`.`schema` IS NOT NULL"
            
            # Save query results of new source tables in dataframe
            new_source_tables = spark.sql(sql_query)
            # Extract the old target table names for all the non derived tables
            pattern_table_name = r"FROM\s+\{[^{}]+\}\.\w+\.(\w+)"
            pattern_schema_name = r"FROM\s+\{[^{}]+\}\.(\w+)\.\w+"
            non_derived_tables = non_derived_tables.withColumn("view_name",  regexp_extract(col("sql_script"), pattern_table_name, 1))
            non_derived_tables = non_derived_tables.withColumn("source_schema", regexp_extract(col("sql_script"), pattern_schema_name, 1))

            columns = non_derived_tables.columns

            # Merge and update the schema of the non derived tables
            updated_df = non_derived_tables.join(
                new_source_tables,
                (non_derived_tables['view_name'] == new_source_tables['object_name']) &
                (non_derived_tables['source_schema'] == new_source_tables['old_schema']),
                'left'
            )

            updated_df = updated_df.withColumn(
                'sql_script',
                when(
                    col('new_schema').isNotNull(),
                    regexp_replace(col('sql_script'), col('old_schema'), col('new_schema'))
                ).otherwise(col('sql_script'))
            )

            columns = updated_df.columns

            columns_to_drop = ['view_name', 'object_name', 'source_schema', 'old_schema', 'new_schema']
            updated_df = updated_df.drop(*columns_to_drop)
            
            return derived_tables.unionByName(updated_df)
        except:
            return tables

    def _merge_cell(self, table_name: str, type: str):
        """
        Create a cell with the necessary information to merge a dataframe with universe_definitions table.
        """
        source = [
            'query = f"""\n',
            '\n',
            '"""\n',
            'object_info = {\n',
            f"    'object_name': '{table_name}',\n",
            f"    'object_type': '{type}',\n",
            f"    'object_universe': f'{{out_schema}}',\n",
            f"    'object_schema': f'{{out_schema}}'\n",
            '}\n',
            'conditions = """\n',
            '   t.object_name = s.object_name AND\n',
            '   t.type = s.type AND\n',
            '   t.universe = s.universe AND\n',
            '   t.schema = s.schema AND\n',
            '   t.parent_databricks_name = s.parent_databricks_name AND\n',
            '   t.parent_schema = s.parent_schema AND\n',
            '   t.parent_type = s.parent_type\n',
            '"""\n',
            'source_df = get_sources(query, object_info)\n',
            '\n',
            '# MERGE INTO\n',
            '(\n',
            '  universe_delta.alias("t")\n',
            '  .merge(\n',
            '      source_df.alias("s"),\n',
            '      condition= conditions\n',
            '  )\n',
            '  .whenMatchedUpdate(\n',
            '      set= {\n',
            '      "ddl_databricks_transformed": "s.ddl_databricks_transformed",\n',
            '      "databricks_name": "s.databricks_name",\n',
            '      "is_simple_select": "s.is_simple_select",\n',
            '      "run": "s.run",\n',
            '      }\n',
            '  )\n',
            '  .whenNotMatchedInsert(\n',
            '      values= {\n',
            '       "object_name": "s.object_name",\n',
            '       "type": "s.type",\n',
            '       "universe": "s.universe",\n',
            '       "schema": "s.schema",\n',
            '       "parent_databricks_name": "s.parent_databricks_name",\n',
            '       "parent_schema": "s.parent_schema",\n',
            '       "parent_type": "s.parent_type",\n',
            '       "ddl_databricks_transformed": "s.ddl_databricks_transformed",\n',
            '       "databricks_name": "s.databricks_name",\n',
            '       "is_simple_select": "s.is_simple_select",\n',
            '       "run": "s.run"\n',
            '      }\n',
            '  )\n',
            '  .execute()\n',
            ')',
        ]
        self._create_cell(type= "code", source= source)

    def generate_notebook(self):
        """
        Generate a notebook with the necessary objects to use in Power BI reports per universe.
        """
        # Update the source of the FROM when in the is_simple_select field is true on the universe_definitions table
        queries = self._update_schema_based_on_metadata_table(self.all_tables)
        # Add to the source of each script a tb_ in the table_name 
        queries = queries.withColumn("sql_script_new", regexp_replace("sql_script", r"(FROM\s+\{.*?\}\.\w+\.)(\w+)", r"$1tb_$2"))
        # Additional fields to be added to the dataframe
        out_schema = queries.first()['universe_name']

        # Notebook content
        self._notebook_content()
        # Python cell with imports
        self._create_cell(type= "markdown", source= ["### Libraries"])
        self._import_libraries(out_schema)
        
        queries_pd = queries.toPandas()
        for _, row in queries_pd.iterrows():
            table_name = row['table_name']
            sql_script = row['sql_script_new']
            object_type = row['type']

            # Markdown cells per object created in Databricks
            self._create_cell(type= "markdown", source= [f"### View: {table_name}"])

            # Spark ddl sql
            spark_sql = f'spark.sql(f"""\n{sql_script}\n""")'
            self._create_cell(type= "code", source= [spark_sql])

            # MERGE INTO in universe_definitions
            if object_type == 'dt':
                self._merge_cell(table_name= table_name, type=object_type)

        with open(self.ntb_path, 'w', encoding='utf-8') as f:
            json.dump(self.notebook, f, indent=2)