import pandas as pd
import numpy as np
import json
from typing import Dict, List
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, lit, when

# spark = SparkSession.builder.appName("GenerateNotebook").getOrCreate()

class GenerateNotebook:
    def __init__(self, derived_tables, alias_tables, original_tables, path):
        self.derived_tables =  derived_tables 
        self.alias_tables = alias_tables 
        self.original_tables =  original_tables
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
            'from pyspark.sql import Row\n',
            'from pyspark.sql.functions import col, lit, when\n',
            '\n',
            'in_catalog = "dev_teradata_migration"\n',
            'out_catalog = "dev_teradata_migration"\n',
            f'out_schema = "{out_schema}"\n',
            '\n',
            "universe_df = spark.table('dev_teradata_migration.report_test_data.universe_definitions')\n",
            'schema = universe_df.schema\n'
        ]
        self._create_cell(type= "code", source= source)

    def _insert_cell(self, object_name: str, universe_name: str, type: str, schema: str):
        """
        Create an INSERT INTO for each object in the databricks notebook.
        """
        if type == 'dt':
            # Derived tables
            source = [
                'query = f"""\n',
                '\n',
                '"""\n',
                'row = Row(\n',
                f'    object_name= "{object_name}", universe= "{universe_name}", type= "{type},"\n',
                '    ddl_content = None, ddl_path= None, parent_view= None, ddl_databricks= None,\n'
                f'    schema= "{schema}", ddl_databricks_transformed= query, is_simple_select= False, run= True, databricks_name= "{object_name}"\n',
                ')\n',
                'spark.createDataFrame([row], schema= schema).write.mode("append").insertInto(f"{{out_catalog}}.report_test_data.universe_definitions")'
            ]
        else:
            # Alias and original views
            source = [
                f'query = spark.sql(f"SHOW CREATE TABLE {{out_catalog}}.{{out_schema}}.{object_name}").first()[0]\n',
                'row = Row(\n',
                f'    object_name= "{object_name}", universe= "{universe_name}", type= "{type},"\n',
                '    ddl_content = None, ddl_path= None, parent_view= None, ddl_databricks= None,\n'
                f'    schema= "{schema}", ddl_databricks_transformed= query, is_simple_select= False, run= True, databricks_name= "{object_name}"\n',
                ')\n',
                'spark.createDataFrame([row], schema= schema).write.mode("append").insertInto(f"{{out_catalog}}.report_test_data.universe_definitions")'
            ]
        self._create_cell(type= "code", source= source)

    def _update_cell(self, object_name: str, universe_name: str, type: str, schema: str):
        """
        Create an UPDATE for each object in the databricks notebook.
        """
        if type == 'dt':
            # Derived tables
            source = [
                'query = f"""\n',
                '    \n',
                '"""\n',
                'df_updated = df.withColumn(\n',
                '    "ddl_databricks_transformed",\n',
                '    when(\n',
                f'        (col("object_name") == "{object_name}") & (col("universe") == "{universe_name}") & (col("type") == "{type}"),\n',
                '        lit(query)\n',
                '    ).otherwise(col("ddl_databricks_transformed"))\n'
                ')\n',
                'df_updated.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{{out_catalog}}.report_test_data.universe_definitions")'
            ]
        else:
            # Alias and original views
            source = [
                f'query = spark.sql(f"SHOW CREATE TABLE {{out_catalog}}.{{out_schema}}.{object_name}").first()[0]\n',
                'df_updated = df.withColumn(\n',
                '    "ddl_databricks_transformed",\n',
                '    when(\n',
                f'        (col("object_name") == "{object_name}") & (col("universe") == "{universe_name}") & (col("type") == "{type}")\n',
                '        lit(query)\n',
                '    ).otherwise(col("ddl_databricks_transformed"))\n',
                ')\n',
                'df_updated.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{{out_catalog}}.report_test_data.universe_definitions")'
            ]
        self._create_cell(type= "code", source= source)

    def _update_schema_based_on_metadata_table(self, non_derived_tables):
        """
        Update the schema of the tables based on the metadata table universe_definitions to repoint to general table.
        """
        catalog = 'dev_teradata_migration' # This may change in Production Environment
        schema = 'report_test_data' # This may change in Production Environment 
        metadata_table = 'universe_definitions'
        universe = non_derived_tables['universe_name'].unique()[0].strip()
        
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
        df_query = spark.sql(sql_query)
        new_source_tables = df_query.toPandas()
        
        # Extract the old target table names for all the non derived tables
        pattern_table_name = r"FROM\s+\{[^{}]+\}\.\w+\.(\w+)"
        pattern_schema_name = r"FROM\s+\{[^{}]+\}\.(\w+)\.\w+"
        non_derived_tables['object_name'] = non_derived_tables['sql_script'].str.extract(pattern_table_name)[3:]
        non_derived_tables['old_schema'] = non_derived_tables['sql_script'].str.extract(pattern_schema_name)
 
        #Merge and update the schema of the non derived tables
        updated_df = pd.merge(non_derived_tables, new_source_tables, on=['object_name','old_schema'], how='left')
        new_sql_script = [script.replace(str(old_sch), str(new_sch)) if not pd.isna(new_sch) else script for script, old_sch, new_sch in zip(updated_df['sql_script'], updated_df['old_schema'], updated_df['new_schema'])]
        
        updated_df['new_sql_script'] = new_sql_script

        updated_df.drop(['object_name','old_schema','new_schema', 'sql_script'], axis=1, inplace=True)
        updated_df.rename(columns={'new_sql_script':'sql_script'}, inplace=True)
        
        return updated_df

    def generate_notebook(self):
        """
        """
        queries = pd.concat([self.alias_tables, self.original_tables]).reset_index(drop=True)
        # updated_non_derived_tables = self._update_schema_based_on_metadata_table(non_derived_tables)
        # queries = pd.concat([self.derived_tables, updated_non_derived_tables])
        
        # Add to the source of each script a tb_ in the table_name 
        pattern = r"(FROM\s+\{.*?\}\.\w+\.)(\w+)"
        queries['sql_script'] = queries['sql_script'].str.replace(pattern, r"\1tb_\2", regex=True)
        
        # Additional fields to be added to the dataframe
        queries['schema'] = queries['universe_name'].str.replace(" ", "_")
        out_schema = queries['schema'].unique()[0]
        # queries_spark = spark.createDataFrame(queries)
        # universe_definitions = spark.table('dev_teradata_migration.report_test_data.universe_definitions').select('object_name', 'schema', 'universe', 'type')
        # left_join = (
        #     queries_spark.alias("src")
        #     .join(
        #         universe_definitions.alias("unv"),
        #         (col("src.schema") == col("unv.schema")) & 
        #         (col("table_name") == col("object_name")) & 
        #         (col("src.type") == col("unv.type")) &
        #         (col("universe_name") == col("universe")),
        #         how='left'
        #     )
        #     .withColumn("is_created", when(col("unv.object_name").isNotNull(), 1).otherwise(0))
        #     .select("src.*", "is_created")
        # )
        # queries = left_join.toPandas()
        
        # Notebook content
        self._notebook_content()
        
        # Python cell with imports
        self._import_libraries(out_schema)
        
        for _, row in queries.iterrows():
            table_name = row['table_name']
            sql_script = row['sql_script']
            universe_name = row['universe_name']
            object_type = row['type']
            # schema = row['schema']
            # is_created = row['is_created']

            # Markdown cells per object created in Databricks
            self._create_cell(type= "markdown", source= [f"### View: {table_name}"])

            # Spark ddl sql
            spark_sql = f'spark.sql(f"""\n{sql_script}\n""")'
            self._create_cell(type= "code", source= [spark_sql])

            # INSERT INTO or UPDATE / MERGE in universe_definitions
            # if is_created == 1:
            #     self._update_cell(object_name= table_name, universe_name= universe_name, type=object_type, schema= schema)
            # else:
            #     self._insert_cell(object_name= table_name, universe_name= universe_name, type=object_type, schema= schema)

        with open(self.ntb_path, 'w', encoding='utf-8') as f:
            json.dump(self.notebook, f, indent=2)