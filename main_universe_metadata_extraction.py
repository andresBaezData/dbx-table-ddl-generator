# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md
# MAGIC ### Libraries

# COMMAND ----------

import base64
import os
import pandas as pd
import logging
import re
from extraction.extract_info import Extraction, logger
from generate_ntb.create_ntb import GenerateNotebook
from datetime import datetime, timezone
from pyspark.sql.functions import col, lit, when, substring, lower, concat
from IPython.display import HTML
import openpyxl

# COMMAND ----------

# MAGIC %pip install openpyxl

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate notebook

# COMMAND ----------

# Configure logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_name = notebook_path.split('/')[-1]
root = '/Workspace' + notebook_path.replace(notebook_name, '')
input_path = root + 'input_files/'
pattern_universe = r"^([^.]+)\."
process_timestamp = datetime.now().strftime('%Y-%m-%dT_%H-%M-%SZ')
processed_path = root + 'generate_csv/'

excel_files = [excel for excel in os.listdir(input_path) if excel.endswith('.xlsx')]

if excel_files:
    for file in excel_files:
        logger.info(f"Currently processing file {file}")
        # Extraction per universe
        universe = os.path.join(input_path, file)
        universe_file = Extraction(universe)
        all_tables, csv = universe_file.get_info()

        # Prepare output path
        clean_name = re.search(pattern_universe, file).group(1)
        output_path = root.replace('Universe Metadata Extraction',clean_name)
        os.makedirs(output_path, exist_ok=True)
        ntb_file_name = clean_name + '_' + process_timestamp + '.ipynb'
        output_path_file = os.path.join(output_path, ntb_file_name)

        # Prepare Notebook
        ntb = GenerateNotebook(all_tables, output_path_file)
        ntb.generate_notebook()
        logger.info(f"Notebook generated in {output_path_file}")

        # Move file to processed folder
        processed_path_file = os.path.join(processed_path, file)
        os.rename(universe, processed_path_file)
        logger.info(f"Input file moved to {processed_path_file}")
else:
    logger.info(f"No files in folder {input_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Download CSV

# COMMAND ----------

# Read excel from generate_csv/
notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_name = notebook_path.split('/')[-1]
root = '/Workspace' + notebook_path.replace(notebook_name, '')
input_path = root + 'generate_csv/'
excel_files = [excel for excel in os.listdir(input_path) if excel.endswith('.xlsx')]
processed_path = root + 'processed_files/'

if excel_files:
    for file in excel_files:
        # Extraction per universe
        universe_path = os.path.join(input_path, file)
        universe_file = Extraction(universe_path)
        _, csv = universe_file.get_info()
        logger.info('csv variable generated.')

        # CSV Dataframe
        universe = csv['SchemaName'].unique()[0]
        spark_csv = spark.createDataFrame(csv)
        derived_tables = spark_csv.filter(col('DbxTableName').startswith('dt_'))
        views = spark_csv.filter(col('DbxTableName').startswith('vw_'))

        # Created tables and views
        created_objects = spark.sql(f"""
            SELECT 
                table_name AS DbxTableName,
                column_name AS DbxFieldName
            FROM dev_teradata_migration.information_schema.columns
            WHERE table_schema = '{universe}'
        """)

        # CSV derived tables
        if not derived_tables.isEmpty():
            spark_derived_csv = derived_tables.join(created_objects, on= 'DbxTableName')\
                .select(
                    derived_tables.UniverseName,
                    created_objects.DbxFieldName.alias('FieldName'),
                    derived_tables.CatalogName,
                    derived_tables.SchemaName,
                    derived_tables.DbxTableName,
                    created_objects.DbxFieldName,
                    derived_tables.IsKey,
                    derived_tables.RelationalTable,
                    derived_tables.RelationalFieldName
            )
                
            spark_derived_csv = spark_derived_csv.withColumn('FieldName', when(col('FieldName').startswith('id_'), 'None').otherwise(col('FieldName')))
            spark_derived_csv = spark_derived_csv.withColumn('IsKey', when(col('DbxFieldName').startswith('id_'), 'yes').otherwise('no'))
            spark_derived_csv = spark_derived_csv.withColumn('RelationalTable', when(col('DbxFieldName').startswith('id_'), lower(substring(col('DbxFieldName'),4, 100))).otherwise('None'))
            spark_derived_csv = spark_derived_csv.withColumn('RelationalFieldName', when(col('DbxFieldName').startswith('id_'), concat(lit('id_'), col('DbxTableName'))).otherwise('None'))

            csv_derived = spark_derived_csv.toPandas()
        else:
            csv_derived = derived_tables.toPandas()

        # Alias and original views
        if not views.isEmpty():
            spark_views_csv = views.join(created_objects, on= ['DbxTableName', 'DbxFieldName'])\
                .select(
                    views.UniverseName,
                    created_objects.DbxFieldName.alias('FieldName'),
                    views.CatalogName,
                    views.SchemaName,
                    views.DbxTableName,
                    created_objects.DbxFieldName,
                    views.IsKey,
                    views.RelationalTable,
                    views.RelationalFieldName
            )
            spark_views_csv = spark_views_csv.withColumn('FieldName', when(col('FieldName').startswith('id_'), 'None').otherwise(col('FieldName')))
            csv_views = spark_views_csv.toPandas()
        else:
            csv_views = views.toPandas()

        # Move file to processed folder
        processed_path_file = os.path.join(processed_path, file)
        os.rename(universe_path, processed_path_file)
        logger.info(f"Input file moved to {processed_path_file}")

        csv_download = pd.concat([csv_derived, csv_views]).sort_values(['DbxTableName', 'IsKey']).to_csv(index=False)
        data = base64.b64encode(csv_download.encode('UTF-8')).decode('ascii')
        logger.info(f"CSV file generated.")
        display(HTML(f'<a href="data:file/csv;base64,{data}" download="dbx_maping.csv">Download CSV</a>'))
        
else:
    logger.info(f"No files in folder {input_path}")