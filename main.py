import pandas as pd
import numpy as np
import os
from extraction.extract_info  import Extraction
from utils import get_file_paths, createSqlQueryDerivedTables, createSqlQueryAliasTables, createSqlOriginalTables, filterAggregationFunctions
from createNotebook import createNotebook
from createForeignKeys import processJoinExpressions

#consigo la ruta absoluta de la carpeta input
main_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(main_dir, 'input')
output_path = os.path.join(main_dir, 'output')

universesPaths = get_file_paths(input_path)

for universe in universesPaths:
    universe_file = Extraction(universe)
    derived_tables, alias_tables = universe_file.get_info()
    # Excel

    # Notebook

    # df con todas las tablas que tiene el universo (alias, derivadas y originales)
    dfTableDetails = pd.read_excel(universe, sheet_name="Table Details", engine="openpyxl", header=1)
    dfJoins = pd.read_excel(universe, sheet_name="Joins", engine="openpyxl", header=1)
    
    #filtro para que no me traiga los objetos que son filtros o no tienen select
    # tambien filtro las funciones de agregación
    dfObjectDetails = pd.read_excel(universe, sheet_name="Object Details", engine="openpyxl", header=1)
    dfObjectDetailsCopy = dfObjectDetails[(dfObjectDetails['Obj Select'].notnull()) & (dfObjectDetails['Obj Where'].isnull())].copy()
    # dfObjectDetailsCopy = filterAggregationFunctions(dfObjectDetailsCopy, "Obj Select", ['min(', 'max(', 'count distinct(', 'sum(', 'avg(', 'count('])

    dfFilterDetails = dfObjectDetails[(dfObjectDetails['Obj Where'].notna())].copy()


    foreignKeys = processJoinExpressions(dfJoins)

    dfDerivedTablesSql = createSqlQueryDerivedTables(dfTableDetails)
    dfAliasTablesSql = createSqlQueryAliasTables(dfTableDetails, dfObjectDetailsCopy,foreignKeys)
    dfOriginalTables = createSqlOriginalTables(dfTableDetails, dfObjectDetailsCopy, foreignKeys)


    # se define el orden de concatenación de las tablas
    queries = pd.concat([dfDerivedTablesSql, dfAliasTablesSql, dfOriginalTables])
    createNotebook(queries, universe, output_path)


    base_name = os.path.basename(universe)
    file_name_without_ext = os.path.splitext(base_name)[0]
    debug_filename = f"debug_fks_{file_name_without_ext}.csv"
    full_output_path = os.path.join(output_path, debug_filename)
    foreignKeys.to_csv(full_output_path, index=False)