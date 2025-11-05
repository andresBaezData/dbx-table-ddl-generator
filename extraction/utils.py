import re
import pandas as pd
from collections import defaultdict

#funcion que se encarga de llamar todas las funciones que limpian el select de cada objeto
def cleanObjectSelect(string, dfObjectDetails):
    stringCleaned = string.lower()
    stringCleaned = re.sub(r'@catalog\((.*?)\)', r'\1', stringCleaned, flags=re.IGNORECASE)


    # Eliminamos el esquema y el catalogo. "Catalogo"."Esquema"."Tabla"."Columna" pasa a "Tabla"."Columna".
    stringCleaned = re.sub(
    r"(?:'[^']+'\.)?(?:\"[^\"]+\"\.){2}\"[^\"]+\"",
    lambda match: (lambda grupos: f'"{grupos[-2]}"."{grupos[-1]}"' if len(grupos) >= 2 else match.group())(
        re.findall(r'"([^"]+)"', match.group())
    ),
    stringCleaned)

    # hace lo mismo que el codigo de arriba pero permite borrar cuando no se usan comillas dobles. Catalogo.Esquema.Tabla.Columna pasa a Tabla.Columna.
    stringCleaned = re.sub(
        r'\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z0-9_\.]+)',
        lambda m: m.group(2),
        stringCleaned
    )

    stringCleaned = commentAggregationFunctions(stringCleaned, ['min(', 'max(', 'count distinct(', 'sum(', 'avg(', 'count(', '@select('])
    


    return stringCleaned

def commentAggregationFunctions(text: str, functionsArr: list):
    textCopy = str(text)
    containsAggr = any(func.lower() in textCopy for func in functionsArr)
    if containsAggr:
        textCopy = '/*' + textCopy + '*/'
        textCopy = textCopy.replace('\n', '\n --')
    
    return textCopy


def clean_table_name(full_name_str):
    # Limpia un nombre de tabla completamente calificado, quitando esquemas y comillas.
    # - '"ESQUEMA"."TABLA"' -> 'TABLA'
    # - 'ESQUEMA.TABLA'     -> 'TABLA'
    # - '"TABLA"'           -> 'TABLA'

    if not isinstance(full_name_str, str):
        return ""
    # Separa por el punto y limpia las comillas de cada parte
    parts = [part.strip().strip('"') for part in full_name_str.split('.')]
    # Devuelve la última parte, que es el nombre de la tabla
    return parts[-1]

def filterAggregationFunctions(df, columna, funciones_agg):
    #retorna un dataframe excluyendo las filas que contengan alguna de las funciones pasados por parametro
    expr_lower = df[columna].str.lower()
    # Crear patrón regex para funciones de agregación
    funciones_agg_escapadas = [re.escape(func.lower()) for func in funciones_agg]
    patron = '|'.join(funciones_agg_escapadas)
    contiene_agg = expr_lower.str.contains(patron, na=False)
    return df.loc[~contiene_agg]


def createSqlQueryDerivedTables(dfTables):
    #filtro para tener las tablas que son derivadas y no son alias
    dfCopy = dfTables[(dfTables['Table Is Alias'] == 0) & (dfTables['Table Is Derived'] == 1)]
    result = []
    if dfCopy is not None and not dfCopy.empty:
        for _, table in dfCopy.iterrows():
            #limpio un poco el nombre de la tabla y del sql
            derivedSql = table['Derived SQL'].replace('_x000D_', '')
            table_name_clean = table['Table Name'].strip('"')

            sql_text = f"""CREATE OR REPLACE TABLE {{catalog}}.{{schema}}.{table_name_clean}\nTBLPROPERTIES(delta.columnMapping.mode = 'name')\nAS {derivedSql};"""
            result.append({'Table_name': table_name_clean, 'SQL Script':sql_text })
    return pd.DataFrame(result)


def createSqlQueryAliasTables(dfTables, dfObjectDetails, dfFKs):
    result_rows = []
    dfCopyTables = dfTables[ (dfTables['Table Is Alias'] == 1) ].copy()
    for index, table in dfCopyTables.iterrows():
        tableCleanName = clean_table_name( table["Table Name"] )
        originalTableClean = clean_table_name( table["Orig Table"] )
        # Construimos el patrón de búsqueda. Para poder encontrar los objetos asociados a la tabla
        # esto se hace porque hay objetos que tienen multiples tablas asociadas. Entonces cuando pasa eso, inyectamos el sql en las dos tablas
        # si no hacemos este regex y buscamos simplemente un substring con el nombre de la tabla pasa que se duplican campos, porque tenes tablas con nombres muy parecidos 
        patron = r'\b' + re.escape(tableCleanName) + r'\b'
        dfCopyObjectDetails = dfObjectDetails[
            dfObjectDetails["Obj Tables"].str.contains(patron, flags=re.IGNORECASE, regex=True, na=False)
        ].copy()

        dfFKsCopy = dfFKs[( dfFKs["originTable"] ==  tableCleanName.upper())].copy()
        #si la tabla tiene objetos asociados los recorremos
        if dfCopyObjectDetails.empty == False:
            selectColumns = []
            for _, objectDetail in dfCopyObjectDetails.iterrows():
                select = objectDetail['Obj Select']
                #reemplazo el nombre de la tabla alias con el de la tabla original
                select = select.replace(table["Table Name"], table["Orig Table"])
                select = cleanObjectSelect( select, dfObjectDetails )
                alias = objectDetail['Obj Name']
                selectColumns.append(f"    {select} AS `{alias}`")
            
            #recorremos los joins
            for _, fk in dfFKsCopy.iterrows():
                # la fk me viene con el nombre de la tabla alias, pero tengo que reemplazarlo con la tabla original
                select = fk['sql'].replace(tableCleanName, originalTableClean)
                alias = 'id_' + fk['endTable']
                selectColumns.append(f"    {select} AS `{alias}`")
            
            selectClause = "\n" + ",\n".join(selectColumns)

            #borro el nombre del esquema y catalogo, solo me quedo con el nombre de la tabla
            cleanedTableName = clean_table_name(table["Table Name"])
            sql_script = f"""CREATE OR REPLACE VIEW {{out_catalog}}.{{out_schema}}.{"vw_" + cleanedTableName} AS SELECT {selectClause} \n FROM {{in_catalog}}.{{in_schema}}.{originalTableClean};"""
            result_rows.append({'Table_name': f"vw_{cleanedTableName}", 'SQL Script': sql_script})


    return pd.DataFrame(result_rows)

def createSqlOriginalTables(dfTables, dfObjectDetails, dfFKs):
    result_rows = []
    dfCopyTables = dfTables[ (dfTables['Table Is Alias'] == 0) & (dfTables['Table Is Derived'] == 0)].copy()
    for index, table in dfCopyTables.iterrows():
        tableCleanName = clean_table_name( table["Table Name"] )

        # Construimos el patrón de búsqueda. Para poder encontrar los objetos asociados a la tabla
        # esto se hace porque hay objetos que tienen multiples tablas asociadas. Entonces cuando pasa eso, inyectamos el sql en las dos tablas
        # si no hacemos este regex y buscamos simplemente un substring con el nombre de la tabla pasa que se duplican campos, porque tenes tablas con nombres muy parecidos 
        patron = r'\b' + re.escape( table["Table Name"] ) + r'\b'
        dfCopyObjectDetails = dfObjectDetails[
            dfObjectDetails["Obj Tables"].str.contains(patron, flags=re.IGNORECASE, regex=True, na=False)
        ].copy()

        #hago un upper porque en en el dfks viene todo en mayuscula
        dfFKsCopy = dfFKs[( dfFKs["originTable"] ==  tableCleanName.upper())].copy()

        #si la tabla tiene objetos asociados los recorremos
        if dfCopyObjectDetails.empty == False:
            selectColumns = []
            for _, objectDetail in dfCopyObjectDetails.iterrows():
                select = cleanObjectSelect( objectDetail['Obj Select'], dfObjectDetails )
                alias = objectDetail['Obj Name']
                selectColumns.append(f"    {select} AS '{alias}'")
            
            #recorremos los joins
            for _, fk in dfFKsCopy.iterrows():
                select = fk['sql']
                alias = 'id_' + fk['endTable']
                selectColumns.append(f"    {select} AS '{alias}'")
            
            selectClause = "\n" + ",\n".join(selectColumns)


            #borro el nombre del esquema y catalogo, solo me quedo con el nombre de la tabla
            cleanedTableName = clean_table_name(table["Table Name"])
            sql_script = f"""CREATE OR REPLACE VIEW {{out_catalog}}.{{out_schema}}.{"vw_" + cleanedTableName} AS SELECT {selectClause} \n FROM {{in_catalog}}.{{in_schema}}.{cleanedTableName};"""
            result_rows.append({'Table_name': f"vw_{cleanedTableName}", 'SQL Script': sql_script})
    return pd.DataFrame(result_rows)




