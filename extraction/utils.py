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

def clean_table_name(table_name: str):
    """
    Extract information about catalog, schema and table name from sources of alias and original views.
    The function returns information about schema and table only.
    """
    parts = table_name.strip('"').strip().split('.')
    if len(parts) == 3:
        _, schema, table = parts
    elif len(parts) == 2:
        schema, table = parts
    else:
        schema = None
        table = parts[0]

    # table = table.lower()
    # if schema != None:
    #     schema = schema.lower()

    return schema, table

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
            universe_name = table['Universe Name'].rstrip('.unx').lower()

            sql_text = f"""CREATE OR REPLACE TABLE {{catalog}}.{{schema}}.{table_name_clean}\nTBLPROPERTIES(delta.columnMapping.mode = 'name')\nAS {derivedSql};"""
            result.append({'Table_name': table_name_clean, 'SQL Script': sql_text , 'Universe Name': universe_name, 'Type': "dt"})
    return pd.DataFrame(result)


def createSqlQueryAliasTables(dfTables, dfObjectDetails, dfFKs):
    """
    Creates the DDL SQL for each alias view in Databricks.
    """
    print(dfFKs[dfFKs['originTable'] == 'A_ADDRESS_LINK'])
    result_rows = []
    alias_views = dfTables[(dfTables['Table Is Alias'] == 1)]

    for _, view in alias_views.iterrows():
        # Extract schema and table name information of the source and the alias view
        source_schema, source_table = clean_table_name(view["Orig Table"])
        _, alias_view = clean_table_name(view["Table Name"])

        # Extract the universe name of each alias view
        universe_name = view['Universe Name'].rstrip('.unx').lower()
        
        # Find all the fields related with each alias view
        pattern = r'\b' + re.escape(alias_view) + r'\b'
        alias_fields = dfObjectDetails[dfObjectDetails["Obj Tables"].str.contains(pattern, flags=re.IGNORECASE, regex=True, na=False)]

        # Find all the joins related with each alias view
        alias_joins = dfFKs[(dfFKs["originTable"] ==  alias_view.upper())]

        if not alias_fields.empty:
            selectColumns = []
            for _, row in alias_fields.iterrows():
                select = row['Obj Select'].split(".")[-1].strip('"').lower() # select.replace(view["Table Name"], view["Orig Table"])
                select = cleanObjectSelect(select, dfObjectDetails)
                alias = row['Obj Name']
                selectColumns.append(f"    {select} AS `{alias}`")
            
            for _, fk in alias_joins.iterrows():
                select = fk['sql'].split(".")[-1].strip('"').lower() # fk['sql'].replace(alias_view, source_table)
                alias = 'id_' + fk['endTable'].lower()
                selectColumns.append(f"    {select} AS `{alias}`")
            
            selectClause = "\n" + ",\n".join(selectColumns)

            from_table = f'{source_schema}.{source_table}' if source_schema != None else source_table
            sql_script = f"""CREATE OR REPLACE VIEW {{out_catalog}}.{{out_schema}}.{"vw_" + alias_view.lower()} AS SELECT {selectClause} \n FROM {{in_catalog}}.{from_table.lower()};"""
            result_rows.append({'Table_name': f"vw_{alias_view}", 'SQL Script': sql_script, 'Universe Name': universe_name, 'Type': 'view_report'})

    return pd.DataFrame(result_rows)

def createSqlOriginalTables(dfTables, dfObjectDetails, dfFKs):
    """
    Creates the DDL SQL for each original view in Databricks.
    """

    result_rows = []
    original_views = dfTables[(dfTables['Table Is Alias'] == 0) & (dfTables['Table Is Derived'] == 0)]
    for _, table in original_views.iterrows():
        # Extract schema and table name information of the original view
        original_schema, original_view = clean_table_name(table["Table Name"])
        
        # Extract the universe name of each original view
        universe_name = table['Universe Name'].rstrip('.unx').lower()

        # Find all the fields related with each original view 
        pattern = r'\b' + re.escape( table["Table Name"] ) + r'\b'
        original_fields = dfObjectDetails[dfObjectDetails["Obj Tables"].str.contains(pattern, flags=re.IGNORECASE, regex=True, na=False)]

        # Find all the joins related with each alias view
        original_joins = dfFKs[( dfFKs["originTable"] ==  original_view.upper())].copy()

        if not original_fields.empty:
            selectColumns = []
            for _, row in original_fields.iterrows():
                select = row['Obj Select'].split(".")[-1].strip('"').lower() 
                select = cleanObjectSelect(select, dfObjectDetails)
                alias = row['Obj Name']
                selectColumns.append(f"    {select} AS '{alias}'")
            
            #recorremos los joins
            for _, fk in original_joins.iterrows():
                select = fk['sql'].split(".")[-1].strip('"').lower()
                alias = 'id_' + fk['endTable'].lower()
                selectColumns.append(f"    {select} AS '{alias}'")
            
            selectClause = "\n" + ",\n".join(selectColumns)


            #borro el nombre del esquema y catalogo, solo me quedo con el nombre de la tabla
            originalTableClean = f'{original_schema}.{original_view}' if original_schema != None else original_view
            sql_script = f"""CREATE OR REPLACE VIEW {{out_catalog}}.{{out_schema}}.{"vw_" + original_view.lower()} AS SELECT {selectClause} \n FROM {{in_catalog}}.{originalTableClean.lower()};"""
            result_rows.append({'Table_name': f"vw_{original_view}", 'SQL Script': sql_script, 'Universe Name': universe_name, 'Type': 'view_report'})
    return pd.DataFrame(result_rows)




