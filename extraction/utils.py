import re
import pandas as pd

def cleanObjectSelect(field: str, source_table: str):
    """
    Cleans and standardizes SQL field expressions by removing catalog and schema prefixes,
    simplifying table references, and commenting out aggregation functions.
    """
    # Removes any "@catalog(...)" references
    source_table = source_table.lower()
    stringCleaned = field.lower()
    stringCleaned = stringCleaned.replace('_x000d_', '')
    stringCleaned = re.sub(r'@catalog\((.*?)\)', r'\1', stringCleaned, flags=re.IGNORECASE)

    # Removes catalog and schema names when they are written with double quotes,
    # keeping only the table and column (e.g., "catalog"."schema"."table"."column" → "table"."column")
    stringCleaned = re.sub(
        r"(?:'[^']+'\.)?(?:\"[^\"]+\"\.){2}\"[^\"]+\"",
        lambda match: (lambda grupos: f'"{grupos[-2]}"."{grupos[-1]}"' if len(grupos) >= 2 else match.group())(re.findall(r'"([^"]+)"', match.group())),
        stringCleaned
    )
    
    # Does the same as above but for unquoted names, turning "catalog.schema.table.column" into "table.column"
    stringCleaned = re.sub(
        r'\b([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*\.[A-Za-z0-9_\.]+)',
        lambda m: m.group(2),
        stringCleaned
    )

    # Removes catalog and schema prefixes (with or without quotes) so only the table or column name remains.
    stringCleaned = re.sub(
        r'(?:\"[^\"]+\"\.)?(?:\"([^\"]+)\"\.)\"([^\"]+)\"'  # "catalog"."schema"."table" o "schema"."table"
        r'|(?:\w+\.)?(\w+)\.(\w+)'                          # catalog.schema.table o schema.table
        ,
        lambda m: (m.group(1) or m.group(3)) + '.' + (m.group(2) or m.group(4)),
        stringCleaned
    )

    # Replace source_table with tb_source_table
    stringCleaned = re.sub(rf'\b{re.escape(source_table)}\b', f'tb_{source_table}', stringCleaned)

    # Comment some agg functions.
    stringCleaned = commentAggregationFunctions(stringCleaned, ['min(', 'max(', 'count distinct(', 'sum(', 'avg(', 'count(', '@select('])

    return stringCleaned

def commentAggregationFunctions(text: str, functionsArr: list):
    """
    The function comments out any SQL expression that contains aggregation functions, preventing it from being executed.
    """
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

    return schema, table

def createSqlQueryDerivedTables(dfTables):
    """
    The function builds DDL SQL statements for all derived tables in the input DataFrame and returns them in a structured format.
    """
    dfCopy = dfTables[(dfTables['Table Is Alias'] == 0) & (dfTables['Table Is Derived'] == 1)]
    result = []
    if dfCopy is not None and not dfCopy.empty:
        for _, table in dfCopy.iterrows():
            derivedSql = table['Derived SQL'].replace('_x000D_', '')
            table_name_clean = table['Table Name'].strip('"').lower()
            universe_name = table['Universe Name'].rstrip('.unx').lower()

            sql_text = f"""CREATE OR REPLACE TABLE {{out_catalog}}.{{out_schema}}.{table_name_clean}\nTBLPROPERTIES(delta.columnMapping.mode = 'name')\nAS {derivedSql};"""
            result.append({'table_name': table_name_clean, 'sql_script': sql_text , 'universe_name': universe_name, 'type': "dt"})
    return pd.DataFrame(result)

def createSqlQueryAliasTables(dfTables, dfObjectDetails, dfFKs):
    """
    Creates the DDL SQL for each alias view in Databricks.
    """
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
                select = row['Obj Select'].replace(view["Table Name"], view["Orig Table"])
                select = cleanObjectSelect(select, source_table)
                alias = row['Obj Name']
                selectColumns.append(f"    {select} AS `{alias}`")
            
            for _, fk in alias_joins.iterrows():
                select = fk['sql'].replace(alias_view, f'tb_{source_table}').lower()
                alias = 'id_' + fk['endTable'].lower()
                selectColumns.append(f"    {select} AS `{alias}`")
            
            selectClause = "\n" + ",\n".join(selectColumns)

            alias_view = alias_view.lower()
            from_table = f'{source_schema}.{source_table}' if source_schema != None else source_table
            from_table = from_table.lower()
            sql_script = f"""CREATE OR REPLACE VIEW {{out_catalog}}.{{out_schema}}.{"vw_" + alias_view} AS SELECT {selectClause} \n FROM {{in_catalog}}.{from_table};"""
            result_rows.append({'table_name': f"vw_{alias_view}", 'sql_script': sql_script, 'universe_name': universe_name, 'type': 'view_report'})

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
        original_joins = dfFKs[( dfFKs["originTable"] ==  original_view.upper())]

        if not original_fields.empty:
            selectColumns = []
            for _, row in original_fields.iterrows():
                select = row['Obj Select']
                select = cleanObjectSelect(select, original_view)
                alias = row['Obj Name']
                selectColumns.append(f"    {select} AS `{alias}`")
            
            for _, fk in original_joins.iterrows():
                select = fk['sql'].replace(original_view, f'tb_{original_view}').lower()
                alias = 'id_' + fk['endTable'].lower()
                selectColumns.append(f"    {select} AS `{alias}`")
            
            selectClause = "\n" + ",\n".join(selectColumns)

            original_view = original_view.lower()
            originalTableClean = f'{original_schema}.{original_view}' if original_schema != None else original_view
            originalTableClean = originalTableClean.lower()
            sql_script = f"""CREATE OR REPLACE VIEW {{out_catalog}}.{{out_schema}}.{"vw_" + original_view} AS SELECT {selectClause} \n FROM {{in_catalog}}.{originalTableClean};"""
            result_rows.append({'table_name': f"vw_{original_view}", 'sql_script': sql_script, 'universe_name': universe_name, 'type': 'view_report'})
    return pd.DataFrame(result_rows)




