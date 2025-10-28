from collections import defaultdict
import pandas as pd

# devuelve un datafram con la tabla de origen, la de destino y el sql para armar la fk
def processJoinExpressions(df_joins):
    final_relationships = []
    #recorre cada join del archivo
    for expression in df_joins["Join Expression"]:
        fields_by_table = defaultdict(list)
        conditions = expression.upper().split(' AND ') if isinstance(expression, str) else []

        for condition in conditions:
            # "separamos el codigo por "=" y "."
            parts = [part.strip().strip('"') for part in condition.split('=', 1)]
            if len(parts) != 2:
                continue
            # separamos las igualdades en left y right
            left, right = parts

            #si en los dos lados se selecciona algun campo, se ejecuta el if, no se ejecuta si de algun lado de la condicción hay algun valor hardcodeado
            if '.' not in left or '.' not in right:
                continue
                #extraemos el nombre de la tabla y del campo y los agregamos al diccionario
                #hacemos eso para la parte izquierda y derecha de la igualdad
            left_arr = left.split('.')
            right_arr = right.split('.')
            
            fields_by_table[left_arr[-2]].append(left_arr[-1])
            fields_by_table[right_arr[-2]].append(right_arr[-1])

        #una vez que ya sacamos los datos de las relaciones, los procesamos
        if len(fields_by_table) == 2:
            #estraemos los nombres de las dos tablas
            table_a_name, table_b_name = list(fields_by_table.keys())
            
            #generamos el SQL para la tabla A, contemplando si hay solo una condición o multiples
            table_a_fields = fields_by_table[table_a_name]
            table_a_sql = ""
            if len(table_a_fields) > 1:
                prefixed_fields = [f"{table_a_name}.{field}" for field in table_a_fields]
                table_a_sql = f"concat_ws('-', {', '.join(prefixed_fields)})"
            elif len(table_a_fields) == 1:
                table_a_sql = f"{table_a_name}.{table_a_fields[0]}"

            #generamos el SQL para la tabla B
            table_b_fields = fields_by_table[table_b_name]
            table_b_sql = ""
            if len(table_b_fields) > 1:
                prefixed_fields = [f"{table_b_name}.{field}" for field in table_b_fields]
                table_b_sql = f"concat_ws('-', {', '.join(prefixed_fields)})"
            elif len(table_b_fields) == 1:
                table_b_sql = f"{table_b_name}.{table_b_fields[0]}"

            #agregamos las relaciones a la lista final
            if table_a_sql and table_b_sql:
                # direccion 1: A -> B
                final_relationships.append({
                    'originTable': table_a_name,
                    'endTable': table_b_name,
                    'sql': table_a_sql
                })
                # direccion 2: B -> A
                final_relationships.append({
                    'originTable': table_b_name,
                    'endTable': table_a_name,
                    'sql': table_b_sql
                })
    return pd.DataFrame(final_relationships)