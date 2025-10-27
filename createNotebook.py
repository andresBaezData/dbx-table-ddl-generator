import os
import pandas as pd
import json


def createNotebook(query_df, universe_path, output_dir):
    """
    Crea un archivo de notebook (.ipynb) y lo guarda en el directorio especificado.
    """
    notebook = {
        "cells": [],
        "metadata": {},
        "nbformat": 4,
        "nbformat_minor": 5
    }

    # Añadir el primer bloque de código con solo '%python'
    python_magic_cell = {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": ["%python"]
    }
    notebook["cells"].append(python_magic_cell)

    # Iterar sobre el DataFrame para crear el resto de las celdas
    for index, row in query_df.iterrows():
        table_name = row['Table_name']
        sql_script = row['SQL Script']

        # Celda de Markdown para el título
        markdown_cell = {
            "cell_type": "markdown",
            "metadata": {},
            "source": [f"### View: {table_name}"]
        }
        
        # Crear la celda de código envuelta en spark.sql()
        spark_sql_code = f'spark.sql(f"""\n{sql_script}\n""")'
        
        code_cell = {
            "cell_type": "code",
            "execution_count": None,
            "metadata": {},
            "outputs": [],
            "source": [spark_sql_code]
        }

        notebook["cells"].append(markdown_cell)
        notebook["cells"].append(code_cell)

    # Crear el nombre del archivo y unirlo con la ruta del directorio de salida
    universe_name = os.path.splitext(os.path.basename(universe_path))[0]
    output_filename = f"{universe_name}.ipynb"
    full_output_path = os.path.join(output_dir, output_filename)
    
    # Guardar el notebook en la ruta completa
    with open(full_output_path, 'w', encoding='utf-8') as f:
        json.dump(notebook, f, indent=2)
        
    return full_output_path