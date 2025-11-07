import pandas as pd
import os
from pathlib import Path
from extraction.extract_info import Extraction
from generate_ntb.create_ntb import GenerateNotebook

# Input and output paths
main_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(main_dir, 'input')
output_path = os.path.join(main_dir, 'output')
os.makedirs(input_path, exist_ok=True)
os.makedirs(output_path, exist_ok=True)

universesPaths = [str(p) for p in Path(input_path).rglob('*.xlsx')]

for universe in universesPaths:
    print(universe)
    # Output file name and Output path
    output_file = os.path.splitext(os.path.basename(universe))[0]
    output_file = f'{output_file}.ipynb'
    output_path_file = os.path.join(output_path, output_file)

    # Extract information from the excel
    universe_file = Extraction(universe)
    derived_tables, alias_tables, original_tables = universe_file.get_info()

    # Notebook
    ntb = GenerateNotebook(derived_tables, alias_tables, original_tables, output_path_file)
    ntb.generate_notebook()