import os
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.orc as orc

# Create a new directory for the output files
output_directory = "output_directory"
os.makedirs(output_directory, exist_ok=True)

# Read the pii.csv file and load it into the DataFrame
df = pd.read_csv('pii.csv')

# Divide the DataFrame into parts
num_parts = 6
dfs = np.array_split(df, num_parts)

# Convert and write ORC files
for i, df_part in enumerate(dfs):
    table = pa.Table.from_pandas(df_part)
    orc_file = os.path.join(output_directory, f'part_{i}.orc')
    with pa.OSFile(orc_file, 'wb') as f:
        orc.write_table(table, f)
