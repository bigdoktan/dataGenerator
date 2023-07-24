import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Read the CSV file
df = pd.read_csv('input.csv')

# Convert the DataFrame to a parquet file
parquet_file = 'output.parquet'
table = pa.Table.from_pandas(df)
pq.write_table(table, parquet_file)

# Divide the parquet file into parts
num_parts = 5

# Read the parquet file into a Table
table = pq.read_table(parquet_file)

# Calculate the number of rows per part
rows_per_part = len(table) // num_parts

# Divide the Table into parts and write each part as a separate parquet file
for i in range(num_parts):
    start_idx = i * rows_per_part
    end_idx = start_idx + rows_per_part

    part_table = table.slice(start_idx, end_idx)
    part_parquet_file = f"part_{i + 1}_{parquet_file}"
    pq.write_table(part_table, part_parquet_file)
