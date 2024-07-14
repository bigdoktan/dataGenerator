import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

def convert_csv_to_parquet(input_csv, output_dir, partition_cols=None):
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(input_csv)

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # If partition_cols are specified, create partitions
    if partition_cols:
        for partition_values, sub_df in df.groupby(partition_cols):
            partition_path = os.path.join(output_dir, '/'.join(f"{col}={val}" for col, val in zip(partition_cols, partition_values)))
            os.makedirs(partition_path, exist_ok=True)
            parquet_file = os.path.join(partition_path, 'data.snappy.parquet')
            table = pa.Table.from_pandas(sub_df)
            pq.write_table(table, parquet_file, compression='snappy')
    else:
        # If no partition_cols are specified, write the entire DataFrame as a single Parquet file
        parquet_file = os.path.join(output_dir, 'data.parquet.snappy')
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parquet_file, compression='snappy')

if __name__ == "__main__":
    # Replace 'input.csv' with the path to your CSV file
    input_csv_file = "/Users/doroktan/git/dataGenerator/data generation script/pii.csv"

    # Replace 'output_directory' with the desired output directory path
    output_directory = "output_directory"

    # Replace the partition_cols with the column names you want to partition by, e.g., ['column1', 'column2']
    partition_cols = ['email']

    convert_csv_to_parquet(input_csv_file, output_directory, partition_cols)
