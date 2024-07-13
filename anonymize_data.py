import dask.dataframe as dd
from faker import Faker
import pandas as pd

fake = Faker()

def anonymize_row(row):
    """
    Anonymize a single row of data.
    """
    row['first_name'] = fake.first_name()
    row['last_name'] = fake.last_name()
    row['address'] = fake.address().replace("\n", " ")
    return row

def anonymize_data(input_file, output_pattern):
    """
    Anonymize the data in the input CSV file and write to new CSV files.
    """
    # Read the large CSV file in chunks
    ddf = dd.read_csv(input_file, blocksize=25e6)  # 25MB chunks
    # Apply anonymization
    ddf = ddf.map_partitions(lambda df: df.apply(anonymize_row, axis=1))
    # Write the anonymized data to new CSV files
    ddf.to_csv(output_pattern, single_file=False)
    print("Data anonymized and written to new CSV files.")

if __name__ == "__main__":
    input_file = 'sample_data.csv'
    output_pattern = 'anonymized_data-*.csv'
    anonymize_data(input_file, output_pattern)
