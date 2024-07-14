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
    anonymize_data(input_file, output_pattern)import csv
import faker
import time
import os
import concurrent.futures
import multiprocessing
import dask.dataframe as dd
from dask.distributed import Client
import random

fake = faker.Faker()

# Function to generate data and write to a chunked CSV file
def generate_data(file_index, records_per_process, chunk_size):
    csv_file = f'data_part_{file_index}.csv'
    with open(csv_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=["first_name", "last_name", "address", "date_of_birth"])
        writer.writeheader()
        for i in range(0, records_per_process, chunk_size):
            data = [
                {
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "address": fake.address().replace('\n', ', '),
                    "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=90)
                }
                for _ in range(min(chunk_size, records_per_process - i))
            ]
            writer.writerows(data)
            print(f"File {csv_file}: {i + len(data)} records written...")
    return csv_file

# Function to merge CSV files into a single file
def merge_csv_files(num_processes, output_file):
    with open(output_file, mode='w', newline='') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=["first_name", "last_name", "address", "date_of_birth"])
        writer.writeheader()
        for i in range(num_processes):
            with open(f'data_part_{i}.csv', mode='r') as infile:
                reader = csv.DictReader(infile)
                for row in reader:
                    writer.writerow(row)
            os.remove(f'data_part_{i}.csv')
    print(f"All partial files merged into {output_file}")

# Function to determine the number of processes based on system capabilities
def get_num_processes(usage_percentage=0.7):
    total_cpus = multiprocessing.cpu_count()
    max_parallel_processes = total_cpus * 5  # 10 processes per CPU
    num_processes = max(1, int(max_parallel_processes * usage_percentage))
    print(f"Using {num_processes} parallel tasks across {total_cpus} CPUs (70% utilization).")
    return num_processes

# Function to run the data generation process
def run_data_generation(num_records, mode):
    num_processes = get_num_processes()
    records_per_process = num_records // num_processes
    chunk_size = 100000  
    print(f"Running in {mode} mode with {num_processes} processes...")


    start_time = time.time()
    print("Data generation in progress...")
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_processes) as executor:
        futures = [executor.submit(generate_data, i, records_per_process, chunk_size) for i in range(num_processes)]
        for future in concurrent.futures.as_completed(futures):
            print(f"Completed: {future.result()}")

    end_time = time.time()
    print(f"CSV files created with {num_records} records in {end_time - start_time:.2f} seconds.")

    output_file = 'data_sample.csv' if mode == 'default' else 'data_2gb.csv'
    merge_csv_files(num_processes, output_file)

    anonymize_data(output_file, f'anonymized_{output_file}')

# Function to anonymize the data using Dask
def anonymize_data(input_file, output_file):
    print(f"Starting data anonymization for {input_file}...")
    
    client = Client()

    ddf = dd.read_csv(input_file)

    start_time = time.time()

    ddf['first_name'] = ddf['first_name'].apply(lambda x: f"Anonymized_{random.randint(1, 100000)}", meta=('x', 'object'))
    ddf['last_name'] = ddf['last_name'].apply(lambda x: f"Anonymized_{random.randint(1, 100000)}", meta=('x', 'object'))
    ddf['address'] = ddf['address'].apply(lambda x: f"Address_{random.randint(1, 100000)}", meta=('x', 'object'))

    ddf.to_csv(output_file, single_file=True, index=False)

    end_time = time.time()
    print(f"Anonymized large CSV file {output_file} created in {end_time - start_time:.2f} seconds.")

# Main function to choose mode and start the process
if __name__ == '__main__':
    mode = input("Choose mode (default/2GB): ").strip().lower()

    if mode == '2gb':
        if os.path.exists('data_2gb.csv'):
            print("data_2gb.csv already exists. Using the existing file.")
            anonymize_data('data_2gb.csv', 'anonymized_data_2gb.csv')
        else:
            print("2GB mode selected. This will take a few minutes...")
            num_records = 30000000  
            run_data_generation(num_records, mode)
    else:
        print("Default mode selected.")
        num_records = input("Enter the number of records (default is 1 million): ").strip()
        if not num_records.isdigit():
            num_records = 1000000
        else:
            num_records = int(num_records)
        run_data_generation(num_records, mode)

